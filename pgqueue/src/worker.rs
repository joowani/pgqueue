//! The worker: dequeues jobs and runs their handlers with panic containment
//! and timeout enforcement, polls for aborts, fires cron jobs, sweeps the
//! queue, heartbeats worker info, and shuts down gracefully.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use jiff::Timestamp;
use serde_json::Value;
use tokio::sync::{broadcast, watch};
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use uuid::Uuid;

use crate::Error;
#[cfg(feature = "dashboard")]
use crate::dashboard::{
    DashboardRuntime, DashboardServer, DashboardServerConfig, bind_dashboard, wait_for_dashboard_exit,
};
use crate::database::{
    Database, DatabaseAbortClaim, DatabaseCronAuthority, DatabaseCronScheduleResult, LeaseConnection, LeaseIntake,
    WriteDeadline,
};
use crate::job::{
    CronDefinition, CronOptions, JobBuilder, JobContext, JobCronEntry, JobDefinition, JobError, JobErrorKind, JobRow,
    JobStateMap, JobStatus, JobType, TypeErasedJobHandler, validate_duration, validate_json_document,
    validate_nonzero_duration,
};
use crate::queue::{Queue, QueueCounters};
use crate::sweeper::SweepOperations;

const WORKER_INFO_TTL_MULTIPLIER: u32 = 3;
const HARD_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const SHUTDOWN_STEP_TIMEOUT: Duration = Duration::from_secs(1);
// A sweep loop stopped mid-shutdown releases its leadership inside the timer step's `SHUTDOWN_STEP_TIMEOUT`, so the
// release's bounds have to fit in it; see `sweeper::RELEASE_UNLOCK_TIMEOUT`.
const _: () = assert!(
    crate::sweeper::RELEASE_UNLOCK_TIMEOUT.as_millis() + crate::sweeper::RELEASE_CLOSE_TIMEOUT.as_millis()
        < SHUTDOWN_STEP_TIMEOUT.as_millis()
);
const FINALIZE_RETRY_INTERVAL: Duration = Duration::from_secs(1);
/// How long an aborted attempt is given to unwind before the worker finalizes
/// it anyway. A cooperative handler stops at its next `.await`, so this only
/// bounds handlers that block their runtime thread.
const ATTEMPT_ABORT_JOIN_GRACE: Duration = Duration::from_secs(1);
const DEFAULT_ABORT_GRACE: Duration = Duration::from_secs(1);
const MAX_SWEEP_DRAIN_TIME: Duration = Duration::from_secs(1);
/// Secondary bound on one drain, for when passes are fast enough that the wall
/// clock alone would let a tick issue an unbounded number of round trips.
///
/// [`MAX_SWEEP_DRAIN_TIME`] is the bound that should normally bind: a pass that
/// deletes a full `sweep_batch_size` takes single-digit milliseconds, so a
/// second of drain is tens to low hundreds of passes. This ceiling sat at 16,
/// which bound first by an order of magnitude and capped retention at roughly
/// `16 * sweep_batch_size` rows per `sweep` tick — 133 rows/second cluster-wide
/// at the defaults, because sweep leadership is held by one process per queue.
/// A queue retiring jobs faster than that grew its table without bound.
const MAX_SWEEP_DRAIN_PASSES: usize = 256;
/// Consecutive drains that may end with work still pending before the sweeper
/// reports unhealthy. A single behind tick is ordinary after a burst; a run of
/// them means retention is not keeping up with the queue, which is otherwise
/// visible only as a table that keeps growing.
const MAX_SWEEP_BEHIND_TICKS: u32 = 3;
/// Consecutive scheduling passes that may skip the same locked cron row before
/// scheduler health degrades. One or two are ordinary peer contention; a run
/// means the cron is making no progress and a warning alone is insufficient.
const MAX_CRON_CONTENDED_TICKS: u32 = 3;
const DEQUEUE_RETRY_INITIAL_MAX_MS: u64 = 3;
const DEQUEUE_RETRY_MAX_MS: u64 = 100;
/// The most processors one worker may be configured to run. See the check in
/// [`WorkerBuilder::build`] for why a ceiling exists at all.
const MAX_WORKER_CONCURRENCY: usize = 65_536;

/// How long one worker database operation may stay in flight before it is
/// abandoned and reported as failed.
///
/// What this defends against is not a slow query but a connection that will
/// never answer: a failover can leave an *already acquired* pooled connection
/// black-holed, which the pool's `acquire_timeout` does not cover (the
/// connection is out of the pool already) and no statement timeout ends (these
/// queries set none), so the send waits on the OS TCP keepalive — hours on a
/// default Linux. Unbounded, one such connection wedged the loop that awaited
/// it — dequeue, heartbeat, abort poll, finalization, or cron scheduling — for
/// that whole time, while health kept reporting the loop's last success and,
/// for a wedged dequeue, the claim's `FOR UPDATE` row locks pinned up to a
/// batch of due jobs that every other worker then skipped.
///
/// The bound sits well past sqlx's default 30s `acquire_timeout` for the same reason the dashboard's round bound does:
/// a saturated pool spends that long before the query even starts, and a merely queued operation must not be read as a
/// wedged one. Expiry is reported exactly like any other failed operation — every caller already survives those. It
/// frees the loop, not the connection: sqlx returns a dropped operation's connection to the pool only after pinging it,
/// and the ping first drains the abandoned reply, so a connection that will never answer keeps its pool slot until the
/// operating system gives up on the socket.
///
/// A statement waiting on a row lock keeps its slot the same way, for as long as the lock's holder stays open, and that
/// cost is per try, not per connection: the loops that retry a write — a finalization's tries, the lease heartbeat, a
/// cron's reconciliation — parked one more connection per retry behind one operator's `SELECT ... FOR UPDATE`, until
/// the pool was gone. Their retried writes therefore end their lock waits on the server at the same deadline (see
/// `WriteDeadline`), and a reconciliation at a shorter one of its own (`CRON_RECONCILE_LOCK_WAIT`).
const WORKER_DB_OPERATION_TIMEOUT: Duration = Duration::from_secs(60);

/// Runs one worker database operation under [`WORKER_DB_OPERATION_TIMEOUT`].
async fn with_db_deadline<T>(operation: impl Future<Output = Result<T, Error>>) -> Result<T, Error> {
    with_db_deadline_at(tokio::time::Instant::now() + WORKER_DB_OPERATION_TIMEOUT, operation).await
}

/// [`with_db_deadline`] against a deadline the caller already holds.
async fn with_db_deadline_at<T>(
    deadline: tokio::time::Instant,
    operation: impl Future<Output = Result<T, Error>>,
) -> Result<T, Error> {
    match tokio::time::timeout_at(deadline, operation).await {
        Ok(result) => result,
        Err(_) => Err(Error::WorkerTask(DB_OPERATION_DEADLINE_EXCEEDED)),
    }
}

/// What a worker reports for a database operation it abandoned at its deadline — or, for a write run under a
/// [`WriteDeadline`], one whose lock wait the server ended there.
const DB_OPERATION_DEADLINE_EXCEEDED: &str = "worker database operation exceeded its deadline";

/// The least time a periodic heartbeat is given before it is abandoned and retried; see [`heartbeat_deadline`].
const HEARTBEAT_DEADLINE_FLOOR: Duration = Duration::from_secs(5);

/// How long one periodic lease heartbeat may stay in flight, opening the connection it writes on included: one
/// heartbeat interval, within [`HEARTBEAT_DEADLINE_FLOOR`]..=[`WORKER_DB_OPERATION_TIMEOUT`].
///
/// [`WORKER_DB_OPERATION_TIMEOUT`] alone is the wrong bound here, because it outlasts the lease the write renews: the
/// lease is three intervals — 30s at the defaults — so a heartbeat parked on a connection that will never answer let
/// the lease lapse half-way through its 60s deadline. A worker that was alive and still running its attempts then
/// claimed nothing, and once the lapse outlasted the sweep grace the sweeper marked those attempts stuck and this
/// worker's own abort loop cancelled them as swept — a `max_attempts = 1` job finished `aborted` for one stalled write.
/// Bounded by the interval, a stalled heartbeat — issued an interval after the last renewal — is abandoned with an
/// interval of the lease still to run, and the ticker, already overdue, retries at once: on a freshly opened
/// connection, or — for a wait on the lease row's lock, which the server ends at the same deadline — once the lock is
/// gone.
///
/// The heartbeat can keep to that bound because it renews on a connection of its own, outside the queue's pool (see
/// `Database::renew_worker_lease`); the pool had two ways to hold one past it. Its queue: an acquire waits its turn
/// under the pool's 30s `acquire_timeout`, so a deadline counted from before the pool handed a connection over
/// abandoned heartbeats that were merely queued, and the pool is fair, so each retry went to the back of the queue
/// again and no heartbeat landed. And its ping: sqlx tests an idle connection before handing it out, with nothing but
/// that same 30s to bound the test, so a connection that went dead while idle — how a failover, a partition or a NAT
/// entry dropped for idleness usually leaves one — held a heartbeat for the whole default lease, and a failover that
/// killed several held it once for each. A connection of its own has neither: a dead one costs a heartbeat its
/// deadline, and the retry opens another. Only a heartbeat that cannot open one goes through the pool, where it waits
/// its turn as before but takes an idle connection without the test.
///
/// The floor keeps a short interval — tests run them in milliseconds — from abandoning writes that are merely slow:
/// below it a lease is shorter than an ordinary latency spike, and abandoning would turn a late heartbeat into a
/// failed one without buying a retry that could still land in time.
fn heartbeat_deadline(interval: Duration) -> Duration {
    interval.clamp(HEARTBEAT_DEADLINE_FLOOR, WORKER_DB_OPERATION_TIMEOUT)
}

fn worker_info_ttl(timer: Duration) -> Duration {
    timer.saturating_mul(WORKER_INFO_TTL_MULTIPLIER)
}

/// A live worker row whose heartbeat has not expired.
///
/// Read-only, and `#[non_exhaustive]` so a column added to `pgqueue.workers`
/// can be surfaced here without a breaking release.
#[derive(Debug, Clone, serde::Serialize, sqlx::FromRow)]
#[non_exhaustive]
pub struct WorkerInfo {
    /// Worker identifier.
    pub id: Uuid,
    /// Queue processed by the worker.
    pub queue: String,
    /// Worker-local completion counters and uptime.
    pub stats: Value,
    /// Optional user metadata.
    pub metadata: Option<Value>,
    /// When this worker run began.
    #[sqlx(try_from = "jiff_sqlx::Timestamp")]
    pub started_at: Timestamp,
    /// Most recent heartbeat.
    #[sqlx(try_from = "jiff_sqlx::Timestamp")]
    pub heartbeat_at: Timestamp,
    /// When the worker is considered dead unless refreshed.
    #[sqlx(try_from = "jiff_sqlx::Timestamp")]
    pub expires_at: Timestamp,
}

/// Filter for [`Queue::workers_page`].
#[derive(Debug, Clone, Default)]
pub struct WorkerFilter {
    /// Page size (default 25, maximum 100).
    pub limit: Option<i64>,
    /// Return workers after this oldest-first cursor.
    pub after: Option<WorkerCursor>,
}

impl WorkerFilter {
    pub(crate) fn limit(&self) -> Result<i64, Error> {
        let limit = self.limit.unwrap_or(25);
        if !(1..=100).contains(&limit) {
            return Err(Error::Config("worker page limit must be between 1 and 100".into()));
        }
        Ok(limit)
    }
}

/// Stable cursor for oldest-first live-worker pagination.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct WorkerCursor {
    /// Start timestamp of the last worker in the previous page.
    pub started_at: Timestamp,
    /// Worker id used to make timestamp ordering deterministic.
    pub id: Uuid,
}

impl From<&WorkerInfo> for WorkerCursor {
    fn from(worker: &WorkerInfo) -> Self {
        Self { started_at: worker.started_at, id: worker.id }
    }
}

/// Background subsystem represented in [`WorkerHealth`].
///
/// Read-only, and `#[non_exhaustive]` for the same reason as
/// [`WorkerHealthFailure`], which reports one of these: a worker that grows
/// another background loop must not need a breaking release to say so.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum WorkerComponent {
    /// PostgreSQL notification listener.
    Notification,
    /// Job dequeue/fetch loop.
    Dequeue,
    /// Dispatching claimed jobs to registered handlers.
    ///
    /// A failure means this worker claimed a job name it does not register.
    /// The job is safely requeued with an attempt refund, but this worker stays
    /// degraded because its immutable handler registry cannot recover without
    /// a restart or replacement. This keeps rolling deployments safe while
    /// making a permanently mismatched fleet visible to health observers.
    Dispatch,
    /// Recording an attempt's outcome — the other half of the processing path,
    /// and the one that can fail while every other loop stays green.
    ///
    /// A finalization that cannot reach the database is retried for as long as
    /// the attempt lives (a [burst](crate::WorkerBuilder::burst) gives up once
    /// its failures have lasted its dequeue timeout, and fails the run), which
    /// pins the processor slot running it. Once every
    /// slot is pinned that way no processor ever goes idle, so the fetch loop is
    /// never asked for work and [`WorkerComponent::Dequeue`] — the component
    /// that would otherwise have reported the outage — never issues a statement
    /// to fail. The claim compounds it: candidates are taken
    /// `FOR UPDATE ... SKIP LOCKED` while a finish takes a plain `FOR UPDATE`,
    /// so anything holding row locks on part of `pgqueue.jobs` blocks finishes
    /// while dequeues step politely over it.
    ///
    /// Without this component such a worker was stalled outright — every slot
    /// held, claimable work untouched — while [`WorkerHealthStatus::Ready`] and
    /// the dashboard's `/health` (a `SELECT`, which a full disk still answers)
    /// both reported it fine.
    Finalize,
    /// Abort polling loop.
    Abort,
    /// Durable cron scheduler.
    Scheduler,
    /// Cleanup and stuck-job recovery.
    Sweeper,
    /// Worker lease and statistics heartbeat.
    WorkerInfo,
}

/// One currently failing worker subsystem.
///
/// Read-only, and `#[non_exhaustive]`: reporting more about a failure must not
/// need a breaking release.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[non_exhaustive]
pub struct WorkerHealthFailure {
    /// Failing subsystem.
    pub component: WorkerComponent,
    /// Most recent error message.
    pub message: String,
    /// When this failure episode began.
    pub since: Timestamp,
}

/// Aggregate worker lifecycle state.
///
/// Read-only, and `#[non_exhaustive]` for the same reason as
/// [`WorkerHealthSnapshot`], which carries it: a new lifecycle state must not
/// need a breaking release.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum WorkerHealthStatus {
    /// Built but not yet accepting work.
    Starting,
    /// Running with no known background failures.
    Ready,
    /// Running with one or more failing background subsystems.
    Degraded,
    /// The worker run has ended.
    Stopped,
}

/// Point-in-time worker health.
///
/// Read-only, and `#[non_exhaustive]` for the same reason as
/// [`WorkerHealthFailure`].
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[non_exhaustive]
pub struct WorkerHealthSnapshot {
    /// Aggregate lifecycle state.
    pub status: WorkerHealthStatus,
    /// Active component failures, in [`WorkerComponent`] declaration order —
    /// which groups them by subsystem, not alphabetically.
    pub failures: Vec<WorkerHealthFailure>,
}

/// Cloneable observer for a worker's local health state.
#[derive(Clone)]
pub struct WorkerHealth {
    receiver: watch::Receiver<WorkerHealthSnapshot>,
    closed: bool,
}

impl WorkerHealth {
    /// Returns the latest health snapshot without waiting.
    pub fn snapshot(&self) -> WorkerHealthSnapshot {
        self.receiver.borrow().clone()
    }

    /// Waits for a health change and returns the new snapshot.
    ///
    /// Stop waiting after observing [`WorkerHealthStatus::Stopped`]; sender
    /// closure returns the final snapshot once, and later calls remain pending.
    pub async fn changed(&mut self) -> WorkerHealthSnapshot {
        if self.closed {
            std::future::pending::<()>().await;
        }
        if self.receiver.changed().await.is_err() {
            self.closed = true;
        }
        // Marked seen as it is read: `changed` marks the version it woke for, and an update landing between that and a
        // plain `borrow` was returned here and then again by the next call, as a change that never happened.
        let snapshot = self.receiver.borrow_and_update().clone();
        // A run that publishes `Stopped` and *then* drops its sender offers two
        // wakeups for one ending: the value update, and the closure behind it.
        // Keying the end on the closure alone therefore handed `Stopped` to the
        // caller twice, so a shutdown observer that runs terminal handling on it
        // ran that handling twice. The status is the end of the stream whichever
        // wakeup carried it.
        if snapshot.status == WorkerHealthStatus::Stopped {
            self.closed = true;
        }
        snapshot
    }
}

impl std::fmt::Debug for WorkerHealth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("WorkerHealth").field(&self.snapshot()).finish()
    }
}

struct WorkerHealthReporter {
    sender: watch::Sender<WorkerHealthSnapshot>,
    failures: Mutex<HashMap<WorkerComponent, WorkerHealthFailure>>,
    running: AtomicBool,
    stopped: AtomicBool,
}

impl WorkerHealthReporter {
    fn new() -> Self {
        let (sender, _) =
            watch::channel(WorkerHealthSnapshot { status: WorkerHealthStatus::Starting, failures: Vec::new() });
        Self {
            sender,
            failures: Mutex::new(HashMap::new()),
            running: AtomicBool::new(false),
            stopped: AtomicBool::new(false),
        }
    }

    fn subscribe(&self) -> WorkerHealth {
        WorkerHealth { receiver: self.sender.subscribe(), closed: false }
    }

    fn ready(&self) {
        self.running.store(true, Ordering::Release);
        self.publish();
    }

    fn failed(&self, component: WorkerComponent, error: &impl std::fmt::Display) {
        let mut failures = self.lock_failures();
        let message = error.to_string();
        failures
            .entry(component)
            .and_modify(|failure| failure.message.clone_from(&message))
            .or_insert_with(|| WorkerHealthFailure { component, message, since: Timestamp::now() });
        self.publish_locked(&failures);
    }

    fn recovered(&self, component: WorkerComponent) {
        // Called after every successful dequeue, so the overwhelmingly common
        // case is "nothing was failing". Republishing an identical snapshot
        // would take the watch lock per dequeue to discover nothing changed.
        let mut failures = self.lock_failures();
        if failures.remove(&component).is_some() {
            self.publish_locked(&failures);
        }
    }

    /// Whether `component` is currently failing. The fetch loop reads this to
    /// tell a lease that *cannot be maintained* — every heartbeat failing, so
    /// waiting cannot help — from one that was closed under a worker whose
    /// heartbeats still land, which is a coordination state to wait out.
    fn is_failing(&self, component: WorkerComponent) -> bool {
        self.lock_failures().contains_key(&component)
    }

    fn stopped(&self) {
        self.stopped.store(true, Ordering::Release);
        self.publish();
    }

    fn publish(&self) {
        self.publish_locked(&self.lock_failures());
    }

    /// Acquires the failures map, recovering it if a panic poisoned the lock.
    /// Everything done under this lock is a plain map operation, so a panic
    /// cannot leave the map in a broken state and the poisoned data is still
    /// valid. Any other fallback points the wrong way: substituting an empty
    /// map would publish a `Ready` snapshot from the failure path itself, and
    /// skipping would freeze health at a possibly stale `Ready` and let
    /// `stopped` go unpublished.
    fn lock_failures(&self) -> std::sync::MutexGuard<'_, HashMap<WorkerComponent, WorkerHealthFailure>> {
        self.failures.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Publishes the snapshot implied by `failures`, which the caller holds the
    /// lock on.
    ///
    /// Mutating the map and sending have to happen under one acquisition. Two
    /// components reporting concurrently could otherwise mutate in one order
    /// and send in the other, leaving the watch permanently contradicting the
    /// map — and nothing repairs that, because `recovered` short-circuits once
    /// the component is gone from the map, so a phantom `Degraded` would
    /// outlive every later recovery. No `await` happens under the lock.
    fn publish_locked(&self, failures: &HashMap<WorkerComponent, WorkerHealthFailure>) {
        let mut failures = failures.values().cloned().collect::<Vec<_>>();
        failures.sort_by_key(|failure| failure.component);
        let status = if self.stopped.load(Ordering::Acquire) {
            WorkerHealthStatus::Stopped
        } else if !failures.is_empty() {
            WorkerHealthStatus::Degraded
        } else if self.running.load(Ordering::Acquire) {
            WorkerHealthStatus::Ready
        } else {
            WorkerHealthStatus::Starting
        };
        let next = WorkerHealthSnapshot { status, failures };
        self.sender.send_if_modified(|current| {
            if *current == next {
                return false;
            }
            *current = next;
            true
        });
    }
}

#[cfg(test)]
mod worker_health_reporter_tests {
    use std::sync::{Arc, Barrier};

    use super::*;

    /// Two components reporting concurrently must never leave the published
    /// snapshot disagreeing with the failure map. Nothing repairs such a
    /// disagreement: `recovered` short-circuits once the component is gone from
    /// the map, so a phantom `Degraded` would outlive every later recovery and
    /// keep a healthy worker out of a load balancer for the rest of its run.
    #[test]
    fn test_worker_health_snapshot_agrees_with_the_failure_map_under_concurrent_reports() {
        const ROUNDS: usize = 100_000;
        let reporter = Arc::new(WorkerHealthReporter::new());
        reporter.ready();
        // Three parties: both reporting threads and the observer between
        // rounds, so every round is checked while nothing else is running.
        let round_start = Arc::new(Barrier::new(3));
        let round_end = Arc::new(Barrier::new(3));
        let threads = [WorkerComponent::Dequeue, WorkerComponent::Sweeper].map(|component| {
            let reporter = Arc::clone(&reporter);
            let round_start = Arc::clone(&round_start);
            let round_end = Arc::clone(&round_end);
            std::thread::spawn(move || {
                for _ in 0..ROUNDS {
                    round_start.wait();
                    reporter.failed(component, &"transient");
                    reporter.recovered(component);
                    round_end.wait();
                }
            })
        });
        for round in 0..ROUNDS {
            round_start.wait();
            round_end.wait();
            let failures = reporter.failures.lock().unwrap();
            assert!(failures.is_empty(), "both components recovered");
            drop(failures);
            assert_eq!(
                reporter.sender.borrow().clone(),
                WorkerHealthSnapshot { status: WorkerHealthStatus::Ready, failures: Vec::new() },
                "round {round} published a snapshot the failure map does not hold"
            );
        }
        for thread in threads {
            thread.join().unwrap();
        }
    }

    /// A panic under the failures lock must not invert health: reporting a
    /// failure while the lock is poisoned has to publish `Degraded`, never an
    /// empty-failures (`Ready`) snapshot on the failure path itself, and a
    /// later publish must not drop the recorded failures either.
    #[test]
    fn test_worker_health_reports_failures_after_lock_poisoning() {
        let reporter = Arc::new(WorkerHealthReporter::new());
        reporter.ready();
        reporter.failed(WorkerComponent::Sweeper, &"sweep failed");
        assert_eq!(reporter.sender.borrow().status, WorkerHealthStatus::Degraded);
        let poisoner = Arc::clone(&reporter);
        std::thread::spawn(move || {
            let _failures = poisoner.failures.lock().unwrap();
            panic!("poison the failures lock");
        })
        .join()
        .unwrap_err();
        assert!(reporter.failures.is_poisoned());
        reporter.failed(WorkerComponent::Dequeue, &"dequeue failed");
        let snapshot = reporter.sender.borrow().clone();
        assert_eq!(snapshot.status, WorkerHealthStatus::Degraded);
        assert_eq!(
            snapshot.failures.iter().map(|failure| failure.component).collect::<Vec<_>>(),
            [WorkerComponent::Dequeue, WorkerComponent::Sweeper]
        );
        reporter.stopped();
        let snapshot = reporter.sender.borrow().clone();
        assert_eq!(snapshot.status, WorkerHealthStatus::Stopped);
        assert_eq!(snapshot.failures.len(), 2);
    }
}

/// Intervals for the worker's periodic loops.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerTimers {
    /// How often in-flight jobs are checked for abort requests. Default 1s.
    pub abort: Duration,
    /// How often cron jobs are (re-)scheduled. Default 1s.
    pub schedule: Duration,
    /// How often the sweeper purges expired rows and recovers stuck jobs.
    /// Default 60s. When a sweep fills its configured batch, the worker drains
    /// more batches — up to 256 passes, and for at most one second (or this
    /// interval, if shorter) — repeating only the operations that filled their
    /// batch, so a large backlog cannot monopolize the pool the worker dequeues
    /// and finalizes with.
    ///
    /// This also bounds how long an attempt whose owner may still be *alive*
    /// stays unavailable: the sweeper asks it to abort in one pass and recovers
    /// it at the first pass after that mark has aged one
    /// [`QueueBuilder::sweep_grace`](crate::QueueBuilder::sweep_grace) — the
    /// next interval, ordinarily, and no sooner than the grace even when a
    /// full batch makes the drain repeat passes back to back. An attempt whose
    /// owner is already past the abort window is marked and recovered in the
    /// same pass; `sweep_grace` documents what "past" means, and why a merely
    /// lapsed lease is not it.
    pub sweep: Duration,
    /// How often this worker renews its liveness lease in `pgqueue.workers`.
    /// The same write publishes the statistics
    /// [`Queue::workers_page`](crate::Queue::workers_page) and the dashboard
    /// show. Default 10s.
    ///
    /// Each renewal keeps the lease live for three intervals (30s at the
    /// default), and the lease is what the queue reads this worker's liveness
    /// from: the worker claims jobs only while it is live, and the sweeper
    /// recovers the attempts of a worker that stopped renewing it — a crashed
    /// one — only once it has expired and
    /// [`QueueBuilder::sweep_grace`](crate::QueueBuilder::sweep_grace) has
    /// passed on top. That covers attempts without a timeout, retryable ones,
    /// and any holding a dedupe key, a cron's `cron:{name}` included, whose
    /// occurrences are skipped in the meantime. So a crash holds them for about
    /// three intervals plus that grace and the [`WorkerTimers::sweep`] pass
    /// that acts on it. Raising this delays that recovery; lowering it shortens
    /// the lease, so a stall — a lock wait, a database outage, a pause — longer than
    /// three intervals plus the grace gets a still-running worker's attempts
    /// cancelled and run again.
    pub worker_info: Duration,
}

impl Default for WorkerTimers {
    fn default() -> Self {
        Self {
            abort: Duration::from_secs(1),
            schedule: Duration::from_secs(1),
            sweep: Duration::from_secs(60),
            worker_info: Duration::from_secs(10),
        }
    }
}

fn validate_runtime_duration(name: &str, duration: Duration, require_nonzero: bool) -> Result<(), Error> {
    if require_nonzero {
        validate_nonzero_duration(name, duration)?;
    } else {
        validate_duration(name, duration)?;
    }
    if tokio::time::Instant::now().checked_add(duration).is_none() {
        return Err(Error::Config(format!("{name} is too large for the runtime clock")));
    }
    Ok(())
}

/// Configures a [`Worker`]. Created by [`Worker::builder`].
#[must_use = "a WorkerBuilder does nothing until built or run"]
pub struct WorkerBuilder {
    queue: Queue,
    handlers: HashMap<&'static str, TypeErasedJobHandler>,
    state: JobStateMap,
    concurrency: usize,
    timers: WorkerTimers,
    crons: Vec<(String, crate::job::JobRequest, CronOptions)>,
    burst: bool,
    max_burst_jobs: Option<usize>,
    dequeue_timeout: Option<Duration>,
    poll_interval: Duration,
    abort_grace: Duration,
    shutdown_grace: Duration,
    metadata: Option<Value>,
    #[cfg(feature = "dashboard")]
    dashboard: Option<DashboardServer>,
    /// [`WORKER_DB_OPERATION_TIMEOUT`] unless a test shortened it; see [`WorkerInner::finalize_deadline`].
    finalize_deadline: Duration,
    error: Option<Error>,
}

impl WorkerBuilder {
    /// Shortens how long one try at recording an attempt's outcome may stay in flight, so a test can drive a
    /// finalization through several abandoned tries without waiting out the shipped one-minute bound. Not a public
    /// knob, for the reason [`WORKER_DB_OPERATION_TIMEOUT`] gives: a deadline short enough to catch a merely queued
    /// write reads as an outage.
    #[cfg(feature = "_test")]
    pub(crate) fn with_finalize_deadline(mut self, deadline: Duration) -> Self {
        self.finalize_deadline = deadline;
        self
    }

    /// Adds a generated handler to the registry unless its exact Rust type is
    /// already present. A shared database name on two distinct types remains a
    /// configuration error: rows are dispatched by that name, so silently
    /// choosing either handler would decode some payloads with the wrong type.
    fn ensure_handler<J: JobType>(&mut self) {
        let handler = J::erased();
        let name = handler.name();
        match self.handlers.get(name) {
            Some(existing) if existing.type_id() == handler.type_id() => {}
            Some(_) if self.error.is_none() => {
                self.error = Some(Error::Config(format!("job name {name:?} is used by multiple job types")));
            }
            Some(_) => {}
            None => {
                self.handlers.insert(name, handler);
            }
        }
    }

    /// Registers a handler defined with `#[pgqueue::job]`.
    ///
    /// A worker must register every job name enqueued on its queue: workers
    /// claim without filtering by name, so a queue is the unit of worker
    /// capability, and a fleet where some workers run some job types is
    /// expressed as one queue per worker shape. A claimed job with no handler
    /// is not lost — the worker requeues it with an attempt refund and a short
    /// delay, which is what makes a rolling deploy safe: jobs of a new type
    /// enqueued before the old workers restart bounce until a new worker picks
    /// them up. The old worker reports [`WorkerComponent::Dispatch`] as failed
    /// after its first bounce, but keeps processing job names it does handle.
    pub fn register_job<J: JobDefinition>(mut self, _job: J) -> Self {
        self.ensure_handler::<J>();
        self
    }

    /// Registers a handler and its compile-time `#[pgqueue::cron]` schedule.
    ///
    /// The schedule runs with the attribute's revision and the default
    /// missed-occurrence policy. For [`CronMisfirePolicy`](crate::CronMisfirePolicy)
    /// variants beyond that default, register the handler as a plain job and use
    /// [`WorkerBuilder::schedule_cron_with_options`].
    pub fn register_cron<J: CronDefinition>(mut self, _cron: J) -> Self {
        self.ensure_handler::<J>();
        // Cron payloads are always `()` (the #[pgqueue::cron] contract), which
        // serializes to null.
        let mut template = crate::job::JobRequest::new(J::NAME, Value::Null);
        template.config = J::config();
        self.crons.push((
            J::SCHEDULE.to_string(),
            template,
            CronOptions { revision: J::CRON_REVISION, ..CronOptions::default() },
        ));
        self
    }

    /// Schedules a job on a standard five-field cron expression decided at
    /// runtime and evaluated in UTC:
    /// `.schedule_cron(&expr_from_config, cleanup::job(()))`.
    ///
    /// The handler is registered by this call. When the schedule is known at
    /// compile time, prefer `#[pgqueue::cron("...")]` and
    /// [`WorkerBuilder::register_cron`]. This shorthand uses revision 0 and the
    /// default skip policy; use [`WorkerBuilder::schedule_cron_with_options`]
    /// before changing a persisted definition.
    ///
    /// Cron jobs are deduplicated on
    /// `cron:{job name}` (or the builder's explicit `dedupe_key`), so a
    /// schedule has at most one live job row across current workers — not
    /// merely one per occurrence. Occurrences never overlap and never queue
    /// behind each other: one that comes due while an earlier occurrence is
    /// still live (queued, running, aborting, or waiting out a retry delay) is
    /// skipped with a warning, and the schedule resumes at the next occurrence
    /// after the holder finishes. Job execution remains at least once. Manual retries are the
    /// exception: [`crate::Queue::retry_job`] on a terminal occurrence produces a keyless one-off
    /// outside the schedule that can run beside a live scheduled occurrence.
    ///
    /// The cron expression owns every occurrence's run time, so a builder
    /// carrying [`JobBuilder::delay`] or [`JobBuilder::at`] makes `build()`
    /// fail instead of silently ignoring the override.
    ///
    /// # Schedule rows are durable
    ///
    /// Registering a cron writes a row to `pgqueue.cron_schedules` keyed by its
    /// dedupe key. The sweeper never removes schedules, so deleting a cron from
    /// code leaves its harmless row behind until [`crate::Queue::remove_cron_schedule`]
    /// removes it. Stop every worker that still registers the key first, or its
    /// next reconciliation pass recreates the row.
    pub fn schedule_cron<J: JobDefinition>(self, expr: &str, job: JobBuilder<J>) -> Self {
        self.schedule_cron_with_options(expr, job, CronOptions::default())
    }

    /// Schedules a config-driven cron job with an explicit durable revision
    /// and misfire policy. Increase the revision whenever the expression or
    /// job template changes. A template-only revision preserves the durable
    /// cursor; changing the expression starts at its next UTC occurrence.
    ///
    /// Reusing a revision for a different definition is a deploy mistake, but
    /// it never stops the worker: the durable definition wins, this cron is
    /// disabled on this worker, and [`Worker::health`] reports
    /// [`WorkerComponent::Scheduler`] as failed while ordinary jobs keep
    /// flowing. Watch health (or the dashboard) to catch it.
    pub fn schedule_cron_with_options<J: JobDefinition>(
        mut self,
        expr: &str,
        job: JobBuilder<J>,
        options: CronOptions,
    ) -> Self {
        self.ensure_handler::<J>();
        match job.into_cron_template() {
            Ok(template) => self.crons.push((expr.to_string(), template, options)),
            Err(error) if self.error.is_none() => self.error = Some(error),
            Err(_) => {}
        }
        self
    }

    /// Shares a value with handlers via the [`crate::JobState`] extractor.
    pub fn state<T: Clone + Send + Sync + 'static>(mut self, value: T) -> Self {
        self.state.insert(value);
        self
    }

    /// Maximum jobs processed concurrently. Default 10. Zero and anything above
    /// 65,536 are rejected by [`WorkerBuilder::build`] — a worker that would
    /// process nothing is a configuration mistake to report, not one to round up
    /// to 1 behind the caller's back, and every other out-of-range value here is
    /// reported. The upper bound is there because each unit is a spawned task
    /// the worker allocates before it is ready, and it also multiplies how many
    /// payloads one dequeue buffers.
    pub fn concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency;
        self
    }

    /// Overrides the periodic loop intervals.
    pub fn timers(mut self, timers: WorkerTimers) -> Self {
        self.timers = timers;
        self
    }

    /// Burst mode: drain currently due work and return instead of running
    /// forever. Future scheduled work, including delayed retries, is left due
    /// for a later worker run, as is a due row another transaction has held a
    /// row lock on for a whole [`WorkerBuilder::dequeue_timeout`].
    /// Requires [`WorkerBuilder::dequeue_timeout`].
    ///
    /// A burst also *ends* on a database it cannot reach, rather than retrying
    /// forever: dequeues that keep failing for a whole
    /// [`WorkerBuilder::dequeue_timeout`] make [`Worker::run_until`] return the
    /// failing [`Error`] instead of `Ok(())`. A queue nobody could read is not
    /// an empty one, and this is the distinction a cron or CI invocation exits
    /// on. The same budget bounds the burst's other ways of silently doing
    /// nothing: registered crons whose reconciliation or publication keeps
    /// failing, a worker lease this worker cannot write — with no live
    /// accepting lease nothing is ever claimable, so waiting longer could only
    /// hang — and an attempt outcome that cannot be recorded, which is left
    /// `running` for recovery. Each fails the run rather than reporting a
    /// drain. Two lease states are deliberately *not* failures: a cron
    /// superseded by a higher revision is skipped exactly as the continuous
    /// scheduler skips it, and a lease closed by an outside actor while this
    /// worker's heartbeats still land is waited out, because whoever closed
    /// intake can reopen it.
    pub fn burst(mut self, burst: bool) -> Self {
        self.burst = burst;
        self
    }

    /// In burst mode, stop after processing this many jobs even if the queue
    /// isn't drained. Requires [`WorkerBuilder::burst`]; `build()` rejects it
    /// otherwise.
    pub fn max_burst_jobs(mut self, max: usize) -> Self {
        self.max_burst_jobs = Some(max);
        self
    }

    /// How long an idle processor waits for work before declaring the queue
    /// drained (burst mode only).
    ///
    /// It bounds the wait for work that is due but *unclaimable* too. The claim
    /// takes candidate rows `FOR UPDATE ... SKIP LOCKED` while the availability
    /// probe beside it does not, so a `queued`, due row that an unrelated open
    /// transaction holds a row lock on — an operator's `SELECT ... FOR UPDATE`,
    /// an uncommitted `UPDATE`, a session simply left idle in a transaction —
    /// reports work no claim can take. A burst worker retries such a row for
    /// this long and then leaves it queued for a later run, so a session like
    /// that delays [`Worker::run_until`] rather than hanging it. That clock
    /// starts at the first fetch that could not take the row and runs alongside
    /// the idle clock above, both of which must elapse, so a burst returns
    /// within roughly twice this after its last claimable job.
    ///
    /// It bounds a run of *failed* dequeues the same way, and for the same
    /// reason: a fetch that never reached the database leaves burst demand
    /// outstanding, so nothing could conclude a drain and `run_until` never
    /// returned at all. Past this long of consecutive failures the burst ends —
    /// as `Err`, not as a drain, so an unreachable database and an empty queue
    /// stay distinguishable to the caller.
    pub fn dequeue_timeout(mut self, timeout: Duration) -> Self {
        self.dequeue_timeout = Some(timeout);
        self
    }

    /// Fallback polling interval when notifications are quiet. Default 1s.
    pub fn poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    /// How long a handler may react to a user abort before its task is forcibly
    /// stopped. Default 1s. Sweeper cancellations, attempt timeouts, and a job
    /// row deleted under a running attempt remain immediate: the row that
    /// granted the attempt its dedupe exclusivity is already gone or has been
    /// handed to another attempt.
    pub fn abort_grace(mut self, grace: Duration) -> Self {
        self.abort_grace = grace;
        self
    }

    /// How long in-flight handlers may finish, and their outcomes be recorded,
    /// after shutdown cancels their cooperative token. Transient failures
    /// writing an outcome are retried inside this window too, so a handler that
    /// succeeded during the drain is not lost to one database blip. When the
    /// grace period expires, the tasks are forcibly stopped and their attempts
    /// are requeued with the attempt refunded — `max_attempts` is raised rather
    /// than `attempts` lowered, so the pair a job displays drifts upward across
    /// restarts that catch it mid-flight (see
    /// [`JobRow::max_attempts`](crate::JobRow)). Default 30s.
    ///
    /// A handler that stops cooperatively may return a [`JobError`] classified
    /// as [`JobErrorKind::Aborted`] to requeue the unfinished attempt with the
    /// same refund. Every other returned error is recorded as the handler's
    /// outcome, even when it happens during this grace period.
    ///
    /// It also bounds shutdown's first durable act — closing this worker's
    /// lease to new work — except that this one step is never given less than
    /// one second, so a zero or very short grace still records the close.
    pub fn shutdown_grace(mut self, grace: Duration) -> Self {
        self.shutdown_grace = grace;
        self
    }

    /// Arbitrary metadata shown alongside this worker in the dashboard.
    pub fn metadata(mut self, metadata: Value) -> Self {
        self.metadata = Some(metadata);
        self
    }

    /// Runs a configured dashboard server in this worker's process. Requires the
    /// `dashboard` feature.
    ///
    /// Bind failures and dashboard task panics are worker infrastructure
    /// errors. The server starts and stops with [`Worker::run`] or
    /// [`Worker::run_until`]. A later call replaces the previous dashboard.
    ///
    /// The socket is bound before processing starts so address conflicts fail
    /// fast. Use the intentionally unauthenticated `/health` endpoint rather
    /// than a TCP-only readiness check.
    /// Multiple workers in one network namespace must use distinct dashboard
    /// addresses or enable the dashboard on only one worker.
    ///
    /// ```no_run
    /// # #[pgqueue::job]
    /// # async fn cleanup(_: ()) {}
    /// # async fn run(queue: pgqueue::Queue) -> anyhow::Result<()> {
    /// let dashboard = pgqueue::Dashboard::new([queue.clone()])
    ///     .basic_auth("admin", "password")
    ///     .serve_on("localhost", 8080);
    /// pgqueue::Worker::builder(queue)
    ///     .register_job(cleanup)
    ///     .dashboard(dashboard)
    ///     .run()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "dashboard")]
    pub fn dashboard(mut self, server: DashboardServer) -> Self {
        self.dashboard = Some(server);
        self
    }

    /// Validates, builds, and runs the worker until `SIGINT` or `SIGTERM` (or
    /// until the queue drains in burst mode).
    ///
    /// Use [`WorkerBuilder::build`] when the worker's id, queue, or health
    /// observer is needed before it starts.
    ///
    /// ```no_run
    /// # #[pgqueue::job]
    /// # async fn cleanup(_: ()) {}
    /// # async fn run(queue: pgqueue::Queue) -> Result<(), pgqueue::Error> {
    /// pgqueue::Worker::builder(queue)
    ///     .register_job(cleanup)
    ///     .run()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn run(self) -> Result<(), Error> {
        self.build()?.run().await
    }

    /// Validates, builds, and runs the worker until `shutdown` is cancelled
    /// (or until the queue drains in burst mode).
    ///
    /// Use [`WorkerBuilder::build`] when the worker's id, queue, or health
    /// observer is needed before it starts.
    pub async fn run_until(self, shutdown: CancellationToken) -> Result<(), Error> {
        self.build()?.run_until(shutdown).await
    }

    /// Validates the configuration and builds the worker.
    pub fn build(self) -> Result<Worker, Error> {
        if let Some(error) = self.error {
            return Err(error);
        }
        if self.handlers.is_empty() {
            return Err(Error::Config("no jobs registered on this worker".into()));
        }
        if self.burst && self.dequeue_timeout.is_none() {
            return Err(Error::Config("burst mode requires WorkerBuilder::dequeue_timeout".into()));
        }
        if self.max_burst_jobs.is_some() && !self.burst {
            return Err(Error::Config("max_burst_jobs requires WorkerBuilder::burst(true)".into()));
        }
        if self.concurrency == 0 {
            return Err(Error::Config("worker concurrency must be greater than zero".into()));
        }
        // Bounded, not merely fitted to the `bigint` the dequeue limit is bound
        // as. `run_until_inner` spawns one processor task per unit before it can
        // report readiness, and a task costs on the order of a few kilobytes:
        // 200,000 processors were measured at 856 MiB of RSS, so `i64::MAX` is
        // an allocation loop that ends at the OOM killer. This builder documents
        // that every out-of-range value is reported rather than silently
        // adjusted, and a concurrency no process can host is out of range
        // however well it fits a column. Being the tighter of the two bounds, it
        // is also the only one: everything that reaches it fits `i64` by
        // construction. The ceiling is far above any real worker — the batch a
        // dequeue claims is the idle-processor count, so concurrency is also
        // the multiplier on how many payloads the intake buffer holds.
        if self.concurrency > MAX_WORKER_CONCURRENCY {
            return Err(Error::Config(format!("worker concurrency must not exceed {MAX_WORKER_CONCURRENCY}")));
        }
        // `pgqueue.workers.metadata` is `jsonb`, which cannot hold `\0`. The
        // lease write reports its failure through health and a log rather than
        // to a caller, so metadata carrying one leaves a worker that starts,
        // holds no lease, and — because dequeueing requires a live accepting
        // lease — processes nothing for as long as it runs. `JobRequest` refuses
        // the same byte on the enqueue side.
        if let Some(metadata) = self.metadata.as_ref() {
            validate_json_document("worker metadata", metadata).map_err(Error::Config)?;
        }
        for (name, duration) in [
            ("abort timer", self.timers.abort),
            ("schedule timer", self.timers.schedule),
            ("sweep timer", self.timers.sweep),
            ("worker info timer", self.timers.worker_info),
            ("poll interval", self.poll_interval),
        ] {
            validate_runtime_duration(name, duration, true)?;
        }
        let worker_info_ttl = worker_info_ttl(self.timers.worker_info);
        validate_duration("worker info TTL", worker_info_ttl)?;
        validate_runtime_duration("abort grace", self.abort_grace, false)?;
        validate_runtime_duration("shutdown grace", self.shutdown_grace, false)?;
        if let Some(timeout) = self.dequeue_timeout {
            validate_runtime_duration("dequeue timeout", timeout, true)?;
        }
        let mut crons = Vec::new();
        let mut cron_keys = HashSet::new();
        for (expr, template, options) in self.crons {
            if !self.handlers.contains_key(template.name.as_str()) {
                return Err(Error::Config(format!("cron job {:?} is not registered on this worker", template.name)));
            }
            let entry = JobCronEntry::with_options(&expr, template, options)?;
            if !cron_keys.insert(entry.dedupe_key.clone()) {
                return Err(Error::Config(format!("cron dedupe key {:?} registered more than once", entry.dedupe_key)));
            }
            // A cron whose priority falls outside the queue's dequeue window
            // publishes an occurrence that can never be claimed — and because
            // that occurrence holds the schedule's dedupe key, every later one
            // is skipped as held. The schedule stops after exactly one
            // occurrence, with nothing on health to say so. The builder already
            // holds both halves, so this is a configuration error to report
            // rather than a runtime state to discover.
            let (low, high) = self.queue.database().priorities();
            let priority = entry.template.config.priority;
            if priority < low || priority > high {
                return Err(Error::Config(format!(
                    "cron {:?} has priority {priority}, outside this queue's dequeue range {low}..={high}; \
                     its occurrences could never be claimed",
                    entry.template.name
                )));
            }
            crons.push(entry);
        }

        let health = WorkerHealthReporter::new();

        #[cfg(feature = "dashboard")]
        let dashboard =
            self.dashboard.map(|dashboard| dashboard.into_server_config(Some(health.subscribe()))).transpose()?;

        let database = self.queue.database_handle();
        Ok(Worker {
            inner: Arc::new(WorkerInner {
                queue: self.queue,
                database,
                handlers: self.handlers,
                state: Arc::new(self.state),
                concurrency: self.concurrency,
                timers: self.timers,
                crons,
                burst: self.burst,
                dequeue_timeout: self.dequeue_timeout,
                poll_interval: self.poll_interval,
                abort_grace: self.abort_grace,
                shutdown_grace: self.shutdown_grace,
                metadata: self.metadata,
                #[cfg(feature = "dashboard")]
                dashboard,
                finalize_deadline: self.finalize_deadline,
                id: Uuid::now_v7(),
                started: OnceLock::new(),
                counters: QueueCounters::default(),
                inflight: Mutex::new(HashMap::new()),
                burst_budget: self.max_burst_jobs.map(AtomicUsize::new),
                intake_open: AtomicBool::new(true),
                unhandled_warned_at: Mutex::new(HashMap::new()),
                finalize_failing: Mutex::new(0),
                health,
            }),
        })
    }
}

/// A job-processing worker bound to one [`Queue`].
///
/// Run workers on Tokio's multi-thread runtime. Timeouts, heartbeats, and
/// shutdown are enforced by tasks that need a runtime thread of their own when
/// a handler blocks one: on the current-thread runtime a single
/// thread-blocking handler stalls every worker loop — the attempt deadline
/// included — until it yields, and blocking work belongs on
/// `tokio::task::spawn_blocking` either way.
pub struct Worker {
    inner: Arc<WorkerInner>,
}

struct WorkerInner {
    queue: Queue,
    database: Arc<Database>,
    handlers: HashMap<&'static str, TypeErasedJobHandler>,
    state: Arc<JobStateMap>,
    concurrency: usize,
    timers: WorkerTimers,
    crons: Vec<JobCronEntry>,
    burst: bool,
    dequeue_timeout: Option<Duration>,
    poll_interval: Duration,
    abort_grace: Duration,
    shutdown_grace: Duration,
    metadata: Option<Value>,
    #[cfg(feature = "dashboard")]
    dashboard: Option<DashboardServerConfig>,
    /// How long one try at recording an attempt's outcome may stay in flight: [`WORKER_DB_OPERATION_TIMEOUT`], which
    /// only a test shortens.
    finalize_deadline: Duration,
    id: Uuid,
    started: OnceLock<std::time::Instant>,
    counters: QueueCounters,
    /// In-flight attempts, keyed by job id *and* attempt number: recovery can
    /// take a row from a live attempt and this worker can then re-claim it as
    /// the next attempt, so the same id is briefly in flight twice. Keying by
    /// id alone let the newcomer overwrite its predecessor's entry, and the
    /// displaced attempt was never asked about again.
    inflight: Mutex<HashMap<(Uuid, i32), WorkerInflightJob>>,
    /// Remaining burst-mode job budget (only meaningful with max_burst_jobs).
    burst_budget: Option<AtomicUsize>,
    /// Whether this worker still takes new work. Every lease write reads it, so
    /// a lease *created* by a heartbeat — the worker's first, or a replacement
    /// for one the sweeper purged while the worker was stalled — starts in the
    /// state the worker is actually in rather than defaulting to accepting.
    intake_open: AtomicBool,
    /// When this worker last warned about claiming each job name it has no
    /// handler for, so a deploy window bouncing many such jobs warns once per
    /// name per [`UNHANDLED_JOB_WARNING_INTERVAL`] instead of once per
    /// bounce. Bounded in [`warn_unhandled_bounce`].
    unhandled_warned_at: Mutex<HashMap<String, std::time::Instant>>,
    /// Attempts whose finalization is failing right now; see [`FinalizeFailure`].
    finalize_failing: Mutex<usize>,
    health: WorkerHealthReporter,
}

struct WorkerHealthStopGuard(Arc<WorkerInner>);

impl Drop for WorkerHealthStopGuard {
    fn drop(&mut self) {
        self.0.health.stopped();
    }
}

impl Worker {
    /// Starts configuring a worker for the given queue.
    pub fn builder(queue: Queue) -> WorkerBuilder {
        WorkerBuilder {
            queue,
            handlers: HashMap::new(),
            state: JobStateMap::default(),
            concurrency: 10,
            timers: WorkerTimers::default(),
            crons: Vec::new(),
            burst: false,
            max_burst_jobs: None,
            dequeue_timeout: None,
            poll_interval: Duration::from_secs(1),
            abort_grace: DEFAULT_ABORT_GRACE,
            shutdown_grace: Duration::from_secs(30),
            metadata: None,
            #[cfg(feature = "dashboard")]
            dashboard: None,
            finalize_deadline: WORKER_DB_OPERATION_TIMEOUT,
            error: None,
        }
    }

    /// This worker's id (UUIDv7, minted at build time).
    pub fn id(&self) -> Uuid {
        self.inner.id
    }

    /// The queue this worker processes.
    pub fn queue(&self) -> &Queue {
        &self.inner.queue
    }

    /// Returns a cloneable observer that remains usable while `run` consumes
    /// the worker.
    pub fn health(&self) -> WorkerHealth {
        self.inner.health.subscribe()
    }

    /// Runs until `SIGINT`/`SIGTERM` (or the queue drains, in burst mode),
    /// then shuts down gracefully.
    pub async fn run(self) -> Result<(), Error> {
        let token = CancellationToken::new();
        let run = self.run_until(token.clone());
        tokio::pin!(run);
        tokio::select! {
            result = &mut run => result,
            _ = wait_for_shutdown_signal() => {
                token.cancel();
                run.await
            }
        }
    }

    /// Runs until `shutdown` is cancelled (or the queue drains, in burst
    /// mode). The embeddable, test-friendly entry point.
    ///
    /// Dropping this future starts the same graceful shutdown in a background
    /// task, so worker infrastructure and in-flight jobs are not abandoned.
    pub async fn run_until(self, shutdown: CancellationToken) -> Result<(), Error> {
        let dropped = CancellationToken::new();
        let drop_guard = dropped.clone().drop_guard();
        let result = tokio::spawn(self.run_until_inner(shutdown, dropped)).await?;
        drop_guard.disarm();
        result
    }

    async fn run_until_inner(self, shutdown: CancellationToken, dropped: CancellationToken) -> Result<(), Error> {
        let inner = self.inner;
        let _health_stop = WorkerHealthStopGuard(inner.clone());
        #[cfg(feature = "dashboard")]
        let bound_dashboard = match before_shutdown(&shutdown, &dropped, bind_dashboard(inner.dashboard.as_ref())).await
        {
            Some(bound) => bound?,
            None => return Ok(()),
        };
        inner.started.get_or_init(std::time::Instant::now);

        tracing::info!(
            worker.id = %inner.id, queue = %inner.queue.name(),
            concurrency = inner.concurrency, burst = inner.burst, "worker starting"
        );
        let mut cron_state = CronSchedulingState::default();
        if !inner.crons.is_empty() {
            // A burst exists to have bounded runtime, so its startup reconciliation gets the same `dequeue_timeout`
            // budget its other stalls get: each wedged operation is otherwise entitled to a whole
            // `WORKER_DB_OPERATION_TIMEOUT`, per cron, before the budget the caller configured in milliseconds is even
            // consulted.
            //
            // Every cron therefore starts out unreconciled, and reconciliation takes each one it settles off the list,
            // so an entry the budget cuts off stays queued for the retries the burst scheduler runs under its own
            // budget, and fails the run with if they cannot settle. Left off the list, it was scheduled as though
            // settled, against a stored row it had never upgraded: a pending revision bump read as a definition
            // conflict and disabled the cron, while `run_until` still returned `Ok`.
            cron_state.unreconciled.extend(inner.crons.iter().map(|entry| entry.dedupe_key.clone()));
            let reconcile = async {
                let reconcile = reconcile_all_crons(&inner, &mut cron_state);
                match inner.burst.then_some(inner.dequeue_timeout).flatten() {
                    Some(budget) => {
                        let _ = tokio::time::timeout(budget, reconcile).await;
                    }
                    None => reconcile.await,
                }
            };
            if before_shutdown(&shutdown, &dropped, reconcile).await.is_none() {
                return Ok(());
            }
        }
        let mut cron_holder_warned = HashSet::new();
        if inner.burst && !inner.crons.is_empty() {
            match before_shutdown(
                &shutdown,
                &dropped,
                schedule_burst_crons(&inner, &mut cron_holder_warned, &mut cron_state),
            )
            .await
            {
                Some(Ok(())) => {}
                // A burst is a cron or CI invocation that exits on this result,
                // and nothing here has written a lease yet, so the failure is
                // the whole answer: a due occurrence that could not be
                // published must not become an exit code of zero.
                Some(Err(error)) => return Err(error),
                None => return Ok(()),
            }
        }
        // Held to a heartbeat's deadline, like every renewal after it: one that fails is retried at once, by the
        // heartbeat loop's first tick, and a write stalled on a pooled connection that will never answer — which the
        // pool hands it untested (see `Database::write_worker_lease_within`) — only held up everything after it.
        let interval = inner.timers.worker_info;
        let startup = write_worker_info_within(&inner, worker_info_ttl(interval), heartbeat_deadline(interval));
        if before_shutdown(&shutdown, &dropped, startup).await.is_none() {
            // The one startup step where "cancelled" does not mean "did not
            // happen": the future is client-side but the INSERT is server-side,
            // so losing the race mid-statement still leaves a committed, live,
            // accepting lease behind. Retire it rather than advertising a
            // worker that is already gone for a full TTL.
            retire_startup_lease(&inner).await;
            return Ok(());
        }

        // The lease is durable from here on. `WorkerShutdown` retires it during
        // an ordinary shutdown, but it does not exist yet, so every early
        // return below has to retire it or this worker keeps advertising itself
        // as live and accepting until the lease TTL expires.
        let listener = inner.database.notify_listener();
        let wakeup = listener.subscribe_wakeup();
        let notification_health = listener.subscribe_health();
        let stop_intake = CancellationToken::new();
        let cooperative_shutdown = CancellationToken::new();
        let force_shutdown = CancellationToken::new();
        let intake = Arc::new(WorkerIntake::new(inner.burst));
        let (fetcher_exit_tx, mut fetcher_exit) = tokio::sync::oneshot::channel();
        if shutdown.is_cancelled() || dropped.is_cancelled() {
            retire_startup_lease(&inner).await;
            return Ok(());
        }
        let fetch_inner = inner.clone();
        let fetch_intake = intake.clone();
        let fetch_stop = stop_intake.clone();
        let mut fetcher = Some(tokio::spawn(async move {
            let outcome = fetch_loop(fetch_inner, fetch_intake, fetch_stop, wakeup).await;
            let _ = fetcher_exit_tx.send(outcome);
        }));
        let mut processors = JoinSet::new();
        for _ in 0..inner.concurrency {
            processors.spawn(processor_loop(
                inner.clone(),
                intake.clone(),
                stop_intake.clone(),
                cooperative_shutdown.clone(),
                force_shutdown.clone(),
            ));
        }

        let timer_token = CancellationToken::new();
        let mut timer_tasks = JoinSet::new();
        let timer_inner = inner.clone();
        let notification_token = timer_token.clone();
        timer_tasks.spawn(async move {
            notification_health_loop(timer_inner, notification_token, notification_health).await;
            "notification health loop"
        });
        let timer_inner = inner.clone();
        let abort_token = timer_token.clone();
        timer_tasks.spawn(async move {
            abort_loop(timer_inner, abort_token).await;
            "abort loop"
        });
        let timer_inner = inner.clone();
        let sweep_token = timer_token.clone();
        timer_tasks.spawn(async move {
            sweep_loop(timer_inner, sweep_token).await;
            "sweep loop"
        });
        let timer_inner = inner.clone();
        let worker_info_token = timer_token.clone();
        timer_tasks.spawn(async move {
            worker_info_loop(timer_inner, worker_info_token).await;
            "worker info loop"
        });
        if !inner.burst && !inner.crons.is_empty() {
            let timer_inner = inner.clone();
            let schedule_token = timer_token.clone();
            timer_tasks.spawn(async move {
                schedule_loop(timer_inner, schedule_token, cron_holder_warned, cron_state).await;
                "schedule loop"
            });
        }
        inner.health.ready();

        #[cfg(feature = "dashboard")]
        let mut dashboard = bound_dashboard.map(DashboardRuntime::start);

        // Wait for a shutdown request, (burst) for every processor to drain,
        // or for a configured dashboard server to fail.
        let mut fetcher_stopped = false;
        // Scoped so the dashboard borrow ends before shutdown stops the server.
        let mut run_error = {
            #[cfg(feature = "dashboard")]
            let dashboard_exit = wait_for_dashboard_exit(&mut dashboard);
            #[cfg(not(feature = "dashboard"))]
            let dashboard_exit = std::future::pending::<Error>();
            tokio::pin!(dashboard_exit);

            tokio::select! {
            _ = wait_for_shutdown_or_drop(&shutdown, &dropped) => {
                tracing::info!(worker.id = %inner.id, "shutdown requested");
                None
            }
            result = wait_for_processors(&mut processors, inner.burst) => {
                match result {
                    Ok(()) => {
                        tracing::info!(worker.id = %inner.id, "burst complete: queue drained");
                        None
                    }
                    Err(error) => Some(error),
                }
            }
            outcome = &mut fetcher_exit => {
                fetcher_stopped = true;
                // The fetcher only ever names a reason for a burst it ended
                // because it could not reach the database or maintain its
                // lease; every other exit leaves this `None` and the join
                // below supplies the error.
                outcome.ok().flatten()
            }
            error = wait_for_background_exit(&mut timer_tasks) => {
                Some(error)
            }
                error = &mut dashboard_exit => {
                    tracing::error!(worker.id = %inner.id, %error, "dashboard server failed");
                    Some(error)
                }
            }
        };

        if fetcher_stopped {
            // `fetcher` is `Some` from its spawn above and this is the only
            // `take` before `WorkerShutdown` is built, so the handle is always
            // here to join.
            let joined = match fetcher.take() {
                Some(fetcher) => unexpected_task_exit("fetch loop", fetcher.await),
                None => unreachable!("the fetch loop handle is taken exactly once"),
            };
            // A fetcher that reported *why* it stopped keeps that reason:
            // `unexpected_task_exit` can only say the loop is gone, and "the
            // dequeue failed for a whole `dequeue_timeout`" is the answer a
            // burst caller needs.
            let error = run_error.unwrap_or(joined);
            tracing::error!(worker.id = %inner.id, %error, "worker infrastructure failed");
            run_error = Some(error);
        } else if let Some(error) = run_error.as_ref() {
            tracing::error!(worker.id = %inner.id, %error, "worker infrastructure failed");
        }

        WorkerShutdown {
            intake,
            stop_intake,
            cooperative_shutdown,
            force_shutdown,
            timer_token,
            fetcher,
            processors,
            timer_tasks,
        }
        .run(&inner, &mut run_error)
        .await;

        #[cfg(feature = "dashboard")]
        if let Some(dashboard) = dashboard.as_mut()
            && let Err(error) = dashboard.finish_shutdown().await
        {
            run_error = run_error.or(Some(error));
        }

        tracing::info!(worker.id = %inner.id, "worker stopped");
        match run_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

impl std::fmt::Debug for Worker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Worker")
            .field("id", &self.inner.id)
            .field("queue", &self.inner.queue.name())
            .field("concurrency", &self.inner.concurrency)
            .finish_non_exhaustive()
    }
}

/// The tasks and cancellation tokens the shutdown sequence owns.
///
/// Kept apart from `run_until_inner` so the five shutdown phases — close
/// intake, drain processors, stop timers, retire the fetcher, and report —
/// can be read and changed without reasoning about startup's early returns.
struct WorkerShutdown {
    intake: Arc<WorkerIntake>,
    stop_intake: CancellationToken,
    cooperative_shutdown: CancellationToken,
    force_shutdown: CancellationToken,
    timer_token: CancellationToken,
    fetcher: Option<JoinHandle<()>>,
    processors: JoinSet<Result<(), Error>>,
    timer_tasks: JoinSet<&'static str>,
}

impl WorkerShutdown {
    async fn run(mut self, inner: &Arc<WorkerInner>, run_error: &mut Option<Error>) {
        // Graceful shutdown: stop taking work, signal cooperative cancellation,
        // then force-stop any attempts that outlive the grace period.
        let grace_deadline = tokio::time::Instant::now() + inner.shutdown_grace;
        self.close_intake(inner, grace_deadline).await;

        // A fetcher may be between a committed dequeue and returning its rows
        // to Rust, so keep it alive while processors still own attempts. Its
        // caretaker heartbeats the lease while it drains committed rows. Once
        // processors are done, the outer timeout gives that drain the hard
        // shutdown bound before aborting it and letting the lease expire.
        let release_fetcher_lease = CancellationToken::new();
        let fetcher_abort = self.fetcher.as_ref().map(JoinHandle::abort_handle);
        let fetcher_caretaker =
            tokio::spawn(finish_fetcher_shutdown(inner.clone(), self.fetcher.take(), release_fetcher_lease.clone()));

        self.drain_processors(inner, grace_deadline, run_error).await;
        self.stop_timers(inner, run_error).await;

        // No processor or timer can mutate a job after this point. The
        // caretaker expires the lease once its fetch/drain side is also done.
        release_fetcher_lease.cancel();
        retire_fetcher(inner, fetcher_caretaker, fetcher_abort, run_error).await;
    }

    /// Phase one: refuse new work locally and durably.
    async fn close_intake(&self, inner: &Arc<WorkerInner>, grace_deadline: tokio::time::Instant) {
        // Before the durable close, so a heartbeat that recreates a purged
        // lease during the drain recreates it closed rather than advertising
        // this worker as taking work again.
        inner.intake_open.store(false, Ordering::Release);
        self.intake.begin_shutdown();
        self.stop_intake.cancel();
        self.cooperative_shutdown.cancel();
        // The grace is the *handler drain* budget, so on its own it is the wrong
        // bound for this one statement: a zero grace — a valid configuration —
        // has already expired when it is constructed, so the durable close was
        // skipped before it was ever issued. A worker that requeues something
        // hid that (`requeue_shutdown` carries `close_intake`, and the final
        // write expires the lease anyway), but an *idle* worker takes neither
        // path and stopped with `accepting = true` on its lease. Harmless while
        // the expiry write lands — every reader of `accepting` also tests
        // `expires_at > now()` — and precisely not harmless when it does not,
        // which is the database blip the grace exists to survive: the worker
        // then advertises itself live and accepting for a whole lease TTL, and
        // the sweeper treats its abandoned attempts as a live owner's.
        //
        // So give it at least `SHUTDOWN_STEP_TIMEOUT`, which is what
        // `retire_startup_lease` already bounds the identical statement by. A
        // longer grace still wins, and `run_until` stays bounded either way.
        let close_deadline = grace_deadline.max(tokio::time::Instant::now() + SHUTDOWN_STEP_TIMEOUT);
        match tokio::time::timeout_at(close_deadline, inner.database.stop_worker_intake(inner.id)).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                tracing::error!(worker.id = %inner.id, %error, "failed to close worker intake");
            }
            Err(_) => {
                tracing::warn!(worker.id = %inner.id, "worker intake close exceeded its shutdown deadline");
            }
        }
    }

    /// Phase two: let attempts finish, then force-stop and finally abort them.
    async fn drain_processors(
        &mut self,
        inner: &Arc<WorkerInner>,
        grace_deadline: tokio::time::Instant,
        run_error: &mut Option<Error>,
    ) {
        if tokio::time::timeout_at(grace_deadline, join_all(&mut self.processors, run_error, false, Result::err))
            .await
            .is_ok()
        {
            return;
        }
        tracing::warn!(worker.id = %inner.id, "grace period expired; force-stopping in-flight jobs");
        self.force_shutdown.cancel();
        if tokio::time::timeout(HARD_SHUTDOWN_TIMEOUT, join_all(&mut self.processors, run_error, false, Result::err))
            .await
            .is_err()
        {
            self.processors.abort_all();
            join_all(&mut self.processors, run_error, true, Result::err).await;
            if run_error.is_none() {
                *run_error = Some(Error::WorkerTask("processor shutdown timed out"));
            }
        }
    }

    /// Phase three: stop the abort, sweep, schedule, and heartbeat loops.
    async fn stop_timers(&mut self, inner: &Arc<WorkerInner>, run_error: &mut Option<Error>) {
        self.timer_token.cancel();
        if tokio::time::timeout(SHUTDOWN_STEP_TIMEOUT, join_all(&mut self.timer_tasks, run_error, false, |_| None))
            .await
            .is_err()
        {
            tracing::warn!(worker.id = %inner.id, "timer task shutdown timed out");
            self.timer_tasks.abort_all();
            join_all(&mut self.timer_tasks, run_error, true, |_| None).await;
            if run_error.is_none() {
                *run_error = Some(Error::WorkerTask("timer shutdown timed out"));
            }
        }
    }
}

/// Phase four: wait for the fetcher caretaker to drain and release the lease,
/// making sure nothing is left detached that could still touch a job row.
async fn retire_fetcher(
    inner: &Arc<WorkerInner>,
    mut caretaker: JoinHandle<Result<(), Error>>,
    fetcher_abort: Option<tokio::task::AbortHandle>,
    run_error: &mut Option<Error>,
) {
    match tokio::time::timeout(HARD_SHUTDOWN_TIMEOUT, &mut caretaker).await {
        Ok(Ok(Ok(()))) => {}
        Ok(Ok(Err(error))) => {
            tracing::error!(worker.id = %inner.id, %error, "fetcher shutdown failed");
            *run_error = run_error.take().or(Some(error));
        }
        Ok(Err(error)) => {
            tracing::error!(worker.id = %inner.id, %error, "fetcher caretaker failed");
            *run_error = run_error.take().or(Some(Error::Task(error)));
        }
        Err(_) => {
            // Do not leave a detached fetcher or caretaker capable of
            // mutating jobs or refreshing the lease after return.
            if let Some(fetcher_abort) = fetcher_abort {
                fetcher_abort.abort();
            }
            caretaker.abort();
            let _ = caretaker.await;
            tracing::warn!(
                worker.id = %inner.id,
                "fetcher cleanup timed out; its worker lease will expire"
            );
            if run_error.is_none() {
                *run_error = Some(Error::WorkerTask("fetcher shutdown timed out"));
            }
        }
    }
}

/// Joins every task in `set`. `exit_error` extracts the error a task that ran to completion returned, if its output
/// carries one.
async fn join_all<T: 'static>(
    set: &mut JoinSet<T>,
    run_error: &mut Option<Error>,
    ignore_cancellation: bool,
    exit_error: fn(T) -> Option<Error>,
) {
    while let Some(result) = set.join_next().await {
        let error = match result {
            Ok(output) => match exit_error(output) {
                Some(error) => error,
                None => continue,
            },
            Err(error) if ignore_cancellation && error.is_cancelled() => continue,
            Err(error) => Error::Task(error),
        };
        tracing::error!(%error, "worker task failed during shutdown");
        if run_error.is_none() {
            *run_error = Some(error);
        }
    }
}

async fn wait_for_processors(set: &mut JoinSet<Result<(), Error>>, burst: bool) -> Result<(), Error> {
    while let Some(result) = set.join_next().await {
        result??;
        if !burst {
            return Err(Error::WorkerTask("processor loop"));
        }
    }
    Ok(())
}

async fn wait_for_background_exit(set: &mut JoinSet<&'static str>) -> Error {
    match set.join_next().await {
        Some(Ok(name)) => Error::WorkerTask(name),
        Some(Err(error)) => Error::Task(error),
        None => Error::WorkerTask("background loops"),
    }
}

fn unexpected_task_exit(name: &'static str, result: Result<(), tokio::task::JoinError>) -> Error {
    match result {
        Ok(()) => Error::WorkerTask(name),
        Err(error) => Error::Task(error),
    }
}

/// Resolves when a component is asked to stop, either by its caller's token or
/// by its owning handle being dropped.
pub(crate) async fn wait_for_shutdown_or_drop(shutdown: &CancellationToken, dropped: &CancellationToken) {
    tokio::select! {
        _ = shutdown.cancelled() => {}
        _ = dropped.cancelled() => {}
    }
}

/// Runs one startup step unless the worker is asked to stop first. `None` means
/// the step did not run to completion and the caller must unwind.
///
/// Startup is a sequence of these, so spelling the race out at each step would
/// repeat it once per step — and dropping either branch by mistake would leave
/// startup unresponsive to `run_until` cancellation or to a dropped handle,
/// which no compiler check catches.
async fn before_shutdown<T>(
    shutdown: &CancellationToken,
    dropped: &CancellationToken,
    step: impl Future<Output = T>,
) -> Option<T> {
    tokio::select! {
        biased;
        _ = wait_for_shutdown_or_drop(shutdown, dropped) => None,
        value = step => Some(value),
    }
}

/// Undoes the startup heartbeat when the worker stops before its fetcher — and
/// with it [`WorkerShutdown`] — exists. Without this, `Queue::workers_page`, the
/// dashboard worker page, and `has_live_workers` all report a live worker that
/// is already gone, and the dequeue path's `accepting` check still lets it
/// claim jobs, until its lease expires.
///
/// Both steps are bounded like every other shutdown database call: `run_until`
/// awaits this, so a wedged backend would otherwise hang it forever.
async fn retire_startup_lease(inner: &Arc<WorkerInner>) {
    inner.intake_open.store(false, Ordering::Release);
    match tokio::time::timeout(SHUTDOWN_STEP_TIMEOUT, inner.database.stop_worker_intake(inner.id)).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            tracing::error!(
                worker.id = %inner.id, %error,
                "failed to close worker intake while stopping during startup"
            );
        }
        Err(_) => {
            tracing::warn!(
                worker.id = %inner.id,
                "worker intake close timed out while stopping during startup"
            );
        }
    }
    if tokio::time::timeout(SHUTDOWN_STEP_TIMEOUT, write_worker_info(inner, Duration::ZERO)).await.is_err() {
        tracing::warn!(
            worker.id = %inner.id,
            "worker lease expiry timed out while stopping during startup"
        );
    }
}

pub(crate) async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        let mut sigterm = match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(signal) => signal,
            Err(error) => {
                tracing::error!(%error, "failed to install SIGTERM handler");
                let _ = tokio::signal::ctrl_c().await;
                return;
            }
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = sigterm.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

const UNHANDLED_JOB_WARNING_INTERVAL: Duration = Duration::from_secs(60);

/// Warns that this worker claimed a job it has no handler for, at most once
/// per name per [`UNHANDLED_JOB_WARNING_INTERVAL`]: a rolling deploy bounces
/// every claim of a new job type until this worker restarts, and one warning
/// per bounce would bury the log — while a single worker-wide cooldown let
/// one frequent unknown name consume the allowance and reduce every *other*
/// missing name to `debug`. Bounces inside a name's cooldown drop to `debug`.
/// The map is bounded by wholesale reset rather than eviction: past the cap
/// the worst case is one early repeat warning per interval, which is cheaper
/// than tracking recency.
fn warn_unhandled_bounce(inner: &WorkerInner, job: &JobRow) {
    const WARNED_NAMES_CAP: usize = 1024;
    inner.health.failed(WorkerComponent::Dispatch, &format!("no handler registered for claimed job {:?}", job.name));
    let mut warned_at = inner.unhandled_warned_at.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    if warned_at.len() >= WARNED_NAMES_CAP && !warned_at.contains_key(job.name.as_str()) {
        warned_at.clear();
    }
    let due = warned_at
        .get(job.name.as_str())
        .is_none_or(|last: &std::time::Instant| last.elapsed() >= UNHANDLED_JOB_WARNING_INTERVAL);
    if due {
        warned_at.insert(job.name.clone(), std::time::Instant::now());
        tracing::warn!(
            job.name = %job.name,
            "no handler registered for a claimed job; requeued with an attempt refund \
             (a worker must register every job name enqueued on its queue)"
        );
    } else {
        tracing::debug!(job.name = %job.name, "unhandled job requeued with an attempt refund");
    }
}

#[derive(Clone)]
struct WorkerInflightJob {
    cooperative: CancellationToken,
    force: CancellationToken,
    finished: CancellationToken,
    abort_reason: Arc<OnceLock<WorkerAbortReason>>,
}

#[derive(Clone)]
enum WorkerAbortReason {
    User(String),
    Swept,
    Missing,
    /// The row is no longer this attempt's: recovery requeued it, or it is
    /// running again one attempt further on.
    Superseded,
}

impl WorkerInflightJob {
    /// Asks an attempt to stop. A user abort gets `grace` to clean up
    /// cooperatively; sweeper recovery, a re-claimed row, and a deleted row do
    /// not, because the row that granted the attempt its dedupe exclusivity is
    /// already gone or has been handed to another attempt.
    ///
    /// The reason is recorded once, so the first one to arrive is the one the
    /// attempt is finished under. An immediate reason still has to force-stop
    /// the handler even when it lost that race, though: a user abort already
    /// under way leaves the attempt running for the whole `grace`, and if the
    /// row is deleted or handed to another attempt in that window — which
    /// `Database::abort_stuck_abandoned_batch` does to an `aborting` row whose
    /// failure retention (`failed_ttl_ms`) is `0` — nothing in the database
    /// guards its writes any more. Returning early there was the difference between the immediacy
    /// this and [`WorkerBuilder::abort_grace`] document and a handler that kept
    /// producing side effects to the end of the grace.
    fn request_abort(&self, reason: WorkerAbortReason, grace: Duration) {
        let immediate =
            matches!(reason, WorkerAbortReason::Swept | WorkerAbortReason::Missing | WorkerAbortReason::Superseded);
        if self.abort_reason.set(reason).is_err() && !immediate {
            return;
        }
        self.cooperative.cancel();
        if immediate || grace.is_zero() {
            self.force.cancel();
            return;
        }

        let force = self.force.clone();
        let finished = self.finished.clone();
        tokio::spawn(async move {
            tokio::select! {
                _ = finished.cancelled() => {}
                _ = tokio::time::sleep(grace) => force.cancel(),
            }
        });
    }
}

/// Acquires the in-flight map, recovering it if a panic poisoned the lock.
/// Everything done under this lock is a plain map operation — insert, remove,
/// key collection, and get-and-clone — so a panic cannot leave the map in a
/// broken state and the poisoned data is still valid. Skipping instead would
/// silently degrade aborts with no health signal: an attempt that fails to
/// register can never be aborted, a finished attempt that fails to deregister
/// leaves a stale claim the abort poll asks the database about forever, and a
/// poll that reads an empty snapshot skips the database entirely while
/// reporting the abort component healthy.
fn lock_inflight(
    inflight: &Mutex<HashMap<(Uuid, i32), WorkerInflightJob>>,
) -> std::sync::MutexGuard<'_, HashMap<(Uuid, i32), WorkerInflightJob>> {
    inflight.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Removes the in-flight entry even if processing unwinds.
struct WorkerInflightJobGuard<'a> {
    inflight: &'a Mutex<HashMap<(Uuid, i32), WorkerInflightJob>>,
    /// The `(id, attempts)` key this attempt registered under. A dequeue only
    /// ever hands out `attempts + 1`, so the key names this attempt alone and
    /// removing it can never take a later attempt's entry with it.
    key: (Uuid, i32),
    finished: CancellationToken,
}

impl Drop for WorkerInflightJobGuard<'_> {
    fn drop(&mut self) {
        self.finished.cancel();
        lock_inflight(self.inflight).remove(&self.key);
    }
}

#[cfg(test)]
mod worker_inflight_tests {
    use super::*;

    fn entry() -> WorkerInflightJob {
        WorkerInflightJob {
            cooperative: CancellationToken::new(),
            force: CancellationToken::new(),
            finished: CancellationToken::new(),
            abort_reason: Arc::new(OnceLock::new()),
        }
    }

    /// A panic under the inflight lock must not degrade abort handling: a new
    /// attempt must still register (an unregistered attempt can never be
    /// aborted), the abort poll must still see and look up registered
    /// attempts, and a finished attempt's guard must still deregister it (a
    /// stale entry is a claim every poll asks the database about forever) —
    /// all with no health signal that anything was lost.
    #[test]
    fn test_worker_inflight_registry_survives_lock_poisoning() {
        let inflight = Arc::new(Mutex::new(HashMap::new()));
        let poisoner = Arc::clone(&inflight);
        std::thread::spawn(move || {
            let _map = poisoner.lock().unwrap();
            panic!("poison the inflight lock");
        })
        .join()
        .unwrap_err();
        assert!(inflight.is_poisoned());
        // Registration (`process`'s insert) must still land.
        let key = (Uuid::now_v7(), 1);
        lock_inflight(&inflight).insert(key, entry());
        // The abort poll's claims snapshot and per-claim lookup must still
        // see the attempt.
        assert_eq!(lock_inflight(&inflight).keys().copied().collect::<Vec<_>>(), [key]);
        assert!(lock_inflight(&inflight).get(&key).cloned().is_some());
        // The finished attempt's guard must still deregister it.
        drop(WorkerInflightJobGuard { inflight: &inflight, key, finished: CancellationToken::new() });
        assert!(lock_inflight(&inflight).is_empty(), "a finished attempt must be deregistered even after poisoning");
    }
}

enum WorkerFetch {
    Job(Box<JobRow>),
    Stop,
    Drained,
}

enum WorkerAttemptResult {
    Success(Value),
    Errored(JobError),
    Cancelled,
    /// This worker registers no handler for the claimed job's name — a
    /// contract violation (a worker handles every job name in its queue),
    /// tolerated because a rolling deploy produces it transiently. The
    /// finalization gives the attempt back with a refund and a delay.
    Unhandled,
}

enum WorkerProcessResult {
    Complete,
    Retried(JobError),
    /// The row was requeued, but by somebody else — typically the sweeper,
    /// which recovered the attempt while this worker was still finalizing it
    /// and recorded the retry on its own counter. Reported like a retry,
    /// because that is what happened to the job; not counted as one here,
    /// because this worker did not make the transition.
    RetriedElsewhere(JobError),
    Failed(JobError),
    Aborted(JobError),
    /// The row was finished `aborted`, but by somebody else — the sweeper's
    /// recovery, which took the attempt while this worker was still running
    /// it and counted the abort on its own counter. Reported like an abort,
    /// because that is what happened to the job; not counted as one here, for
    /// the reason [`WorkerProcessResult::RetriedElsewhere`] is not.
    AbortedElsewhere(JobError),
    Requeued,
    /// Claimed with no handler registered; given back with a refund and a
    /// delay. Not a retry: the attempt never ran, and the counter must not
    /// say it did.
    Bounced,
    Unconfirmed,
}

/// One processing slot: fetch → process, until stopped (or drained in burst).
/// In-process handoff between the worker's single fetcher and its processor
/// slots: one batched dequeue per wakeup instead of a thundering herd of
/// per-slot `dequeue(1)` statements, each taking a pooled connection and a
/// round trip of its own to claim one row. The dequeue takes no advisory lock —
/// it is a single statement whose candidates are `FOR UPDATE ... SKIP LOCKED`,
/// so concurrent claims never block each other — which is exactly why the cost
/// being saved here is the per-claim round trip and connection, not lock
/// contention.
struct WorkerIntake {
    buffer: Mutex<VecDeque<JobRow>>,
    /// Wakes idle processors when the buffer is refilled; see [`WorkerIntake::end_fetch`] for which.
    refilled: tokio::sync::Notify,
    /// Wakes the fetcher when a processor goes idle (new demand).
    demand: tokio::sync::Notify,
    /// Processors currently waiting for work — the fetcher's batch size.
    idle: AtomicUsize,
    /// Monotonic demand and drain-proof generations. A burst processor can
    /// only drain after a valid underfilled fetch begun after its demand — one
    /// that either found nothing due, or spent the whole `dequeue_timeout`
    /// unable to claim what it did find.
    demand_generation: AtomicU64,
    drained_generation: AtomicU64,
    /// Whether a dequeue sized by the current demand is in flight, from the
    /// snapshot that sized it until its rows are in the buffer. Both ends are
    /// under the buffer lock, as is a processor's decision to drain, which
    /// waits this out.
    ///
    /// A drain proof covers the demand it was taken for, not a dequeue started
    /// after it. A burst processor whose idle timeout ran out while a newer
    /// dequeue — one that counted it — was still claiming used to drain on an
    /// older proof and leave, and the row that dequeue then claimed went into
    /// the buffer with no idle processor left to take it: `running` under this
    /// worker, outside the in-flight registry the abort loop polls, its handler
    /// not started, until a busy slot happened to free up or the sweeper, past
    /// the row's timeout, aborted it as stuck.
    fetching: AtomicBool,
    /// Set under the buffer lock before shutdown so no buffered row can race
    /// from fetcher cleanup into a processor.
    stopping: AtomicBool,
    /// Whether this is a burst worker's intake: the only kind whose idle
    /// processors ever conclude a drain, and so the only one with a reason to
    /// wake a processor that has no row to take.
    burst: bool,
}

impl WorkerIntake {
    fn new(burst: bool) -> Self {
        Self {
            buffer: Mutex::new(VecDeque::new()),
            refilled: tokio::sync::Notify::new(),
            demand: tokio::sync::Notify::new(),
            idle: AtomicUsize::new(0),
            demand_generation: AtomicU64::new(0),
            drained_generation: AtomicU64::new(0),
            fetching: AtomicBool::new(false),
            stopping: AtomicBool::new(false),
            burst,
        }
    }

    /// Acquires the intake buffer, recovering it if a panic poisoned the lock.
    /// Everything done under this lock is a single non-panicking deque or
    /// atomic operation, so a panic cannot leave the buffer in a broken state
    /// and the poisoned data is still valid. Skipping instead would silently
    /// wedge the worker: rows in the buffer are already claimed in the
    /// database, so a `claim` that stops handing them out strands them until
    /// lease-expiry recovery, and a fetcher that stops seeing demand never
    /// dequeues again — all while health keeps reporting `Ready`.
    fn lock_buffer(&self) -> std::sync::MutexGuard<'_, VecDeque<JobRow>> {
        self.buffer.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// An idle processor's look at the intake: a buffered job to claim, or —
    /// for a burst processor whose idle timeout has elapsed — a drain to
    /// conclude, or `None` to keep waiting. Either answer withdraws this
    /// processor's demand under the same buffer lock the fetcher sizes its
    /// dequeues under, giving it a coherent `(buffered, idle)` snapshot, and
    /// a drain is concluded only while no dequeue is in flight (see
    /// [`WorkerIntake::fetching`]): checked apart from that lock, the decision
    /// could interleave with the snapshot that counted this processor.
    fn claim_or_drain(&self, generation: u64, deadline_elapsed: bool) -> Option<WorkerFetch> {
        let mut buffer = self.lock_buffer();
        if self.stopping.load(Ordering::Acquire) {
            return None;
        }
        if let Some(job) = buffer.pop_front() {
            self.idle.fetch_sub(1, Ordering::AcqRel);
            return Some(WorkerFetch::Job(Box::new(job)));
        }
        if deadline_elapsed
            && !self.fetching.load(Ordering::Acquire)
            && self.drained_generation.load(Ordering::Acquire) >= generation
        {
            self.idle.fetch_sub(1, Ordering::AcqRel);
            return Some(WorkerFetch::Drained);
        }
        None
    }

    fn register_demand(&self) -> u64 {
        let _buffer = self.lock_buffer();
        let generation = self.demand_generation.fetch_add(1, Ordering::AcqRel) + 1;
        self.idle.fetch_add(1, Ordering::AcqRel);
        generation
    }

    /// The fetcher's demand snapshot: unmet demand (idle processors minus
    /// already-buffered rows) and the demand generation it was taken at,
    /// coherent under the buffer lock. Nonzero demand starts a fetch, which
    /// the fetcher ends with [`WorkerIntake::end_fetch`] whatever the dequeue
    /// it sized does.
    fn begin_fetch(&self) -> (usize, u64) {
        let buffer = self.lock_buffer();
        let want = self.idle.load(Ordering::Acquire).saturating_sub(buffer.len());
        if want > 0 {
            self.fetching.store(true, Ordering::Release);
        }
        (want, self.demand_generation.load(Ordering::Acquire))
    }

    /// Ends the fetch [`WorkerIntake::begin_fetch`] started: buffers what its
    /// dequeue claimed — nothing, for one that came back empty or failed — and
    /// clears the in-flight flag in the same critical section, so no processor
    /// can find the flag clear and the rows not yet there. Wakes idle
    /// processors to take the rows, and returns how many rows were buffered.
    ///
    /// A continuous worker wakes one idle processor per row, and none for a
    /// fetch that buffered nothing. Every idle processor registers with
    /// `refilled` before it looks at the buffer, so each `notify_one` reaches a
    /// processor that has yet to see these rows, or leaves a permit for the
    /// next one to register; a processor that takes a row some other way hands
    /// its unconsumed notification on as it drops it; and `next_job` re-checks
    /// the buffer every `poll_interval` regardless. Waking every idle processor
    /// on every dequeue instead cost each of them a wakeup, a turn at the
    /// buffer's lock and fresh timers, whatever the dequeue buffered — twice
    /// over for an underfilled one, whose drain proof woke them all again,
    /// although only a burst ever reads a proof — so CPU per job grew with
    /// concurrency: 200 no-op jobs a second took 3.4 cores at a concurrency of
    /// 1,000, against 0.09 at 10.
    ///
    /// A burst worker still wakes every idle processor: one whose idle timeout
    /// has run out may be waiting on nothing but this fetch ending (see
    /// [`WorkerIntake::fetching`]), with no row of its own to be woken for.
    fn end_fetch(&self, jobs: Vec<JobRow>) -> usize {
        let fetched = jobs.len();
        {
            let mut buffer = self.lock_buffer();
            buffer.extend(jobs);
            self.fetching.store(false, Ordering::Release);
        }
        if self.burst {
            self.refilled.notify_waiters();
        } else {
            for _ in 0..fetched {
                self.refilled.notify_one();
            }
        }
        fetched
    }

    /// Records that a fetch sized by the demand at `generation` proved it
    /// drained, and on a burst worker — the only kind whose processors ever act
    /// on a proof — lets every idle processor re-check.
    fn prove_drained(&self, generation: u64) {
        self.drained_generation.fetch_max(generation, Ordering::AcqRel);
        if self.burst {
            self.refilled.notify_waiters();
        }
    }

    fn withdraw_demand(&self) {
        let _buffer = self.lock_buffer();
        self.idle.fetch_sub(1, Ordering::AcqRel);
    }

    fn begin_shutdown(&self) {
        let _buffer = self.lock_buffer();
        self.stopping.store(true, Ordering::Release);
    }
}

#[cfg(test)]
mod worker_intake_tests {
    use crate::job::JobRetryBackoff;

    use super::*;

    fn buffered_job() -> JobRow {
        JobRow {
            id: Uuid::now_v7(),
            dedupe_key: None,
            queue: "default".to_string(),
            name: "buffered".to_string(),
            payload: Value::Null,
            status: JobStatus::Running,
            priority: 0,
            attempts: 1,
            refunds: 0,
            max_attempts: 1,
            timeout_ms: None,
            retry_delay_ms: 0,
            backoff: JobRetryBackoff::None,
            result_ttl_ms: None,
            failed_ttl_ms: None,
            scheduled_at: Timestamp::now(),
            enqueued_at: Timestamp::now(),
            started_at: None,
            touched_at: None,
            completed_at: None,
            expires_at: None,
            result: None,
            error: None,
            meta: Value::Null,
            worker_id: None,
            kind: "job".to_string(),
            cron_expr: None,
            retried_at: None,
        }
    }

    /// A panic under the intake lock must not wedge the worker. Rows in the
    /// buffer are already claimed in the database, so a `claim` that stops
    /// handing them out strands them until lease-expiry recovery, and a fetcher
    /// that stops seeing demand never dequeues again — all while the worker
    /// keeps running and reports itself healthy.
    #[test]
    fn test_worker_intake_keeps_moving_jobs_after_lock_poisoning() {
        let intake = Arc::new(WorkerIntake::new(false));
        // One processor goes idle; the fetcher buffers a row for it.
        let generation = intake.register_demand();
        intake.buffer.lock().unwrap().push_back(buffered_job());
        let poisoner = Arc::clone(&intake);
        std::thread::spawn(move || {
            let _buffer = poisoner.buffer.lock().unwrap();
            panic!("poison the intake lock");
        })
        .join()
        .unwrap_err();
        assert!(intake.buffer.is_poisoned());
        // The processor must still receive the buffered, database-claimed row,
        // and taking it must keep the fetcher's demand snapshot coherent.
        let job = intake.claim_or_drain(generation, false);
        assert!(matches!(job, Some(WorkerFetch::Job(_))), "a buffered, database-claimed job must remain claimable");
        assert_eq!(intake.begin_fetch(), (0, generation));
        // The fetcher must keep seeing new demand, not a permanent zero.
        let next_generation = intake.register_demand();
        assert_eq!(next_generation, generation + 1);
        assert_eq!(intake.begin_fetch(), (1, next_generation));
        // The fetcher must still buffer rows it dequeued, and shutdown must
        // still freeze intake so no buffered row can race from fetcher cleanup
        // into a processor.
        assert_eq!(intake.end_fetch(vec![buffered_job()]), 1);
        intake.begin_shutdown();
        assert!(
            intake.claim_or_drain(next_generation, true).is_none(),
            "intake must stop handing out jobs once shutdown began"
        );
        assert_eq!(intake.lock_buffer().len(), 1, "the frozen row stays buffered for the shutdown drain to requeue");
    }

    /// A burst processor's drain proof covers the demand it was taken for, not a dequeue that counted the processor
    /// after it. Concluded while that dequeue was in flight, the drain left its row in the buffer with no idle
    /// processor to take it; it has to wait for the dequeue to land and take the row, and may drain only once one
    /// comes back empty.
    #[test]
    fn test_a_burst_processor_does_not_drain_while_a_fetch_that_counted_it_is_in_flight() {
        let intake = WorkerIntake::new(true);
        let generation = intake.register_demand();
        // An earlier fetch found nothing due, so only the idle timeout stood between this processor and a drain.
        intake.drained_generation.fetch_max(generation, Ordering::AcqRel);

        // A new job is due and the fetcher's next dequeue counts this processor.
        assert_eq!(intake.begin_fetch(), (1, generation));
        assert!(
            intake.claim_or_drain(generation, true).is_none(),
            "a processor counted by a dequeue still in flight must wait for it rather than drain"
        );
        assert_eq!(intake.end_fetch(vec![buffered_job()]), 1);
        assert!(matches!(intake.claim_or_drain(generation, true), Some(WorkerFetch::Job(_))));
        assert_eq!(intake.idle.load(Ordering::Acquire), 0);

        // Idle again; a dequeue sized by the new demand comes back empty this time, which settles it.
        let generation = intake.register_demand();
        intake.drained_generation.fetch_max(generation, Ordering::AcqRel);
        assert_eq!(intake.begin_fetch(), (1, generation));
        assert!(intake.claim_or_drain(generation, true).is_none());
        assert_eq!(intake.end_fetch(Vec::new()), 0);
        assert!(matches!(intake.claim_or_drain(generation, true), Some(WorkerFetch::Drained)));
        assert_eq!(intake.idle.load(Ordering::Acquire), 0, "a drained processor withdraws its demand");
        // Without its idle timeout elapsed, nothing ever drains.
        let generation = intake.register_demand();
        intake.drained_generation.fetch_max(generation, Ordering::AcqRel);
        assert!(intake.claim_or_drain(generation, false).is_none());
    }

    type Waiter<'a> = std::pin::Pin<Box<tokio::sync::futures::Notified<'a>>>;

    /// `count` idle processors waiting for a refill, each registered with the intake's `Notify` as `next_job` registers
    /// one before it looks at the buffer.
    fn idle_processors(intake: &WorkerIntake, count: usize) -> Vec<Waiter<'_>> {
        let mut waiters: Vec<Waiter<'_>> = (0..count).map(|_| Box::pin(intake.refilled.notified())).collect();
        for waiter in &mut waiters {
            waiter.as_mut().enable();
        }
        waiters
    }

    /// Polls each waiter once, counting the ones a notification has woken.
    fn woken(waiters: &mut [Waiter<'_>]) -> usize {
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        waiters.iter_mut().map(|waiter| waiter.as_mut().poll(&mut context)).filter(|poll| poll.is_ready()).count()
    }

    /// A continuous worker's fetch wakes one idle processor per row it buffered, and none for a fetch that buffered
    /// nothing. Every fetch used to wake every idle processor, so each dequeue cost each of them a wakeup, a turn at
    /// the buffer's lock and a fresh timer — twice over for an underfilled batch, whose drain proof woke them all again
    /// although only a burst ever reads one — and a worker's CPU per job grew with its concurrency: 200 no-op jobs a
    /// second cost 3.4 cores at a concurrency of 1,000.
    #[test]
    fn test_a_continuous_fetch_wakes_one_idle_processor_per_row() {
        let intake = WorkerIntake::new(false);
        let mut waiters = idle_processors(&intake, 64);
        assert_eq!(intake.end_fetch(vec![buffered_job()]), 1);
        assert_eq!(woken(&mut waiters), 1, "one buffered row is work for one idle processor");
        drop(waiters);

        let mut waiters = idle_processors(&intake, 64);
        assert_eq!(intake.end_fetch(Vec::new()), 0);
        intake.prove_drained(intake.register_demand());
        assert_eq!(
            woken(&mut waiters),
            0,
            "an empty fetch has nothing for an idle processor to take, and no continuous one acts on a drain proof"
        );
        drop(waiters);

        let mut waiters = idle_processors(&intake, 64);
        assert_eq!(intake.end_fetch(vec![buffered_job(), buffered_job(), buffered_job()]), 3);
        assert_eq!(woken(&mut waiters), 3);
    }

    /// A burst processor whose idle timeout has run out may be waiting on nothing but the fetch that counted it — no
    /// row of its own to be woken for — so on a burst worker every fetch's end, and every drain proof, wakes every idle
    /// processor to re-check.
    #[test]
    fn test_a_burst_fetch_wakes_every_idle_processor_to_recheck_its_drain() {
        let intake = WorkerIntake::new(true);
        let generation = intake.register_demand();
        // An earlier fetch found nothing due; a newer one, which counted this processor, is still in flight.
        intake.prove_drained(generation);
        assert_eq!(intake.begin_fetch(), (1, generation));
        let mut waiters = idle_processors(&intake, 8);
        assert!(intake.claim_or_drain(generation, true).is_none(), "the fetch in flight must be waited out");
        assert_eq!(intake.end_fetch(Vec::new()), 0);
        assert_eq!(woken(&mut waiters), 8, "the fetch's end is what a processor waiting to drain waits for");
        assert!(matches!(intake.claim_or_drain(generation, true), Some(WorkerFetch::Drained)));
        drop(waiters);

        let mut waiters = idle_processors(&intake, 8);
        intake.prove_drained(intake.register_demand());
        assert_eq!(woken(&mut waiters), 8, "and so is a drain proof");
    }
}

/// The worker's single dequeuer: fetches `idle`-sized batches on wakeup hints
/// (with an interval fallback — notifications can be lost across listener
/// reconnects) and hands jobs to processors through the intake buffer.
///
/// Returns the error that ended the loop, if one did. Only a burst worker that
/// spent its whole [`WorkerBuilder::dequeue_timeout`] unable to reach the
/// database produces one; every other exit is a shutdown, which is not a
/// failure. Whatever comes back becomes [`Worker::run_until`]'s result, because
/// "the database was unreachable" and "the queue was empty" must not be the same
/// answer to a burst.
async fn fetch_loop(
    inner: Arc<WorkerInner>,
    intake: Arc<WorkerIntake>,
    stop: CancellationToken,
    mut wakeup: broadcast::Receiver<()>,
) -> Option<Error> {
    let mut retry_max_ms = DEQUEUE_RETRY_INITIAL_MAX_MS;
    // Burst only: how long due work this worker can see but cannot claim may
    // keep its demand outstanding before the fetcher reports the drain anyway,
    // and — below — how long dequeues may keep failing outright before the
    // burst gives up. See the `work_available`-but-empty and `Err` arms below.
    // `build` refuses `burst` without a `dequeue_timeout`, so in burst mode this
    // is always `Some`.
    let unclaimable_budget = inner.burst.then_some(inner.dequeue_timeout).flatten();
    // When the current run of `work_available`-but-empty fetches began. Reset
    // wherever `retry_max_ms` is, plus after a failed dequeue: both track "the
    // last time this loop learned something definitive", and a fetch that never
    // reached the database has learned nothing about who holds what.
    let mut unclaimable_since: Option<tokio::time::Instant> = None;
    // When the current run of failed dequeues began, reset by every fetch that
    // reached the database at all.
    let mut failing_since: Option<tokio::time::Instant> = None;
    // When the current run of intake-closed dequeues began *while the lease
    // could not be written*, reset by every fetch that proved the lease open
    // and whenever the heartbeat lands again. Burst only, like the two budgets
    // above: a lease this worker cannot maintain means nothing is ever
    // claimable, and the intake-closed arm cannot conclude a drain, so without
    // a bound `run_until` never returned.
    let mut intake_closed_since: Option<tokio::time::Instant> = None;
    loop {
        // Fill demand: batch size = processors currently waiting.
        loop {
            if stop.is_cancelled() {
                drain_on_shutdown(&inner, &intake).await;
                return None;
            }
            let (want, demand_generation) = intake.begin_fetch();
            if want == 0 {
                break;
            }
            let mut dequeue = with_db_deadline(inner.database.dequeue_worker(want as i64, inner.id)).await;
            // Ended before anything below can await or return, whatever the dequeue did: the claimed rows enter
            // shared state at once, which keeps task cancellation lossless, and a processor waiting on this fetch
            // to decide whether it may drain is released.
            let fetched =
                intake.end_fetch(dequeue.as_mut().map(|batch| std::mem::take(&mut batch.jobs)).unwrap_or_default());
            if dequeue.is_ok() {
                inner.health.recovered(WorkerComponent::Dequeue);
                failing_since = None;
            }
            match dequeue {
                Ok(result) if fetched == 0 && result.intake_open && !result.work_available => {
                    retry_max_ms = DEQUEUE_RETRY_INITIAL_MAX_MS;
                    unclaimable_since = None;
                    intake_closed_since = None;
                    intake.prove_drained(demand_generation);
                    break;
                }
                Ok(result) if fetched == 0 && result.intake_open => {
                    // `SKIP LOCKED` can produce an empty batch while a matching
                    // ready row is being inspected or updated elsewhere —
                    // usually another claim in flight, gone again within a round
                    // trip. Keep burst demand outstanding while that is still
                    // plausible, so a later fetch makes the drain decision on
                    // what it finds rather than on this one empty batch.
                    //
                    // Once the backoff has saturated, hand back to the outer
                    // `select!` rather than retrying at `DEQUEUE_RETRY_MAX_MS`
                    // forever. The claim uses `SKIP LOCKED` and the availability
                    // probe does not, so one `queued`, due row
                    // held under a row lock by an unrelated open transaction
                    // reports work that no claim can ever take — and that pinned
                    // every idle worker in the fleet in this loop at ~22x its
                    // configured `poll_interval`, for as long as the lock was
                    // held. `retry_max_ms` is deliberately left saturated so
                    // later passes re-check once per `poll_interval` instead of
                    // climbing the ramp again; it is reset by every arm that
                    // learns something definitive. A processor going idle
                    // notifies `demand`, so nothing waits out a poll interval
                    // that had work to hand it.
                    //
                    // The lock holder need not be another claim, though: an
                    // operator session left idle in a transaction over a
                    // `SELECT ... FOR UPDATE`, or an uncommitted `UPDATE`, holds
                    // one indefinitely. Leaving `drained_generation` untouched
                    // for as long as that lasts left no burst processor able to
                    // conclude a drain, so `run_until` never returned and
                    // `max_burst_jobs` did not bound it — budget is only spent
                    // per job actually processed. Past `unclaimable_budget` the
                    // fetch therefore counts as underfilled like any other:
                    // burst gives the row up and leaves it queued for a later
                    // run. A lock released inside that window is still picked up
                    // by one of the retries above, and a processor needs its own
                    // idle `dequeue_timeout` on top before it acts on this, so
                    // nothing here shortens the patience that was configured.
                    intake_closed_since = None;
                    let since = *unclaimable_since.get_or_insert_with(tokio::time::Instant::now);
                    if unclaimable_budget.is_some_and(|budget| since.elapsed() >= budget) {
                        intake.prove_drained(demand_generation);
                        break;
                    }
                    if retry_max_ms >= DEQUEUE_RETRY_MAX_MS {
                        break;
                    }
                    if !wait_for_dequeue_retry(&stop, retry_max_ms).await {
                        drain_on_shutdown(&inner, &intake).await;
                        return None;
                    }
                    retry_max_ms = (retry_max_ms * 2).min(DEQUEUE_RETRY_MAX_MS);
                }
                Ok(_) if fetched == 0 => {
                    retry_max_ms = DEQUEUE_RETRY_INITIAL_MAX_MS;
                    unclaimable_since = None;
                    tracing::debug!(
                        worker.id = %inner.id,
                        "dequeue skipped while the worker intake lease is closed or expired"
                    );
                    // A closed lease has three causes with three answers. Mid-
                    // shutdown, the stop token — cancelled before the durable
                    // close — ends this loop. Closed *under* a worker whose
                    // heartbeats still land — an operator or test managing the
                    // lease row directly — it is a coordination state a burst
                    // waits out indefinitely, because whoever closed intake can
                    // reopen it. But a lease this worker *cannot write at all* —
                    // the startup write failed and every heartbeat since kept
                    // failing, with dequeue reads healthy — can never reopen by
                    // waiting, and this arm can never conclude a drain either:
                    // nothing is claimable without a live accepting lease, so
                    // `run_until` simply never returned. That case is exactly a
                    // failing `WorkerComponent::WorkerInfo`, and past the same
                    // budget the other burst stalls get, the run ends as the
                    // failure it is; the heartbeat's own error is on health and
                    // in the log.
                    if inner.health.is_failing(WorkerComponent::WorkerInfo) {
                        let since = *intake_closed_since.get_or_insert_with(tokio::time::Instant::now);
                        if unclaimable_budget.is_some_and(|budget| since.elapsed() >= budget) {
                            drain_on_shutdown(&inner, &intake).await;
                            return Some(Error::WorkerTask(
                                "worker lease could not be maintained for the whole dequeue timeout",
                            ));
                        }
                    } else {
                        intake_closed_since = None;
                    }
                    if !sleep_unless_stopped(&stop, Duration::from_millis(100)).await {
                        drain_on_shutdown(&inner, &intake).await;
                        return None;
                    }
                    break;
                }
                Ok(result) => {
                    retry_max_ms = DEQUEUE_RETRY_INITIAL_MAX_MS;
                    unclaimable_since = None;
                    intake_closed_since = None;
                    // A dequeue in flight when shutdown began can still return
                    // after intake was frozen. `end_fetch` has buffered its rows
                    // already, so the shutdown drain requeues them.
                    if stop.is_cancelled() {
                        drain_on_shutdown(&inner, &intake).await;
                        return None;
                    }
                    if fetched < want && !result.work_available {
                        intake.prove_drained(demand_generation);
                        break;
                    }
                }
                Err(error) => {
                    unclaimable_since = None;
                    inner.health.failed(WorkerComponent::Dequeue, &error);
                    tracing::error!(worker.id = %inner.id, %error, "dequeue failed");
                    // A fetch that never reached the database leaves burst
                    // demand outstanding, exactly as an unclaimable one does —
                    // and for as long as the failures last, no processor can
                    // reach `WorkerFetch::Drained`, so `run_until` never
                    // returned. `max_burst_jobs` did not bound that either:
                    // budget is only spent per job actually processed. So the
                    // run of failures gets the same `unclaimable_budget`, and
                    // ends the burst by *failing* it rather than by reporting a
                    // drain — a queue nobody could read is not an empty one, and
                    // a cron run that exits zero on an unreachable database is
                    // the silent failure this whole path exists to avoid.
                    let since = *failing_since.get_or_insert_with(tokio::time::Instant::now);
                    if unclaimable_budget.is_some_and(|budget| since.elapsed() >= budget) {
                        drain_on_shutdown(&inner, &intake).await;
                        return Some(error);
                    }
                    if !sleep_unless_stopped(&stop, Duration::from_secs(1)).await {
                        drain_on_shutdown(&inner, &intake).await;
                        return None;
                    }
                    break;
                }
            }
        }
        tokio::select! {
            _ = stop.cancelled() => {
                drain_on_shutdown(&inner, &intake).await;
                return None;
            }
            _ = wakeup.recv() => {}
            _ = intake.demand.notified() => {}
            _ = tokio::time::sleep(inner.poll_interval) => {}
        }
    }
}

async fn wait_for_dequeue_retry(stop: &CancellationToken, max_ms: u64) -> bool {
    let delay_ms = 1 + u64::from(rand::random::<u8>()) % max_ms;
    sleep_unless_stopped(stop, Duration::from_millis(delay_ms)).await
}

/// Sleeps for `duration`, returning `false` early if `stop` is cancelled
/// first, so the fetch loop's backoffs never delay shutdown.
async fn sleep_unless_stopped(stop: &CancellationToken, duration: Duration) -> bool {
    tokio::select! {
        _ = stop.cancelled() => false,
        _ = tokio::time::sleep(duration) => true,
    }
}

/// Keeps an intake-stopped fetcher's lease alive until it has drained every
/// committed row, then expires the lease once processor shutdown permits it.
async fn finish_fetcher_shutdown(
    inner: Arc<WorkerInner>,
    fetcher: Option<JoinHandle<()>>,
    release_lease: CancellationToken,
) -> Result<(), Error> {
    let mut heartbeat = tokio::time::interval(inner.timers.worker_info);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut fetch_error = None;
    if let Some(mut fetcher) = fetcher {
        loop {
            tokio::select! {
                biased;
                result = &mut fetcher => {
                    if let Err(error) = result {
                        fetch_error = Some(Error::Task(error));
                    }
                    break;
                }
                _ = heartbeat.tick() => refresh_fetcher_lease(&inner).await,
            }
        }
    }
    loop {
        tokio::select! {
            biased;
            _ = release_lease.cancelled() => break,
            _ = heartbeat.tick() => refresh_fetcher_lease(&inner).await,
        }
    }
    write_worker_info(&inner, Duration::ZERO).await;
    match fetch_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

async fn refresh_fetcher_lease(inner: &Arc<WorkerInner>) {
    let ttl = worker_info_ttl(inner.timers.worker_info);
    // The write is held to the step's own bound, so a refresh abandoned behind a lock on the lease row leaves no wait
    // on the server to keep its connection while the drain goes on refreshing.
    if tokio::time::timeout(SHUTDOWN_STEP_TIMEOUT, write_worker_info_within(inner, ttl, SHUTDOWN_STEP_TIMEOUT))
        .await
        .is_err()
    {
        tracing::warn!(worker.id = %inner.id, "fetcher lease heartbeat timed out");
    }
}

/// Requeues buffered-but-unclaimed jobs when the worker stops taking work.
async fn drain_on_shutdown(inner: &Arc<WorkerInner>, intake: &WorkerIntake) {
    loop {
        // Take the row rather than cloning it: these carry full payloads, and
        // shutdown is the worst moment to allocate a copy per iteration.
        let Some(job) = intake.lock_buffer().pop_front() else {
            return;
        };
        let settled = match inner.database.requeue_shutdown(&job, "cancelled", None).await {
            Ok(true) => true,
            Ok(false) => match inner.database.finish(&job, JobStatus::Aborted, None, None, None).await {
                Ok(true) => {
                    inner.counters.record_abort();
                    true
                }
                Ok(false) => true,
                Err(error) => {
                    tracing::error!(job.id = %job.id, %error, "failed to finalize aborted buffered job during shutdown");
                    false
                }
            },
            Err(error) => {
                tracing::error!(job.id = %job.id, %error, "failed to requeue buffered job during shutdown");
                false
            }
        };
        if !settled {
            intake.lock_buffer().push_front(job);
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

async fn processor_loop(
    inner: Arc<WorkerInner>,
    intake: Arc<WorkerIntake>,
    stop: CancellationToken,
    cooperative_shutdown: CancellationToken,
    force_shutdown: CancellationToken,
) -> Result<(), Error> {
    loop {
        // Burst cap: reserve budget BEFORE fetching so `concurrency`
        // processors can't all slip past the check together.
        if inner.burst_budget.as_ref().is_some_and(|budget| {
            budget.try_update(Ordering::AcqRel, Ordering::Acquire, |remaining| remaining.checked_sub(1)).is_err()
        }) {
            return Ok(());
        }
        match next_job(&inner, &intake, &stop).await {
            WorkerFetch::Job(job) => process(&inner, *job, &cooperative_shutdown, &force_shutdown).await?,
            WorkerFetch::Stop => return Ok(()),
            WorkerFetch::Drained => {
                tracing::debug!(worker.id = %inner.id, "processor drained");
                return Ok(());
            }
        }
    }
}

/// Waits for a job from the intake buffer (the fetcher does all DB work).
async fn next_job(inner: &Arc<WorkerInner>, intake: &WorkerIntake, stop: &CancellationToken) -> WorkerFetch {
    let deadline =
        inner.burst.then(|| inner.dequeue_timeout).flatten().and_then(|t| tokio::time::Instant::now().checked_add(t));

    // Register demand once for the whole idle period (the counter is the
    // fetcher's batch size), pinging the fetcher so a refill between the
    // buffer check and the wait can't be missed.
    let demand_generation = intake.register_demand();
    intake.demand.notify_one();
    let mut deadline_elapsed = false;
    let refill = intake.refilled.notified();
    // Made once for the whole idle period, not once per wakeup: the token's
    // waiter, the in-memory re-check fallback (the fetcher owns all DB
    // polling), and a burst's idle timeout.
    let stopped = stop.cancelled();
    let recheck = tokio::time::sleep(inner.poll_interval);
    let idle_timeout = tokio::time::sleep_until(deadline.unwrap_or_else(tokio::time::Instant::now));
    tokio::pin!(refill, stopped, recheck, idle_timeout);
    let result = loop {
        // Register with Notify before inspecting the buffer. `end_fetch`
        // wakes only processors already registered — beyond the one permit a
        // continuous worker's `notify_one` keeps when none is — so registering
        // inside the select, after `claim`, would leave a lost-wakeup window.
        refill.as_mut().enable();
        if stop.is_cancelled() {
            break WorkerFetch::Stop;
        }
        if let Some(fetch) = intake.claim_or_drain(demand_generation, deadline_elapsed) {
            break fetch;
        }
        tokio::select! {
            _ = &mut stopped => break WorkerFetch::Stop,
            _ = &mut refill => refill.set(intake.refilled.notified()),
            _ = &mut recheck => recheck.as_mut().reset(tokio::time::Instant::now() + inner.poll_interval),
            _ = &mut idle_timeout, if deadline.is_some() && !deadline_elapsed => deadline_elapsed = true,
        }
    };
    if !matches!(result, WorkerFetch::Job(_)) {
        // `claim_or_drain` withdrew a drained processor's demand already, under the lock it decided under.
        if matches!(result, WorkerFetch::Stop) {
            intake.withdraw_demand();
        }
        // A processor that exits without taking a job returns its burst budget.
        if let Some(budget) = &inner.burst_budget {
            budget.fetch_add(1, Ordering::AcqRel);
        }
    }
    result
}

/// Runs one dequeued job through its handler and finalization. An error is a
/// burst's outcome that could not be recorded (see [`finalize`]), which ends
/// the run.
async fn process(
    inner: &Arc<WorkerInner>,
    job: JobRow,
    cooperative_shutdown: &CancellationToken,
    force_shutdown: &CancellationToken,
) -> Result<(), Error> {
    let cooperative = cooperative_shutdown.child_token();
    let force = force_shutdown.child_token();
    let finished = CancellationToken::new();
    let abort_reason = Arc::new(OnceLock::new());
    let key = (job.id, job.attempts);
    lock_inflight(&inner.inflight).insert(
        key,
        WorkerInflightJob {
            cooperative: cooperative.clone(),
            force: force.clone(),
            finished: finished.clone(),
            abort_reason: abort_reason.clone(),
        },
    );
    let _guard = WorkerInflightJobGuard { inflight: &inner.inflight, key, finished };

    // The context owns the dequeue snapshot outright — the row is moved in,
    // never cloned — and the handler decodes its payload from that snapshot by
    // reference, so an attempt costs zero payload copies however large the
    // document is. Finalization borrows the same snapshot back out.
    let ctx = JobContext::new(inner.queue.clone(), job, inner.id, inner.state.clone(), cooperative);
    let job = ctx.job();
    // `attempt` is what the handler reads from `JobContext::attempt`, so its
    // lines and the worker's agree under this span: a claim refunded without
    // running — a shutdown requeue, a bounce — is not an attempt. `claim` is
    // the raw counter: what every guard fences this attempt on, half of its
    // in-flight `key`, and what the abort loop's warnings name it by.
    let span = tracing::info_span!(
        "job.run",
        job.name = %job.name,
        job.id = %job.id,
        attempt = ctx.attempt(),
        claim = job.attempts,
        queue = %inner.queue.name(),
    );

    async {
        let end = run_attempt(inner, job, &ctx, &force).await;
        let result = finalize(inner, job, end, &abort_reason, force_shutdown, cooperative_shutdown).await?;
        match &result {
            WorkerProcessResult::Complete => inner.counters.record_complete(),
            WorkerProcessResult::Retried(_) | WorkerProcessResult::Requeued => inner.counters.record_retry(),
            WorkerProcessResult::Failed(_) => inner.counters.record_failed(),
            WorkerProcessResult::Aborted(_) => inner.counters.record_abort(),
            WorkerProcessResult::Bounced
            | WorkerProcessResult::RetriedElsewhere(_)
            | WorkerProcessResult::AbortedElsewhere(_)
            | WorkerProcessResult::Unconfirmed => {}
        }
        match &result {
            WorkerProcessResult::Complete => tracing::info!("job complete"),
            WorkerProcessResult::Retried(e) => {
                tracing::warn!(error = %e, "job attempt failed; retrying")
            }
            WorkerProcessResult::RetriedElsewhere(e) => {
                tracing::warn!(error = %e, "job attempt failed; already requeued elsewhere")
            }
            WorkerProcessResult::Failed(e) => tracing::error!(error = %e, "job failed"),
            WorkerProcessResult::Aborted(e) => tracing::warn!(error = %e, "job aborted"),
            WorkerProcessResult::AbortedElsewhere(e) => tracing::warn!(error = %e, "job aborted elsewhere"),
            WorkerProcessResult::Requeued => tracing::info!("job requeued for shutdown"),
            WorkerProcessResult::Bounced => warn_unhandled_bounce(inner, job),
            WorkerProcessResult::Unconfirmed => {
                tracing::warn!("job result was not confirmed by the database")
            }
        }
        Ok(())
    }
    .instrument(span)
    .await
}

/// Executes the handler in an owned task for panic containment, under the
/// job's timeout and force-stop token.
async fn run_attempt(
    inner: &Arc<WorkerInner>,
    job: &JobRow,
    ctx: &JobContext,
    force: &CancellationToken,
) -> WorkerAttemptResult {
    let Some(handler) = inner.handlers.get(job.name.as_str()).cloned() else {
        return WorkerAttemptResult::Unhandled;
    };

    let ctx = ctx.clone();
    // Under the `job.run` span `process` runs this in. A spawned task does not inherit its spawner's span, so the
    // handler's every line — payload decoding and extractors included — was filed under no span at all: a log or trace
    // filtered by `job.id` showed the worker's verdict on an attempt and not one line of the handler's explaining it.
    let attempt = async move { handler.call(ctx).await }.in_current_span();
    let mut task = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(attempt));
    let timeout = job.timeout();
    tokio::select! {
        biased;
        _ = force.cancelled() => {
            let _ = join_after_abort(&mut task).await;
            // An explicit shutdown/abort request wins even if the handler
            // happened to become ready in the same scheduler turn.
            WorkerAttemptResult::Cancelled
        }
        result = &mut task => classify_attempt_join(result, WorkerAttemptResult::Cancelled),
        _ = async {
            match timeout {
                Some(timeout) => tokio::time::sleep(timeout).await,
                None => std::future::pending().await,
            }
        } => {
            // The select is biased, so reaching this arm means the handler was
            // not ready: the attempt really did exceed its limit, and a late
            // success must not overwrite that. A panic seen while unwinding is
            // the one outcome more informative than the timeout.
            match join_after_abort(&mut task).await {
                Some(Err(join_error)) if join_error.is_panic() => WorkerAttemptResult::Errored(
                    JobError::new(JobErrorKind::Panic, panic_message(join_error)),
                ),
                // A handler that was already past its last yield point when the
                // deadline fired runs to completion regardless of `abort`, so
                // its error is in hand. Reporting a synthetic timeout instead
                // would throw away the only actionable diagnosis the attempt
                // produced — the same reason a panic outranks the timeout.
                //
                // Its *success* is deliberately not treated the same way, and
                // falls through to the timeout below. A late error adds a reason
                // to an outcome that is a failure either way; a late success would
                // change the outcome, and accepting one makes the deadline
                // advisory — the attempt did exceed its limit, and by then the
                // sweeper may already have adjudicated it stuck. Documented on
                // `JobConfig::timeout`, and pinned by
                // `test_attempt_that_overruns_its_timeout_is_not_retried_past_max_attempts`.
                Some(Ok(Err(job_error))) => handler_errored(job_error),
                _ => WorkerAttemptResult::Errored(JobError::new(
                    JobErrorKind::Timeout,
                    format!("attempt exceeded {:?}", timeout.unwrap_or_default()),
                )),
            }
        }
    }
}

/// Aborts the handler task and waits a bounded time for it to unwind.
///
/// `JoinHandle::abort` only takes effect at the task's next yield point, so a
/// handler that blocks its runtime thread (a synchronous client, `std::fs`, a
/// CPU-bound loop) never completes. Waiting for it without a bound would pin
/// the job row, the processor slot, and — for cron jobs — every future
/// occurrence. Returns `None` when the task did not settle in time.
async fn join_after_abort(
    task: &mut tokio_util::task::AbortOnDropHandle<Result<Value, JobError>>,
) -> Option<Result<Result<Value, JobError>, tokio::task::JoinError>> {
    task.abort();
    match tokio::time::timeout(ATTEMPT_ABORT_JOIN_GRACE, task).await {
        Ok(result) => Some(result),
        Err(_) => {
            tracing::warn!(
                grace = ?ATTEMPT_ABORT_JOIN_GRACE,
                "handler did not yield after abort; finalizing without it. A handler that \
                 blocks its runtime thread cannot be cancelled — use spawn_blocking."
            );
            None
        }
    }
}

/// Rebuilds a handler's own [`JobError`] through [`JobError::new`], the one
/// place that substitutes the NUL PostgreSQL `text` cannot store (`22021`).
///
/// The constructor alone is not enough: `JobError`'s fields are public and
/// `IntoJobResult` is a public trait, so an error can reach this point without
/// ever having been through one — a struct literal, a deserialized error, a
/// user's own `IntoJobResult`. Storing such a message fails identically forever
/// while `finalize` retries once a second, so the attempt keeps its processor
/// slot and its row stays `running` under a healthy lease that nothing can
/// recover. Every handler-supplied error crosses into finalization here or in
/// [`classify_attempt_join`], and both go through this.
fn handler_errored(error: JobError) -> WorkerAttemptResult {
    WorkerAttemptResult::Errored(JobError::new(error.kind, error.message))
}

fn classify_attempt_join(
    result: Result<Result<Value, JobError>, tokio::task::JoinError>,
    cancelled: WorkerAttemptResult,
) -> WorkerAttemptResult {
    match result {
        // `jsonb` stores nesting `serde_json` cannot decode, so
        // `validate_finalization` refuses it and `finalize` would retry that
        // refusal once a second forever. Fail the attempt here instead, at the
        // one place a handler's value becomes a success.
        //
        // A NUL (`22P05`), a string past `jsonb`'s ceiling (`54000`), and
        // nesting `serde_json` cannot decode are all permanent: `finalize`
        // retries such a write once a second for ever while the attempt keeps
        // its row `running` and its processor slot. An attempt whose timeout is
        // disabled is then unrecoverable for as long as its worker heartbeats,
        // and one such result per slot stops the worker. Reported as what it is:
        // a result that cannot be encoded.
        Ok(Ok(value)) => match validate_json_document("job result", &value) {
            Err(message) => {
                WorkerAttemptResult::Errored(JobError::new(JobErrorKind::Decode, format!("result encode: {message}")))
            }
            Ok(()) => WorkerAttemptResult::Success(value),
        },
        Ok(Err(job_error)) => handler_errored(job_error),
        Err(join_error) if join_error.is_panic() => {
            WorkerAttemptResult::Errored(JobError::new(JobErrorKind::Panic, panic_message(join_error)))
        }
        Err(_) => cancelled,
    }
}

/// Applies the attempt's end state to the database. The in-flight guard and
/// worker ownership stay live while transient database errors are retried.
///
/// The retry is bounded by `force_shutdown`, not by the intake stop: closing
/// intake is shutdown's *first* durable act, so binding it to that token gave
/// every attempt finishing during the drain exactly zero retries — one pool
/// timeout and a job that had already succeeded was left `running`, then swept
/// to `aborted` with its result thrown away. The worker lease is deliberately
/// held alive for the whole drain, so nothing can recover the row while a
/// retry is in flight anyway. `drain_processors` caps the total at the
/// shutdown grace plus [`HARD_SHUTDOWN_TIMEOUT`] and aborts past it, so
/// retrying through the grace cannot hang shutdown.
///
/// Before shutdown, a burst also gives up once consecutive failures have lasted
/// its `dequeue_timeout`, and returns the error that ends the run. That is the
/// fetch loop's failed-dequeue budget, with the same granularity: the clock
/// starts at the first failure and is read after each one, so a call that never
/// answers can hold it past the budget for up to [`WORKER_DB_OPERATION_TIMEOUT`]
/// per attempt. Nothing else would end the retry: a processor
/// retrying here never goes idle, so once every processor is doing so the fetch
/// loop is never asked for work and its failed-dequeue budget is never spent —
/// a burst whose database went away mid-attempt never returned. The row is left
/// `running` for recovery once shutdown has expired this worker's lease.
async fn finalize(
    inner: &Arc<WorkerInner>,
    job: &JobRow,
    end: WorkerAttemptResult,
    abort_reason: &OnceLock<WorkerAbortReason>,
    force_shutdown: &CancellationToken,
    cooperative_shutdown: &CancellationToken,
) -> Result<WorkerProcessResult, Error> {
    // `build` refuses `burst` without a `dequeue_timeout`, so in burst mode
    // this is always `Some`.
    let burst_budget = inner.burst.then_some(inner.dequeue_timeout).flatten();
    let mut failing_since: Option<tokio::time::Instant> = None;
    let mut failure = FinalizeFailure { inner, counted: false };
    loop {
        let deadline = tokio::time::Instant::now() + inner.finalize_deadline;
        // A retry's writes end their lock waits on the server at the deadline it is abandoned at, so retrying behind a
        // row lock — an operator's `SELECT ... FOR UPDATE` left open, say — leaves behind no statement still queued
        // there to keep its connection, as every try abandoned on the client alone did, one more per retry, until the
        // worker's pool was gone. The first try keeps the hot path's single round trip per write; see
        // `Database::finish`.
        let bound = failing_since.map(|_| WriteDeadline { at: deadline, exceeded: DB_OPERATION_DEADLINE_EXCEEDED });
        let attempt = try_finalize(inner, job, &end, abort_reason, cooperative_shutdown, bound);
        match with_db_deadline_at(deadline, attempt).await {
            Ok(result) => {
                failure.settle();
                return Ok(result);
            }
            Err(error) => {
                // Reported, not merely logged: this loop pins its processor slot
                // until it succeeds, and a worker whose every slot is pinned
                // here issues no dequeue at all — so nothing else in the worker
                // has a statement left to fail, and health said `Ready` while it
                // processed nothing. See [`WorkerComponent::Finalize`]. A
                // refused transition is `Ok(false)`, not an error, so the
                // ordinary "the row moved on" path never lands here.
                failure.failed(&error);
                let since = *failing_since.get_or_insert_with(tokio::time::Instant::now);
                if !cooperative_shutdown.is_cancelled() && burst_budget.is_some_and(|budget| since.elapsed() >= budget)
                {
                    tracing::error!(%error, "failed to finalize job for the whole dequeue timeout; ending burst");
                    return Err(error);
                }
                tracing::error!(%error, "failed to finalize job; retrying");
                tokio::select! {
                    _ = force_shutdown.cancelled() => return Ok(WorkerProcessResult::Unconfirmed),
                    _ = tokio::time::sleep(FINALIZE_RETRY_INTERVAL) => {}
                }
            }
        }
    }
}

/// One attempt's share of [`WorkerInner::finalize_failing`]: counted from its first failed finalization until it ends,
/// however it ends — recorded, abandoned at shutdown, or its task aborted.
///
/// The component recovers only once no attempt is counted. Recovered by any finalization that succeeded, it was
/// last-writer-wins across processors: with ten slots, one pinned by a row lock or a full disk was reported for the
/// moment between its own retries and cleared by every other slot's success — `Ready` for all but milliseconds of
/// each retry interval, and `since` reset each time — which is the stalled-slot report this component exists to give.
struct FinalizeFailure<'a> {
    inner: &'a WorkerInner,
    counted: bool,
}

impl FinalizeFailure<'_> {
    fn failed(&mut self, error: &Error) {
        let mut failing = self.inner.finalize_failing.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if !self.counted {
            *failing += 1;
            self.counted = true;
        }
        // Under the count's lock, so a concurrent `settle` cannot recover the component between the two.
        self.inner.health.failed(WorkerComponent::Finalize, error);
    }

    fn settle(&mut self) {
        let mut failing = self.inner.finalize_failing.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if self.counted {
            *failing -= 1;
            self.counted = false;
        }
        if *failing == 0 {
            self.inner.health.recovered(WorkerComponent::Finalize);
        }
    }
}

impl Drop for FinalizeFailure<'_> {
    fn drop(&mut self) {
        if self.counted {
            self.settle();
        }
    }
}

/// One try at recording `end`. `bound` is set for a retry: see [`finalize`].
async fn try_finalize(
    inner: &Arc<WorkerInner>,
    job: &JobRow,
    end: &WorkerAttemptResult,
    abort_reason: &OnceLock<WorkerAbortReason>,
    cooperative_shutdown: &CancellationToken,
    bound: Option<WriteDeadline>,
) -> Result<WorkerProcessResult, Error> {
    let database = &inner.database;
    match end {
        WorkerAttemptResult::Success(value) => {
            let outcome = finish_with_swept_fallback(
                database,
                job,
                JobStatus::Complete,
                Some(value.clone()),
                None,
                WorkerProcessResult::Complete,
                bound,
            )
            .await;
            // serde_json/arbitrary_precision permits numbers outside jsonb's numeric range (SQLSTATE 22003).
            // Retrying that result can never succeed. Store a terminal decode failure without the result instead.
            if let Err(Error::Db(sqlx::Error::Database(error))) = &outcome
                && error.code().as_deref() == Some("22003")
            {
                let error = JobError::new(JobErrorKind::Decode, format!("result encode: {error}"));
                let stored_error = error.to_string();
                return finish_with_swept_fallback(
                    database,
                    job,
                    JobStatus::Failed,
                    None,
                    Some(&stored_error),
                    WorkerProcessResult::Failed(error),
                    bound,
                )
                .await;
            }
            outcome
        }
        WorkerAttemptResult::Errored(error) => {
            // The global token says only that shutdown overlapped the outcome,
            // not that it caused it: a handler error, panic, timeout, or decode
            // failure can settle just before or during the same grace window.
            // Refund only an explicitly aborted handler outcome. That kind is
            // the handler's provenance that it stopped for cancellation; every
            // other kind remains a genuine attempt result. A user abort still
            // wins through `abort_reason` and the guarded database transition.
            if error.kind == JobErrorKind::Aborted
                && cooperative_shutdown.is_cancelled()
                && abort_reason.get().is_none()
            {
                let stored_error = error.to_string();
                return match database.requeue_shutdown(job, &stored_error, bound).await {
                    Ok(true) => Ok(WorkerProcessResult::Requeued),
                    // See the shutdown arm of `Cancelled` below.
                    Ok(false) => finish_aborted_fallback(database, job, bound).await,
                    Err(db_error) => Err(db_error),
                };
            }
            if job.is_retryable() && error.kind.is_retryable() {
                let stored_error = error.to_string();
                match database.retry(job, &stored_error, bound).await {
                    Ok(true) => Ok(WorkerProcessResult::Retried(error.clone())),
                    // Retry refused. `Database::retry` itself converts a
                    // sweeper-marked abort into this retry — the marker-guarded
                    // requeue carries the handler's error, so the sweeper
                    // losing the race to a real failure does not replace the
                    // reportable reason with its `swept` marker — which leaves
                    // a refusal meaning the row moved beyond this attempt: a
                    // user abort (never resurrected as a retry), another
                    // attempt's row, or no row at all. Classify from the row.
                    Ok(false) => swept_retry_refusal_result(database, job, error.clone(), bound).await,
                    Err(db_error) => Err(db_error),
                }
            } else {
                let stored_error = error.to_string();
                finish_with_swept_fallback(
                    database,
                    job,
                    JobStatus::Failed,
                    None,
                    Some(&stored_error),
                    WorkerProcessResult::Failed(error.clone()),
                    bound,
                )
                .await
            }
        }
        WorkerAttemptResult::Cancelled => match abort_reason.get() {
            Some(WorkerAbortReason::Swept) if job.is_retryable() => {
                // No handler error to record: the attempt ended because the
                // sweeper took it away, and the marker already on the row says
                // exactly that.
                let error = JobError::new(JobErrorKind::Timeout, "swept");
                retry_swept_or_refuse(database, job, error, None, bound).await
            }
            Some(abort_reason) => {
                let reason = match abort_reason {
                    WorkerAbortReason::Swept => "swept",
                    WorkerAbortReason::User(reason) => reason.as_str(),
                    WorkerAbortReason::Missing => "job row was deleted while the attempt was running",
                    // The row is another attempt's now. Every write path guards
                    // on `(attempts, worker_id)`, so recording anything here
                    // would be refused; report it unconfirmed and leave the row
                    // to its owner.
                    WorkerAbortReason::Superseded => {
                        return Ok(WorkerProcessResult::Unconfirmed);
                    }
                };
                let error = JobError::new(JobErrorKind::Aborted, reason);
                match database.finish(job, JobStatus::Aborted, None, Some(reason), bound).await {
                    Ok(true) => Ok(WorkerProcessResult::Aborted(error)),
                    Ok(false) => Ok(WorkerProcessResult::Unconfirmed),
                    Err(db_error) => Err(db_error),
                }
            }
            // Shutdown: requeue unconditionally. Two refusals still leave the row this attempt's own: a user abort that
            // landed before the abort loop saw it (`aborting` without the sweeper's marker pair, which a requeue must
            // never resurrect), and a refund refused at the `max_attempts` ceiling. The abort fallback finishes both,
            // the user abort under the operator's own reason. Left `aborting`, the row waited for a sweeper to reach it
            // once this worker's lease had lapsed — holding its dedupe key and every waiter until then, and
            // indefinitely when no worker was left running. A row that truly moved on refuses the fallback too.
            None => match database.requeue_shutdown(job, "cancelled", bound).await {
                Ok(true) => Ok(WorkerProcessResult::Requeued),
                Ok(false) => finish_aborted_fallback(database, job, bound).await,
                Err(db_error) => Err(db_error),
            },
        },
        // A refusal usually means the row moved beyond this attempt while the
        // bounce was in flight, but two refusals leave it still ours: a user
        // abort that raced the bounce (`aborting` without the sweeper's
        // marker pair), and a refund refused at the `max_attempts` ceiling.
        // The abort fallback settles all three — it finishes a still-owned
        // row `aborted` while preserving a user abort's stored reason, and
        // no-ops into `Unconfirmed` when the row truly moved on. Returning
        // `Unconfirmed` directly here left the user-abort case `aborting`
        // under a live worker that had already forgotten it, beyond the abort
        // loop (its in-flight entry is removed with this processor) and
        // beyond the sweeper (a live lease defers to the owner).
        WorkerAttemptResult::Unhandled => match database.requeue_unhandled(job, bound).await {
            Ok(true) => Ok(WorkerProcessResult::Bounced),
            Ok(false) => finish_aborted_fallback(database, job, bound).await,
            Err(db_error) => Err(db_error),
        },
    }
}

async fn finish_with_swept_fallback(
    database: &Database,
    job: &JobRow,
    status: JobStatus,
    result: Option<Value>,
    error: Option<&str>,
    process_result: WorkerProcessResult,
    bound: Option<WriteDeadline>,
) -> Result<WorkerProcessResult, Error> {
    // `Database::finish` already lets a handler complete through a sweeper's
    // grace window while never overwriting a user-requested abort.
    match database.finish(job, status, result, error, bound).await {
        Ok(true) => Ok(process_result),
        Ok(false) => finish_aborted_fallback(database, job, bound).await,
        Err(db_error) => Err(db_error),
    }
}

async fn retry_swept_or_refuse(
    database: &Database,
    job: &JobRow,
    error: JobError,
    stored_error: Option<&str>,
    bound: Option<WriteDeadline>,
) -> Result<WorkerProcessResult, Error> {
    match database.retry_swept(job, stored_error, bound).await {
        Ok(true) => Ok(WorkerProcessResult::Retried(error)),
        Ok(false) => swept_retry_refusal_result(database, job, error, bound).await,
        Err(db_error) => Err(db_error),
    }
}

async fn finish_aborted_fallback(
    database: &Database,
    job: &JobRow,
    bound: Option<WriteDeadline>,
) -> Result<WorkerProcessResult, Error> {
    let aborted = JobError::new(JobErrorKind::Aborted, "abort requested during attempt");
    match database.finish(job, JobStatus::Aborted, None, None, bound).await {
        Ok(true) => Ok(WorkerProcessResult::Aborted(aborted)),
        Ok(false) => {
            tracing::debug!("job already finalized elsewhere (likely swept)");
            Ok(WorkerProcessResult::Unconfirmed)
        }
        Err(db_error) => Err(db_error),
    }
}

async fn swept_retry_refusal_result(
    database: &Database,
    job: &JobRow,
    retry_error: JobError,
    bound: Option<WriteDeadline>,
) -> Result<WorkerProcessResult, Error> {
    match database.job(job.id).await {
        Ok(Some(current))
            if current.attempts > job.attempts || matches!(current.status, JobStatus::Queued | JobStatus::Running) =>
        {
            Ok(WorkerProcessResult::RetriedElsewhere(retry_error))
        }
        // Neither requeue this follows ever writes `aborted`, so another actor finished the row: the sweeper, which
        // counted the abort it made. Counting it again here put it on this worker's heartbeat as well.
        Ok(Some(current)) if current.status == JobStatus::Aborted => {
            let error = JobError::new(JobErrorKind::Aborted, current.error.as_deref().unwrap_or("aborted"));
            Ok(WorkerProcessResult::AbortedElsewhere(error))
        }
        Ok(Some(_)) => finish_aborted_fallback(database, job, bound).await,
        Ok(None) => Ok(WorkerProcessResult::Unconfirmed),
        Err(db_error) => Err(db_error),
    }
}

fn panic_message(join_error: tokio::task::JoinError) -> String {
    let payload = join_error.into_panic();
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "handler panicked".to_string()
    }
}

/// Cancels in-flight attempts whose rows moved to `aborting`/`aborted`, were
/// taken away by recovery — requeued, or re-claimed as a later attempt — or
/// disappeared.
async fn abort_loop(inner: Arc<WorkerInner>, token: CancellationToken) {
    let mut interval = tokio::time::interval(inner.timers.abort);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = token.cancelled() => return,
            _ = interval.tick() => {}
        }
        let claims: Vec<DatabaseAbortClaim> = lock_inflight(&inner.inflight)
            .keys()
            .map(|(id, attempts)| DatabaseAbortClaim { id: *id, attempts: *attempts })
            .collect();
        if claims.is_empty() {
            inner.health.recovered(WorkerComponent::Abort);
            continue;
        }
        // Cancellable mid-poll, like every other timer loop: `stop_timers`
        // gives all of them one second *together*, and a statement that is
        // already on the wire cannot be hurried. Without this, a poll caught by
        // a lock wait or a slow round trip turned an otherwise clean shutdown
        // into `Error::WorkerTask("timer shutdown timed out")`. Dropping this
        // read costs nothing: it is a `SELECT`, and by the time the timer token
        // is cancelled the processors have already drained.
        let poll = tokio::select! {
            biased;
            _ = token.cancelled() => return,
            poll = with_db_deadline(inner.database.aborting_of(&claims, inner.id)) => poll,
        };
        match poll {
            Ok(poll) => {
                inner.health.recovered(WorkerComponent::Abort);
                for aborting in poll.aborting {
                    let entry = lock_inflight(&inner.inflight).get(&(aborting.id, aborting.attempts)).cloned();
                    // No worker check here: `aborting_of` reports a row whose
                    // `worker_id` is not this worker's as superseded, so
                    // everything that reaches `aborting` is already ours.
                    if let Some(entry) = entry {
                        let reason = if aborting.swept {
                            WorkerAbortReason::Swept
                        } else {
                            WorkerAbortReason::User(aborting.reason.unwrap_or_else(|| "aborted".to_string()))
                        };
                        entry.request_abort(reason, inner.abort_grace);
                    }
                }
                for claim in poll.missing {
                    let entry = lock_inflight(&inner.inflight).get(&(claim.id, claim.attempts)).cloned();
                    if let Some(entry) = entry {
                        entry.request_abort(WorkerAbortReason::Missing, inner.abort_grace);
                        tracing::warn!(
                            job.id = %claim.id,
                            "in-flight job row was deleted; cancelling its handler"
                        );
                    }
                }
                for claim in poll.superseded {
                    // The lookup is by `(id, attempts)`, so a row this worker
                    // re-claimed in the meantime is a different entry: the
                    // attempt that lost the row is cancelled and its live
                    // successor is left alone.
                    let entry = lock_inflight(&inner.inflight).get(&(claim.id, claim.attempts)).cloned();
                    if let Some(entry) = entry {
                        entry.request_abort(WorkerAbortReason::Superseded, inner.abort_grace);
                        // `claim`, as the `job.run` span names the raw counter:
                        // `attempt` there excludes refunded claims, and the poll
                        // knows nothing of the attempt beyond this key.
                        tracing::warn!(
                            job.id = %claim.id,
                            claim = claim.attempts,
                            "in-flight job row is no longer this attempt's; \
                             cancelling its handler"
                        );
                    }
                }
            }
            Err(error) => {
                inner.health.failed(WorkerComponent::Abort, &error);
                tracing::warn!(%error, "abort poll failed");
            }
        }
    }
}

async fn notification_health_loop(
    inner: Arc<WorkerInner>,
    token: CancellationToken,
    mut health: watch::Receiver<Option<String>>,
) {
    loop {
        match health.borrow_and_update().clone() {
            Some(error) => inner.health.failed(WorkerComponent::Notification, &error),
            None => inner.health.recovered(WorkerComponent::Notification),
        }
        tokio::select! {
            _ = token.cancelled() => return,
            changed = health.changed() => {
                if changed.is_err() {
                    inner.health.failed(
                        WorkerComponent::Notification,
                        &"notification listener stopped",
                    );
                    token.cancelled().await;
                    return;
                }
            }
        }
    }
}

/// Advances durable cron cursors. Schedule rows are the authority; local
/// entries only act when their revision and canonical definition still match.
async fn schedule_loop(
    inner: Arc<WorkerInner>,
    token: CancellationToken,
    mut holder_warned: HashSet<String>,
    mut state: CronSchedulingState,
) {
    let mut interval = tokio::time::interval(inner.timers.schedule);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = token.cancelled() => return,
            _ = interval.tick() => {}
        }
        // Deadline-bounded like every other worker database loop. A pass cut
        // short is retried whole on the next tick: `due_crons` re-selects
        // whatever is still due, the occurrence claims keep a republish from
        // double-firing, and each published cron advanced its own cursor, so
        // interrupted passes still make monotonic progress.
        tokio::select! {
            biased;
            _ = token.cancelled() => return,
            outcome = tokio::time::timeout(
                WORKER_DB_OPERATION_TIMEOUT,
                schedule_crons_once(&inner, &mut holder_warned, &mut state, None),
            ) => {
                if outcome.is_err() {
                    inner.health.failed(WorkerComponent::Scheduler, &"cron scheduling pass exceeded its deadline");
                    tracing::warn!(worker.id = %inner.id, "cron scheduling pass exceeded its deadline");
                }
            }
        }
    }
}

/// Schedules every cron occurrence due when a burst starts, without admitting
/// recurrences that become due while the queue is draining.
///
/// Trouble is retried within the burst's `dequeue_timeout`, exactly as the
/// fetch loop treats failed dequeues, and then fails the run: a burst is a
/// cron or CI invocation that exits on [`Worker::run_until`]'s result, and an
/// exit code of zero with a due occurrence unpublished — or unknowable,
/// because its reconciliation kept failing — is the silent failure the burst
/// contract exists to rule out. A cron *disabled* by revision arbitration or a
/// rejected definition is not trouble: that is the documented rolling-deploy
/// state, it degrades health, and ordinary jobs keep flowing. Only the
/// transient set counts against the budget, and `state.unreconciled` is
/// exactly that set — every failed reconciliation and every failed
/// `schedule_cron` lands there for the next pass to retry.
async fn schedule_burst_crons(
    inner: &Arc<WorkerInner>,
    holder_warned: &mut HashSet<String>,
    state: &mut CronSchedulingState,
) -> Result<(), Error> {
    const RETRY_INTERVAL: Duration = Duration::from_secs(1);
    const FAILED: Error = Error::WorkerTask("burst cron scheduling failed for the whole dequeue timeout");
    // `build` refuses burst mode without a dequeue timeout, so the fallback is
    // unreachable — and fails closed if that ever changes.
    let deadline = tokio::time::Instant::now() + inner.dequeue_timeout.unwrap_or(Duration::ZERO);
    // The budget bounds how long *trouble* is retried, never a healthy first
    // attempt: a caller may configure it in nanoseconds to mean "one look and
    // out", and clamping individual operations to it starved reads that would
    // have answered in a millisecond. A genuinely wedged operation therefore
    // still costs up to one `WORKER_DB_OPERATION_TIMEOUT` before the budget
    // takes over, which is the documented worst case.
    let through = loop {
        match with_db_deadline(inner.database.now()).await {
            Ok(through) => break through,
            Err(error) => {
                let mut failures = state.failures();
                failures.push(format!("could not read burst cron boundary: {error}"));
                inner.health.failed(WorkerComponent::Scheduler, &failures.join("; "));
                if tokio::time::Instant::now() >= deadline {
                    return Err(FAILED);
                }
                tokio::time::sleep(RETRY_INTERVAL).await;
            }
        }
    };
    loop {
        // The same per-pass deadline the continuous loop applies, so a wedged
        // pass is observed rather than holding `run_until` open indefinitely.
        // The *pass* keeps the fixed operation deadline rather than the
        // remaining budget, deliberately: a burst waits for a due schedule row
        // another transaction holds locked — `FOR UPDATE`, not `SKIP LOCKED`,
        // is the burst's documented choice, pinned by
        // `test_burst_worker_waits_for_a_locked_due_cron_schedule` — and a
        // lock wait is indistinguishable from a wedge at this level. The
        // budget governs *failures*: a pass that returns trouble consults it
        // above, and one wedged past the operation deadline is cut and then
        // fails the run through the same check.
        let advanced = match tokio::time::timeout(
            WORKER_DB_OPERATION_TIMEOUT,
            schedule_crons_once(inner, holder_warned, state, Some(through)),
        )
        .await
        {
            Ok(advanced) => advanced,
            Err(_) => {
                inner.health.failed(WorkerComponent::Scheduler, &"cron scheduling pass exceeded its deadline");
                tracing::warn!(worker.id = %inner.id, "burst cron scheduling pass exceeded its deadline");
                if tokio::time::Instant::now() >= deadline {
                    return Err(FAILED);
                }
                // The pass spent its own deadline already; retry immediately.
                continue;
            }
        };
        if state.unreconciled.is_empty() {
            if advanced {
                continue;
            }
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(FAILED);
        }
        tokio::time::sleep(RETRY_INTERVAL).await;
    }
}

/// What startup reconciliation decided about this worker's crons.
#[derive(Default)]
struct CronSchedulingState {
    /// Dedupe keys this worker must not schedule.
    disabled: HashSet<String>,
    /// Dedupe keys that need reconciling again for a reason that may pass on a
    /// later attempt — a reconciliation that failed transiently or found its
    /// schedule row held, or a schedule row that went missing under a running
    /// worker — so every scheduling pass retries them, once it has scheduled
    /// the rest.
    unreconciled: HashSet<String>,
    /// Why each permanently rejected cron was disabled. Held apart from
    /// [`Self::rejected`] and never cleared: a disabled cron is never
    /// re-evaluated, so nothing can ever re-add its failure, and folding it in
    /// with the retryable ones let an *unrelated* transient cron recovering
    /// erase it and report the worker healthy.
    disabled_reasons: Vec<String>,
    /// Reconciliation failures that a later attempt may clear. Discarded and
    /// re-collected whenever those crons are retried.
    rejected: Vec<String>,
    /// Consecutive passes in which each schedule row this worker had to publish
    /// against, or reconcile, was locked. Entries disappear after a pass gets
    /// through to the row, or finds it neither due nor waiting to be reconciled.
    contended: HashMap<String, u32>,
}

impl CronSchedulingState {
    /// Every reconciliation failure still in force, permanent ones first.
    fn failures(&self) -> Vec<String> {
        let mut failures: Vec<String> = self.disabled_reasons.iter().chain(&self.rejected).cloned().collect();
        let mut contended: Vec<String> = self
            .contended
            .iter()
            .filter(|(_, ticks)| **ticks >= MAX_CRON_CONTENDED_TICKS)
            .map(|(key, _)| {
                format!(
                    "{key}: cron schedule row remained locked for at least \
                     {MAX_CRON_CONTENDED_TICKS} consecutive passes"
                )
            })
            .collect();
        contended.sort();
        failures.extend(contended);
        failures
    }
}

/// Reconciles every registered cron against the durable schedule rows.
///
/// A cron problem never stops the worker. A superseded revision is normal for
/// a not-yet-upgraded process during a rolling deploy, but it is also the
/// permanent state after an unsafe rollback. Both supersession and rejected
/// definitions therefore degrade `Scheduler` health while ordinary jobs keep
/// flowing.
async fn reconcile_all_crons(inner: &Arc<WorkerInner>, state: &mut CronSchedulingState) {
    reconcile_crons_into(inner, state, None).await;
    let failures = state.failures();
    if failures.is_empty() {
        inner.health.recovered(WorkerComponent::Scheduler);
    } else {
        inner.health.failed(WorkerComponent::Scheduler, &failures.join("; "));
    }
}

/// Reconciles the registered crons — all of them, or only `retry_keys` when the
/// scheduling loop is retrying earlier failures — recording the outcome in
/// `state`. Leaves health alone; the caller owns that.
async fn reconcile_crons_into(
    inner: &Arc<WorkerInner>,
    state: &mut CronSchedulingState,
    retry_keys: Option<&HashSet<String>>,
) {
    let selected = || inner.crons.iter().filter(|entry| retry_keys.is_none_or(|keys| keys.contains(&entry.dedupe_key)));
    for entry in selected() {
        // One clock reading per entry. `reconcile_cron` is a round trip of its
        // own, so a reading shared across the loop is already stale by the time
        // a large registry reaches its last entries — and a cursor computed
        // from a stale clock lands in the past, making the cron instantly "due"
        // with an occurrence its own misfire policy then has to skip.
        let now = match with_db_deadline(inner.database.now()).await {
            Ok(now) => now,
            Err(error) => {
                // Infrastructure, not a definition problem: a pool timeout or a
                // failover during a rolling restart must not silently stop every
                // cron for the rest of this process's life, so these stay pending
                // and the scheduling loop retries them.
                tracing::error!(
                    cron = %entry.template.name,
                    dedupe_key = %entry.dedupe_key,
                    %error,
                    "cron reconciliation could not read the database clock"
                );
                state.rejected.push(format!("{}: {error}", entry.dedupe_key));
                state.unreconciled.insert(entry.dedupe_key.clone());
                continue;
            }
        };
        let reconciled = with_db_deadline(inner.database.reconcile_cron(entry, now)).await;
        if !matches!(reconciled, Ok(DatabaseCronAuthority::Contended)) {
            state.contended.remove(&entry.dedupe_key);
        }
        match reconciled {
            Ok(DatabaseCronAuthority::Active) => {
                state.unreconciled.remove(&entry.dedupe_key);
            }
            // Another transaction held the schedule row for the whole wait
            // reconciliation allows it. Retried like a transient failure, and
            // reported like the scheduler's own contention: a warning on the
            // first pass of a run, and degraded health once the run persists.
            Ok(DatabaseCronAuthority::Contended) => {
                state.unreconciled.insert(entry.dedupe_key.clone());
                let ticks = state.contended.entry(entry.dedupe_key.clone()).or_default();
                *ticks = ticks.saturating_add(1);
                if *ticks == 1 {
                    tracing::warn!(
                        cron = %entry.template.name,
                        dedupe_key = %entry.dedupe_key,
                        "cron schedule row is locked by another transaction; reconciliation deferred"
                    );
                }
            }
            Ok(DatabaseCronAuthority::Inactive { revision }) => {
                tracing::info!(
                    cron = %entry.template.name,
                    dedupe_key = %entry.dedupe_key,
                    local.revision = entry.options.revision,
                    authority.revision = revision,
                    "cron superseded by a higher revision; not scheduled by this worker"
                );
                state.unreconciled.remove(&entry.dedupe_key);
                state.disabled.insert(entry.dedupe_key.clone());
                state.disabled_reasons.push(format!(
                    "{}: local cron revision {} is superseded by durable revision {}",
                    entry.dedupe_key, entry.options.revision, revision
                ));
            }
            // A rejected *definition* is a deploy mistake that no retry can
            // fix, so it disables the cron. Anything else is treated as
            // transient and retried on the next scheduling pass.
            Err(error) => {
                let permanent = matches!(error, Error::Config(_));
                tracing::error!(
                    cron = %entry.template.name,
                    dedupe_key = %entry.dedupe_key,
                    %error,
                    permanent,
                    "cron reconciliation failed"
                );
                let reason = format!("{}: {error}", entry.dedupe_key);
                if permanent {
                    state.unreconciled.remove(&entry.dedupe_key);
                    state.disabled.insert(entry.dedupe_key.clone());
                    state.disabled_reasons.push(reason);
                } else {
                    state.unreconciled.insert(entry.dedupe_key.clone());
                    state.rejected.push(reason);
                }
            }
        }
    }
}

/// One scheduling pass: publishes whatever is due among the crons this worker
/// has reconciled, then retries reconciling the rest, and reports the outcome
/// on [`WorkerComponent::Scheduler`]. Returns whether a burst, whose passes run
/// back to back until one changes nothing, has another to make: it published or
/// skipped an occurrence, or reconciled a cron for the next pass to schedule.
///
/// Reconciliation goes second so that it cannot hold back the rest. A cron is
/// retried here because something kept it from settling — a database blip, a
/// schedule row lost under the worker, a row another transaction held — and
/// retried first, it stood between every reconciled cron and its occurrence:
/// a held row, which reconciliation once waited on without bound, stopped
/// every cron of the worker's for as long as it was held.
async fn schedule_crons_once(
    inner: &Arc<WorkerInner>,
    holder_warned: &mut HashSet<String>,
    state: &mut CronSchedulingState,
    through: Option<Timestamp>,
) -> bool {
    let mut pass_failures = Vec::new();
    let candidates: Vec<String> = inner
        .crons
        .iter()
        .filter(|entry| !state.disabled.contains(&entry.dedupe_key) && !state.unreconciled.contains(&entry.dedupe_key))
        .map(|entry| entry.dedupe_key.clone())
        .collect();
    // One read for the whole registry instead of a transaction per cron, and
    // none at all for a worker whose crons are every one of them disabled —
    // the permanent state of a worker a higher revision has superseded. The
    // pre-filter is an optimisation only, so a failure falls back to asking
    // every cron directly rather than skipping the pass.
    let due = if candidates.is_empty() {
        HashSet::new()
    } else {
        match inner.database.due_crons(&candidates, through).await {
            Ok(due) => due,
            Err(error) => {
                tracing::debug!(%error, "cron due-check failed; scheduling every cron directly");
                candidates.iter().cloned().collect()
            }
        }
    };
    state.contended.retain(|key, _| due.contains(key) || state.unreconciled.contains(key));
    let mut advance_again = false;
    for entry in &inner.crons {
        if !due.contains(&entry.dedupe_key) {
            continue;
        }
        let scheduled = inner.database.schedule_cron(entry, through).await;
        if !matches!(&scheduled, Ok(DatabaseCronScheduleResult::Contended)) {
            state.contended.remove(&entry.dedupe_key);
        }
        match scheduled {
            Ok(DatabaseCronScheduleResult::NotDue) => {}
            // Another transaction holds the schedule row. Ordinarily that is a
            // peer's publication and gone within a round trip, which is why the
            // scheduler simply tries again next tick — but an *outside* holder
            // (an operator's `SELECT ... FOR UPDATE`, a session left open in a
            // transaction) holds it indefinitely, and this arm was silent: no
            // log, no health change, the cursor frozen and the cron simply not
            // firing for as long as it lasted. Warn on the first pass in each
            // consecutive run, then let health carry a persistent one.
            Ok(DatabaseCronScheduleResult::Contended) => {
                let ticks = state.contended.entry(entry.dedupe_key.clone()).or_default();
                *ticks = ticks.saturating_add(1);
                if *ticks == 1 {
                    tracing::warn!(
                        cron = %entry.template.name,
                        dedupe_key = %entry.dedupe_key,
                        "cron schedule row is locked by another transaction; occurrence deferred"
                    );
                }
            }
            Ok(DatabaseCronScheduleResult::Published { id, occurrence }) => {
                advance_again |= through.is_some();
                holder_warned.remove(&entry.dedupe_key);
                tracing::info!(
                    cron = %entry.template.name,
                    job.id = %id,
                    scheduled_at = %occurrence,
                    "published cron occurrence"
                );
            }
            Ok(DatabaseCronScheduleResult::AlreadyPublished { occurrence }) => {
                advance_again |= through.is_some();
                holder_warned.remove(&entry.dedupe_key);
                tracing::debug!(
                    cron = %entry.template.name,
                    scheduled_at = %occurrence,
                    "cron occurrence was already published"
                );
            }
            Ok(DatabaseCronScheduleResult::SkippedStale { occurrence }) => {
                advance_again |= through.is_some();
                tracing::warn!(
                    cron = %entry.template.name,
                    scheduled_at = %occurrence,
                    "skipped stale cron occurrence"
                );
            }
            Ok(DatabaseCronScheduleResult::SkippedHeld { occurrence, existing }) => {
                advance_again |= through.is_some();
                if holder_warned.insert(entry.dedupe_key.clone()) {
                    tracing::warn!(
                        cron = %entry.template.name,
                        scheduled_at = %occurrence,
                        dedupe_key = %entry.dedupe_key,
                        holder.scheduled_at = %existing.scheduled_at,
                        holder.kind = %existing.kind,
                        holder.name = %existing.name,
                        "cron dedupe key is held by another live job; occurrence skipped"
                    );
                }
            }
            // Another worker published a higher revision while this one was
            // running. This is expected during a rolling deploy, but the same
            // state is permanent after an unsafe rollback. Disable the local
            // cron and degrade health without stopping ordinary job handling.
            // See `CronOptions::revision`.
            Ok(DatabaseCronScheduleResult::Inactive { revision }) => {
                state.disabled.insert(entry.dedupe_key.clone());
                state.disabled_reasons.push(format!(
                    "{}: local cron revision {} is superseded by durable revision {}",
                    entry.dedupe_key, entry.options.revision, revision
                ));
                tracing::warn!(
                    cron = %entry.template.name,
                    dedupe_key = %entry.dedupe_key,
                    local.revision = entry.options.revision,
                    authority.revision = revision,
                    "cron superseded by a higher revision; not scheduled by this worker"
                );
            }
            // Not a deploy in progress but a deploy mistake: the stored
            // definition differs at this worker's own revision or below. The
            // same mismatch at startup is an `Error::Config` that degrades the
            // scheduler, so it degrades it here too rather than being logged as
            // a supersession that the revisions themselves contradict.
            Ok(DatabaseCronScheduleResult::Conflicting { revision }) => {
                state.disabled.insert(entry.dedupe_key.clone());
                let reason = format!(
                    "{}: stored definition conflicts with revision {}",
                    entry.dedupe_key, entry.options.revision
                );
                tracing::error!(
                    cron = %entry.template.name,
                    dedupe_key = %entry.dedupe_key,
                    local.revision = entry.options.revision,
                    authority.revision = revision,
                    "cron definition conflicts with the stored schedule; not scheduled by this worker"
                );
                // The reason is read after the loop, so degradation takes
                // effect on this pass and persists: a disabled cron is never
                // re-evaluated.
                state.disabled_reasons.push(reason);
            }
            // Queued for reconciliation, exactly like a transient
            // reconciliation failure: `reconcile_crons` runs once, at startup,
            // so `state.unreconciled` is the only thing that can rewrite a
            // schedule row this worker lost underneath it — and a lost row is
            // what `schedule_cron` reports as "was not reconciled", every tick,
            // forever. This cannot spin: a definition the database genuinely
            // refuses comes back from reconciliation as `Error::Config` and
            // lands in `state.disabled`.
            Err(error) => {
                tracing::warn!(%error, cron = %entry.template.name, "cron scheduling failed");
                state.unreconciled.insert(entry.dedupe_key.clone());
                pass_failures.push(format!("{}: {error}", entry.template.name));
            }
        }
    }
    // Retry any cron whose reconciliation failed, so a blip at startup costs a
    // pass rather than every occurrence for the lifetime of the process.
    if !state.unreconciled.is_empty() {
        // Only the retryable failures are discarded here; `disabled_reasons`
        // survives, so a permanently rejected cron keeps degrading health even
        // while an unrelated transient one recovers.
        state.rejected.clear();
        let retry_keys = state.unreconciled.clone();
        reconcile_crons_into(inner, state, Some(&retry_keys)).await;
        // A cron settled here is scheduled from the next pass on, which a burst
        // would otherwise not make.
        advance_again |= state.unreconciled.len() < retry_keys.len();
    }
    let mut failed = state.failures();
    failed.extend(pass_failures);
    if failed.is_empty() {
        inner.health.recovered(WorkerComponent::Scheduler);
    } else {
        inner.health.failed(WorkerComponent::Scheduler, &failed.join("; "));
    }
    advance_again
}

/// Runs the sweeper on its timer; leadership is advisory-lock coordinated.
async fn sweep_loop(inner: Arc<WorkerInner>, token: CancellationToken) {
    run_sweep_loop_with_drain_time(inner, token, MAX_SWEEP_DRAIN_TIME).await;
}

async fn run_sweep_loop_with_drain_time(inner: Arc<WorkerInner>, token: CancellationToken, max_drain_time: Duration) {
    let mut sweeper = inner.database.sweeper();
    let mut interval = tokio::time::interval(inner.timers.sweep);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut behind_ticks: u32 = 0;
    loop {
        tokio::select! {
            _ = token.cancelled() => {
                sweeper.release().await;
                return;
            }
            _ = interval.tick() => {}
        }
        let drain_until = tokio::time::Instant::now() + inner.timers.sweep.min(max_drain_time);
        // The sweeper shares the worker's pool with dequeues and finalization,
        // so a drain is bounded by passes as well as wall clock, and each pass
        // repeats only the operations that filled their batch.
        let mut operations = SweepOperations::ALL;
        // Which budget ended the drain with work still pending, if either did.
        // Health is settled once per tick rather than per pass, so that a drain
        // that ends behind is not immediately reported as recovered by its own
        // last successful pass.
        let mut exhausted: Option<&'static str> = None;
        let mut failure = None;
        for pass in 1..=MAX_SWEEP_DRAIN_PASSES {
            // A pass issues several statements against the shared pool and can
            // outlast the shutdown budget on a loaded database. Without a
            // cancellation point *inside* the pass, an ordinary shutdown that
            // lands mid-sweep would exhaust that budget and report a timer
            // shutdown failure for an otherwise clean stop.
            let swept = tokio::select! {
                biased;
                _ = token.cancelled() => {
                    sweeper.release().await;
                    return;
                }
                swept = sweeper.sweep_operations(operations) => swept,
            };
            match swept {
                Ok(report) if report.has_more_work() => {
                    if pass == MAX_SWEEP_DRAIN_PASSES {
                        exhausted = Some("passes");
                        break;
                    }
                    if tokio::time::Instant::now() >= drain_until {
                        exhausted = Some("time");
                        break;
                    }
                    operations = report.unfinished;
                    tokio::select! {
                        biased;
                        _ = token.cancelled() => {
                            sweeper.release().await;
                            return;
                        }
                        _ = tokio::task::yield_now() => {}
                    }
                }
                Ok(_) => break,
                Err(error) => {
                    failure = Some(error);
                    break;
                }
            }
        }
        match (failure, exhausted) {
            (Some(error), _) => {
                behind_ticks = 0;
                inner.health.failed(WorkerComponent::Sweeper, &error);
                tracing::warn!(%error, "sweep failed");
            }
            // Still behind when the drain stopped. One tick of this is how a
            // burst clears, so it degrades health only once it persists.
            (None, Some(budget)) => {
                behind_ticks = behind_ticks.saturating_add(1);
                if behind_ticks >= MAX_SWEEP_BEHIND_TICKS {
                    let error = format!(
                        "sweeper is falling behind: {behind_ticks} consecutive drains exhausted their \
                         {budget} budget with work still pending. Raise `sweep_batch_size` or shorten \
                         the `sweep` timer."
                    );
                    tracing::warn!(behind_ticks, budget, "sweeper is falling behind");
                    inner.health.failed(WorkerComponent::Sweeper, &error);
                } else {
                    tracing::debug!(budget, "sweep drain budget exhausted");
                    inner.health.recovered(WorkerComponent::Sweeper);
                }
            }
            (None, None) => {
                behind_ticks = 0;
                inner.health.recovered(WorkerComponent::Sweeper);
            }
        }
    }
}

#[cfg(feature = "_test")]
pub(crate) async fn run_sweep_loop_for_test(worker: Worker, token: CancellationToken, max_drain_time: Duration) {
    run_sweep_loop_with_drain_time(worker.inner, token, max_drain_time).await;
}

/// Renews this worker's lease every [`WorkerTimers::worker_info`]; the same row carries the statistics
/// `Queue::workers_page` and the dashboard show.
async fn worker_info_loop(inner: Arc<WorkerInner>, token: CancellationToken) {
    let mut interval = tokio::time::interval(inner.timers.worker_info);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Outside the queue's pool, for the reason `heartbeat_deadline` gives; closed when the loop ends.
    let mut lease = LeaseConnection::default();
    loop {
        tokio::select! {
            _ = token.cancelled() => return,
            _ = interval.tick() => {}
        }
        // Also cancellable mid-write (see `abort_loop`). A heartbeat is an
        // idempotent upsert of this worker's own lease, and shutdown retires
        // that lease immediately afterwards, so abandoning one in flight loses
        // nothing — while waiting for it to land can spend the whole
        // timer-shutdown budget on a write whose result no longer matters.
        tokio::select! {
            biased;
            _ = token.cancelled() => return,
            _ = renew_worker_info(&inner, &mut lease) => {}
        }
    }
}

/// Writes this worker's lease and statistics on its queue's pool, reporting the outcome on
/// [`WorkerComponent::WorkerInfo`].
async fn write_worker_info(inner: &Arc<WorkerInner>, ttl: Duration) {
    write_worker_info_within(inner, ttl, WORKER_DB_OPERATION_TIMEOUT).await;
}

/// [`write_worker_info`] with the write itself — not the wait for a connection to make it on, which stays under the
/// pool's `acquire_timeout` and [`WORKER_DB_OPERATION_TIMEOUT`] like every other operation's — abandoned after
/// `deadline`, and its wait for the lease row's lock ended there on the server too; see
/// `Database::write_worker_lease_within`.
async fn write_worker_info_within(inner: &Arc<WorkerInner>, ttl: Duration, deadline: Duration) {
    let Some((stats, intake)) = worker_info(inner) else {
        return;
    };
    let write =
        inner.database.write_worker_lease_within(inner.id, stats, inner.metadata.clone(), ttl, intake, deadline);
    report_worker_info(inner, with_db_deadline(write).await);
}

/// One periodic heartbeat: [`write_worker_info`] on the heartbeat loop's own `lease` connection rather than the pool,
/// held to [`heartbeat_deadline`] with opening that connection included; see `Database::renew_worker_lease`.
async fn renew_worker_info(inner: &Arc<WorkerInner>, lease: &mut LeaseConnection) {
    let Some((stats, intake)) = worker_info(inner) else {
        return;
    };
    let ttl = worker_info_ttl(inner.timers.worker_info);
    let deadline = heartbeat_deadline(inner.timers.worker_info);
    let renewal =
        inner.database.renew_worker_lease(lease, inner.id, stats, inner.metadata.clone(), ttl, intake, deadline);
    report_worker_info(inner, with_db_deadline(renewal).await);
}

/// What a lease write carries: this worker's statistics — a failure to serialize them is reported on
/// [`WorkerComponent::WorkerInfo`], and nothing is written — and the intake state of a lease the write has to create.
fn worker_info(inner: &WorkerInner) -> Option<(Value, LeaseIntake)> {
    let stats = match stats_json(inner) {
        Ok(stats) => stats,
        Err(error) => {
            inner.health.failed(WorkerComponent::WorkerInfo, &error);
            tracing::warn!(%error, "failed to serialize worker info");
            return None;
        }
    };
    // Never `Reopen`: a worker's heartbeat is not a request for work, so it
    // must leave a lease `close_intake` already closed alone. `Open`/`Closed`
    // only decide what a lease this write has to *create* starts as.
    let intake = if inner.intake_open.load(Ordering::Acquire) { LeaseIntake::Open } else { LeaseIntake::Closed };
    Some((stats, intake))
}

fn report_worker_info(inner: &WorkerInner, written: Result<(), Error>) {
    match written {
        Ok(()) => inner.health.recovered(WorkerComponent::WorkerInfo),
        Err(error) => {
            inner.health.failed(WorkerComponent::WorkerInfo, &error);
            tracing::warn!(%error, "failed to write worker info");
        }
    }
}

fn stats_json(inner: &WorkerInner) -> Result<Value, Error> {
    let mut value = serde_json::to_value(inner.counters.snapshot())?;
    if let Value::Object(fields) = &mut value {
        fields.insert(
            "uptime_ms".into(),
            Value::from(inner.started.get().map(|started| started.elapsed().as_millis() as u64).unwrap_or_default()),
        );
    }
    Ok(value)
}

#[cfg(test)]
mod loop_tests {
    use super::*;

    /// The timeout arm keeps a settled handler's error instead of the deadline,
    /// so it reaches finalization without passing through `classify_attempt_join`
    /// — both routes have to sanitize, and they share one constructor so they
    /// cannot drift apart.
    #[test]
    fn test_handler_error_loses_its_nul_on_every_route_into_finalization() {
        let raw = JobError { kind: JobErrorKind::Timeout, message: "bad\u{0}input".to_string() };
        for result in [
            handler_errored(raw.clone()),
            classify_attempt_join(Ok(Err(raw)), WorkerAttemptResult::Cancelled),
        ] {
            match result {
                WorkerAttemptResult::Errored(error) => {
                    assert_eq!(error.message, "bad\u{fffd}input");
                    assert_eq!(error.kind, JobErrorKind::Timeout, "the kind is preserved");
                }
                _ => panic!("a handler error must fail the attempt"),
            }
        }
    }

    /// `validate_json_document` runs several walks over a handler's result, and
    /// only `json_exceeds_depth` is bounded. The others recurse one frame per
    /// container, so the bounded one has to run *first* — otherwise a deep enough
    /// result overflows the runtime thread's stack, which aborts the process
    /// rather than failing the attempt. The order lives in that one helper now;
    /// this pins it from the call site that once got it wrong.
    ///
    /// A value that violates both rules is the only way to observe the order:
    /// whichever walk runs first names the error.
    #[test]
    fn test_the_bounded_depth_walk_runs_before_the_unbounded_ones() {
        let mut value = Value::String("bad\u{0}input".to_string());
        for _ in 0..(crate::job::MAX_JSON_DEPTH + 8) {
            value = Value::Array(vec![value]);
        }
        match classify_attempt_join(Ok(Ok(value)), WorkerAttemptResult::Cancelled) {
            WorkerAttemptResult::Errored(error) => assert!(
                error.message.contains("nest deeper"),
                "the bounded depth walk must run first, got: {}",
                error.message
            ),
            _ => panic!("an unencodable result must fail the attempt"),
        }
    }

    /// The deadline turns an operation that will never answer into an ordinary
    /// failed one — the shape every caller already survives — and leaves an
    /// operation that answers in time untouched. Paused time, so the sixty
    /// seconds are reached without being spent.
    #[tokio::test(start_paused = true)]
    async fn test_with_db_deadline_abandons_an_operation_that_never_answers() {
        assert!(matches!(with_db_deadline(async { Ok::<_, Error>(7) }).await, Ok(7)));

        let error = with_db_deadline(std::future::pending::<Result<(), Error>>()).await.unwrap_err();
        assert!(matches!(error, Error::WorkerTask("worker database operation exceeded its deadline")), "{error}");
    }

    /// A heartbeat whose write stalls — parked on a connection that never answers, or behind a lock on its lease row
    /// — must be given up while the lease it renews still has time for a retry to land. Under the 60s operation
    /// deadline the default 30s lease lapsed first, and the sweeper then took a live worker's attempts away from it.
    #[test]
    fn test_a_stalled_heartbeat_is_retried_before_its_lease_lapses() {
        let interval = WorkerTimers::default().worker_info;
        let deadline = heartbeat_deadline(interval);
        assert_eq!(deadline, interval);
        // The ticker is overdue when the stalled write is abandoned, so the retry starts then and has the rest of the
        // lease: two intervals in, one interval left. Opening the retry's connection is counted in that interval, and
        // can be: it is a fresh one of the heartbeat's own, with no pool to queue behind or idle connection to test —
        // see `heartbeat_deadline`.
        assert!(interval + deadline + interval <= worker_info_ttl(interval), "no time left for a retry to land");

        assert_eq!(heartbeat_deadline(Duration::from_millis(100)), HEARTBEAT_DEADLINE_FLOOR);
        assert_eq!(heartbeat_deadline(Duration::from_secs(3_600)), WORKER_DB_OPERATION_TIMEOUT);
    }

    #[tokio::test]
    async fn test_worker_health_ignores_unchanged_snapshots() {
        let reporter = WorkerHealthReporter::new();
        let mut health = reporter.subscribe();

        reporter.recovered(WorkerComponent::Notification);
        assert!(tokio::time::timeout(Duration::from_millis(50), health.changed()).await.is_err());

        reporter.ready();
        let snapshot = tokio::time::timeout(Duration::from_secs(1), health.changed()).await.unwrap();
        assert_eq!(snapshot.status, WorkerHealthStatus::Ready);
    }

    /// The sibling of the test below, for the ordering a real run produces:
    /// `Stopped` is published and the sender is dropped behind it. Both are
    /// wakeups, so an observer that keys the end of the stream on closure alone
    /// received the terminal snapshot twice and ran its shutdown handling twice.
    #[tokio::test]
    async fn test_worker_health_emits_its_terminal_snapshot_once() {
        let reporter = WorkerHealthReporter::new();
        let mut health = reporter.subscribe();
        reporter.ready();
        // Observed, so the terminal publish below is a change this receiver has
        // not seen rather than one collapsed into it — a `watch` keeps only the
        // latest value.
        let snapshot = tokio::time::timeout(Duration::from_secs(1), health.changed()).await.unwrap();
        assert_eq!(snapshot.status, WorkerHealthStatus::Ready);

        reporter.stopped();
        drop(reporter);

        let snapshot = tokio::time::timeout(Duration::from_secs(1), health.changed()).await.unwrap();
        assert_eq!(snapshot.status, WorkerHealthStatus::Stopped);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), health.changed()).await.is_err(),
            "the terminal snapshot was emitted more than once"
        );
    }

    #[tokio::test]
    async fn test_worker_health_reports_channel_close_once_without_spinning() {
        let reporter = WorkerHealthReporter::new();
        let mut health = reporter.subscribe();
        drop(reporter);

        let snapshot = tokio::time::timeout(Duration::from_secs(1), health.changed()).await.unwrap();
        assert_eq!(snapshot.status, WorkerHealthStatus::Starting);
        assert!(tokio::time::timeout(Duration::from_millis(50), health.changed()).await.is_err());
    }

    #[tokio::test]
    async fn test_wait_for_processors_rejects_clean_exit_when_continuous() {
        let mut processors = JoinSet::new();
        processors.spawn(async { Ok(()) });

        let error = wait_for_processors(&mut processors, false).await.unwrap_err();

        assert!(matches!(error, Error::WorkerTask("processor loop")));
    }

    #[tokio::test]
    async fn test_wait_for_processors_allows_clean_exits_when_burst() {
        let mut processors = JoinSet::new();
        processors.spawn(async { Ok(()) });
        processors.spawn(async { Ok(()) });

        wait_for_processors(&mut processors, true).await.unwrap();
    }

    /// A burst processor that could not record an outcome ends the run with that error, not with a drain.
    #[tokio::test]
    async fn test_wait_for_processors_reports_a_burst_processor_error() {
        let mut processors = JoinSet::new();
        processors.spawn(async { Err(Error::WorkerTask("finalize")) });

        let error = wait_for_processors(&mut processors, true).await.unwrap_err();

        assert!(matches!(error, Error::WorkerTask("finalize")));
    }

    #[tokio::test]
    async fn test_wait_for_processors_reports_panics() {
        let mut processors = JoinSet::<Result<(), Error>>::new();
        processors.spawn(async { panic!("processor panic") });

        let error = wait_for_processors(&mut processors, false).await.unwrap_err();

        assert!(matches!(error, Error::Task(error) if error.is_panic()));
    }

    #[tokio::test]
    async fn test_wait_for_background_exit_reports_loop_name() {
        let mut tasks = JoinSet::new();
        tasks.spawn(async { "test loop" });

        let error = wait_for_background_exit(&mut tasks).await;

        assert!(matches!(error, Error::WorkerTask("test loop")));
    }

    fn at(iso: &str) -> Timestamp {
        iso.parse().unwrap()
    }

    #[test]
    fn test_cron_validity_grace_scales_with_the_period() {
        let next = at("2026-01-01T00:00:00Z");
        let minutely = entry("* * * * *");
        // Every minute: a fifth of the period.
        assert_eq!(minutely.publication_deadline(next, at("2026-01-01T00:01:00Z")), at("2026-01-01T00:00:12Z"));
        let every_five_minutes = entry("*/5 * * * *");
        assert_eq!(
            every_five_minutes.publication_deadline(next, at("2026-01-01T00:05:00Z")),
            at("2026-01-01T00:01:00Z")
        );
        // Daily: a fifth of a day, not a fixed minute. An absolute cap here made
        // a daily schedule drop its occurrence over a worker gap of barely a
        // minute — one rolling restart — while leaving the minutely schedule
        // above a grace proportionate to its own period.
        let daily = entry("0 0 * * *");
        assert_eq!(daily.publication_deadline(next, at("2026-01-02T00:00:00Z")), at("2026-01-01T04:48:00Z"));
        // Still never the whole period: that is `FireOnce`, a different policy.
        assert!(daily.publication_deadline(next, at("2026-01-02T00:00:00Z")) < at("2026-01-02T00:00:00Z"));
    }

    fn entry(expr: &str) -> JobCronEntry {
        JobCronEntry::new(expr, crate::job::JobRequest::new("tick", Value::Null)).unwrap()
    }

    #[test]
    fn test_previous_cron_occurrence_finds_boundary_within_lookback() {
        let minutely = entry("* * * * *");
        assert_eq!(minutely.previous_occurrence(at("2026-01-01T00:05:07Z")).unwrap(), at("2026-01-01T00:05:00Z"));
        // A boundary exactly at `now` counts: the strictly-after `next`
        // computation would otherwise skip it forever.
        assert_eq!(minutely.previous_occurrence(at("2026-01-01T00:05:00Z")).unwrap(), at("2026-01-01T00:05:00Z"));
    }

    #[test]
    fn test_previous_cron_occurrence_finds_sparse_boundary_without_scanning() {
        let daily = entry("0 0 * * *");
        assert_eq!(daily.previous_occurrence(at("2026-01-01T12:00:00Z")).unwrap(), at("2026-01-01T00:00:00Z"));
    }
}

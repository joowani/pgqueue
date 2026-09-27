//! The Postgres-backed queue: connection, notifications, lifecycle
//! transitions, sweeping, and introspection.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde::Deserialize;
use serde_json::Value;
use sqlx::postgres::{PgListener, PgPool, PgPoolOptions};
use tokio::sync::{broadcast, mpsc, watch};
use uuid::Uuid;

use crate::Error;
#[cfg(feature = "_test")]
use crate::database::DatabaseEnqueueResult;
use crate::database::{Database, DatabaseConnectOptions};
#[cfg(feature = "_test")]
use crate::job::{EnqueueResult, JobRequest};
use crate::job::{JobFilter, JobRow, JobStatus, MIN_TIMESTAMPTZ, validate_dedupe_key};
use crate::sweeper::Sweeper;
use crate::worker::{WorkerFilter, WorkerInfo};

/// Current and retained job counts for one queue.
///
/// Read-only, and `#[non_exhaustive]` so a new gauge can be reported without a
/// breaking release.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, sqlx::FromRow)]
#[non_exhaustive]
pub struct QueueCounts {
    /// Jobs ready to run now.
    pub queued: i64,
    /// Jobs currently running or finishing abort cleanup.
    pub running: i64,
    /// Jobs queued for a future execution time.
    pub scheduled: i64,
    /// Retained jobs whose handler reported an error on their last allowed
    /// attempt.
    ///
    /// Not every exhausted job: an attempt the [`Sweeper`]
    /// recovers never reports a handler error, so when it was the last one the
    /// row finishes `aborted` with `error = "swept"` and is counted below
    /// instead. Alert on both, or a worker killed mid-attempt on its final try
    /// leaves this gauge flat.
    pub failed: i64,
    /// Retained jobs aborted before completion — by
    /// [`Queue::abort_job`], and by the [`Sweeper`] recovering an attempt that had no attempts
    /// left to retry with.
    pub aborted: i64,
    /// When the oldest job that is ready to run now became due, or `None`
    /// when nothing is ready. Its age is the queue's backlog latency: the
    /// counts above cannot tell a busy queue from a stalled one, and this
    /// can — alert on it growing, not on `queued` alone.
    #[sqlx(try_from = "crate::database::OptionalTimestamp")]
    pub oldest_ready_at: Option<jiff::Timestamp>,
}

/// Counters accumulated by this queue handle since start.
///
/// Read-only, and `#[non_exhaustive]` so a new counter can be reported without
/// a breaking release.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
#[non_exhaustive]
pub struct QueueStats {
    /// Jobs finished successfully.
    pub complete: u64,
    /// Jobs whose handler reported an error on their last allowed attempt.
    /// Exhausting the attempts by way of the [`Sweeper`] counts
    /// under [`QueueStats::aborted`] instead — see [`QueueCounts::failed`].
    pub failed: u64,
    /// Retries scheduled.
    pub retried: u64,
    /// Jobs aborted, including attempts the [`Sweeper`]
    /// recovered with no attempts left.
    pub aborted: u64,
}

/// The counters behind every [`QueueStats`] snapshot, shared by queue handles
/// and workers so the fields and their assembly exist exactly once.
#[derive(Default)]
pub(crate) struct QueueCounters {
    complete: AtomicU64,
    failed: AtomicU64,
    retried: AtomicU64,
    aborted: AtomicU64,
}

impl QueueCounters {
    pub(crate) fn record_complete(&self) {
        self.complete.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_failed(&self) {
        self.failed.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_retry(&self) {
        self.retried.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_abort(&self) {
        self.aborted.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self) -> QueueStats {
        QueueStats {
            complete: self.complete.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
            retried: self.retried.load(Ordering::Relaxed),
            aborted: self.aborted.load(Ordering::Relaxed),
        }
    }
}

/// A job-finished notification from this queue's completion channel.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct QueueDoneEvent {
    pub(crate) id: Uuid,
    pub(crate) status: JobStatus,
}

/// Completion waiters, keyed by the job id each one is waiting for.
///
/// A broadcast channel stood here, and it made every waiter pay for every other
/// waiter's job: each completion woke all of them, and the shared 256-event
/// buffer meant that past a few hundred concurrent waiters essentially every one
/// lost its event to `Lagged` and fell back to polling. Measured on a 12-core
/// host, resolution latency (the gap between the row's `completed_at` and the
/// waiter returning) went 6 ms at 10 waiters, 46 ms at 100, 262 ms at 500 and
/// 1815 ms at 3000 — for a feature whose contract is "results arrive promptly".
/// Raising the buffer moved the cliff without removing it, because the cost was
/// the fan-out itself: O(waiters x completions).
///
/// Keyed delivery makes a completion cost one map lookup and wakes only the
/// waiters that asked for that id. Nothing else subscribed to completions, so
/// this is the whole consumer set.
type QueueDoneWaiters = std::sync::Mutex<HashMap<Uuid, Vec<(u64, mpsc::Sender<QueueDoneEvent>)>>>;

/// One PostgreSQL listener fanned out to every subscriber on this queue handle.
pub(crate) struct QueueNotifyListener {
    wakeup: broadcast::Sender<()>,
    done: Arc<QueueDoneWaiters>,
    next_done_key: AtomicU64,
    health: watch::Sender<Option<String>>,
    task: tokio::task::JoinHandle<()>,
}

/// One waiter's registration in [`QueueNotifyListener`]'s completion map.
///
/// Deregisters on drop, so a wait that returns, times out, or is cancelled
/// leaves nothing behind — the map holds exactly the waiters currently waiting.
/// The key distinguishes registrations that share a job id, which is what lets
/// a drop remove precisely its own.
pub(crate) struct QueueDoneSubscription {
    waiters: Arc<QueueDoneWaiters>,
    id: Uuid,
    key: u64,
    receiver: mpsc::Receiver<QueueDoneEvent>,
}

impl QueueDoneSubscription {
    /// The next completion for this subscription's job id. It never ends: this subscription keeps the waiter map alive,
    /// and only its own drop removes its sender from the map, so the channel cannot close. A listener task that dies
    /// leaves it pending, and the waiter's polling fallback carries the wait.
    pub(crate) async fn recv(&mut self) -> QueueDoneEvent {
        match self.receiver.recv().await {
            Some(event) => event,
            None => std::future::pending().await,
        }
    }
}

impl Drop for QueueDoneSubscription {
    fn drop(&mut self) {
        let mut waiters = self.waiters.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(registrations) = waiters.get_mut(&self.id) {
            registrations.retain(|(key, _)| *key != self.key);
            if registrations.is_empty() {
                waiters.remove(&self.id);
            }
        }
    }
}

/// Reconnect delay bounds for the notification listener. The delay starts low
/// so a momentary blip (a terminated backend, a restart) is healed almost
/// immediately, doubles on every failed attempt so a long outage settles at
/// one connection attempt — and one warn — per cap interval instead of two per
/// second, and resets once a subscription is re-established. The cap costs
/// only push latency: while the listener is down, worker wakeups are carried
/// by `poll_interval` and result waits by [`crate::JobHandle::wait`]'s polling
/// fallback, both of which cover a gap of any length.
///
/// These bounds govern the loop, not the observed cadence, and for the outages
/// that matter most they are not what an operator will measure. sqlx retries
/// a refused connection *inside* `acquire` — `ConnectionRefused`,
/// `53300 too_many_connections` and `57P03 cannot_connect_now` are all treated
/// as transient — so a server that is down, saturated or still starting up
/// spends the pool's 30-second `acquire_timeout` before this loop sees a single
/// failure. Measured against a stopped server, successive warnings arrived
/// ~30s apart rather than at this 5s cap. The delays below dominate only for
/// errors sqlx surfaces immediately, such as a revoked `CONNECT`. Recovery is
/// correspondingly quicker than a strict backoff would give: sqlx's inner retry
/// picks the server up within a second or two of it answering again.
const LISTENER_RECONNECT_INITIAL_DELAY: Duration = Duration::from_millis(500);
const LISTENER_RECONNECT_MAX_DELAY: Duration = Duration::from_secs(5);

async fn connect_notify_listener(
    pool: &PgPool,
    notify_channel: &str,
    done_channel: &str,
) -> Result<PgListener, sqlx::Error> {
    let mut listener = PgListener::connect_with(pool).await?;
    listener.listen_all([notify_channel, done_channel]).await?;
    listener.eager_reconnect(false);
    Ok(listener)
}

impl QueueNotifyListener {
    /// Never fails: a listener that cannot connect yet starts disconnected and
    /// heals through the same reconnect loop that covers a listener lost later.
    /// Starting it any other way would make a momentary refusal — the dedicated
    /// LISTEN connection lives outside the query pool, so it can be refused
    /// while the pool is perfectly usable — permanent for this queue handle.
    pub(crate) fn start(database: &Database) -> Self {
        // LISTEN is held for this queue handle's lifetime. Keep it outside the
        // query pool so independently constructed queues cannot reserve every
        // slot of a shared pool. Lazy so construction cannot fail on a refused
        // connection.
        let pool =
            PgPoolOptions::new().max_connections(1).connect_lazy_with((*database.pool().connect_options()).clone());
        let notify_channel = database.notify_channel().to_string();
        let done_channel = database.done_channel().to_string();

        let (wakeup, _) = broadcast::channel(16);
        let done: Arc<QueueDoneWaiters> = Arc::new(std::sync::Mutex::new(HashMap::new()));
        let (health, _) = watch::channel(None);
        let queue_name = database.name().to_string();
        let wakeup_tx = wakeup.clone();
        let done_tx = Arc::clone(&done);
        let health_tx = health.clone();
        let task = tokio::spawn(async move {
            // `None` sends the loop straight to its reconnect arm, so the
            // first subscription and every later one share one code path.
            let mut listener = match connect_notify_listener(&pool, &notify_channel, &done_channel).await {
                Ok(listener) => Some(listener),
                Err(error) => {
                    health_tx.send_replace(Some(error.to_string()));
                    tracing::warn!(
                        queue = %queue_name, %error,
                        "notification listener unavailable at start; retrying in the background"
                    );
                    None
                }
            };
            let mut reconnect_delay = LISTENER_RECONNECT_INITIAL_DELAY;
            loop {
                // PgListener absorbs simple drops itself. A surfaced error
                // requires a fresh subscription; polling fallbacks cover
                // notifications lost while that subscription is rebuilt.
                let Some(active_listener) = listener.as_mut() else {
                    tokio::time::sleep(reconnect_delay).await;
                    match connect_notify_listener(&pool, &notify_channel, &done_channel).await {
                        Ok(reconnected) => {
                            reconnect_delay = LISTENER_RECONNECT_INITIAL_DELAY;
                            listener = Some(reconnected);
                            health_tx.send_replace(None);
                            let _ = wakeup_tx.send(());
                        }
                        Err(error) => {
                            reconnect_delay = (reconnect_delay * 2).min(LISTENER_RECONNECT_MAX_DELAY);
                            health_tx.send_replace(Some(error.to_string()));
                            tracing::warn!(
                                queue = %queue_name,
                                %error,
                                "notification listener reconnect failed"
                            );
                        }
                    }
                    continue;
                };
                match active_listener.try_recv().await {
                    Ok(Some(notification)) => {
                        health_tx.send_replace(None);
                        if notification.channel() == done_channel {
                            match serde_json::from_str::<QueueDoneEvent>(notification.payload()) {
                                Ok(event) => {
                                    // Only the waiters on this id, and never
                                    // blocking on a slow one: the capacity-1
                                    // channel already holds a completion for
                                    // that waiter, and a second event for one
                                    // job says nothing new.
                                    let registrations = done_tx
                                        .lock()
                                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                                        .get(&event.id)
                                        .map(|registrations| {
                                            registrations.iter().map(|(_, sender)| sender.clone()).collect::<Vec<_>>()
                                        })
                                        .unwrap_or_default();
                                    for sender in registrations {
                                        let _ = sender.try_send(event.clone());
                                    }
                                }
                                Err(error) => tracing::warn!(
                                    queue = %queue_name,
                                    %error,
                                    "malformed done notification"
                                ),
                            }
                        } else {
                            // Only the enqueue channel wakes fetchers. A completion never makes a row claimable, so
                            // waking on one had every idle worker on the queue run a claim to find nothing, per awaited
                            // completion.
                            let _ = wakeup_tx.send(());
                        }
                    }
                    Ok(None) => {
                        health_tx.send_replace(Some("notification listener disconnected".to_string()));
                        listener.take();
                        tracing::warn!(
                            queue = %queue_name,
                            "notification listener disconnected"
                        );
                    }
                    Err(error) => {
                        health_tx.send_replace(Some(error.to_string()));
                        listener.take();
                        tracing::warn!(queue = %queue_name, %error, "notification listener error");
                    }
                }
            }
        });

        Self { wakeup, done, next_done_key: AtomicU64::new(0), health, task }
    }

    pub(crate) fn subscribe_wakeup(&self) -> broadcast::Receiver<()> {
        self.wakeup.subscribe()
    }

    /// Registers interest in one job's completion. Callers must register before
    /// their first status read, so a finish landing in between cannot be missed.
    pub(crate) fn subscribe_done(&self, id: Uuid) -> QueueDoneSubscription {
        let (sender, receiver) = mpsc::channel(1);
        let key = self.next_done_key.fetch_add(1, Ordering::Relaxed);
        self.done.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).entry(id).or_default().push((key, sender));
        QueueDoneSubscription { waiters: Arc::clone(&self.done), id, key, receiver }
    }

    pub(crate) fn subscribe_health(&self) -> watch::Receiver<Option<String>> {
        self.health.subscribe()
    }
}

impl Drop for QueueNotifyListener {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// A handle to one named queue in the fixed `pgqueue` Postgres schema.
///
/// Cheap to clone (internally an `Arc`); clones share the connection pool and
/// stat counters. Obtain one with [`Queue::connect`] or [`Queue::builder`].
///
/// Connections must reach PostgreSQL directly or through a proxy in session
/// pooling mode. PGQueue uses a dedicated `LISTEN` session for wakeups and a
/// session-scoped advisory lock for sweeper leadership. PgBouncer transaction
/// and statement pooling do not preserve those guarantees and are unsupported.
#[derive(Clone)]
pub struct Queue {
    database: Arc<Database>,
}

/// Low-level consumer bound to one worker identity.
///
/// Most applications should use [`crate::Worker`]. This capability-oriented
/// API exists for custom consumers that need to run the queue protocol
/// themselves without passing forgeable row snapshots back to [`Queue`]. A
/// custom consumer must call [`Consumer::heartbeat`] before dequeueing and keep
/// that lease alive while attempts run. Without a live lease, an attempt becomes
/// sweepable a sweep grace past its last heartbeat — a timeout, however long,
/// buys no slack against that, since the two recovery triggers are additive.
///
/// The identity is one *incarnation*, not a stable name — see
/// [`Queue::consumer`] for the lifecycle rule and what reusing an id across
/// restarts does to crash recovery.
///
/// Aborts and timeouts are the consumer's own to observe: nothing here runs
/// the [`crate::Worker`]'s abort-polling loop, so a user abort or a sweeper
/// recovery that lands mid-attempt surfaces as a *refused transition* when the
/// attempt is finalized. The protocol is: [`Attempt::retry`] a failed attempt
/// (a sweeper recovery request becomes that retry); when `retry` answers
/// `false` or the attempt is not retryable, [`Attempt::finish`]; when a finish
/// as complete or failed answers `false`, acknowledge the abort by finishing
/// as aborted. A long-running handler that should *react* to aborts before it
/// ends must poll [`Queue::fetch_job`] itself — the row's `aborting` status,
/// `attempts` and `worker_id` are exactly what the worker's own poll reads —
/// and per-attempt timeouts are likewise the worker's feature: for an attempt
/// the consumer is still holding, the backstops are lease expiry and the
/// drop recovery on [`Attempt`] itself.
#[derive(Clone)]
pub struct Consumer {
    queue: Queue,
    worker_id: Uuid,
}

/// One dequeued attempt owned by a [`Consumer`].
///
/// Dropping an attempt that never settled — no [`Attempt::finish`] or
/// [`Attempt::retry`] answered `Ok(true)` — hands it to a background recovery
/// task rather than abandoning it: the consumer's own task may have panicked
/// or been cancelled while its heartbeat loop runs on, and a heartbeat is
/// precisely the assertion that every claimed attempt is still being worked,
/// so nothing else would ever reclaim an untimed one. The recovery requeues
/// the attempt (spending it, with the job's own retry delay) or, when no
/// attempts remain or an abort landed meanwhile, finishes it aborted; every
/// transition is guarded, so a row that already moved on is left alone.
/// Settling explicitly remains the contract — the drop path is the net under
/// it, not a way to finish work.
#[must_use = "an unsettled attempt is requeued by a background recovery task when dropped"]
pub struct Attempt {
    queue: Queue,
    row: JobRow,
    runtime: Option<tokio::runtime::Handle>,
    settled: std::sync::atomic::AtomicBool,
}

impl Consumer {
    /// The worker identity written onto dequeued attempts and heartbeats.
    pub fn worker_id(&self) -> Uuid {
        self.worker_id
    }

    /// Dequeues up to `limit` due jobs and returns guarded attempt capabilities.
    ///
    /// A live, accepting lease is required: call [`Consumer::heartbeat`] first
    /// and refresh it until every returned attempt has been finished or
    /// retried. Without one this claims nothing and returns an empty vector,
    /// because the sweeper would otherwise treat the claim as abandoned and
    /// hand the job to another consumer while it is still running.
    ///
    /// Claims are taken with `FOR UPDATE SKIP LOCKED`, so concurrent consumers
    /// never wait on each other: a dequeue either claims rows or reports none.
    pub async fn dequeue(&self, limit: i64) -> Result<Vec<Attempt>, Error> {
        let runtime = tokio::runtime::Handle::try_current().ok();
        Ok(self
            .queue
            .database
            .dequeue_consumer(limit, self.worker_id)
            .await?
            .into_iter()
            .map(|row| Attempt {
                queue: self.queue.clone(),
                row,
                runtime: runtime.clone(),
                settled: std::sync::atomic::AtomicBool::new(false),
            })
            .collect())
    }

    /// Upserts this consumer's worker lease and introspection metadata. Custom
    /// consumers must refresh it before `ttl` elapses while attempts are live.
    ///
    /// Renewing the lease asserts that every attempt claimed under this
    /// `worker_id` is still running in this process. That assertion is the
    /// whole reason recovery leaves a live lease's attempts alone, so it must
    /// be true: heartbeat an id only while this incarnation owns its claims —
    /// never a predecessor's id (see [`Queue::consumer`]).
    ///
    /// `ttl` must be greater than zero. A zero one writes a lease that has
    /// already expired by the time any later transaction reads it, and
    /// [`Consumer::dequeue`] requires a live lease — so every subsequent claim
    /// would come back empty, indistinguishable from an empty queue.
    ///
    /// `stats` and `metadata` must not nest containers more than 127 levels or
    /// serialize beyond 1 MiB. `serde_json` stops deserializing at 128, so a
    /// deeper document could not be read back.
    ///
    /// Neither may contain a NUL, which `jsonb` cannot store at all. Both are
    /// refused as [`Error::Config`], because a heartbeat loop is expected to
    /// retry a transient error and neither of these ever becomes storable: it
    /// would spin without renewing the lease until the sweeper reclaimed every
    /// attempt the caller has claimed.
    pub async fn heartbeat(&self, stats: Value, metadata: Option<Value>, ttl: Duration) -> Result<(), Error> {
        crate::job::validate_nonzero_duration("worker lease TTL", ttl)?;
        self.queue
            .database
            .write_worker_info(self.worker_id, stats, metadata, ttl, crate::database::LeaseIntake::Reopen)
            .await
    }
}

impl Attempt {
    /// The immutable job row snapshot returned by dequeue.
    pub fn job(&self) -> &JobRow {
        &self.row
    }

    /// Moves this attempt to a terminal state if it still owns the row.
    ///
    /// The capability is borrowed so callers can retry after a transient
    /// infrastructure error or apply a fallback after a refused transition.
    pub async fn finish(&self, status: JobStatus, result: Option<Value>, error: Option<&str>) -> Result<bool, Error> {
        let finished = self.queue.database.finish(&self.row, status, result, error).await?;
        if finished {
            self.settled.store(true, std::sync::atomic::Ordering::Release);
        }
        Ok(finished)
    }

    /// Requeues this failed attempt if it still owns the row and may retry.
    ///
    /// An attempt the sweeper marked for stuck-job recovery mid-flight is
    /// requeued the same way — the recovery request becomes this retry, with
    /// this error — so a consumer needs no separate transition for it. A
    /// *user* abort is never resurrected: `retry` refuses it, and the
    /// acknowledgment is [`Attempt::finish`] as aborted.
    ///
    /// The capability is borrowed so callers can retry after a transient
    /// infrastructure error, finish an exhausted final attempt as failed, or
    /// acknowledge an abort that landed mid-attempt by finishing as aborted.
    pub async fn retry(&self, error: &str) -> Result<bool, Error> {
        let retried = self.queue.database.retry(&self.row, error).await?;
        if retried {
            self.settled.store(true, std::sync::atomic::Ordering::Release);
        }
        Ok(retried)
    }
}

impl Drop for Attempt {
    fn drop(&mut self) {
        // Only a transition this capability *made* settles it. A refused one —
        // `Ok(false)` — deliberately does not: the refusal may mean the row is
        // an abort awaiting acknowledgment or an exhausted final attempt, and
        // if the caller drops instead of following the documented protocol,
        // the recovery task resolves it — its transitions carry the same
        // guards every write here does, so a row that genuinely moved on to
        // another attempt or a terminal state is a no-op.
        if !self.settled.load(std::sync::atomic::Ordering::Acquire) {
            self.queue.database.spawn_dropped_attempt_recovery(&self.row, self.runtime.clone());
        }
    }
}

impl std::fmt::Debug for Consumer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Consumer").field("queue", &self.queue.name()).field("worker_id", &self.worker_id).finish()
    }
}

impl std::fmt::Debug for Attempt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Attempt")
            .field("id", &self.row.id)
            .field("attempts", &self.row.attempts)
            .field("worker_id", &self.row.worker_id)
            .finish_non_exhaustive()
    }
}

/// Configures and connects a [`Queue`].
#[must_use = "a QueueBuilder does nothing until connected"]
pub struct QueueBuilder {
    url: String,
    pool: Option<PgPool>,
    name: String,
    max_connections: u32,
    min_connections: u32,
    priorities: (i16, i16),
    sweep_grace: Duration,
    sweep_batch_size: u32,
    migration_lock_timeout: Duration,
    notify_throttle: Option<Duration>,
}

impl QueueBuilder {
    /// Queue name; jobs are namespaced within the `pgqueue` schema. Names must be
    /// non-empty, at most 255 bytes, contain no control characters, and not be
    /// the dot segments `.` or `..`.
    /// Default `"default"`.
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    /// Use an existing pool instead of connecting from the URL. A lazily
    /// started notification listener opens one additional connection without
    /// occupying a slot in this pool.
    ///
    /// The pool must connect directly to PostgreSQL or through a session-mode
    /// proxy. Transaction and statement pooling are unsupported because the
    /// listener and sweeper leadership lock depend on PostgreSQL session state.
    pub fn pool(mut self, pool: PgPool) -> Self {
        self.pool = Some(pool);
        self
    }

    /// Pool sizing (ignored when [`QueueBuilder::pool`] is used). Defaults:
    /// 2..=10. Two connections live outside this pool, so a worker peaks at
    /// `max + 2` against the server's own `max_connections`: a lazily started
    /// notification listener, and — while this process holds sweep leadership —
    /// the connection carrying the session-scoped advisory lock, which is taken
    /// from the pool and then detached, freeing the pool slot but not the
    /// server's.
    ///
    /// Sizing this past what the server will grant fails late and opaquely.
    /// PostgreSQL answers a refused connection with `53300 too_many_connections`,
    /// which sqlx classifies as transient and retries for the pool's whole
    /// 30-second acquire timeout before giving up — so exhausting a role's
    /// `CONNECTION LIMIT` (or the server's `max_connections`) makes
    /// [`QueueBuilder::connect`] block for that long and then return
    /// `Error::Db(sqlx::Error::PoolTimedOut)`, which names neither the role nor
    /// the limit. Size `max + 2` per process against the limit rather than
    /// diagnosing it from that error.
    pub fn connections(mut self, min: u32, max: u32) -> Self {
        self.min_connections = min;
        self.max_connections = max;
        self
    }

    /// Restrict dequeues from this handle to a priority range (inclusive).
    /// Default: all priorities.
    pub fn priorities(mut self, low: i16, high: i16) -> Self {
        self.priorities = (low, high);
        self
    }

    /// How long the sweeper waits past each recovery trigger before declaring
    /// an attempt stuck, giving its worker a window to finalize normally.
    /// Default 5s.
    ///
    /// It applies to both triggers: past a job's `timeout`, and past the expiry
    /// of the `pgqueue.workers` lease that covers the attempt. The second is
    /// what absorbs a heartbeat that stalled without the worker dying, so raise
    /// this on deployments where a lock wait, a pool stall or a GC pause can
    /// outlast a worker's lease TTL — otherwise a still-running attempt is
    /// cancelled and re-run. Expired worker leases are purged only once they
    /// have been expired for *twice* this long, and that purge is what answers
    /// the owner-gone question below.
    ///
    /// It sizes the cooperative abort window too. The sweeper marks a stuck
    /// attempt `aborting` and then leaves its owner one more grace to notice and
    /// end the attempt itself before taking the row away; only an owner whose
    /// lease row is *gone* — purged after twice this long, or never written — is
    /// treated as gone rather than merely stalled. So this is the single knob
    /// for how much unresponsiveness a still-running attempt survives.
    ///
    /// Must be non-zero: zero leaves no cushion at all, so connecting rejects it.
    pub fn sweep_grace(mut self, grace: Duration) -> Self {
        self.sweep_grace = grace;
        self
    }

    /// Maximum rows handled by one bounded sweeper operation. Default 500.
    pub fn sweep_batch_size(mut self, size: u32) -> Self {
        self.sweep_batch_size = size;
        self
    }

    /// Rate-limits the wakeup notifications this handle's enqueues emit to
    /// one per `window`. Default: none, every due enqueue notifies.
    ///
    /// Every `NOTIFY` is a cluster-wide serialization point: PostgreSQL
    /// commits notifying transactions one at a time, holding one lock through
    /// each commit's WAL flush, so a fleet of producers enqueueing at speed
    /// degrades to one durable commit at a time however many connections it
    /// has. A throttle caps how many of those commits this process asks for.
    ///
    /// What it costs is wakeup latency, not delivery: a job enqueued inside a
    /// window after a worker went idle is claimed at that worker's next
    /// [`WorkerBuilder::poll_interval`](crate::WorkerBuilder::poll_interval)
    /// rather than at once. Workers that are busy are unaffected, because a
    /// fetcher with work re-polls on its own. So this suits bulk producers,
    /// and [`Queue::enqueue_batch`] — one notification per batch — usually
    /// suits them better still. The throttle is per handle: it counts this
    /// process's enqueues, not the queue's, and it spends its slot when an
    /// enqueue is attempted, so a deduplicated or failed one still consumes
    /// the window. A throttled handle also decides on its own clock whether a
    /// scheduled row is due, so a row due within a clock skew of now may wake
    /// nobody; without a throttle that decision is the server's. Zero
    /// disables it.
    pub fn notify_throttle(mut self, window: Duration) -> Self {
        self.notify_throttle = Some(window);
        self
    }

    /// Maximum time schema initialization waits for a PostgreSQL lock. Default
    /// 30 seconds. Connecting with a current migration history does not take
    /// the migrator's advisory lock. The previous session setting is restored
    /// before a caller-supplied pool connection is returned.
    pub fn migration_lock_timeout(mut self, timeout: Duration) -> Self {
        self.migration_lock_timeout = timeout;
        self
    }

    /// Connects, verifies the server is PostgreSQL 18+, and applies missing
    /// PGQueue migrations.
    ///
    /// A current migration history is checked without DDL or the migrator's
    /// advisory lock. Applying a migration needs DDL privileges. A deployment
    /// with a restricted application role can run the published
    /// `pgqueue-migrate` command with a schema-owner role before starting the
    /// application. Migration history does not detect or repair tables, indexes,
    /// or other objects changed manually after a migration ran.
    ///
    /// # Durability
    ///
    /// At-least-once delivery is a property of the *database*, not of this
    /// crate: it holds exactly as far as PostgreSQL's commit durability does.
    /// Under `synchronous_commit = off` a commit is acknowledged before its WAL
    /// record reaches disk, so a crash loses jobs this queue already reported as
    /// enqueued — measured at 3,263 acknowledged commits lost to one
    /// `SIGKILL` — and `fsync = off` extends that to arbitrary corruption. Both
    /// are legitimate throughput trade-offs and neither is refused here, but a
    /// deployment that wants the delivery guarantee must leave
    /// `synchronous_commit` at `on` (or `remote_apply`/`remote_write`).
    ///
    /// This repository's own `compose.yaml` sets `fsync=off`,
    /// `synchronous_commit=off` and `full_page_writes=off` deliberately, to make
    /// the test suite fast. That database is not durable, and a crash-recovery
    /// test written against it proves nothing unless it overrides the setting
    /// first.
    pub async fn connect(self) -> Result<Queue, Error> {
        Ok(Queue {
            database: Arc::new(
                Database::connect(DatabaseConnectOptions {
                    url: self.url,
                    pool: self.pool,
                    name: self.name,
                    priorities: self.priorities,
                    sweep_grace: self.sweep_grace,
                    sweep_batch_size: self.sweep_batch_size,
                    max_connections: self.max_connections,
                    min_connections: self.min_connections,
                    migration_lock_timeout: self.migration_lock_timeout,
                    notify_throttle: self.notify_throttle,
                })
                .await?,
            ),
        })
    }
}

impl Queue {
    /// Connects to queue `default` in the `pgqueue` schema and applies missing
    /// migrations. Use [`Queue::builder`] to customize the queue or pool. The
    /// URL must reach PostgreSQL directly or through a session-mode proxy; see
    /// [`Queue`] for why transaction and statement pooling are unsupported.
    pub async fn connect(url: &str) -> Result<Queue, Error> {
        Queue::builder(url).connect().await
    }

    /// Starts configuring a queue connection.
    pub fn builder(url: &str) -> QueueBuilder {
        QueueBuilder {
            url: url.to_string(),
            pool: None,
            name: "default".to_string(),
            max_connections: 10,
            min_connections: 2,
            priorities: (i16::MIN, i16::MAX),
            sweep_grace: Duration::from_secs(5),
            sweep_batch_size: 500,
            migration_lock_timeout: Duration::from_secs(30),
            notify_throttle: None,
        }
    }

    /// This queue's name.
    pub fn name(&self) -> &str {
        self.database.name()
    }

    /// The underlying connection pool.
    pub fn pool(&self) -> &PgPool {
        self.database.pool()
    }

    /// Creates a low-level consumer bound to `worker_id`.
    ///
    /// `worker_id` names one *incarnation* of a consumer, not a stable service
    /// identity: mint a fresh one — `Uuid::now_v7()` — for every process start,
    /// exactly as [`Worker`](crate::Worker) mints its own per build, and never
    /// run two consumers under one id. The lease this id heartbeats is what
    /// tells crash recovery its attempts are still running, and the lease knows
    /// only the id: a restart that reuses its predecessor's id renews the
    /// predecessor's lease too, asserting attempts the new process has no
    /// [`Attempt`] for are alive. Those attempts then never meet the dead-owner
    /// recovery trigger, and one with its timeout disabled is stranded
    /// `running` for as long as the reused id keeps heartbeating.
    pub fn consumer(&self, worker_id: Uuid) -> Consumer {
        Consumer { queue: self.clone(), worker_id }
    }

    pub(crate) fn database(&self) -> &Database {
        &self.database
    }

    /// A shared handle for the few callers that must outlive this `Queue`
    /// borrow; everything on a request or dequeue path wants [`Self::database`].
    pub(crate) fn database_handle(&self) -> Arc<Database> {
        Arc::clone(&self.database)
    }

    /// The lazily-started notification listener for this queue handle. The first
    /// caller opens one LISTEN connection outside the query pool; enqueue-only
    /// processes never pay for it.
    pub(crate) fn notify_listener(&self) -> &QueueNotifyListener {
        self.database.notify_listener()
    }

    /// Enqueues an untyped job. Only this crate's own integration tests reach
    /// this: the supported API is the typed `#[pgqueue::job]` path, so every
    /// job name is a compile-time constant — which is what lets a worker
    /// register every name enqueued on its queue. Tests use it to drive the
    /// enqueue SQL with configurations the macro would otherwise have to
    /// produce one generated type at a time.
    ///
    /// A dedupe-key collision returns the existing live job's id.
    #[cfg(feature = "_test")]
    pub async fn enqueue_raw(&self, job: JobRequest) -> Result<EnqueueResult<Uuid>, Error> {
        raw_enqueue_result(self.database.enqueue_raw_delayed_result(job, None, false).await?)
    }

    /// Enqueues an untyped job inside a caller-owned transaction. Test-only,
    /// as [`Queue::enqueue_raw`] is.
    ///
    /// The row and notification become visible only when the caller commits.
    /// Dedupe-key advisory locks remain held until that commit — except inside
    /// a savepoint, where `ROLLBACK TO SAVEPOINT` releases the lock together
    /// with the row it guarded, so the lock still covers exactly the decision
    /// it was taken for.
    ///
    /// PostgreSQL's default `READ COMMITTED` isolation is required to observe a
    /// dedupe-key owner that commits while this call waits for its lock. At
    /// `REPEATABLE READ` or `SERIALIZABLE`, retry the whole transaction if such
    /// a concurrent owner is outside the caller's snapshot.
    #[cfg(feature = "_test")]
    pub async fn enqueue_raw_in(
        &self,
        transaction: &mut sqlx::PgTransaction<'_>,
        job: JobRequest,
    ) -> Result<EnqueueResult<Uuid>, Error> {
        raw_enqueue_result(self.database.enqueue_raw_delayed_in_result(transaction, job, None, false).await?)
    }

    /// Requests an abort. Queued jobs finish as `aborted` immediately; running
    /// jobs move to `aborting` and are canceled by their worker's abort loop.
    /// A job the sweeper has already marked for stuck-job recovery is claimed
    /// the same way: the pending recovery retry becomes this abort, so the job
    /// finishes `aborted` instead of running again.
    /// Queued jobs with delete-immediately retention remain observable until
    /// the next sweep so result waiters can resolve the aborted result.
    /// Returns `false` if the job wasn't queued or running (it is terminal,
    /// missing, or an abort is already pending).
    pub async fn abort_job(&self, job_id: Uuid, reason: &str) -> Result<bool, Error> {
        self.database.abort(job_id, reason).await
    }

    /// Deletes up to `limit` queued jobs from this queue, oldest due first,
    /// and returns how many were deleted. `name` narrows the purge to one job
    /// name. Loop until it returns zero to empty the queue.
    ///
    /// An operator's tool for the enqueue that should not have happened — a
    /// bad deploy's fan-out, a runaway producer — rather than part of any
    /// normal flow: purged jobs simply vanish, with no terminal row, no
    /// completion notification, and no attempt spent. A caller waiting on one
    /// observes it as a missing job. Scheduled jobs are queued jobs and are
    /// purged too; running attempts are never touched, and a row a claim is
    /// locking at that instant is skipped rather than waited for.
    pub async fn purge_queued_jobs(&self, name: Option<&str>, limit: u32) -> Result<u64, Error> {
        self.database.purge_queued_jobs(name, limit).await
    }

    /// Creates a fresh occurrence of a terminal job with one more attempt.
    /// The terminal row remains unchanged so existing handles keep observing
    /// its result. A terminal occurrence can be retried once; returns `false`
    /// if it is not terminal, was already retried, or its dedupe key already
    /// belongs to a live occurrence.
    ///
    /// A cron occurrence is retried *without* the schedule's dedupe key — carrying it would
    /// collide with the schedule loop's next occurrence and refuse the retry — so the rerun is a
    /// keyless one-off outside the cron non-overlap guarantee: it can run beside a live scheduled
    /// occurrence, and the schedule's own cadence is unaffected.
    ///
    /// ```no_run
    /// # use pgqueue::{Error, Queue};
    /// # use uuid::Uuid;
    /// # async fn retry(queue: &Queue, id: Uuid) -> Result<(), Error> {
    /// let enqueued = queue.retry_job(id, "manual retry").await?;
    /// assert!(enqueued);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn retry_job(&self, id: Uuid, reason: &str) -> Result<bool, Error> {
        Ok(self.retry_job_occurrence(id, reason).await?.is_some())
    }

    /// Creates a fresh occurrence of a terminal job and returns its new ID.
    ///
    /// Unlike [`Queue::retry_job`], this exposes the new occurrence so callers
    /// can fetch or wait on it. Returns `None` under the same conditions that
    /// make `retry_job` return `false`.
    ///
    /// ```no_run
    /// # use pgqueue::{Error, Queue};
    /// # use uuid::Uuid;
    /// # async fn retry(queue: &Queue, failed_id: Uuid) -> Result<(), Error> {
    /// if let Some(retry_id) = queue
    ///     .retry_job_occurrence(failed_id, "manual retry")
    ///     .await?
    /// {
    ///     let retry = queue.fetch_job(retry_id).await?;
    ///     assert!(retry.is_some());
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn retry_job_occurrence(&self, id: Uuid, reason: &str) -> Result<Option<Uuid>, Error> {
        self.database.retry_job_occurrence(id, reason).await
    }

    /// Removes one durable cron schedule from this queue.
    ///
    /// Existing jobs and occurrence history are left alone. A running worker
    /// that still registers the same key will recreate the schedule during its
    /// next reconciliation pass, so remove the registration from the deployed
    /// worker before calling this method.
    pub async fn remove_cron_schedule(&self, dedupe_key: &str) -> Result<bool, Error> {
        validate_dedupe_key(dedupe_key)?;
        self.database.remove_cron_schedule(dedupe_key).await
    }

    /// Fetches one job by its job ID.
    pub async fn fetch_job(&self, job_id: Uuid) -> Result<Option<JobRow>, Error> {
        self.database.job(job_id).await
    }

    /// Lists jobs for this queue, newest first, with optional filters.
    ///
    /// The page *order* is index-backed; the `status` and `name` filters are
    /// not. They are applied to rows already in that order, so a filter whose
    /// matches sit far down the ordering reads every newer row first: the cost
    /// of a filtered page grows with retention rather than with the page size.
    /// The alternative is two more indexes on the queue's hot table, which
    /// every enqueue and every attempt state change would pay for; the
    /// dashboard does not need them, because its own listing pages through a
    /// kind-qualified strategy that the existing indexes serve in full.
    pub async fn jobs_page(&self, filter: JobFilter) -> Result<Vec<JobRow>, Error> {
        let limit = filter.limit()?;
        // The same boundary every other caller-supplied string gets: PostgreSQL
        // `text` cannot hold a NUL, so left to reach the query it came back as
        // `Error::Db(22021)` — a transient-looking answer to a permanently
        // invalid filter — where `Error::Config` says what is actually wrong.
        if filter.name.as_deref().is_some_and(|name| name.contains('\0')) {
            return Err(Error::Config("job name filter must not contain NUL".into()));
        }
        // The same floor `JobRequest::validate` and the dashboard's cursors
        // hold: `Timestamp` reaches ISO year -9999, below what `timestamptz`
        // can represent, and a deserialized cursor carrying such an instant
        // came back as `Error::Db(22008)` instead of naming the bad input.
        if filter.before.is_some_and(|cursor| cursor.enqueued_at < crate::job::MIN_TIMESTAMPTZ) {
            return Err(Error::Config("job page cursor is below PostgreSQL's supported timestamp range".into()));
        }
        let before = filter.before;
        self.database.jobs_page(filter.status.map(JobStatus::as_str), filter.name.as_deref(), limit, before).await
    }

    /// Current queued/running/scheduled and retained failure counts.
    ///
    /// Each counter is index-served, so the cost tracks the rows actually
    /// counted — live, failed and aborted jobs — rather than the queue's total
    /// retained history. Retained `complete` rows are never read.
    ///
    /// The `pg_monitor` role can read every PGQueue table, so a metrics
    /// exporter needs no custom grant.
    pub async fn counts(&self) -> Result<QueueCounts, Error> {
        self.database.counts().await
    }

    /// Lists workers with unexpired leases, oldest first.
    ///
    /// Each worker's `stats` and `metadata` documents are returned whole, but
    /// each document is capped at 1 MiB when written and each page contains at
    /// most 100 workers. Build the next cursor from the last returned worker.
    pub async fn workers_page(&self, filter: WorkerFilter) -> Result<Vec<WorkerInfo>, Error> {
        let limit = filter.limit()?;
        if filter.after.is_some_and(|cursor| cursor.started_at < MIN_TIMESTAMPTZ) {
            return Err(Error::Config("worker page cursor is below PostgreSQL's supported timestamp range".into()));
        }
        self.database.workers_page(limit, filter.after).await
    }

    /// Counters accumulated by this handle since creation.
    pub fn stats(&self) -> QueueStats {
        self.database.stats()
    }

    /// Creates a sweeper for this queue. At most one sweeper per queue is
    /// running across all processes (advisory-lock leadership); the rest no-op.
    pub fn sweeper(&self) -> Sweeper {
        self.database.sweeper()
    }
}

#[cfg(feature = "_test")]
fn raw_enqueue_result(result: DatabaseEnqueueResult) -> Result<EnqueueResult<Uuid>, Error> {
    match result {
        DatabaseEnqueueResult::Inserted(id) => Ok(EnqueueResult::Enqueued(id)),
        DatabaseEnqueueResult::Deduplicated { id, .. } => Ok(EnqueueResult::Deduplicated(id)),
    }
}

impl std::fmt::Debug for Queue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Queue").field("name", &self.database.name()).finish_non_exhaustive()
    }
}

//! End-to-end worker tests: processing, retries, timeouts, panics, aborts,
//! burst mode, and graceful shutdown against real Postgres.

use std::future::Future;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::{
    DEQUEUE_CLAIM, DEQUEUE_PROBE, FINISH_STATEMENT, Stats, expire_worker, hold_gate, install_statement_gate,
    leased_consumer, new_job, test_timers, wait_for_advisory_waiter, wait_for_lock_waiter,
};
use crate::{
    EnqueueResultTestExt, QueueProtocolTestExt, TestDb, backdate_job_liveness, list_workers, pool_with_max,
    wait_for_some, wait_for_worker_intake_closed, wait_until,
};
use pgqueue::Sweeper;
use pgqueue::{
    Error, JobContext, JobError, JobErrorKind, JobRequest, JobRetention, JobState, JobStatus, Queue, Worker,
    WorkerBuilder, WorkerComponent, WorkerHealthStatus, WorkerTimers,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::{Connection, PgConnection, PgPool, Row};
use std::sync::atomic::AtomicUsize;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// Shared, clonable log of what handlers saw.
type Log = Arc<Mutex<Vec<String>>>;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Payload {
    tag: String,
}

#[pgqueue::job(max_attempts = 1)]
async fn record(args: Payload, log: JobState<Log>, ctx: JobContext) -> anyhow::Result<String> {
    // Exercise the full JobContext surface.
    anyhow::ensure!(ctx.attempt() >= 1);
    anyhow::ensure!(ctx.worker_id() != Uuid::nil());
    anyhow::ensure!(ctx.queue().name() == "default");
    anyhow::ensure!(!ctx.cancellation().is_cancelled());
    anyhow::ensure!(ctx.job().name == "record");
    anyhow::ensure!(format!("{ctx:?}").contains("record"));
    log.0.lock().map_err(|_| anyhow::anyhow!("record log lock poisoned"))?.push(args.tag.clone());
    Ok(format!("done:{}", args.tag))
}

#[pgqueue::job(max_attempts = 2)]
async fn always_fails(_: ()) -> anyhow::Result<()> {
    anyhow::bail!("boom")
}

#[pgqueue::job(name = "registration_collision")]
async fn registration_collision_a(_: ()) {}

#[pgqueue::job(name = "registration_collision")]
async fn registration_collision_b(_: ()) {}

#[pgqueue::job(max_attempts = 1, timeout_ms = 200)]
async fn sleeps_forever(_: ()) -> anyhow::Result<()> {
    std::future::pending().await
}

#[pgqueue::job(max_attempts = 1, timeout_ms = 30_000)]
async fn slow_but_abortable(_: ()) -> anyhow::Result<()> {
    std::future::pending().await
}

#[derive(Clone)]
struct ShutdownLeaseProbe {
    started: Arc<tokio::sync::Notify>,
}

#[pgqueue::job(max_attempts = 1, timeout_ms = 30_000)]
async fn holds_during_shutdown(_: (), probe: JobState<ShutdownLeaseProbe>) -> anyhow::Result<()> {
    probe.0.started.notify_one();
    std::future::pending().await
}

#[derive(Clone)]
struct SlotFillProbe {
    barrier: Arc<tokio::sync::Barrier>,
}

#[pgqueue::job]
async fn fills_worker_slot(_: (), probe: JobState<SlotFillProbe>) -> anyhow::Result<()> {
    probe.0.barrier.wait().await;
    Ok(())
}

#[pgqueue::job(max_attempts = 1)]
async fn cancels_observer_token(_: (), ctx: JobContext) -> anyhow::Result<()> {
    let cancellation = ctx.cancellation();
    cancellation.cancel();
    anyhow::ensure!(cancellation.is_cancelled());
    Ok(())
}

#[derive(Clone)]
struct CancellationObservedProbe {
    started: Arc<tokio::sync::Notify>,
    observed: Arc<tokio::sync::Notify>,
}

#[pgqueue::job(max_attempts = 1, timeout_ms = 30_000)]
async fn observes_shutdown_cancellation(
    _: (),
    probe: JobState<CancellationObservedProbe>,
    ctx: JobContext,
) -> anyhow::Result<()> {
    probe.0.started.notify_one();
    let cancellation = ctx.cancellation();
    cancellation.cancelled().await;
    probe.0.observed.notify_one();
    Ok(())
}

/// Reacts to shutdown the way [`JobContext::cancellation`] documents, reporting
/// the unfinished attempt as explicitly aborted.
#[pgqueue::job(max_attempts = 1, timeout_ms = 30_000)]
async fn errs_on_shutdown_cancellation(
    _: (),
    probe: JobState<CancellationObservedProbe>,
    ctx: JobContext,
) -> Result<(), JobError> {
    probe.0.started.notify_one();
    ctx.cancellation().cancelled().await;
    probe.0.observed.notify_one();
    Err(JobError::new(JobErrorKind::Aborted, "stopped for shutdown"))
}

/// Produces a genuine handler failure after shutdown has begun. Waiting for the
/// token makes the overlap deterministic; the error is deliberately not an
/// `Aborted` classification, so shutdown must not rewrite its meaning.
#[pgqueue::job(max_attempts = 1, timeout_ms = 30_000)]
async fn fails_during_shutdown(
    _: (),
    probe: JobState<CancellationObservedProbe>,
    ctx: JobContext,
) -> anyhow::Result<()> {
    probe.0.started.notify_one();
    ctx.cancellation().cancelled().await;
    probe.0.observed.notify_one();
    anyhow::bail!("cleanup failed")
}

#[derive(Clone)]
struct HandlerTickProbe {
    started: Arc<tokio::sync::Notify>,
    ticks: Arc<AtomicU32>,
}

/// Never looks at its cancellation token: only a forced stop ends it. Each tick
/// stands in for an externally visible side effect.
#[pgqueue::job(max_attempts = 1, timeout_ms = 60_000)]
async fn ticks_until_forced_to_stop(_: (), probe: JobState<HandlerTickProbe>) -> anyhow::Result<()> {
    probe.0.started.notify_one();
    loop {
        tokio::time::sleep(Duration::from_millis(20)).await;
        probe.0.ticks.fetch_add(1, Ordering::SeqCst);
    }
}

#[derive(Clone)]
struct CooperativeTickProbe {
    started: Arc<tokio::sync::Notify>,
    cancelled: Arc<tokio::sync::Notify>,
    ticks: Arc<AtomicU32>,
}

/// Reports the cooperative cancellation a user abort raises and then keeps
/// working through it, so only a forced stop ends it. Each tick stands in for
/// an externally visible side effect.
#[pgqueue::job(max_attempts = 1, timeout_ms = 60_000)]
async fn ticks_through_a_cooperative_abort(
    _: (),
    probe: JobState<CooperativeTickProbe>,
    ctx: JobContext,
) -> anyhow::Result<()> {
    probe.0.started.notify_one();
    let cancellation = ctx.cancellation();
    let mut reported = false;
    loop {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if !reported && cancellation.is_cancelled() {
            reported = true;
            probe.0.cancelled.notify_one();
        }
        probe.0.ticks.fetch_add(1, Ordering::SeqCst);
    }
}

#[derive(Clone)]
struct AbortCleanupProbe {
    started: Arc<tokio::sync::Notify>,
    observed: Arc<tokio::sync::Notify>,
    cleaned: Arc<tokio::sync::Notify>,
}

#[pgqueue::job(max_attempts = 1, timeout_ms = 30_000)]
async fn cleans_up_after_user_abort(_: (), probe: JobState<AbortCleanupProbe>, ctx: JobContext) -> anyhow::Result<()> {
    probe.0.started.notify_one();
    let cancellation = ctx.cancellation();
    cancellation.cancelled().await;
    probe.0.observed.notify_one();
    tokio::time::sleep(Duration::from_millis(25)).await;
    probe.0.cleaned.notify_one();
    Ok(())
}

#[derive(Clone)]
struct ForcedAbortProbe {
    started: Arc<tokio::sync::Notify>,
    observed: Arc<tokio::sync::Notify>,
    dropped: Arc<tokio::sync::Notify>,
}

struct ForcedAbortDrop(Arc<tokio::sync::Notify>);

impl Drop for ForcedAbortDrop {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}

#[pgqueue::job(max_attempts = 1, timeout_ms = 30_000)]
async fn exceeds_abort_grace(_: (), probe: JobState<ForcedAbortProbe>, ctx: JobContext) -> anyhow::Result<()> {
    let _drop = ForcedAbortDrop(probe.0.dropped.clone());
    probe.0.started.notify_one();
    let cancellation = ctx.cancellation();
    cancellation.cancelled().await;
    probe.0.observed.notify_one();
    std::future::pending().await
}

#[pgqueue::job(max_attempts = 1)]
async fn panics(_: ()) -> anyhow::Result<()> {
    panic!("kaboom {}", 42); // String panic payload
}

#[pgqueue::job(max_attempts = 1)]
async fn panics_static(_: ()) -> anyhow::Result<()> {
    panic!("static kaboom"); // &'static str panic payload
}

#[pgqueue::job(max_attempts = 1)]
async fn panics_weird(_: ()) -> anyhow::Result<()> {
    std::panic::panic_any(42u32); // non-string panic payload
}

#[pgqueue::job(max_attempts = 5)]
async fn needs_missing_state(_: (), missing: JobState<Uuid>) -> anyhow::Result<()> {
    let _ = missing;
    Ok(())
}

#[pgqueue::job(max_attempts = 5)]
async fn decodes_payload(args: Payload) -> anyhow::Result<()> {
    let _ = args;
    Ok(())
}

#[pgqueue::job(max_attempts = 5)]
async fn returns_decode_error(_: ()) -> Result<(), JobError> {
    Err(JobError::new(JobErrorKind::Decode, "handler decode failure"))
}

#[pgqueue::job]
async fn counts(_: (), counter: JobState<Arc<AtomicU32>>) -> anyhow::Result<u32> {
    Ok(counter.0.fetch_add(1, Ordering::SeqCst) + 1)
}

#[pgqueue::job(max_attempts = 2, timeout_ms = 30_000)]
async fn swept_once(_: (), attempts: JobState<Arc<AtomicU32>>) -> anyhow::Result<()> {
    if attempts.0.fetch_add(1, Ordering::SeqCst) == 0 {
        std::future::pending::<()>().await;
    }
    Ok(())
}

#[derive(Clone)]
struct MissingRowProbe {
    started: tokio::sync::mpsc::UnboundedSender<u8>,
    dropped: tokio::sync::mpsc::UnboundedSender<u8>,
}

struct MissingRowDrop {
    tag: u8,
    dropped: tokio::sync::mpsc::UnboundedSender<u8>,
}

impl Drop for MissingRowDrop {
    fn drop(&mut self) {
        let _ = self.dropped.send(self.tag);
    }
}

/// A minute's timeout, so the worker's own deadline never ends it inside a test: only recovery — the sweeper, or the
/// row deleted — can.
#[pgqueue::job(max_attempts = 1, timeout_ms = 60_000)]
async fn waits_after_row_deletion(tag: u8, probe: JobState<MissingRowProbe>) -> anyhow::Result<()> {
    let _drop = MissingRowDrop { tag, dropped: probe.0.dropped.clone() };
    let _ = probe.0.started.send(tag);
    std::future::pending().await
}

#[derive(Clone)]
struct AbortFailureRace {
    started: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

#[derive(Clone)]
struct AbortSuccessRace {
    started: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

#[pgqueue::job(max_attempts = 1, timeout_ms = 30_000)]
async fn succeeds_during_abort(_: (), state: JobState<AbortSuccessRace>) -> anyhow::Result<String> {
    state.0.started.notify_one();
    state.0.release.notified().await;
    Ok("handler finished".to_string())
}

#[pgqueue::job(max_attempts = 1, timeout_ms = 30_000)]
async fn succeeds_as_sweeper_marks(_: (), state: JobState<AbortSuccessRace>) -> anyhow::Result<()> {
    state.0.started.notify_one();
    state.0.release.notified().await;
    Ok(())
}

#[pgqueue::job(max_attempts = 1, timeout_ms = 30_000)]
async fn fails_during_abort(_: (), state: JobState<AbortFailureRace>) -> anyhow::Result<()> {
    state.0.started.notify_one();
    state.0.release.notified().await;
    anyhow::bail!("handler failed while abort was pending")
}

#[derive(Clone)]
struct SweepFailureRace {
    started: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

#[pgqueue::job(max_attempts = 2, timeout_ms = 30_000)]
async fn fails_as_sweeper_marks(_: (), state: JobState<SweepFailureRace>, ctx: JobContext) -> anyhow::Result<()> {
    if ctx.attempt() == 1 {
        state.0.started.notify_one();
        state.0.release.notified().await;
        anyhow::bail!("handler failed as sweep abort landed");
    }
    Ok(())
}

#[derive(Clone)]
struct SweepFailureReasonRace {
    started: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
    retried: Arc<tokio::sync::Notify>,
    hold: Arc<tokio::sync::Notify>,
}

/// Fails the first attempt on cue, then parks the second so the row the retry
/// produced can be read while it still carries the reason the first one ended.
#[pgqueue::job(max_attempts = 3, timeout_ms = 30_000)]
async fn fails_then_parks_as_sweeper_marks(
    _: (),
    state: JobState<SweepFailureReasonRace>,
    ctx: JobContext,
) -> anyhow::Result<()> {
    if ctx.attempt() == 1 {
        state.0.started.notify_one();
        state.0.release.notified().await;
        anyhow::bail!("the real handler failure");
    }
    state.0.retried.notify_one();
    state.0.hold.notified().await;
    Ok(())
}

fn test_worker(queue: Queue) -> WorkerBuilder {
    Worker::builder(queue)
        .timers(test_timers())
        .poll_interval(Duration::from_millis(50))
        .abort_grace(Duration::from_millis(100))
        .shutdown_grace(Duration::from_secs(5))
}

/// Keeps recovery out of tests that isolate the user-abort path. The default one-millisecond sweep grace can let a
/// saturated runner mistake a delayed test heartbeat for a dead worker and settle the row before the abort reaches it.
async fn build_abort_test_db(pool: PgPool) -> TestDb {
    TestDb::with(pool, |builder| builder.sweep_grace(Duration::from_secs(60))).await
}

async fn wait_for_worker_lease(queue: &Queue, worker_id: Uuid) {
    wait_for_some(Duration::from_secs(5), Duration::from_millis(10), "worker did not register its lease", || async {
        list_workers(queue).await.iter().any(|worker| worker.id == worker_id).then_some(())
    })
    .await;
}

/// Polls until the job reaches a terminal status (or the deadline passes).
async fn wait_terminal(queue: &Queue, id: Uuid, secs: u64) -> pgqueue::JobRow {
    wait_for_some(Duration::from_secs(secs), Duration::from_millis(25), &format!("job {id} never finished"), || async {
        queue.fetch_job(id).await.unwrap().filter(|row| row.status.is_terminal())
    })
    .await
}

async fn wait_status(queue: &Queue, id: Uuid, status: JobStatus, secs: u64) -> pgqueue::JobRow {
    wait_for_some(
        Duration::from_secs(secs),
        Duration::from_millis(25),
        &format!("job {id} never reached {status}"),
        || async { queue.fetch_job(id).await.unwrap().filter(|row| row.status == status) },
    )
    .await
}

#[sqlx::test(migrations = "./migrations")]
async fn test_worker_health_tracks_start_ready_and_stopped(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let worker = test_worker(db.queue.clone()).register_job(counts).state(Arc::new(AtomicU32::new(0))).build().unwrap();
    let health = worker.health();
    assert_eq!(health.snapshot().status, WorkerHealthStatus::Starting);
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));
    wait_until(Duration::from_secs(3), Duration::from_millis(10), "worker did not report ready health", || async {
        health.snapshot().status == WorkerHealthStatus::Ready
    })
    .await;

    shutdown.cancel();
    run.await.unwrap().unwrap();
    assert_eq!(health.snapshot().status, WorkerHealthStatus::Stopped);
}

#[pgqueue::job]
async fn reports_its_attempt(_: (), seen: JobState<Arc<AtomicU32>>, ctx: JobContext) {
    seen.0.store(ctx.attempt(), Ordering::SeqCst);
    tracing::info!("handler line");
}

/// A claim given back without running — here three shutdown requeues — is not an attempt. `JobContext::attempt` is
/// documented as 1 on a job's first run, and read `attempts`, which counts every claim: it said 4 here, and a handler
/// keying first-run behavior on it skipped that behavior on the one run that needed it.
#[sqlx::test(migrations = "./migrations")]
async fn test_a_refunded_claim_does_not_count_as_an_attempt(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let handle = db.queue.enqueue(reports_its_attempt::job(())).await.unwrap().unwrap();
    for _ in 0..3 {
        let claimed = db.queue.dequeue(1, Uuid::now_v7()).await.unwrap();
        assert!(db.queue.requeue_shutdown(&claimed[0], "cancelled").await.unwrap());
    }

    let seen = Arc::new(AtomicU32::new(0));
    test_worker(db.queue.clone())
        .register_job(reports_its_attempt)
        .state(seen.clone())
        .burst(true)
        .dequeue_timeout(Duration::from_millis(50))
        .run()
        .await
        .unwrap();

    assert_eq!(seen.load(Ordering::SeqCst), 1, "the first run of a job refunded three times is its first attempt");
    let row = handle.fetch_job().await.unwrap();
    assert_eq!((row.status, row.attempts, row.refunds, row.max_attempts), (JobStatus::Complete, 4, 3, 4));
}

/// The attempt numbers one `job.run` span or one event carried, as the subscriber received them.
#[derive(Clone, Debug, Default)]
struct AttemptFields {
    message: String,
    attempt: Option<i64>,
    claim: Option<i64>,
    job_id: Option<String>,
}

impl tracing::field::Visit for AttemptFields {
    // The typed `record_*` methods all default to this one, so it sees `message` and every integer, signed or not.
    // A value that is no integer records as `None`, failing the assertion rather than panicking a worker's task.
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        let value = format!("{value:?}");
        match field.name() {
            "message" => self.message = value,
            "attempt" => self.attempt = value.parse().ok(),
            "claim" => self.claim = value.parse().ok(),
            "job.id" => self.job_id = Some(value),
            _ => {}
        }
    }
}

/// One event as the subscriber received it: its own fields, and those of the `job.run` span it was emitted under, if
/// any.
struct RecordedEvent {
    fields: AttemptFields,
    run: Option<AttemptFields>,
}

/// The `job.run` span fields an event was filed under: `(attempt, claim, job.id)`.
type RunScope = (Option<i64>, Option<i64>, Option<String>);

/// Records the attempt numbers of every `job.run` span opened and every event emitted while it is the default
/// subscriber.
#[derive(Clone, Default)]
struct RecordedAttempts {
    spans: Arc<Mutex<Vec<AttemptFields>>>,
    events: Arc<Mutex<Vec<RecordedEvent>>>,
}

impl RecordedAttempts {
    /// Starts recording, returning the recorder and the guard that stops it. Thread-local, so it takes precedence over
    /// the suite's global subscriber without disturbing tests on other threads — and still sees a worker's processors,
    /// because `#[sqlx::test]` runs a current-thread runtime that polls every spawned task on this thread.
    fn start() -> (Self, tracing::subscriber::DefaultGuard) {
        use tracing_subscriber::layer::SubscriberExt;

        let recorded = Self::default();
        let guard =
            tracing::subscriber::set_default(tracing_subscriber::registry::Registry::default().with(recorded.clone()));
        (recorded, guard)
    }

    fn spans(&self) -> Vec<(Option<i64>, Option<i64>)> {
        self.spans.lock().expect("recorded spans").iter().map(|span| (span.attempt, span.claim)).collect()
    }

    fn events(&self, message: &str) -> Vec<(Option<i64>, Option<i64>)> {
        let events = self.events.lock().expect("recorded events");
        events
            .iter()
            .filter(|event| event.fields.message == message)
            .map(|event| (event.fields.attempt, event.fields.claim))
            .collect()
    }

    /// The `job.run` span each event logged as `message` was filed under, `None` for one emitted outside any.
    fn runs(&self, message: &str) -> Vec<Option<RunScope>> {
        let events = self.events.lock().expect("recorded events");
        events
            .iter()
            .filter(|event| event.fields.message == message)
            .map(|event| event.run.as_ref().map(|run| (run.attempt, run.claim, run.job_id.clone())))
            .collect()
    }
}

impl<S> tracing_subscriber::layer::Layer<S> for RecordedAttempts
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        id: &tracing::span::Id,
        ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        if attrs.metadata().name() == "job.run" {
            let mut fields = AttemptFields::default();
            attrs.record(&mut fields);
            // Kept on the span too, for the events filed under it to be matched back to it.
            if let Some(span) = ctx.span(id) {
                span.extensions_mut().insert(fields.clone());
            }
            self.spans.lock().expect("recorded spans").push(fields);
        }
    }

    fn on_event(&self, event: &tracing::Event<'_>, ctx: tracing_subscriber::layer::Context<'_, S>) {
        let mut fields = AttemptFields::default();
        event.record(&mut fields);
        let run = ctx.event_scope(event).and_then(|scope| {
            scope
                .from_root()
                .find(|span| span.name() == "job.run")
                .and_then(|span| span.extensions().get::<AttemptFields>().cloned())
        });
        self.events.lock().expect("recorded events").push(RecordedEvent { fields, run });
    }
}

/// Every line a handler and its finalization log is filed under the `job.run` span, whose `attempt` read `attempts`,
/// the claim counter: a job refunded three times ran its first attempt under `attempt=4` while `ctx.attempt()` said 1,
/// so an operator matching the handler's logs to the worker's, or alerting on a retried attempt, saw a retry that never
/// happened. The claim number stays on the span as `claim`, because that is what identifies the attempt to the fence
/// and the abort poll.
///
/// The handler's own lines are the ones that matter most there, and they were filed under no span at all: the handler
/// runs in a task of its own, and a spawned task does not inherit its spawner's span, so a log or trace filtered by
/// `job.id` showed the worker's verdict on an attempt without a single line of the handler's explaining it.
#[sqlx::test(migrations = "./migrations")]
async fn test_job_run_span_reports_the_attempt_the_handler_sees(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let job_id = db.queue.enqueue(reports_its_attempt::job(())).await.unwrap().unwrap().id().to_string();
    for _ in 0..3 {
        let claimed = db.queue.dequeue(1, Uuid::now_v7()).await.unwrap();
        assert!(db.queue.requeue_shutdown(&claimed[0], "cancelled").await.unwrap());
    }

    let seen = Arc::new(AtomicU32::new(0));
    let (recorded, guard) = RecordedAttempts::start();
    test_worker(db.queue.clone())
        .register_job(reports_its_attempt)
        .state(seen.clone())
        .burst(true)
        .dequeue_timeout(Duration::from_millis(50))
        .run()
        .await
        .unwrap();
    drop(guard);

    let seen = i64::from(seen.load(Ordering::SeqCst));
    assert_eq!(seen, 1, "the handler ran its first attempt");
    assert_eq!(
        recorded.spans(),
        [(Some(seen), Some(4))],
        "the span must name the attempt `ctx.attempt()` reports, and the claim it fences on"
    );
    let attempt = Some((Some(seen), Some(4), Some(job_id)));
    let handler = std::slice::from_ref(&attempt);
    assert_eq!(recorded.runs("handler line"), handler, "the handler's own line must be filed under its span");
    assert_eq!(recorded.runs("job complete"), [attempt], "and so must the line its finalization logs");
}

/// `retry scheduled` names the attempt that failed beside the `delay_ms` its retry waits, and the backoff behind that
/// delay grows with the attempts spent. The event read `attempts` instead, so after one shutdown refund it named
/// attempt 2 for a delay the backoff had computed for attempt 1.
#[sqlx::test(migrations = "./migrations")]
async fn test_retry_scheduled_reports_the_attempt_its_backoff_grew_from(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    db.queue
        .enqueue_raw(crate::with_config("refunded_then_failed", |config| config.max_attempts = 2))
        .await
        .unwrap()
        .unwrap();
    let claimed = db.queue.dequeue(1, Uuid::now_v7()).await.unwrap();
    assert!(db.queue.requeue_shutdown(&claimed[0], "cancelled").await.unwrap());
    let claimed = db.queue.dequeue(1, Uuid::now_v7()).await.unwrap();
    let job = &claimed[0];
    assert_eq!((job.attempts, job.refunds, job.max_attempts), (2, 1, 3), "one refunded claim, then the one that fails");

    let (recorded, guard) = RecordedAttempts::start();
    assert!(db.queue.retry(job, "boom").await.unwrap());
    drop(guard);

    assert_eq!(
        recorded.events("retry scheduled"),
        [(Some(1), Some(2))],
        "the event must name the attempt the backoff counted, and the claim it retried"
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn test_worker_builder_run_processes_jobs_without_an_explicit_build(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let counter = Arc::new(AtomicU32::new(0));
    let handle = db.queue.enqueue(counts::job(())).await.unwrap().unwrap();

    test_worker(db.queue.clone())
        .register_job(counts)
        .state(counter.clone())
        .burst(true)
        .dequeue_timeout(Duration::from_millis(50))
        .run()
        .await
        .unwrap();

    assert_eq!(counter.load(Ordering::SeqCst), 1);
    assert_eq!(handle.fetch_job().await.unwrap().status, JobStatus::Complete);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_run_until_returns_without_starting_when_shutdown_is_pre_cancelled(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let counter = Arc::new(AtomicU32::new(0));
    let handle = db.queue.enqueue(counts::job(())).await.unwrap().unwrap();
    let worker = test_worker(db.queue.clone()).register_job(counts).state(counter.clone()).build().unwrap();
    let worker_id = worker.id();
    let shutdown = CancellationToken::new();
    shutdown.cancel();

    tokio::time::timeout(Duration::from_secs(1), worker.run_until(shutdown))
        .await
        .expect("pre-cancelled worker should stop promptly")
        .expect("pre-cancelled worker should stop cleanly");

    assert_eq!(counter.load(Ordering::SeqCst), 0);
    let row = handle.fetch_job().await.unwrap();
    assert_eq!(row.status, JobStatus::Queued);
    assert_eq!(row.attempts, 0);
    assert_eq!(row.max_attempts, 1);
    let worker_exists = sqlx::query_scalar::<_, bool>("SELECT EXISTS (SELECT 1 FROM pgqueue.workers WHERE id = $1)")
        .bind(worker_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(!worker_exists);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_notification_listener_recovers_after_postgres_terminates_connection(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let counter = Arc::new(AtomicU32::new(0));
    let worker = test_worker(db.queue.clone())
        .register_job(counts)
        .state(counter.clone())
        .poll_interval(Duration::from_secs(30))
        .build()
        .unwrap();
    let health = worker.health();
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));
    wait_until(Duration::from_secs(3), Duration::from_millis(10), "worker did not report ready health", || async {
        health.snapshot().status == WorkerHealthStatus::Ready
    })
    .await;

    let initial_pid = wait_for_some(
        Duration::from_secs(5),
        Duration::from_millis(20),
        "notification listener did not subscribe",
        || async {
            sqlx::query_scalar::<_, i32>(
                "SELECT pid FROM pg_stat_activity \
                 WHERE datname = current_database() AND query LIKE 'LISTEN %' \
                 ORDER BY backend_start LIMIT 1",
            )
            .fetch_optional(&pool)
            .await
            .unwrap()
        },
    )
    .await;
    let mut health_changes = health.clone();
    let degraded = tokio::spawn(async move {
        loop {
            let snapshot = health_changes.changed().await;
            if snapshot.failures.iter().any(|failure| failure.component == WorkerComponent::Notification) {
                return;
            }
        }
    });
    tokio::task::yield_now().await;

    let terminated = sqlx::query_scalar::<_, bool>("SELECT pg_terminate_backend($1)")
        .bind(initial_pid)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(terminated);
    tokio::time::timeout(Duration::from_secs(5), degraded)
        .await
        .expect("notification failure was not reported")
        .unwrap();

    let replacement_pid = wait_for_some(
        Duration::from_secs(5),
        Duration::from_millis(20),
        "notification listener did not reconnect",
        || async {
            sqlx::query_scalar::<_, i32>(
                "SELECT pid FROM pg_stat_activity \
                 WHERE datname = current_database() AND query LIKE 'LISTEN %' \
                 ORDER BY backend_start LIMIT 1",
            )
            .fetch_optional(&pool)
            .await
            .unwrap()
            .filter(|pid| *pid != initial_pid)
        },
    )
    .await;
    assert_ne!(replacement_pid, initial_pid);
    wait_until(Duration::from_secs(5), Duration::from_millis(20), "notification health did not recover", || async {
        health.snapshot().status == WorkerHealthStatus::Ready
    })
    .await;

    let handle = db.queue.enqueue(counts::job(())).await.unwrap().unwrap();
    // The counter is bumped *inside* the handler, so it goes up one full
    // `Database::finish` round trip before the row is terminal. Waiting on the
    // counter alone and then asserting the status raced that round trip and
    // read `running`, so the terminal row is part of what is waited for.
    wait_for_some(
        Duration::from_secs(5),
        Duration::from_millis(20),
        "reconnected listener did not wake the worker",
        || {
            let counter = counter.clone();
            let handle = handle.clone();
            async move {
                (counter.load(Ordering::SeqCst) == 1 && handle.fetch_job().await.ok()?.status == JobStatus::Complete)
                    .then_some(())
            }
        },
    )
    .await;

    shutdown.cancel();
    run.await.unwrap().unwrap();
}

#[sqlx::test(migrations = "./migrations")]
async fn test_worker_processes_typed_jobs_end_to_end(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let log: Log = Arc::new(Mutex::new(Vec::new()));

    let mut handles = Vec::new();
    for i in 0..3 {
        let handle = db.queue.enqueue(record::job(Payload { tag: format!("job{i}") })).await.unwrap().unwrap();
        handles.push(handle);
    }

    let worker = test_worker(db.queue.clone()).register_job(record).state(log.clone()).concurrency(2).build().unwrap();
    assert_ne!(worker.id(), Uuid::nil());
    assert_eq!(worker.queue().name(), "default");
    assert!(format!("{worker:?}").contains("Worker"));

    let token = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(token.clone()));

    for (i, handle) in handles.iter().enumerate() {
        let row = wait_terminal(&db.queue, handle.id(), 10).await;
        assert_eq!(row.status, JobStatus::Complete, "{:?}", row.error);
        assert_eq!(row.result, Some(json!(format!("done:job{i}"))));
        assert!(row.completed_at.is_some());
        assert!(row.worker_id.is_some());
    }
    let mut seen = log.lock().unwrap().clone();
    seen.sort();
    assert_eq!(seen, vec!["job0", "job1", "job2"]);

    token.cancel();
    run.await.unwrap().unwrap();
    assert_eq!(db.queue.stats().complete, 3);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_job_context_cancellation_does_not_cancel_attempt(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let handle = db.queue.enqueue(cancels_observer_token::job(())).await.unwrap().unwrap();
    let worker = test_worker(db.queue.clone()).register_job(cancels_observer_token).build().unwrap();
    let token = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(token.clone()));

    let row = wait_terminal(&db.queue, handle.id(), 10).await;
    token.cancel();
    run.await.unwrap().unwrap();

    assert_eq!(row.status, JobStatus::Complete);
    assert_eq!(row.attempts, 1);
    assert_eq!(db.queue.stats().complete, 1);
    assert_eq!(db.queue.stats().retried, 0);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_job_context_cancellation_fires_when_shutdown_begins(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let handle = db.queue.enqueue(observes_shutdown_cancellation::job(())).await.unwrap().unwrap();
    let probe = CancellationObservedProbe {
        started: Arc::new(tokio::sync::Notify::new()),
        observed: Arc::new(tokio::sync::Notify::new()),
    };
    let worker = test_worker(db.queue.clone())
        .register_job(observes_shutdown_cancellation)
        .state(probe.clone())
        .shutdown_grace(Duration::from_secs(5))
        .build()
        .unwrap();
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));

    tokio::time::timeout(Duration::from_secs(5), probe.started.notified()).await.expect("handler did not start");
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(2), probe.observed.notified())
        .await
        .expect("handler did not observe cancellation at shutdown start");
    run.await.unwrap().unwrap();

    assert_eq!(handle.fetch_job().await.unwrap().status, JobStatus::Complete);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_shutdown_requeues_a_handler_that_reports_cancellation_as_an_error(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let handle = db.queue.enqueue(errs_on_shutdown_cancellation::job(())).await.unwrap().unwrap();
    let probe = CancellationObservedProbe {
        started: Arc::new(tokio::sync::Notify::new()),
        observed: Arc::new(tokio::sync::Notify::new()),
    };
    let worker = test_worker(db.queue.clone())
        .register_job(errs_on_shutdown_cancellation)
        .state(probe.clone())
        .shutdown_grace(Duration::from_secs(5))
        .build()
        .unwrap();
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));

    tokio::time::timeout(Duration::from_secs(5), probe.started.notified()).await.expect("handler did not start");
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(2), probe.observed.notified())
        .await
        .expect("handler did not observe cancellation at shutdown start");
    run.await.unwrap().unwrap();

    // Returning early for shutdown is not a failed attempt: the job goes back
    // to the queue with its attempt refunded, exactly like a handler that is
    // force-stopped at grace expiry, and the reason stays on the row.
    let row = handle.fetch_job().await.unwrap();
    assert_eq!(row.status, JobStatus::Queued);
    assert_eq!(row.attempts, 1);
    assert_eq!(row.max_attempts, 2);
    assert_eq!(row.error.as_deref(), Some("aborted: stopped for shutdown"));
    assert_eq!(db.queue.stats().failed, 0);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_shutdown_records_a_handler_failure_that_is_not_classified_as_aborted(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let handle = db.queue.enqueue(fails_during_shutdown::job(())).await.unwrap().unwrap();
    let probe = CancellationObservedProbe {
        started: Arc::new(tokio::sync::Notify::new()),
        observed: Arc::new(tokio::sync::Notify::new()),
    };
    let worker = test_worker(db.queue.clone())
        .register_job(fails_during_shutdown)
        .state(probe.clone())
        .shutdown_grace(Duration::from_secs(5))
        .build()
        .unwrap();
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));

    tokio::time::timeout(Duration::from_secs(5), probe.started.notified()).await.expect("handler did not start");
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(2), probe.observed.notified())
        .await
        .expect("handler did not observe cancellation at shutdown start");
    run.await.unwrap().unwrap();

    let row = handle.fetch_job().await.unwrap();
    assert_eq!(row.status, JobStatus::Failed);
    assert_eq!(row.attempts, 1);
    assert_eq!(row.max_attempts, 1, "a genuine failure must not receive a shutdown refund");
    assert_eq!(row.error.as_deref(), Some("failed: cleanup failed"));
    assert_eq!(db.queue.stats().failed, 1);
    assert_eq!(db.queue.stats().retried, 0);
}

/// A user abort that lands before the abort poll sees it leaves the row `aborting` under the attempt, and the shutdown
/// requeue refuses it — a user abort must never come back as a retry. The row is still this attempt's to settle, so
/// shutdown finishes it `aborted` under the operator's reason. Leaving it `aborting` held its dedupe key and every
/// waiter until a sweeper reached it after this worker's lease lapsed — and for good when no worker was left running.
#[sqlx::test(migrations = "./migrations")]
async fn test_shutdown_finishes_a_user_abort_its_requeue_refuses(pool: PgPool) {
    let db = build_abort_test_db(pool.clone()).await;
    let handle = db.queue.enqueue(errs_on_shutdown_cancellation::job(())).await.unwrap().unwrap();
    let probe = CancellationObservedProbe {
        started: Arc::new(tokio::sync::Notify::new()),
        observed: Arc::new(tokio::sync::Notify::new()),
    };
    let worker = test_worker(db.queue.clone())
        .register_job(errs_on_shutdown_cancellation)
        .state(probe.clone())
        // The abort poll must not see the abort, so the attempt ends as a
        // shutdown return that the requeue guard then refuses.
        .timers(WorkerTimers { abort: Duration::from_secs(600), ..test_timers() })
        .shutdown_grace(Duration::from_secs(5))
        .build()
        .unwrap();
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));

    tokio::time::timeout(Duration::from_secs(5), probe.started.notified()).await.expect("handler did not start");
    assert!(handle.abort("user abort during shutdown").await.unwrap());
    shutdown.cancel();
    run.await.unwrap().unwrap();

    let row = handle.fetch_job().await.unwrap();
    assert_eq!(row.status, JobStatus::Aborted);
    assert_eq!(row.error.as_deref(), Some("user abort during shutdown"), "the operator's reason must survive");
    assert_eq!(row.attempts, 1);
    assert_eq!(row.max_attempts, 1, "a refused requeue refunds nothing");
    assert_eq!(db.queue.stats().aborted, 1);
    assert_eq!(db.queue.stats().retried, 0);
}

/// The same refusal reached through a forced stop: a handler that never looks at its cancellation token outlives the
/// shutdown grace, so its attempt ends cancelled rather than with an error, and the shutdown requeue that follows
/// refuses the user abort just the same.
#[sqlx::test(migrations = "./migrations")]
async fn test_shutdown_finishes_a_user_abort_its_forced_stop_requeue_refuses(pool: PgPool) {
    let db = build_abort_test_db(pool.clone()).await;
    let handle = db.queue.enqueue(ticks_until_forced_to_stop::job(())).await.unwrap().unwrap();
    let probe = HandlerTickProbe { started: Arc::new(tokio::sync::Notify::new()), ticks: Arc::new(AtomicU32::new(0)) };
    let worker = test_worker(db.queue.clone())
        .register_job(ticks_until_forced_to_stop)
        .state(probe.clone())
        // As above, the abort poll must not see the abort.
        .timers(WorkerTimers { abort: Duration::from_secs(600), ..test_timers() })
        .shutdown_grace(Duration::from_millis(100))
        .build()
        .unwrap();
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));

    tokio::time::timeout(Duration::from_secs(5), probe.started.notified()).await.expect("handler did not start");
    assert!(handle.abort("user abort during shutdown").await.unwrap());
    shutdown.cancel();
    run.await.unwrap().unwrap();

    let row = handle.fetch_job().await.unwrap();
    assert_eq!(row.status, JobStatus::Aborted);
    assert_eq!(row.error.as_deref(), Some("user abort during shutdown"), "the operator's reason must survive");
    assert_eq!(row.attempts, 1);
    assert_eq!(row.max_attempts, 1, "a refused requeue refunds nothing");
    assert_eq!(db.queue.stats().aborted, 1);
    assert_eq!(db.queue.stats().retried, 0);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_deleted_job_row_stops_its_handler_without_waiting_for_the_abort_grace(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db.queue.enqueue(ticks_until_forced_to_stop::job(())).await.unwrap().unwrap().id();
    let probe = HandlerTickProbe { started: Arc::new(tokio::sync::Notify::new()), ticks: Arc::new(AtomicU32::new(0)) };
    let worker = test_worker(db.queue.clone())
        .register_job(ticks_until_forced_to_stop)
        .state(probe.clone())
        // Long enough that a handler waiting out the grace would still be
        // running when this test's deadline passes.
        .abort_grace(Duration::from_secs(60))
        .shutdown_grace(Duration::from_secs(5))
        .build()
        .unwrap();
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));

    tokio::time::timeout(Duration::from_secs(5), probe.started.notified()).await.expect("handler did not start");
    assert_eq!(
        sqlx::query("DELETE FROM pgqueue.jobs WHERE id = $1")
            .bind(id)
            .execute(db.queue.pool())
            .await
            .unwrap()
            .rows_affected(),
        1
    );

    // The row was the attempt's only claim to dedupe exclusivity, so the handler
    // is force-stopped as soon as the abort poll notices, without the
    // cooperative grace a user abort gets.
    let deleted_at = tokio::time::Instant::now();
    wait_until(
        Duration::from_secs(5),
        Duration::from_millis(25),
        "handler kept running after its job row was deleted",
        // Three consecutive quiet windows, not one: this handler ticks every
        // 20 ms, so a single 100 ms window in which a loaded runtime did not poll
        // the task looks exactly like a force-stop. That direction fails open —
        // revert the cancellation and the test still passes — so it takes more
        // than one window to believe.
        || async {
            for _ in 0..3 {
                let before = probe.ticks.load(Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(100)).await;
                if probe.ticks.load(Ordering::SeqCst) != before {
                    return false;
                }
            }
            true
        },
    )
    .await;
    assert!(deleted_at.elapsed() < Duration::from_secs(5));

    shutdown.cancel();
    run.await.unwrap().unwrap();
}

/// The same immediacy for a row that recovery has already *finished* `aborted` under the live attempt:
/// `Database::abort_stuck_abandoned_batch` does that to a keyless attempt past its timeout with no retries left while
/// its owner still heartbeats. Finishing clears the sweeper's marker pair, so the abort poll read the terminal row as a
/// user abort and granted the handler the whole cooperative grace on a row that was no longer its own.
#[sqlx::test(migrations = "./migrations")]
async fn test_a_row_recovery_finished_stops_its_handler_without_waiting_for_the_abort_grace(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db.queue.enqueue(ticks_until_forced_to_stop::job(())).await.unwrap().unwrap().id();
    let probe = HandlerTickProbe { started: Arc::new(tokio::sync::Notify::new()), ticks: Arc::new(AtomicU32::new(0)) };
    let worker = test_worker(db.queue.clone())
        .register_job(ticks_until_forced_to_stop)
        .state(probe.clone())
        // Long enough that a handler waiting out the grace would still be running when this test's deadline passes.
        .abort_grace(Duration::from_secs(60))
        .shutdown_grace(Duration::from_secs(5))
        .build()
        .unwrap();
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));

    tokio::time::timeout(Duration::from_secs(5), probe.started.notified()).await.expect("handler did not start");
    // The row exactly as `abort_stuck_abandoned_batch` leaves it.
    assert_eq!(
        sqlx::query(
            "UPDATE pgqueue.jobs
             SET status = 'aborted', error = 'swept', result = NULL, completed_at = now(), touched_at = now()
             WHERE id = $1 AND status = 'running'",
        )
        .bind(id)
        .execute(db.queue.pool())
        .await
        .unwrap()
        .rows_affected(),
        1
    );

    let finished_at = tokio::time::Instant::now();
    wait_until(
        Duration::from_secs(5),
        Duration::from_millis(25),
        "handler kept running on a row recovery had finished",
        // Three consecutive quiet windows, for the reason the deleted-row test above gives.
        || async {
            for _ in 0..3 {
                let before = probe.ticks.load(Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(100)).await;
                if probe.ticks.load(Ordering::SeqCst) != before {
                    return false;
                }
            }
            true
        },
    )
    .await;
    assert!(finished_at.elapsed() < Duration::from_secs(5));
    let row = db.queue.fetch_job(id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Aborted);
    assert_eq!(row.error.as_deref(), Some("swept"), "the owner must not overwrite recovery's finish");

    shutdown.cancel();
    run.await.unwrap().unwrap();
}

/// The same immediacy, but with a user abort already under way. The abort
/// reason is recorded once, so the deletion loses that race — and losing it used
/// to skip the force-cancel with it, leaving the handler running, and producing
/// side effects, for the whole remaining `abort_grace` with no row left in the
/// database to guard its writes. The library reaches this state on its own:
/// `Database::abort_stuck_abandoned_batch` deletes an `aborting` row whose
/// `result_ttl_ms` is `0` out from under a live attempt.
#[sqlx::test(migrations = "./migrations")]
async fn test_deleted_job_row_stops_its_handler_when_a_user_abort_is_already_pending(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let handle = db.queue.enqueue(ticks_through_a_cooperative_abort::job(())).await.unwrap().unwrap();
    let id = handle.id();
    let probe = CooperativeTickProbe {
        started: Arc::new(tokio::sync::Notify::new()),
        cancelled: Arc::new(tokio::sync::Notify::new()),
        ticks: Arc::new(AtomicU32::new(0)),
    };
    let worker = test_worker(db.queue.clone())
        .register_job(ticks_through_a_cooperative_abort)
        .state(probe.clone())
        // Long enough that a handler waiting out the grace would still be
        // running when this test's deadline passes.
        .abort_grace(Duration::from_secs(60))
        .shutdown_grace(Duration::from_secs(5))
        .build()
        .unwrap();
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));

    tokio::time::timeout(Duration::from_secs(5), probe.started.notified()).await.expect("handler did not start");
    assert!(handle.abort("user abort").await.unwrap());
    // Waiting for the handler to see the cooperative cancellation is what puts
    // the `User` reason in first: the deletion below is the second request.
    tokio::time::timeout(Duration::from_secs(5), probe.cancelled.notified())
        .await
        .expect("the user abort never reached the handler");
    assert_eq!(
        sqlx::query("DELETE FROM pgqueue.jobs WHERE id = $1")
            .bind(id)
            .execute(db.queue.pool())
            .await
            .unwrap()
            .rows_affected(),
        1
    );

    let deleted_at = tokio::time::Instant::now();
    wait_until(
        Duration::from_secs(5),
        Duration::from_millis(25),
        "handler kept running after its job row was deleted",
        // Three consecutive quiet windows, not one: this handler ticks every
        // 20 ms, so a single 100 ms window in which a loaded runtime did not poll
        // the task looks exactly like a force-stop. That direction fails open —
        // revert the cancellation and the test still passes — so it takes more
        // than one window to believe.
        || async {
            for _ in 0..3 {
                let before = probe.ticks.load(Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(100)).await;
                if probe.ticks.load(Ordering::SeqCst) != before {
                    return false;
                }
            }
            true
        },
    )
    .await;
    assert!(deleted_at.elapsed() < Duration::from_secs(5));

    shutdown.cancel();
    run.await.unwrap().unwrap();
}

#[sqlx::test(migrations = "./migrations")]
async fn test_processors_observe_refills_without_polling_after_startup_race(pool: PgPool) {
    const CONCURRENCY: usize = 32;

    let db = TestDb::new(pool.clone()).await;
    let probe = SlotFillProbe { barrier: Arc::new(tokio::sync::Barrier::new(CONCURRENCY + 1)) };
    let mut handles = Vec::with_capacity(CONCURRENCY);
    for _ in 0..CONCURRENCY {
        handles.push(db.queue.enqueue(fills_worker_slot::job(())).await.unwrap().unwrap());
    }
    let worker = test_worker(db.queue.clone())
        .register_job(fills_worker_slot)
        .state(probe.clone())
        .concurrency(CONCURRENCY)
        // This test isolates refill notifications. A 300 ms lease can expire
        // while the completions wait for the fixture's five pooled connections
        // on CI, letting the startup sweep recover otherwise successful jobs.
        .timers(WorkerTimers { worker_info: Duration::from_secs(10), ..test_timers() })
        .poll_interval(Duration::from_secs(30))
        .build()
        .unwrap();
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));

    tokio::time::timeout(Duration::from_secs(5), probe.barrier.wait())
        .await
        .expect("a processor missed the refill notification");
    for handle in handles {
        let row = wait_terminal(&db.queue, handle.id(), 10).await;
        assert_eq!(row.status, JobStatus::Complete);
    }

    shutdown.cancel();
    run.await.unwrap().unwrap();
}

#[sqlx::test(migrations = "./migrations")]
async fn test_one_connection_pool_still_processes_jobs_after_listener_start(pool: PgPool) {
    let pool = pool_with_max(&pool, 1).await;
    let db = TestDb::new(pool).await;
    let handle = db.queue.enqueue(counts::job(())).await.unwrap().unwrap();
    let counter = Arc::new(AtomicU32::new(0));
    let worker = test_worker(db.queue.clone())
        .register_job(counts)
        .state(counter.clone())
        .burst(true)
        .dequeue_timeout(Duration::from_millis(400))
        .build()
        .unwrap();

    tokio::time::timeout(Duration::from_secs(10), worker.run_until(CancellationToken::new()))
        .await
        .expect("the LISTEN connection must not consume the only pooled connection")
        .unwrap();

    assert_eq!(counter.load(Ordering::SeqCst), 1);
    assert_eq!(handle.fetch_job().await.unwrap().status, JobStatus::Complete);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_failing_job_retries_then_fails(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let handle = db.queue.enqueue(always_fails::job(())).await.unwrap().unwrap();

    let worker = test_worker(db.queue.clone()).register_job(always_fails).build().unwrap();
    let token = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(token.clone()));

    let row = wait_terminal(&db.queue, handle.id(), 10).await;
    assert_eq!(row.status, JobStatus::Failed);
    assert_eq!(row.attempts, 2, "one retry (max_attempts = 2)");
    assert_eq!(row.error.as_deref(), Some("failed: boom"));

    token.cancel();
    run.await.unwrap().unwrap();
    assert_eq!(db.queue.stats().failed, 1);
    assert_eq!(db.queue.stats().retried, 1);
}

/// The subject is the *worker's own* attempt timeout, so the sweeper must not
/// race it. Every other queue here uses `sweep_grace(MIN_SWEEP_GRACE)`, which under
/// parallel load let the sweep leader's abort land inside `sleeps_forever`'s
/// 200ms timeout and settle the job `Aborted` instead of `Failed`. Both
/// outcomes are intended product behaviour; only one of them is under test.
#[sqlx::test(migrations = "./migrations")]
async fn test_worker_times_out_slow_job(pool: PgPool) {
    let db = TestDb::with(pool.clone(), |builder| builder.sweep_grace(Duration::from_secs(60))).await;
    let handle = db.queue.enqueue(sleeps_forever::job(())).await.unwrap().unwrap();

    let worker = test_worker(db.queue.clone()).register_job(sleeps_forever).build().unwrap();
    let token = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(token.clone()));

    let row = wait_terminal(&db.queue, handle.id(), 10).await;
    assert_eq!(row.status, JobStatus::Failed);
    assert!(row.error.as_deref().unwrap_or_default().starts_with("timeout:"), "{:?}", row.error);

    token.cancel();
    run.await.unwrap().unwrap();
}

#[sqlx::test(migrations = "./migrations")]
async fn test_panicking_job_fails_without_killing_the_worker(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let boom = db.queue.enqueue(panics::job(())).await.unwrap().unwrap();
    let fine = db.queue.enqueue(record::job(Payload { tag: "survivor".into() })).await.unwrap().unwrap();

    let boom_static = db.queue.enqueue(panics_static::job(())).await.unwrap().unwrap();
    let boom_weird = db.queue.enqueue(panics_weird::job(())).await.unwrap().unwrap();
    let log: Log = Arc::new(Mutex::new(Vec::new()));
    let worker = test_worker(db.queue.clone())
        .register_job(panics)
        .register_job(panics_static)
        .register_job(panics_weird)
        .register_job(record)
        .state(log)
        .build()
        .unwrap();
    let token = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(token.clone()));

    let row = wait_terminal(&db.queue, boom.id(), 10).await;
    assert_eq!(row.status, JobStatus::Failed);
    assert_eq!(row.error.as_deref(), Some("panic: kaboom 42"));

    let row = wait_terminal(&db.queue, boom_static.id(), 10).await;
    assert_eq!(row.error.as_deref(), Some("panic: static kaboom"));

    let row = wait_terminal(&db.queue, boom_weird.id(), 10).await;
    assert_eq!(row.error.as_deref(), Some("panic: handler panicked"));

    // The worker survived the panic and keeps processing.
    let row = wait_terminal(&db.queue, fine.id(), 10).await;
    assert_eq!(row.status, JobStatus::Complete);

    token.cancel();
    run.await.unwrap().unwrap();
}

#[sqlx::test(migrations = "./migrations")]
async fn test_abort_cancels_a_running_job(pool: PgPool) {
    let db = build_abort_test_db(pool.clone()).await;
    let handle = db.queue.enqueue(slow_but_abortable::job(())).await.unwrap().unwrap();
    let worker = test_worker(db.queue.clone()).register_job(slow_but_abortable).build().unwrap();
    let token = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(token.clone()));

    wait_status(&db.queue, handle.id(), JobStatus::Running, 10).await;
    assert!(handle.abort("operator said stop").await.unwrap());

    let row = wait_terminal(&db.queue, handle.id(), 10).await;
    assert_eq!(row.status, JobStatus::Aborted);
    assert_eq!(row.error.as_deref(), Some("operator said stop"));

    token.cancel();
    run.await.unwrap().unwrap();
    assert_eq!(db.queue.stats().aborted, 1);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_user_abort_allows_handler_cleanup_and_finishes_aborted(pool: PgPool) {
    let db = build_abort_test_db(pool.clone()).await;
    let handle = db.queue.enqueue(cleans_up_after_user_abort::job(())).await.unwrap().unwrap();
    let probe = AbortCleanupProbe {
        started: Arc::new(tokio::sync::Notify::new()),
        observed: Arc::new(tokio::sync::Notify::new()),
        cleaned: Arc::new(tokio::sync::Notify::new()),
    };
    let worker = test_worker(db.queue.clone())
        .register_job(cleans_up_after_user_abort)
        .state(probe.clone())
        .abort_grace(Duration::from_secs(2))
        .build()
        .unwrap();
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));

    tokio::time::timeout(Duration::from_secs(5), probe.started.notified()).await.expect("handler did not start");
    assert!(handle.abort("cleanup requested").await.unwrap());
    tokio::time::timeout(Duration::from_secs(2), probe.observed.notified())
        .await
        .expect("handler did not observe the user abort");
    tokio::time::timeout(Duration::from_secs(2), probe.cleaned.notified())
        .await
        .expect("handler cleanup did not finish within abort grace");

    let row = wait_terminal(&db.queue, handle.id(), 5).await;
    assert_eq!(row.status, JobStatus::Aborted);
    assert_eq!(row.error.as_deref(), Some("cleanup requested"));
    assert!(row.result.is_none());

    shutdown.cancel();
    run.await.unwrap().unwrap();
}

/// The handler seeing its cancellation proves a cooperative window opened, not how long it lasted, so the time to the
/// forced stop is measured too. A lower bound cannot flake: the grace starts after the abort request, and timers never
/// fire early. The grace is long enough that a stop which cut it short still lands well inside it, since the abort poll
/// that notices the request takes only a timer tick and a round trip.
#[sqlx::test(migrations = "./migrations")]
async fn test_user_abort_forces_handler_stop_when_cleanup_exceeds_abort_grace(pool: PgPool) {
    const ABORT_GRACE: Duration = Duration::from_secs(1);
    let db = build_abort_test_db(pool.clone()).await;
    let handle = db.queue.enqueue(exceeds_abort_grace::job(())).await.unwrap().unwrap();
    let probe = ForcedAbortProbe {
        started: Arc::new(tokio::sync::Notify::new()),
        observed: Arc::new(tokio::sync::Notify::new()),
        dropped: Arc::new(tokio::sync::Notify::new()),
    };
    let worker = test_worker(db.queue.clone())
        .register_job(exceeds_abort_grace)
        .state(probe.clone())
        .abort_grace(ABORT_GRACE)
        .build()
        .unwrap();
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));

    tokio::time::timeout(Duration::from_secs(5), probe.started.notified()).await.expect("handler did not start");
    let requested = tokio::time::Instant::now();
    assert!(handle.abort("cleanup timed out").await.unwrap());
    tokio::time::timeout(Duration::from_secs(2), probe.observed.notified())
        .await
        .expect("handler did not get a cooperative cancellation opportunity");
    tokio::time::timeout(Duration::from_secs(5), probe.dropped.notified())
        .await
        .expect("handler was not forcibly stopped after abort grace");
    let stopped_after = requested.elapsed();
    assert!(stopped_after >= ABORT_GRACE, "the handler was stopped {stopped_after:?} after the abort request");

    let row = wait_terminal(&db.queue, handle.id(), 5).await;
    assert_eq!(row.status, JobStatus::Aborted);
    assert_eq!(row.error.as_deref(), Some("cleanup timed out"));

    shutdown.cancel();
    run.await.unwrap().unwrap();
}

#[sqlx::test(migrations = "./migrations")]
async fn test_user_abort_stops_handler_immediately_when_abort_grace_is_zero(pool: PgPool) {
    let db = build_abort_test_db(pool.clone()).await;
    let handle = db.queue.enqueue(exceeds_abort_grace::job(())).await.unwrap().unwrap();
    let probe = ForcedAbortProbe {
        started: Arc::new(tokio::sync::Notify::new()),
        observed: Arc::new(tokio::sync::Notify::new()),
        dropped: Arc::new(tokio::sync::Notify::new()),
    };
    let worker = test_worker(db.queue.clone())
        .register_job(exceeds_abort_grace)
        .state(probe.clone())
        .abort_grace(Duration::ZERO)
        .build()
        .unwrap();
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));

    tokio::time::timeout(Duration::from_secs(5), probe.started.notified()).await.expect("handler did not start");
    assert!(handle.abort("no cleanup grace").await.unwrap());
    tokio::time::timeout(Duration::from_secs(2), probe.dropped.notified())
        .await
        .expect("handler was not stopped for zero abort grace");

    let row = wait_terminal(&db.queue, handle.id(), 5).await;
    assert_eq!(row.status, JobStatus::Aborted);
    assert_eq!(row.error.as_deref(), Some("no cleanup grace"));

    shutdown.cancel();
    run.await.unwrap().unwrap();
}

#[sqlx::test(migrations = "./migrations")]
async fn test_pending_abort_wins_over_a_final_attempt_failure(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let handle = db.queue.enqueue(fails_during_abort::job(())).await.unwrap().unwrap();
    let state = AbortFailureRace {
        started: Arc::new(tokio::sync::Notify::new()),
        release: Arc::new(tokio::sync::Notify::new()),
    };
    let worker = test_worker(db.queue.clone())
        .register_job(fails_during_abort)
        .state(state.clone())
        .timers(WorkerTimers { abort: Duration::from_secs(5), ..test_timers() })
        .build()
        .unwrap();
    let token = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(token.clone()));

    state.started.notified().await;
    // Let the abort loop consume its immediate first interval tick. Its next
    // poll is deliberately later than the handler failure below.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(handle.abort("operator abort won").await.unwrap());
    state.release.notify_one();

    let row = wait_terminal(&db.queue, handle.id(), 5).await;
    assert_eq!(row.status, JobStatus::Aborted);
    assert_eq!(row.error.as_deref(), Some("operator abort won"));

    token.cancel();
    run.await.unwrap().unwrap();
}

#[sqlx::test(migrations = "./migrations")]
async fn test_pending_abort_wins_over_a_successful_handler(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let handle = db.queue.enqueue(succeeds_during_abort::job(())).await.unwrap().unwrap();
    let state = AbortSuccessRace {
        started: Arc::new(tokio::sync::Notify::new()),
        release: Arc::new(tokio::sync::Notify::new()),
    };
    let worker = test_worker(db.queue.clone())
        .register_job(succeeds_during_abort)
        .state(state.clone())
        .timers(WorkerTimers { abort: Duration::from_secs(5), ..test_timers() })
        .build()
        .unwrap();
    let token = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(token.clone()));

    state.started.notified().await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(handle.abort("operator abort won").await.unwrap());
    state.release.notify_one();

    let row = wait_terminal(&db.queue, handle.id(), 5).await;
    assert_eq!(row.status, JobStatus::Aborted);
    assert_eq!(row.error.as_deref(), Some("operator abort won"));
    assert!(row.result.is_none());

    token.cancel();
    run.await.unwrap().unwrap();
}

/// Inserts `count` already-expired terminal job rows for the sweeper to purge.
async fn insert_expired_jobs(db: &TestDb, count: i64) {
    sqlx::query(
        "INSERT INTO pgqueue.jobs (queue, name, payload, status, expires_at, max_attempts)
         SELECT 'default', 'purge_me', '{}'::jsonb, 'complete',
                now() - interval '1 second', 1
         FROM generate_series(1, $1)",
    )
    .bind(count as i32)
    .execute(db.queue.pool())
    .await
    .expect("insert expired jobs");
}

async fn count_jobs_named(db: &TestDb, name: &str) -> i64 {
    sqlx::query_scalar::<_, i64>("SELECT count(*) FROM pgqueue.jobs WHERE name = $1")
        .bind(name)
        .fetch_one(db.queue.pool())
        .await
        .expect("count jobs")
}

#[sqlx::test(migrations = "./migrations")]
async fn test_sweep_operations_none_does_not_acquire_leadership(pool: PgPool) {
    let db = TestDb::new(pool).await;
    let mut idle = db.queue.sweeper();

    let idle_report = idle.sweep_operations(pgqueue::SweepOperations::NONE).await.unwrap();
    assert!(!idle_report.leader);
    assert!(!idle_report.has_more_work());

    let mut leader = db.queue.sweeper();
    let leader_report = leader.sweep().await.unwrap();
    assert!(leader_report.leader, "the no-op pass must leave leadership available");

    let retained_report = leader.sweep_operations(pgqueue::SweepOperations::NONE).await.unwrap();
    assert!(retained_report.leader, "a no-op pass must not release leadership already held");
    assert!(!retained_report.has_more_work());

    let idle_report = idle.sweep_operations(pgqueue::SweepOperations::NONE).await.unwrap();
    assert!(!idle_report.leader, "a no-op pass must not contend with the existing leader");
    leader.release().await;
}

#[sqlx::test(migrations = "./migrations")]
async fn test_sweep_reports_only_the_operations_that_filled_their_batch(pool: PgPool) {
    let db = TestDb::with(pool.clone(), |builder| builder.sweep_batch_size(1)).await;
    insert_expired_jobs(&db, 2).await;
    sqlx::query(
        "INSERT INTO pgqueue.cron_occurrences (queue, dedupe_key, scheduled_at, expires_at)
         SELECT 'default', 'cron:expired', now() - make_interval(secs => i),
                now() - interval '1 second'
         FROM generate_series(1, 2) AS s(i)",
    )
    .execute(db.queue.pool())
    .await
    .unwrap();

    let mut sweeper = db.queue.sweeper();
    let report = sweeper.sweep().await.unwrap();
    assert!(report.leader);
    assert_eq!(report.purged_jobs, 1);
    assert!(report.has_more_work());
    assert!(report.unfinished.expired_jobs);
    assert!(report.unfinished.cron_occurrences);
    assert!(!report.unfinished.workers);
    assert!(!report.unfinished.stuck_jobs);

    // A drain pass runs only the operations that reported more work, so an
    // operation left out is not re-issued at all.
    let only_jobs = pgqueue::SweepOperations { expired_jobs: true, ..pgqueue::SweepOperations::NONE };
    let next = sweeper.sweep_operations(only_jobs).await.unwrap();
    assert_eq!(next.purged_jobs, 1);
    assert_eq!(next.unfinished, only_jobs, "a full batch may have more work");
    assert_eq!(count_jobs_named(&db, "purge_me").await, 0);

    let drained = sweeper.sweep_operations(only_jobs).await.unwrap();
    assert_eq!(drained.purged_jobs, 0);
    assert_eq!(drained.unfinished, pgqueue::SweepOperations::NONE);
    assert!(!drained.has_more_work());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM pgqueue.cron_occurrences")
            .fetch_one(db.queue.pool())
            .await
            .unwrap(),
        1,
        "the drain left the cron occurrence purge out, so it did not run"
    );
    sweeper.release().await;
}

#[sqlx::test(migrations = "./migrations")]
async fn test_sweep_drain_stops_at_the_pass_budget(pool: PgPool) {
    // One row per pass makes the drain's pass budget directly observable, so the
    // backlog has to outrun that budget by enough that draining it whole is
    // impossible in one tick.
    const BACKLOG: i64 = 600;
    const MAX_SWEEP_DRAIN_PASSES: i64 = 256;

    let db = TestDb::with(pool.clone(), |builder| builder.sweep_batch_size(1)).await;
    insert_expired_jobs(&db, BACKLOG).await;
    let worker = Worker::builder(db.queue.clone())
        // Only the sweeper runs: every other loop is parked for this test.
        .timers(WorkerTimers {
            abort: Duration::from_secs(600),
            schedule: Duration::from_secs(600),
            sweep: Duration::from_secs(600),
            worker_info: Duration::from_secs(600),
        })
        .poll_interval(Duration::from_secs(600))
        .register_job(counts)
        .state(Arc::new(AtomicU32::new(0)))
        .build()
        .unwrap();
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));

    wait_until(Duration::from_secs(5), Duration::from_millis(25), "sweeper never drained a batch", || async {
        count_jobs_named(&db, "purge_me").await < BACKLOG
    })
    .await;
    // A drain is bounded by `MAX_SWEEP_DRAIN_TIME` as well as by its pass
    // budget, and at one row per pass the clock is normally the one that stops
    // it — so wait for the tick to settle rather than for a particular number of
    // passes. Requiring the whole pass budget inside a wall-clock second made
    // this test fail under load, with no second tick coming (the sweep timer
    // here is 600s).
    let mut remaining = count_jobs_named(&db, "purge_me").await;
    let settled = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        tokio::time::sleep(Duration::from_millis(400)).await;
        let next = count_jobs_named(&db, "purge_me").await;
        if next == remaining {
            break;
        }
        assert!(tokio::time::Instant::now() < settled, "the sweep drain never settled: {next} of {BACKLOG} left");
        remaining = next;
    }

    // The sweeper shares the worker's pool, so one tick drains a bounded number
    // of batches instead of running back-to-back passes for its whole budget:
    // the backlog is more than twice the budget, and what is left proves the
    // tick stopped well short of draining it.
    let purged = BACKLOG - remaining;
    assert!(
        (1..=MAX_SWEEP_DRAIN_PASSES).contains(&purged),
        "one tick purged {purged} of {BACKLOG} rows, outside the 1..={MAX_SWEEP_DRAIN_PASSES} \
         pass budget"
    );

    shutdown.cancel();
    run.await.unwrap().unwrap();
}

/// A sweeper that never catches up is otherwise invisible: retention silently
/// falls behind and the only symptom is a table that keeps growing.
#[sqlx::test(migrations = "./migrations")]
async fn test_sweeper_reports_unhealthy_when_it_stays_behind(pool: PgPool) {
    // Far more expired rows than a drain can retire at one row per pass, so
    // every tick ends on a budget with work still pending.
    const BACKLOG: i64 = 5_000;

    let db = TestDb::with(pool.clone(), |builder| builder.sweep_batch_size(1)).await;
    insert_expired_jobs(&db, BACKLOG).await;
    let worker = Worker::builder(db.queue.clone())
        // Only the sweeper runs, and it ticks fast enough to exhaust its budget
        // repeatedly inside the test.
        .timers(WorkerTimers {
            abort: Duration::from_secs(600),
            schedule: Duration::from_secs(600),
            sweep: Duration::from_millis(100),
            worker_info: Duration::from_secs(600),
        })
        .poll_interval(Duration::from_secs(600))
        .register_job(counts)
        .state(Arc::new(AtomicU32::new(0)))
        .build()
        .unwrap();
    let health = worker.health();
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));

    wait_until(
        Duration::from_secs(30),
        Duration::from_millis(25),
        "sweeper never reported that it was falling behind",
        || async {
            health
                .snapshot()
                .failures
                .iter()
                .any(|failure| failure.component == WorkerComponent::Sweeper && failure.message.contains("behind"))
        },
    )
    .await;

    shutdown.cancel();
    run.await.unwrap().unwrap();
}

#[sqlx::test(migrations = "./migrations")]
async fn test_sweeper_abort_racing_a_retryable_failure_still_retries(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let mut sweeper = db.queue.sweeper();
    assert!(sweeper.sweep().await.unwrap().leader);

    let handle = db.queue.enqueue(fails_as_sweeper_marks::job(())).await.unwrap().unwrap();
    let state = SweepFailureRace {
        started: Arc::new(tokio::sync::Notify::new()),
        release: Arc::new(tokio::sync::Notify::new()),
    };
    let worker = test_worker(db.queue.clone())
        .register_job(fails_as_sweeper_marks)
        .state(state.clone())
        .timers(WorkerTimers { abort: Duration::from_secs(5), ..test_timers() })
        .build()
        .unwrap();
    let worker_id = worker.id();
    let token = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(token.clone()));

    state.started.notified().await;
    wait_for_worker_lease(&db.queue, worker_id).await;
    backdate_job_liveness(&db, handle.id()).await;
    assert_eq!(sweeper.sweep().await.unwrap().cancelling, vec![handle.id()]);
    state.release.notify_one();

    let row = wait_terminal(&db.queue, handle.id(), 5).await;
    assert_eq!(row.status, JobStatus::Complete);
    assert_eq!(row.attempts, 2);

    token.cancel();
    run.await.unwrap().unwrap();
    sweeper.release().await;
}

/// The user-abort half of the race above: an operator abort that lands while
/// the row is sweeper-marked `aborting` wins over the sweeper's pending
/// retry. The owning worker's failing attempt finalizes `aborted` with the
/// operator's reason instead of being requeued and run again.
#[sqlx::test(migrations = "./migrations")]
async fn test_user_abort_during_the_sweeper_marked_window_wins_over_the_retry(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let mut sweeper = db.queue.sweeper();
    assert!(sweeper.sweep().await.unwrap().leader);

    let handle = db.queue.enqueue(fails_as_sweeper_marks::job(())).await.unwrap().unwrap();
    let state = SweepFailureRace {
        started: Arc::new(tokio::sync::Notify::new()),
        release: Arc::new(tokio::sync::Notify::new()),
    };
    let worker = test_worker(db.queue.clone())
        .register_job(fails_as_sweeper_marks)
        .state(state.clone())
        .timers(WorkerTimers { abort: Duration::from_secs(5), ..test_timers() })
        .build()
        .unwrap();
    let worker_id = worker.id();
    let token = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(token.clone()));

    state.started.notified().await;
    wait_for_worker_lease(&db.queue, worker_id).await;
    backdate_job_liveness(&db, handle.id()).await;
    assert_eq!(sweeper.sweep().await.unwrap().cancelling, vec![handle.id()]);
    assert!(
        db.queue.abort_job(handle.id(), "operator said stop").await.unwrap(),
        "the abort claims the sweeper-marked row"
    );
    state.release.notify_one();

    let row = wait_terminal(&db.queue, handle.id(), 5).await;
    assert_eq!(row.status, JobStatus::Aborted);
    assert_eq!(row.error.as_deref(), Some("operator said stop"));
    assert_eq!(row.attempts, 1, "the job never ran again");

    token.cancel();
    run.await.unwrap().unwrap();
    sweeper.release().await;
}

/// The retry that carries a failed attempt past a sweeper abort must carry its
/// reason too. It used to store nothing, which left the sweeper's internal
/// `swept` marker standing as the row's `error` for the whole retry-backoff
/// window and the next attempt — so `fetch_job`, `jobs_page` and the dashboard
/// all reported "swept" for a job that failed with a real, actionable error.
/// The non-retryable half of the identical race has always preserved it (see
/// `test_final_handler_failure_finishes_through_a_sweeper_abort`).
#[sqlx::test(migrations = "./migrations")]
async fn test_sweeper_abort_racing_a_retryable_failure_keeps_the_handler_error(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let mut sweeper = db.queue.sweeper();
    assert!(sweeper.sweep().await.unwrap().leader);

    let handle = db.queue.enqueue(fails_then_parks_as_sweeper_marks::job(())).await.unwrap().unwrap();
    let state = SweepFailureReasonRace {
        started: Arc::new(tokio::sync::Notify::new()),
        release: Arc::new(tokio::sync::Notify::new()),
        retried: Arc::new(tokio::sync::Notify::new()),
        hold: Arc::new(tokio::sync::Notify::new()),
    };
    let worker = test_worker(db.queue.clone())
        .register_job(fails_then_parks_as_sweeper_marks)
        .state(state.clone())
        .timers(WorkerTimers { abort: Duration::from_secs(5), ..test_timers() })
        .build()
        .unwrap();
    let worker_id = worker.id();
    let token = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(token.clone()));

    // The sweeper marks the attempt for abort while the handler is parked, so
    // the failure it returns next lands on an `aborting` row: `retry` is
    // refused and the swept-abort retry is what requeues it.
    state.started.notified().await;
    wait_for_worker_lease(&db.queue, worker_id).await;
    backdate_job_liveness(&db, handle.id()).await;
    assert_eq!(sweeper.sweep().await.unwrap().cancelling, vec![handle.id()]);
    state.release.notify_one();

    // The second attempt parks, so the row is read at exactly the point an
    // operator would see it: after the retry, before it is overwritten.
    state.retried.notified().await;
    let row = db.queue.fetch_job(handle.id()).await.unwrap().expect("the requeued job row");
    assert_eq!(row.attempts, 2);
    assert_eq!(
        row.error.as_deref(),
        Some("failed: the real handler failure"),
        "the retry must report why the attempt failed, not the sweeper's marker"
    );

    state.hold.notify_one();
    let row = wait_terminal(&db.queue, handle.id(), 10).await;
    assert_eq!(row.status, JobStatus::Complete);

    token.cancel();
    run.await.unwrap().unwrap();
    sweeper.release().await;
}

#[sqlx::test(migrations = "./migrations")]
async fn test_successful_handler_finishes_through_a_sweeper_abort(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let mut sweeper = db.queue.sweeper();
    assert!(sweeper.sweep().await.unwrap().leader);
    let handle = db.queue.enqueue(succeeds_as_sweeper_marks::job(())).await.unwrap().unwrap();
    let state = AbortSuccessRace {
        started: Arc::new(tokio::sync::Notify::new()),
        release: Arc::new(tokio::sync::Notify::new()),
    };
    let worker = test_worker(db.queue.clone())
        .register_job(succeeds_as_sweeper_marks)
        .state(state.clone())
        .timers(WorkerTimers { abort: Duration::from_secs(5), ..test_timers() })
        .build()
        .unwrap();
    let worker_id = worker.id();
    let token = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(token.clone()));

    state.started.notified().await;
    wait_for_worker_lease(&db.queue, worker_id).await;
    backdate_job_liveness(&db, handle.id()).await;
    assert_eq!(sweeper.sweep().await.unwrap().cancelling, vec![handle.id()]);
    state.release.notify_one();

    let row = wait_terminal(&db.queue, handle.id(), 5).await;
    assert_eq!(row.status, JobStatus::Complete);
    assert_eq!(row.attempts, 1);
    assert_eq!(db.queue.counts().await.unwrap().queued, 0);

    token.cancel();
    run.await.unwrap().unwrap();
    sweeper.release().await;
}

#[sqlx::test(migrations = "./migrations")]
async fn test_final_handler_failure_finishes_through_a_sweeper_abort(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let mut sweeper = db.queue.sweeper();
    assert!(sweeper.sweep().await.unwrap().leader);
    let handle = db.queue.enqueue(fails_as_sweeper_marks::job(()).max_attempts(1)).await.unwrap().unwrap();
    let state = SweepFailureRace {
        started: Arc::new(tokio::sync::Notify::new()),
        release: Arc::new(tokio::sync::Notify::new()),
    };
    let worker = test_worker(db.queue.clone())
        .register_job(fails_as_sweeper_marks)
        .state(state.clone())
        .timers(WorkerTimers { abort: Duration::from_secs(5), ..test_timers() })
        .build()
        .unwrap();
    let worker_id = worker.id();
    let token = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(token.clone()));

    state.started.notified().await;
    wait_for_worker_lease(&db.queue, worker_id).await;
    backdate_job_liveness(&db, handle.id()).await;
    assert_eq!(sweeper.sweep().await.unwrap().cancelling, vec![handle.id()]);
    state.release.notify_one();

    let row = wait_terminal(&db.queue, handle.id(), 5).await;
    assert_eq!(row.status, JobStatus::Failed);
    assert_eq!(row.error.as_deref(), Some("failed: handler failed as sweep abort landed"));

    token.cancel();
    run.await.unwrap().unwrap();
    sweeper.release().await;
}

#[sqlx::test(migrations = "./migrations")]
async fn test_undecodable_payload_fails_with_decode_error(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    // Enqueue raw JSON that does not match `Payload`.
    let id = db.queue.enqueue_raw(JobRequest::new("record", json!({"wrong": "shape"}))).await.unwrap().unwrap();

    let log: Log = Arc::new(Mutex::new(Vec::new()));
    let worker = test_worker(db.queue.clone()).register_job(record).state(log).build().unwrap();
    let token = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(token.clone()));

    let row = wait_terminal(&db.queue, id, 10).await;
    assert_eq!(row.status, JobStatus::Failed);
    assert!(row.error.as_deref().unwrap_or_default().starts_with("decode:"), "{:?}", row.error);

    token.cancel();
    run.await.unwrap().unwrap();
}

#[sqlx::test(migrations = "./migrations")]
async fn test_missing_state_fails_with_extract_error(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let handle = db.queue.enqueue(needs_missing_state::job(())).await.unwrap().unwrap();

    let worker = test_worker(db.queue.clone()).register_job(needs_missing_state).build().unwrap();
    let token = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(token.clone()));

    let row = wait_terminal(&db.queue, handle.id(), 10).await;
    assert_eq!(row.status, JobStatus::Failed);
    // Extract failures are deterministic: no retry despite max_attempts = 5.
    assert_eq!(row.attempts, 1);
    let error = row.error.as_deref().unwrap_or_default();
    assert!(error.starts_with("extract:") && error.contains("Uuid"), "{error:?}");

    token.cancel();
    run.await.unwrap().unwrap();
}

#[sqlx::test(migrations = "./migrations")]
async fn test_undecodable_payload_fails_without_retry_when_attempts_remain(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id =
        db.queue.enqueue_raw(JobRequest::new("decodes_payload", json!({"wrong": "shape"}))).await.unwrap().unwrap();

    let worker = test_worker(db.queue.clone()).register_job(decodes_payload).build().unwrap();
    let token = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(token.clone()));

    let row = wait_terminal(&db.queue, id, 10).await;
    assert_eq!(row.status, JobStatus::Failed);
    // Decode failures are deterministic: no retry despite max_attempts = 5.
    assert_eq!(row.attempts, 1);
    assert!(row.error.as_deref().unwrap_or_default().starts_with("decode:"), "{:?}", row.error);

    token.cancel();
    run.await.unwrap().unwrap();
}

#[sqlx::test(migrations = "./migrations")]
async fn test_returned_job_error_preserves_its_kind_and_retry_policy(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let handle = db.queue.enqueue(returns_decode_error::job(())).await.unwrap().unwrap();

    let worker = test_worker(db.queue.clone()).register_job(returns_decode_error).build().unwrap();
    let token = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(token.clone()));

    let row = wait_terminal(&db.queue, handle.id(), 10).await;
    assert_eq!(row.status, JobStatus::Failed);
    assert_eq!(row.attempts, 1, "decode errors must not retry");
    assert_eq!(row.error.as_deref(), Some("decode: handler decode failure"));

    token.cancel();
    run.await.unwrap().unwrap();
}

/// A worker handles every job name in its queue. A worker that violates the
/// contract — a rolling deploy's old binary is the ordinary case — claims the
/// job anyway and gives it back: requeued with an attempt refund and a delay,
/// the reason stored on the row, and nothing spent from the job's own budget.
#[sqlx::test(migrations = "./migrations")]
async fn test_a_worker_missing_the_handler_bounces_the_job_with_a_refund(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let handle = db.queue.enqueue(counts::job(())).await.unwrap().unwrap();

    let worker = test_worker(db.queue.clone())
        .register_job(always_fails)
        .burst(true)
        .dequeue_timeout(Duration::from_millis(200))
        .build()
        .unwrap();
    let health = worker.health();
    let before = jiff::Timestamp::now();
    worker.run_until(CancellationToken::new()).await.unwrap();

    let row = handle.fetch_job().await.unwrap();
    assert_eq!(row.status, JobStatus::Queued);
    assert_eq!(row.error.as_deref(), Some("no handler registered for job \"counts\""));
    assert_eq!(row.attempts, 1, "the claim spent the attempt the refund below gives back");
    assert_eq!(row.max_attempts, 2, "the bounce must refund the attempt, not burn it");
    assert!(
        row.scheduled_at > before + jiff::SignedDuration::from_secs(5),
        "the bounce must delay the row so the same worker does not respin it: scheduled_at {}",
        row.scheduled_at
    );
    let failure = health
        .snapshot()
        .failures
        .into_iter()
        .find(|failure| failure.component == WorkerComponent::Dispatch)
        .expect("an unhandled job must degrade dispatch health");
    assert!(failure.message.contains("counts"), "{}", failure.message);

    // A worker that registers the handler picks the job up once the delay
    // lapses; collapse the delay rather than wait it out.
    sqlx::query("UPDATE pgqueue.jobs SET scheduled_at = now() WHERE id = $1")
        .bind(handle.id())
        .execute(db.queue.pool())
        .await
        .unwrap();
    let counter = Arc::new(AtomicU32::new(0));
    let worker = test_worker(db.queue.clone())
        .register_job(counts)
        .state(counter.clone())
        .burst(true)
        .dequeue_timeout(Duration::from_millis(200))
        .build()
        .unwrap();
    worker.run_until(CancellationToken::new()).await.unwrap();
    assert_eq!(counter.load(Ordering::SeqCst), 1);
    assert_eq!(handle.fetch_job().await.unwrap().status, JobStatus::Complete);
}

/// A batch mixing handled and unhandled names completes the handled jobs and
/// bounces the rest: the unhandled row goes back `queued` with its reason
/// stored, and the bounce never blocks the work the worker can do.
#[sqlx::test(migrations = "./migrations")]
async fn test_batch_dequeues_bounce_unhandled_names_and_complete_the_rest(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    db.queue.enqueue_raw(JobRequest::new("unhandled", json!(null))).await.unwrap();
    let handles = vec![
        db.queue.enqueue(counts::job(())).await.unwrap().unwrap(),
        db.queue.enqueue(counts::job(())).await.unwrap().unwrap(),
        db.queue.enqueue(counts::job(())).await.unwrap().unwrap(),
    ];

    let counter = Arc::new(AtomicU32::new(0));
    test_worker(db.queue.clone())
        .register_job(counts)
        .state(counter.clone())
        .concurrency(3)
        .burst(true)
        .dequeue_timeout(Duration::from_millis(200))
        .build()
        .unwrap()
        .run_until(CancellationToken::new())
        .await
        .unwrap();

    assert_eq!(counter.load(Ordering::SeqCst), 3);
    for handle in handles {
        assert_eq!(handle.fetch_job().await.unwrap().status, JobStatus::Complete);
    }
    let (status, error) = sqlx::query_as::<_, (String, Option<String>)>(
        "SELECT status, error FROM pgqueue.jobs WHERE name = 'unhandled'",
    )
    .fetch_one(db.queue.pool())
    .await
    .unwrap();
    assert_eq!(status, "queued", "a bounced job waits for a worker that registers its handler");
    assert_eq!(error.as_deref(), Some("no handler registered for job \"unhandled\""));
}

/// Matched against `pg_stat_activity.query`, which PostgreSQL truncates at
/// `track_activity_query_size` — 1 KB by default. So the marker has to be at
/// the *start* of the claim rather than anywhere inside it: a marker past
/// 1 KB is invisible here, which reads exactly like a worker that never
/// paused. `claim_lock` opens the claim statement and appears in nothing
/// else.
const DEQUEUE_STATEMENT: &str = "%WITH claim_lock AS MATERIALIZED%";

const DEQUEUE_HANDOFF_GATE: i32 = 20_561;
const DEQUEUE_CONNECTION_GATE: i32 = 20_562;
const SHUTDOWN_REQUEUE_GATE: i32 = 20_563;

async fn install_dequeue_handoff_gate(pool: &PgPool) {
    install_statement_gate(
        pool,
        "wait_at_dequeue_handoff",
        DEQUEUE_HANDOFF_GATE,
        "UPDATE",
        "OLD.status = 'queued' AND NEW.status = 'running'",
    )
    .await;
}

async fn install_shutdown_requeue_gate(pool: &PgPool) {
    install_statement_gate(
        pool,
        "wait_at_shutdown_requeue",
        SHUTDOWN_REQUEUE_GATE,
        "UPDATE",
        "OLD.status = 'running' AND NEW.status = 'queued' AND NEW.error = 'cancelled'",
    )
    .await;
}

#[sqlx::test(migrations = "./migrations")]
async fn test_dequeue_worker_returns_committed_batch_when_post_commit_probe_fails(pool: PgPool) {
    let control = TestDb::new(pool_with_max(&pool, 5).await).await;
    let worker_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_millis(500))
        .connect_with(pool.connect_options().as_ref().clone())
        .await
        .unwrap();
    let worker_db = TestDb::new(worker_pool.clone()).await;
    install_dequeue_handoff_gate(control.queue.pool()).await;

    let dequeue_gate = hold_gate(control.queue.pool(), DEQUEUE_HANDOFF_GATE, &control.database).await;
    let connection_gate = hold_gate(control.queue.pool(), DEQUEUE_CONNECTION_GATE, &control.database).await;

    let worker_id = Uuid::now_v7();
    control.queue.write_worker_info(worker_id, json!({}), None, Duration::from_secs(60)).await.unwrap();
    let handle = control.queue.enqueue(counts::job(())).await.unwrap().unwrap();

    let worker_queue = worker_db.queue.clone();
    let mut dequeue =
        tokio::spawn(async move { pgqueue::__test_support::dequeue_worker(&worker_queue, 2, worker_id).await });
    wait_for_lock_waiter(&control, DEQUEUE_STATEMENT, "worker did not pause while claiming the job").await;

    let database = control.database.clone();
    let connection_pool = worker_pool.clone();
    let mut connection_stealer = tokio::spawn(async move {
        sqlx::query("SELECT pg_advisory_xact_lock($1, hashtext($2))")
            .bind(DEQUEUE_CONNECTION_GATE)
            .bind(database)
            .execute(&connection_pool)
            .await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut connection_stealer).await.is_err(),
        "probe connection was not queued behind the dequeue"
    );

    dequeue_gate.rollback().await.unwrap();
    wait_for_lock_waiter(
        &control,
        "%SELECT pg_advisory_xact_lock($1, hashtext($2))%",
        "connection stealer did not hold the committed dequeue connection",
    )
    .await;
    let jobs = tokio::time::timeout(Duration::from_secs(2), &mut dequeue)
        .await
        .expect("worker dequeue did not return after its probe timed out")
        .unwrap()
        .expect("post-commit probe failure discarded the committed batch");

    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].id, handle.id());
    assert_eq!(jobs[0].status, JobStatus::Running);
    assert_eq!(jobs[0].worker_id, Some(worker_id));

    connection_gate.rollback().await.unwrap();
    connection_stealer.await.unwrap().unwrap();
    assert!(control.queue.finish(&jobs[0], JobStatus::Complete, Some(json!(null)), None).await.unwrap());
    assert_eq!(handle.fetch_job().await.unwrap().status, JobStatus::Complete);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_graceful_shutdown_requeues_inflight_jobs(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let handle =
        db.queue.enqueue(slow_but_abortable::job(()).retry_delay(Duration::from_secs(60 * 60))).await.unwrap().unwrap();
    let scheduled_at = handle.fetch_job().await.unwrap().scheduled_at;

    let worker = test_worker(db.queue.clone())
        .register_job(slow_but_abortable)
        .shutdown_grace(Duration::from_millis(100))
        .build()
        .unwrap();
    let token = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(token.clone()));

    wait_status(&db.queue, handle.id(), JobStatus::Running, 10).await;
    token.cancel();
    run.await.unwrap().unwrap();

    let row = handle.fetch_job().await.unwrap();
    assert_eq!(row.status, JobStatus::Queued, "requeued on shutdown");
    assert_eq!(row.error.as_deref(), Some("cancelled"));
    assert_eq!(row.attempts, 1, "execution history remains monotonic");
    assert_eq!(row.max_attempts, 2, "shutdown refunds the retry budget");
    assert_eq!(db.queue.stats().retried, 1);
    assert_eq!(row.scheduled_at, scheduled_at, "shutdown does not apply failure backoff");
}

/// The subject is the shutdown handoff, so the worker's own sweeper must not
/// race it. This test holds a row lock on `pgqueue.workers` to freeze the
/// worker's heartbeat, so its 75ms lease lapses on purpose — and under
/// `sweep_grace(MIN_SWEEP_GRACE)` a lapsed lease makes the buffered attempt recoverable
/// immediately, letting the sweep leader mark it `aborting` before the assertion
/// below reads it. The cushion is the same one `test_worker_times_out_slow_job`
/// needs, for the same reason.
#[sqlx::test(migrations = "./migrations")]
async fn test_graceful_shutdown_waits_for_buffered_job_requeue(pool: PgPool) {
    let control = TestDb::new(pool_with_max(&pool, 6).await).await;
    let db = TestDb::with(pool_with_max(&pool, 5).await, |builder| builder.sweep_grace(Duration::from_secs(60))).await;
    install_dequeue_handoff_gate(control.queue.pool()).await;
    install_shutdown_requeue_gate(control.queue.pool()).await;

    let dequeue_gate = hold_gate(control.queue.pool(), DEQUEUE_HANDOFF_GATE, &control.database).await;
    let requeue_gate = hold_gate(control.queue.pool(), SHUTDOWN_REQUEUE_GATE, &control.database).await;

    let counter = Arc::new(AtomicU32::new(0));
    let handle = control.queue.enqueue(counts::job(())).await.unwrap().unwrap();
    let worker = test_worker(db.queue.clone())
        .register_job(counts)
        .state(counter.clone())
        .concurrency(1)
        .timers(WorkerTimers {
            abort: Duration::from_secs(60),
            worker_info: Duration::from_millis(25),
            ..test_timers()
        })
        .shutdown_grace(Duration::from_secs(10))
        .build()
        .unwrap();
    let worker_id = worker.id();
    let token = CancellationToken::new();
    let mut run = tokio::spawn(worker.run_until(token.clone()));

    wait_for_lock_waiter(&control, DEQUEUE_STATEMENT, "worker did not pause while dequeuing").await;
    let mut worker_gate = control.queue.pool().begin().await.unwrap();
    sqlx::query("UPDATE pgqueue.workers SET expires_at = now() - interval '1 second' WHERE id = $1")
        .bind(worker_id)
        .execute(&mut *worker_gate)
        .await
        .unwrap();

    token.cancel();
    wait_for_lock_waiter(
        &control,
        "%UPDATE pgqueue.workers SET accepting = false%",
        "shutdown did not freeze worker intake",
    )
    .await;
    dequeue_gate.rollback().await.unwrap();
    wait_for_lock_waiter(&control, "%WITH requeued AS (%", "fetcher did not begin draining its buffered job").await;
    assert_eq!(handle.fetch_job().await.unwrap().status, JobStatus::Running);

    worker_gate.rollback().await.unwrap();
    wait_for_worker_intake_closed(&control, worker_id).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(250), &mut run).await.is_err(),
        "worker returned before its buffered job was requeued"
    );

    requeue_gate.rollback().await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .expect("worker did not finish after its buffered job was requeued")
        .unwrap()
        .unwrap();

    let row = handle.fetch_job().await.unwrap();
    assert_eq!(counter.load(Ordering::SeqCst), 0);
    assert_eq!(row.status, JobStatus::Queued);
    assert_eq!(row.error.as_deref(), Some("cancelled"));
    assert_eq!(db.queue.stats().retried, 1);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_graceful_shutdown_finalizes_abort_when_job_is_buffered(pool: PgPool) {
    let control = TestDb::new(pool_with_max(&pool, 5).await).await;
    let db = TestDb::new(pool_with_max(&pool, 2).await).await;
    let counter = Arc::new(AtomicU32::new(0));
    let probe = DatabaseLossProbe {
        started: Arc::new(tokio::sync::Notify::new()),
        release: Arc::new(tokio::sync::Notify::new()),
    };
    let grace_job = control.queue.enqueue(quick_nap::job(())).await.unwrap().unwrap();

    let worker = test_worker(db.queue.clone())
        .register_job(counts)
        .register_job(quick_nap)
        .state(counter.clone())
        .state(probe.clone())
        .concurrency(3)
        .timers(WorkerTimers { abort: Duration::from_secs(60), worker_info: Duration::from_secs(60), ..test_timers() })
        .shutdown_grace(Duration::from_secs(10))
        .build()
        .unwrap();
    let worker_id = worker.id();
    let token = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(token.clone()));

    tokio::time::timeout(Duration::from_secs(5), probe.started.notified()).await.expect("grace job did not start");
    install_dequeue_handoff_gate(control.queue.pool()).await;
    let dequeue_gate = hold_gate(control.queue.pool(), DEQUEUE_HANDOFF_GATE, &control.database).await;
    let connection_gate = hold_gate(control.queue.pool(), DEQUEUE_CONNECTION_GATE, &control.database).await;
    let handle = control.queue.enqueue(counts::job(()).timeout(Duration::from_secs(60 * 60))).await.unwrap().unwrap();
    wait_for_lock_waiter(&control, DEQUEUE_STATEMENT, "worker did not pause while dequeuing").await;

    let mut worker_gate = control.queue.pool().begin().await.unwrap();
    sqlx::query("UPDATE pgqueue.workers SET expires_at = now() - interval '1 second' WHERE id = $1")
        .bind(worker_id)
        .execute(&mut *worker_gate)
        .await
        .unwrap();
    token.cancel();
    wait_for_lock_waiter(
        &control,
        "%UPDATE pgqueue.workers SET accepting = false%",
        "shutdown did not freeze worker intake",
    )
    .await;

    let worker_pool = db.queue.pool().clone();
    let database = db.database.clone();
    let mut connection_stealer = tokio::spawn(async move {
        sqlx::query("SELECT pg_advisory_xact_lock($1, hashtext($2))")
            .bind(DEQUEUE_CONNECTION_GATE)
            .bind(database)
            .execute(&worker_pool)
            .await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut connection_stealer).await.is_err(),
        "worker pool connections were not occupied by dequeue and shutdown"
    );

    dequeue_gate.rollback().await.unwrap();
    wait_for_lock_waiter(
        &control,
        "%SELECT pg_advisory_xact_lock($1, hashtext($2))%",
        "worker connection was not held after committing its dequeue",
    )
    .await;
    assert_eq!(handle.fetch_job().await.unwrap().status, JobStatus::Running);

    assert!(handle.abort("buffered abort").await.unwrap());
    assert_eq!(handle.fetch_job().await.unwrap().status, JobStatus::Aborting);
    connection_gate.rollback().await.unwrap();
    connection_stealer.await.unwrap().unwrap();
    wait_for_lock_waiter(&control, "%WITH requeued AS (%", "fetcher did not take responsibility for the buffered job")
        .await;
    worker_gate.rollback().await.unwrap();

    let aborted = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let row = handle.fetch_job().await.unwrap();
            if row.status == JobStatus::Aborted {
                return row;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    probe.release.notify_one();
    let run_result =
        tokio::time::timeout(Duration::from_secs(5), run).await.expect("worker did not finish graceful shutdown");

    let row = aborted.expect("buffered abort was not finalized");
    run_result.unwrap().unwrap();
    assert_eq!(counter.load(Ordering::SeqCst), 0);
    assert_eq!(row.status, JobStatus::Aborted);
    assert_eq!(row.error.as_deref(), Some("buffered abort"));
    assert!(row.completed_at.is_some());
    assert_eq!(grace_job.fetch_job().await.unwrap().status, JobStatus::Complete);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_run_until_requeues_inflight_jobs_when_aborted(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let handle = db.queue.enqueue(slow_but_abortable::job(())).await.unwrap().unwrap();
    let worker = test_worker(db.queue.clone())
        .register_job(slow_but_abortable)
        .shutdown_grace(Duration::from_millis(100))
        .build()
        .unwrap();
    let worker_id = worker.id();
    let run = tokio::spawn(worker.run_until(CancellationToken::new()));

    wait_status(&db.queue, handle.id(), JobStatus::Running, 10).await;
    run.abort();
    assert!(run.await.unwrap_err().is_cancelled());

    let row = wait_status(&db.queue, handle.id(), JobStatus::Queued, 10).await;
    assert_eq!(row.error.as_deref(), Some("cancelled"));
    assert_eq!(row.attempts, 1, "execution history remains monotonic");
    assert_eq!(row.max_attempts, 2, "shutdown refunds the retry budget");
    wait_for_some(
        Duration::from_secs(5),
        Duration::from_millis(25),
        "aborted run_until left a live worker lease",
        || async { list_workers(&db.queue).await.iter().all(|worker| worker.id != worker_id).then_some(()) },
    )
    .await;
}

#[sqlx::test(migrations = "./migrations")]
async fn test_burst_mode_drains_the_queue_and_exits(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let counter = Arc::new(AtomicU32::new(0));
    for _ in 0..3 {
        db.queue.enqueue(counts::job(())).await.unwrap().unwrap();
    }

    let worker = test_worker(db.queue.clone())
        .register_job(counts)
        .state(counter.clone())
        .burst(true)
        .dequeue_timeout(Duration::from_millis(400))
        .concurrency(2)
        .build()
        .unwrap();

    // No external cancellation: burst mode returns when drained.
    tokio::time::timeout(Duration::from_secs(15), worker.run_until(CancellationToken::new()))
        .await
        .expect("burst worker should exit on its own")
        .unwrap();
    assert_eq!(counter.load(Ordering::SeqCst), 3);
}

/// A row lock a burst worker keeps losing the race for is normally another claim in flight, gone again within a round
/// trip, so the fetcher spends `dequeue_timeout` waiting the row out rather than concluding a drain over it.
///
/// That wait shows only once the processor's own idle timeout has run out. A processor leaves when its timeout has
/// passed *and* the fetcher has concluded a drain, and both clocks start when it asks for work, so a lock released
/// inside the timeout hands its row over either way, and a test built on that passed with the wait removed. So the
/// clocks are separated here. The first claim meets the locked row, then closed intake holds the fetcher, which
/// restarts its wait without concluding anything, until the processor's timeout has passed. When intake reopens with
/// the row still locked, only the fetcher's wait keeps the run going. A worker that concluded a drain over the locked
/// row has let its one processor go by then, and the row stays queued.
//noinspection SqlNoDataSourceInspection
#[sqlx::test(migrations = "./migrations")]
async fn test_burst_waits_for_ready_job_when_row_is_locked(pool: PgPool) {
    // Closing intake below has to land inside the fetcher's first wait for the row, or the worker gives the row up
    // before the test has separated anything; five seconds leaves that room on a loaded runner.
    const DEQUEUE_TIMEOUT: Duration = Duration::from_secs(5);
    let control = TestDb::new(pool_with_max(&pool, 2).await).await.queue;
    let db = TestDb::new(pool_with_max(&pool, 1).await).await;
    let Some(mut stats) = Stats::new(&db.database).await else {
        Stats::skipped("test_burst_waits_for_ready_job_when_row_is_locked");
        return;
    };
    let handle = db.queue.enqueue(counts::job(())).await.unwrap().unwrap();
    let mut lock = control.pool().begin().await.unwrap();
    sqlx::query("SELECT id FROM pgqueue.jobs WHERE id = $1 FOR UPDATE")
        .bind(handle.id())
        .fetch_one(&mut *lock)
        .await
        .unwrap();

    let counter = Arc::new(AtomicU32::new(0));
    let worker = test_worker(db.queue.clone())
        .register_job(counts)
        .state(counter.clone())
        // A heartbeat inside the test would move the lease this test closes by hand.
        .timers(WorkerTimers { worker_info: Duration::from_secs(30), ..test_timers() })
        // One processor, so a drain concluded at the first claim covers every processor there is.
        .concurrency(1)
        .burst(true)
        .dequeue_timeout(DEQUEUE_TIMEOUT)
        // Ends the run with its one job instead of idling out another `dequeue_timeout`.
        .max_burst_jobs(1)
        .build()
        .unwrap();
    let worker_id = worker.id();
    let started = tokio::time::Instant::now();
    let run = tokio::spawn(worker.run_until(CancellationToken::new()));

    // Counted from a visible lease, so the claim waited for here met the locked row with intake open.
    wait_for_worker_lease(&control, worker_id).await;
    let claims = stats.since_now(DEQUEUE_CLAIM).await;
    stats.wait_for_calls(&claims, 1, "the worker never tried to claim the locked row").await;
    sqlx::query("UPDATE pgqueue.workers SET accepting = false, expires_at = now() - interval '1 second' WHERE id = $1")
        .bind(worker_id)
        .execute(control.pool())
        .await
        .unwrap();

    tokio::time::sleep_until(started + DEQUEUE_TIMEOUT + Duration::from_secs(1)).await;
    assert!(!run.is_finished(), "a locked ready row must not make a burst worker report a drain");

    control.write_worker_info(worker_id, json!({}), None, Duration::from_secs(30)).await.unwrap();
    let claims = stats.since_now(DEQUEUE_CLAIM).await;
    stats.wait_for_calls(&claims, 1, "the worker never claimed again once intake reopened").await;
    assert_eq!(handle.fetch_job().await.unwrap().status, JobStatus::Queued);

    lock.rollback().await.unwrap();
    tokio::time::timeout(Duration::from_secs(20), run)
        .await
        .expect("worker should process the row after its lock is released")
        .unwrap()
        .unwrap();
    assert_eq!(counter.load(Ordering::SeqCst), 1, "the worker left the locked row behind");
    assert_eq!(handle.fetch_job().await.unwrap().status, JobStatus::Complete);
}

/// And the same wait has to end. The claim takes its candidates
/// `FOR UPDATE ... SKIP LOCKED` while the availability probe beside it does
/// not, so one `queued`, due, name-matching row under a row lock held by an
/// unrelated open transaction — an operator's `SELECT ... FOR UPDATE`, an
/// uncommitted `UPDATE`, a session simply left idle in a transaction — reports
/// work that no claim can ever take. Burst demand stayed outstanding for
/// exactly as long as that lock was held, no processor could conclude a drain,
/// and `run_until` hung: for minutes, for hours, for as long as somebody's psql
/// window stayed open. `max_burst_jobs` did not bound it either — that budget is
/// only spent per job actually processed.
//noinspection SqlNoDataSourceInspection
#[sqlx::test(migrations = "./migrations")]
async fn test_burst_returns_while_an_idle_transaction_holds_a_due_row(pool: PgPool) {
    let control = TestDb::new(pool_with_max(&pool, 2).await).await.queue;
    let db = TestDb::new(pool_with_max(&pool, 1).await).await;
    let handle = db.queue.enqueue(counts::job(())).await.unwrap().unwrap();
    // Never released: the session holding it is not coming back.
    let mut lock = control.pool().begin().await.unwrap();
    sqlx::query("SELECT id FROM pgqueue.jobs WHERE id = $1 FOR UPDATE")
        .bind(handle.id())
        .fetch_one(&mut *lock)
        .await
        .unwrap();

    let counter = Arc::new(AtomicU32::new(0));
    let worker = test_worker(db.queue.clone())
        .register_job(counts)
        .state(counter.clone())
        .burst(true)
        .dequeue_timeout(Duration::from_millis(200))
        .build()
        .unwrap();

    tokio::time::timeout(Duration::from_secs(15), worker.run_until(CancellationToken::new()))
        .await
        .expect("a burst worker must return while a due row stays locked")
        .unwrap();

    // Given up rather than consumed: the row is untouched and still due, so the
    // next run — burst or continuous — has it.
    assert_eq!(counter.load(Ordering::SeqCst), 0);
    let row = handle.fetch_job().await.unwrap();
    assert_eq!(row.status, JobStatus::Queued);
    assert_eq!(row.attempts, 0, "a row never claimed must not lose attempts");
    lock.rollback().await.unwrap();
}

//noinspection SqlNoDataSourceInspection
#[sqlx::test(migrations = "./migrations")]
async fn test_worker_skips_locked_ready_row_and_processes_next(pool: PgPool) {
    let control = TestDb::new(pool_with_max(&pool, 2).await).await.queue;
    let db = TestDb::new(pool_with_max(&pool, 1).await).await;
    let locked = db.queue.enqueue(counts::job(())).await.unwrap().unwrap();
    let next = db.queue.enqueue(counts::job(())).await.unwrap().unwrap();
    let mut lock = control.pool().begin().await.unwrap();
    sqlx::query("SELECT id FROM pgqueue.jobs WHERE id = $1 FOR UPDATE")
        .bind(locked.id())
        .fetch_one(&mut *lock)
        .await
        .unwrap();

    let counter = Arc::new(AtomicU32::new(0));
    let worker = test_worker(db.queue.clone())
        .register_job(counts)
        .state(counter.clone())
        .concurrency(1)
        .burst(true)
        // The timeout has to outlast the wait below: it is the budget the burst
        // spends on the locked row, and this test is about coming back for that
        // row once it is released, not about giving up on it.
        .dequeue_timeout(Duration::from_secs(3))
        .build()
        .unwrap();
    let run = tokio::spawn(worker.run_until(CancellationToken::new()));

    wait_for_some(Duration::from_secs(2), Duration::from_millis(10), "worker did not skip the locked row", || {
        let counter = counter.clone();
        let next = next.clone();
        async move {
            (counter.load(Ordering::SeqCst) == 1 && next.fetch_job().await.ok()?.status == JobStatus::Complete)
                .then_some(())
        }
    })
    .await;
    assert_eq!(locked.fetch_job().await.unwrap().status, JobStatus::Queued);
    assert_eq!(next.fetch_job().await.unwrap().status, JobStatus::Complete);

    lock.rollback().await.unwrap();
    tokio::time::timeout(Duration::from_secs(20), run)
        .await
        .expect("worker should process the locked row after release")
        .unwrap()
        .unwrap();
    assert_eq!(counter.load(Ordering::SeqCst), 2);
    assert_eq!(locked.fetch_job().await.unwrap().status, JobStatus::Complete);
}

//noinspection SqlNoDataSourceInspection
#[sqlx::test(migrations = "./migrations")]
async fn test_burst_does_not_drain_when_worker_intake_is_closed(pool: PgPool) {
    let control = TestDb::new(pool_with_max(&pool, 2).await).await.queue;
    let db = TestDb::new(pool_with_max(&pool, 1).await).await;
    let handle = db.queue.enqueue(counts::job(())).await.unwrap().unwrap();
    // Holding the only ready row keeps the worker's first fetch empty until the
    // test has closed its intake lease. Those fetches see work they cannot
    // claim, which a burst worker gives up on after one `dequeue_timeout` — so
    // the timeout below has to outlast the worker's startup, and the window
    // this test then observes has to outlast the timeout, or an intake-gated
    // result really satisfying burst drain would go unnoticed inside it
    // (`next_job` returns `Drained` only once the idle deadline has also
    // elapsed, and that deadline is this same value).
    //
    // Both clocks start at worker startup, so at one second the test had to
    // observe the lease *and* close intake inside it or the worker legitimately
    // drained. Five leaves that room on a runner busy with the rest of this
    // suite; the observation below is widened to match, and neither number is
    // load-bearing beyond the ordering between them.
    let mut lock = control.pool().begin().await.unwrap();
    sqlx::query("SELECT id FROM pgqueue.jobs WHERE id = $1 FOR UPDATE")
        .bind(handle.id())
        .fetch_one(&mut *lock)
        .await
        .unwrap();
    let counter = Arc::new(AtomicU32::new(0));
    let worker = test_worker(db.queue.clone())
        .register_job(counts)
        .state(counter.clone())
        .timers(WorkerTimers { worker_info: Duration::from_secs(10), ..test_timers() })
        .burst(true)
        .dequeue_timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let worker_id = worker.id();
    let run = tokio::spawn(worker.run_until(CancellationToken::new()));

    wait_for_some(Duration::from_secs(5), Duration::from_millis(10), "worker did not start", || async {
        list_workers(&control).await.iter().any(|worker| worker.id == worker_id).then_some(())
    })
    .await;
    // Keep the closed lease live: an expired one can be deleted by the startup
    // sweep, after which a heartbeat creates an accepting replacement.
    sqlx::query(
        "UPDATE pgqueue.workers
         SET accepting = false, expires_at = clock_timestamp() + interval '30 seconds'
         WHERE id = $1",
    )
    .bind(worker_id)
    .execute(control.pool())
    .await
    .unwrap();
    lock.rollback().await.unwrap();

    tokio::time::sleep(Duration::from_secs(7)).await;
    assert!(!run.is_finished(), "an intake-gated empty result must not satisfy burst drain");
    assert_eq!(handle.fetch_job().await.unwrap().status, JobStatus::Queued);

    control.write_worker_info(worker_id, json!({}), None, Duration::from_secs(30)).await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), run)
        .await
        .expect("worker should continue after its intake lease is restored")
        .unwrap()
        .unwrap();
    assert_eq!(counter.load(Ordering::SeqCst), 1);
    assert_eq!(handle.fetch_job().await.unwrap().status, JobStatus::Complete);
}

/// A burst whose dequeues keep failing used to hang forever: the `Err` arm of
/// the fill loop reset the unclaimable clock and never touched
/// `drained_generation`, so no processor could reach `WorkerFetch::Drained`,
/// `wait_for_processors` never completed, and `run_until` never returned.
/// `max_burst_jobs` did not bound it either — budget is only spent per job
/// actually processed — so a cron or CI invocation pointed at a database that
/// had gone away simply never exited.
///
/// It must also not end as a *drain*: an unreachable database and an empty queue
/// are different outcomes, and only one of them means the work is done.
#[sqlx::test(migrations = "./migrations")]
async fn test_burst_fails_instead_of_hanging_when_dequeues_keep_failing(pool: PgPool) {
    let db = TestDb::new(pool_with_max(&pool, 2).await).await;
    // Statement-level, so it fires even though the claim's `UPDATE` matches no
    // row: this is a dequeue that never reaches an answer, exactly as one
    // against a database that has gone away does not.
    sqlx::raw_sql(sqlx::AssertSqlSafe(
        "CREATE FUNCTION pgqueue.repro_fail_dequeue() RETURNS trigger
         LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'dequeue is unavailable'; END $$;
         CREATE TRIGGER repro_fail_dequeue
         BEFORE UPDATE ON pgqueue.jobs
         FOR EACH STATEMENT EXECUTE FUNCTION pgqueue.repro_fail_dequeue();"
            .to_string(),
    ))
    .execute(db.queue.pool())
    .await
    .unwrap();

    let error = tokio::time::timeout(
        Duration::from_secs(20),
        test_worker(db.queue.clone())
            .register_job(counts)
            .state(Arc::new(AtomicU32::new(0)))
            .burst(true)
            .dequeue_timeout(Duration::from_secs(1))
            .max_burst_jobs(1)
            .build()
            .unwrap()
            .run_until(CancellationToken::new()),
    )
    .await
    .expect("a burst whose dequeues all fail must still return")
    .expect_err("a burst that never reached the database must not report a drain");
    assert!(
        error.to_string().contains("dequeue is unavailable"),
        "the burst must report the failure that ended it, not a generic task exit: {error}"
    );
}

/// The finalization half of the same contract. `finalize` retries a failed outcome write for as long as the attempt
/// lives, and in a burst nothing ever ended that: with every processor pinned in the retry loop none goes idle, so the
/// fetch loop never dequeues and the failed-dequeue budget above is never spent. A burst whose database became
/// unreachable while its attempts were in flight therefore never returned — a cron or CI invocation that hung rather
/// than failing, exactly what `WorkerBuilder::burst` documents it does not do.
#[sqlx::test(migrations = "./migrations")]
async fn test_burst_fails_instead_of_hanging_when_finalization_keeps_failing(pool: PgPool) {
    let db = TestDb::new(pool_with_max(&pool, 4).await).await;
    let handle = db.queue.enqueue(repro_noop::job(())).await.unwrap().unwrap();
    sqlx::raw_sql(
        r#"
        CREATE FUNCTION pgqueue.repro_always_fail_finalization() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            RAISE EXCEPTION 'injected persistent finalization failure';
        END
        $$;
        CREATE TRIGGER repro_always_fail_finalization
        BEFORE UPDATE ON pgqueue.jobs
        FOR EACH ROW
        WHEN (OLD.status = 'running' AND NEW.status IS DISTINCT FROM OLD.status)
        EXECUTE FUNCTION pgqueue.repro_always_fail_finalization();
        "#,
    )
    .execute(db.queue.pool())
    .await
    .expect("install persistent finalization failure");

    let error = tokio::time::timeout(
        Duration::from_secs(30),
        test_worker(db.queue.clone())
            .register_job(repro_noop)
            .concurrency(1)
            .burst(true)
            .dequeue_timeout(Duration::from_secs(1))
            .shutdown_grace(Duration::from_millis(500))
            .build()
            .unwrap()
            .run_until(CancellationToken::new()),
    )
    .await
    .expect("a burst whose finalizations all fail must still return")
    .expect_err("a burst that could not record its outcomes must not report a drain");
    assert!(
        error.to_string().contains("injected persistent finalization failure"),
        "the burst must report the failure that ended it: {error}"
    );
    // Left for recovery rather than lost: the attempt is still the worker's own, and its lease is gone.
    assert_eq!(handle.fetch_job().await.unwrap().status, JobStatus::Running);
}

/// A burst whose registered cron could not be reconciled or published used to
/// report success: `schedule_burst_crons` converted every failure into health
/// state and returned `()`, the run drained an empty queue, and `run_until`
/// answered `Ok(())` with the due occurrence unpublished or unknowable — an
/// exit code of zero for a cron invocation that did nothing. Scheduling
/// trouble now retries within `dequeue_timeout` and then fails the run, the
/// same contract failed dequeues already carry.
//noinspection SqlNoDataSourceInspection
#[sqlx::test(migrations = "./migrations")]
async fn test_burst_fails_when_cron_scheduling_cannot_settle(pool: PgPool) {
    let db = TestDb::new(pool_with_max(&pool, 2).await).await;
    // Every write to the schedule table fails, so reconciliation — the step
    // that decides whether an occurrence is even due — can never settle.
    sqlx::raw_sql(sqlx::AssertSqlSafe(
        "CREATE FUNCTION pgqueue.repro_fail_cron_schedules() RETURNS trigger
         LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'cron schedules are unavailable'; END $$;
         CREATE TRIGGER repro_fail_cron_schedules
         BEFORE INSERT OR UPDATE ON pgqueue.cron_schedules
         FOR EACH STATEMENT EXECUTE FUNCTION pgqueue.repro_fail_cron_schedules();"
            .to_string(),
    ))
    .execute(db.queue.pool())
    .await
    .unwrap();

    let error = tokio::time::timeout(
        Duration::from_secs(20),
        test_worker(db.queue.clone())
            .register_job(counts)
            .state(Arc::new(AtomicU32::new(0)))
            .schedule_cron("* * * * *", counts::job(()))
            .burst(true)
            .dequeue_timeout(Duration::from_millis(300))
            .build()
            .unwrap()
            .run_until(CancellationToken::new()),
    )
    .await
    .expect("a burst whose cron scheduling keeps failing must still return")
    .expect_err("a burst that could not settle its crons must not report a drain");
    assert!(matches!(error, Error::WorkerTask(_)), "{error}");
}

/// A burst whose worker lease cannot be written used to hang forever: the
/// startup lease write only logs its failure, the fetcher then receives empty
/// batches with `intake_open = false`, and that arm neither concludes a drain
/// (nothing is claimable without a live accepting lease) nor counted against
/// any budget — `run_until` simply never returned. With
/// `WorkerComponent::WorkerInfo` failing the whole time, the run now ends as
/// an error within `dequeue_timeout`. A lease closed by an outside actor while
/// heartbeats still land keeps the wait-for-reopen behavior pinned by
/// `test_burst_does_not_drain_when_worker_intake_is_closed`.
//noinspection SqlNoDataSourceInspection
#[sqlx::test(migrations = "./migrations")]
async fn test_burst_fails_when_its_worker_lease_cannot_be_written(pool: PgPool) {
    let db = TestDb::new(pool_with_max(&pool, 2).await).await;
    sqlx::raw_sql(sqlx::AssertSqlSafe(
        "CREATE FUNCTION pgqueue.repro_fail_workers() RETURNS trigger
         LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'worker leases are unavailable'; END $$;
         CREATE TRIGGER repro_fail_workers
         BEFORE INSERT OR UPDATE ON pgqueue.workers
         FOR EACH STATEMENT EXECUTE FUNCTION pgqueue.repro_fail_workers();"
            .to_string(),
    ))
    .execute(db.queue.pool())
    .await
    .unwrap();

    let error = tokio::time::timeout(
        Duration::from_secs(20),
        test_worker(db.queue.clone())
            .register_job(counts)
            .state(Arc::new(AtomicU32::new(0)))
            .burst(true)
            .dequeue_timeout(Duration::from_millis(500))
            .build()
            .unwrap()
            .run_until(CancellationToken::new()),
    )
    .await
    .expect("a burst whose lease cannot be written must still return")
    .expect_err("a leaseless burst must not report a drain");
    assert!(matches!(&error, Error::WorkerTask(message) if message.contains("lease")), "{error}");
}

/// The claim uses `FOR UPDATE SKIP LOCKED` and the availability probe does not,
/// so a `queued`, due, name-matching row held under a row lock by an unrelated
/// open transaction reports work that no claim can ever take. The fill loop's
/// "empty batch but work is available" arm only backed off, capped at
/// `DEQUEUE_RETRY_MAX_MS` (100 ms), and never broke out — so every idle worker in
/// the fleet was pinned there issuing a claim *and* a probe roughly every 50 ms,
/// ignoring `poll_interval` entirely, for as long as the lock was held. Measured
/// before the fix at 45 claims + 45 probes in 2 s against the ~2 a one-second
/// poll interval implies, per worker.
//noinspection SqlNoDataSourceInspection
#[sqlx::test(migrations = "./migrations")]
async fn test_idle_worker_polls_at_its_poll_interval_while_a_ready_row_is_locked(pool: PgPool) {
    let control = TestDb::new(pool_with_max(&pool, 2).await).await.queue;
    let db = TestDb::new(pool_with_max(&pool, 2).await).await;
    let Some(mut stats) = Stats::new(&db.database).await else {
        Stats::skipped("test_idle_worker_polls_at_its_poll_interval_while_a_ready_row_is_locked");
        return;
    };
    let handle = db.queue.enqueue(counts::job(())).await.unwrap().unwrap();
    let mut lock = control.pool().begin().await.unwrap();
    sqlx::query("SELECT id FROM pgqueue.jobs WHERE id = $1 FOR UPDATE")
        .bind(handle.id())
        .fetch_one(&mut *lock)
        .await
        .unwrap();

    let counter = Arc::new(AtomicU32::new(0));
    // Continuous, not burst: `poll_interval` is the whole contract here, and
    // `WorkerFetch::Drained` is unreachable without `dequeue_timeout` anyway.
    let worker = test_worker(db.queue.clone())
        .register_job(counts)
        .state(counter.clone())
        .timers(WorkerTimers {
            worker_info: Duration::from_secs(10),
            schedule: Duration::from_secs(10),
            ..test_timers()
        })
        .poll_interval(Duration::from_secs(1))
        .build()
        .unwrap();
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));

    // Let the startup ramp finish before measuring, so the window covers the
    // steady state the fix is about rather than the deliberate initial backoff.
    tokio::time::sleep(Duration::from_millis(700)).await;
    let claims = stats.since_now(DEQUEUE_CLAIM).await;
    let probes = stats.since_now(DEQUEUE_PROBE).await;

    // Timed by the loop's own passes, not by a wall clock: wait for five claims
    // and assert how long they took. Counting claims inside a fixed sleep bounds
    // the rate only if the sleep really is the window it claims to be, and on a
    // loaded runner or a slow database it is not — the count is server-side, so
    // no paused clock can fix that. Phrased this way, anything slow only makes
    // the window *longer*, which is the direction that passes; `wait_for_calls`
    // fails after 30s if the loop stopped claiming altogether, which is the
    // failure the old lower bound of 1 was there for.
    let opened = tokio::time::Instant::now();
    stats.wait_for_calls(&claims, 5, "an idle worker stopped claiming altogether").await;
    let window = opened.elapsed();
    let claims = stats.delta(&claims).await;
    let probes = stats.delta(&probes).await;

    // Five passes at a one-second `poll_interval` measure 4.4s here. The pre-fix
    // loop measured 45 claims in 2s — the backoff saturated at
    // `DEQUEUE_RETRY_MAX_MS`, ~22 per second — and would reach five in about
    // 0.2s, so 3s leaves a third of the margin on this side and an order of
    // magnitude on the other.
    assert!(
        window >= Duration::from_secs(3),
        "an idle worker made {claims} claims in {window:?} at a 1s poll interval"
    );
    // The probe rides the same pass as the claim, so it is bounded by the same
    // thing rather than by the window — one extra for a pass that lands between
    // the two reads above.
    assert!(probes <= claims + 1, "an idle worker issued {probes} availability probes against {claims} claims");
    assert_eq!(handle.fetch_job().await.unwrap().status, JobStatus::Queued);

    // Backing off must not mean giving up: the row is still claimed promptly
    // once its lock is released.
    lock.rollback().await.unwrap();
    wait_terminal(&db.queue, handle.id(), 10).await;
    assert_eq!(counter.load(Ordering::SeqCst), 1);
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(10), run).await.expect("worker did not stop").unwrap().unwrap();
}

#[sqlx::test(migrations = "./migrations")]
async fn test_max_burst_jobs_caps_processing_even_under_concurrency(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let counter = Arc::new(AtomicU32::new(0));
    for _ in 0..10 {
        db.queue.enqueue(counts::job(())).await.unwrap().unwrap();
    }

    // Ten processors race a cap of 2: the budget must hold exactly.
    let worker = test_worker(db.queue.clone())
        .register_job(counts)
        .state(counter.clone())
        .burst(true)
        .dequeue_timeout(Duration::from_millis(400))
        .max_burst_jobs(2)
        .concurrency(10)
        .build()
        .unwrap();

    tokio::time::timeout(Duration::from_secs(15), worker.run_until(CancellationToken::new()))
        .await
        .expect("capped burst worker should exit")
        .unwrap();
    assert_eq!(counter.load(Ordering::SeqCst), 2, "cap must hold exactly");
    assert_eq!(db.queue.counts().await.unwrap().queued, 8, "remaining jobs left untouched");
}

/// One sweep tick drains far more than four full batches. The production loop
/// also has a one-second wall-clock budget, which can expire after a single
/// pass when the test runner is starved by another test's compilation. This
/// test gives the same loop a test-owned time budget so the pass limit is the
/// only subject. The tick interval is a minute, so nothing below can be
/// finished by a second tick.
#[sqlx::test(migrations = "./migrations")]
async fn test_sweep_loop_drains_more_than_four_full_batches_per_tick(pool: PgPool) {
    let db = TestDb::with(pool.clone(), |builder| builder.sweep_batch_size(1)).await;
    sqlx::query(
        r#"
        INSERT INTO pgqueue.jobs (
            queue, name, payload, status, completed_at, expires_at
        )
        SELECT $1, 'expired-worker-batch', 'null'::jsonb, 'complete', now(),
               now() - interval '1 second'
        FROM generate_series(1, 5)
        "#,
    )
    .bind(db.queue.name())
    .execute(db.queue.pool())
    .await
    .unwrap();

    let quiet = Duration::from_secs(60);
    let worker = test_worker(db.queue.clone())
        .register_job(always_fails)
        .timers(WorkerTimers { sweep: quiet, abort: quiet, schedule: quiet, worker_info: quiet })
        .build()
        .unwrap();
    let shutdown = CancellationToken::new();
    let run =
        tokio::spawn(pgqueue::__test_support::run_worker_sweeper(worker, shutdown.clone(), Duration::from_secs(60)));

    wait_until(
        Duration::from_secs(20),
        Duration::from_millis(20),
        "worker did not drain all expired sweep batches on its first tick",
        || async {
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM pgqueue.jobs WHERE queue = $1 AND name = 'expired-worker-batch'",
            )
            .bind(db.queue.name())
            .fetch_one(db.queue.pool())
            .await
            .unwrap()
                == 0
        },
    )
    .await;

    shutdown.cancel();
    run.await.unwrap();
}

#[sqlx::test(migrations = "./migrations")]
async fn test_live_worker_retries_a_sweeper_cancelled_attempt(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let attempts = Arc::new(AtomicU32::new(0));
    let handle = db.queue.enqueue(swept_once::job(())).await.unwrap().unwrap();
    // Hold sweep leadership outside the worker so phase 2 cannot race the live
    // worker's abort poll and hide a terminal-abort regression.
    let mut sweeper = db.queue.sweeper();
    assert!(sweeper.sweep().await.unwrap().leader);
    let worker = test_worker(db.queue.clone())
        .register_job(swept_once)
        .state(attempts.clone())
        .timers(WorkerTimers { abort: Duration::from_millis(20), sweep: Duration::from_secs(60), ..test_timers() })
        .concurrency(1)
        .build()
        .unwrap();
    let token = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(token.clone()));

    wait_status(&db.queue, handle.id(), JobStatus::Running, 5).await;
    backdate_job_liveness(&db, handle.id()).await;
    let report = sweeper.sweep().await.unwrap();
    assert_eq!(report.cancelling, vec![handle.id()]);
    let row = wait_terminal(&db.queue, handle.id(), 10).await;
    assert_eq!(row.status, JobStatus::Complete);
    assert_eq!(row.attempts, 2, "the swept first attempt must be retried");
    assert_eq!(attempts.load(Ordering::SeqCst), 2);

    token.cancel();
    run.await.unwrap().unwrap();
    sweeper.release().await;
}

#[sqlx::test(migrations = "./migrations")]
async fn test_abort_loop_cancels_handler_when_swept_row_is_deleted_before_observation(pool: PgPool) {
    let db = TestDb::with(pool.clone(), |builder| builder.sweep_batch_size(1)).await;
    let (started_tx, mut started_rx) = tokio::sync::mpsc::unbounded_channel();
    let (dropped_tx, mut dropped_rx) = tokio::sync::mpsc::unbounded_channel();
    let completed = Arc::new(AtomicU32::new(0));
    let worker = test_worker(db.queue.clone())
        .register_job(waits_after_row_deletion)
        .register_job(counts)
        .state(MissingRowProbe { started: started_tx, dropped: dropped_tx })
        .state(completed.clone())
        .timers(WorkerTimers { abort: Duration::from_millis(500), sweep: Duration::from_millis(25), ..test_timers() })
        .concurrency(1)
        .build()
        .unwrap();
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));

    // The sweeper finishes the row `aborted`, so the failure retention is the one that deletes it. Backdating the
    // attempt past its timeout is what makes the sweeper take it: the handler's own deadline is a minute away, so no
    // finish of the worker's can be what deletes the row. The sweeper marks the attempt in one pass and aborts it in
    // the next, 25ms later, so the abort poll — every 500ms — almost always first sees the row gone; seeing it marked
    // or finished stops the handler just as immediately.
    for (tag, retention) in [
        (1, JobRetention::DeleteImmediately),
        (2, JobRetention::For(Duration::from_millis(1))),
    ] {
        let handle =
            db.queue.enqueue(waits_after_row_deletion::job(tag).failed_retention(retention)).await.unwrap().unwrap();
        assert_eq!(tokio::time::timeout(Duration::from_secs(3), started_rx.recv()).await.unwrap(), Some(tag));
        backdate_job_liveness(&db, handle.id()).await;
        wait_until(
            Duration::from_secs(3),
            Duration::from_millis(10),
            "sweeper did not delete the stuck job row",
            || async { db.queue.fetch_job(handle.id()).await.unwrap().is_none() },
        )
        .await;
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), dropped_rx.recv()).await.unwrap(),
            Some(tag),
            "the abort poll did not cancel the handler of a row the sweeper deleted"
        );
    }

    let follow_up = db.queue.enqueue(counts::job(())).await.unwrap().unwrap();
    assert_eq!(
        wait_terminal(&db.queue, follow_up.id(), 3).await.status,
        JobStatus::Complete,
        "deleted attempts kept the worker's only processor slot"
    );
    assert_eq!(completed.load(Ordering::SeqCst), 1);

    shutdown.cancel();
    run.await.unwrap().unwrap();
}

/// Two workers, one queue each — a queue is the unit of worker capability, so
/// a fleet running different job types uses one queue per worker shape — and
/// each heartbeat reports only its own worker's outcomes.
#[sqlx::test(migrations = "./migrations")]
async fn test_worker_heartbeat_reports_only_jobs_processed_by_that_worker(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let other = db.another_queue(|builder| builder.name("other")).await;
    let counter = Arc::new(AtomicU32::new(0));
    let complete_worker = test_worker(db.queue.clone()).register_job(counts).state(counter).build().unwrap();
    let failed_worker = test_worker(other.clone()).register_job(always_fails).build().unwrap();
    let complete_worker_id = complete_worker.id();
    let failed_worker_id = failed_worker.id();

    let complete_job = db.queue.enqueue(counts::job(())).await.unwrap().unwrap();
    let failed_job = other.enqueue(always_fails::job(())).await.unwrap().unwrap();

    let complete_token = CancellationToken::new();
    let failed_token = CancellationToken::new();
    let complete_run = tokio::spawn(complete_worker.run_until(complete_token.clone()));
    let failed_run = tokio::spawn(failed_worker.run_until(failed_token.clone()));

    wait_terminal(&db.queue, complete_job.id(), 5).await;
    wait_terminal(&other, failed_job.id(), 5).await;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let complete_workers = list_workers(&db.queue).await;
        let failed_workers = list_workers(&other).await;
        let complete_stats =
            complete_workers.iter().find(|worker| worker.id == complete_worker_id).map(|worker| &worker.stats);
        let failed_stats =
            failed_workers.iter().find(|worker| worker.id == failed_worker_id).map(|worker| &worker.stats);
        if complete_stats.is_some_and(|stats| {
            stats["complete"] == 1 && stats["failed"] == 0 && stats["retried"] == 0 && stats["aborted"] == 0
        }) && failed_stats.is_some_and(|stats| {
            stats["complete"] == 0 && stats["failed"] == 1 && stats["retried"] == 1 && stats["aborted"] == 0
        }) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "worker heartbeats did not report isolated counters: {:?} / {:?}",
            complete_workers,
            failed_workers
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let aggregate = db.queue.stats();
    assert_eq!((aggregate.complete, aggregate.failed, aggregate.retried), (1, 0, 0));
    let aggregate = other.stats();
    assert_eq!((aggregate.complete, aggregate.failed, aggregate.retried), (0, 1, 1));

    complete_token.cancel();
    failed_token.cancel();
    complete_run.await.unwrap().unwrap();
    failed_run.await.unwrap().unwrap();
}

/// Attribution *within* one queue — the normal topology under the capability
/// contract, where every worker registers every job. Which worker runs which
/// job is nondeterministic, so the pinned property is the sum: per-worker
/// heartbeats must partition the queue's completions, not each republish the
/// queue-wide total.
#[sqlx::test(migrations = "./migrations")]
async fn test_worker_heartbeats_partition_completions_across_one_queue(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let counter = Arc::new(AtomicU32::new(0));
    let first = test_worker(db.queue.clone()).register_job(counts).state(counter.clone()).build().unwrap();
    let second = test_worker(db.queue.clone()).register_job(counts).state(counter.clone()).build().unwrap();
    let worker_ids = [first.id(), second.id()];

    let jobs: i64 = 6;
    let mut handles = Vec::new();
    for _ in 0..jobs {
        handles.push(db.queue.enqueue(counts::job(())).await.unwrap().unwrap());
    }

    let token = CancellationToken::new();
    let first_run = tokio::spawn(first.run_until(token.clone()));
    let second_run = tokio::spawn(second.run_until(token.clone()));
    for handle in &handles {
        wait_terminal(&db.queue, handle.id(), 10).await;
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let workers = list_workers(&db.queue).await;
        let per_worker: Vec<i64> = worker_ids
            .iter()
            .map(|id| {
                workers
                    .iter()
                    .find(|worker| worker.id == *id)
                    .and_then(|worker| worker.stats["complete"].as_i64())
                    .unwrap_or(0)
            })
            .collect();
        if per_worker.iter().sum::<i64>() == jobs {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "per-worker completions must sum to the queue's total, not republish it: {per_worker:?}",
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    token.cancel();
    first_run.await.unwrap().unwrap();
    second_run.await.unwrap().unwrap();
}

#[sqlx::test(migrations = "./migrations")]
async fn test_worker_lease_stays_live_during_processor_shutdown_grace(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let probe = ShutdownLeaseProbe { started: Arc::new(tokio::sync::Notify::new()) };
    db.queue.enqueue(holds_during_shutdown::job(())).await.unwrap().unwrap();
    let worker = test_worker(db.queue.clone())
        .register_job(holds_during_shutdown)
        .state(probe.clone())
        // Coverage instrumentation and the parallel Postgres suite can delay
        // the intake-close observation; leave enough grace to inspect the
        // still-live lease before the processor is cancelled. Two seconds was
        // not enough on a fully loaded machine.
        .shutdown_grace(Duration::from_secs(5))
        .build()
        .unwrap();
    let worker_id = worker.id();
    let token = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(token.clone()));
    tokio::time::timeout(Duration::from_secs(5), probe.started.notified())
        .await
        .expect("handler did not claim the job");

    token.cancel();
    wait_for_worker_intake_closed(&db, worker_id).await;
    assert!(
        list_workers(&db.queue).await.iter().any(|worker| worker.id == worker_id),
        "the lease expired while a processor still owned work"
    );
    run.await.unwrap().unwrap();
    assert!(!list_workers(&db.queue).await.iter().any(|worker| worker.id == worker_id));
}

#[sqlx::test(migrations = "./migrations")]
async fn test_worker_builder_rejects_invalid_configuration(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;

    let err = Worker::builder(db.queue.clone()).build().unwrap_err();
    assert!(err.to_string().contains("no jobs registered"), "{err}");

    let err = Worker::builder(db.queue.clone()).register_job(always_fails).burst(true).build().unwrap_err();
    assert!(err.to_string().contains("dequeue_timeout"), "{err}");

    let err = Worker::builder(db.queue.clone()).register_job(always_fails).max_burst_jobs(5).build().unwrap_err();
    assert!(err.to_string().contains("max_burst_jobs requires"), "{err}");

    Worker::builder(db.queue.clone()).register_job(always_fails).register_job(always_fails).build().unwrap();

    let err = Worker::builder(db.queue.clone())
        .register_job(registration_collision_a)
        .register_job(registration_collision_b)
        .build()
        .unwrap_err();
    assert!(err.to_string().contains("multiple job types"), "{err}");

    Worker::builder(db.queue.clone()).schedule_cron("0 * * * *", counts::job(())).build().unwrap();

    let err = Worker::builder(db.queue.clone()).schedule_cron("not a cron", counts::job(())).build().unwrap_err();
    assert!(err.to_string().contains("invalid cron expression"), "{err}");

    let err = Worker::builder(db.queue.clone())
        .schedule_cron("0 * * * *", counts::job(()))
        .schedule_cron("30 * * * *", counts::job(()))
        .build()
        .unwrap_err();
    assert!(err.to_string().contains("cron dedupe key"), "{err}");

    let err = Worker::builder(db.queue.clone())
        .register_job(always_fails)
        .timers(WorkerTimers { abort: Duration::ZERO, ..WorkerTimers::default() })
        .build()
        .unwrap_err();
    assert!(err.to_string().contains("abort timer"), "{err}");

    let err =
        Worker::builder(db.queue.clone()).register_job(always_fails).poll_interval(Duration::ZERO).build().unwrap_err();
    assert!(err.to_string().contains("poll interval"), "{err}");

    let err =
        Worker::builder(db.queue.clone()).register_job(always_fails).abort_grace(Duration::MAX).build().unwrap_err();
    assert!(err.to_string().contains("abort grace"), "{err}");

    let err = Worker::builder(db.queue.clone())
        .register_job(always_fails)
        .burst(true)
        .dequeue_timeout(Duration::ZERO)
        .build()
        .unwrap_err();
    assert!(err.to_string().contains("dequeue timeout"), "{err}");

    // Silently clamped to 1 once, alone among this builder's out-of-range
    // values: a worker asked to run nothing concurrently ran everything.
    let err = Worker::builder(db.queue.clone()).register_job(always_fails).concurrency(0).build().unwrap_err();
    assert!(err.to_string().contains("concurrency"), "{err}");

    if usize::BITS > i64::BITS {
        let err =
            Worker::builder(db.queue.clone()).register_job(always_fails).concurrency(usize::MAX).build().unwrap_err();
        assert!(err.to_string().contains("concurrency"), "{err}");
    }

    // Fitting `bigint` was the only ceiling, so `i64::MAX` built successfully
    // and the run then spawned that many processor tasks before it could report
    // readiness — measured at ~4.3 KB of resident memory per processor, so an
    // allocation loop ending at the OOM killer, where this builder documents
    // that every out-of-range value is *reported*.
    let err = Worker::builder(db.queue.clone()).register_job(always_fails).concurrency(65_537).build().unwrap_err();
    assert!(err.to_string().contains("concurrency"), "{err}");
    let err = Worker::builder(db.queue.clone())
        .register_job(always_fails)
        .concurrency(i64::MAX as usize)
        .build()
        .unwrap_err();
    assert!(err.to_string().contains("concurrency"), "{err}");
    // The bound itself is still accepted.
    Worker::builder(db.queue.clone()).register_job(always_fails).concurrency(65_536).build().expect("the cap builds");
}

#[sqlx::test(migrations = "./migrations")]
async fn test_worker_builder_run_until_rejects_invalid_configuration(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let error = Worker::builder(db.queue).run_until(CancellationToken::new()).await.unwrap_err();

    assert!(error.to_string().contains("no jobs registered"), "{error}");
}

#[derive(Clone)]
struct DatabaseLossProbe {
    started: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

#[pgqueue::job]
async fn quick_nap(_: (), state: JobState<DatabaseLossProbe>) -> anyhow::Result<()> {
    state.0.started.notify_one();
    state.0.release.notified().await;
    Ok(())
}

#[sqlx::test(migrations = "./migrations")]
async fn test_abort_health_recovers_when_no_attempts_remain(pool: PgPool) {
    let control = TestDb::new(pool.clone()).await;
    let worker_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(5)
        .connect_with(pool.connect_options().as_ref().clone().options([("lock_timeout", "100ms")]))
        .await
        .unwrap();
    let worker_db = TestDb::new(worker_pool).await;
    let probe = DatabaseLossProbe {
        started: Arc::new(tokio::sync::Notify::new()),
        release: Arc::new(tokio::sync::Notify::new()),
    };
    control.queue.enqueue(quick_nap::job(())).await.unwrap().unwrap();
    let worker = test_worker(worker_db.queue.clone())
        .register_job(quick_nap)
        .state(probe.clone())
        .timers(WorkerTimers { abort: Duration::from_secs(2), ..test_timers() })
        .concurrency(1)
        .build()
        .unwrap();
    let health = worker.health();
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));
    tokio::time::timeout(Duration::from_secs(5), probe.started.notified()).await.expect("job did not start");

    let mut lock = control.queue.pool().begin().await.unwrap();
    sqlx::query("LOCK TABLE pgqueue.jobs IN ACCESS EXCLUSIVE MODE").execute(&mut *lock).await.unwrap();
    wait_until(Duration::from_secs(5), Duration::from_millis(10), "abort poll failure was not reported", || async {
        health.snapshot().failures.iter().any(|failure| failure.component == WorkerComponent::Abort)
    })
    .await;

    lock.rollback().await.unwrap();
    probe.release.notify_one();
    wait_until(
        Duration::from_secs(5),
        Duration::from_millis(10),
        "abort health did not recover after the attempt finished",
        || async { !health.snapshot().failures.iter().any(|failure| failure.component == WorkerComponent::Abort) },
    )
    .await;

    shutdown.cancel();
    run.await.unwrap().unwrap();
}

#[pgqueue::job(max_attempts = 2)]
async fn fails_after_release(_: (), state: JobState<DatabaseLossProbe>, ctx: JobContext) -> anyhow::Result<()> {
    if ctx.attempt() == 1 {
        state.0.started.notify_one();
        state.0.release.notified().await;
    }
    anyhow::bail!("attempt failed")
}

async fn fail_first_running_transition(pool: &PgPool) {
    sqlx::raw_sql(
        r#"
        CREATE SEQUENCE pgqueue.finalization_failures;
        CREATE FUNCTION pgqueue.fail_first_finalization() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            IF nextval('pgqueue.finalization_failures') = 1 THEN
                RAISE EXCEPTION 'injected transient finalization failure';
            END IF;
            RETURN NEW;
        END
        $$;
        CREATE TRIGGER fail_first_finalization
        BEFORE UPDATE ON pgqueue.jobs
        FOR EACH ROW
        WHEN (OLD.status = 'running' AND NEW.status IS DISTINCT FROM OLD.status)
        EXECUTE FUNCTION pgqueue.fail_first_finalization();
        "#,
    )
    .execute(pool)
    .await
    .expect("install transient finalization failure");
}

#[sqlx::test(migrations = "./migrations")]
async fn test_worker_retries_a_transient_finish_failure(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let handle = db.queue.enqueue(quick_nap::job(())).await.unwrap().unwrap();
    let probe = DatabaseLossProbe {
        started: Arc::new(tokio::sync::Notify::new()),
        release: Arc::new(tokio::sync::Notify::new()),
    };
    let worker = test_worker(db.queue.clone()).register_job(quick_nap).state(probe.clone()).build().unwrap();
    let token = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(token.clone()));

    probe.started.notified().await;
    fail_first_running_transition(&pool).await;
    probe.release.notify_one();

    let row = wait_terminal(&db.queue, handle.id(), 10).await;
    assert_eq!(row.status, JobStatus::Complete);
    assert_eq!(row.attempts, 1);

    token.cancel();
    run.await.unwrap().unwrap();
}

#[sqlx::test(migrations = "./migrations")]
async fn test_worker_retries_a_transient_retry_failure(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let handle = db.queue.enqueue(fails_after_release::job(())).await.unwrap().unwrap();
    let probe = DatabaseLossProbe {
        started: Arc::new(tokio::sync::Notify::new()),
        release: Arc::new(tokio::sync::Notify::new()),
    };
    let worker = test_worker(db.queue.clone()).register_job(fails_after_release).state(probe.clone()).build().unwrap();
    let token = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(token.clone()));

    probe.started.notified().await;
    fail_first_running_transition(&pool).await;
    probe.release.notify_one();

    let row = wait_terminal(&db.queue, handle.id(), 10).await;
    assert_eq!(row.status, JobStatus::Failed);
    assert_eq!(row.attempts, 2);
    assert_eq!(row.error.as_deref(), Some("failed: attempt failed"));

    token.cancel();
    run.await.unwrap().unwrap();
}

/// Drops the schema from a connection of its own, retrying while the worker
/// still holds the locks it needs.
///
/// Issuing it through the worker's pool made it compete with the very
/// statements it is trying to break — the drop needs `ACCESS EXCLUSIVE` on
/// tables a live worker is reading — and an unbounded wait for that lock is
/// what failed the test outright when it lost the race. A bounded wait that is
/// retried lets the worker's statements through in between, so the schema goes
/// once there is a gap, and any error other than that contention still fails
/// the test.
async fn drop_schema_outside_the_pool(db: &TestDb) {
    use sqlx::Connection;

    let mut conn =
        PgConnection::connect_with(db.pool.connect_options().as_ref()).await.expect("connect for schema drop");
    sqlx::query("SET lock_timeout = '500ms'").execute(&mut conn).await.expect("bound the drop's lock wait");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        match sqlx::query("DROP SCHEMA pgqueue CASCADE").execute(&mut conn).await {
            Ok(_) => break,
            Err(error) => {
                let code = error.as_database_error().and_then(|error| error.code()).map(|code| code.to_string());
                // 55P03 lock_not_available, 40P01 deadlock_detected: both mean
                // the worker was mid-statement, which is exactly the state this
                // test creates on purpose.
                assert!(
                    matches!(code.as_deref(), Some("55P03" | "40P01")),
                    "dropping the schema failed for an unexpected reason: {error}"
                );
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "the schema drop never won its lock against the worker"
                );
            }
        }
    }
    conn.close().await.expect("close schema drop connection");
}

#[sqlx::test(migrations = "./migrations")]
async fn test_worker_survives_the_database_disappearing(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    db.queue.enqueue(quick_nap::job(())).await.unwrap().unwrap();
    let probe = DatabaseLossProbe {
        started: Arc::new(tokio::sync::Notify::new()),
        release: Arc::new(tokio::sync::Notify::new()),
    };

    let worker = test_worker(db.queue.clone())
        .register_job(quick_nap)
        .state(probe.clone())
        .timers(WorkerTimers {
            abort: Duration::from_millis(50),
            schedule: Duration::from_millis(100),
            sweep: Duration::from_millis(200),
            worker_info: Duration::from_millis(100),
        })
        .concurrency(2)
        .build()
        .unwrap();
    let token = CancellationToken::new();
    let mut run = tokio::spawn(worker.run_until(token.clone()));

    // Wait until the nap job is mid-flight, then nuke the schema: dequeues,
    // abort polls, sweeps, heartbeats, and the job finalization all start
    // failing. The worker must log and keep running, never crash.
    tokio::time::timeout(Duration::from_secs(5), probe.started.notified()).await.expect("job did not start");
    drop_schema_outside_the_pool(&db).await;

    probe.release.notify_one();
    assert!(
        tokio::time::timeout(Duration::from_millis(500), &mut run).await.is_err(),
        "worker must survive database loss"
    );

    token.cancel();
    run.await.unwrap().unwrap();
}

#[sqlx::test(migrations = "./migrations")]
async fn test_cron_with_unserializable_payload_fails_at_build(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let err = Worker::builder(db.queue.clone())
        .schedule_cron("* * * * *", bad_payload::job([((1u32, 2u32), 3u32)].into_iter().collect()))
        .build()
        .unwrap_err();
    assert!(matches!(err, Error::Serde(_)), "{err}");
}

/// JSON object keys must be strings; a tuple-keyed map cannot serialize.
#[pgqueue::job]
async fn bad_payload(_args: std::collections::HashMap<(u32, u32), u32>) -> anyhow::Result<()> {
    Ok(())
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "stress test"]
async fn test_dedupe_enqueue_accepts_one_winner_under_stress(pool: PgPool) {
    let db = TestDb::new(pool).await;
    for round in 0..100 {
        let key = format!("stress-dedupe-{round}");
        let mut tasks = Vec::new();
        for _ in 0..32 {
            let queue = db.queue.clone();
            let key = key.clone();
            tasks.push(tokio::spawn(async move { queue.enqueue(counts::job(()).dedupe_key(key)).await }));
        }
        let mut winners = 0;
        for task in tasks {
            if task.await.unwrap().unwrap().is_some() {
                winners += 1;
            }
        }
        assert_eq!(winners, 1, "round {round}");
    }
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "stress test"]
async fn test_shutdown_abort_retry_and_sweep_interoperate_under_stress(pool: PgPool) {
    let db = TestDb::new(pool).await;
    for _ in 0..25 {
        let handle = db.queue.enqueue(counts::job(())).await.unwrap().unwrap();
        // Hold the only ready row so the worker starts, finds nothing
        // claimable, and shuts down with the job still queued.
        let mut lock = db.queue.pool().begin().await.unwrap();
        sqlx::query("SELECT id FROM pgqueue.jobs WHERE id = $1 FOR UPDATE")
            .bind(handle.id())
            .fetch_one(&mut *lock)
            .await
            .unwrap();

        let counter = Arc::new(AtomicU32::new(0));
        let worker = test_worker(db.queue.clone()).register_job(counts).state(counter.clone()).build().unwrap();
        let token = CancellationToken::new();
        let run = tokio::spawn(worker.run_until(token.clone()));
        tokio::time::sleep(Duration::from_millis(100)).await;
        token.cancel();
        tokio::time::timeout(Duration::from_secs(1), run)
            .await
            .expect("contended worker shutdown timed out")
            .unwrap()
            .unwrap();
        lock.rollback().await.unwrap();

        assert!(handle.abort("stress abort").await.unwrap());
        assert!(db.queue.retry_job(handle.id(), "stress retry").await.unwrap());
        let mut sweeper = db.queue.sweeper();
        assert!(sweeper.sweep().await.unwrap().leader);
        sweeper.release().await;

        test_worker(db.queue.clone())
            .register_job(counts)
            .state(counter.clone())
            .burst(true)
            .dequeue_timeout(Duration::from_millis(400))
            .build()
            .unwrap()
            .run_until(CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }
}

/// Blocks its runtime thread for a fixed time without ever yielding, like a
/// synchronous HTTP client or a CPU-bound loop inside a handler.
fn block_runtime_thread() {
    std::thread::sleep(Duration::from_secs(3));
}

#[pgqueue::job(name = "thread_blocking", timeout_ms = 200, max_attempts = 1)]
async fn thread_blocking(_: ()) -> anyhow::Result<()> {
    block_runtime_thread();
    Ok(())
}

/// `JoinHandle::abort` only lands at the next yield point, so an uncancellable
/// handler must not be able to hold the attempt — and its late `Ok` must not
/// overwrite the timeout that already fired.
///
/// Needs a multi-threaded runtime: on `#[sqlx::test]`'s current-thread one the
/// blocking handler would stall the test itself.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_worker_times_out_an_attempt_that_blocks_its_runtime_thread() {
    crate::init_tracing();
    let url = crate::fresh_database("blocking_timeout").await;
    let queue = Queue::connect(&url).await.unwrap();
    let id = queue.enqueue(thread_blocking::job(())).await.unwrap().job_id();

    let shutdown = CancellationToken::new();
    let worker = Worker::builder(queue.clone())
        .register_job(thread_blocking)
        .concurrency(1)
        .timers(test_timers())
        .build()
        .unwrap();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));

    wait_until(
        Duration::from_secs(30),
        Duration::from_millis(20),
        "the blocked attempt was never finalized",
        || async { queue.fetch_job(id).await.unwrap().is_some_and(|row| row.status.is_terminal()) },
    )
    .await;

    // The outcome is the signal, not the clock: waiting for the handle without
    // a bound would let the handler's eventual `Ok(())` land instead, marking a
    // job that ran 15x over its timeout `complete` with no error at all.
    let row = queue.fetch_job(id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Failed);
    assert!(
        row.error.unwrap_or_default().contains("attempt exceeded"),
        "the timeout must win; a late handler success must not overwrite it"
    );

    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(10), run)
        .await
        .expect("worker shutdown must not wait for the blocking handler")
        .unwrap()
        .unwrap();
}

#[pgqueue::job(name = "listenerless", max_attempts = 1)]
async fn listenerless(_: ()) -> anyhow::Result<()> {
    Ok(())
}

/// The dedicated LISTEN connection lives outside the query pool, so it can be
/// refused while the pool is fine. Fetching already has a polling fallback, so
/// the worker must degrade instead of dying (which would be a crash loop).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_worker_processes_jobs_by_polling_when_the_listener_cannot_connect() {
    crate::init_tracing();
    let url = crate::fresh_database("listenerless").await;
    Queue::connect(&url).await.unwrap();
    // Warm a full pool, then revoke the role's right to open *new* connections.
    // Established connections keep working, so the queue stays usable while the
    // listener — which needs its own connection outside the pool — cannot start.
    let client_url = crate::limited_role_url(&url, -1).await;
    let pool =
        sqlx::postgres::PgPoolOptions::new().min_connections(4).max_connections(4).connect(&client_url).await.unwrap();
    crate::warm_pool(&pool, 4).await;
    crate::revoke_connect(&url, &client_url).await;
    let queue = Queue::builder(&client_url).pool(pool).connect().await.unwrap();
    let id = queue.enqueue(listenerless::job(())).await.unwrap().job_id();

    let shutdown = CancellationToken::new();
    let worker = Worker::builder(queue.clone())
        .register_job(listenerless)
        .timers(test_timers())
        .poll_interval(Duration::from_millis(50))
        .build()
        .unwrap();
    let health = worker.health();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));

    wait_until(
        Duration::from_secs(60),
        Duration::from_millis(50),
        "job was never processed by the polling fallback",
        || async { queue.fetch_job(id).await.unwrap().is_some_and(|row| row.status == JobStatus::Complete) },
    )
    .await;
    // Losing notifications is a real degradation, so it is reported as one.
    // Nothing orders the listener's failed connect against the poll loop that
    // completes the job, so the failure can land after the job does.
    wait_until(
        Duration::from_secs(60),
        Duration::from_millis(50),
        "notification failure was never reported",
        || async {
            health.snapshot().failures.iter().any(|failure| failure.component == WorkerComponent::Notification)
        },
    )
    .await;

    shutdown.cancel();
    // Shutdown wants connections this role is deliberately not allowed, so its
    // `Err` is not what this test asserts — but a panicking loop or a worker that
    // never stopped at all still is, and both come back as `None`.
    assert!(crate::join_worker(run, Duration::from_secs(30)).await.is_some(), "the worker did not return a result");
}

/// Burns CPU without reaching an `await`, so `JoinHandle::abort` cannot take
/// effect and the handler runs to completion after the deadline has fired.
fn spin_for(duration: Duration) {
    let deadline = std::time::Instant::now() + duration;
    while std::time::Instant::now() < deadline {
        std::hint::spin_loop();
    }
}

// ---------------------------------------------------------------------------
// The timeout arm must not invent an outcome the handler already produced
// ---------------------------------------------------------------------------

/// Counts handler bodies that ran all the way to their final statement.
#[derive(Clone)]
struct SideEffects(Arc<AtomicUsize>);

/// The last `await` resolves on the same instant as the attempt deadline, so
/// the biased select can take the timeout arm while this handler is already
/// running on another worker thread. Past that point it has no yield point
/// left, `abort()` cannot stop it, and it settles with a real error.
///
/// (Waking *earlier* does not exercise the race: while the handler holds a
/// runtime thread the select is never polled, so the task arm wins. Waking
/// *later* means `abort()` lands at a yield point and there is no outcome to
/// keep.) The counter is bumped immediately before returning, so it is exactly
/// how many attempts completed — which of them win the race depends on machine
/// load, but the invariant asserted below does not.
#[pgqueue::job(name = "repro_late_error", max_attempts = 1, timeout_ms = 200)]
async fn repro_late_error(_: (), state: JobState<SideEffects>) -> anyhow::Result<()> {
    tokio::time::sleep(Duration::from_millis(200)).await;
    spin_for(Duration::from_millis(50));
    state.0.0.fetch_add(1, Ordering::SeqCst);
    anyhow::bail!("payment gateway declined: card_expired")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_attempt_records_the_handler_error_when_it_settles_after_the_timeout() {
    crate::init_tracing();
    let url = crate::fresh_database("late_error").await;
    let queue = Queue::connect(&url).await.unwrap();
    let mut ids = Vec::new();
    for _ in 0..8 {
        ids.push(queue.enqueue(repro_late_error::job(())).await.unwrap().job_id());
    }

    let completions = Arc::new(AtomicUsize::new(0));
    let shutdown = CancellationToken::new();
    let worker = Worker::builder(queue.clone())
        .register_job(repro_late_error)
        .state(SideEffects(completions.clone()))
        // One at a time: a handler that is spinning starves this runtime, and
        // the race under test needs the worker's select to stay pollable.
        .concurrency(1)
        .timers(test_timers())
        .poll_interval(Duration::from_millis(50))
        .build()
        .unwrap();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));

    let mut errors = Vec::new();
    for id in ids {
        errors.push(wait_terminal(&queue, id, 30).await.error.unwrap_or_default());
    }
    shutdown.cancel();
    // Asserted, not discarded: a worker that ended in `Err` — a background loop
    // that panicked, a shutdown step that timed out — would otherwise pass this
    // test silently.
    assert!(
        matches!(crate::join_worker(run, Duration::from_secs(10)).await, Some(Ok(()))),
        "the worker did not shut down cleanly"
    );

    // Every attempt that ran to completion must have its own diagnosis
    // recorded; the rest were genuinely cancelled at a yield point and
    // correctly report the deadline. Replacing a completed handler's error with
    // a synthetic timeout would break this equality.
    let completed = completions.load(Ordering::SeqCst);
    let real = errors.iter().filter(|e| e.contains("card_expired")).count();
    assert_eq!(real, completed, "every completed attempt must record its own error; got {errors:?}");
    assert!(completed > 0, "no attempt settled after its deadline, so the race went untested");
}

/// Reaches the counter only by running to completion, so the count is proof the
/// attempt genuinely succeeded rather than timing out.
#[pgqueue::job(name = "repro_late_success", max_attempts = 2, timeout_ms = 200, retry_delay_ms = 0)]
async fn repro_late_success(_: (), state: JobState<SideEffects>) -> anyhow::Result<()> {
    tokio::time::sleep(Duration::from_millis(200)).await;
    spin_for(Duration::from_millis(50));
    state.0.0.fetch_add(1, Ordering::SeqCst);
    Ok(())
}

/// A handler that overran its deadline still failed the attempt — the timeout
/// is a real limit, not advisory — but it must not be reported as a timeout
/// twice over: the attempt is retried at most as far as `max_attempts`, and the
/// recorded reason names the deadline.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_attempt_that_overruns_its_timeout_is_not_retried_past_max_attempts() {
    crate::init_tracing();
    let url = crate::fresh_database("late_success").await;
    let queue = Queue::connect(&url).await.unwrap();
    let id = queue.enqueue(repro_late_success::job(())).await.unwrap().job_id();

    let effects = Arc::new(AtomicUsize::new(0));
    let shutdown = CancellationToken::new();
    let worker = Worker::builder(queue.clone())
        .register_job(repro_late_success)
        .state(SideEffects(effects.clone()))
        .timers(test_timers())
        .poll_interval(Duration::from_millis(50))
        .build()
        .unwrap();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));

    let row = wait_terminal(&queue, id, 30).await;
    shutdown.cancel();
    // Asserted, not discarded: a worker that ended in `Err` — a background loop
    // that panicked, a shutdown step that timed out — would otherwise pass this
    // test silently.
    assert!(
        matches!(crate::join_worker(run, Duration::from_secs(10)).await, Some(Ok(()))),
        "the worker did not shut down cleanly"
    );

    let error = row.error.unwrap_or_default();
    assert_eq!(row.status, JobStatus::Failed, "{error}");
    assert!(error.contains("attempt exceeded"), "{error:?}");
    assert_eq!(row.attempts, 2, "the attempt budget must still be honoured");
    assert!(effects.load(Ordering::SeqCst) <= 2, "a late success must not run more attempts than the budget allows");
}

// ---------------------------------------------------------------------------
// Recovering from connections the database refuses
// ---------------------------------------------------------------------------

async fn grant_connect(admin: &str, client_url: &str) {
    let (_, db_name) = admin.rsplit_once('/').unwrap();
    let role = client_url.split_once("://").and_then(|(_, rest)| rest.split_once(':')).unwrap().0.to_string();
    let mut conn = PgConnection::connect(admin).await.unwrap();
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        r#"GRANT CONNECT ON DATABASE "{db_name}" TO "{role}";
           GRANT CONNECT ON DATABASE "{db_name}" TO PUBLIC;"#
    )))
    .execute(&mut conn)
    .await
    .unwrap();
    conn.close().await.unwrap();
}

/// Warms a full pool for a role, then withdraws its right to open *new*
/// connections. Established connections keep working, so the query pool stays
/// healthy while the listener — which needs its own connection outside the
/// pool — cannot start.
async fn queue_without_a_listener(tag: &str) -> (String, String, Queue) {
    let url = crate::fresh_database(tag).await;
    Queue::connect(&url).await.unwrap();
    let client_url = crate::limited_role_url(&url, -1).await;
    let pool =
        sqlx::postgres::PgPoolOptions::new().min_connections(4).max_connections(4).connect(&client_url).await.unwrap();
    crate::warm_pool(&pool, 4).await;
    crate::revoke_connect(&url, &client_url).await;
    let queue = Queue::builder(&client_url).pool(pool).connect().await.unwrap();
    (url, client_url, queue)
}

#[pgqueue::job(name = "repro_noop", max_attempts = 1)]
async fn repro_noop(_: ()) -> anyhow::Result<()> {
    Ok(())
}

/// The listener is refused at startup, so the worker falls back to polling and
/// reports the degradation. Once the database stops refusing connections it
/// must reconnect on its own: a one-second refusal that permanently downgraded
/// a worker to polling — and pinned it Degraded — would need an operator to
/// notice and restart it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_notification_listener_recovers_after_a_refused_start() {
    crate::init_tracing();
    let (url, client_url, queue) = queue_without_a_listener("listener_restart").await;

    let shutdown = CancellationToken::new();
    let worker = Worker::builder(queue.clone())
        .register_job(repro_noop)
        .timers(test_timers())
        .poll_interval(Duration::from_millis(50))
        .build()
        .unwrap();
    let health = worker.health();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));

    wait_until(
        Duration::from_secs(60),
        Duration::from_millis(50),
        "notification listener never reported a failure",
        || async {
            health.snapshot().failures.iter().any(|failure| failure.component == WorkerComponent::Notification)
        },
    )
    .await;

    grant_connect(&url, &client_url).await;

    let admin = PgPool::connect(&url).await.unwrap();
    wait_until(
        Duration::from_secs(30),
        Duration::from_millis(100),
        "the listener never reconnected after the database healed",
        || async {
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM pg_stat_activity
                 WHERE datname = current_database() AND query LIKE 'LISTEN %'",
            )
            .fetch_one(&admin)
            .await
            .unwrap()
                > 0
        },
    )
    .await;
    wait_until(Duration::from_secs(30), Duration::from_millis(100), "notification health never recovered", || async {
        !health.snapshot().failures.iter().any(|failure| failure.component == WorkerComponent::Notification)
    })
    .await;

    shutdown.cancel();
    // Asserted, not discarded: a worker that ended in `Err` — a background loop
    // that panicked, a shutdown step that timed out — would otherwise pass this
    // test silently.
    assert!(
        matches!(crate::join_worker(run, Duration::from_secs(30)).await, Some(Ok(()))),
        "the worker did not shut down cleanly"
    );
}

/// A sustained outage must not be retried at a fixed cadence: the reconnect
/// delay doubles per failed attempt, so a long outage costs one connection
/// attempt (and one warn) per capped interval — and the listener must still
/// reconnect on its own once the outage ends. Only lower bounds are asserted:
/// doubling from 500ms puts at least three seconds of sleep between the first
/// and fourth failures the health watch reports, where the old fixed cadence
/// spaced them 1.5s apart, and a loaded machine only widens the gaps.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_notification_listener_reconnect_backs_off_during_an_outage() {
    crate::init_tracing();
    let (url, client_url, queue) = queue_without_a_listener("listener_backoff").await;

    // Subscribing starts the listener, whose every failed attempt re-sends on
    // the health watch; gaps are >= 500ms, so none are coalesced away.
    let mut health = pgqueue::__test_support::listener_health(&queue);
    let started = tokio::time::Instant::now();
    let mut failures = Vec::new();
    while failures.len() < 4 {
        tokio::time::timeout(Duration::from_secs(30), health.changed())
            .await
            .expect("listener health stalled during the outage")
            .expect("listener dropped its health channel");
        if health.borrow_and_update().is_some() {
            failures.push(started.elapsed());
        }
    }
    let spread = failures[3] - failures[0];
    assert!(spread >= Duration::from_secs(3), "four reconnect failures within {spread:?} imply a fixed retry cadence");

    // Ending the outage must heal the listener within one capped delay.
    grant_connect(&url, &client_url).await;
    tokio::time::timeout(Duration::from_secs(15), async {
        while health.borrow_and_update().is_some() {
            health.changed().await.expect("listener dropped its health channel");
        }
    })
    .await
    .expect("the listener never reconnected after the outage ended");
}

/// A TCP relay to the test database whose open connections can all be silenced at once: from then on they pass no bytes
/// either way while both of their sockets stay open, which is how a connection a NAT or load balancer dropped for
/// idleness looks to each end. Connections opened after that are relayed normally — unless the relay has been told to
/// refuse them, closing each as it arrives, or to stall them, holding each open without ever passing it on.
struct SilenceableRelay {
    address: std::net::SocketAddr,
    generation: tokio::sync::watch::Sender<u64>,
    /// What the relay does with a new connection: [`RELAY`], [`REFUSE`] or [`STALL`] it.
    accepting: Arc<std::sync::atomic::AtomicU8>,
}

const RELAY: u8 = 0;
const REFUSE: u8 = 1;
const STALL: u8 = 2;

impl SilenceableRelay {
    async fn start(upstream: (String, u16)) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (generation, silenced) = tokio::sync::watch::channel(0_u64);
        let accepting = Arc::new(std::sync::atomic::AtomicU8::new(RELAY));
        let accept = Arc::clone(&accepting);
        tokio::spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                match accept.load(Ordering::SeqCst) {
                    REFUSE => {
                        drop(client);
                        continue;
                    }
                    STALL => {
                        tokio::spawn(async move {
                            let _client = client;
                            std::future::pending::<()>().await;
                        });
                        continue;
                    }
                    _ => {}
                }
                let mut silenced = silenced.clone();
                let opened_in = *silenced.borrow_and_update();
                let upstream = upstream.clone();
                tokio::spawn(async move {
                    let Ok(server) = tokio::net::TcpStream::connect((upstream.0.as_str(), upstream.1)).await else {
                        return;
                    };
                    let (mut client_read, mut client_write) = client.into_split();
                    let (mut server_read, mut server_write) = server.into_split();
                    let relay = async {
                        tokio::join!(
                            tokio::io::copy(&mut client_read, &mut server_write),
                            tokio::io::copy(&mut server_read, &mut client_write),
                        )
                    };
                    tokio::select! {
                        _ = relay => {}
                        // Owned by this task, the four halves stay open — no FIN, no RST — for as long as it lives.
                        _ = async {
                            let _ = silenced.wait_for(|current| *current > opened_in).await;
                        } => std::future::pending::<()>().await,
                    }
                });
            }
        });
        Self { address, generation, accepting }
    }

    fn silence_open_connections(&self) {
        self.generation.send_modify(|generation| *generation += 1);
    }

    /// Closes every connection opened from now on as it arrives, leaving the open ones relayed.
    fn refuse_new_connections(&self) {
        self.accepting.store(REFUSE, Ordering::SeqCst);
    }

    /// Accepts every connection opened from now on and never passes it on, leaving the open ones relayed.
    fn stall_new_connections(&self) {
        self.accepting.store(STALL, Ordering::SeqCst);
    }
}

/// A `LISTEN` session that stops answering — dropped for idleness by a NAT or load balancer, with neither end told — has
/// to be noticed and replaced. It receives nothing and sends nothing, and sqlx sets no TCP keepalive, so it read as a
/// quiet channel for as long as the process ran: every wakeup and completion event lost, and the health watch
/// reporting a healthy subscription throughout.
#[sqlx::test(migrations = "./migrations")]
async fn test_notification_listener_replaces_a_session_that_stopped_answering(pool: PgPool) {
    let options = pool.connect_options();
    let relay = SilenceableRelay::start((options.get_host().to_string(), options.get_port())).await;
    let relayed_options = options.as_ref().clone().host("127.0.0.1").port(relay.address.port());
    let relayed = sqlx::postgres::PgPoolOptions::new().max_connections(2).connect_with(relayed_options).await.unwrap();
    let queue = Queue::builder("postgres://unused").pool(relayed).connect().await.unwrap();
    assert!(pgqueue::__test_support::set_listener_probe(
        &queue,
        Duration::from_millis(200),
        Duration::from_millis(500)
    ));
    let mut health = pgqueue::__test_support::listener_health(&queue);
    crate::wait_for_done_listeners(&pool, 1).await;

    relay.silence_open_connections();
    tokio::time::timeout(Duration::from_secs(10), async {
        while health.borrow_and_update().is_none() {
            health.changed().await.expect("listener dropped its health channel");
        }
    })
    .await
    .expect("a session that stopped answering was still reported as subscribed");

    // Replaced on a fresh connection, which the relay carries, and delivering wakeups again.
    tokio::time::timeout(Duration::from_secs(15), async {
        while health.borrow_and_update().is_some() {
            health.changed().await.expect("listener dropped its health channel");
        }
    })
    .await
    .expect("the listener never replaced the silent session");
    let mut wakeups = pgqueue::__test_support::listener_wakeups(&queue);
    sqlx::query("SELECT pg_notify($1, 'enqueue')")
        .bind(pgqueue::__test_support::notify_channel(queue.name()))
        .execute(&pool)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), wakeups.recv())
        .await
        .expect("no wakeup reached the replacement session")
        .unwrap();

    // The silenced sessions are still attached on the server, and would keep the test database from being dropped.
    drop(queue);
    sqlx::query(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity
         WHERE datname = current_database() AND pid <> pg_backend_pid()",
    )
    .execute(&pool)
    .await
    .unwrap();
}

/// The backend pid holding the single-key advisory lock `key` in the database
/// `pool` is connected to — the queue's current sweep leader, if any.
async fn sweep_leader_pid(pool: &PgPool, key: i64) -> Option<i32> {
    let class_id = i64::from((key as u64 >> 32) as u32);
    let object_id = i64::from(key as u64 as u32);
    sqlx::query_scalar::<_, i32>(
        "SELECT pid FROM pg_locks
         WHERE locktype = 'advisory' AND granted AND objsubid = 1
           AND classid::bigint = $1 AND objid::bigint = $2
           AND database = (SELECT oid FROM pg_database WHERE datname = current_database())",
    )
    .bind(class_id)
    .bind(object_id)
    .fetch_optional(pool)
    .await
    .expect("inspect sweep leadership lock")
}

/// Sweep leadership is revalidated by a liveness probe on the dedicated
/// leadership connection, not by sweep progress — and the two can diverge: a
/// firewall or NAT change that blocks new flows leaves established sessions
/// working, so the leader's *pooled* connections (which sweep passes run on)
/// die while the leadership connection answers every probe. Retaining the lock
/// then stops cluster-wide stuck-job recovery and expired-row purging silently,
/// because every peer's `pg_try_advisory_lock` reports the ordinary
/// `leader: false`. A leader whose passes keep failing must release the lock so
/// a healthy peer can take over.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_sweep_leadership_fails_over_when_the_leader_cannot_sweep() {
    crate::init_tracing();
    let url = crate::fresh_database("sweep_failover").await;
    Queue::connect(&url).await.unwrap();
    let admin = PgPool::connect(&url).await.unwrap();

    // Worker A runs as a dedicated role so its right to open *new* connections
    // can be withdrawn while everyone else's stays. A short acquire timeout
    // bounds how long each doomed pass waits on the refused pool.
    let client_url = crate::limited_role_url(&url, -1).await;
    let role = client_url.split_once("://").and_then(|(_, rest)| rest.split_once(':')).unwrap().0.to_string();
    let pool_a = sqlx::postgres::PgPoolOptions::new()
        .min_connections(3)
        .max_connections(3)
        .acquire_timeout(Duration::from_millis(500))
        .connect(&client_url)
        .await
        .unwrap();
    let queue_a = Queue::builder(&client_url).pool(pool_a).sweep_grace(crate::MIN_SWEEP_GRACE).connect().await.unwrap();

    let shutdown_a = CancellationToken::new();
    let worker_a = Worker::builder(queue_a.clone())
        .register_job(repro_noop)
        .timers(WorkerTimers { sweep: Duration::from_millis(100), ..test_timers() })
        .build()
        .unwrap();
    let health_a = worker_a.health();
    let run_a = tokio::spawn(worker_a.run_until(shutdown_a.clone()));

    let (_, db_name) = url.rsplit_once('/').unwrap();
    let key = pgqueue::__test_support::sweep_lock_key(db_name, queue_a.name());
    let leader_pid = wait_for_some(
        Duration::from_secs(30),
        Duration::from_millis(20),
        "worker A never took sweep leadership",
        || async { sweep_leader_pid(&admin, key).await },
    )
    .await;

    // The outage: new flows are refused while established sessions survive.
    // Killing every backend of A's role *except* the leadership session leaves
    // exactly the claimed shape — the liveness probe keeps succeeding on the
    // leadership connection while every sweep pass fails, because the pool it
    // sweeps through can neither reuse its dead connections nor open new ones.
    crate::revoke_connect(&url, &client_url).await;
    wait_until(
        Duration::from_secs(10),
        Duration::from_millis(50),
        "worker A's pooled connections were never terminated",
        || async {
            sqlx::query(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity
                 WHERE datname = current_database() AND usename = $1 AND pid <> $2",
            )
            .bind(&role)
            .bind(leader_pid)
            .execute(&admin)
            .await
            .unwrap();
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM pg_stat_activity
                 WHERE datname = current_database() AND usename = $1 AND pid <> $2",
            )
            .bind(&role)
            .bind(leader_pid)
            .fetch_one(&admin)
            .await
            .unwrap()
                == 0
        },
    )
    .await;

    // Work only a sweep can recover: a running attempt whose worker vanished.
    let queue_b = Queue::builder(&url).sweep_grace(crate::MIN_SWEEP_GRACE).connect().await.unwrap();
    let id = queue_b.enqueue(repro_noop::job(()).max_attempts(1)).await.unwrap().job_id();
    let claimed = queue_b.dequeue(1, Uuid::now_v7()).await.unwrap();
    assert_eq!(claimed.len(), 1, "the stuck job was not claimed");
    sqlx::query(
        "UPDATE pgqueue.jobs
         SET started_at = now() - interval '1 hour', touched_at = now() - interval '1 hour'
         WHERE id = $1",
    )
    .bind(id)
    .execute(&admin)
    .await
    .unwrap();

    // A's passes are failing — and it still holds the lock, exactly the state
    // that used to persist for as long as the leadership connection lived.
    wait_until(
        Duration::from_secs(30),
        Duration::from_millis(50),
        "worker A's sweep passes never started failing",
        || async { health_a.snapshot().failures.iter().any(|failure| failure.component == WorkerComponent::Sweeper) },
    )
    .await;

    let shutdown_b = CancellationToken::new();
    let worker_b = Worker::builder(queue_b.clone())
        .register_job(repro_noop)
        .timers(WorkerTimers { sweep: Duration::from_millis(100), ..test_timers() })
        .build()
        .unwrap();
    let run_b = tokio::spawn(worker_b.run_until(shutdown_b.clone()));

    // The failing leader must surrender the lock, and the healthy worker must
    // pick it up: a new backend holds leadership and the stuck job is swept.
    wait_until(
        Duration::from_secs(60),
        Duration::from_millis(50),
        "sweep leadership never failed over to the healthy worker",
        || async { sweep_leader_pid(&admin, key).await.is_some_and(|pid| pid != leader_pid) },
    )
    .await;
    wait_until(
        Duration::from_secs(60),
        Duration::from_millis(50),
        "the new leader never recovered the stuck job",
        || async { queue_b.fetch_job(id).await.unwrap().is_some_and(|row| row.status.is_terminal()) },
    )
    .await;

    shutdown_b.cancel();
    shutdown_a.cancel();
    // Asserted, not discarded: a worker that ended in `Err` — a background loop
    // that panicked, a shutdown step that timed out — would otherwise pass this
    // test silently.
    assert!(
        matches!(crate::join_worker(run_b, Duration::from_secs(30)).await, Some(Ok(()))),
        "the worker did not shut down cleanly"
    );
    // Worker A's shutdown wants connections its role is deliberately refused, so
    // its `Err` is not what this test asserts — but a panic or a hang is.
    assert!(crate::join_worker(run_a, Duration::from_secs(30)).await.is_some(), "worker A did not return a result");
}

/// `run_until_inner` registers a lease before the fetcher — and with it the
/// shutdown machinery — exists. Stopping in that window must still retire the
/// lease, or the worker keeps advertising itself as live and accepting for up
/// to three heartbeat intervals after it is gone.
///
/// Stopping as soon as the lease is visible lands in that window only when the worker has not yet resumed from the
/// write, which the scheduler rarely allows; `test_worker_retires_a_lease_committed_by_a_cancelled_startup_heartbeat`
/// gates the write to land there every time. What this pins is the property whichever path the stop takes: a worker
/// that returned advertises no live, accepting lease.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_worker_retires_its_lease_when_shutdown_lands_during_startup() {
    crate::init_tracing();
    let (url, _client_url, queue) = queue_without_a_listener("startup_lease").await;
    let admin = PgPool::connect(&url).await.unwrap();

    let shutdown = CancellationToken::new();
    let worker = Worker::builder(queue.clone())
        .register_job(repro_noop)
        .timers(test_timers())
        .poll_interval(Duration::from_millis(50))
        .build()
        .unwrap();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));

    let worker_id: Uuid = wait_for_some(
        Duration::from_secs(30),
        Duration::from_millis(1),
        "worker never registered its lease",
        || async {
            sqlx::query("SELECT id FROM pgqueue.workers")
                .fetch_optional(&admin)
                .await
                .unwrap()
                .map(|row| row.get::<Uuid, _>("id"))
        },
    )
    .await;
    shutdown.cancel();

    tokio::time::timeout(Duration::from_secs(60), run).await.expect("worker run did not return").unwrap().unwrap();

    let live: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM pgqueue.workers
             WHERE id = $1 AND accepting AND expires_at > now()
         )",
    )
    .bind(worker_id)
    .fetch_one(&admin)
    .await
    .unwrap();
    assert!(!live, "a worker that returned must not still advertise a live, accepting lease");
}

/// `shutdown_grace` is the handler-drain budget, and shutdown's first durable
/// act — closing this worker's lease to new work — must not inherit it as its
/// only bound. A zero grace is a valid configuration, and its deadline has
/// already passed when it is constructed, so the close was skipped before it was
/// ever issued.
///
/// A worker that requeues something hides that: `requeue_shutdown` carries
/// `close_intake`, and the final expiry write retires the lease anyway. An
/// *idle* worker takes neither path, so it stopped with `accepting = true` —
/// `write_worker_info` under `LeaseIntake::Closed` leaves an open lease open by
/// design, so nothing later in the shutdown repaired it. Harmless while the
/// expiry write lands, and precisely not harmless when it does not, which is the
/// database blip the grace exists to survive: the worker then advertises itself
/// live and accepting for a whole lease TTL, and the sweeper reads its abandoned
/// attempts as a live owner's.
#[sqlx::test(migrations = "./migrations")]
async fn test_a_zero_shutdown_grace_still_closes_an_idle_worker_s_intake(pool: PgPool) {
    let db = TestDb::new(pool).await;
    let worker = test_worker(db.queue.clone()).register_job(repro_noop).shutdown_grace(Duration::ZERO).build().unwrap();
    let worker_id = worker.id();
    let token = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(token.clone()));

    wait_until(Duration::from_secs(30), Duration::from_millis(5), "worker never registered its lease", || async {
        sqlx::query_scalar::<_, bool>("SELECT accepting FROM pgqueue.workers WHERE id = $1")
            .bind(worker_id)
            .fetch_optional(db.queue.pool())
            .await
            .unwrap()
            == Some(true)
    })
    .await;

    token.cancel();
    tokio::time::timeout(Duration::from_secs(60), run).await.expect("worker run did not return").unwrap().unwrap();

    // The row is still on disk — the expiry write updates it rather than
    // deleting it — so `accepting` is readable, and it is what a lease that
    // outlives its expiry write would advertise.
    let accepting = sqlx::query_scalar::<_, bool>("SELECT accepting FROM pgqueue.workers WHERE id = $1")
        .bind(worker_id)
        .fetch_optional(db.queue.pool())
        .await
        .unwrap();
    assert_eq!(accepting, Some(false), "a zero shutdown grace must not skip the durable intake close");
}

// ---------------------------------------------------------------------------
// A heartbeat that recreates a purged lease must not reopen intake
// ---------------------------------------------------------------------------

/// Parks a handler mid-attempt so the shutdown drain stays open for the test.
#[derive(Clone)]
struct DrainGate {
    started: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

#[pgqueue::job(name = "repro_drain", max_attempts = 1)]
async fn repro_drain(_: (), state: JobState<DrainGate>) -> anyhow::Result<()> {
    state.0.started.notify_one();
    state.0.release.notified().await;
    Ok(())
}

/// A worker that stalls past its lease TTL has its row deleted by the sweep
/// leader's `purge_worker_leases`. Its next heartbeat therefore inserts rather
/// than updates — and `stop_worker_intake` left nothing behind for the insert
/// to inherit, so a lease created during the shutdown drain has to start
/// closed on its own.
///
/// Getting this wrong is not cosmetic. `accepting` is read by the two claim
/// paths — the dequeue statement and its underfilled-batch probe — so a worker
/// that republishes itself as accepting keeps claiming jobs it has already
/// promised to stop taking, and then has to abandon them at the end of its
/// drain. (Sweeper suppression comes from the lease being *live*, which this
/// heartbeat preserves by design: it is what protects the attempt still
/// draining.)
#[sqlx::test(migrations = "./migrations")]
async fn test_shutting_down_worker_recreates_a_purged_lease_closed(pool: PgPool) {
    // A liveness grace, unlike the suite's usual zero: deleting the lease below
    // is also what makes the attempt still draining look abandoned, and with no
    // grace this worker's own sweeper raced the heartbeat for it — when the
    // sweep won, the attempt was aborted, the drain ended, and the worker
    // stopped heartbeating the very lease under test.
    let db = TestDb::with(pool_with_max(&pool, 10).await, |builder| builder.sweep_grace(Duration::from_secs(30))).await;
    let gate =
        DrainGate { started: Arc::new(tokio::sync::Notify::new()), release: Arc::new(tokio::sync::Notify::new()) };
    db.queue.enqueue(repro_drain::job(())).await.unwrap();

    let shutdown = CancellationToken::new();
    let worker = Worker::builder(db.queue.clone())
        .register_job(repro_drain)
        .state(gate.clone())
        .timers(test_timers())
        .poll_interval(Duration::from_millis(50))
        .shutdown_grace(Duration::from_secs(20))
        .build()
        .unwrap();
    let worker_id = worker.id();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));

    // An attempt is in flight, so the drain — and with it the heartbeat that
    // keeps the lease alive — has to stay open.
    gate.started.notified().await;
    shutdown.cancel();
    wait_for_worker_intake_closed(&db, worker_id).await;

    // What the sweep leader does to a lease that outlived its TTL.
    sqlx::query("DELETE FROM pgqueue.workers WHERE id = $1").bind(worker_id).execute(&pool).await.unwrap();

    let accepting = wait_for_some(
        Duration::from_secs(10),
        Duration::from_millis(10),
        "the draining worker never recreated its purged lease",
        || async {
            sqlx::query("SELECT accepting FROM pgqueue.workers WHERE id = $1")
                .bind(worker_id)
                .fetch_optional(&pool)
                .await
                .unwrap()
                .map(|row| row.get::<bool, _>("accepting"))
        },
    )
    .await;
    // The invariant under test, asserted on the recreated row itself.
    assert!(!accepting, "a shutting-down worker must not recreate its lease as accepting");

    // Liveness is the other half — it is what keeps the sweeper off the attempt
    // still draining — but it is a property of the heartbeat, not of that one
    // read: the lease lives for three heartbeat intervals, so a single
    // observation can land in a window a stalled heartbeat has not refreshed
    // yet. Poll for it rather than requiring the read above to have caught it.
    wait_until(
        Duration::from_secs(10),
        Duration::from_millis(10),
        "the draining worker never kept its recreated lease live",
        || async {
            sqlx::query_scalar::<_, bool>("SELECT expires_at > now() FROM pgqueue.workers WHERE id = $1")
                .bind(worker_id)
                .fetch_optional(&pool)
                .await
                .unwrap()
                .unwrap_or(false)
        },
    )
    .await;

    gate.release.notify_one();
    tokio::time::timeout(Duration::from_secs(30), run)
        .await
        .expect("worker did not finish its drain")
        .unwrap()
        .unwrap();

    let live: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM pgqueue.workers
             WHERE id = $1 AND accepting AND expires_at > now()
         )",
    )
    .bind(worker_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(!live, "the retired lease must not be live and accepting");
}

// ---------------------------------------------------------------------------
// Shutdown while a sweep is in flight
// ---------------------------------------------------------------------------

/// A sweep pass issues several statements against the shared pool and can
/// outlast the shutdown budget on a loaded database. Without a cancellation
/// point inside the pass, an ordinary graceful shutdown that lands mid-sweep
/// reports a timer-shutdown failure and the process exits non-zero.
#[sqlx::test(migrations = "./migrations")]
async fn test_shutdown_succeeds_when_a_sweep_is_in_flight(pool: PgPool) {
    let worker_pool = pool_with_max(&pool, 12).await;
    let db = TestDb::with(pool.clone(), |builder| builder.pool(worker_pool)).await;

    let owner = Uuid::now_v7();
    let handle = db.queue.enqueue_raw(new_job("stuck", |_| {})).await.unwrap();
    let claimed = db.queue.dequeue(1, owner).await.unwrap();
    assert_eq!(claimed.len(), 1);
    backdate_job_liveness(&db, handle.job_id()).await;

    install_statement_gate(
        &pool,
        "repro_sweep_gate",
        4242,
        "UPDATE",
        "NEW.status = 'aborting' AND OLD.status = 'running'",
    )
    .await;
    let gate = hold_gate(&pool, 4242, &db.database).await;

    let shutdown = CancellationToken::new();
    let worker = Worker::builder(db.queue.clone())
        .register_job(repro_noop)
        .timers(WorkerTimers { sweep: Duration::from_millis(100), ..test_timers() })
        .poll_interval(Duration::from_millis(50))
        .shutdown_grace(Duration::from_secs(2))
        .build()
        .unwrap();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));

    // Phase one's mark, matched on the CTE that opens it: `pg_stat_activity.query` keeps only the first kilobyte, and
    // the statement's `SET` lies past it.
    wait_for_lock_waiter(&db, "%WITH markable AS (%", "the sweeper never reached the gated statement").await;

    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(20), run)
        .await
        .expect("shutdown never returned")
        .unwrap()
        .expect("a shutdown landing mid-sweep is still a clean shutdown");

    gate.rollback().await.unwrap();
}

// ---------------------------------------------------------------------------
// Statement counts, measured with pg_stat_statements
// ---------------------------------------------------------------------------

/// The worker fetch loop *does* consume the probe, so it must keep running.
#[sqlx::test(migrations = "./migrations")]
async fn test_worker_dequeue_still_runs_the_availability_probe(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let Some(mut stats) = Stats::new(&db.database).await else {
        Stats::skipped("test_worker_dequeue_still_runs_the_availability_probe");
        return;
    };
    let worker_id = Uuid::now_v7();
    db.queue.write_worker_info(worker_id, json!({}), None, Duration::from_secs(30)).await.unwrap();

    let probes = stats.since_now(DEQUEUE_PROBE).await;
    pgqueue::__test_support::dequeue_worker(&db.queue, 4, worker_id).await.unwrap();

    assert_eq!(stats.delta(&probes).await, 1);
}

/// Finishing an attempt the sweeper marked `aborting` used to be refused by the
/// owner-only guards and retried under a second set, running the same 40-line
/// statement twice. One predicate covers both.
#[sqlx::test(migrations = "./migrations")]
async fn test_finish_of_a_swept_attempt_costs_one_round_trip(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let Some(mut stats) = Stats::new(&db.database).await else {
        Stats::skipped("test_finish_of_a_swept_attempt_costs_one_round_trip");
        return;
    };

    // A live owner, so the sweeper marks the attempt `aborting` (phase one) and
    // leaves recovery to it rather than requeueing it itself.
    let owner = Uuid::now_v7();
    db.queue.write_worker_info(owner, json!({}), None, Duration::from_secs(30)).await.unwrap();
    let handle = db.queue.enqueue_raw(new_job("swept", |_| {})).await.unwrap();
    let job = db.queue.dequeue(1, owner).await.unwrap().remove(0);
    backdate_job_liveness(&db, handle.job_id()).await;

    db.queue.sweeper().sweep().await.unwrap();
    let status: String = sqlx::query_scalar("SELECT status::text FROM pgqueue.jobs WHERE id = $1")
        .bind(handle.job_id())
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(status, "aborting", "the sweeper did not mark the attempt");

    let finishes = stats.since_now(FINISH_STATEMENT).await;
    assert!(
        db.queue.finish(&job, JobStatus::Complete, Some(json!("ok")), None).await.unwrap(),
        "the owner must still be able to finish an attempt the sweeper marked"
    );

    assert_eq!(stats.delta(&finishes).await, 1);
}

// ---------------------------------------------------------------------------
// A configured timeout must not disable dead-owner recovery
// ---------------------------------------------------------------------------

/// `pgqueue.job_is_stuck` used to make its two recovery triggers mutually
/// exclusive: the "owner's lease is provably gone" branch was gated on
/// `timeout_ms IS NULL`. So a SIGKILLed worker's hour-long attempt stayed
/// `running` for the full hour — holding its dedupe key, which silently
/// deduplicates every re-enqueue and every cron occurrence under that key.
///
/// The two rows here differ only in `timeout_ms`, so the assertion isolates the
/// gate: `timeout_ms` must only ever *add* a way to recover an attempt.
#[sqlx::test(migrations = "./migrations")]
async fn test_a_timed_attempt_is_recovered_when_its_worker_lease_is_gone(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    for (name, timeout) in [
        ("timed", Some(Duration::from_secs(3600))),
        ("untimed", None),
    ] {
        db.queue
            .enqueue_raw(new_job(name, |job| {
                job.config.timeout = timeout;
                job.config.max_attempts = 2;
                job.dedupe_key = Some(name.to_string());
            }))
            .await
            .unwrap();
    }

    let owner = Uuid::now_v7();
    db.queue.write_worker_info(owner, json!({}), None, Duration::from_secs(30)).await.unwrap();
    assert_eq!(db.queue.dequeue(2, owner).await.unwrap().len(), 2);
    // SIGKILL: the lease expires and the sweep leader purges the row. Both
    // attempts are minutes old at most, so neither timeout is anywhere near
    // elapsed and only the dead owner can recover them.
    sqlx::query("DELETE FROM pgqueue.workers WHERE id = $1").bind(owner).execute(&pool).await.unwrap();

    let stuck: Vec<(String, bool)> = sqlx::query(
        "SELECT j.name, pgqueue.job_is_stuck(j, 0, lease.expires_at) AS stuck
         FROM pgqueue.jobs j
         LEFT JOIN pgqueue.workers lease
             ON lease.id = j.worker_id AND lease.queue = j.queue
         WHERE j.queue = $1 ORDER BY j.name",
    )
    .bind(db.queue.name())
    .fetch_all(&pool)
    .await
    .unwrap()
    .into_iter()
    .map(|row| (row.get("name"), row.get("stuck")))
    .collect();
    assert_eq!(
        stuck,
        vec![("timed".to_string(), true), ("untimed".to_string(), true)],
        "a provably dead owner must make an attempt recoverable with or without a timeout"
    );

    // One pass: both owners are provably gone, so the mark and the requeue
    // happen together rather than a sweep interval apart.
    db.queue.sweeper().sweep().await.unwrap();
    let statuses: Vec<(String, String)> =
        sqlx::query("SELECT name, status::text AS status FROM pgqueue.jobs ORDER BY name")
            .fetch_all(&pool)
            .await
            .unwrap()
            .into_iter()
            .map(|row| (row.get("name"), row.get("status")))
            .collect();
    assert_eq!(
        statuses,
        vec![
            ("timed".to_string(), "queued".to_string()),
            ("untimed".to_string(), "queued".to_string()),
        ],
        "the timed attempt is still held by a worker that no longer exists"
    );

    // Recovery hands the key on with the work rather than dropping it. This holds
    // with or without the gate — a stranded `running` row dedupes too — so it is
    // the `queued` status above, not this, that shows the key is no longer held
    // by a worker that does not exist.
    assert!(
        db.queue
            .enqueue_raw(new_job("timed", |job| {
                job.dedupe_key = Some("timed".to_string());
            }))
            .await
            .unwrap()
            .is_deduplicated(),
        "the recovered row is queued again, so it legitimately still dedupes"
    );
}

/// The other half of the same gate: an attempt whose owner is *alive* must not
/// become recoverable just because the liveness grace elapsed. Only its
/// configured timeout may end it while the lease still covers it.
#[sqlx::test(migrations = "./migrations")]
async fn test_an_attempt_is_not_stuck_while_its_worker_lease_is_live(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let handle = db
        .queue
        .enqueue_raw(new_job("live_owner", |job| {
            job.config.timeout = Some(Duration::from_secs(86_400));
        }))
        .await
        .unwrap();
    let owner = Uuid::now_v7();
    db.queue.write_worker_info(owner, json!({}), None, Duration::from_secs(30)).await.unwrap();
    assert_eq!(db.queue.dequeue(1, owner).await.unwrap().len(), 1);
    // Far past any liveness grace, but nowhere near the configured timeout.
    backdate_job_liveness(&db, handle.job_id()).await;

    let stuck: bool = sqlx::query_scalar(
        "SELECT pgqueue.job_is_stuck(j, 0, lease.expires_at)
             FROM pgqueue.jobs j
             LEFT JOIN pgqueue.workers lease
                 ON lease.id = j.worker_id AND lease.queue = j.queue
             WHERE j.id = $1",
    )
    .bind(handle.job_id())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(!stuck, "a live lease still protects an in-flight attempt from recovery");

    db.queue.sweeper().sweep().await.unwrap();
    let status: String = sqlx::query_scalar("SELECT status::text FROM pgqueue.jobs WHERE id = $1")
        .bind(handle.job_id())
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(status, "running");
}

/// `Sweeper::recover_stuck_jobs` applies this predicate to every `running` and
/// `aborting` row of the queue, unbounded by the sweep batch size and with an
/// `ORDER BY` that gives the executor no early exit. PostgreSQL's
/// `inline_function` refuses any SQL function whose body holds a sublink, so
/// looking the lease up inside the body kept the function an opaque per-row
/// call: the whole `pgqueue.jobs` tuple was built as a composite datum and the
/// lease was re-read per row instead of being pulled into one hashed join.
/// Measured over 20,000 active rows and 50 leases at 180 ms / 20,484 buffers
/// against 6.6 ms / 460 inlined — so the shape of the body is the behaviour
/// here, and the plan is the only place it is observable.
#[sqlx::test(migrations = "./migrations")]
async fn test_stuck_predicate_inlines_into_the_plan_that_scans_for_it(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let plan = sqlx::query_scalar::<_, String>(
        "EXPLAIN (COSTS OFF)
         SELECT j.id
         FROM pgqueue.jobs AS j
         LEFT JOIN pgqueue.workers AS lease
             ON lease.id = j.worker_id AND lease.queue = j.queue
         WHERE j.queue = $1
           AND j.status IN ('running', 'aborting')
           AND pgqueue.job_is_stuck(j, $2, lease.expires_at)",
    )
    .bind(db.queue.name())
    .bind(30_000_i64)
    .fetch_all(&pool)
    .await
    .unwrap()
    .join("\n");
    assert!(
        !plan.contains("job_is_stuck"),
        "the stuck predicate must inline into the scan that applies it, not stay \
         a per-row function call: {plan}"
    );
    // The inlined body is what the plan filters on, so it is there to be seen.
    assert!(
        plan.contains("timeout_ms") && plan.contains("expires_at"),
        "the inlined plan must carry both recovery triggers: {plan}"
    );
}

// ---------------------------------------------------------------------------
// `run_until` must not return leaving a live lease behind
// ---------------------------------------------------------------------------

/// Cancelling `run_until` while the startup heartbeat's INSERT is executing is
/// the one startup step where "the future was cancelled" does not mean "it did
/// not happen": the future is client-side, the INSERT is server-side. The
/// early return above the `retire_startup_lease` guard therefore returned
/// `Ok(())` while PostgreSQL went on to commit a live, accepting lease that
/// `Queue::workers_page`, the dashboard, and the dequeue path all honoured for a full
/// TTL.
#[sqlx::test(migrations = "./migrations")]
async fn test_worker_retires_a_lease_committed_by_a_cancelled_startup_heartbeat(pool: PgPool) {
    let db = TestDb::new(pool_with_max(&pool, 8).await).await;
    // Parks the startup heartbeat's INSERT inside PostgreSQL so the test can
    // cancel `run_until` while the statement is in flight.
    sqlx::raw_sql(
        "CREATE FUNCTION pgqueue.repro_lease_gate() RETURNS trigger
         LANGUAGE plpgsql AS $$
         BEGIN
             PERFORM pg_advisory_xact_lock(9911, hashtext(current_database()));
             RETURN NEW;
         END $$;
         CREATE TRIGGER repro_lease_gate
         BEFORE INSERT ON pgqueue.workers
         FOR EACH ROW EXECUTE FUNCTION pgqueue.repro_lease_gate();",
    )
    .execute(&pool)
    .await
    .unwrap();
    let gate = hold_gate(&pool, 9911, &db.database).await;

    let shutdown = CancellationToken::new();
    let worker = Worker::builder(db.queue.clone())
        .register_job(repro_noop)
        .timers(test_timers())
        .poll_interval(Duration::from_millis(50))
        .build()
        .unwrap();
    let worker_id = worker.id();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));

    wait_for_lock_waiter(&db, "%INSERT INTO pgqueue.workers%", "the startup heartbeat never reached the gated INSERT")
        .await;
    shutdown.cancel();
    // The INSERT commits after the cancellation, exactly as a server-side
    // statement that outlived its client-side future does.
    gate.rollback().await.unwrap();

    tokio::time::timeout(Duration::from_secs(30), run).await.expect("worker run did not return").unwrap().unwrap();

    let live: Option<bool> =
        sqlx::query_scalar("SELECT accepting AND expires_at > now() FROM pgqueue.workers WHERE id = $1")
            .bind(worker_id)
            .fetch_optional(&pool)
            .await
            .unwrap();
    assert_ne!(
        live,
        Some(true),
        "run_until returned Ok(()) while its committed lease still advertised a live, \
         accepting worker"
    );
}

// ---------------------------------------------------------------------------
// Retiring the startup lease must be bounded when the backend is wedged
// ---------------------------------------------------------------------------

/// `retire_startup_lease` is awaited by `run_until` itself, and it was the only
/// database step in shutdown with no bound — every sibling wraps the identical
/// calls in `SHUTDOWN_STEP_TIMEOUT`. A backend wedged on a lock therefore hung
/// `run_until` forever instead of degrading to a lease that expires on its own.
#[sqlx::test(migrations = "./migrations")]
async fn test_startup_lease_retirement_is_bounded_when_the_backend_is_wedged(pool: PgPool) {
    let db = TestDb::with(pool_with_max(&pool, 8).await, |builder| builder).await;
    sqlx::raw_sql(
        "CREATE FUNCTION pgqueue.repro_retire_gate() RETURNS trigger
         LANGUAGE plpgsql AS $$
         BEGIN
             PERFORM pg_advisory_xact_lock(9912, hashtext(current_database()));
             RETURN NEW;
         END $$;
         CREATE TRIGGER repro_retire_gate
         BEFORE INSERT ON pgqueue.workers
         FOR EACH ROW EXECUTE FUNCTION pgqueue.repro_retire_gate();",
    )
    .execute(&pool)
    .await
    .unwrap();
    // Held for the rest of the test: the backend never recovers, which is the
    // condition the bound exists for.
    let gate = hold_gate(&pool, 9912, &db.database).await;

    let shutdown = CancellationToken::new();
    let worker = Worker::builder(db.queue.clone())
        .register_job(repro_noop)
        .timers(test_timers())
        .poll_interval(Duration::from_millis(50))
        .build()
        .unwrap();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));

    // Parked inside the startup heartbeat, so cancelling lands on the startup
    // retirement path — whose own lease write hits the same wedged gate.
    wait_for_lock_waiter(&db, "%INSERT INTO pgqueue.workers%", "the startup heartbeat never reached the gated INSERT")
        .await;
    shutdown.cancel();

    tokio::time::timeout(Duration::from_secs(20), run)
        .await
        .expect("run_until never returned while its lease retirement was wedged")
        .unwrap()
        .unwrap();
    gate.rollback().await.unwrap();
}

// ---------------------------------------------------------------------------
// The dead-owner grace must run from the lease, not from the attempt
// ---------------------------------------------------------------------------

/// Cycle 2 made `pgqueue.job_is_stuck` additive, so a configured `timeout_ms`
/// stopped disabling dead-owner recovery. It measured that branch's grace from
/// the *attempt* (`COALESCE(touched_at, started_at)`), which for anything
/// running longer than the grace is already in the past — so an attempt became
/// recoverable the instant its owner missed one heartbeat window. A workers-row
/// lock wait, a pool stall, a GC pause or a failover was then enough to cancel
/// and re-run work that was still in flight, where before it had the whole
/// `timeout + grace`.
///
/// The grace now also runs from `expires_at`, and expired leases are retained
/// that long so the lapse stays observable.
#[sqlx::test(migrations = "./migrations")]
async fn test_a_recently_lapsed_lease_still_covers_its_attempt(pool: PgPool) {
    let grace = Duration::from_secs(30);
    let db = TestDb::with(pool.clone(), |builder| builder.sweep_grace(grace)).await;
    let handle = db
        .queue
        .enqueue_raw(new_job("stalled_heartbeat", |job| {
            job.config.timeout = Some(Duration::from_secs(3600));
        }))
        .await
        .unwrap();
    let owner = Uuid::now_v7();
    db.queue.write_worker_info(owner, json!({}), None, Duration::from_secs(30)).await.unwrap();
    assert_eq!(db.queue.dequeue(1, owner).await.unwrap().len(), 1);
    // The attempt has been running far longer than the grace and its heartbeat
    // has just stalled out: the whole point is that this is *not* a crash.
    backdate_job_liveness(&db, handle.job_id()).await;
    expire_worker(&db, owner).await;

    let stuck = |grace_ms: i64| {
        let pool = pool.clone();
        let id = handle.job_id();
        async move {
            sqlx::query_scalar::<_, bool>(
                "SELECT pgqueue.job_is_stuck(j, $2, lease.expires_at)
                 FROM pgqueue.jobs j
                 LEFT JOIN pgqueue.workers lease
                     ON lease.id = j.worker_id AND lease.queue = j.queue
                 WHERE j.id = $1",
            )
            .bind(id)
            .bind(grace_ms)
            .fetch_one(&pool)
            .await
            .unwrap()
        }
    };
    assert!(!stuck(30_000).await, "a lease that lapsed a second ago still covers the attempt it leased");

    let mut sweeper = db.queue.sweeper();
    let report = sweeper.sweep().await.unwrap();
    assert!(report.cancelling.is_empty(), "{report:?}");
    let lease_exists = |owner: Uuid| {
        let pool = pool.clone();
        async move {
            sqlx::query_scalar::<_, bool>("SELECT EXISTS (SELECT 1 FROM pgqueue.workers WHERE id = $1)")
                .bind(owner)
                .fetch_one(&pool)
                .await
                .unwrap()
        }
    };
    assert!(
        lease_exists(owner).await,
        "purging the lease immediately would make its expiry unobservable, and a \
         missing row is indistinguishable from one that lapsed an hour ago"
    );
    let status: String = sqlx::query_scalar("SELECT status::text FROM pgqueue.jobs WHERE id = $1")
        .bind(handle.job_id())
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(status, "running");

    // Past the grace the owner is provably gone, and the hour of timeout still
    // left on the attempt does not hold recovery back.
    sqlx::query("UPDATE pgqueue.workers SET expires_at = now() - interval '1 minute' WHERE id = $1")
        .bind(owner)
        .execute(&pool)
        .await
        .unwrap();
    assert!(stuck(30_000).await);
    assert_eq!(sweeper.sweep().await.unwrap().cancelling, vec![handle.job_id()]);
    assert!(!lease_exists(owner).await, "the lapsed lease is purged too");
    sweeper.release().await;
}

// ---------------------------------------------------------------------------
// A transient finalize failure during the shutdown drain is still retried
// ---------------------------------------------------------------------------

#[pgqueue::job(name = "repro_drain_result", max_attempts = 1)]
async fn repro_drain_result(_: (), state: JobState<DrainGate>) -> anyhow::Result<i64> {
    state.0.started.notify_one();
    state.0.release.notified().await;
    Ok(42)
}

/// Installs a `BEFORE UPDATE` trigger that fails the first attempt to move a
/// row out of `running`, standing in for one transient database error — a pool
/// timeout is entirely plausible during a drain, where the drain, the sweeper,
/// both heartbeaters and every other processor contend for the same pool.
async fn fail_first_finalization(pool: &PgPool) {
    sqlx::raw_sql(
        r#"
        CREATE SEQUENCE pgqueue.repro_finalization_failures;
        CREATE FUNCTION pgqueue.repro_fail_first_finalization() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            IF nextval('pgqueue.repro_finalization_failures') = 1 THEN
                RAISE EXCEPTION 'injected transient finalization failure';
            END IF;
            RETURN NEW;
        END
        $$;
        CREATE TRIGGER repro_fail_first_finalization
        BEFORE UPDATE ON pgqueue.jobs
        FOR EACH ROW
        WHEN (OLD.status = 'running' AND NEW.status IS DISTINCT FROM OLD.status)
        EXECUTE FUNCTION pgqueue.repro_fail_first_finalization();
        "#,
    )
    .execute(pool)
    .await
    .expect("install transient finalization failure");
}

/// Closing intake is shutdown's *first* durable act, so a finalize retry bound
/// to that token has an already-ready cancellation branch for the whole drain:
/// a handler that completed during the drain and hit one transient error got
/// zero retries, whatever `shutdown_grace` said. The row then stayed
/// `running`/`worker_id = W`, the sweeper's dead-owner branch marked it
/// `aborting`, and with the default `max_attempts = 1` it was finalized
/// `aborted` with `result = NULL` — a job that succeeded reported as aborted,
/// its result lost, and `enqueue_and_wait` handed an `Error::Job`.
///
/// The lease is deliberately kept alive for the whole drain, so nothing can
/// recover the row while the retry is in flight; the retry belongs to the grace
/// window, not to the instant intake closes.
#[sqlx::test(migrations = "./migrations")]
async fn test_finalize_retries_a_transient_failure_when_shutdown_has_started(pool: PgPool) {
    let db = TestDb::new(pool_with_max(&pool, 10).await).await;
    let gate =
        DrainGate { started: Arc::new(tokio::sync::Notify::new()), release: Arc::new(tokio::sync::Notify::new()) };
    let handle = db.queue.enqueue(repro_drain_result::job(())).await.unwrap();

    let shutdown = CancellationToken::new();
    let worker = Worker::builder(db.queue.clone())
        .register_job(repro_drain_result)
        .state(gate.clone())
        .timers(test_timers())
        .poll_interval(Duration::from_millis(50))
        .shutdown_grace(Duration::from_secs(30))
        .build()
        .unwrap();
    let worker_id = worker.id();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));

    gate.started.notified().await;
    fail_first_finalization(&pool).await;
    // Shutdown is under way and intake is durably closed *before* the handler
    // returns, so the finalize below happens entirely inside the drain.
    shutdown.cancel();
    wait_for_worker_intake_closed(&db, worker_id).await;
    gate.release.notify_one();

    tokio::time::timeout(Duration::from_secs(30), run)
        .await
        .expect("worker did not finish its drain")
        .unwrap()
        .unwrap();

    let row = db
        .queue
        .fetch_job(handle.job_id())
        .await
        .unwrap()
        .expect("the job row must survive its own successful attempt");
    assert_eq!(
        (row.status, row.attempts, row.result),
        (JobStatus::Complete, 1, Some(json!(42))),
        "a handler that succeeded during the drain must not lose its result to \
         one transient finalize error"
    );
}

// ---------------------------------------------------------------------------
// Recovery backoff, the availability probe, and outcomes PostgreSQL cannot store
// ---------------------------------------------------------------------------

/// Every sweeper test used `retry_delay: Duration::ZERO`, so the delayed arm of
/// `retry_swept_abandoned_batch` — the `$4::bigint[]` the per-row delays ride
/// in on — had never run. A recovered attempt has to respect its own backoff:
/// requeueing it to run immediately is how a crash loop becomes a hot loop.
#[sqlx::test(migrations = "./migrations")]
async fn test_swept_attempt_is_requeued_behind_its_own_retry_delay(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let handle = db
        .queue
        .enqueue_raw(crate::with_config("delayed_sweep", |config| {
            config.max_attempts = 3;
            config.retry_delay = Duration::from_secs(600);
        }))
        .await
        .unwrap();
    let owner = Uuid::now_v7();
    db.queue.write_worker_info(owner, json!({}), None, Duration::from_secs(30)).await.unwrap();
    assert_eq!(db.queue.dequeue(1, owner).await.unwrap().len(), 1);
    backdate_job_liveness(&db, handle.job_id()).await;
    expire_worker(&db, owner).await;

    let mut sweeper = db.queue.sweeper();
    // The lease is expired, so the mark and the requeue land in one pass.
    let report = sweeper.sweep().await.unwrap();
    assert_eq!(report.cancelling, vec![handle.job_id()]);
    assert_eq!(report.swept, vec![handle.job_id()]);
    sweeper.release().await;

    let row = db.queue.fetch_job(handle.job_id()).await.unwrap().expect("the swept job row");
    assert_eq!(row.status, JobStatus::Queued);
    assert!(
        row.scheduled_at > jiff::Timestamp::now() + jiff::SignedDuration::from_secs(300),
        "a recovered attempt must wait out its retry delay, not run immediately: \
         scheduled_at {}",
        row.scheduled_at
    );
}

/// The underfilled-batch probe: `work_available` stays true for ready rows the
/// claim stepped over because another transaction holds their row locks, so a
/// burst worker cannot mistake transient lock contention for a drained queue.
#[sqlx::test(migrations = "./migrations")]
async fn test_dequeue_probe_reports_ready_work_the_claim_could_not_lock(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    db.queue.enqueue_raw(new_job("held", |_| {})).await.unwrap();
    let worker_id = Uuid::now_v7();
    db.queue.write_worker_info(worker_id, json!({}), None, Duration::from_secs(30)).await.unwrap();

    // Hold the only ready row's lock from another session, as a concurrent
    // claim in flight would.
    let mut holder = pool.begin().await.unwrap();
    sqlx::query("SELECT id FROM pgqueue.jobs WHERE queue = 'default' FOR UPDATE")
        .fetch_all(&mut *holder)
        .await
        .unwrap();

    let work_available = pgqueue::__test_support::dequeue_worker_probe(&db.queue, 8, worker_id).await.unwrap();
    assert!(work_available, "a ready row held by another transaction is still work");

    holder.rollback().await.unwrap();
    // With the lock released the row claims normally, and a drained queue
    // reports no ready work behind the underfilled batch.
    let jobs = pgqueue::__test_support::dequeue_worker(&db.queue, 8, worker_id).await.unwrap();
    assert_eq!(jobs.len(), 1);
    let work_available = pgqueue::__test_support::dequeue_worker_probe(&db.queue, 8, worker_id).await.unwrap();
    assert!(!work_available, "a drained queue has no ready work behind an underfilled batch");
}

/// PostgreSQL `jsonb` cannot represent `\0` (`22P05`) and `text` cannot store
/// it either (`22021`), so finalizing either of these attempts is a write the
/// server refuses permanently. Finalization retries every database error once a
/// second until shutdown, so before the fix each of these attempts held its
/// processor slot forever, left its row `running` under a live lease, and — once
/// that lease finally lapsed — handed the sweeper a job whose next attempt
/// wedged identically.
#[pgqueue::job(name = "repro_nul_result", max_attempts = 3, retry_delay_ms = 0)]
async fn repro_nul_result(_: ()) -> anyhow::Result<String> {
    Ok("bad\u{0}value".to_string())
}

#[pgqueue::job(name = "repro_nul_error", max_attempts = 1)]
async fn repro_nul_error(_: ()) -> anyhow::Result<()> {
    anyhow::bail!("bad\u{0}message")
}

#[sqlx::test(migrations = "./migrations")]
async fn test_worker_finalizes_an_attempt_whose_result_or_error_carries_a_nul(pool: PgPool) {
    // Finalization and slot release are the subjects here, not stuck-job
    // recovery. Prevent trybuild compilation from starving this current-thread
    // runtime long enough for the test fixture's 1ms sweep grace to abort a
    // live attempt.
    let db = TestDb::with(pool.clone(), |builder| builder.sweep_grace(Duration::from_secs(60))).await;
    let result_id = db.queue.enqueue(repro_nul_result::job(())).await.unwrap().job_id();
    let error_id = db.queue.enqueue(repro_nul_error::job(())).await.unwrap().job_id();

    let shutdown = CancellationToken::new();
    let worker = Worker::builder(db.queue.clone())
        .register_job(repro_nul_result)
        .register_job(repro_nul_error)
        // One slot, so the second job runs only once the first has genuinely
        // let go of it: an attempt that cannot be finalized never does.
        .concurrency(1)
        .timers(test_timers())
        .poll_interval(Duration::from_millis(50))
        .build()
        .unwrap();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));

    let result_row = wait_terminal(&db.queue, result_id, 30).await;
    let error_row = wait_terminal(&db.queue, error_id, 30).await;
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(10), run).await.expect("worker did not stop").unwrap().unwrap();

    // A result that cannot be encoded is a decode failure — deterministic, so
    // the two remaining attempts are not spent rediscovering it.
    assert_eq!(result_row.status, JobStatus::Failed);
    assert_eq!(result_row.attempts, 1);
    assert_eq!(result_row.error.as_deref(), Some("decode: result encode: job result must not contain NUL"));
    assert!(result_row.result.is_none(), "{:?}", result_row.result);

    // The handler's own message survives; only the byte PostgreSQL cannot store
    // is substituted, and visibly so.
    assert_eq!(error_row.status, JobStatus::Failed);
    assert_eq!(error_row.error.as_deref(), Some("failed: bad\u{fffd}message"));
}

#[pgqueue::job(max_attempts = 3, timeout_ms = 0)]
async fn arbitrary_number(number: String) -> anyhow::Result<Value> {
    Ok(serde_json::from_str(&number)?)
}

#[sqlx::test(migrations = "./migrations")]
async fn test_worker_releases_its_slot_after_an_unrepresentable_numeric_result(pool: PgPool) {
    let db = TestDb::with(pool, |builder| builder.sweep_grace(Duration::from_secs(60))).await;
    let mut ids = Vec::new();
    for number in ["1e200000", "1e-20000", "1e308"] {
        ids.push(db.queue.enqueue(arbitrary_number::job(number.into())).await.unwrap().job_id());
    }
    let shutdown = CancellationToken::new();
    let worker = Worker::builder(db.queue.clone())
        .register_job(arbitrary_number)
        .concurrency(1)
        .timers(test_timers())
        .poll_interval(Duration::from_millis(50))
        .build()
        .unwrap();
    let health = worker.health();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));
    let mut rows = Vec::new();
    for id in ids {
        rows.push(wait_terminal(&db.queue, id, 10).await);
    }
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(10), run).await.expect("worker did not stop").unwrap().unwrap();

    for row in &rows[..2] {
        assert_eq!(row.status, JobStatus::Failed);
        assert_eq!(row.attempts, 1, "an unrepresentable result must not spend the other attempts");
        assert!(row.error.as_deref().unwrap().starts_with("decode: result encode:"));
        assert!(row.result.is_none());
    }
    assert_eq!(rows[2].status, JobStatus::Complete, "the same processor must keep handling jobs");
    assert_eq!(rows[2].result.as_ref().unwrap().to_string(), format!("1{}", "0".repeat(308)));
    assert!(!health.snapshot().failures.iter().any(|failure| failure.component == WorkerComponent::Finalize));
}

/// `JobError::new` substitutes the NUL, but it is not the only way to make a
/// `JobError`: the fields are public, and `IntoJobResult` hands a handler's own
/// error back verbatim rather than rebuilding it. An `anyhow::bail!` is
/// `String`-backed and so goes through `JobError::failed`, which is why the
/// constructor's guard looked sufficient; this shape skips the constructor
/// entirely and reaches the `text` column with the byte still in it.
#[pgqueue::job(name = "repro_nul_job_error", max_attempts = 1)]
async fn repro_nul_job_error(_: ()) -> Result<(), JobError> {
    Err(JobError { kind: JobErrorKind::Failed, message: "bad\u{0}input".to_string() })
}

#[sqlx::test(migrations = "./migrations")]
async fn test_worker_finalizes_a_handler_built_job_error_whose_message_carries_a_nul(pool: PgPool) {
    // See the sibling NUL finalization test: the sweeper is deliberately kept
    // out of this finalization and processor-slot assertion.
    let db = TestDb::with(pool.clone(), |builder| builder.sweep_grace(Duration::from_secs(60))).await;
    let wedge_id = db.queue.enqueue(repro_nul_job_error::job(())).await.unwrap().job_id();
    let next_id = db.queue.enqueue(repro_noop::job(())).await.unwrap().job_id();

    let shutdown = CancellationToken::new();
    let worker = Worker::builder(db.queue.clone())
        .register_job(repro_nul_job_error)
        .register_job(repro_noop)
        // One slot, so the follow-up job runs only once the unstorable error has
        // genuinely released it.
        .concurrency(1)
        .timers(test_timers())
        .poll_interval(Duration::from_millis(50))
        .build()
        .unwrap();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));

    let wedge_row = wait_terminal(&db.queue, wedge_id, 30).await;
    let next_row = wait_terminal(&db.queue, next_id, 30).await;
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(10), run).await.expect("worker did not stop").unwrap().unwrap();

    assert_eq!(wedge_row.status, JobStatus::Failed);
    assert_eq!(wedge_row.error.as_deref(), Some("failed: bad\u{fffd}input"));
    assert_eq!(next_row.status, JobStatus::Complete, "the processor slot must come back");
}

// ---------------------------------------------------------------------------
// Results and metadata nested deeper than they can be read back
// ---------------------------------------------------------------------------

/// A JSON document of `depth` nested objects wrapped around `null`.
fn nested_json(depth: usize) -> Value {
    let mut value = Value::Null;
    for _ in 0..depth {
        value = json!({ "a": value });
    }
    value
}

/// The mirror image of the NUL result above: `jsonb` stores nesting happily,
/// but `serde_json`'s deserializer stops at 128 nested containers, so writing
/// one poisoned every later read of the queue — `fetch_job`, `jobs_page`, the
/// dashboard listing — and wedged the dequeue path itself: the batch decode
/// fails inside the claiming transaction, rolls the claim back, and the next
/// dequeue re-selects the same row. Refusing it only in `validate_finalization`
/// is not enough either: `finalize` retries every error once a second until
/// shutdown, so the attempt would hold its processor slot forever. It has to
/// fail the attempt where a handler's value becomes a success.
#[pgqueue::job(name = "repro_deep_result", max_attempts = 3, retry_delay_ms = 0)]
async fn repro_deep_result(_: ()) -> anyhow::Result<Value> {
    Ok(nested_json(128))
}

#[sqlx::test(migrations = "./migrations")]
async fn test_worker_fails_an_attempt_whose_result_nests_deeper_than_it_can_be_read_back(pool: PgPool) {
    // This is the same finalization path and needs the same sweep isolation as
    // the two NUL cases above.
    let db = TestDb::with(pool.clone(), |builder| builder.sweep_grace(Duration::from_secs(60))).await;
    let deep_id = db.queue.enqueue(repro_deep_result::job(())).await.unwrap().job_id();
    let next_id = db.queue.enqueue(repro_noop::job(())).await.unwrap().job_id();

    let shutdown = CancellationToken::new();
    let worker = Worker::builder(db.queue.clone())
        .register_job(repro_deep_result)
        .register_job(repro_noop)
        // One slot, so the follow-up runs only once the undecodable result has
        // genuinely released it.
        .concurrency(1)
        .timers(test_timers())
        .poll_interval(Duration::from_millis(50))
        .build()
        .unwrap();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));

    let deep_row = wait_terminal(&db.queue, deep_id, 30).await;
    let next_row = wait_terminal(&db.queue, next_id, 30).await;
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(10), run).await.expect("worker did not stop").unwrap().unwrap();

    // Deterministic, so the two remaining attempts are not spent rediscovering
    // it — exactly as the NUL result is treated.
    assert_eq!(deep_row.status, JobStatus::Failed);
    assert_eq!(deep_row.attempts, 1);
    assert_eq!(
        deep_row.error.as_deref(),
        Some("decode: result encode: job result must not nest deeper than 127 levels")
    );
    assert!(deep_row.result.is_none(), "{:?}", deep_row.result);
    assert_eq!(next_row.status, JobStatus::Complete, "the processor slot must come back");
}

/// `pgqueue.workers.metadata` is `jsonb`, which cannot hold `\0`. The lease
/// write reports its failure through health and a log rather than to a caller,
/// so a worker built with such metadata started, created no lease, and — since
/// dequeueing requires a live accepting lease — processed nothing for as long as
/// it ran. The enqueue side already refuses the same byte in a payload or in
/// job meta, so this is validated where the rest of the worker configuration is.
#[sqlx::test(migrations = "./migrations")]
async fn test_worker_build_rejects_metadata_containing_a_nul(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;

    let built =
        Worker::builder(db.queue.clone()).register_job(repro_noop).metadata(json!({ "host": "web-01\u{0}" })).build();
    let Err(error) = built else {
        panic!("metadata PostgreSQL cannot store must be refused");
    };
    match error {
        Error::Config(message) => {
            assert!(message.contains("must not contain NUL"), "{message}");
        }
        other => panic!("unstorable metadata must be a config error: {other}"),
    }

    // The same metadata without the byte still builds, so the guard is about
    // NUL and not about metadata.
    Worker::builder(db.queue.clone())
        .register_job(repro_noop)
        .metadata(json!({ "host": "web-01" }))
        .build()
        .expect("valid metadata must still build");
}

/// The mirror image of the NUL above, and just as terminal: `jsonb` stores the
/// nesting happily, but `serde_json` stops decoding at 128 nested containers, so
/// the lease write succeeds and every later read of `pgqueue.workers` — the
/// dashboard's worker listing and detail pages — fails for the whole queue
/// instead. Nothing surfaces it to the operator who set the metadata, so it is
/// refused at build time next to the byte guard.
#[sqlx::test(migrations = "./migrations")]
async fn test_worker_build_rejects_metadata_nested_deeper_than_it_can_be_read_back(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;

    let built = Worker::builder(db.queue.clone()).register_job(repro_noop).metadata(nested_json(128)).build();
    let Err(error) = built else {
        panic!("metadata this crate can never read back must be refused");
    };
    match error {
        Error::Config(message) => {
            assert_eq!(message, "worker metadata must not nest deeper than 127 levels");
        }
        other => panic!("undecodable metadata must be a config error: {other}"),
    }

    // 127 is the deepest `serde_json` decodes, so it must still build: an
    // off-by-one here silently narrows the documented metadata space.
    Worker::builder(db.queue.clone())
        .register_job(repro_noop)
        .metadata(nested_json(127))
        .build()
        .expect("the deepest readable metadata must still build");
}

// ---------------------------------------------------------------------------
// A cancelled sweep must not strand the sweep-leadership advisory lock
// ---------------------------------------------------------------------------

/// How many backends hold the single-key advisory lock `key` in this database.
async fn sweep_lock_holders(pool: &PgPool, key: i64) -> i64 {
    let class_id = i64::from((key as u64 >> 32) as u32);
    let object_id = i64::from(key as u64 as u32);
    sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM pg_locks
         WHERE locktype = 'advisory' AND granted AND objsubid = 1
           AND classid::bigint = $1 AND objid::bigint = $2
           AND database = (SELECT oid FROM pg_database WHERE datname = current_database())",
    )
    .bind(class_id)
    .bind(object_id)
    .fetch_one(pool)
    .await
    .expect("count sweep lock holders")
}

/// Sweep leadership is a *session*-scoped `pg_try_advisory_lock`, so the lock
/// outlives the statement that took it. Cancelling the acquisition between the
/// server granting the lock and the client storing the connection handed a
/// locked connection back to the pool: sqlx resets nothing on release, so the
/// lock sat on an idle pooled connection with nothing left holding a handle to
/// it — `release` and `Drop` both saw `None`. Sweeping then stopped **cluster
/// wide** and silently, because a refused sweeper reports the ordinary
/// `leader: false`. `sweep_loop` drops exactly this future on every worker
/// shutdown, and the first tick fires immediately at startup.
#[sqlx::test(migrations = "./migrations")]
async fn test_cancelled_sweep_acquisition_leaves_no_stranded_leadership_lock(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let key = pgqueue::__test_support::sweep_lock_key(&db.database, db.queue.name());
    // Its own pool: another worker process in the cluster, which is what a
    // stranded lock locks out until sqlx reaps the connection holding it.
    let elsewhere = Queue::builder("postgres://unused").pool(pool_with_max(&pool, 2).await).connect().await.unwrap();

    for attempt in 1..=8 {
        let mut sweeper = db.queue.sweeper();
        {
            let mut sweep = Box::pin(sweeper.sweep());
            // One poll at a time, with the task's own waker, stopping the moment
            // the server reports the lock granted: the answer is then in flight
            // and the future has not seen it — the exact window a `select!` on a
            // cancellation token drops the future in.
            let mut granted = false;
            for _ in 0..200 {
                let progress =
                    std::future::poll_fn(|context| std::task::Poll::Ready(Future::poll(sweep.as_mut(), context))).await;
                assert!(
                    progress.is_pending(),
                    "attempt {attempt}: the sweep finished before its lock could be cancelled"
                );
                if sweep_lock_holders(&pool, key).await > 0 {
                    granted = true;
                    break;
                }
            }
            assert!(granted, "attempt {attempt}: the sweep never took its leadership lock");
        }
        drop(sweeper);

        wait_until(
            Duration::from_secs(5),
            Duration::from_millis(20),
            &format!("attempt {attempt}: a cancelled acquisition stranded its advisory lock"),
            || async { sweep_lock_holders(&pool, key).await == 0 },
        )
        .await;

        let mut replacement = elsewhere.sweeper();
        assert!(
            replacement.sweep().await.unwrap().leader,
            "attempt {attempt}: another process was refused leadership by a stranded lock"
        );
        replacement.release().await;
    }
}

// ---------------------------------------------------------------------------
// A superseded attempt must be cancelled and release its processor slot
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct SupersededProbe {
    started: Arc<tokio::sync::Notify>,
    ticks: Arc<AtomicUsize>,
}

/// Never reads its cancellation token and has no timeout, so nothing but the
/// abort loop can end it. Each tick stands in for an externally visible side
/// effect the attempt is no longer entitled to produce.
#[pgqueue::job(max_attempts = 3, timeout_ms = 0)]
async fn repro_superseded(_: (), probe: JobState<SupersededProbe>) -> anyhow::Result<()> {
    probe.0.started.notify_one();
    loop {
        tokio::time::sleep(Duration::from_millis(20)).await;
        probe.0.ticks.fetch_add(1, Ordering::SeqCst);
    }
}

/// Runs only once the superseded attempt has given its processor slot back.
#[pgqueue::job(max_attempts = 1)]
async fn repro_after_supersede(_: ()) -> anyhow::Result<()> {
    Ok(())
}

/// Recovery hands a stuck attempt to another worker by requeueing the row and
/// letting the next dequeue claim it with `attempts + 1` under a new owner. The
/// original handler keeps running: its row is neither `aborting` (the new
/// attempt is `running`) nor gone, so the abort poll used to have nothing to
/// match on, and the attempt held its processor slot for as long as it lived —
/// forever, with the timeout disabled.
#[sqlx::test(migrations = "./migrations")]
async fn test_superseded_attempt_is_cancelled_and_releases_its_processor_slot(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db.queue.enqueue(repro_superseded::job(())).await.unwrap().job_id();
    let probe = SupersededProbe { started: Arc::new(tokio::sync::Notify::new()), ticks: Arc::new(AtomicUsize::new(0)) };
    let worker = Worker::builder(db.queue.clone())
        .timers(test_timers())
        .poll_interval(Duration::from_millis(50))
        .concurrency(1)
        // Long enough that an attempt waiting out the cooperative grace would
        // still be ticking when this test's deadline passes: the row already
        // belongs to another attempt, so there is nothing left to clean up.
        .abort_grace(Duration::from_secs(60))
        .shutdown_grace(Duration::from_secs(5))
        .register_job(repro_superseded)
        .register_job(repro_after_supersede)
        .state(probe.clone())
        .build()
        .unwrap();
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));
    tokio::time::timeout(Duration::from_secs(10), probe.started.notified()).await.expect("handler did not start");

    // The new owner is a live worker: without a lease of its own the sweeper
    // would treat the re-claimed row as abandoned, which is a different story.
    let thief = Uuid::now_v7();
    let _thief_lease = leased_consumer(&db.queue, thief).await;
    assert_eq!(
        sqlx::query(
            "UPDATE pgqueue.jobs
             SET attempts = attempts + 1, worker_id = $2,
                 started_at = now(), touched_at = now()
             WHERE id = $1 AND status = 'running'"
        )
        .bind(id)
        .bind(thief)
        .execute(db.queue.pool())
        .await
        .unwrap()
        .rows_affected(),
        1,
        "the row was not running when the re-claim was simulated"
    );

    wait_until(
        Duration::from_secs(10),
        Duration::from_millis(25),
        "the superseded handler kept running after its row was re-claimed",
        // Three consecutive quiet windows, not one: this handler ticks every
        // 20 ms, so a single 100 ms window in which a loaded runtime did not poll
        // the task looks exactly like a force-stop. That direction fails open —
        // revert the cancellation and the test still passes — so it takes more
        // than one window to believe.
        || async {
            for _ in 0..3 {
                let before = probe.ticks.load(Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(100)).await;
                if probe.ticks.load(Ordering::SeqCst) != before {
                    return false;
                }
            }
            true
        },
    )
    .await;

    // The row is the new attempt's; a cancelled predecessor must not write it.
    let row = db.queue.fetch_job(id).await.unwrap().expect("job row");
    assert_eq!(row.status, JobStatus::Running);
    assert_eq!(row.attempts, 2);
    assert_eq!(row.worker_id, Some(thief));

    // The slot is the point: with one processor, nothing else runs until the
    // superseded attempt lets go of it.
    let next = db.queue.enqueue(repro_after_supersede::job(())).await.unwrap().job_id();
    wait_until(Duration::from_secs(10), Duration::from_millis(25), "the processor slot was never released", || async {
        db.queue.fetch_job(next).await.unwrap().is_some_and(|row| row.status == JobStatus::Complete)
    })
    .await;

    shutdown.cancel();
    run.await.unwrap().unwrap();
}

// ---------------------------------------------------------------------------
// The sweeper's own requeue must end the attempt it takes the row from
// ---------------------------------------------------------------------------

/// Tick counters per attempt number, so a job that is briefly in flight twice
/// at once can be asked *which* of its attempts is still producing side
/// effects. Indexed by attempt; `max_attempts` bounds the index.
#[derive(Clone)]
struct AttemptTicks(Arc<[AtomicUsize; 4]>);

impl AttemptTicks {
    fn new() -> Self {
        Self(Arc::new(std::array::from_fn(|_| AtomicUsize::new(0))))
    }

    fn count(&self, attempt: u32) -> usize {
        self.0[attempt as usize].load(Ordering::SeqCst)
    }

    /// Whether `attempt` produced a side effect during `window`.
    async fn advanced(&self, attempt: u32, window: Duration) -> bool {
        let before = self.count(attempt);
        tokio::time::sleep(window).await;
        self.count(attempt) > before
    }

    /// Whether `attempt` produced no side effect across three consecutive
    /// windows.
    ///
    /// The invariant these callers assert is "the handler was force-stopped", and
    /// a single quiet window does not carry it: this handler ticks every 20 ms
    /// with no `await` between ticks other than the sleep, so on a loaded runner
    /// one 100 ms window in which the task simply was not polled satisfies it.
    /// That direction fails *open* — revert the cancellation and the test still
    /// passes — so it takes more than one window to believe. Three consecutive
    /// quiet windows cannot be a scheduling artefact of a runtime that is still
    /// running the task.
    async fn stopped(&self, attempt: u32, window: Duration) -> bool {
        for _ in 0..3 {
            if self.advanced(attempt, window).await {
                return false;
            }
        }
        true
    }
}

/// Ticks under its own attempt number forever: no timeout and no cancellation
/// check, so nothing but the abort loop can end it, and each tick stands in for
/// an externally visible side effect that attempt is no longer entitled to
/// produce.
#[pgqueue::job(max_attempts = 3, timeout_ms = 0)]
async fn repro_sweep_requeued(_: (), ticks: JobState<AttemptTicks>, ctx: JobContext) -> anyhow::Result<()> {
    loop {
        ticks.0.0[ctx.attempt() as usize].fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Builds the worker both tests below drive by hand: it sweeps and heartbeats
/// only when they say so, and polls for aborts on a long enough interval that
/// the whole recovery lands between two polls.
fn requeue_worker(db: &TestDb, ticks: &AttemptTicks, concurrency: usize) -> Worker {
    Worker::builder(db.queue.clone())
        .timers(WorkerTimers {
            // Polls once at startup and then well after the recovery below: a
            // poll landing inside the `aborting` window would cancel the
            // attempt for a reason these tests are not about.
            abort: Duration::from_secs(5),
            // Recovery and the lease are driven by hand here.
            sweep: Duration::from_secs(3_600),
            worker_info: Duration::from_secs(3_600),
            ..test_timers()
        })
        .poll_interval(Duration::from_millis(50))
        .concurrency(concurrency)
        // Long enough that an attempt waiting out a cooperative grace would
        // still be ticking at the end of these tests: a row that is no longer
        // this attempt's leaves it nothing to clean up, so it gets none.
        .abort_grace(Duration::from_secs(60))
        .shutdown_grace(Duration::from_secs(1))
        .register_job(repro_sweep_requeued)
        .state(ticks.clone())
        .build()
        .unwrap()
}

/// Parks every lease write the worker makes until the returned transaction
/// ends, so a lease these tests expire stays expired for the sweep that has to
/// see it lapsed.
///
/// A heartbeat is an `INSERT ... ON CONFLICT`, and `BEFORE INSERT` fires before
/// the conflict is resolved, so a parked heartbeat holds no row lock: the
/// tests' own updates of that row run straight past it.
async fn park_worker_heartbeats(db: &TestDb) -> sqlx::Transaction<'static, sqlx::Postgres> {
    sqlx::raw_sql(
        "CREATE FUNCTION pgqueue.repro_park_heartbeat() RETURNS trigger
         LANGUAGE plpgsql AS $$
         BEGIN
             PERFORM pg_advisory_xact_lock(919, hashtext(current_database()));
             RETURN NEW;
         END $$;
         CREATE TRIGGER repro_park_heartbeat
         BEFORE INSERT ON pgqueue.workers
         FOR EACH ROW EXECUTE FUNCTION pgqueue.repro_park_heartbeat();",
    )
    .execute(db.queue.pool())
    .await
    .expect("install the heartbeat park");
    hold_gate(db.queue.pool(), 919, &db.database).await
}

/// Brings a stalled worker's lease back, as its next heartbeat does — an
/// upsert, not an update: the sweep that declared the owner gone is the one
/// that purged its row, so by the time this runs there is usually nothing left
/// to update.
async fn revive_worker_lease(db: &TestDb, worker_id: Uuid) {
    sqlx::query(
        "INSERT INTO pgqueue.workers (id, queue, expires_at, heartbeat_at, accepting)
         VALUES ($1, $2, now() + interval '1 hour', now(), true)
         ON CONFLICT (id) DO UPDATE
         SET expires_at = EXCLUDED.expires_at,
             heartbeat_at = EXCLUDED.heartbeat_at,
             accepting = EXCLUDED.accepting",
    )
    .bind(worker_id)
    .bind(db.queue.name())
    .execute(db.queue.pool())
    .await
    .expect("revive worker lease");
}

/// Runs the recovery both tests below share: the owner's lease lapses and its
/// attempt passes the grace, so one pass marks the row `aborting` and requeues
/// it — all while the handler is still running.
async fn sweep_away_the_attempt(db: &TestDb, sweeper: &mut Sweeper, id: Uuid, worker_id: Uuid) {
    // A full pass, lease purge included: an owner is past the cooperative abort
    // window only once its `pgqueue.workers` row is *gone*, and that purge is
    // what removes it (`Sweeper::recover_stuck_jobs`). The worker's returning
    // heartbeat is an upsert and recreates what the purge took, which is why
    // `revive_worker_lease` is one too.
    expire_worker(db, worker_id).await;
    backdate_job_liveness(db, id).await;
    let report = sweeper.sweep().await.unwrap();
    assert_eq!(report.cancelling, vec![id], "phase one did not mark the abandoned attempt");
    assert_eq!(report.swept, vec![id], "phase two did not requeue the abandoned attempt");
}

/// Recovery's second phase requeues an abandoned attempt, and it used to leave
/// `worker_id` — like `attempts`, which it deliberately keeps — exactly as the
/// still-live owner holds it. `aborting_of` then read that row back as "still
/// running as claimed" and signalled nothing, so a worker that was only
/// *presumed* dead (a stalled heartbeat, a pool stall, a GC pause) kept running
/// the handler, kept producing side effects it was no longer entitled to, and
/// kept its processor slot — forever, with the timeout disabled.
///
/// The queued row is all the worker has to notice here: nothing has re-claimed
/// it, and with one processor nothing can until this attempt lets go.
#[sqlx::test(migrations = "./migrations")]
async fn test_sweep_requeued_attempt_is_cancelled_before_anything_reclaims_the_row(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    // Leadership is taken here and held for the whole test, so the worker's own
    // sweep loop cannot slip a pass between this test's two.
    let mut sweeper = db.queue.sweeper();
    assert!(sweeper.sweep().await.unwrap().leader);

    let ticks = AttemptTicks::new();
    let id = db.queue.enqueue(repro_sweep_requeued::job(())).await.unwrap().job_id();
    let worker = requeue_worker(&db, &ticks, 1);
    let worker_id = worker.id();
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));
    wait_until(Duration::from_secs(10), Duration::from_millis(10), "the first attempt never started", || async {
        ticks.count(1) > 0
    })
    .await;

    let gate = park_worker_heartbeats(&db).await;
    sweep_away_the_attempt(&db, &mut sweeper, id, worker_id).await;
    let row = db.queue.fetch_job(id).await.unwrap().expect("job row");
    assert_eq!(row.status, JobStatus::Queued);
    assert_eq!(row.attempts, 1, "recovery leaves the attempt counter alone");
    assert_eq!(row.worker_id, None, "a requeued row must not still advertise the owner it was taken from");

    // The heartbeat comes back, so the worker may take work again — it just has
    // no processor free to take any with.
    gate.rollback().await.unwrap();
    revive_worker_lease(&db, worker_id).await;
    wait_until(
        Duration::from_secs(30),
        Duration::from_millis(25),
        "the attempt recovery took the row from kept running",
        || async { ticks.stopped(1, Duration::from_millis(100)).await },
    )
    .await;
    // The slot is the point: with one processor, the requeued row cannot be
    // worked at all until the displaced attempt lets go of it.
    wait_until(Duration::from_secs(30), Duration::from_millis(25), "the processor slot was never released", || async {
        ticks.count(2) > 0
    })
    .await;

    shutdown.cancel();
    run.await.unwrap().unwrap();
}

/// Clearing the owner is only half of it: the same worker can re-claim the very
/// row recovery took from it, as the next attempt. While in-flight attempts
/// were keyed by job id alone, that newcomer overwrote its predecessor's entry
/// and the displaced attempt was never asked about again — so it kept running,
/// two handlers deep on one job.
#[sqlx::test(migrations = "./migrations")]
async fn test_sweep_requeued_attempt_is_cancelled_when_its_own_worker_reclaims_the_row(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let mut sweeper = db.queue.sweeper();
    assert!(sweeper.sweep().await.unwrap().leader);

    let ticks = AttemptTicks::new();
    let id = db.queue.enqueue(repro_sweep_requeued::job(())).await.unwrap().job_id();
    // Two processors, so the replacement attempt can start while the displaced
    // one is still holding a slot.
    let worker = requeue_worker(&db, &ticks, 2);
    let worker_id = worker.id();
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));
    wait_until(Duration::from_secs(10), Duration::from_millis(10), "the first attempt never started", || async {
        ticks.count(1) > 0
    })
    .await;

    let gate = park_worker_heartbeats(&db).await;
    sweep_away_the_attempt(&db, &mut sweeper, id, worker_id).await;
    assert!(
        ticks.advanced(1, Duration::from_millis(60)).await,
        "the displaced attempt ended before its worker could re-claim the row, so \
         this run never reached the case the test is here for"
    );

    // The stalled worker's heartbeat comes back and it takes the same row as
    // attempt 2: two live attempts of one job on one worker.
    gate.rollback().await.unwrap();
    revive_worker_lease(&db, worker_id).await;
    wait_until(
        Duration::from_secs(10),
        Duration::from_millis(10),
        "the worker never re-claimed the requeued row",
        || async { ticks.count(2) > 0 },
    )
    .await;

    wait_until(
        Duration::from_secs(30),
        Duration::from_millis(25),
        "the attempt recovery took the row from kept running",
        || async { ticks.stopped(1, Duration::from_millis(100)).await },
    )
    .await;
    assert!(
        ticks.advanced(2, Duration::from_millis(100)).await,
        "the attempt that owns the row must not be cancelled with its predecessor"
    );

    // The row is attempt 2's, and its cancelled predecessor wrote nothing.
    let row = db.queue.fetch_job(id).await.unwrap().expect("job row");
    assert_eq!(row.status, JobStatus::Running);
    assert_eq!(row.attempts, 2);
    assert_eq!(row.worker_id, Some(worker_id));

    shutdown.cancel();
    run.await.unwrap().unwrap();
}

// ---------------------------------------------------------------------------
// Hand-written rows, and the fetch loop's own backoffs
// ---------------------------------------------------------------------------

/// `backoff` is a tagged `jsonb` this crate only ever writes from
/// `JobRetryBackoff`, but a row written by hand — or by a newer build during a
/// mixed-version rollout — can carry a tag this build has never heard of. A
/// strict decoder would fail the batch inside the claiming transaction, roll
/// the claim back, and fail identically when the next dequeue re-selected the
/// same row: one unreadable value blocking every job queued behind it, claim
/// after claim, until repaired by hand. The lenient decode claims the batch
/// and runs the poison row with the flat retry delay instead.
#[sqlx::test(migrations = "./migrations")]
async fn test_dequeue_claims_a_batch_carrying_an_unreadable_backoff(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let mut healthy = Vec::new();
    for index in 0..4 {
        healthy.push(db.queue.enqueue_raw(new_job(&format!("healthy-{index}"), |_| {})).await.unwrap().job_id());
    }
    let poison = db.queue.enqueue_raw(new_job("poison", |_| {})).await.unwrap().job_id();

    // A row that predates the column's CHECK is exactly what the decoder has to
    // survive; dropping it here is what puts one on disk.
    sqlx::query("ALTER TABLE pgqueue.jobs DROP CONSTRAINT IF EXISTS jobs_backoff_check").execute(&pool).await.unwrap();
    sqlx::query(r#"UPDATE pgqueue.jobs SET backoff = '{"type":"linear"}' WHERE id = $1"#)
        .bind(poison)
        .execute(&pool)
        .await
        .unwrap();

    let consumer = leased_consumer(&db.queue, Uuid::now_v7()).await;
    let batch = consumer.dequeue(10).await.expect("one unreadable strategy must not fail the batch it rode in on");
    assert_eq!(batch.len(), 5, "every row the statement claimed must reach the caller");
    let claimed = batch
        .iter()
        .map(pgqueue::Attempt::job)
        .find(|row| row.id == poison)
        .expect("the poison row is claimed like any other");
    assert_eq!(
        claimed.backoff,
        pgqueue::JobRetryBackoff::None,
        "an unreadable strategy falls back to the flat retry delay"
    );

    // And the listing the operator would reach for to find the bad row keeps
    // working for the whole queue instead of failing outright.
    let page = db
        .queue
        .jobs_page(pgqueue::JobFilter::default())
        .await
        .expect("the listing must survive one unreadable strategy");
    assert_eq!(page.len(), 5, "the listing must still page the queue");
}

#[pgqueue::job(name = "repro_zero_timeout", max_attempts = 1)]
async fn repro_zero_timeout(_: ()) -> anyhow::Result<()> {
    tokio::time::sleep(Duration::from_millis(50)).await;
    Ok(())
}

/// `#[pgqueue::job(timeout_ms = 0)]` means "no timeout" and `JobConfig` refuses
/// a zero `Duration`, so 0 in the column is a value only a hand-written row
/// carries — and it decoded as `Some(Duration::ZERO)`, cancelling every attempt
/// before the handler could run its first statement. That is the harmful
/// reading of the two, which is the one `JobRetention::from_result_ttl_ms`
/// deliberately avoids for its own column.
#[sqlx::test(migrations = "./migrations")]
async fn test_worker_runs_an_attempt_whose_stored_timeout_is_zero(pool: PgPool) {
    // A grace the attempt cannot outlive: `pgqueue.job_is_stuck` reads the same
    // zero as `started_at + grace < now()`, so the default zero grace would
    // sweep the attempt out from under the reading actually under test.
    let db = TestDb::with(pool.clone(), |builder| builder.sweep_grace(Duration::from_secs(300))).await;
    let id = db.queue.enqueue(repro_zero_timeout::job(())).await.unwrap().job_id();

    // A row that predates the column's CHECK, as above.
    sqlx::query("ALTER TABLE pgqueue.jobs DROP CONSTRAINT IF EXISTS jobs_timeout_ms_check")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE pgqueue.jobs SET timeout_ms = 0 WHERE id = $1").bind(id).execute(&pool).await.unwrap();

    Worker::builder(db.queue.clone())
        .register_job(repro_zero_timeout)
        .burst(true)
        .timers(test_timers())
        .dequeue_timeout(Duration::from_millis(50))
        .run()
        .await
        .unwrap();

    let row = db.queue.fetch_job(id).await.unwrap().unwrap();
    let error = row.error.clone().unwrap_or_default();
    assert_eq!(row.status, JobStatus::Complete, "a stored zero timeout must mean unlimited, not instant: {error}");
}

#[sqlx::test(migrations = "./migrations")]
async fn test_shutdown_interrupts_the_dequeue_error_backoff(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let worker = test_worker(db.queue.clone()).register_job(counts).state(Arc::new(AtomicU32::new(0))).build().unwrap();
    let health = worker.health();
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));
    wait_until(Duration::from_secs(3), Duration::from_millis(10), "worker did not report ready health", || async {
        health.snapshot().status == WorkerHealthStatus::Ready
    })
    .await;

    // Break dequeues (and only dequeues, among the shutdown-path statements):
    // the workers table stays intact so intake close and lease writes succeed.
    sqlx::query("ALTER TABLE pgqueue.jobs RENAME TO jobs_hidden").execute(&pool).await.unwrap();

    // The fetch loop reports the dequeue failure immediately before its
    // 1-second error sleep, so health flipping to a Dequeue failure means the
    // sleep has just started.
    wait_until(Duration::from_secs(3), Duration::from_millis(5), "worker never reported a dequeue failure", || async {
        health.snapshot().failures.iter().any(|failure| failure.component == WorkerComponent::Dequeue)
    })
    .await;

    // The fetcher is now no more than a few polls into its 1-second error
    // backoff. An uncancellable backoff holds shutdown for the rest of that
    // second (the caretaker joins the fetcher before releasing its lease);
    // racing it against `stop` ends it immediately.
    //
    // The suite's only wall-clock ceiling, so it is placed from both sides
    // rather than picked. Measured on this box (Darwin 25.5, PostgreSQL 18.4 in
    // Docker, the full 342-test integration binary running in parallel beside
    // it): the interrupted shutdown this asserts takes 7.5ms, and 2-4ms with
    // the test run alone. With the backoff made uncancellable it takes
    // 999.6ms-1.004s over three runs — it cannot be quicker than the second it
    // is sleeping, less however long this test took to notice the health flip
    // and cancel. 800ms is therefore ~100x the failure-free case and ~200ms
    // clear of the fastest uninterrupted one, which is the margin that has to
    // absorb a loaded runner on both sides.
    let started = std::time::Instant::now();
    shutdown.cancel();
    run.await.unwrap().unwrap();
    let elapsed = started.elapsed();
    assert!(elapsed < Duration::from_millis(800), "shutdown during the dequeue error backoff took {elapsed:?}");
}

/// The worker-level half of the claim's liveness. A prefix of held rows longer
/// than the registry-filtered claim's unlocked window used to starve everything
/// behind it, and a *burst* worker turned that into a false success: it retried,
/// processed nothing, and returned as though the queue had drained, leaving
/// every row queued. This asserts the contract the fetch loop actually promises
/// — work it can see, it takes.
#[sqlx::test(migrations = "./migrations")]
async fn test_a_burst_worker_drains_behind_a_locked_prefix(pool: PgPool) {
    let control = TestDb::new(pool_with_max(&pool, 4).await).await;
    let db = TestDb::new(pool_with_max(&pool, 4).await).await;
    for _ in 0..40 {
        db.queue.enqueue(counts::job(())).await.unwrap().unwrap();
    }

    // Hold the first 33 rows in the claim order: one more than the window, so
    // only the fallback walk can reach past them.
    let mut lock = control.queue.pool().begin().await.unwrap();
    let held = sqlx::query_scalar::<_, Uuid>(
        "SELECT id FROM pgqueue.jobs
         WHERE queue = $1 AND status = 'queued'
         ORDER BY priority, scheduled_at, id LIMIT 33 FOR UPDATE",
    )
    .bind(db.queue.name())
    .fetch_all(&mut *lock)
    .await
    .unwrap();
    assert_eq!(held.len(), 33);

    let counter = Arc::new(AtomicU32::new(0));
    let worker = test_worker(db.queue.clone())
        .register_job(counts)
        .state(counter.clone())
        .concurrency(1)
        .burst(true)
        .dequeue_timeout(Duration::from_secs(2))
        .build()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(30), worker.run_until(CancellationToken::new()))
        .await
        .expect("burst worker did not finish")
        .unwrap();

    // The seven rows nobody holds must have run, not been reported as drained.
    assert_eq!(counter.load(Ordering::SeqCst), 7, "the burst reported a drain it had not achieved");
    lock.rollback().await.unwrap();
}

// ---------------------------------------------------------------------------
// A worker that cannot record outcomes must not report itself healthy
// ---------------------------------------------------------------------------

/// `finalize` retries a failed outcome write for as long as the attempt lives,
/// and that retry loop pins the processor slot running it. Once every slot is
/// pinned there no processor goes idle, so the fetch loop is never asked for
/// work and `WorkerComponent::Dequeue` — the component that would otherwise
/// report the outage — never issues a statement to fail.
///
/// So a worker with a healthy read path and a broken write path was stalled
/// outright, with claimable work sitting in the queue, while `Worker::health`
/// reported `Ready` and the dashboard's `/health` (a `SELECT`, which even a full
/// disk answers) agreed. That was reachable three ways in testing: a full
/// tablespace, an operator holding a row lock, and a full `NOTIFY` queue.
///
/// The failure has to *clear* too, or the first blip would strand a healthy
/// worker outside a load balancer for the rest of its run.
#[sqlx::test(migrations = "./migrations")]
async fn test_a_worker_that_cannot_finalize_reports_itself_degraded(pool: PgPool) {
    let db = TestDb::new(pool_with_max(&pool, 6).await).await;
    let handle = db.queue.enqueue(repro_noop::job(())).await.unwrap().unwrap();
    // Persistent, unlike `fail_first_finalization`: the point is a worker that
    // stays stuck, not one that retries its way out.
    sqlx::raw_sql(
        r#"
        CREATE FUNCTION pgqueue.repro_always_fail_finalization() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            RAISE EXCEPTION 'injected persistent finalization failure';
        END
        $$;
        CREATE TRIGGER repro_always_fail_finalization
        BEFORE UPDATE ON pgqueue.jobs
        FOR EACH ROW
        WHEN (OLD.status = 'running' AND NEW.status IS DISTINCT FROM OLD.status)
        EXECUTE FUNCTION pgqueue.repro_always_fail_finalization();
        "#,
    )
    .execute(db.queue.pool())
    .await
    .expect("install persistent finalization failure");

    let shutdown = CancellationToken::new();
    let worker = test_worker(db.queue.clone()).register_job(repro_noop).concurrency(1).build().unwrap();
    let health = worker.health();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));

    wait_until(
        Duration::from_secs(30),
        Duration::from_millis(25),
        "worker never reported the finalize failure",
        || {
            let health = health.clone();
            async move {
                let snapshot = health.snapshot();
                snapshot.status == WorkerHealthStatus::Degraded
                    && snapshot.failures.iter().any(|failure| failure.component == WorkerComponent::Finalize)
            }
        },
    )
    .await;

    sqlx::raw_sql("DROP TRIGGER repro_always_fail_finalization ON pgqueue.jobs")
        .execute(db.queue.pool())
        .await
        .expect("remove the finalization failure");

    wait_until(Duration::from_secs(30), Duration::from_millis(25), "worker health never cleared", || {
        let health = health.clone();
        async move { health.snapshot().status == WorkerHealthStatus::Ready }
    })
    .await;
    assert_eq!(wait_terminal(&db.queue, handle.id(), 30).await.status, JobStatus::Complete);

    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(30), run).await.expect("worker did not stop").unwrap().unwrap();
}

#[pgqueue::job]
async fn finalizes_by_payload(_: String) {}

/// The same stall with other slots still working. `Finalize` recovered on any success, so it was last-writer-wins
/// across processors: one slot pinned retrying a write that cannot land was reported for the instant after each of its
/// retries, and cleared by every other slot's finish — a worker losing slots one at a time read `Ready` until the last.
#[sqlx::test(migrations = "./migrations")]
async fn test_a_stalled_finalization_stays_reported_while_other_slots_finish(pool: PgPool) {
    let db = TestDb::new(pool_with_max(&pool, 6).await).await;
    sqlx::raw_sql(
        r#"
        CREATE FUNCTION pgqueue.repro_fail_one_finalization() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            RAISE EXCEPTION 'injected finalization failure for one job';
        END
        $$;
        CREATE TRIGGER repro_fail_one_finalization
        BEFORE UPDATE ON pgqueue.jobs
        FOR EACH ROW
        WHEN (OLD.status = 'running' AND NEW.status IS DISTINCT FROM OLD.status AND OLD.payload = '"stalled"'::jsonb)
        EXECUTE FUNCTION pgqueue.repro_fail_one_finalization();
        "#,
    )
    .execute(db.queue.pool())
    .await
    .expect("install one job's finalization failure");
    let stalled = db.queue.enqueue(finalizes_by_payload::job("stalled".into())).await.unwrap().unwrap();

    let shutdown = CancellationToken::new();
    let worker = test_worker(db.queue.clone()).register_job(finalizes_by_payload).concurrency(2).build().unwrap();
    let health = worker.health();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));
    let finalize_failing = |health: &pgqueue::WorkerHealth| {
        health.snapshot().failures.iter().any(|f| f.component == WorkerComponent::Finalize)
    };
    wait_until(
        Duration::from_secs(30),
        Duration::from_millis(25),
        "the stalled finalization was never reported",
        || {
            let reported = finalize_failing(&health);
            async move { reported }
        },
    )
    .await;

    for round in 0..3 {
        let healthy = db.queue.enqueue(finalizes_by_payload::job(format!("healthy {round}"))).await.unwrap().unwrap();
        assert_eq!(wait_terminal(&db.queue, healthy.id(), 30).await.status, JobStatus::Complete);
        assert!(
            finalize_failing(&health),
            "round {round}: another slot's finish cleared the stalled slot's failure: {:?}",
            health.snapshot()
        );
    }

    sqlx::raw_sql("DROP TRIGGER repro_fail_one_finalization ON pgqueue.jobs")
        .execute(db.queue.pool())
        .await
        .expect("remove the finalization failure");
    wait_until(Duration::from_secs(30), Duration::from_millis(25), "worker health never cleared", || {
        let ready = health.snapshot().status == WorkerHealthStatus::Ready;
        async move { ready }
    })
    .await;
    assert_eq!(wait_terminal(&db.queue, stalled.id(), 30).await.status, JobStatus::Complete);

    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(30), run).await.expect("worker did not stop").unwrap().unwrap();
}

// ---------------------------------------------------------------------------
// The worker lease, and writes retried behind a lock
// ---------------------------------------------------------------------------

/// `WorkerTimers::worker_info` is the interval of the worker's liveness lease, not only of a stats refresh: every
/// heartbeat renews the lease for three intervals, and claiming and crash recovery are both timed by that lease. Pinned
/// through the public listing, so the documented relationship cannot drift from the multiplier behind it.
#[sqlx::test(migrations = "./migrations")]
async fn test_a_worker_lease_lasts_three_worker_info_intervals(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let worker = test_worker(db.queue.clone())
        .register_job(counts)
        .state(Arc::new(AtomicU32::new(0)))
        .timers(WorkerTimers { worker_info: Duration::from_millis(300), ..test_timers() })
        .build()
        .unwrap();
    let worker_id = worker.id();
    let token = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(token.clone()));

    let lease = wait_for_some(
        Duration::from_secs(5),
        Duration::from_millis(10),
        "worker did not register its lease",
        || async { list_workers(&db.queue).await.into_iter().find(|worker| worker.id == worker_id) },
    )
    .await;
    // Two readings of the server clock within one statement, normally microseconds apart, in whichever order the
    // server evaluates them: the slack still tells three intervals from two or four, however the readings fall.
    let ttl = lease.expires_at.duration_since(lease.heartbeat_at);
    assert!(
        ttl > jiff::SignedDuration::from_millis(850) && ttl < jiff::SignedDuration::from_millis(1_050),
        "a lease must last three worker_info intervals, not {ttl:?}"
    );

    token.cancel();
    run.await.unwrap().unwrap();
}

/// Neither `retry` nor `retry_swept` ever writes `aborted`, so one that is refused and finds the row finished `aborted`
/// under its own attempt has found somebody else's transition: the sweeper's, which counted the abort on its own
/// handle. It was counted again on this worker's heartbeat, so the per-worker statistics the dashboard shows claimed an
/// abort the worker never made, where the same refusal finding the row requeued is `RetriedElsewhere` and uncounted.
#[sqlx::test(migrations = "./migrations")]
async fn test_a_sweeper_abort_a_refused_retry_finds_is_not_counted_on_the_worker(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    // Leadership is held here, so the worker's own sweep loop cannot settle the attempt first.
    let mut sweeper = db.queue.sweeper();
    assert!(sweeper.sweep().await.unwrap().leader);

    let handle = db.queue.enqueue(fails_as_sweeper_marks::job(())).await.unwrap().unwrap();
    let state = SweepFailureRace {
        started: Arc::new(tokio::sync::Notify::new()),
        release: Arc::new(tokio::sync::Notify::new()),
    };
    let worker = test_worker(db.queue.clone())
        .register_job(fails_as_sweeper_marks)
        .register_job(counts)
        .state(state.clone())
        .state(Arc::new(AtomicU32::new(0)))
        // One slot, so the follow-up below runs only once the swept attempt's `process`, counters included, is done.
        .concurrency(1)
        // Rare enough that the abort poll does not see the operator's abort before recovery settles it.
        .timers(WorkerTimers { abort: Duration::from_secs(5), ..test_timers() })
        .build()
        .unwrap();
    let worker_id = worker.id();
    let token = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(token.clone()));

    state.started.notified().await;
    wait_for_worker_lease(&db.queue, worker_id).await;
    // An operator abort recovery finishes before the worker sees it: the attempt is past its timeout, and without a
    // dedupe key a live lease does not keep the sweeper off it.
    assert!(db.queue.abort_job(handle.id(), "operator said stop").await.unwrap());
    backdate_job_liveness(&db, handle.id()).await;
    assert_eq!(sweeper.sweep().await.unwrap().swept, vec![handle.id()]);
    let row = handle.fetch_job().await.unwrap();
    assert_eq!((row.status, row.attempts), (JobStatus::Aborted, 1));
    assert_eq!(row.error.as_deref(), Some("operator said stop"));

    // The handler now fails retryably, and its refused retry finds the row the sweeper finished.
    state.release.notify_one();
    let follow_up = db.queue.enqueue(counts::job(())).await.unwrap().unwrap();
    assert_eq!(wait_terminal(&db.queue, follow_up.id(), 10).await.status, JobStatus::Complete);

    let stats = wait_for_some(
        Duration::from_secs(5),
        Duration::from_millis(25),
        "the worker never heartbeated its completion",
        || async {
            list_workers(&db.queue)
                .await
                .into_iter()
                .find(|worker| worker.id == worker_id)
                .map(|worker| worker.stats)
                .filter(|stats| stats["complete"] == 1)
        },
    )
    .await;
    assert_eq!(stats["aborted"], 0, "the sweeper's abort was counted on the worker that found it: {stats}");
    assert_eq!(stats["retried"], 0, "{stats}");
    assert_eq!(db.queue.stats().aborted, 1, "the sweeper counts the abort it made, once");

    token.cancel();
    run.await.unwrap().unwrap();
    sweeper.release().await;
}

#[derive(Clone)]
struct BurstSlotHold {
    started: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

/// Keeps its processor busy until released. No timeout, so only the test ends it.
#[pgqueue::job(max_attempts = 1, timeout_ms = 0)]
async fn holds_a_burst_slot(_: (), hold: JobState<BurstSlotHold>) -> anyhow::Result<()> {
    hold.0.started.notify_one();
    hold.0.release.notified().await;
    Ok(())
}

/// A burst processor concludes a drain once its idle timeout has passed and a fetch begun after its demand came back
/// underfilled. It used to conclude that while a *newer* fetch, sized by its own demand, was still in flight, and
/// leave: the row that fetch then claimed went into the intake buffer with no idle processor left to take it, and sat
/// `running` under this worker with no handler until a busy slot happened to free up — or, once past its timeout, until
/// the sweeper aborted it as stuck without its handler ever having run.
///
/// The fetch is held at a gate across the idle processor's timeout, standing in for the few milliseconds of claim it
/// took to straddle that timeout in production.
#[sqlx::test(migrations = "./migrations")]
async fn test_a_burst_processor_does_not_drain_under_a_fetch_that_counted_it(pool: PgPool) {
    const DEQUEUE_TIMEOUT: Duration = Duration::from_secs(3);
    const GATE: i32 = 47_130;
    let db = TestDb::new(pool.clone()).await;
    install_statement_gate(
        &pool,
        "repro_hold_counts_claim",
        GATE,
        "UPDATE",
        "OLD.status = 'queued' AND NEW.status = 'running' AND NEW.name = 'counts'",
    )
    .await;
    let hold =
        BurstSlotHold { started: Arc::new(tokio::sync::Notify::new()), release: Arc::new(tokio::sync::Notify::new()) };
    let counter = Arc::new(AtomicU32::new(0));
    db.queue.enqueue(holds_a_burst_slot::job(())).await.unwrap().unwrap();
    let worker = test_worker(db.queue.clone())
        .register_job(holds_a_burst_slot)
        .register_job(counts)
        .state(hold.clone())
        .state(counter.clone())
        .concurrency(2)
        .burst(true)
        .dequeue_timeout(DEQUEUE_TIMEOUT)
        .build()
        .unwrap();
    let run = tokio::spawn(worker.run_until(CancellationToken::new()));

    // One processor is busy for good. The other went idle at startup and has seen its fetch come back empty, so only
    // its idle timeout stands between it and a drain.
    hold.started.notified().await;
    let idle_since = tokio::time::Instant::now();
    let gate = hold_gate(&pool, GATE, &db.database).await;
    let job = db.queue.enqueue(counts::job(())).await.unwrap().unwrap();
    wait_for_advisory_waiter(&pool, GATE, "the idle processor's fetch never reached the new job").await;
    // The idle processor's timeout runs out while the fetch its demand sized is still in flight.
    tokio::time::sleep_until(idle_since + DEQUEUE_TIMEOUT + Duration::from_secs(1)).await;
    gate.rollback().await.unwrap();

    let row = wait_terminal(&db.queue, job.id(), 10).await;
    assert_eq!(row.status, JobStatus::Complete, "the fetched job must run while the other slot is still busy");
    assert_eq!(counter.load(Ordering::SeqCst), 1);

    hold.release.notify_one();
    tokio::time::timeout(Duration::from_secs(30), run)
        .await
        .expect("the burst must still drain and return")
        .unwrap()
        .unwrap();
}

async fn lease_of(queue: &Queue, worker_id: Uuid) -> pgqueue::WorkerInfo {
    list_workers(queue).await.into_iter().find(|worker| worker.id == worker_id).expect("the worker's lease")
}

/// A heartbeat does not wait its turn behind the queue's pool at all: it renews on a connection of its own. Waiting
/// there cost it its deadline whichever way the deadline was counted. Counted from before the pool handed a connection
/// over, it abandoned heartbeats that were merely queued, and the pool is fair, so each retry the overdue ticker issued
/// at once went to the back of the queue: with every acquire waiting longer than one interval, but within the 30s
/// acquire timeout every other worker operation tolerates, no heartbeat ever landed and a live worker's lease lapsed
/// under it. Counted from after, a saturated pool held the heartbeat for as long as that acquire timeout: the whole
/// default lease.
#[sqlx::test(migrations = "./migrations")]
async fn test_a_heartbeat_is_not_queued_behind_a_saturated_pool(pool: PgPool) {
    let db = TestDb::new(pool_with_max(&pool, 1).await).await;
    let worker_id = Uuid::now_v7();
    db.queue.write_worker_lease(worker_id, json!({}), None, Duration::from_secs(30), true).await.unwrap();
    let before = lease_of(&db.queue, worker_id).await;

    let held = db.queue.pool().acquire().await.unwrap();
    tokio::time::timeout(
        Duration::from_secs(10),
        pgqueue::__test_support::write_worker_lease_within(
            &db.queue,
            worker_id,
            Duration::from_secs(30),
            Duration::from_millis(500),
        ),
    )
    .await
    .expect("the heartbeat waited for the pool's only connection")
    .expect("a heartbeat on a connection of its own renews the lease whatever the pool is doing");
    drop(held);

    let after = lease_of(&db.queue, worker_id).await;
    assert!(after.heartbeat_at > before.heartbeat_at, "the heartbeat did not land: {before:?} -> {after:?}");
    assert!(after.expires_at > before.expires_at, "the lease was not renewed: {before:?} -> {after:?}");
}

/// A heartbeat that cannot open a connection of its own — a server or role out of connections, a proxy refusing one
/// more — renews through the pool instead: the lease is what keeps a worker's attempts its own, and a worker whose pool
/// still works has to keep it. There it takes an idle connection as it is, or waits its turn for one, and a heartbeat
/// that waited is still written: its deadline bounds the write, not the wait, which counted in abandoned heartbeats
/// that were merely queued, and the pool is fair, so each retry went to the back of the queue and none ever landed.
#[sqlx::test(migrations = "./migrations")]
async fn test_a_heartbeat_queued_for_a_connection_is_not_abandoned(pool: PgPool) {
    let (relay, queue) = relayed_queue(&pool, 1).await;
    relay.refuse_new_connections();
    let worker_id = Uuid::now_v7();
    let ttl = Duration::from_secs(30);
    let mut lease = pgqueue::__test_support::WorkerLease::new(&queue, worker_id);
    tokio::time::timeout(Duration::from_secs(10), lease.renew(ttl, Duration::from_millis(500)))
        .await
        .expect("the heartbeat never returned")
        .expect("a heartbeat refused a connection of its own renews on the pool's idle one");
    let before = lease_expiry(&pool, worker_id).await;

    let held = queue.pool().acquire().await.unwrap();
    let heartbeat = tokio::spawn(async move { lease.renew(ttl, Duration::from_millis(500)).await });
    // Queued behind the pool's only connection for four of its deadlines.
    tokio::time::sleep(Duration::from_secs(2)).await;
    drop(held);
    tokio::time::timeout(Duration::from_secs(10), heartbeat)
        .await
        .expect("the heartbeat never returned")
        .unwrap()
        .expect("a heartbeat that waited its turn for a connection must still be written");
    assert!(lease_expiry(&pool, worker_id).await > before, "the lease was not renewed");
}

/// A connection the server accepts without ever serving it — a session-mode proxy out of server connections queues a
/// client at its first statement — is as useless to a heartbeat as one it refuses, and costs it more: found out only at
/// its deadline, there was no time left to renew through the pool. Opening one is given half the deadline.
#[sqlx::test(migrations = "./migrations")]
async fn test_a_heartbeat_whose_own_connection_is_never_served_renews_through_the_pool(pool: PgPool) {
    let (relay, queue) = relayed_queue(&pool, 1).await;
    relay.stall_new_connections();
    let worker_id = Uuid::now_v7();
    let mut lease = pgqueue::__test_support::WorkerLease::new(&queue, worker_id);
    tokio::time::timeout(Duration::from_secs(10), lease.renew(Duration::from_secs(30), Duration::from_secs(2)))
        .await
        .expect("the heartbeat never returned")
        .expect("a heartbeat whose own connection is never served renews through the pool");
    assert!(lease_expiry(&pool, worker_id).await > jiff::Timestamp::now(), "the lease was not written");
}

/// How many backends of this test's database wait on a lock while writing a worker lease.
async fn lease_writes_waiting_on_a_lock(pool: &PgPool) -> i64 {
    sqlx::query_scalar::<_, i64>(
        "SELECT count(*)::bigint FROM pg_stat_activity
         WHERE datname = current_database() AND wait_event_type = 'Lock'
           AND query LIKE '%INSERT INTO pgqueue.workers%'",
    )
    .fetch_one(pool)
    .await
    .unwrap()
}

/// The other half: a heartbeat whose *write* stalls is still abandoned at its deadline — here behind the lease row an
/// operator's open transaction holds, which `Sweeper::recover_stuck_jobs` relies on blocking heartbeats and nothing
/// else — and gives its connection back when it is: the server ends the write's wait for the lock at the same deadline.
/// Abandoned on the client alone, the upsert stayed queued on the server behind the lock for as long as the operator
/// kept it, holding the connection under it: on the pool, sqlx pings a connection before pooling it again, so the
/// heartbeat loop, retrying at once on its overdue ticker, parked one more pooled connection per interval until the
/// pool was gone, and the worker's dequeues, abort polls and finalizations with it; on a connection of the heartbeat's
/// own, each retry would leave one more of the server's connections waiting.
#[sqlx::test(migrations = "./migrations")]
async fn test_a_heartbeat_blocked_on_its_lease_row_gives_its_connection_back(pool: PgPool) {
    let db = TestDb::new(pool_with_max(&pool, 1).await).await;
    let worker_id = Uuid::now_v7();
    let ttl = Duration::from_secs(30);
    db.queue.write_worker_lease(worker_id, json!({}), None, ttl, true).await.unwrap();

    let mut operator = pool.begin().await.unwrap();
    sqlx::query("SELECT id FROM pgqueue.workers WHERE id = $1 FOR UPDATE")
        .bind(worker_id)
        .fetch_one(&mut *operator)
        .await
        .unwrap();
    let mut lease = pgqueue::__test_support::WorkerLease::new(&db.queue, worker_id);
    for round in 0..3 {
        let error = tokio::time::timeout(Duration::from_secs(10), lease.renew(ttl, Duration::from_millis(250)))
            .await
            .unwrap_or_else(|_| panic!("round {round}: a blocked heartbeat must be abandoned at its deadline"))
            .expect_err("the lease row is locked");
        assert!(
            matches!(error, Error::WorkerTask("worker lease heartbeat exceeded its deadline")),
            "round {round}: {error}"
        );
        // With the lock still held, nothing is left waiting on it.
        wait_until(
            Duration::from_secs(5),
            Duration::from_millis(10),
            &format!("round {round}: the abandoned heartbeat kept waiting on the lease row's lock"),
            || async { lease_writes_waiting_on_a_lock(&pool).await == 0 },
        )
        .await;
    }

    operator.rollback().await.unwrap();
    lease.renew(ttl, Duration::from_secs(5)).await.expect("released, the lease row takes the next heartbeat");
}

/// A queue whose pool of `size` connections, every one of them open and idle, reaches the test database through a
/// [`SilenceableRelay`].
async fn relayed_queue(pool: &PgPool, size: u32) -> (SilenceableRelay, Queue) {
    let options = pool.connect_options();
    let relay = SilenceableRelay::start((options.get_host().to_string(), options.get_port())).await;
    let relayed_options = options.as_ref().clone().host("127.0.0.1").port(relay.address.port());
    let relayed = sqlx::postgres::PgPoolOptions::new()
        .min_connections(size)
        .max_connections(size)
        .connect_with(relayed_options)
        .await
        .unwrap();
    crate::warm_pool(&relayed, size).await;
    let queue = Queue::builder("postgres://unused").pool(relayed).connect().await.unwrap();
    (relay, queue)
}

/// When `worker_id`'s lease expires, read over `pool` rather than through a relay a test may have silenced.
async fn lease_expiry(pool: &PgPool, worker_id: Uuid) -> jiff::Timestamp {
    sqlx::query_scalar::<_, jiff_sqlx::Timestamp>("SELECT expires_at FROM pgqueue.workers WHERE id = $1")
        .bind(worker_id)
        .fetch_one(pool)
        .await
        .unwrap()
        .to_jiff()
}

/// Ends every other session of the test database: silenced ones are still attached on the server, and would keep the
/// database from being dropped.
async fn end_other_sessions(pool: &PgPool) {
    sqlx::query(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity
         WHERE datname = current_database() AND pid <> pg_backend_pid()",
    )
    .execute(pool)
    .await
    .unwrap();
}

/// A heartbeat's deadline has to bound every wait for a connection to write on, and the pool's own `acquire` is one it
/// cannot see into: sqlx pings an idle pooled connection before handing it out, bounded only by the pool's 30s acquire
/// timeout, and a connection that went dead while idle — a failover, a partition, a NAT entry dropped for idleness —
/// never answers the ping. A heartbeat that took one sat out the whole default lease before it could even fail, and a
/// failover that killed every pooled connection held it once per connection, so the lease of a worker that was alive
/// and running its attempts lapsed and the sweeper took them away from it.
#[sqlx::test(migrations = "./migrations")]
async fn test_a_heartbeat_is_not_held_by_pooled_connections_that_went_dead_while_idle(pool: PgPool) {
    let (relay, queue) = relayed_queue(&pool, 4).await;
    let worker_id = Uuid::now_v7();
    queue.write_worker_lease(worker_id, json!({}), None, Duration::from_secs(30), true).await.unwrap();
    let before = lease_expiry(&pool, worker_id).await;

    relay.silence_open_connections();
    tokio::time::timeout(
        Duration::from_secs(10),
        pgqueue::__test_support::write_worker_lease_within(
            &queue,
            worker_id,
            Duration::from_secs(30),
            Duration::from_millis(500),
        ),
    )
    .await
    .expect("the heartbeat waited on a pooled connection that will never answer")
    .expect("a heartbeat that does not wait on the pool renews the lease");
    assert!(lease_expiry(&pool, worker_id).await > before, "the lease was not renewed");

    drop(queue);
    end_other_sessions(&pool).await;
}

/// The heartbeat's own connection can go dead the same way, and then the heartbeat on it is abandoned at its deadline
/// and the connection with it: the next heartbeat — at once, since the ticker is overdue by then — opens a fresh one,
/// while the lease still has an interval to run. Kept, the dead connection would have failed every heartbeat after. A
/// heartbeat dropped with its future mid-write, as shutdown drops one, leaves its connection the same way: in a state
/// nothing knows, never to be written on again.
#[sqlx::test(migrations = "./migrations")]
async fn test_a_heartbeat_replaces_its_own_connection_once_it_stops_answering(pool: PgPool) {
    let (relay, queue) = relayed_queue(&pool, 1).await;
    let worker_id = Uuid::now_v7();
    let ttl = Duration::from_secs(30);
    let mut lease = pgqueue::__test_support::WorkerLease::new(&queue, worker_id);
    lease.renew(ttl, Duration::from_millis(500)).await.expect("the first heartbeat opens the lease's own connection");
    let before = lease_expiry(&pool, worker_id).await;

    relay.silence_open_connections();
    let error = tokio::time::timeout(Duration::from_secs(10), lease.renew(ttl, Duration::from_millis(500)))
        .await
        .expect("a heartbeat on a connection that stopped answering must be abandoned at its deadline")
        .expect_err("the silenced connection cannot answer");
    assert!(matches!(error, Error::WorkerTask("worker lease heartbeat exceeded its deadline")), "{error}");
    tokio::time::timeout(Duration::from_secs(10), lease.renew(ttl, Duration::from_millis(500)))
        .await
        .expect("the heartbeat after an abandoned one never returned")
        .expect("the heartbeat after an abandoned one must land on a fresh connection");
    let renewed = lease_expiry(&pool, worker_id).await;
    assert!(renewed > before, "the lease was not renewed");

    relay.silence_open_connections();
    let dropped = tokio::time::timeout(Duration::from_millis(200), lease.renew(ttl, Duration::from_secs(5))).await;
    assert!(dropped.is_err(), "a write on a silenced connection cannot finish");
    tokio::time::timeout(Duration::from_secs(10), lease.renew(ttl, Duration::from_millis(500)))
        .await
        .expect("the heartbeat after a dropped one never returned")
        .expect("the heartbeat after a dropped one must land on a fresh connection");
    assert!(lease_expiry(&pool, worker_id).await > renewed, "the lease was not renewed");

    drop(lease);
    drop(queue);
    end_other_sessions(&pool).await;
}

/// How many backends of this test's database wait on a lock while running the guarded finish. Nothing else in the
/// test below opens with the same `candidate` CTE.
async fn blocked_finishes(pool: &PgPool) -> i64 {
    sqlx::query_scalar::<_, i64>(
        "SELECT count(*)::bigint FROM pg_stat_activity
         WHERE datname = current_database() AND wait_event_type = 'Lock' AND query LIKE $1",
    )
    .bind(FINISH_STATEMENT)
    .fetch_one(pool)
    .await
    .unwrap()
}

/// A finalization retried behind a row lock keeps at most the one connection its first try is parked on. Every try
/// used to be abandoned on the client alone, at its deadline, and an abandoned statement stays queued on the server
/// behind the lock while the ping sqlx runs before pooling the connection again waits it out — so each retry parked one
/// more pooled connection for as long as the lock was held. An operator's `SELECT ... FOR UPDATE` on a running job,
/// left open, drained the pool and took the heartbeat, the abort poll and every other slot's finalization with it.
#[sqlx::test(migrations = "./migrations")]
async fn test_a_finalization_retried_behind_a_row_lock_parks_one_connection(pool: PgPool) {
    let db = TestDb::new(pool_with_max(&pool, 8).await).await;
    let state = AbortSuccessRace {
        started: Arc::new(tokio::sync::Notify::new()),
        release: Arc::new(tokio::sync::Notify::new()),
    };
    let handle = db.queue.enqueue(succeeds_during_abort::job(())).await.unwrap().unwrap();
    // A try every one and a half seconds: half a second in flight, then the one-second retry interval.
    let worker = pgqueue::__test_support::worker_finalize_deadline(
        test_worker(db.queue.clone()).register_job(succeeds_during_abort).state(state.clone()).concurrency(1),
        Duration::from_millis(500),
    )
    .build()
    .unwrap();
    let token = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(token.clone()));

    state.started.notified().await;
    let mut operator = pool.begin().await.unwrap();
    sqlx::query("SELECT id FROM pgqueue.jobs WHERE id = $1 FOR UPDATE")
        .bind(handle.id())
        .fetch_one(&mut *operator)
        .await
        .unwrap();
    state.release.notify_one();

    // Six tries' worth, which parked six connections before.
    let mut most = 0;
    let until = tokio::time::Instant::now() + Duration::from_secs(9);
    while tokio::time::Instant::now() < until {
        most = most.max(blocked_finishes(&pool).await);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(most >= 1, "the finalization never waited on the locked row, so nothing here was exercised");
    assert!(
        most <= 2,
        "a finalization retried behind one row lock held {most} connections; the parked first try and the one retry in \
         flight are all it may hold"
    );

    operator.rollback().await.unwrap();
    assert_eq!(wait_terminal(&db.queue, handle.id(), 10).await.status, JobStatus::Complete);
    token.cancel();
    tokio::time::timeout(Duration::from_secs(30), run).await.expect("worker did not stop").unwrap().unwrap();
}

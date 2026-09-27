//! Background and cron job processing backed by PostgreSQL 18+.
//!
//! `pgqueue` is an opinionated job queue for tokio applications: jobs are plain
//! `async fn`s annotated with [`macro@job`], enqueued with full type safety, and
//! processed by [`Worker`]s that coordinate through a single Postgres database
//! using `FOR UPDATE SKIP LOCKED` and `LISTEN`/`NOTIFY`.
//!
//! The built-in web dashboard is behind the optional `dashboard` feature.
//!
//! ```no_run
//! #[derive(serde::Serialize, serde::Deserialize)]
//! struct SendEmail { to: String, body: String }
//!
//! #[pgqueue::job]
//! async fn send_email(args: SendEmail) -> anyhow::Result<()> {
//!     println!("emailing {}", args.to);
//!     Ok(())
//! }
//!
//! # async fn run() -> anyhow::Result<()> {
//! let queue = pgqueue::Queue::connect(&std::env::var("DATABASE_URL")?).await?;
//! queue.enqueue(send_email::job(SendEmail { to: "a@b.c".into(), body: "hi".into() })).await?;
//! pgqueue::Worker::builder(queue).register_job(send_email).run().await?;
//! # Ok(())
//! # }
//! ```

// Macro expansions use this stable path when invoked from this package, while
// downstream crates use the dependency name resolved from their Cargo.toml.
extern crate self as pgqueue;

use uuid::Uuid;

/// Infrastructure failure returned by queue and worker operations.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// A database operation failed.
    #[error(transparent)]
    Db(#[from] sqlx::Error),

    /// JSON (de)serialization of a payload, result, or metadata failed.
    #[error(transparent)]
    Serde(#[from] serde_json::Error),

    /// Applying or validating the embedded SQLx migrations failed.
    #[error(transparent)]
    Migration(#[from] sqlx::migrate::MigrateError),

    /// Invalid queue, job, or worker configuration.
    #[error("configuration error: {0}")]
    Config(String),

    /// A dedupe-key decision raced with a writer bypassing the enqueue
    /// advisory lock (SQL writing `pgqueue.jobs` directly): the key's live
    /// holder appeared and vanished mid-operation, leaving no job to report
    /// as the collision winner. Transient, unlike [`Error::Config`] — retry
    /// the operation.
    #[error("dedupe race: {0}")]
    DedupeRace(String),

    /// An internal asynchronous task panicked or was cancelled.
    #[error(transparent)]
    Task(#[from] tokio::task::JoinError),

    /// A worker infrastructure task stopped unexpectedly, could not stop
    /// within its hard shutdown bound, or abandoned a database operation that
    /// exceeded its deadline (a connection that will never answer — a
    /// black-holed socket after a failover — rather than one that is merely
    /// slow; see the sweeper and worker loop docs).
    #[error("worker task failed: {0}")]
    WorkerTask(&'static str),

    /// A dashboard could not bind, its server task panicked, or its
    /// authentication state was unavailable.
    #[cfg(feature = "dashboard")]
    #[error(transparent)]
    Dashboard(std::io::Error),

    /// The job does not exist (deleted, expired, or never enqueued).
    #[error("job not found: {0}")]
    JobNotFound(Uuid),

    /// The job completed, but retention deleted its result before it could
    /// be read.
    #[error("job {0} completed but its result was already deleted")]
    ResultExpired(Uuid),

    /// A job waited on via `enqueue_and_wait` or `wait` finished unsuccessfully.
    #[error(transparent)]
    Job(#[from] JobError),

    /// Waiting for a job result exceeded the caller's deadline.
    #[error("timed out waiting for job result")]
    WaitTimeout,
}

#[cfg(feature = "dashboard")]
mod dashboard;
mod database;
mod job;
mod queue;
mod sweeper;
mod worker;

#[cfg(feature = "dashboard")]
pub use dashboard::{Dashboard, DashboardServer, DashboardServerHandle};

/// The untyped job template behind [`Queue::enqueue_raw`]. Test-only, as that
/// method is: the supported enqueue path is the typed `#[pgqueue::job]` API.
#[cfg(feature = "_test")]
pub use job::JobRequest;
pub use job::{
    CronDefinition, CronMisfirePolicy, CronOptions, EnqueueResult, FromJobContext, JobBuilder, JobConfig, JobContext,
    JobCursor, JobDefinition, JobError, JobErrorKind, JobFilter, JobHandle, JobRetention, JobRetryBackoff, JobRow,
    JobState, JobStatus, JobType, MAX_ENQUEUE_BATCH_BYTES, MAX_ENQUEUE_BATCH_JOBS,
};
pub use queue::{Attempt, Consumer, Queue, QueueBuilder, QueueCounts, QueueStats};
pub use sweeper::{SweepOperations, Sweeper, SweeperReport};
pub use worker::{
    Worker, WorkerBuilder, WorkerComponent, WorkerCursor, WorkerFilter, WorkerHealth, WorkerHealthFailure,
    WorkerHealthSnapshot, WorkerHealthStatus, WorkerInfo, WorkerTimers,
};

/// Marks an `async fn` as a cron job handler run on a schedule. The first
/// attribute argument is a UTC cron expression. Its syntax and whether it can
/// ever produce an occurrence are checked at compile time; a never-firing
/// expression is a compile error.
///
/// Cron functions take no payload — every parameter is an extractor.
///
/// Occurrences of one schedule never overlap and never queue behind each
/// other: every occurrence publishes under the schedule's one dedupe key, so
/// while an earlier occurrence is still live — queued, running, finishing an
/// abort, or waiting out a retry delay — an occurrence that comes due is
/// *skipped*, with a warning, and the schedule resumes at the next occurrence
/// after the holder finishes. A minutely handler that runs for ninety seconds
/// therefore fires on alternate minutes rather than piling up. This holds
/// under every [`CronMisfirePolicy`], which governs occurrences missed while
/// no scheduler was *able* to publish, not ones withheld because the previous
/// run is still alive.
///
/// The guarantee covers *scheduled* occurrences. Manually retrying a terminal occurrence
/// ([`Queue::retry_job`]) deliberately produces a keyless one-off outside the schedule, which can
/// run beside a live scheduled occurrence; see that method's documentation.
///
/// Like [`macro@job`], the annotated function must be a free function at module or block scope,
/// `#[expect(...)]` on it behaves as `#[allow(...)]`, and a `#[deprecated]` one needs
/// `deny(deprecated)` rather than an enclosing `forbid(deprecated)`.
///
/// Every attribute [`macro@job`] accepts is accepted here too, plus:
///
/// - `revision = N` (default `0`) — coordinates this definition across workers. Raise it when the
///   schedule or the options change; the highest revision wins, and workers on older ones stop
///   scheduling this cron. Reusing a revision for a different definition is rejected.
///
/// A compile-time cron always uses the default missed-occurrence policy
/// ([`CronMisfirePolicy::Skip`] with the adaptive grace). To run with
/// [`CronMisfirePolicy::FireOnce`] or an explicit grace, register the handler
/// as a plain [`macro@job`] and schedule it with
/// [`WorkerBuilder::schedule_cron_with_options`](crate::WorkerBuilder::schedule_cron_with_options),
/// which is the one path that takes a full [`CronOptions`].
///
/// `name` is capped at 250 bytes rather than 255, because a cron's dedupe key
/// is the derived `cron:{name}`.
///
/// ```no_run
/// #[pgqueue::cron("*/5 * * * *")]
/// async fn cleanup(ctx: pgqueue::JobContext) -> anyhow::Result<u64> {
///     Ok(ctx.queue().counts().await?.queued as u64)
/// }
///
/// #[pgqueue::cron(
///     "0 * * * *",
///     revision = 1,
///     name = "collect_hourly_metrics",
///     max_attempts = 2,
///     timeout_ms = 120_000,
///     result_ttl_ms = 604_800_000,
///     retry_delay_ms = 1_000,
///     max_backoff_ms = 60_000,
///     priority = 10,
/// )]
/// async fn collect_metrics() -> anyhow::Result<()> {
///     Ok(())
/// }
/// # async fn run(queue: pgqueue::Queue) -> anyhow::Result<()> {
/// // Register the handler and its embedded schedule:
/// pgqueue::Worker::builder(queue).register_cron(cleanup).run().await?;
/// # Ok(())
/// # }
/// ```
pub use pgqueue_macros::cron;
/// Marks an `async fn` as a job handler.
///
/// The first parameter is the job's payload — use `_: ()` for a job that takes
/// none — and every parameter after it is an extractor ([`JobState`],
/// [`JobContext`]). The expansion adds `::job(args)` for building an enqueue
/// request, `::call(..)` for invoking the handler directly, and a [`JobType`]
/// implementation carrying the configuration below.
///
/// The annotated function must be a free function at module or block scope: the expansion writes a
/// struct and three `impl` blocks beside it, none of which is legal inside another `impl` or a
/// trait. An attribute macro cannot see where it was invoked, so that shows up as several errors
/// pointing into generated code rather than as one message.
///
/// `#[expect(...)]` on a job behaves as `#[allow(...)]` and never reports as unfulfilled. One
/// written item becomes several, the lint fires on whichever of them it applies to, and every
/// other copy would otherwise report as unfulfilled through no fault of the caller's.
///
/// `#[deprecated]` on a job deprecates the job's struct, so enqueueing or registering it warns.
/// The expansion's `impl` blocks have to name that struct, so they carry `#[allow(deprecated)]` —
/// and an *enclosing* `forbid(deprecated)` (a crate or module attribute, a `[lints]` table,
/// `-F deprecated`) refuses any inner `allow`, as `error[E0453]` pointing at the attribute. In a
/// crate that deprecates its own jobs, use `deny(deprecated)` instead: it still fails the build on
/// every use. A `forbid` written on the job function itself is fine; it is lowered to `deny`.
///
/// - `name = "..."` (default: the function's name, with any `r#` stripped) — name stored with the
///   job, at most 255 bytes. Keep it stable until every job published under the old name has
///   finished. **The name is a queue-wide key, not a module path**: rows are dispatched by it, so
///   two handlers that share one — `a::cleanup` and `b::cleanup`, both defaulting to `"cleanup"` —
///   are the same job as far as the queue is concerned, and a payload published by one is decoded
///   by whichever the worker registered. [`WorkerBuilder::register_job`](crate::WorkerBuilder)
///   rejects the collision, but only when both types are registered on the *same* worker; split
///   across two binaries, or with one name owned by a dependency, nothing catches it. Give at least
///   one of them an explicit `name`.
/// - `max_attempts = N` (default `1`) — total attempts, including the first run.
/// - `timeout_ms = N` (default `10_000`) — maximum duration of one attempt. `0` disables the
///   timeout; an attempt is still recovered once its worker's lease has been expired for the
///   queue's sweep grace. The deadline cancels the handler's task, which takes effect at its next
///   `.await`: a handler that blocks its runtime thread cannot be force-stopped in-process, so
///   past a short grace the attempt is finalized as timed out without it, and its thread stays
///   occupied until it returns — run blocking work on `tokio::task::spawn_blocking`. What the
///   attempt guards fence off is the runaway handler's *finalizations of this attempt* — never
///   its external side effects, nor jobs it enqueues through its context, which is one of the
///   overlaps at-least-once delivery already requires handlers to tolerate: keep them
///   idempotent. Finalizing without the handler needs a
///   runtime thread to run on: on Tokio's current-thread runtime — or a multi-thread one whose
///   every worker thread is blocked — the deadline, like everything else in the worker, waits
///   until the handler yields, which is why workers belong on the multi-thread runtime. A handler
///   already holding a finished result when the deadline is checked keeps that result rather than
///   a synthetic timeout.
/// - `result_ttl_ms = N` (default `600_000`) — how long a completed job's row, with its result, is
///   retained. `0` deletes it as it finishes.
/// - `failed_ttl_ms = N` (default `604_800_000`, seven days) — how long a failed or aborted job's
///   row, with its error, is retained. A separate clock from `result_ttl_ms` because failures are
///   looked at later than results: with one shared clock, the minutes that suit a result purged
///   every failure before an operator saw it. `0` deletes the row as it finishes.
/// - `retry_delay_ms = N` (default `0`) — base delay before a retry.
/// - `max_backoff_ms = N` (default: disabled) — exponential backoff capped at `N`. Requires a
///   non-zero `retry_delay_ms`.
/// - `priority = N` (default `0`) — dequeue priority as an `i16`, lower values first.
///
/// Durations are milliseconds and must not exceed the maximum a queue supports;
/// an out-of-range one fails the build rather than the enqueue.
///
/// ```no_run
/// #[derive(serde::Serialize, serde::Deserialize)]
/// struct Email { address: String }
///
/// #[pgqueue::job(
///     name = "deliver_email",
///     max_attempts = 5,
///     timeout_ms = 30_000,
///     result_ttl_ms = 3_600_000,
///     retry_delay_ms = 500,
///     max_backoff_ms = 60_000,
///     priority = -10,
/// )]
/// async fn send_email(args: Email) -> anyhow::Result<String> {
///     Ok(args.address)
/// }
/// # async fn run(queue: pgqueue::Queue) -> anyhow::Result<()> {
/// queue.enqueue(send_email::job(Email { address: "user@example.com".into() })).await?;
/// # Ok(())
/// # }
/// ```
pub use pgqueue_macros::job;

/// Support machinery for macro-generated code. Not part of the public API;
/// anything here may change without notice.
#[doc(hidden)]
pub mod __private {
    use serde::Serialize;
    use serde::de::DeserializeOwned;
    pub use serde_json::Value;
    /// Boxes each generated handler future. Named through this crate rather than as `::std::boxed::Box`, which a
    /// `#![no_std]` crate defining its jobs cannot resolve.
    pub use std::boxed::Box;

    pub use crate::job::MAX_DURATION_MS;
    pub use crate::job::{IntoJobResult, JobHandlerFuture, TypeErasedJobHandler};
    use crate::{JobError, JobErrorKind};

    /// Deserializes a stored payload into the handler's argument type.
    ///
    /// Borrowed, not owned: the payload lives in the [`crate::JobContext`]'s
    /// row snapshot, and decoding from a reference is what lets the execution
    /// path run without ever cloning it.
    pub fn decode_payload<T: DeserializeOwned>(payload: &Value) -> Result<T, JobError> {
        T::deserialize(payload).map_err(|e| JobError::new(JobErrorKind::Decode, format!("payload decode: {e}")))
    }

    /// Normalizes and serializes a handler's return value.
    ///
    /// Through the crate's checked encoding, not bare `serde_json::to_value`:
    /// JSON has no NaN or infinity, and `serde_json` maps them to `null` — a
    /// handler returning one was recorded as a clean `complete` whose typed
    /// waiters then failed to decode, or, for an optional float, silently
    /// observed `None`. The refusal surfaces here as the deterministic encode
    /// failure it is.
    pub fn encode_result<R>(result: R) -> Result<Value, JobError>
    where
        R: IntoJobResult,
        R::Output: Serialize,
    {
        let output = result.into_job_result()?;
        crate::job::encode_json(&output).map_err(|e| JobError::new(JobErrorKind::Decode, format!("result encode: {e}")))
    }
}

/// Raw-protocol access for this crate's own integration tests, which compile as
/// a separate crate and so can only reach the crate through its public API.
///
/// Gated behind the non-default, internal `_test` feature so ordinary builds —
/// including every release build of a downstream crate — never expose it. Not
/// semver-stable, and never for application code: it takes a caller-supplied
/// [`JobRow`] and bypasses the [`Consumer`] and [`Attempt`] guards.
/// [`__test_support::dequeue`] bypasses the worker lease as well, so a job
/// claimed through it has none, and the sweeper will treat it as abandoned and
/// hand it to a second worker while it is still running.
/// [`__test_support::dequeue_worker`] and
/// [`__test_support::dequeue_worker_probe`] run the worker's own dequeue
/// protocol and do require a live, accepting lease; what they skip is the
/// [`Consumer`] and [`Attempt`] guards around it.
#[cfg(feature = "_test")]
#[doc(hidden)]
pub mod __test_support {
    use std::time::Duration;

    use serde_json::Value;
    use uuid::Uuid;

    use crate::{Dashboard, Error, JobRow, JobStatus, Queue, Sweeper, Worker, WorkerBuilder};

    /// The serialized-size ceiling every `jsonb` write enforces, so a test can
    /// pin the boundary the guard below enforces rather than restating the
    /// number.
    pub fn max_json_document_bytes() -> usize {
        crate::job::MAX_JSON_DOCUMENT_BYTES
    }

    /// The check that enforces it, exposed so a test can exercise the boundary
    /// against a custom budget.
    pub fn json_exceeds_bytes(value: &Value, max: usize) -> bool {
        crate::job::json_exceeds_bytes(value, max)
    }

    /// Returns the completion channel used by a queue.
    pub fn done_channel(queue: &str) -> String {
        crate::database::done_channel(queue)
    }

    /// Returns the wakeup channel a queue's enqueues notify on.
    pub fn notify_channel(queue: &str) -> String {
        crate::database::channel_name(queue, "")
    }

    /// Returns the advisory-lock namespace used by dedupe enqueues.
    pub fn dedupe_enqueue_lock_key(database: &str) -> i32 {
        crate::database::dedupe_enqueue_lock_key(database)
    }

    /// Returns the advisory namespace that orders unacknowledged-claim
    /// resolution behind the claim transaction it resolves, so a test can hold
    /// the claim side of that ordering open.
    pub fn claim_resolution_lock_key(database: &str) -> i32 {
        crate::database::claim_resolution_lock_key(database)
    }

    /// Returns the advisory lock used for one queue's sweep leadership.
    pub fn sweep_lock_key(database: &str, queue: &str) -> i64 {
        crate::database::sweep_lock_key(database, queue)
    }

    /// How many dedupe-key locks one transaction may take through `queue`, as derived from its server's settings when
    /// it connected, unless a test lowered it.
    pub fn dedupe_lock_budget(queue: &Queue) -> i64 {
        queue.database().dedupe_lock_budget()
    }

    /// Lowers `queue`'s dedupe-lock budget, so a test can cross it without holding the thousands of locks the shipped
    /// one allows on a server it shares with the rest of the suite.
    pub fn set_dedupe_lock_budget(queue: &Queue, budget: i64) {
        queue.database().set_dedupe_lock_budget(budget);
    }

    /// The dequeue claim, so a plan-shape test can pin the shipped SQL rather
    /// than a copy of it.
    pub fn dequeue_claim_sql() -> &'static str {
        crate::database::DEQUEUE_CLAIM_SQL
    }

    /// [`Queue::purge_queued_jobs`]'s statement, for the same reason.
    pub fn purge_queued_jobs_sql() -> &'static str {
        crate::database::PURGE_QUEUED_JOBS_SQL
    }

    /// The guarded finish that writes every attempt's outcome, so a plan-shape
    /// test can pin that it reaches the row by primary key.
    pub fn finish_guarded_sql() -> &'static str {
        crate::database::FINISH_GUARDED_SQL
    }

    /// The guarded requeue behind every retry and give-back, for the same
    /// reason.
    pub fn requeue_guarded_sql() -> &'static str {
        crate::database::REQUEUE_GUARDED_SQL
    }

    /// The unsettled-claim resolvers' abort fallback, for the same reason.
    pub fn abort_unsettled_claim_sql() -> &'static str {
        crate::database::ABORT_UNSETTLED_CLAIM_SQL
    }

    /// The worker's abort poll, which reads the rows behind every in-flight
    /// attempt back by id once a second, for the same reason.
    pub fn abort_poll_sql() -> &'static str {
        crate::database::ABORT_POLL_SQL
    }

    /// [`Queue::jobs_page`]'s statement, so a plan-shape test can pin that its
    /// cursor bounds the index scan rather than filtering every newer row.
    pub fn jobs_page_sql() -> &'static str {
        crate::database::JOBS_PAGE_SQL
    }

    /// The statement the `/health` liveness probe runs, so a plan-shape test
    /// can pin the shipped SQL rather than a copy of it.
    pub fn health_probe_sql() -> &'static str {
        crate::dashboard::HEALTH_PROBE_SQL
    }

    /// The statement every open dashboard polls per queue, for the same reason.
    pub fn dashboard_signals_sql() -> &'static str {
        crate::dashboard::DASHBOARD_SIGNALS_SQL
    }

    /// The statement the job-name typeahead runs per keystroke, so a plan-shape
    /// test can pin which index the shipped SQL reaches.
    pub fn job_name_typeahead_sql() -> &'static str {
        crate::dashboard::JOB_NAME_TYPEAHEAD_SQL
    }

    /// The job listing's page as the dashboard's default view runs it, so a
    /// plan-shape test can pin the row lookup to primary-key descents rather
    /// than a scan of every retained row.
    pub fn job_page_sql() -> &'static str {
        crate::dashboard::JOB_PAGE_SQL
    }

    /// The job listing's page as it is run with a `?name=` filter, for the same
    /// reason: the name has to reach the index as an equality, not as a filter
    /// the planner applies to every row it already had to read.
    pub fn job_page_by_name_sql() -> &'static str {
        crate::dashboard::JOB_PAGE_BY_NAME_SQL
    }

    /// Overrides how long a `/health` probe result is reused, so a test can
    /// assert *that* the cache answered without racing the shipped 500ms
    /// window while the rest of the suite runs beside it.
    pub fn dashboard_health_probe_ttl(dashboard: Dashboard, ttl: Duration) -> Dashboard {
        dashboard.with_health_probe_ttl(ttl)
    }

    /// The same, for the `/api/queues` fan-out: its shipped 1s window is well
    /// under the 5s poll an open dashboard runs, and equally well under what a
    /// test can hold to while the rest of the suite runs beside it.
    pub fn dashboard_queue_signals_ttl(dashboard: Dashboard, ttl: Duration) -> Dashboard {
        dashboard.with_queue_signals_ttl(ttl)
    }

    /// Overrides how long Retry and Abort wait for a lock on their job in a router without request limits, so a test
    /// can see one refused without waiting out the shipped 30 seconds.
    pub fn dashboard_action_lock_wait(dashboard: Dashboard, wait: Duration) -> Dashboard {
        dashboard.with_action_lock_wait(wait)
    }

    /// How long a request waits for the cached round it is riding on before
    /// answering 503. A test that parks a round and then observes it is racing
    /// this, so it can bound its own window with it rather than with a fixed
    /// sleep that has no relationship to it.
    pub fn dashboard_round_wait_timeout() -> Duration {
        crate::dashboard::ROUND_WAIT_TIMEOUT
    }

    /// Overrides how long one sweep pass may stay in flight before it is
    /// abandoned and leadership released, so a test can wedge a pass without
    /// waiting out the shipped one-minute bound.
    pub fn sweeper_pass_deadline(sweeper: Sweeper, deadline: Duration) -> Sweeper {
        sweeper.with_pass_deadline(deadline)
    }

    /// The server-side TCP settings of the session `sweeper` holds leadership on, as the server reports them, in the
    /// order `tcp_keepalives_idle`, `tcp_keepalives_interval`, `tcp_keepalives_count`, `tcp_user_timeout`; `None`
    /// while it holds no leadership.
    pub async fn sweeper_leadership_tcp_settings(sweeper: &mut Sweeper) -> Result<Option<[String; 4]>, Error> {
        sweeper.leadership_tcp_settings().await
    }

    /// Overrides how long one try at recording an attempt's outcome may stay in flight before it is abandoned and
    /// retried, so a test can drive a finalization through several tries without waiting out the shipped one-minute
    /// bound.
    pub fn worker_finalize_deadline(builder: WorkerBuilder, deadline: Duration) -> WorkerBuilder {
        builder.with_finalize_deadline(deadline)
    }

    /// One heartbeat of a worker's own heartbeat loop, on a connection opened for it, held to `deadline` as that loop
    /// holds it to one heartbeat interval, so a test can stall it without waiting out the shipped floor.
    pub async fn write_worker_lease_within(
        queue: &Queue,
        worker_id: Uuid,
        ttl: Duration,
        deadline: Duration,
    ) -> Result<(), Error> {
        WorkerLease::new(queue, worker_id).renew(ttl, deadline).await
    }

    /// One worker's lease, renewed as its heartbeat loop renews it — on a connection of its own outside the queue's
    /// pool, kept from one heartbeat to the next and replaced once one does not land — for as many heartbeats as a
    /// test drives.
    pub struct WorkerLease {
        queue: Queue,
        worker_id: Uuid,
        connection: crate::database::LeaseConnection,
    }

    impl WorkerLease {
        /// A lease that has not opened its connection yet; the first heartbeat does.
        pub fn new(queue: &Queue, worker_id: Uuid) -> Self {
            Self { queue: queue.clone(), worker_id, connection: crate::database::LeaseConnection::default() }
        }

        /// One heartbeat, renewing the lease for `ttl`, held to `deadline`.
        pub async fn renew(&mut self, ttl: Duration, deadline: Duration) -> Result<(), Error> {
            let stats = Value::Object(serde_json::Map::new());
            let intake = crate::database::LeaseIntake::Open;
            let database = self.queue.database();
            database.renew_worker_lease(&mut self.connection, self.worker_id, stats, None, ttl, intake, deadline).await
        }
    }

    /// Runs only a worker's sweep loop with a test-owned drain budget, so tests
    /// can exercise the pass limit without racing the shipped wall-clock limit.
    pub async fn run_worker_sweeper(
        worker: Worker,
        shutdown: tokio_util::sync::CancellationToken,
        max_drain_time: Duration,
    ) {
        crate::worker::run_sweep_loop_for_test(worker, shutdown, max_drain_time).await;
    }

    /// Raw dequeue that does not require a worker lease.
    pub async fn dequeue(queue: &Queue, limit: i64, worker_id: Uuid) -> Result<Vec<JobRow>, Error> {
        queue.database().dequeue_unleased(limit, worker_id).await
    }

    /// Raw access to the worker dequeue protocol.
    pub async fn dequeue_worker(queue: &Queue, limit: i64, worker_id: Uuid) -> Result<Vec<JobRow>, Error> {
        Ok(queue.database().dequeue_worker(limit, worker_id).await?.jobs)
    }

    /// The diagnostic half of the worker dequeue protocol: whether ready work
    /// is waiting behind an underfilled batch.
    pub async fn dequeue_worker_probe(queue: &Queue, limit: i64, worker_id: Uuid) -> Result<bool, Error> {
        let batch = queue.database().dequeue_worker(limit, worker_id).await?;
        Ok(batch.work_available)
    }

    /// Raw access to the unhandled-claim requeue: gives an attempt back with a
    /// refund and the unhandled delay, as a worker missing the job's handler
    /// does.
    pub async fn requeue_unhandled(queue: &Queue, job: &JobRow) -> Result<bool, Error> {
        queue.database().requeue_unhandled(job, None).await
    }

    /// Raw access to guarded finalization.
    pub async fn finish(
        queue: &Queue,
        job: &JobRow,
        status: JobStatus,
        result: Option<Value>,
        error: Option<&str>,
    ) -> Result<bool, Error> {
        queue.database().finish(job, status, result, error, None).await
    }

    /// Raw access to guarded retry.
    pub async fn retry(queue: &Queue, job: &JobRow, error: &str) -> Result<bool, Error> {
        queue.database().retry(job, error, None).await
    }

    /// Raw access to the requeue of an attempt the sweeper marked for abort, on
    /// behalf of the worker that still owns it.
    pub async fn retry_swept(queue: &Queue, job: &JobRow, error: Option<&str>) -> Result<bool, Error> {
        queue.database().retry_swept(job, error, None).await
    }

    /// Raw access to the requeue a worker performs for an attempt it gave up on
    /// at shutdown.
    pub async fn requeue_shutdown(queue: &Queue, job: &JobRow, error: &str) -> Result<bool, Error> {
        queue.database().requeue_shutdown(job, error, None).await
    }

    /// One claim as the dequeue commit guard records it: the row's id, the attempt number the claim wrote, and the
    /// `started_at` it stamped (`None` matches any).
    pub type UnacknowledgedClaim = (Uuid, i32, Option<jiff::Timestamp>);

    fn unacknowledged_claims(claims: &[UnacknowledgedClaim]) -> Vec<crate::database::DatabaseUnacknowledgedClaim> {
        claims
            .iter()
            .map(|(id, attempts, started_at)| crate::database::DatabaseUnacknowledgedClaim {
                id: *id,
                attempts: *attempts,
                started_at: *started_at,
            })
            .collect()
    }

    /// Runs one unacknowledged-claim resolver pass synchronously, so a test can
    /// drive the recovery a dequeue whose COMMIT acknowledgement was lost
    /// spawns in the background. Returns how many claims matched a committed
    /// row and were requeued; a committed claim refused at the attempt ceiling
    /// is finished `aborted` instead and counts nothing.
    pub async fn requeue_unacknowledged_claims(
        queue: &Queue,
        worker_id: Uuid,
        claims: &[UnacknowledgedClaim],
    ) -> Result<u64, Error> {
        let database = queue.database();
        let mut claims = unacknowledged_claims(claims);
        Ok(database.requeue_unacknowledged_claims(worker_id, &mut claims).await?)
    }

    /// Arms the dequeue commit guard for the given claims and drops it without
    /// disarming — exactly what a dequeue future cancelled mid-commit does — so
    /// a test can observe the cancellation path hand its claims to the
    /// background resolver.
    pub fn drop_armed_dequeue_claim_guard(queue: &Queue, worker_id: Uuid, claims: &[UnacknowledgedClaim]) {
        crate::database::drop_armed_claim_guard(queue.database(), worker_id, unacknowledged_claims(claims));
    }

    /// Raw access to worker lease rows, reopening intake the way a
    /// [`Consumer`](crate::Consumer) heartbeat does.
    pub async fn write_worker_info(
        queue: &Queue,
        worker_id: Uuid,
        stats: Value,
        metadata: Option<Value>,
        ttl: Duration,
    ) -> Result<(), Error> {
        queue.database().write_worker_info(worker_id, stats, metadata, ttl, crate::database::LeaseIntake::Reopen).await
    }

    /// Raw access to worker lease rows the way a worker writes its own: never
    /// reopening a closed lease, and creating a missing one in whichever intake
    /// state the worker is currently in.
    pub async fn write_worker_lease(
        queue: &Queue,
        worker_id: Uuid,
        stats: Value,
        metadata: Option<Value>,
        ttl: Duration,
        accepting: bool,
    ) -> Result<(), Error> {
        let intake = if accepting { crate::database::LeaseIntake::Open } else { crate::database::LeaseIntake::Closed };
        queue.database().write_worker_info(worker_id, stats, metadata, ttl, intake).await
    }

    /// Subscribes to the wakeups the notification listener hands idle fetchers. Starts the listener if the first
    /// caller.
    pub fn listener_wakeups(queue: &Queue) -> tokio::sync::broadcast::Receiver<()> {
        queue.database().notify_listener().subscribe_wakeup()
    }

    /// Shortens how long the notification listener lets a session stay silent before probing it, and how long the
    /// probe may take, so a test can black-hole the session without waiting out the shipped minute. Only before the
    /// listener starts: answers `false` once it has.
    pub fn set_listener_probe(queue: &Queue, idle: Duration, timeout: Duration) -> bool {
        queue.database().set_listener_probe(crate::queue::ListenerProbe { idle, timeout })
    }

    /// The notification listener's health watch: `None` while subscribed, the
    /// latest error while disconnected. Every failed reconnect re-sends, so a
    /// test can observe the retry cadence. Starts the listener if the first
    /// caller.
    pub fn listener_health(queue: &Queue) -> tokio::sync::watch::Receiver<Option<String>> {
        queue.database().notify_listener().subscribe_health()
    }
}

#[cfg(test)]
mod tests {
    use crate::{Error, JobError, JobErrorKind};

    fn assert_error_renders_source_once(error: Error, expected: &str) {
        let error = anyhow::Error::new(error);
        assert_eq!(error.to_string(), expected);
        assert_eq!(format!("{error:#}"), expected);
    }

    #[tokio::test]
    async fn test_sourced_error_variants_render_their_cause_once() {
        let task = tokio::spawn(std::future::pending::<()>());
        task.abort();
        let task_error = task.await.unwrap_err();
        let task_message = task_error.to_string();
        assert_error_renders_source_once(Error::Task(task_error), &task_message);

        #[cfg(feature = "dashboard")]
        assert_error_renders_source_once(Error::Dashboard(std::io::Error::other("socket closed")), "socket closed");

        assert_error_renders_source_once(
            Error::Job(JobError::new(JobErrorKind::Failed, "handler failed")),
            "failed: handler failed",
        );
    }

    #[test]
    fn test_private_helpers_round_trip_and_surface_errors() {
        let value = crate::__private::encode_result(Ok::<u32, String>(7)).unwrap();
        assert_eq!(value, serde_json::json!(7));

        let decoded: u32 = crate::__private::decode_payload(&serde_json::json!(7)).unwrap();
        assert_eq!(decoded, 7);

        // JSON object keys must be strings, so a tuple-keyed map cannot be
        // encoded: the encode error path.
        type BadKeys = std::collections::HashMap<(u32, u32), u32>;
        let bad: BadKeys = [((1, 2), 3)].into_iter().collect();
        let err = crate::__private::encode_result(Ok::<BadKeys, String>(bad)).unwrap_err();
        assert_eq!(err.kind, JobErrorKind::Decode);
        assert!(err.message.contains("result encode"), "{}", err.message);

        // And the decode error path.
        let err = crate::__private::decode_payload::<u32>(&serde_json::json!("nope")).unwrap_err();
        assert_eq!(err.kind, JobErrorKind::Decode);

        // JSON has no NaN: the result gate refuses it as the encode failure
        // it is, instead of recording a `null` "success" typed waiters then
        // cannot decode.
        let err = crate::__private::encode_result(Ok::<f64, String>(f64::NAN)).unwrap_err();
        assert_eq!(err.kind, JobErrorKind::Decode);
        assert!(err.message.contains("result encode"), "{}", err.message);
    }
}

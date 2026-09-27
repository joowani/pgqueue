//! Real-Postgres integration tests.
//!
//! SQLx creates a migrated database for every test, so suites can run in
//! parallel without schema-name plumbing or asynchronous cleanup.

// Marks the shared helpers as test code, so `clippy.toml` lets them use `unwrap()` and `expect()` like `#[test]`s.
#![cfg(test)]

use std::future::Future;
use std::time::Duration;

use pgqueue::{
    EnqueueResult, Error, JobConfig, JobRequest, JobRow, JobStatus, Queue, QueueBuilder, WorkerFilter, WorkerInfo,
    WorkerTimers,
};
use serde_json::Value;
use sqlx::{Connection, PgConnection, PgPool};
use uuid::Uuid;

#[path = "cron_test.rs"]
mod cron_test;
#[path = "dashboard_test.rs"]
mod dashboard_test;
#[path = "job_test.rs"]
mod job_test;
#[path = "queue_test.rs"]
mod queue_test;
#[path = "robustness_test.rs"]
mod robustness_test;
#[path = "stress_test.rs"]
mod stress_test;
#[path = "worker_test.rs"]
mod worker_test;

/// The shortest non-zero sweep grace, used wherever a test wants a sweep to
/// reclaim without waiting. `Queue::connect` refuses a zero grace.
pub const MIN_SWEEP_GRACE: Duration = Duration::from_millis(1);

pub struct TestDb {
    pub queue: Queue,
    pub pool: PgPool,
    pub database: String,
}

pub async fn list_workers(queue: &Queue) -> Vec<WorkerInfo> {
    queue.workers_page(WorkerFilter::default()).await.expect("list workers")
}

/// Keeps existing setup terse while preserving the former Option assertions:
/// "some" means inserted and "none" means deduplicated.
pub trait EnqueueResultTestExt<H> {
    fn unwrap(self) -> H;
    fn expect(self, message: &str) -> H;
    fn is_some(&self) -> bool;
    fn is_none(&self) -> bool;
}

impl<H> EnqueueResultTestExt<H> for EnqueueResult<H> {
    fn unwrap(self) -> H {
        match self {
            EnqueueResult::Enqueued(handle) => handle,
            EnqueueResult::Deduplicated(_) => panic!("expected a newly enqueued job"),
        }
    }

    fn expect(self, message: &str) -> H {
        match self {
            EnqueueResult::Enqueued(handle) => handle,
            EnqueueResult::Deduplicated(_) => panic!("{message}"),
        }
    }

    fn is_some(&self) -> bool {
        self.is_enqueued()
    }

    fn is_none(&self) -> bool {
        self.is_deduplicated()
    }
}

#[allow(async_fn_in_trait)]
pub trait QueueProtocolTestExt {
    async fn dequeue(&self, limit: i64, worker_id: Uuid) -> Result<Vec<JobRow>, Error>;
    async fn finish(
        &self,
        job: &JobRow,
        status: JobStatus,
        result: Option<Value>,
        error: Option<&str>,
    ) -> Result<bool, Error>;
    async fn retry(&self, job: &JobRow, error: &str) -> Result<bool, Error>;
    async fn retry_swept(&self, job: &JobRow, error: Option<&str>) -> Result<bool, Error>;
    async fn requeue_shutdown(&self, job: &JobRow, error: &str) -> Result<bool, Error>;
    async fn requeue_unhandled(&self, job: &JobRow) -> Result<bool, Error>;
    async fn write_worker_info(
        &self,
        worker_id: Uuid,
        stats: Value,
        metadata: Option<Value>,
        ttl: Duration,
    ) -> Result<(), Error>;
    /// The lease write a worker performs for itself: it never reopens intake,
    /// and `accepting` is the state a lease it has to *create* starts in.
    async fn write_worker_lease(
        &self,
        worker_id: Uuid,
        stats: Value,
        metadata: Option<Value>,
        ttl: Duration,
        accepting: bool,
    ) -> Result<(), Error>;
}

impl QueueProtocolTestExt for Queue {
    async fn dequeue(&self, limit: i64, worker_id: Uuid) -> Result<Vec<JobRow>, Error> {
        pgqueue::__test_support::dequeue(self, limit, worker_id).await
    }

    async fn finish(
        &self,
        job: &JobRow,
        status: JobStatus,
        result: Option<Value>,
        error: Option<&str>,
    ) -> Result<bool, Error> {
        pgqueue::__test_support::finish(self, job, status, result, error).await
    }

    async fn retry(&self, job: &JobRow, error: &str) -> Result<bool, Error> {
        pgqueue::__test_support::retry(self, job, error).await
    }

    async fn retry_swept(&self, job: &JobRow, error: Option<&str>) -> Result<bool, Error> {
        pgqueue::__test_support::retry_swept(self, job, error).await
    }

    async fn requeue_shutdown(&self, job: &JobRow, error: &str) -> Result<bool, Error> {
        pgqueue::__test_support::requeue_shutdown(self, job, error).await
    }

    async fn requeue_unhandled(&self, job: &JobRow) -> Result<bool, Error> {
        pgqueue::__test_support::requeue_unhandled(self, job).await
    }

    async fn write_worker_info(
        &self,
        worker_id: Uuid,
        stats: Value,
        metadata: Option<Value>,
        ttl: Duration,
    ) -> Result<(), Error> {
        pgqueue::__test_support::write_worker_info(self, worker_id, stats, metadata, ttl).await
    }

    async fn write_worker_lease(
        &self,
        worker_id: Uuid,
        stats: Value,
        metadata: Option<Value>,
        ttl: Duration,
        accepting: bool,
    ) -> Result<(), Error> {
        pgqueue::__test_support::write_worker_lease(self, worker_id, stats, metadata, ttl, accepting).await
    }
}

pub fn init_tracing() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::level_filters::LevelFilter::TRACE)
            .with_test_writer()
            .try_init();
    });
}

impl TestDb {
    pub async fn new(pool: PgPool) -> Self {
        Self::with(pool, |builder| builder).await
    }

    pub async fn with(pool: PgPool, customize: impl FnOnce(QueueBuilder) -> QueueBuilder) -> Self {
        init_tracing();
        let database = sqlx::query_scalar::<_, String>("SELECT current_database()::text")
            .fetch_one(&pool)
            .await
            .expect("read test database name");
        // The smallest legal grace, so a sweep reclaims as promptly as a test
        // needs without waiting out a production-sized cushion. Zero is refused
        // by `connect`: it collapses the recovery window entirely.
        let queue = customize(Queue::builder("postgres://unused").pool(pool.clone()).sweep_grace(MIN_SWEEP_GRACE))
            .connect()
            .await
            .expect("test queue connect");
        Self { queue, pool, database }
    }

    pub async fn another_queue(&self, customize: impl FnOnce(QueueBuilder) -> QueueBuilder) -> Queue {
        customize(Queue::builder("postgres://unused").pool(self.pool.clone()).sweep_grace(MIN_SWEEP_GRACE))
            .connect()
            .await
            .expect("second queue connect")
    }
}

/// Creates an empty database and returns its URL, for the few tests that need a
/// multi-threaded runtime and so cannot use the `#[sqlx::test]` fixture (it
/// builds a current-thread one). `Queue::connect` migrates it.
///
/// The name is derived from `tag` rather than randomized, and any previous
/// database is dropped first, so repeated runs reuse one database per test
/// instead of accumulating them (a panicking test cannot leak one either).
pub async fn fresh_database(tag: &str) -> String {
    use sqlx::Connection;
    let base = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://pgqueue:pgqueue@localhost:3532/pgqueue".to_string());
    let name = format!("it_{tag}");
    let mut conn = PgConnection::connect(&base).await.expect("connect for database creation");
    // One statement per call: `raw_sql` batches into an implicit transaction,
    // and `DROP DATABASE` cannot run inside one.
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(r#"DROP DATABASE IF EXISTS "{name}" WITH (FORCE)"#)))
        .execute(&mut conn)
        .await
        .expect("drop stale test database");
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(r#"CREATE DATABASE "{name}""#)))
        .execute(&mut conn)
        .await
        .expect("create test database");
    conn.close().await.expect("close creation connection");
    let (prefix, _) = base.rsplit_once('/').expect("database url has a path");
    format!("{prefix}/{name}")
}

/// Creates a non-superuser role with a hard connection limit and returns a URL
/// for `db_url` that authenticates as it, so a test can exhaust the connection
/// allowance. Superusers are exempt from `CONNECTION LIMIT`.
///
/// Roles are cluster-wide and outlive a database, so the name is derived from
/// `db_url` and recreated rather than randomized; repeated runs reuse one role
/// per test instead of accumulating them.
pub async fn limited_role_url(db_url: &str, limit: i32) -> String {
    use sqlx::Connection;
    let base = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://pgqueue:pgqueue@localhost:3532/pgqueue".to_string());
    let (prefix, db_name) = db_url.rsplit_once('/').expect("database url has a path");
    let role = format!("{db_name}_role");
    let mut conn = PgConnection::connect(&base).await.expect("connect for role creation");
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        r#"DROP ROLE IF EXISTS "{role}";
           CREATE ROLE "{role}" LOGIN PASSWORD 'test' CONNECTION LIMIT {limit};
           GRANT ALL ON DATABASE "{db_name}" TO "{role}";"#
    )))
    .execute(&mut conn)
    .await
    .expect("create limited role");
    conn.close().await.expect("close role connection");

    // Schema grants must be issued from inside the target database.
    let mut db_conn = PgConnection::connect(db_url).await.expect("connect for schema grants");
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        r#"GRANT ALL ON SCHEMA pgqueue TO "{role}";
           GRANT ALL ON ALL TABLES IN SCHEMA pgqueue TO "{role}";
           GRANT ALL ON ALL SEQUENCES IN SCHEMA pgqueue TO "{role}";"#
    )))
    .execute(&mut db_conn)
    .await
    .expect("grant schema privileges");
    db_conn.close().await.expect("close grant connection");

    let host = prefix.rsplit_once('@').expect("database url has a host").1;
    format!("postgres://{role}:test@{host}/{db_name}")
}

/// Waits until `pool` actually holds `size` connections.
///
/// `PgPoolOptions::connect` returns as soon as *one* connection is established;
/// `min_connections` is filled by a background task afterwards. So a test that
/// builds a pool and immediately calls [`revoke_connect`] is racing that task,
/// and when it wins the pool is stuck at whatever it had warmed — sometimes a
/// single connection, which the sweeper then detaches for its session-scoped
/// leadership lock, leaving the worker with none and no way to open more. That
/// is a test starving itself, not a queue failing, and it looked like a rare
/// hang in the polling-fallback tests.
pub async fn warm_pool(pool: &PgPool, size: u32) {
    wait_until(Duration::from_secs(30), Duration::from_millis(5), "pool never reached its minimum size", || async {
        pool.size() >= size
    })
    .await;
}

/// Revokes the right to open new connections for the role in `client_url`.
/// Connections already established keep working, so a warm pool survives while
/// anything needing a fresh connection is refused.
pub async fn revoke_connect(admin_url: &str, client_url: &str) {
    use sqlx::Connection;
    let (_, db_name) = admin_url.rsplit_once('/').expect("database url has a path");
    let role = client_url
        .split_once("://")
        .and_then(|(_, rest)| rest.split_once(':'))
        .expect("client url carries a role")
        .0
        .to_string();
    let mut conn = PgConnection::connect(admin_url).await.expect("connect to revoke");
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        r#"REVOKE CONNECT ON DATABASE "{db_name}" FROM "{role}";
           REVOKE CONNECT ON DATABASE "{db_name}" FROM PUBLIC;"#
    )))
    .execute(&mut conn)
    .await
    .expect("revoke connect");
    conn.close().await.expect("close revoke connection");
}

/// A consumer holding the live lease that [`pgqueue::Consumer::dequeue`]
/// requires, matching the documented "heartbeat before dequeueing" contract.
pub async fn leased_consumer(queue: &Queue, worker_id: Uuid) -> pgqueue::Consumer {
    let consumer = queue.consumer(worker_id);
    consumer.heartbeat(serde_json::json!({}), None, Duration::from_secs(30)).await.expect("consumer heartbeat");
    consumer
}

pub fn new_job(name: &str, customize: impl FnOnce(&mut JobRequest)) -> JobRequest {
    let mut job = JobRequest::new(name, serde_json::json!({"n": 1}));
    customize(&mut job);
    job
}

pub fn with_config(name: &str, customize: impl FnOnce(&mut JobConfig)) -> JobRequest {
    new_job(name, |job| customize(&mut job.config))
}

/// The version of every migration the crate embeds, oldest first — what a current history table holds — so tests about
/// the history table do not have to be edited each time a migration is added.
pub fn migration_versions() -> Vec<i64> {
    sqlx::migrate!("./migrations").iter().map(|migration| migration.version).collect()
}

pub async fn pool_with_max(pool: &PgPool, max_connections: u32) -> PgPool {
    sqlx::postgres::PgPoolOptions::new()
        .max_connections(max_connections)
        .connect_with(pool.connect_options().as_ref().clone())
        .await
        .expect("connect test pool")
}

/// Fast loop intervals for worker tests; override single fields with struct
/// update syntax: `WorkerTimers { abort: ..., ..test_timers() }`.
pub fn test_timers() -> WorkerTimers {
    WorkerTimers {
        abort: Duration::from_millis(50),
        schedule: Duration::from_millis(100),
        sweep: Duration::from_secs(60),
        worker_info: Duration::from_millis(100),
    }
}

/// Installs a trigger that parks a matching statement on advisory lock `key`
/// until [`hold_gate`]'s transaction ends, letting a test interleave work inside
/// a single library call. `event` is the trigger event (`INSERT` or `UPDATE`) and
/// `when` its row condition.
pub async fn install_statement_gate(pool: &PgPool, name: &str, key: i32, event: &str, when: &str) {
    // Test-only DDL assembled from constants in this crate, not user input.
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "CREATE FUNCTION pgqueue.{name}() RETURNS trigger
         LANGUAGE plpgsql AS $$
         BEGIN
             PERFORM pg_advisory_xact_lock({key}, hashtext(current_database()));
             RETURN NEW;
         END
         $$;
         CREATE TRIGGER {name}
         BEFORE {event} ON pgqueue.jobs
         FOR EACH ROW
         WHEN ({when})
         EXECUTE FUNCTION pgqueue.{name}();"
    )))
    .execute(pool)
    .await
    .unwrap_or_else(|error| panic!("install {name} gate: {error}"));
}

/// Installs a statement-level trigger that parks an `INSERT` on `pgqueue.jobs`
/// on advisory lock `key` only when the statement inserted no rows — a dedupe
/// insert whose `ON CONFLICT DO NOTHING` fired — pausing a library call between
/// its conflict decision and its collision re-read. Inserts that land rows pass
/// through untouched.
pub async fn install_conflicted_insert_gate(pool: &PgPool, name: &str, key: i32) {
    // Test-only DDL assembled from constants in this crate, not user input.
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "CREATE FUNCTION pgqueue.{name}() RETURNS trigger
         LANGUAGE plpgsql AS $$
         BEGIN
             IF NOT EXISTS (SELECT 1 FROM inserted_rows) THEN
                 PERFORM pg_advisory_xact_lock({key}, hashtext(current_database()));
             END IF;
             RETURN NULL;
         END
         $$;
         CREATE TRIGGER {name}
         AFTER INSERT ON pgqueue.jobs
         REFERENCING NEW TABLE AS inserted_rows
         FOR EACH STATEMENT
         EXECUTE FUNCTION pgqueue.{name}();"
    )))
    .execute(pool)
    .await
    .unwrap_or_else(|error| panic!("install {name} gate: {error}"));
}

/// Polls until some backend in the test database waits on advisory lock `key`
/// (the first half of a two-int advisory key, reported as `classid`).
pub async fn wait_for_advisory_waiter(pool: &PgPool, key: i32, message: &str) {
    wait_until(Duration::from_secs(5), Duration::from_millis(10), message, || async {
        sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (
                     SELECT 1 FROM pg_locks
                     WHERE locktype = 'advisory' AND NOT granted
                       AND classid = $1::oid
                       AND database = (SELECT oid FROM pg_database
                                       WHERE datname = current_database())
                 )",
        )
        .bind(key)
        .fetch_one(pool)
        .await
        .expect("inspect advisory lock waiters")
    })
    .await;
}

/// Holds a gate closed until the returned transaction is committed or rolled
/// back. Statements that reach the gate block until then.
pub async fn hold_gate(pool: &PgPool, key: i32, database: &str) -> sqlx::Transaction<'static, sqlx::Postgres> {
    let mut gate = pool.begin().await.expect("begin gate transaction");
    sqlx::query("SELECT pg_advisory_xact_lock($1, hashtext($2))")
        .bind(key)
        .bind(database)
        .execute(&mut *gate)
        .await
        .expect("take gate lock");
    gate
}

/// Records, from inside every statement that inserts or updates one of the queue's tables, the
/// `idle_in_transaction_session_timeout` that statement ran under, so a test can see which of the queue's writes ran
/// in a transaction that bounds how long the server lets it sit idle. Statement-level, so a write that changes no row
/// is seen too. Read it back with [`idle_bounds_seen`].
pub async fn record_idle_bounds(pool: &PgPool) {
    sqlx::raw_sql(
        "CREATE TABLE pgqueue.idle_bounds_seen (
             seen bigserial PRIMARY KEY,
             writer text NOT NULL,
             setting text NOT NULL
         );
         CREATE FUNCTION pgqueue.record_idle_bound() RETURNS trigger
         LANGUAGE plpgsql AS $$
         BEGIN
             INSERT INTO pgqueue.idle_bounds_seen (writer, setting)
             VALUES (TG_OP || ' ' || TG_TABLE_NAME, current_setting('idle_in_transaction_session_timeout'));
             RETURN NULL;
         END
         $$;
         CREATE TRIGGER record_idle_bound AFTER INSERT OR UPDATE ON pgqueue.jobs
             FOR EACH STATEMENT EXECUTE FUNCTION pgqueue.record_idle_bound();
         CREATE TRIGGER record_idle_bound AFTER INSERT OR UPDATE ON pgqueue.workers
             FOR EACH STATEMENT EXECUTE FUNCTION pgqueue.record_idle_bound();
         CREATE TRIGGER record_idle_bound AFTER INSERT OR UPDATE ON pgqueue.cron_schedules
             FOR EACH STATEMENT EXECUTE FUNCTION pgqueue.record_idle_bound();
         CREATE TRIGGER record_idle_bound AFTER INSERT OR UPDATE ON pgqueue.cron_occurrences
             FOR EACH STATEMENT EXECUTE FUNCTION pgqueue.record_idle_bound();",
    )
    .execute(pool)
    .await
    .expect("install the idle-bound recorder");
}

/// The writes [`record_idle_bounds`] has recorded since it was last asked, oldest first, as `(writer, setting)` — the
/// writer as its operation and table, `INSERT jobs` — and forgets them. Only committed writes are recorded.
pub async fn idle_bounds_seen(pool: &PgPool) -> Vec<(String, String)> {
    sqlx::query_as::<_, (String, String)>(
        "WITH seen AS (DELETE FROM pgqueue.idle_bounds_seen RETURNING seen, writer, setting)
         SELECT writer, setting FROM seen ORDER BY seen",
    )
    .fetch_all(pool)
    .await
    .expect("read the idle-bound recorder")
}

/// The bound every transaction the queue owns runs under, as `idle_in_transaction_session_timeout` reports it, on a
/// session that sets none of its own.
pub const QUEUE_TRANSACTION_IDLE_BOUND: &str = "30s";

/// Expires a worker's lease so sweep and intake logic treat it as dead.
pub async fn expire_worker(db: &TestDb, worker_id: Uuid) {
    sqlx::query("UPDATE pgqueue.workers SET expires_at = now() - interval '1 second' WHERE id = $1")
        .bind(worker_id)
        .execute(db.queue.pool())
        .await
        .expect("expire worker lease");
}

pub async fn backdate_job_liveness(db: &TestDb, id: Uuid) {
    sqlx::query(
        "UPDATE pgqueue.jobs
         SET started_at = now() - interval '1 hour', touched_at = now() - interval '1 hour'
         WHERE id = $1",
    )
    .bind(id)
    .execute(db.queue.pool())
    .await
    .expect("backdate job liveness");
}

/// Waits `within` for a shut-down worker to stop, and on expiry *requests* its
/// shutdown rather than walking away from it, handing back whatever the run
/// returned.
///
/// `let _ = tokio::time::timeout(within, run).await` looks equivalent and is not:
/// on expiry it drops the `JoinHandle` and never touches it again, so nothing
/// ever asks the worker to stop and it runs on against a pool the `#[sqlx::test]`
/// fixture is about to drop, at whichever test comes next.
///
/// "Requests" rather than "aborts", precisely: `Worker::run_until` spawns
/// `run_until_inner` and awaits *its* handle, so aborting the outer task drops
/// that inner handle — which detaches the inner task rather than cancelling it.
/// What actually stops the worker is the `drop_guard` the outer future was
/// holding: dropping it cancels the `dropped` token, and the worker runs its
/// ordinary graceful shutdown. Bounded and finite, which the alternative was not.
///
/// `None` means it had to be abandoned that way, or its task did not return a
/// result at all; a caller with a documented reason to ignore the outcome still
/// has to say so.
///
/// Deliberately non-panicking. Several callers stop a worker whose shutdown is
/// *expected* to go badly — one of them runs as a role denied the connections its
/// shutdown wants — so turning that into a failure inside a shared helper would
/// fail those tests for the very thing they exist to demonstrate.
pub async fn join_worker(
    mut run: tokio::task::JoinHandle<Result<(), Error>>,
    within: Duration,
) -> Option<Result<(), Error>> {
    match tokio::time::timeout(within, &mut run).await {
        Ok(Ok(result)) => Some(result),
        Ok(Err(join_error)) => {
            eprintln!("worker task did not return a result: {join_error}");
            None
        }
        Err(_) => {
            run.abort();
            let _ = run.await;
            None
        }
    }
}

/// Polls until `poll` yields a value, panicking with `message` once the
/// deadline passes.
pub async fn wait_for_some<T, F, Fut>(timeout: Duration, interval: Duration, message: &str, mut poll: F) -> T
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Option<T>>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(value) = poll().await {
            return value;
        }
        assert!(tokio::time::Instant::now() < deadline, "{message}");
        tokio::time::sleep(interval).await;
    }
}

pub async fn wait_until<F, Fut>(timeout: Duration, interval: Duration, message: &str, mut condition: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    wait_for_some(timeout, interval, message, || {
        let check = condition();
        async move { check.await.then_some(()) }
    })
    .await;
}

/// Polls until some backend in the test database blocks on a lock while
/// running a query matching `pattern`.
pub async fn wait_for_lock_waiter(db: &TestDb, pattern: &str, message: &str) {
    wait_until(Duration::from_secs(5), Duration::from_millis(10), message, || async {
        sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (
                     SELECT 1 FROM pg_stat_activity
                     WHERE datname = current_database() AND wait_event_type = 'Lock'
                       AND query LIKE $1
                 )",
        )
        .bind(pattern)
        .fetch_one(db.queue.pool())
        .await
        .expect("inspect lock waiters")
    })
    .await;
}

pub async fn wait_for_dequeue_lock_waiter(queue: &Queue, waiting: bool) {
    wait_until(
        Duration::from_secs(5),
        Duration::from_millis(10),
        &format!("dequeue lock waiter did not become {waiting}"),
        || async {
            let found = sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS (
                     SELECT 1 FROM pg_locks locks
                     JOIN pg_stat_activity activity USING (pid)
                     WHERE locks.locktype = 'advisory' AND NOT locks.granted
                       AND activity.datname = current_database()
                 )",
            )
            .fetch_one(queue.pool())
            .await
            .expect("inspect dequeue lock waiter");
            found == waiting
        },
    )
    .await;
}

pub async fn wait_for_worker_intake_closed(db: &TestDb, worker_id: Uuid) {
    wait_until(Duration::from_secs(5), Duration::from_millis(10), "worker intake did not close", || async {
        sqlx::query_scalar::<_, bool>("SELECT accepting FROM pgqueue.workers WHERE id = $1")
            .bind(worker_id)
            .fetch_optional(db.queue.pool())
            .await
            .expect("inspect worker intake")
            == Some(false)
    })
    .await;
}

pub async fn wait_for_done_listener(db: &TestDb) {
    wait_for_done_listeners(db.queue.pool(), 1).await;
}

pub async fn wait_for_done_listeners(pool: &PgPool, count: i64) {
    wait_until(
        Duration::from_secs(5),
        Duration::from_millis(10),
        &format!("{count} completion listeners did not subscribe"),
        || async {
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM pg_stat_activity
                 WHERE datname = current_database() AND query LIKE 'LISTEN %'",
            )
            .fetch_one(pool)
            .await
            .expect("inspect completion listener")
                >= count
        },
    )
    .await;
}

/// Statement counters for the tests that assert how many round trips a
/// path costs. They use `pg_stat_statements`, which `compose.yaml` preloads;
/// each such test skips itself when the extension is unavailable, so the
/// suite still runs against a plain Postgres.
pub fn admin_url() -> String {
    std::env::var("DATABASE_URL").unwrap_or_else(|_| "postgres://pgqueue:pgqueue@localhost:3532/pgqueue".to_string())
}

pub struct Stats {
    conn: PgConnection,
    dbname: String,
}

impl Stats {
    /// `None` when `pg_stat_statements` is not usable, so the suite still runs
    /// against a plain `docker compose up`. Callers must say so — see
    /// [`Stats::skipped`].
    ///
    /// Reading the view is the only answer, for two reasons. Creating the
    /// extension is not enough to tell: its objects survive in a database that
    /// was set up while the overlay was running, and only reading the view
    /// reports that the library is no longer preloaded. And `CREATE EXTENSION
    /// IF NOT EXISTS` is not concurrency-safe — the extension is cluster-wide
    /// and the tests that want it run in parallel, so two of them racing fail
    /// one on `pg_extension_name_index` (or on `tuple concurrently updated`)
    /// rather than agreeing the extension is there. Taken as the answer, that
    /// error silently disabled a test whose only problem was a sibling asking
    /// for the same thing at the same time. It is a single autocommit
    /// statement, so a failed one leaves the connection usable for the read.
    pub async fn new(dbname: &str) -> Option<Self> {
        let mut conn = PgConnection::connect(&admin_url()).await.unwrap();
        let _ = sqlx::raw_sql("CREATE EXTENSION IF NOT EXISTS pg_stat_statements").execute(&mut conn).await;
        sqlx::query_scalar::<_, i64>("SELECT count(*)::bigint FROM pg_stat_statements")
            .fetch_one(&mut conn)
            .await
            .ok()?;
        Some(Self { conn, dbname: dbname.to_string() })
    }

    /// Reports that a test measured nothing because the extension is missing.
    ///
    /// A bare `return` reads as a pass, and these tests are the only cover for
    /// the statement counts they assert; a silent one is a green run that
    /// checked nothing.
    pub fn skipped(test: &str) {
        eprintln!("skipping {test}: pg_stat_statements is unavailable");
    }

    /// Counts executions from `pattern` onwards. `pg_stat_statements_reset` is
    /// cluster-wide, so resetting would race every other test in the run;
    /// each test has its own database, so a baseline isolates it instead.
    pub async fn since_now(&mut self, pattern: &'static str) -> StatsCounter {
        let baseline = self.calls(pattern).await;
        StatsCounter { pattern, baseline }
    }

    pub async fn delta(&mut self, counter: &StatsCounter) -> i64 {
        self.calls(counter.pattern).await - counter.baseline
    }

    /// Polls until `counter` has seen `count` executions. Statements arrive
    /// when the runtime gets round to issuing them, so a test that needs some
    /// to have happened waits for them instead of sleeping for a while and
    /// hoping — a starved timer is not the failure these tests are about.
    ///
    /// Takes `&mut self` all the way down, so it cannot be phrased as a
    /// `wait_until` closure.
    pub async fn wait_for_calls(&mut self, counter: &StatsCounter, count: i64, message: &str) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            let seen = self.delta(counter).await;
            if seen >= count {
                return;
            }
            assert!(tokio::time::Instant::now() < deadline, "{message} ({seen} of {count})");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Total executions of statements matching `pattern` in the test database.
    pub async fn calls(&mut self, pattern: &str) -> i64 {
        sqlx::query_scalar(
            "SELECT COALESCE(sum(s.calls), 0)::bigint
             FROM pg_stat_statements s
             JOIN pg_database d ON d.oid = s.dbid
             WHERE d.datname = $1 AND s.query LIKE $2",
        )
        .bind(&self.dbname)
        .bind(pattern)
        .fetch_one(&mut self.conn)
        .await
        .unwrap()
    }
}

pub struct StatsCounter {
    pattern: &'static str,
    baseline: i64,
}

/// `LIKE` is order-sensitive, so this names only what opens the claim
/// statement and what it cannot be without: the `claim_lock` CTE at the top
/// and the `SKIP LOCKED` of its candidate walk. Anchoring on interior text
/// silently matched nothing when the statement's shape changed — which reads
/// exactly like a worker that never claimed.
pub const DEQUEUE_CLAIM: &str = "%WITH claim_lock AS%SKIP LOCKED%";
pub const DEQUEUE_PROBE: &str = "%AS intake_open%";

/// `finish_with_guards` opens with a bare `candidate` CTE, but so do
/// `Database::abort` and the unsettled-claim abort, so a count over this is
/// exact only in a window where neither of those runs. The sweeper's
/// near-identical batch abort opens with `WITH requested AS` and never matches.
pub const FINISH_STATEMENT: &str = "WITH candidate AS%";

/// The per-queue statement `/api/queues` fans out. Distinctive enough on its
/// `has_live_workers` signal, which nothing else selects.
pub const QUEUE_SIGNALS_STATEMENT: &str = "%AS has_live_workers%";

/// The `/health` probe, distinctive on the alias its subquery carries.
pub const HEALTH_PROBE_STATEMENT: &str = "%) AS probe%";

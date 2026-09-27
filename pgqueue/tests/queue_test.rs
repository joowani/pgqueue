//! Integration tests for the Postgres queue core, against a real database.

use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use std::time::Duration;

use crate::{
    DEQUEUE_CLAIM, DEQUEUE_PROBE, Stats, hold_gate, install_statement_gate, leased_consumer, wait_for_lock_waiter,
};
use crate::{
    EnqueueResultTestExt, QueueProtocolTestExt, TestDb, backdate_job_liveness, list_workers, new_job, wait_for_some,
    wait_until, with_config,
};
use jiff::{SignedDuration, Timestamp};
use pgqueue::{
    EnqueueResult, Error, JobConfig, JobCursor, JobFilter, JobRetention, JobRetryBackoff, JobStatus, Queue,
    WorkerCursor, WorkerFilter,
};
use serde_json::json;
use sqlx::migrate::Migrate;
use sqlx::{Connection, PgConnection, Row};
use uuid::Uuid;

async fn connect_queue(pool: PgPool) -> Result<Queue, Error> {
    Queue::builder("postgres://unused").pool(pool).connect().await
}

async fn wait_for_recovery_stats(queue: &Queue, retried: u64, aborted: u64) {
    wait_until(
        Duration::from_secs(5),
        Duration::from_millis(10),
        "background recovery counters did not reach the expected values",
        || async {
            let stats = queue.stats();
            stats.retried == retried && stats.aborted == aborted
        },
    )
    .await;
}

async fn install_empty_migration_history(pool: &PgPool) {
    sqlx::raw_sql(
        "DROP SCHEMA pgqueue CASCADE;
         CREATE SCHEMA pgqueue;
         CREATE TABLE pgqueue.migrations (
             version BIGINT PRIMARY KEY,
             description TEXT NOT NULL,
             installed_on TIMESTAMPTZ NOT NULL DEFAULT now(),
             success BOOLEAN NOT NULL,
             checksum BYTEA NOT NULL,
             execution_time BIGINT NOT NULL
         );",
    )
    .execute(pool)
    .await
    .unwrap();
}

async fn wait_for_blocked_baseline_migration(pool: &PgPool) -> i32 {
    wait_for_some(
        Duration::from_secs(5),
        Duration::from_millis(10),
        "baseline migration did not block while creating the jobs table",
        || async {
            sqlx::query_scalar::<_, i32>(
                "SELECT pid FROM pg_stat_activity
                 WHERE datname = current_database()
                   AND wait_event_type = 'Lock'
                   AND query LIKE 'CREATE TABLE pgqueue.jobs%'
                 LIMIT 1",
            )
            .fetch_optional(pool)
            .await
            .unwrap()
        },
    )
    .await
}

#[sqlx::test(migrations = "./migrations")]
async fn test_connect_is_idempotent_and_checks_migrations_again(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    // A second connect checks the migration history without running DDL.
    let again = db.another_queue(|b| b).await;
    assert_eq!(again.name(), db.queue.name());
}

#[sqlx::test(migrations = "./migrations")]
async fn test_queue_builder_installs_a_missing_schema(pool: PgPool) {
    sqlx::query("DROP SCHEMA pgqueue CASCADE").execute(&pool).await.unwrap();

    let queue = Queue::builder("postgres://unused").pool(pool).connect().await.unwrap();
    let has_jobs = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (
             SELECT 1 FROM information_schema.tables
             WHERE table_schema = 'pgqueue' AND table_name = 'jobs'
         )",
    )
    .fetch_one(queue.pool())
    .await
    .unwrap();
    assert!(has_jobs);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_migrations_install_the_current_jobs_and_workers_columns(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let job_columns = sqlx::query_scalar::<_, String>(
        "SELECT column_name FROM information_schema.columns
         WHERE table_schema = 'pgqueue' AND table_name = 'jobs'
           AND column_name IN ('cron_expr', 'dedupe_key', 'heartbeat_ms', 'kind', 'max_attempts',
                               'result_ttl_ms', 'retried_at', 'ttl_ms', 'unique_key')
         ORDER BY column_name",
    )
    .fetch_all(db.queue.pool())
    .await
    .unwrap();
    assert_eq!(
        job_columns,
        [
            "cron_expr",
            "dedupe_key",
            "kind",
            "max_attempts",
            "result_ttl_ms",
            "retried_at"
        ]
    );

    let worker_accepting = sqlx::query_scalar::<_, String>(
        "SELECT column_name FROM information_schema.columns
         WHERE table_schema = 'pgqueue' AND table_name = 'workers' AND column_name = 'accepting'",
    )
    .fetch_optional(db.queue.pool())
    .await
    .unwrap();
    assert_eq!(worker_accepting.as_deref(), Some("accepting"));
}

#[sqlx::test(migrations = "./migrations")]
async fn test_migrations_reject_timestamps_jiff_cannot_decode(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let constraints = sqlx::query_as::<_, (String, bool)>(
        "SELECT conname, convalidated FROM pg_constraint
         WHERE connamespace = 'pgqueue'::regnamespace
           AND conname LIKE '%_timestamps_jiff_range_check'
         ORDER BY conname",
    )
    .fetch_all(db.queue.pool())
    .await
    .unwrap();
    assert_eq!(
        constraints,
        [
            ("cron_occurrences_timestamps_jiff_range_check".to_owned(), true,),
            ("cron_schedules_timestamps_jiff_range_check".to_owned(), true,),
            ("jobs_timestamps_jiff_range_check".to_owned(), true),
            ("workers_timestamps_jiff_range_check".to_owned(), true),
        ]
    );

    let valid = sqlx::query_scalar::<_, Uuid>(
        "INSERT INTO pgqueue.jobs (queue, name, enqueued_at)
         VALUES ($1, 'max-jiff-timestamp', TIMESTAMPTZ '9999-12-30 22:00:00.999999+00')
         RETURNING id",
    )
    .bind(db.queue.name())
    .fetch_one(db.queue.pool())
    .await
    .unwrap();
    db.queue.fetch_job(valid).await.unwrap().unwrap();

    for timestamp in [
        "9999-12-30 22:00:01+00",
        "9999-12-31 00:00:00+00",
        "10000-01-01 00:00:00+00",
        "infinity",
    ] {
        let error = sqlx::query(
            "INSERT INTO pgqueue.jobs (queue, name, enqueued_at)
             VALUES ($1, 'bad-timestamp', $2::timestamptz)",
        )
        .bind(db.queue.name())
        .bind(timestamp)
        .execute(db.queue.pool())
        .await
        .unwrap_err();
        let database = error.as_database_error().unwrap();
        assert_eq!(database.code().as_deref(), Some("23514"));
        assert_eq!(database.constraint(), Some("jobs_timestamps_jiff_range_check"));
    }
}

#[sqlx::test(migrations = "./migrations")]
async fn test_migrations_install_the_cron_occurrence_ledger_and_expiry_index(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let primary_key = sqlx::query_scalar::<_, String>(
        "SELECT pg_get_constraintdef(oid) FROM pg_constraint
         WHERE conrelid = 'pgqueue.cron_occurrences'::regclass AND contype = 'p'",
    )
    .fetch_one(db.queue.pool())
    .await
    .unwrap();
    assert!(primary_key.contains("PRIMARY KEY (queue, dedupe_key, scheduled_at)"), "{primary_key}");

    let expiry_index = sqlx::query_scalar::<_, String>(
        "SELECT indexdef FROM pg_indexes
         WHERE schemaname = 'pgqueue' AND indexname = 'cron_occurrences_expiry_idx'",
    )
    .fetch_one(db.queue.pool())
    .await
    .unwrap();
    assert!(expiry_index.contains("(queue, expires_at)"));
}

/// The baseline installs one function per shape of the listing's `?name=` filter,
/// so the name is an unconditional equality and reaches its index under a generic
/// plan. No obsolete overload should leak into a fresh schema.
#[sqlx::test(migrations = "./migrations")]
async fn test_migrations_install_only_the_current_job_page_keys_functions(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let arities = sqlx::query_scalar::<_, i16>(
        "SELECT pronargs FROM pg_proc
         WHERE pronamespace = 'pgqueue'::regnamespace AND proname = 'job_page_keys'
         ORDER BY pronargs",
    )
    .fetch_all(db.queue.pool())
    .await
    .unwrap();
    assert_eq!(arities, [6], "a fresh schema contains only the current unfiltered-page function");

    let by_name = sqlx::query_scalar::<_, i16>(
        "SELECT pronargs FROM pg_proc
         WHERE pronamespace = 'pgqueue'::regnamespace AND proname = 'job_page_keys_by_name'",
    )
    .fetch_all(db.queue.pool())
    .await
    .unwrap();
    assert_eq!(
        by_name,
        [7],
        "the name-filtered page has a function of its own so the name is an \
         equality rather than a parameter the planner cannot fold"
    );
}

/// No name-leading dequeue index, deliberately: workers claim without a name
/// filter (a worker handles every name enqueued on its queue), so a name index
/// would only give the planner a second claim plan to defect to — the
/// read-everything-and-sort shape the plan test below pins the claim against.
#[sqlx::test(migrations = "./migrations")]
async fn test_migrations_install_no_name_filtered_dequeue_index(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let definitions = sqlx::query_scalar::<_, String>(
        "SELECT indexdef FROM pg_indexes
         WHERE schemaname = 'pgqueue' AND indexname = 'jobs_dequeue_name_idx'",
    )
    .fetch_all(db.queue.pool())
    .await
    .unwrap();
    assert!(definitions.is_empty(), "the name-filtered dequeue index must stay deleted: {definitions:?}");
}

/// The claim keeps its ordered walk under the generic plan — the only plan
/// shape it is allowed to have. The defect this pins against: after enough
/// executions a prepared statement may settle into the generic plan, and a
/// planner with an attractive rival access path there changes the claim's
/// algorithm from "walk the ordered index, stop at `LIMIT`" to "materialize
/// and sort every ready row" — measured at 117 ms and a 7 MB disk spill per
/// claim against 0.5 ms for the walk, with nothing but the plan itself able
/// to prove the switch absent. `jobs_dequeue_idx` is the only *index-ordered*
/// path (a sequential scan plus sort always exists in principle), so this
/// test is the enforcement, run over deliberately unfriendly statistics:
/// mixed priorities, a majority of rows scheduled in the future, and high
/// name cardinality, each of which nudges the cost model toward a
/// materializing plan.
#[sqlx::test(migrations = "./migrations")]
async fn test_the_dequeue_claim_walks_the_dequeue_index_under_the_generic_plan(pool: PgPool) {
    let _db = TestDb::new(pool.clone()).await;
    // 20,000 rows: five priorities, three quarters scheduled in the future
    // (in the partial index but failing the `scheduled_at` predicate), high
    // name cardinality.
    sqlx::query(
        "INSERT INTO pgqueue.jobs (queue, name, status, priority, scheduled_at, enqueued_at)
         SELECT 'default', 'job_' || (g % 1000)::text, 'queued', ((g % 5) - 2)::smallint,
                CASE WHEN g % 4 = 0 THEN now() - (g * interval '1 millisecond')
                     ELSE now() + interval '1 hour' + (g * interval '1 millisecond') END,
                now()
         FROM generate_series(1, 20000) AS g",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::raw_sql("ANALYZE pgqueue.jobs").execute(&pool).await.unwrap();

    for (label, priorities) in [
        ("the full priority range", "(-32768)::smallint, 32767::smallint"),
        ("a narrow priority band", "0::smallint, 2::smallint"),
    ] {
        let plan = generic_claim_plan(
            &pool,
            pgqueue::__test_support::dequeue_claim_sql(),
            &format!("'default', {priorities}, 24, gen_random_uuid(), false, 1"),
        )
        .await;
        assert!(plan.contains("jobs_dequeue_idx"), "the claim must walk the dequeue index over {label}:\n{plan}");
        assert!(
            !plan.contains("Sort"),
            "the claim must never sort: a sort node means the planner materialized \
             the backlog instead of walking the ordered index (over {label}):\n{plan}"
        );
    }
}

/// The claim's plan under `force_generic_plan`, which is what sqlx's prepared
/// statements settle into and the only mode in which the parameter-folding above
/// is impossible.
async fn generic_claim_plan(pool: &PgPool, sql: &str, args: &str) -> String {
    let types = "text, smallint, smallint, bigint, uuid, boolean, int";
    let mut connection = pool.acquire().await.unwrap();
    sqlx::raw_sql("SET plan_cache_mode = force_generic_plan").execute(&mut *connection).await.unwrap();
    // The only interpolation is this crate's own statement text and this
    // function's own callers' literals.
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!("PREPARE claim_under_test({types}) AS {sql}")))
        .execute(&mut *connection)
        .await
        .unwrap();
    // `ANALYZE`, so the plan reports which nodes actually ran — the gating of
    // the contention fallback is the thing worth pinning, and a costs-only plan
    // cannot show it. The claim really does lock and update rows, so it runs
    // inside a transaction that is thrown away.
    sqlx::raw_sql("BEGIN").execute(&mut *connection).await.unwrap();
    let plan = sqlx::query_scalar::<_, String>(sqlx::AssertSqlSafe(format!(
        "EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF) EXECUTE claim_under_test({args})"
    )))
    .fetch_all(&mut *connection)
    .await
    .unwrap()
    .join("\n");
    sqlx::raw_sql("ROLLBACK").execute(&mut *connection).await.unwrap();
    // Leave the session as it was found, and *after* the ROLLBACK: `PREPARE` and
    // `DEALLOCATE` are both transactional, so a deallocation inside that
    // transaction would be undone with it. Both statements are session-scoped and
    // sqlx runs no reset when a connection goes back to the pool, so a caller that
    // asks for two plans — this test does — gets the same connection back often
    // enough to fail on `42P05 prepared statement "claim_under_test" already
    // exists`, and every later user of that connection inherits
    // `force_generic_plan`.
    sqlx::raw_sql("DEALLOCATE claim_under_test").execute(&mut *connection).await.unwrap();
    sqlx::raw_sql("RESET plan_cache_mode").execute(&mut *connection).await.unwrap();
    plan
}

/// Every index on `jobs` is paid for by every enqueue and by every attempt
/// state change — which already rewrites five entries — so the set is pinned
/// here and adding one has to be a deliberate decision. `jobs_name_status_idx`
/// was not: no statement the crate issues ever selected it. `Queue::jobs_page`
/// documents that its `status` and `name` filters are deliberately *not*
/// index-backed for exactly this reason, and the dashboard's own listing pages
/// through the kind-qualified indexes below.
#[sqlx::test(migrations = "./migrations")]
async fn test_migrations_install_only_the_job_indexes_the_crate_reads(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let indexes = sqlx::query_scalar::<_, String>(
        "SELECT indexname FROM pg_indexes
         WHERE schemaname = 'pgqueue' AND tablename = 'jobs'
         ORDER BY indexname",
    )
    .fetch_all(db.queue.pool())
    .await
    .unwrap();
    assert_eq!(
        indexes,
        [
            "jobs_active_idx",
            "jobs_dashboard_name_page_idx",
            "jobs_dashboard_name_prefix_idx",
            "jobs_dashboard_ready_idx",
            "jobs_dashboard_status_page_idx",
            "jobs_dashboard_terminal_idx",
            "jobs_dedupe_key_idx",
            "jobs_dequeue_idx",
            "jobs_expires_idx",
            "jobs_page_idx",
            "jobs_pkey",
        ]
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn test_migrations_install_the_cron_registry_key(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let schedule_key = sqlx::query_scalar::<_, String>(
        "SELECT pg_get_constraintdef(oid) FROM pg_constraint
         WHERE conrelid = 'pgqueue.cron_schedules'::regclass AND contype = 'p'",
    )
    .fetch_one(db.queue.pool())
    .await
    .unwrap();
    assert!(schedule_key.contains("PRIMARY KEY (queue, dedupe_key)"), "{schedule_key}");
}

#[sqlx::test(migrations = "./migrations")]
async fn test_migrations_install_the_live_dedupe_key_index(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let (indexdef, predicate) = sqlx::query_as::<_, (String, String)>(
        "SELECT pg_get_indexdef(indexes.indexrelid), pg_get_expr(indexes.indpred, indexes.indrelid) \
         FROM pg_index indexes \
         JOIN pg_class index_class ON index_class.oid = indexes.indexrelid \
         JOIN pg_class table_class ON table_class.oid = indexes.indrelid \
         JOIN pg_namespace namespace ON namespace.oid = table_class.relnamespace \
         WHERE namespace.nspname = 'pgqueue' AND index_class.relname = 'jobs_dedupe_key_idx'",
    )
    .fetch_one(db.queue.pool())
    .await
    .unwrap();
    assert!(indexdef.contains("UNIQUE INDEX"));
    assert!(indexdef.contains("(queue, dedupe_key)"));
    assert!(predicate.contains("dedupe_key IS NOT NULL"), "{predicate}");
    for status in ["queued", "running", "aborting"] {
        assert!(predicate.contains(status), "live index predicate missing {status}: {predicate}");
    }
}

#[sqlx::test(migrations = "./migrations")]
async fn test_migrations_use_an_isolated_history_table(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let pgqueue_history = sqlx::query_scalar::<_, Option<String>>("SELECT to_regclass('pgqueue.migrations')::text")
        .fetch_one(db.queue.pool())
        .await
        .unwrap();
    // `_sqlx_migrations` in `public` is what the migrator uses when it reads no `sqlx.toml`, so naming it here is what
    // makes this an assertion about the configuration having taken effect rather than about an arbitrary table.
    let public_history = sqlx::query_scalar::<_, Option<String>>("SELECT to_regclass('public._sqlx_migrations')::text")
        .fetch_one(db.queue.pool())
        .await
        .unwrap();
    assert!(pgqueue_history.is_some());
    assert!(public_history.is_none());
    let versions = sqlx::query_scalar::<_, i64>("SELECT version FROM pgqueue.migrations ORDER BY version")
        .fetch_all(db.queue.pool())
        .await
        .unwrap();
    assert_eq!(versions, [1]);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_failed_migration_check_returns_the_shared_pool_connection(pool: PgPool) {
    let shared = crate::pool_with_max(&pool, 1).await;
    let checksum = sqlx::query_scalar::<_, Vec<u8>>("SELECT checksum FROM pgqueue.migrations WHERE version = 1")
        .fetch_one(&shared)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE pgqueue.migrations
         SET checksum = checksum || decode('00', 'hex')
         WHERE version = 1",
    )
    .execute(&shared)
    .await
    .unwrap();

    let error = Queue::builder("postgres://unused").pool(shared.clone()).connect().await.unwrap_err();
    assert!(matches!(error, Error::Migration(sqlx::migrate::MigrateError::VersionMismatch(1))));

    sqlx::query("UPDATE pgqueue.migrations SET checksum = $1 WHERE version = 1")
        .bind(checksum)
        .execute(&shared)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), Queue::builder("postgres://unused").pool(shared).connect())
        .await
        .expect("migration check did not return the shared pool connection")
        .unwrap();
}

#[sqlx::test(migrations = "./migrations")]
async fn test_cancelled_migration_closes_the_lock_bearing_connection(pool: PgPool) {
    install_empty_migration_history(&pool).await;
    let shared = crate::pool_with_max(&pool, 1).await;
    let mut blocker = pool.begin().await.unwrap();
    sqlx::query("CREATE TABLE pgqueue.jobs (id integer)").execute(&mut *blocker).await.unwrap();

    let applying_pool = shared.clone();
    let applying = tokio::spawn(async move { Queue::builder("postgres://unused").pool(applying_pool).connect().await });
    let migration_pid = wait_for_blocked_baseline_migration(&pool).await;
    let holds_advisory_lock = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (
             SELECT 1 FROM pg_locks
             WHERE pid = $1 AND locktype = 'advisory' AND granted
         )",
    )
    .bind(migration_pid)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(holds_advisory_lock);

    applying.abort();
    assert!(applying.await.unwrap_err().is_cancelled());
    blocker.rollback().await.unwrap();

    wait_until(
        Duration::from_secs(5),
        Duration::from_millis(10),
        "cancelled migration backend kept its advisory lock",
        || async {
            !sqlx::query_scalar::<_, bool>("SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid = $1)")
                .bind(migration_pid)
                .fetch_one(&pool)
                .await
                .unwrap()
        },
    )
    .await;

    tokio::time::timeout(Duration::from_secs(2), Queue::builder("postgres://unused").pool(shared).connect())
        .await
        .expect("cancelled migration left the lock-bearing connection in the shared pool")
        .unwrap();
}

#[sqlx::test(migrations = "./migrations")]
async fn test_migration_ddl_lock_timeout_is_bounded_and_restored(pool: PgPool) {
    install_empty_migration_history(&pool).await;
    let shared = crate::pool_with_max(&pool, 1).await;
    sqlx::query("SET lock_timeout = '7s'").execute(&shared).await.unwrap();
    let mut blocker = pool.begin().await.unwrap();
    sqlx::query("CREATE TABLE pgqueue.jobs (id integer)").execute(&mut *blocker).await.unwrap();

    let started = tokio::time::Instant::now();
    let error = Queue::builder("postgres://unused")
        .pool(shared.clone())
        .migration_lock_timeout(Duration::from_millis(50))
        .connect()
        .await
        .unwrap_err();
    assert!(matches!(&error, Error::Migration(_)), "{error}");
    assert!(started.elapsed() < Duration::from_secs(2), "migration ignored its lock timeout");

    let restored =
        sqlx::query_scalar::<_, String>("SELECT current_setting('lock_timeout')").fetch_one(&shared).await.unwrap();
    assert_eq!(restored, "7s");
    let held_advisory_locks = sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM pg_locks
         WHERE locktype = 'advisory' AND database = (SELECT oid FROM pg_database WHERE datname = current_database())",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(held_advisory_locks, 0);
    blocker.rollback().await.unwrap();

    Queue::builder("postgres://unused").pool(shared).connect().await.unwrap();
}

#[sqlx::test(migrations = "./migrations")]
async fn test_migration_advisory_lock_timeout_is_bounded_and_restored(pool: PgPool) {
    let deleted = sqlx::query("DELETE FROM pgqueue.migrations").execute(&pool).await.unwrap();
    assert_eq!(deleted.rows_affected(), 1);

    let mut blocker = pool.acquire().await.unwrap();
    Migrate::lock(&mut *blocker).await.unwrap();
    let shared = crate::pool_with_max(&pool, 1).await;
    sqlx::query("SET lock_timeout = '7s'").execute(&shared).await.unwrap();

    let started = tokio::time::Instant::now();
    let error = Queue::builder("postgres://unused")
        .pool(shared.clone())
        .migration_lock_timeout(Duration::from_millis(50))
        .connect()
        .await
        .unwrap_err();
    // The lock's own timeout, `55P03`, and nothing after it: with the history deleted, a connect that skipped the lock
    // fails at once too — on the first `CREATE TABLE` of a schema that already exists — as the same variant.
    assert!(
        matches!(
            &error,
            Error::Migration(sqlx::migrate::MigrateError::Execute(sqlx::Error::Database(database)))
                if database.code().as_deref() == Some("55P03")
        ),
        "{error}"
    );
    assert!(started.elapsed() < Duration::from_secs(2), "migration ignored its advisory lock timeout");

    let restored =
        sqlx::query_scalar::<_, String>("SELECT current_setting('lock_timeout')").fetch_one(&shared).await.unwrap();
    assert_eq!(restored, "7s");
    Migrate::unlock(&mut *blocker).await.unwrap();
}

#[sqlx::test(migrations = "./migrations")]
async fn test_migration_lock_timeout_rejects_unrepresentable_values(pool: PgPool) {
    for timeout in [
        Duration::ZERO,
        Duration::from_millis(i64::from(i32::MAX) as u64 + 1),
    ] {
        let error = Queue::builder("postgres://unused")
            .pool(pool.clone())
            .migration_lock_timeout(timeout)
            .connect()
            .await
            .unwrap_err();
        assert!(matches!(error, Error::Config(_)), "{error}");
    }
}

//noinspection SqlNoDataSourceInspection
#[sqlx::test(migrations = "./migrations")]
async fn test_current_schema_needs_no_schema_ddl_privilege(pool: PgPool) {
    let restricted = crate::pool_with_max(&pool, 1).await;
    sqlx::query("SET ROLE pg_read_all_data").execute(&restricted).await.unwrap();
    let can_create = sqlx::query_scalar::<_, bool>(r#"SELECT has_schema_privilege(current_user, 'pgqueue', 'CREATE')"#)
        .fetch_one(&restricted)
        .await
        .unwrap();
    assert!(!can_create);

    let queue = Queue::builder("postgres://unused").pool(restricted).connect().await.unwrap();
    assert_eq!(queue.name(), "default");
}

//noinspection SqlNoDataSourceInspection
#[sqlx::test(migrations = "./migrations")]
async fn test_restricted_connection_rechecks_history_after_waiting_for_migration_lock(pool: PgPool) {
    let mut privileged = pool.acquire().await.unwrap();
    Migrate::lock(&mut *privileged).await.unwrap();
    let saved = sqlx::query_as::<_, (i64, String, bool, Vec<u8>, i64)>(
        "DELETE FROM pgqueue.migrations
         RETURNING version, description, success, checksum, execution_time",
    )
    .fetch_one(&mut *privileged)
    .await
    .unwrap();

    let restricted = crate::pool_with_max(&pool, 1).await;
    sqlx::query("SET ROLE pg_read_all_data").execute(&restricted).await.unwrap();
    let applying_pool = restricted.clone();
    let applying = tokio::spawn(async move { Queue::builder("postgres://unused").pool(applying_pool).connect().await });
    wait_until(
        Duration::from_secs(5),
        Duration::from_millis(10),
        "restricted connection did not wait for the migration lock",
        || async {
            sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS (
                     SELECT 1 FROM pg_locks
                     WHERE locktype = 'advisory' AND NOT granted
                       AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
                 )",
            )
            .fetch_one(&pool)
            .await
            .unwrap()
        },
    )
    .await;

    sqlx::query(
        "INSERT INTO pgqueue.migrations
             (version, description, success, checksum, execution_time)
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(saved.0)
    .bind(saved.1)
    .bind(saved.2)
    .bind(saved.3)
    .bind(saved.4)
    .execute(&mut *privileged)
    .await
    .unwrap();
    Migrate::unlock(&mut *privileged).await.unwrap();

    let queue = applying.await.unwrap().unwrap();
    assert_eq!(queue.name(), "default");
}

#[sqlx::test(migrations = "./migrations")]
async fn test_connect_rejects_a_dirty_migration(pool: PgPool) {
    let changed =
        sqlx::query("UPDATE pgqueue.migrations SET success = false WHERE version = 1").execute(&pool).await.unwrap();
    assert_eq!(changed.rows_affected(), 1);

    let error = connect_queue(pool.clone()).await.unwrap_err();
    assert!(matches!(error, Error::Migration(sqlx::migrate::MigrateError::Dirty(1))), "{error}");
}

#[sqlx::test(migrations = "./migrations")]
async fn test_connect_rejects_an_unknown_migration(pool: PgPool) {
    let inserted = sqlx::query(
        "INSERT INTO pgqueue.migrations
             (version, description, success, checksum, execution_time)
         SELECT 999999, 'unknown', true, checksum, 0
         FROM pgqueue.migrations WHERE version = 1",
    )
    .execute(&pool)
    .await
    .unwrap();
    assert_eq!(inserted.rows_affected(), 1);

    let error = connect_queue(pool).await.unwrap_err();
    assert!(matches!(error, Error::Migration(sqlx::migrate::MigrateError::VersionMissing(999999))), "{error}");
}

#[sqlx::test(migrations = "./migrations")]
async fn test_connect_rejects_a_modified_migration(pool: PgPool) {
    let changed = sqlx::query(
        "UPDATE pgqueue.migrations SET checksum = checksum || decode('00', 'hex')
         WHERE version = 1",
    )
    .execute(&pool)
    .await
    .unwrap();
    assert_eq!(changed.rows_affected(), 1);

    let error = connect_queue(pool).await.unwrap_err();
    assert!(matches!(error, Error::Migration(sqlx::migrate::MigrateError::VersionMismatch(1))), "{error}");
}

#[sqlx::test(migrations = "./migrations")]
async fn test_connect_releases_the_migration_lock_after_applying_a_missing_history_entry_fails(pool: PgPool) {
    let deleted = sqlx::query(
        "DELETE FROM pgqueue.migrations
         WHERE version = (SELECT max(version) FROM pgqueue.migrations)",
    )
    .execute(&pool)
    .await
    .unwrap();
    assert_eq!(deleted.rows_affected(), 1);

    let error = connect_queue(pool.clone()).await.unwrap_err();
    assert!(matches!(error, Error::Migration(_)), "{error}");

    //noinspection SpellCheckingInspection
    let held_migration_locks = sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM pg_locks
         WHERE locktype = 'advisory' AND database = (SELECT oid FROM pg_database WHERE datname = current_database())",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(held_migration_locks, 0);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_connect_rejects_unsafe_queue_configuration(pool: PgPool) {
    for name in ["", ".", "..", "bad\nname"] {
        let err = Queue::builder("postgres://unused")
            .pool(pool.clone())
            .name(name)
            .connect()
            .await
            .expect_err("queue name should be rejected");
        assert!(matches!(err, Error::Config(_)), "{name:?}: {err}");
    }
    let dotted = Queue::builder("postgres://unused").pool(pool.clone()).name("jobs.v2").connect().await.unwrap();
    assert_eq!(dotted.name(), "jobs.v2");
    let builder = Queue::builder("postgres://unused").pool(pool.clone()).priorities(1, -1);
    assert!(matches!(builder.connect().await, Err(Error::Config(_))));
    // A zero sweep grace collapses the recovery cushion the knob exists to size.
    let zero_grace = Queue::builder("postgres://unused")
        .pool(pool)
        .sweep_grace(Duration::ZERO)
        .connect()
        .await
        .expect_err("a zero sweep grace should be rejected");
    assert!(matches!(&zero_grace, Error::Config(message) if message.contains("sweep grace")), "{zero_grace}");
    for builder in [
        Queue::builder("postgres://unused").connections(2, 1),
        Queue::builder("postgres://unused").connections(0, 0),
    ] {
        assert!(matches!(builder.connect().await, Err(Error::Config(_))));
    }
}

#[sqlx::test(migrations = "./migrations")]
async fn test_enqueue_rejects_values_that_break_database_arithmetic(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let invalid = [
        with_config("zero-max-attempts", |config| config.max_attempts = 0),
        with_config("too-many-max-attempts", |config| config.max_attempts = u32::MAX),
        with_config("zero-timeout", |config| config.timeout = Some(Duration::ZERO)),
        with_config("zero-delay-backoff", |config| config.backoff = JobRetryBackoff::Exponential { max: None }),
        with_config("zero-max-backoff", |config| {
            config.retry_delay = Duration::from_millis(1);
            config.backoff = JobRetryBackoff::Exponential { max: Some(Duration::ZERO) };
        }),
        with_config("huge-delay", |config| config.retry_delay = Duration::MAX),
        new_job("", |_| {}),
        new_job("nul", |job| job.dedupe_key = Some("bad\0key".into())),
        new_job("long-dedupe", |job| job.dedupe_key = Some("x".repeat(256))),
    ];
    for job in invalid {
        assert!(
            matches!(db.queue.enqueue_raw(job).await, Err(Error::Config(_))),
            "invalid job must fail before reaching PostgreSQL"
        );
    }
    assert_eq!(db.queue.counts().await.unwrap().queued, 0);
}

/// The 255-byte limits above bind only Rust writers, and foreign SQL writers
/// exist by design (the enqueue advisory lock is opt-in). Without the DDL
/// checks an oversized dedupe key was not even refused: the dedupe index is
/// partial, so a terminal row carrying one landed silently, and the first
/// retry to copy that key onto a live row failed forever with a raw B-tree
/// `index row size exceeds` error. The columns now refuse at insert, at the
/// same byte counts as the Rust validators.
#[sqlx::test(migrations = "./migrations")]
async fn test_length_checks_refuse_oversized_foreign_writes(pool: PgPool) {
    let insert = "INSERT INTO pgqueue.jobs (queue, name, dedupe_key, status, max_attempts)
                  VALUES ($1, $2, $3, $4, 1)";

    // At the limit every column is still accepted — the checks must not be
    // tighter than the validators — and an empty dedupe key stays legal
    // because no Rust writer refuses one.
    for dedupe_key in [Some("k".repeat(255)), Some(String::new()), None] {
        sqlx::query(insert)
            .bind("q".repeat(255))
            .bind("n".repeat(255))
            .bind(&dedupe_key)
            .bind("queued")
            .execute(&pool)
            .await
            .unwrap_or_else(|error| panic!("{dedupe_key:?} must be accepted: {error}"));
        sqlx::query("DELETE FROM pgqueue.jobs WHERE TRUE").execute(&pool).await.unwrap();
    }

    // One byte over — including 128 two-byte characters, because the limits
    // are `octet_length` bytes exactly as `str::len` counts them — and the
    // empty values `JobRequest::validate` and `validate_queue_name` refuse.
    // `status = 'failed'` on the dedupe cases is the write the partial index
    // used to wave through.
    let rejected = [
        ("q".repeat(256), "n".into(), None, "jobs_queue_check"),
        (String::new(), "n".into(), None, "jobs_queue_check"),
        ("q".into(), "n".repeat(256), None, "jobs_name_check"),
        ("q".into(), "é".repeat(128), None, "jobs_name_check"),
        ("q".into(), String::new(), None, "jobs_name_check"),
        ("q".into(), "n".into(), Some("k".repeat(256)), "jobs_dedupe_key_check"),
        ("q".into(), "n".into(), Some("é".repeat(128)), "jobs_dedupe_key_check"),
    ];
    for (queue, name, dedupe_key, constraint) in rejected {
        let error = sqlx::query(insert)
            .bind(&queue)
            .bind(&name)
            .bind(&dedupe_key)
            .bind(if dedupe_key.is_some() { "failed" } else { "queued" })
            .execute(&pool)
            .await
            .unwrap_err();
        assert_eq!(
            error.as_database_error().and_then(|error| error.constraint()),
            Some(constraint),
            "queue {} / name {} / dedupe key {:?} must be refused: {error}",
            queue.len(),
            name.len(),
            dedupe_key.as_deref().map(str::len),
        );
    }
}

#[sqlx::test(migrations = "./migrations")]
async fn test_error_length_check_refuses_oversized_foreign_writes(pool: PgPool) {
    let insert = "INSERT INTO pgqueue.jobs (queue, name, status, error)
                  VALUES ('default', 'foreign-error', 'failed', $1)";

    sqlx::query(insert).bind("x".repeat(1_048_576)).execute(&pool).await.unwrap();
    let error = sqlx::query(insert).bind("é".repeat(524_289)).execute(&pool).await.unwrap_err();

    assert_eq!(error.as_database_error().and_then(|error| error.constraint()), Some("jobs_error_check"));
}

#[sqlx::test(migrations = "./migrations")]
async fn test_enqueue_round_trips_all_fields(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let result = db
        .queue
        .enqueue_raw(new_job("send_email", |job| {
            job.payload = json!({"to": "a@b.c"});
            job.meta = json!({"trace": "xyz"});
            job.config.priority = -3;
            job.config.max_attempts = 5;
            job.config.timeout = Some(Duration::from_secs(30));
            job.config.retry_delay = Duration::from_millis(250);
            job.config.backoff = JobRetryBackoff::Exponential { max: Some(Duration::from_secs(60)) };
            job.config.retention = JobRetention::For(Duration::from_secs(3600));
        }))
        .await
        .unwrap();
    assert!(result.is_enqueued());
    let id = result.job_id();

    let row = db.queue.fetch_job(id).await.unwrap().expect("job exists");
    assert_eq!(row.id, id);
    assert_eq!(row.queue, "default");
    assert_eq!(row.name, "send_email");
    assert_eq!(row.payload, json!({"to": "a@b.c"}));
    assert_eq!(row.meta, json!({"trace": "xyz"}));
    assert_eq!(row.status, JobStatus::Queued);
    assert_eq!(row.priority, -3);
    assert_eq!(row.attempts, 0);
    assert_eq!(row.max_attempts, 5);
    assert_eq!(row.timeout(), Some(Duration::from_secs(30)));
    assert_eq!(row.retry_delay_ms, 250);
    assert_eq!(row.backoff, JobRetryBackoff::Exponential { max: Some(Duration::from_secs(60)) });
    assert_eq!(row.retention(), JobRetention::For(Duration::from_secs(3600)));
    assert!(row.is_retryable());
    assert!(row.started_at.is_none());
    assert!(row.completed_at.is_none());
    assert!(row.result.is_none());
    assert!(row.error.is_none());
    assert!(row.worker_id.is_none());
    assert!(row.dedupe_key.is_none());
}

#[sqlx::test(migrations = "./migrations")]
async fn test_enqueue_raw_in_obeys_the_caller_transaction(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let mut committed = db.queue.pool().begin().await.unwrap();
    let first = db.queue.enqueue_raw_in(&mut committed, new_job("committed-a", |_| {})).await.unwrap().into_job_id();
    let second = db.queue.enqueue_raw_in(&mut committed, new_job("committed-b", |_| {})).await.unwrap().into_job_id();
    assert!(db.queue.fetch_job(first).await.unwrap().is_none());
    assert!(db.queue.fetch_job(second).await.unwrap().is_none());
    committed.commit().await.unwrap();
    assert!(db.queue.fetch_job(first).await.unwrap().is_some());
    assert!(db.queue.fetch_job(second).await.unwrap().is_some());

    let mut rolled_back = db.queue.pool().begin().await.unwrap();
    let discarded =
        db.queue.enqueue_raw_in(&mut rolled_back, new_job("discarded", |_| {})).await.unwrap().into_job_id();
    rolled_back.rollback().await.unwrap();
    assert!(db.queue.fetch_job(discarded).await.unwrap().is_none());
}

/// The advisory lock the enqueue takes binds only writers that take it too, so
/// a row committed straight into `pgqueue.jobs` can claim the key between the
/// guarded read and the insert. `READ COMMITTED` sees that row on the next
/// statement, so the collision is reported the way every other collision is —
/// with the live holder's id.
#[sqlx::test(migrations = "./migrations")]
async fn test_enqueue_raw_in_deduplicates_against_a_dedupe_owner_committed_out_of_band(pool: PgPool) {
    const INSERT_GATE: i32 = 20_571;

    let control = TestDb::new(crate::pool_with_max(&pool, 3).await).await;
    let db = TestDb::new(crate::pool_with_max(&pool, 2).await).await;
    // Pause the caller transaction between its visibility read and its insert.
    install_statement_gate(
        control.queue.pool(),
        "wait_at_job_insert",
        INSERT_GATE,
        "INSERT",
        "NEW.name = 'gated-insert'",
    )
    .await;
    let gate = hold_gate(control.queue.pool(), INSERT_GATE, &control.database).await;

    let queue = db.queue.clone();
    let pool_for_caller = db.queue.pool().clone();
    let enqueue = tokio::spawn(async move {
        let mut caller = pool_for_caller.begin().await.unwrap();
        let result = queue
            .enqueue_raw_in(
                &mut caller,
                new_job("gated-insert", |job| {
                    job.dedupe_key = Some("snapshot:key".into());
                }),
            )
            .await;
        caller.rollback().await.unwrap();
        result
    });
    wait_for_lock_waiter(&control, "%WITH inserted AS (%", "transactional enqueue did not reach its insert").await;

    // A row that takes the dedupe key without going through the enqueue
    // advisory lock: the caller's snapshot cannot see it, but the partial unique
    // index still refuses the insert.
    let owner = sqlx::query_scalar::<_, Uuid>(
        r#"INSERT INTO pgqueue.jobs (queue, name, payload, dedupe_key, status, max_attempts)
           VALUES ($1, 'out-of-band', 'null'::jsonb, 'snapshot:key', 'queued', 1)
           RETURNING id"#,
    )
    .bind(control.queue.name())
    .fetch_one(control.queue.pool())
    .await
    .unwrap();
    gate.rollback().await.unwrap();

    let result = enqueue.await.unwrap().expect("a dedupe collision is not an error");
    assert!(result.is_deduplicated(), "the key was already taken");
    assert_eq!(result.into_job_id(), owner, "the collision must name the live holder of the key");
}

/// The other half of the out-of-band collision above: the foreign row that
/// blocked the insert is gone again by the time the collision is re-read, so
/// there is no live holder to report. Nothing about the request is invalid —
/// retrying the same enqueue succeeds — so the failure must stay
/// distinguishable from a permanent configuration error.
#[sqlx::test(migrations = "./migrations")]
async fn test_enqueue_raw_reports_a_vanished_out_of_band_dedupe_owner_as_retryable(pool: PgPool) {
    const INSERT_GATE: i32 = 20_572;
    const CONFLICT_GATE: i32 = 20_573;

    let control = TestDb::new(crate::pool_with_max(&pool, 4).await).await;
    let db = TestDb::new(crate::pool_with_max(&pool, 2).await).await;
    // Pause the enqueue between its visibility read and its insert...
    install_statement_gate(
        control.queue.pool(),
        "wait_at_job_insert",
        INSERT_GATE,
        "INSERT",
        "NEW.name = 'gated-insert'",
    )
    .await;
    // ...and between its conflict decision and its collision re-read.
    crate::install_conflicted_insert_gate(control.queue.pool(), "wait_at_conflict", CONFLICT_GATE).await;
    let insert_gate = hold_gate(control.queue.pool(), INSERT_GATE, &control.database).await;
    let conflict_gate = hold_gate(control.queue.pool(), CONFLICT_GATE, &control.database).await;

    let queue = db.queue.clone();
    let enqueue = tokio::spawn(async move {
        queue
            .enqueue_raw(new_job("gated-insert", |job| {
                job.dedupe_key = Some("vanishing:key".into());
            }))
            .await
    });
    wait_for_lock_waiter(&control, "%WITH inserted AS (%", "enqueue did not reach its insert").await;

    // A row that takes the dedupe key without the enqueue advisory lock...
    let owner = sqlx::query_scalar::<_, Uuid>(
        r#"INSERT INTO pgqueue.jobs (queue, name, payload, dedupe_key, status, max_attempts)
           VALUES ($1, 'out-of-band', 'null'::jsonb, 'vanishing:key', 'queued', 1)
           RETURNING id"#,
    )
    .bind(control.queue.name())
    .fetch_one(control.queue.pool())
    .await
    .unwrap();
    insert_gate.rollback().await.unwrap();
    crate::wait_for_advisory_waiter(
        control.queue.pool(),
        CONFLICT_GATE,
        "conflicted enqueue did not reach its collision re-read",
    )
    .await;
    // ...and releases it again before the enqueue can name it.
    sqlx::query("DELETE FROM pgqueue.jobs WHERE id = $1").bind(owner).execute(control.queue.pool()).await.unwrap();
    conflict_gate.rollback().await.unwrap();

    let error = enqueue.await.unwrap().unwrap_err();
    assert!(
        matches!(&error, Error::DedupeRace(message) if message.contains("released again")),
        "a transient dedupe race must not look like invalid input: {error:?}"
    );

    // The race is transient: the identical request enqueues cleanly afterwards.
    let retried = db
        .queue
        .enqueue_raw(new_job("gated-insert", |job| {
            job.dedupe_key = Some("vanishing:key".into());
        }))
        .await
        .unwrap();
    assert!(retried.is_enqueued(), "retrying the raced enqueue must succeed");
}

#[sqlx::test(migrations = "./migrations")]
async fn test_transactional_dedupe_does_not_lock_the_existing_job_row(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let dedupe_job = || new_job("deduplicated", |job| job.dedupe_key = Some("transactional-dedupe".into()));
    let id = db.queue.enqueue_raw(dedupe_job()).await.unwrap().unwrap();
    let active = db.queue.dequeue(1, Uuid::now_v7()).await.unwrap().remove(0);

    let mut transaction = db.queue.pool().begin().await.unwrap();
    let result = db.queue.enqueue_raw_in(&mut transaction, dedupe_job()).await.unwrap();
    assert_eq!(result.job_id(), id);
    assert!(matches!(
        result,
        EnqueueResult::Deduplicated(existing) if existing == id
    ));

    let queue = db.queue.clone();
    let finishing =
        tokio::spawn(async move { queue.finish(&active, JobStatus::Complete, Some(json!("done")), None).await });
    assert!(
        tokio::time::timeout(Duration::from_secs(1), finishing)
            .await
            .expect("dedupe read held a row lock until caller commit")
            .unwrap()
            .unwrap()
    );
    transaction.rollback().await.unwrap();
    assert_eq!(db.queue.fetch_job(id).await.unwrap().unwrap().status, JobStatus::Complete);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_enqueue_raw_in_returns_serialization_failure_for_invisible_dedupe_owner(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let dedupe_job = || new_job("snapshot-dedupe", |job| job.dedupe_key = Some("snapshot-dedupe".into()));
    let mut transaction = db.queue.pool().begin().await.unwrap();
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ").execute(&mut *transaction).await.unwrap();
    sqlx::query_scalar::<_, i32>(r#"SELECT 1"#).fetch_one(&mut *transaction).await.unwrap();

    db.queue.enqueue_raw(dedupe_job()).await.unwrap();
    let error = db.queue.enqueue_raw_in(&mut transaction, dedupe_job()).await.unwrap_err();
    match error {
        Error::Db(sqlx::Error::Database(error)) => {
            assert_eq!(error.code().as_deref(), Some("40001"));
        }
        other => panic!("expected PostgreSQL serialization failure, got {other}"),
    }
    transaction.rollback().await.unwrap();
}

#[sqlx::test(migrations = "./migrations")]
async fn test_enqueue_rounds_nonzero_fractional_milliseconds_up(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db
        .queue
        .enqueue_raw(new_job("precise", |job| {
            job.config.timeout = Some(Duration::from_micros(500));
            job.config.retry_delay = Duration::from_nanos(1);
            job.config.retention = JobRetention::For(Duration::from_micros(1_500));
            job.config.backoff = JobRetryBackoff::Exponential { max: Some(Duration::from_micros(500)) };
        }))
        .await
        .unwrap()
        .unwrap();

    let row = db.queue.fetch_job(id).await.unwrap().unwrap();
    assert_eq!(row.timeout_ms, Some(1));
    assert_eq!(row.retry_delay_ms, 1);
    assert_eq!(row.result_ttl_ms, Some(2));
    assert_eq!(row.backoff, JobRetryBackoff::Exponential { max: Some(Duration::from_millis(1)) });
}

//noinspection SqlNoDataSourceInspection
#[sqlx::test(migrations = "./migrations")]
async fn test_stored_backoff_without_max_ms_still_decodes_and_dequeues(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db.queue.enqueue_raw(new_job("external", |_| {})).await.unwrap().unwrap();
    // An external client (ops script, manual UPDATE) may store the
    // exponential variant without a max_ms key; the row must not poison
    // every dequeue batch that selects it.
    sqlx::query(r#"UPDATE pgqueue.jobs SET backoff = '{"type":"exponential"}'::jsonb WHERE id = $1"#)
        .bind(id)
        .execute(db.queue.pool())
        .await
        .unwrap();

    let row = db.queue.fetch_job(id).await.unwrap().unwrap();
    assert_eq!(row.backoff, JobRetryBackoff::Exponential { max: None });
    let active = db.queue.dequeue(1, Uuid::now_v7()).await.unwrap().remove(0);
    assert_eq!(active.id, id);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_missing_job_operations_return_their_documented_results(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    assert!(db.queue.fetch_job(Uuid::now_v7()).await.unwrap().is_none());
    assert!(!db.queue.abort_job(Uuid::now_v7(), "x").await.unwrap());
    assert!(!db.queue.retry_job(Uuid::now_v7(), "x").await.unwrap());
    assert!(matches!(db.queue.dequeue(0, Uuid::now_v7()).await, Err(Error::Config(_))));
    assert!(matches!(
        db.queue.jobs_page(JobFilter { limit: Some(-1), ..JobFilter::default() }).await,
        Err(Error::Config(_))
    ));
    assert!(matches!(
        db.queue.jobs_page(JobFilter { limit: Some(1001), ..JobFilter::default() }).await,
        Err(Error::Config(_))
    ));
}

#[sqlx::test(migrations = "./migrations")]
async fn test_dedupe_key_dedupes_live_jobs_and_preserves_terminal_occurrences(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db
        .queue
        .enqueue_raw(new_job("cron_job", |job| {
            job.dedupe_key = Some("cron:cron_job".into());
        }))
        .await
        .unwrap()
        .expect("first enqueue");
    let t0 = db.queue.fetch_job(id).await.unwrap().unwrap().scheduled_at;
    let enqueue_at = |at| {
        new_job("cron_job", move |job| {
            job.dedupe_key = Some("cron:cron_job".into());
            job.scheduled_at = Some(at);
        })
    };
    // Live job with the same key: dedupe hit.
    assert!(db.queue.enqueue_raw(enqueue_at(t0 + SignedDuration::from_secs(5))).await.unwrap().is_none());

    // Finish it, then re-enqueue with a later schedule: a new occurrence gets
    // a new ID while the first result remains addressable.
    let worker = Uuid::now_v7();
    let jobs = db.queue.dequeue(1, worker).await.unwrap();
    assert_eq!(jobs.len(), 1);
    db.queue.finish(&jobs[0], JobStatus::Complete, None, None).await.unwrap();

    let second = db
        .queue
        .enqueue_raw(enqueue_at(t0 + SignedDuration::from_secs(1)))
        .await
        .unwrap()
        .expect("enqueue after terminal occurrence");
    assert_ne!(second, id, "each occurrence needs a stable ID");
    assert_eq!(db.queue.fetch_job(id).await.unwrap().unwrap().status, JobStatus::Complete);
    let row = db.queue.fetch_job(second).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Queued);
    assert_eq!(row.attempts, 0, "revive resets attempts");
    assert!(row.result.is_none());

    // Finish again; ordinary deduplicated jobs enqueue even with an earlier/equal
    // schedule. Only the cron-specific enqueue path applies occurrence ordering.
    sqlx::query("UPDATE pgqueue.jobs SET scheduled_at = now() WHERE id = $1")
        .bind(second)
        .execute(db.queue.pool())
        .await
        .unwrap();
    let jobs = db.queue.dequeue(1, worker).await.unwrap();
    assert_eq!(jobs.len(), 1, "revived occurrence is due by now");
    db.queue.finish(&jobs[0], JobStatus::Complete, None, None).await.unwrap();
    let third = db
        .queue
        .enqueue_raw(enqueue_at(t0))
        .await
        .unwrap()
        .expect("terminal dedupe key can be reused regardless of schedule");
    assert_ne!(third, id);
    assert_ne!(third, second);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_concurrent_dedupe_enqueues_accept_exactly_one_live_occurrence(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let mut enqueues = tokio::task::JoinSet::new();
    for _ in 0..100 {
        let queue = db.queue.clone();
        enqueues.spawn(async move {
            queue
                .enqueue_raw(new_job("singleton", |job| {
                    job.dedupe_key = Some("contended-key".into());
                }))
                .await
                .unwrap()
        });
    }
    let mut accepted = Vec::new();
    while let Some(result) = enqueues.join_next().await {
        if let EnqueueResult::Enqueued(id) = result.unwrap() {
            accepted.push(id);
        }
    }
    assert_eq!(accepted.len(), 1);
    assert_eq!(db.queue.counts().await.unwrap().queued, 1);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_retry_job_occurrence_uses_wall_clock_after_dedupe_lock_contention(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db
        .queue
        .enqueue_raw(new_job("retry-contended", |job| {
            job.dedupe_key = Some("retry-contended".into());
        }))
        .await
        .unwrap()
        .unwrap();
    assert!(db.queue.abort_job(id, "terminal").await.unwrap());

    let mut lock_tx = db.queue.pool().begin().await.unwrap();
    sqlx::query("SELECT pg_advisory_xact_lock($1, hashtext(length($2)::text || ':' || $2 || $3))")
        .bind(pgqueue::__test_support::dedupe_enqueue_lock_key(&db.database))
        .bind(db.queue.name())
        .bind("retry-contended")
        .execute(&mut *lock_tx)
        .await
        .unwrap();

    let queue = db.queue.clone();
    let mut retrying = tokio::spawn(async move { queue.retry_job_occurrence(id, "manual retry").await });
    crate::wait_for_dequeue_lock_waiter(&db.queue, true).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut retrying).await.is_err(),
        "retry did not wait for the dedupe-key lock"
    );
    let released_at = sqlx::query_scalar::<_, jiff_sqlx::Timestamp>(r#"SELECT clock_timestamp()"#)
        .fetch_one(db.queue.pool())
        .await
        .unwrap()
        .to_jiff();
    lock_tx.rollback().await.unwrap();

    let retry_id = tokio::time::timeout(Duration::from_secs(5), retrying).await.unwrap().unwrap().unwrap().unwrap();
    let row = db.queue.fetch_job(retry_id).await.unwrap().unwrap();
    assert!(row.scheduled_at >= released_at);
    assert!(row.enqueued_at >= released_at);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_aborted_future_scheduled_deduplicated_jobs_can_be_reenqueued(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    // A deduplicated job scheduled for tomorrow, aborted while still queued: the
    // terminal row keeps its future schedule.
    let id = db
        .queue
        .enqueue_raw(new_job("report", |job| {
            job.dedupe_key = Some("report:x".into());
            job.scheduled_at = Some(Timestamp::now() + SignedDuration::from_hours(24));
        }))
        .await
        .unwrap()
        .unwrap();
    assert!(db.queue.abort_job(id, "changed plans").await.unwrap());

    // Re-enqueueing the key to run now must create a new occurrence, not no-op
    // until tomorrow.
    let next = db
        .queue
        .enqueue_raw(new_job("report", |job| {
            job.dedupe_key = Some("report:x".into());
        }))
        .await
        .unwrap()
        .expect("dead future-scheduled key must be reusable");
    assert_ne!(next, id);
    assert_eq!(db.queue.fetch_job(id).await.unwrap().unwrap().status, JobStatus::Aborted);
    let row = db.queue.fetch_job(next).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Queued);
    assert!(row.scheduled_at <= Timestamp::now() + SignedDuration::from_secs(1));
}

#[sqlx::test(migrations = "./migrations")]
async fn test_dequeue_orders_by_priority_then_schedule(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    for priority in [1i16, -1, 0] {
        db.queue.enqueue_raw(with_config("prio", |c| c.priority = priority)).await.unwrap().unwrap();
    }
    let jobs = db.queue.dequeue(10, Uuid::now_v7()).await.unwrap();
    let got: Vec<i16> = jobs.iter().map(|j| j.priority).collect();
    assert_eq!(got, vec![-1, 0, 1]);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_dequeue_marks_rows_active(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    db.queue.enqueue_raw(new_job("j", |_| {})).await.unwrap();
    let worker = Uuid::now_v7();
    let jobs = db.queue.dequeue(5, worker).await.unwrap();
    assert_eq!(jobs.len(), 1);
    let row = &jobs[0];
    assert_eq!(row.status, JobStatus::Running);
    assert_eq!(row.attempts, 1);
    assert_eq!(row.worker_id, Some(worker));
    assert!(row.started_at.is_some());
    assert!(row.touched_at.is_some());

    // Nothing left.
    assert!(db.queue.dequeue(5, worker).await.unwrap().is_empty());
}

#[sqlx::test(migrations = "./migrations")]
async fn test_dequeue_rolls_back_every_claim_when_a_row_cannot_decode(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    sqlx::query(
        "ALTER TABLE pgqueue.jobs
         DROP CONSTRAINT jobs_timestamps_jiff_range_check",
    )
    .execute(db.queue.pool())
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO pgqueue.jobs (queue, name, enqueued_at)
         VALUES ($1, 'decode-invalid', TIMESTAMPTZ '10000-01-01 00:00:00+00'),
                ($1, 'decode-valid', clock_timestamp())",
    )
    .bind(db.queue.name())
    .execute(db.queue.pool())
    .await
    .unwrap();

    let error = db.queue.dequeue(2, Uuid::now_v7()).await.unwrap_err();
    assert!(matches!(error, Error::Db(sqlx::Error::ColumnDecode { .. })));

    let rows = sqlx::query_as::<_, (String, String, i32, Option<Uuid>)>(
        "SELECT name, status, attempts, worker_id
         FROM pgqueue.jobs
         WHERE queue = $1 AND name LIKE 'decode-%'
         ORDER BY name",
    )
    .bind(db.queue.name())
    .fetch_all(db.queue.pool())
    .await
    .unwrap();
    assert_eq!(
        rows,
        [
            ("decode-invalid".to_owned(), "queued".to_owned(), 0, None),
            ("decode-valid".to_owned(), "queued".to_owned(), 0, None),
        ]
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn test_consumer_heartbeat_and_finish_use_guarded_capabilities(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let worker_id = Uuid::now_v7();
    let consumer = db.queue.consumer(worker_id);
    consumer.heartbeat(json!({"complete": 0}), Some(json!({"kind": "custom"})), Duration::from_secs(30)).await.unwrap();
    let id = db.queue.enqueue_raw(new_job("custom-consumer", |_| {})).await.unwrap().unwrap();

    let attempts = consumer.dequeue(1).await.unwrap();
    assert_eq!(attempts.len(), 1);
    let attempt = attempts.into_iter().next().unwrap();
    assert_eq!(attempt.job().id, id);
    assert_eq!(attempt.job().worker_id, Some(worker_id));
    assert!(attempt.finish(JobStatus::Complete, Some(json!("ok")), None).await.unwrap());

    let workers = list_workers(&db.queue).await;
    assert!(workers.iter().any(|worker| worker.id == worker_id));
    assert_eq!(db.queue.fetch_job(id).await.unwrap().unwrap().status, JobStatus::Complete);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_consumer_attempt_finish_can_retry_after_a_pool_timeout(pool: PgPool) {
    let constrained = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(1))
        .connect_with(pool.connect_options().as_ref().clone())
        .await
        .unwrap();
    let db = TestDb::new(constrained).await;
    let id = db.queue.enqueue_raw(new_job("retry-finish", |_| {})).await.unwrap().unwrap();
    let attempt =
        leased_consumer(&db.queue, Uuid::now_v7()).await.dequeue(1).await.unwrap().into_iter().next().unwrap();

    let connection = db.pool.acquire().await.unwrap();
    assert!(matches!(
        attempt.finish(JobStatus::Complete, Some(json!("ok")), None).await,
        Err(Error::Db(sqlx::Error::PoolTimedOut))
    ));
    drop(connection);

    assert!(attempt.finish(JobStatus::Complete, Some(json!("ok")), None).await.unwrap());
    assert_eq!(db.queue.fetch_job(id).await.unwrap().unwrap().status, JobStatus::Complete);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_consumer_attempt_retry_refusal_leaves_the_attempt_finishable(pool: PgPool) {
    let db = TestDb::new(pool).await;
    let id = db
        .queue
        .enqueue_raw(with_config("refused-retry", |config| {
            config.max_attempts = 1;
        }))
        .await
        .unwrap()
        .unwrap();
    let attempt =
        leased_consumer(&db.queue, Uuid::now_v7()).await.dequeue(1).await.unwrap().into_iter().next().unwrap();

    assert!(!attempt.retry("attempt failed").await.unwrap());
    assert!(attempt.finish(JobStatus::Failed, None, Some("attempt failed")).await.unwrap());
    assert_eq!(db.queue.fetch_job(id).await.unwrap().unwrap().status, JobStatus::Failed);
}

/// A sweeper recovery request that lands mid-attempt is a retry request, and a
/// consumer holding the attempt capability must be able to answer it as one:
/// `Attempt::retry` converts the marked row back to `queued` under the
/// consumer's own error, exactly as the worker's finalization does — without
/// any worker-private transition. A *user* abort stays out of reach: `retry`
/// refuses it, and the acknowledgment is a finish as aborted.
#[sqlx::test(migrations = "./migrations")]
async fn test_consumer_retry_converts_a_sweeper_recovery_request_into_its_retry(pool: PgPool) {
    let db = TestDb::new(pool).await;
    let id = db
        .queue
        .enqueue_raw(with_config("consumer-swept", |config| {
            config.max_attempts = 3;
            config.timeout = Some(Duration::from_millis(20));
        }))
        .await
        .unwrap()
        .unwrap();
    let consumer = leased_consumer(&db.queue, Uuid::now_v7()).await;
    let attempt = consumer.dequeue(1).await.unwrap().into_iter().next().unwrap();
    backdate_job_liveness(&db, id).await;
    // The sweeper's phase one marks the stuck attempt for abort; the live
    // lease keeps phase two off it.
    let mut sweeper = db.queue.sweeper();
    sweeper.sweep().await.unwrap();
    sweeper.release().await;
    assert_eq!(db.queue.fetch_job(id).await.unwrap().unwrap().status, JobStatus::Aborting);

    assert!(attempt.retry("failed: handler error").await.unwrap(), "a recovery request is a retry request");
    let row = db.queue.fetch_job(id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Queued);
    assert_eq!(row.attempts, 1);
    assert_eq!(row.error.as_deref(), Some("failed: handler error"), "the consumer's reason replaces the marker");
    assert_eq!(row.result, None, "the marker pair must not survive the requeue");

    // A user abort is terminal for the attempt: never a retry, only an
    // acknowledgment.
    let attempt = consumer.dequeue(1).await.unwrap().into_iter().next().unwrap();
    assert!(db.queue.abort_job(id, "operator").await.unwrap());
    assert!(!attempt.retry("failed: too late").await.unwrap(), "a user abort must not be resurrected");
    assert!(attempt.finish(JobStatus::Aborted, None, None).await.unwrap(), "the abort is acknowledged instead");
    assert_eq!(db.queue.fetch_job(id).await.unwrap().unwrap().status, JobStatus::Aborted);
}

/// A JSON document of `depth` nested objects wrapped around `null`.
fn nested_json(depth: usize) -> serde_json::Value {
    let mut value = serde_json::Value::Null;
    for _ in 0..depth {
        value = json!({ "a": value });
    }
    value
}

/// `json_contains_nul` guarded the one value `jsonb` cannot *hold*, but nothing
/// guarded the values `serde_json` cannot *read back*: its deserializer stops at
/// 128 nested containers, and every read of `payload`, `meta` and `result` goes
/// through it. So the crate accepted, wrote and acknowledged a document it could
/// never decode again.
///
/// The damage was not confined to the bad row. The dequeue decodes its batch
/// inside the claiming transaction, so one undecodable row failed the whole
/// batch's decode, rolled the claim back — and was re-selected by the next
/// dequeue, which failed identically: every job queued behind it was blocked,
/// claim after claim. `fetch_job`, `jobs_page` and
/// the dashboard listing failed for the entire queue for as long as the row was
/// retained.
#[sqlx::test(migrations = "./migrations")]
async fn test_enqueue_refuses_json_nested_deeper_than_it_can_be_read_back(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;

    // 127 is the deepest `serde_json` decodes, so it must still be accepted:
    // an off-by-one here silently narrows the documented payload space.
    let deepest = nested_json(127);
    let id = db
        .queue
        .enqueue_raw(new_job("deep-ok", |job| {
            job.payload = deepest.clone();
            job.meta = deepest.clone();
        }))
        .await
        .unwrap()
        .unwrap();
    let row = db.queue.fetch_job(id).await.unwrap().unwrap();
    assert_eq!(row.payload, deepest, "the deepest legal payload must survive");
    assert_eq!(row.meta, deepest);
    // The read path the poison row broke for the whole queue.
    let claimed = leased_consumer(&db.queue, Uuid::now_v7()).await.dequeue(10).await.unwrap();
    assert_eq!(claimed.len(), 1);

    for (field, customize) in [
        (
            "job payload",
            Box::new(|job: &mut pgqueue::JobRequest| job.payload = nested_json(128))
                as Box<dyn FnOnce(&mut pgqueue::JobRequest)>,
        ),
        ("job meta", Box::new(|job: &mut pgqueue::JobRequest| job.meta = nested_json(200))),
    ] {
        let refused = db.queue.enqueue_raw(new_job("too-deep", customize)).await.unwrap_err();
        assert!(
            matches!(&refused, Error::Config(message)
                if *message == format!("{field} must not nest deeper than 127 levels")),
            "{field}: {refused:?}"
        );
    }
    // Refused before the write, not after it: nothing named `too-deep` exists.
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM pgqueue.jobs WHERE queue = $1 AND name = 'too-deep'")
            .bind(db.queue.name())
            .fetch_one(db.queue.pool())
            .await
            .unwrap(),
        0,
        "a payload that can never be decoded must not reach the table"
    );

    // The same limit on the finalization value, which the consumer API reaches.
    let attempt = claimed.into_iter().next().unwrap();
    let refused = attempt.finish(JobStatus::Complete, Some(nested_json(128)), None).await.unwrap_err();
    assert!(
        matches!(&refused, Error::Config(message)
            if message == "job result must not nest deeper than 127 levels"),
        "{refused:?}"
    );
    // Refused, not spent: the attempt is still the caller's to finalize.
    assert!(attempt.finish(JobStatus::Complete, Some(nested_json(127)), None).await.unwrap());
    assert_eq!(db.queue.fetch_job(id).await.unwrap().unwrap().result, Some(nested_json(127)));
}

/// The depth guard was added to the four writers that reach `jobs`, but
/// `Consumer::heartbeat` — the public lease writer of the custom-consumer API —
/// validated only its TTL, so `stats` and `metadata` went into
/// `pgqueue.workers.jsonb` unchecked. A NUL there *fails* the write (`22P05`);
/// excess depth *succeeds* and breaks the reads, and not only for the offending
/// lease: the old unpaged worker read decoded every live lease in one statement,
/// so both public and dashboard worker views failed for the whole queue as long
/// as the consumer kept renewing it.
#[sqlx::test(migrations = "./migrations")]
async fn test_consumer_heartbeat_refuses_json_nested_deeper_than_it_can_be_read_back(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let worker_id = Uuid::now_v7();
    let consumer = db.queue.consumer(worker_id);

    for (field, stats, metadata) in [
        ("worker stats", nested_json(128), None),
        ("worker metadata", json!({}), Some(nested_json(200))),
    ] {
        let refused = consumer.heartbeat(stats, metadata, Duration::from_secs(30)).await.unwrap_err();
        assert!(
            matches!(&refused, Error::Config(message)
                if *message == format!("{field} must not nest deeper than 127 levels")),
            "{field}: {refused:?}"
        );
    }
    // Refused before the write, not after it: no lease exists to poison the
    // queue's worker reads.
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM pgqueue.workers WHERE id = $1")
            .bind(worker_id)
            .fetch_one(db.queue.pool())
            .await
            .unwrap(),
        0,
        "a lease that can never be decoded must not reach the table"
    );

    // 127 is the deepest `serde_json` decodes, so it must still be accepted, and
    // the read that a deeper one broke must still answer.
    let deepest = nested_json(127);
    consumer.heartbeat(deepest.clone(), Some(deepest.clone()), Duration::from_secs(30)).await.unwrap();
    let workers = list_workers(&db.queue).await;
    let worker = workers.iter().find(|worker| worker.id == worker_id).expect("the accepted lease is listed");
    assert_eq!(worker.stats, deepest);
    assert_eq!(worker.metadata, Some(deepest));
}

/// The other half of the same invariant, and the half the depth guard above left
/// out: `jsonb` cannot store a NUL at all, so one in `stats` or `metadata` came
/// back as `Error::Db` (`22P05`) — indistinguishable from the pool timeout a
/// heartbeat loop is documented to run through. Retrying it renews nothing, so
/// the loop spins until the lease expires and the sweeper reclaims every attempt
/// the consumer has claimed. Every other writer of these columns already refuses
/// one locally.
#[sqlx::test(migrations = "./migrations")]
async fn test_consumer_heartbeat_refuses_a_nul_in_its_lease_values(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let worker_id = Uuid::now_v7();
    let consumer = db.queue.consumer(worker_id);

    for (field, stats, metadata) in [
        ("worker stats", json!({"queue": "bu\0sy"}), None),
        ("worker metadata", json!({}), Some(json!({"host": ["a\0b"]}))),
    ] {
        let refused = consumer.heartbeat(stats, metadata, Duration::from_secs(30)).await.unwrap_err();
        assert!(
            matches!(&refused, Error::Config(message)
                if *message == format!("{field} must not contain NUL")),
            "{field}: {refused:?}"
        );
    }
    // Refused before a connection is taken, so it cannot be read as pool
    // exhaustion either — and no lease was written.
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM pgqueue.workers WHERE id = $1")
            .bind(worker_id)
            .fetch_one(db.queue.pool())
            .await
            .unwrap(),
        0,
        "an unstorable lease must not reach the table"
    );

    // The same values without the NUL still write, so the guard refuses the
    // character and not the shape.
    consumer.heartbeat(json!({"queue": "busy"}), Some(json!({"host": ["ab"]})), Duration::from_secs(30)).await.unwrap();
    let workers = list_workers(&db.queue).await;
    let worker = workers.iter().find(|worker| worker.id == worker_id).expect("the accepted lease is listed");
    assert_eq!(worker.stats, json!({"queue": "busy"}));
    assert_eq!(worker.metadata, Some(json!({"host": ["ab"]})));
}

/// A NUL is permanently invalid, not a transient failure: `jsonb` answers one
/// with `22P05` and `text` with `22021`, both `Error::Db` — indistinguishable
/// from the pool timeout these two methods explicitly invite the caller to
/// retry. A conforming retry loop therefore spun forever and the row never left
/// `running`. Every other writer on this side of the wire already refuses one;
/// the public consumer API was the one that did not.
#[sqlx::test(migrations = "./migrations")]
async fn test_consumer_attempt_refuses_a_nul_in_a_finalization_value(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db
        .queue
        .enqueue_raw(with_config("nul-finalization", |config| {
            config.max_attempts = 2;
        }))
        .await
        .unwrap()
        .unwrap();
    let attempt =
        leased_consumer(&db.queue, Uuid::now_v7()).await.dequeue(1).await.unwrap().into_iter().next().unwrap();

    for (field, result, error) in [
        ("job result", Some(json!({"log": ["do\0ne"]})), None),
        ("job error", None, Some("fai\0led")),
    ] {
        let refused = attempt.finish(JobStatus::Failed, result, error).await.unwrap_err();
        assert!(
            matches!(&refused, Error::Config(message)
                if *message == format!("{field} must not contain NUL")),
            "{refused:?}"
        );
    }
    let refused = attempt.retry("fai\0led").await.unwrap_err();
    assert!(
        matches!(&refused, Error::Config(message)
            if message == "job error must not contain NUL"),
        "{refused:?}"
    );

    // Refused, not spent: the attempt is still the caller's to finalize.
    assert!(attempt.retry("failed").await.unwrap());
    assert_eq!(db.queue.fetch_job(id).await.unwrap().unwrap().status, JobStatus::Queued);
}

/// The two requeue paths a worker takes without going through `Database::retry`
/// have to refuse the same input it does. Both stored `error` straight into
/// `text`, so a NUL would have been `22021` — permanent, and indistinguishable
/// from the pool timeout that `finalize` retries once a second forever, pinning
/// the processor slot. Today every caller launders its reason through
/// `JobError::new`, which substitutes NULs, so the invariant held only as long
/// as three `worker.rs` call sites each remembered it.
///
/// The queue check is here too: these take a caller-supplied [`JobRow`], and one
/// from another queue would otherwise be matched by id alone.
#[sqlx::test(migrations = "./migrations")]
async fn test_sweep_and_shutdown_requeues_refuse_a_nul_reason(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let other = db.another_queue(|builder| builder.name("other")).await;
    db.queue
        .enqueue_raw(with_config("nul-requeue", |config| {
            config.max_attempts = 3;
        }))
        .await
        .unwrap()
        .unwrap();
    let attempt = db.queue.dequeue(1, Uuid::now_v7()).await.unwrap().remove(0);

    for refused in [
        db.queue.retry_swept(&attempt, Some("swe\0pt")).await.unwrap_err(),
        db.queue.requeue_shutdown(&attempt, "cancel\0led").await.unwrap_err(),
    ] {
        assert!(
            matches!(&refused, Error::Config(message)
                if message == "job error must not contain NUL"),
            "{refused:?}"
        );
    }

    for refused in [
        other.retry_swept(&attempt, Some("swept")).await.unwrap_err(),
        other.requeue_shutdown(&attempt, "cancelled").await.unwrap_err(),
        other.requeue_unhandled(&attempt).await.unwrap_err(),
    ] {
        assert!(matches!(&refused, Error::Config(message) if message.contains("belongs to queue")), "{refused:?}");
    }

    // Refused, not spent: a valid reason still requeues the same attempt.
    assert!(db.queue.requeue_shutdown(&attempt, "cancelled").await.unwrap());
    let row = db.queue.fetch_job(attempt.id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Queued);
    assert_eq!(row.error.as_deref(), Some("cancelled"));
}

/// The unhandled-claim requeue: a worker that claimed a job it registers no
/// handler for gives the attempt back with a refund and a delay. The refund is
/// the load-bearing half — at the default `max_attempts = 1`, a bounce that
/// spent the attempt would fail the job outright during an ordinary rolling
/// deploy — and the delay keeps the same worker from respinning the row.
#[sqlx::test(migrations = "./migrations")]
async fn test_requeue_unhandled_refunds_the_attempt_and_delays_the_row(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    db.queue.enqueue_raw(new_job("no-handler", |_| {})).await.unwrap().unwrap();
    let attempt = db.queue.dequeue(1, Uuid::now_v7()).await.unwrap().remove(0);
    assert_eq!((attempt.attempts, attempt.max_attempts), (1, 1));

    let before = jiff::Timestamp::now();
    assert!(db.queue.requeue_unhandled(&attempt).await.unwrap());

    let row = db.queue.fetch_job(attempt.id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Queued);
    assert_eq!(row.attempts, 1, "the spent claim stays counted; the refund raises the budget instead");
    assert_eq!(row.max_attempts, 2, "the bounce must refund the attempt");
    assert_eq!(row.worker_id, None, "a bounced row belongs to nobody");
    assert_eq!(row.error.as_deref(), Some("no handler registered for job \"no-handler\""));
    assert!(
        row.scheduled_at > before + jiff::SignedDuration::from_secs(5)
            && row.scheduled_at <= before + jiff::SignedDuration::from_secs(30),
        "the bounce must delay the row by the unhandled requeue delay: scheduled_at {}",
        row.scheduled_at
    );

    // Refused once the row has moved beyond the attempt: a second bounce of
    // the same stale claim matches nothing and refunds nothing.
    assert!(!db.queue.requeue_unhandled(&attempt).await.unwrap());
    let row = db.queue.fetch_job(attempt.id).await.unwrap().unwrap();
    assert_eq!(row.max_attempts, 2, "a refused bounce must not refund again");
}

/// The two races a bounce can lose without the row moving beyond the attempt,
/// and what settles each. A sweeper phase-one mark converts into the bounce —
/// `allow_swept_abort` is load-bearing, exactly as it is for `retry` — while a
/// *user* abort refuses it, and the worker's abort fallback (`Database::finish`
/// with no error) then finishes the row `aborted` preserving the user's reason
/// rather than leaving it `aborting` under a live worker forever.
#[sqlx::test(migrations = "./migrations")]
async fn test_requeue_unhandled_converts_a_sweeper_mark_and_defers_to_a_user_abort(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let worker_id = Uuid::now_v7();
    let swept = db.queue.enqueue_raw(new_job("swept-bounce", |_| {})).await.unwrap().job_id();
    let aborted = db.queue.enqueue_raw(new_job("aborted-bounce", |_| {})).await.unwrap().job_id();
    let claimed = db.queue.dequeue(2, worker_id).await.unwrap();
    let swept_attempt = claimed.iter().find(|job| job.id == swept).unwrap().clone();
    let aborted_attempt = claimed.iter().find(|job| job.id == aborted).unwrap().clone();

    // The sweeper's phase-one mark, exactly as it writes it: the bounce
    // converts the recovery request instead of refusing it.
    sqlx::query(
        r#"UPDATE pgqueue.jobs
           SET status = 'aborting', error = 'swept', result = '"pgqueue:swept"', touched_at = now()
           WHERE id = $1"#,
    )
    .bind(swept)
    .execute(&pool)
    .await
    .unwrap();
    assert!(db.queue.requeue_unhandled(&swept_attempt).await.unwrap());
    let row = db.queue.fetch_job(swept).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Queued, "a sweeper-marked bounce must requeue, not strand the recovery");
    assert_eq!(row.max_attempts, 2, "the conversion still refunds the attempt");

    // A user abort refuses the bounce; the worker then runs its abort
    // fallback, which finishes the row while keeping the user's reason.
    assert!(db.queue.abort_job(aborted, "operator asked").await.unwrap());
    assert!(!db.queue.requeue_unhandled(&aborted_attempt).await.unwrap(), "a user abort must not be resurrected");
    assert!(db.queue.finish(&aborted_attempt, JobStatus::Aborted, None, None).await.unwrap());
    let row = db.queue.fetch_job(aborted).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Aborted);
    assert_eq!(row.error.as_deref(), Some("operator asked"), "the fallback must preserve the user's reason");
}

/// The two *public operator* reasons take the same boundary the internal
/// requeues do: both land in the `error` column, where a NUL is `22021` — an
/// `Error::Db` a caller would retry — while every other writer of the column
/// answers `Error::Config`. The refused action must also not have happened.
#[sqlx::test(migrations = "./migrations")]
async fn test_abort_and_manual_retry_refuse_a_nul_reason(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let queued = db.queue.enqueue_raw(new_job("nul-abort", |_| {})).await.unwrap().job_id();
    let refused = db.queue.abort_job(queued, "sto\0p").await.unwrap_err();
    assert!(matches!(&refused, Error::Config(message) if message == "job error must not contain NUL"), "{refused:?}");
    let row = db.queue.fetch_job(queued).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Queued, "a refused abort must not have aborted");

    let attempt = db.queue.dequeue(1, Uuid::now_v7()).await.unwrap().remove(0);
    assert!(db.queue.finish(&attempt, JobStatus::Failed, None, Some("failed: boom")).await.unwrap());
    let refused = db.queue.retry_job_occurrence(attempt.id, "re\0try").await.unwrap_err();
    assert!(matches!(&refused, Error::Config(message) if message == "job error must not contain NUL"), "{refused:?}");
    let source = db.queue.fetch_job(attempt.id).await.unwrap().unwrap();
    assert_eq!(source.retried_at, None, "a refused retry must not consume the row's one retry");

    // Refused, not spent: valid reasons still act on the same rows.
    assert!(db.queue.retry_job_occurrence(attempt.id, "retry").await.unwrap().is_some());
}

#[sqlx::test(migrations = "./migrations")]
async fn test_consumer_attempt_cannot_finish_a_newer_attempt(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db
        .queue
        .enqueue_raw(with_config("fenced-consumer", |config| {
            config.max_attempts = 2;
            config.timeout = Some(Duration::from_millis(10));
        }))
        .await
        .unwrap()
        .unwrap();
    let first_worker = Uuid::now_v7();
    let first = leased_consumer(&db.queue, first_worker).await.dequeue(1).await.unwrap().into_iter().next().unwrap();
    backdate_job_liveness(&db, id).await;
    // The sweeper only recovers an attempt whose owner is gone, so retire the
    // first consumer's lease before asking it to.
    crate::expire_worker(&db, first_worker).await;
    let mut sweeper = db.queue.sweeper();
    // One pass, not two: the cooperative `aborting` window exists for an owner
    // that is still there to use it, and this one's lease is gone.
    let report = sweeper.sweep().await.unwrap();
    assert_eq!(report.cancelling, vec![id]);
    assert_eq!(report.swept, vec![id]);
    sweeper.release().await;

    let second = leased_consumer(&db.queue, Uuid::now_v7()).await.dequeue(1).await.unwrap().into_iter().next().unwrap();
    assert_eq!(second.job().attempts, 2);
    assert!(!first.finish(JobStatus::Complete, Some(json!("stale")), None).await.unwrap());
    assert!(second.finish(JobStatus::Complete, Some(json!("fresh")), None).await.unwrap());
}

/// Dropping an unsettled [`Attempt`] is a documented guarantee — the consumer's
/// own task may have panicked or been cancelled while its heartbeat loop runs on,
/// and a heartbeat is precisely the assertion that every claimed attempt is still
/// being worked, so nothing else would ever reclaim an untimed one.
///
/// Both branches, because they are the ones that can silently invert: the attempt
/// is *spent*, not refunded (the handler may have run arbitrarily far), so a row
/// with tries left comes back `queued` one attempt down, and a row on its last try
/// must finish `aborted` rather than sit `running` under a live lease until it
/// expires.
///
/// And the exhausted branch twice over, once under
/// [`JobRetention::DeleteImmediately`]: that retention takes the `DELETE` arm of
/// the recovery's `finish_rows_sql!` skeleton rather than the `UPDATE` arm, and
/// this is the only test that reaches it through `abort_unsettled_claim`.
#[sqlx::test(migrations = "./migrations")]
async fn test_dropping_an_unsettled_consumer_attempt_recovers_the_job(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let consumer = leased_consumer(&db.queue, Uuid::now_v7()).await;

    // The exhausted branch first, and only then the retryable one: a requeued row
    // goes back to `queued` and sorts ahead of anything enqueued after it, so the
    // other order had the second dequeue hand back the *first* job.
    //
    // One attempt allowed, so there is nothing left to requeue with and the row
    // has to finish rather than stay claimed under a live lease.
    let exhausted = db
        .queue
        .enqueue_raw(with_config("dropped-exhausted", |config| config.max_attempts = 1))
        .await
        .unwrap()
        .unwrap();
    let attempt = consumer.dequeue(1).await.unwrap().into_iter().next().unwrap();
    assert_eq!(attempt.job().id, exhausted);
    drop(attempt);

    let row = wait_for_some(Duration::from_secs(10), Duration::from_millis(10), "dropped attempt was not aborted", {
        let queue = db.queue.clone();
        move || {
            let queue = queue.clone();
            async move { queue.fetch_job(exhausted).await.unwrap().filter(|row| row.status.is_terminal()) }
        }
    })
    .await;
    // `aborted`, not `failed`: nothing here ever saw a handler report an error.
    assert_eq!(row.status, JobStatus::Aborted);
    assert_eq!(row.error.as_deref(), Some("attempt dropped without settlement"));
    wait_for_recovery_stats(&db.queue, 0, 1).await;

    // Two attempts allowed: the drop spends the first and hands the row back.
    let retryable = db
        .queue
        .enqueue_raw(with_config("dropped-retryable", |config| config.max_attempts = 2))
        .await
        .unwrap()
        .unwrap();
    let attempt = consumer.dequeue(1).await.unwrap().into_iter().next().unwrap();
    assert_eq!(attempt.job().id, retryable);
    assert_eq!(attempt.job().attempts, 1);
    drop(attempt);

    let row = wait_for_some(Duration::from_secs(10), Duration::from_millis(10), "dropped attempt was not requeued", {
        let queue = db.queue.clone();
        move || {
            let queue = queue.clone();
            async move { queue.fetch_job(retryable).await.unwrap().filter(|row| row.status == JobStatus::Queued) }
        }
    })
    .await;
    // Spent, not refunded, and the owner is cleared so the row is nobody's.
    assert_eq!(row.attempts, 1);
    assert_eq!(row.max_attempts, 2);
    assert_eq!(row.worker_id, None);
    assert_eq!(row.error.as_deref(), Some("attempt dropped without settlement"));
    wait_for_recovery_stats(&db.queue, 1, 1).await;

    // Drain the row the branch above handed back, for the reason the branch
    // order itself exists: it is `queued` again and sorts ahead of the job
    // enqueued next, so leaving it would have the last dequeue return it.
    let attempt = consumer.dequeue(1).await.unwrap().into_iter().next().unwrap();
    assert_eq!(attempt.job().id, retryable);
    assert!(attempt.finish(JobStatus::Complete, None, None).await.unwrap());

    // The exhausted branch again, now with immediate retention: the recovery
    // deletes the row instead of updating it, which is the one arm of the
    // statement's shared `finish_rows_sql!` skeleton that no other test reaches
    // through this call site.
    let ephemeral = db
        .queue
        .enqueue_raw(with_config("dropped-ephemeral", |config| {
            config.max_attempts = 1;
            // The recovery finishes the row `aborted`, so it is the failure
            // retention that deletes it.
            config.failed_retention = JobRetention::DeleteImmediately;
        }))
        .await
        .unwrap()
        .unwrap();
    let attempt = consumer.dequeue(1).await.unwrap().into_iter().next().unwrap();
    assert_eq!(attempt.job().id, ephemeral);
    drop(attempt);

    wait_until(
        Duration::from_secs(10),
        Duration::from_millis(10),
        "dropped immediate-delete attempt was not deleted",
        {
            let queue = db.queue.clone();
            move || {
                let queue = queue.clone();
                async move { queue.fetch_job(ephemeral).await.unwrap().is_none() }
            }
        },
    )
    .await;
    wait_for_recovery_stats(&db.queue, 1, 2).await;
}

/// An [`Attempt`] is `Send`, so custom consumers may hand CPU work to a plain thread. That thread has no entered Tokio
/// runtime, but dropping the attempt there must still use the runtime that dequeued it: the live lease deliberately
/// prevents the sweeper from reclaiming this untimed row instead.
#[sqlx::test(migrations = "./migrations")]
async fn test_dropping_an_unsettled_consumer_attempt_off_runtime_recovers_the_job(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let consumer = leased_consumer(&db.queue, Uuid::now_v7()).await;
    let id = db
        .queue
        .enqueue_raw(new_job("dropped-off-runtime", |job| {
            job.dedupe_key = Some("dropped-off-runtime".to_string());
            job.config.max_attempts = 1;
            job.config.timeout = None;
        }))
        .await
        .unwrap()
        .unwrap();
    let attempt = consumer.dequeue(1).await.unwrap().into_iter().next().unwrap();
    assert_eq!(attempt.job().id, id);
    assert_eq!(attempt.job().timeout(), None);

    std::thread::spawn(move || {
        assert!(tokio::runtime::Handle::try_current().is_err());
        drop(attempt);
    })
    .join()
    .unwrap();

    let row = wait_for_some(
        Duration::from_secs(10),
        Duration::from_millis(10),
        "attempt dropped off the runtime was not aborted",
        {
            let queue = db.queue.clone();
            move || {
                let queue = queue.clone();
                async move { queue.fetch_job(id).await.unwrap().filter(|row| row.status == JobStatus::Aborted) }
            }
        },
    )
    .await;
    assert_eq!(row.error.as_deref(), Some("attempt dropped without settlement"));
    wait_for_recovery_stats(&db.queue, 0, 1).await;

    let replacement = db
        .queue
        .enqueue_raw(new_job("dropped-off-runtime", |job| {
            job.dedupe_key = Some("dropped-off-runtime".to_string());
        }))
        .await
        .unwrap()
        .unwrap();
    assert_ne!(replacement, id, "recovery must release the terminal job's dedupe key");
}

/// And the recovery's guards make it a no-op once the row has moved on, so a
/// settled attempt dropping in the ordinary way cannot disturb its successor.
#[sqlx::test(migrations = "./migrations")]
async fn test_dropping_a_settled_consumer_attempt_leaves_the_row_alone(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id =
        db.queue.enqueue_raw(with_config("dropped-settled", |config| config.max_attempts = 3)).await.unwrap().unwrap();
    let consumer = leased_consumer(&db.queue, Uuid::now_v7()).await;
    let attempt = consumer.dequeue(1).await.unwrap().into_iter().next().unwrap();
    assert!(attempt.finish(JobStatus::Complete, Some(json!("done")), None).await.unwrap());
    drop(attempt);

    // Nothing to race: a settled attempt spawns no resolver at all. Give one a
    // chance to land anyway, then assert the terminal row is untouched.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let row = db.queue.fetch_job(id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Complete);
    assert_eq!(row.result, Some(json!("done")));
    assert_eq!(row.attempts, 1);
    assert_eq!(db.queue.stats().complete, 1);
    assert_eq!(db.queue.stats().retried, 0);
    assert_eq!(db.queue.stats().aborted, 0);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_dequeue_respects_limit_schedule_and_priority_range(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    for _ in 0..3 {
        db.queue.enqueue_raw(new_job("j", |_| {})).await.unwrap();
    }
    // A job scheduled in the future must not be dequeued.
    db.queue
        .enqueue_raw(new_job("future", |job| {
            job.scheduled_at = Some(Timestamp::now() + SignedDuration::from_secs(60));
        }))
        .await
        .unwrap();
    // A job outside a restricted handle's priority range must not be dequeued by it.
    db.queue.enqueue_raw(with_config("low", |c| c.priority = -10)).await.unwrap();

    let restricted = db.another_queue(|b| b.priorities(0, 10)).await;
    let worker = Uuid::now_v7();
    let first = restricted.dequeue(2, worker).await.unwrap();
    assert_eq!(first.len(), 2);
    let rest = restricted.dequeue(10, worker).await.unwrap();
    assert_eq!(rest.len(), 1, "third in-range job");
    assert!(restricted.dequeue(10, worker).await.unwrap().is_empty());

    // The unrestricted handle still sees the low-priority job.
    let low = db.queue.dequeue(10, worker).await.unwrap();
    assert_eq!(low.len(), 1);
    assert_eq!(low[0].priority, -10);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_concurrent_dequeues_get_disjoint_jobs(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    for _ in 0..4 {
        db.queue.enqueue_raw(new_job("j", |_| {})).await.unwrap();
    }
    let other = db.another_queue(|b| b).await;
    let (a, b) = tokio::join!(db.queue.dequeue(2, Uuid::now_v7()), other.dequeue(2, Uuid::now_v7()),);
    let (a, b) = (a.unwrap(), b.unwrap());
    assert_eq!(a.len() + b.len(), 4);
    for job_a in &a {
        assert!(b.iter().all(|job_b| job_b.id != job_a.id), "SKIP LOCKED overlap");
    }
}

#[sqlx::test(migrations = "./migrations")]
async fn test_concurrent_dequeues_make_disjoint_progress_at_scale(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    for _ in 0..64 {
        db.queue.enqueue_raw(new_job("parallel", |_| {})).await.unwrap().unwrap();
    }

    let mut dequeues = tokio::task::JoinSet::new();
    for _ in 0..8 {
        let queue = db.queue.clone();
        dequeues.spawn(async move { queue.dequeue(16, Uuid::now_v7()).await });
    }
    let mut ids = std::collections::HashSet::new();
    while let Some(result) = dequeues.join_next().await {
        for job in result.unwrap().unwrap() {
            assert!(ids.insert(job.id), "two dequeues returned the same job");
        }
    }
    assert_eq!(ids.len(), 64);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_finish_complete_stores_result_and_expiry(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db
        .queue
        .enqueue_raw(with_config("j", |c| c.retention = JobRetention::For(Duration::from_secs(60))))
        .await
        .unwrap()
        .unwrap();
    let active = db.queue.dequeue(1, Uuid::now_v7()).await.unwrap().remove(0);

    assert!(db.queue.finish(&active, JobStatus::Complete, Some(json!(42)), None).await.unwrap());
    let row = db.queue.fetch_job(id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Complete);
    assert_eq!(row.result, Some(json!(42)));
    assert!(row.completed_at.is_some());
    let expires = row.expires_at.expect("expiry from retention");
    assert!(expires > Timestamp::now() + SignedDuration::from_secs(50));

    // Double-finish is refused (already terminal).
    assert!(!db.queue.finish(&active, JobStatus::Failed, None, Some("late")).await.unwrap());
    let row = db.queue.fetch_job(id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Complete);

    assert_eq!(db.queue.stats().complete, 1);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_finish_rejects_nonterminal_statuses(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    db.queue.enqueue_raw(new_job("j", |_| {})).await.unwrap();
    let active = db.queue.dequeue(1, Uuid::now_v7()).await.unwrap().remove(0);
    let error = db.queue.finish(&active, JobStatus::Running, None, None).await.unwrap_err();
    assert!(matches!(error, Error::Config(_)), "{error}");
    assert_eq!(db.queue.fetch_job(active.id).await.unwrap().unwrap().status, JobStatus::Running);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_finish_retention_starts_after_a_row_lock_wait(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let consumer = leased_consumer(&db.queue, Uuid::now_v7()).await;
    let ttl = Duration::from_millis(500);
    for status in [
        Some(JobStatus::Complete),
        Some(JobStatus::Failed),
        Some(JobStatus::Aborted),
        None,
    ] {
        let id = db
            .queue
            .enqueue_raw(with_config("locked-finish", |config| {
                config.retention = JobRetention::For(ttl);
                config.failed_retention = JobRetention::For(ttl);
            }))
            .await
            .unwrap()
            .unwrap();
        let attempt = if status.is_some() { Some(consumer.dequeue(1).await.unwrap().remove(0)) } else { None };
        let mut holder = pool.begin().await.unwrap();
        sqlx::query("SELECT id FROM pgqueue.jobs WHERE id = $1 FOR UPDATE")
            .bind(id)
            .fetch_one(&mut *holder)
            .await
            .unwrap();
        let queue = db.queue.clone();
        let finishing = tokio::spawn(async move {
            match (attempt, status) {
                (Some(attempt), Some(status)) => {
                    attempt.finish(status, (status == JobStatus::Complete).then(|| json!(42)), None).await
                }
                _ => queue.abort_job(id, "cancel queued job").await,
            }
        });
        wait_for_lock_waiter(&db, "%WITH candidate AS%", "finishing did not wait for the job lock").await;
        tokio::time::sleep(ttl + Duration::from_millis(100)).await;
        let released_at = sqlx::query_scalar::<_, jiff_sqlx::Timestamp>("SELECT clock_timestamp()")
            .fetch_one(&pool)
            .await
            .unwrap()
            .to_jiff();
        holder.rollback().await.unwrap();
        assert!(tokio::time::timeout(Duration::from_secs(5), finishing).await.unwrap().unwrap().unwrap());
        let row = db.queue.fetch_job(id).await.unwrap().unwrap();
        assert_eq!(row.status, status.unwrap_or(JobStatus::Aborted));
        assert!(row.completed_at.unwrap() >= released_at, "completion must follow the lock wait");
        assert!(
            row.expires_at.unwrap() >= released_at + SignedDuration::from_millis(500),
            "the full retention must remain after the lock wait"
        );
    }
}

#[sqlx::test(migrations = "./migrations")]
async fn test_finish_retention_forever_and_delete_immediately(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let forever =
        db.queue.enqueue_raw(with_config("keep", |c| c.retention = JobRetention::Forever)).await.unwrap().unwrap();
    let ephemeral = db
        .queue
        .enqueue_raw(with_config("gone", |c| c.retention = JobRetention::DeleteImmediately))
        .await
        .unwrap()
        .unwrap();
    let active = db.queue.dequeue(2, Uuid::now_v7()).await.unwrap();
    let forever_row = active.iter().find(|j| j.id == forever).unwrap();
    let ephemeral_row = active.iter().find(|j| j.id == ephemeral).unwrap();

    db.queue.finish(forever_row, JobStatus::Complete, None, None).await.unwrap();
    let row = db.queue.fetch_job(forever).await.unwrap().unwrap();
    assert!(row.expires_at.is_none(), "forever rows never expire");

    db.queue.finish(ephemeral_row, JobStatus::Complete, None, None).await.unwrap();
    assert!(db.queue.fetch_job(ephemeral).await.unwrap().is_none(), "deleted on finish");
}

#[sqlx::test(migrations = "./migrations")]
async fn test_finish_failed_counts_and_stores_error(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db.queue.enqueue_raw(new_job("j", |_| {})).await.unwrap().unwrap();
    let active = db.queue.dequeue(1, Uuid::now_v7()).await.unwrap().remove(0);
    db.queue.finish(&active, JobStatus::Failed, None, Some("failed: boom")).await.unwrap();
    let row = db.queue.fetch_job(id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Failed);
    assert_eq!(row.error.as_deref(), Some("failed: boom"));
    assert_eq!(db.queue.stats().failed, 1);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_retry_requeues_with_delay(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    db.queue
        .enqueue_raw(with_config("j", |c| {
            c.max_attempts = 3;
            c.retry_delay = Duration::from_millis(30_000);
        }))
        .await
        .unwrap();
    let row = db.queue.dequeue(1, Uuid::now_v7()).await.unwrap().remove(0);

    assert!(db.queue.retry(&row, "failed: transient").await.unwrap());
    let updated = db.queue.fetch_job(row.id).await.unwrap().unwrap();
    assert_eq!(updated.status, JobStatus::Queued);
    assert_eq!(updated.error.as_deref(), Some("failed: transient"));
    assert_eq!(updated.attempts, 1, "attempts preserved across retry");
    assert!(updated.scheduled_at > Timestamp::now() + SignedDuration::from_secs(20), "retry delay applied");
    assert!(updated.started_at.is_none());
    assert_eq!(db.queue.stats().retried, 1);

    // Retrying a job that is no longer running is refused.
    assert!(!db.queue.retry(&row, "again").await.unwrap());
    // And it is not dequeueable before its delay elapses.
    assert!(db.queue.dequeue(1, Uuid::now_v7()).await.unwrap().is_empty());
}

/// A requeued attempt belongs to nobody, so the row must stop naming the worker
/// that gave it up: `JobRow::worker_id` is public API and the dashboard's job
/// detail renders it as the owner, so a `queued` row carrying a stale — often
/// already exited — worker misattributes the job to whoever looks at it.
#[sqlx::test(migrations = "./migrations")]
async fn test_retry_clears_the_worker_id_on_the_requeued_row(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db.queue.enqueue_raw(with_config("j", |config| config.max_attempts = 3)).await.unwrap().unwrap();
    let worker_id = Uuid::now_v7();
    let consumer = leased_consumer(&db.queue, worker_id).await;
    let attempt = consumer.dequeue(1).await.unwrap().remove(0);
    assert_eq!(attempt.job().worker_id, Some(worker_id));

    assert!(attempt.retry("failed: transient").await.unwrap());

    let updated = db.queue.fetch_job(id).await.unwrap().unwrap();
    assert_eq!(updated.status, JobStatus::Queued);
    assert_eq!(updated.worker_id, None, "a requeued job must not advertise the worker that gave it up");
}

#[sqlx::test(migrations = "./migrations")]
async fn test_retry_refuses_when_attempts_are_exhausted(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db.queue.enqueue_raw(with_config("j", |config| config.max_attempts = 1)).await.unwrap().unwrap();
    let mut row = db.queue.dequeue(1, Uuid::now_v7()).await.unwrap().remove(0);
    assert!(!row.is_retryable());
    row.max_attempts += 1;
    assert!(row.is_retryable(), "the caller snapshot can be modified");

    assert!(!db.queue.retry(&row, "failed: permanent").await.unwrap());
    let updated = db.queue.fetch_job(id).await.unwrap().unwrap();
    assert_eq!(updated.status, JobStatus::Running);
    assert_eq!(updated.attempts, 1);
    assert!(updated.error.is_none());
    assert_eq!(db.queue.stats().retried, 0);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_abort_queued_job_finishes_immediately(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db.queue.enqueue_raw(new_job("j", |_| {})).await.unwrap().unwrap();
    assert!(db.queue.abort_job(id, "not needed").await.unwrap());
    let row = db.queue.fetch_job(id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Aborted);
    assert_eq!(row.error.as_deref(), Some("not needed"));
    assert!(row.completed_at.is_some());
    assert!(row.expires_at.is_some(), "retention applies to aborted rows");
    assert_eq!(db.queue.stats().aborted, 1);

    // A terminal job can't be aborted again.
    assert!(!db.queue.abort_job(id, "again").await.unwrap());
}

#[sqlx::test(migrations = "./migrations")]
async fn test_abort_queued_delete_immediately_survives_until_sweep(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    // An abort is a failure outcome, so the failure retention is the one that
    // applies.
    let id = db
        .queue
        .enqueue_raw(with_config("j", |config| config.failed_retention = JobRetention::DeleteImmediately))
        .await
        .unwrap()
        .unwrap();
    assert!(db.queue.abort_job(id, "not needed").await.unwrap());
    let row = db.queue.fetch_job(id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Aborted);
    assert!(row.expires_at.is_some());

    let mut sweeper = db.queue.sweeper();
    assert_eq!(sweeper.sweep().await.unwrap().purged_jobs, 1);
    assert!(db.queue.fetch_job(id).await.unwrap().is_none());
    sweeper.release().await;
}

#[sqlx::test(migrations = "./migrations")]
async fn test_abort_running_job_goes_through_aborting(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db.queue.enqueue_raw(new_job("j", |_| {})).await.unwrap().unwrap();
    let active = db.queue.dequeue(1, Uuid::now_v7()).await.unwrap().remove(0);
    backdate_job_liveness(&db, id).await;
    let touched_before_abort = db.queue.fetch_job(id).await.unwrap().unwrap().touched_at.unwrap();

    assert!(db.queue.abort_job(id, "stop it").await.unwrap());
    let row = db.queue.fetch_job(id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Aborting, "worker must cancel it");
    assert_eq!(db.queue.counts().await.unwrap().running, 1, "aborting work still occupies a worker");
    assert!(row.touched_at.unwrap() > touched_before_abort, "abort updates the job's lifecycle timestamp");

    assert!(
        !db.queue.finish(&active, JobStatus::Complete, Some(json!("too late")), None).await.unwrap(),
        "a public finish must not overwrite a committed abort"
    );
    assert_eq!(db.queue.fetch_job(id).await.unwrap().unwrap().status, JobStatus::Aborting);

    // The worker's abort loop then finishes it.
    assert!(db.queue.finish(&active, JobStatus::Aborted, None, Some("stop it")).await.unwrap());
    let row = db.queue.fetch_job(id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Aborted);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_retry_job_grants_terminal_jobs_one_more_attempt(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db.queue.enqueue_raw(new_job("j", |_| {})).await.unwrap().unwrap();

    // Not retryable while queued.
    assert!(!db.queue.retry_job(id, "from ui").await.unwrap());

    let active = db.queue.dequeue(1, Uuid::now_v7()).await.unwrap().remove(0);
    db.queue.finish(&active, JobStatus::Failed, None, Some("boom")).await.unwrap();

    assert!(db.queue.retry_job(id, "from ui").await.unwrap());
    assert_eq!(db.queue.stats().retried, 1);
    assert!(
        !db.queue.retry_job(id, "duplicate click").await.unwrap(),
        "one terminal occurrence can only be retried once"
    );
    let original = db.queue.fetch_job(id).await.unwrap().unwrap();
    assert_eq!(original.status, JobStatus::Failed);
    assert_eq!(original.error.as_deref(), Some("boom"));

    // It is immediately dequeueable and can succeed this time.
    let jobs = db.queue.dequeue(1, Uuid::now_v7()).await.unwrap();
    assert_eq!(jobs.len(), 1);
    assert_ne!(jobs[0].id, id);
    assert_eq!(jobs[0].max_attempts, jobs[0].attempts, "the dequeue consumed exactly one added attempt");
}

/// `JobRow` omitted `kind`, `cron_expr` and `retried_at` — columns the schema
/// has carried all along and the dashboard already selected — so the public API
/// could not tell a cron occurrence from a plain job, nor see whether
/// [`Queue::retry_job`] had already been spent on a row.
#[sqlx::test(migrations = "./migrations")]
async fn test_job_rows_report_kind_schedule_and_whether_they_were_retried(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db.queue.enqueue_raw(new_job("plain", |_| {})).await.unwrap().unwrap();
    let row = db.queue.fetch_job(id).await.unwrap().unwrap();
    assert_eq!(row.kind, "job");
    assert_eq!(row.cron_expr, None);
    assert_eq!(row.retried_at, None);

    let active = db.queue.dequeue(1, Uuid::now_v7()).await.unwrap().remove(0);
    assert_eq!(active.kind, "job", "the dequeue path reports them too");
    assert!(db.queue.finish(&active, JobStatus::Failed, None, Some("boom")).await.unwrap());
    assert!(db.queue.retry_job(id, "from ui").await.unwrap());
    let row = db.queue.fetch_job(id).await.unwrap().unwrap();
    assert!(row.retried_at.is_some(), "a spent retry is what refuses the second one, so it must be visible");

    // A cron occurrence names the schedule that produced it.
    let occurrence: Uuid = sqlx::query_scalar(
        "INSERT INTO pgqueue.jobs (queue, name, kind, cron_expr)
         VALUES ($1, 'nightly', 'cron', '0 3 * * *') RETURNING id",
    )
    .bind(db.queue.name())
    .fetch_one(&pool)
    .await
    .unwrap();
    let page =
        db.queue.jobs_page(JobFilter { name: Some("nightly".to_string()), ..JobFilter::default() }).await.unwrap();
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].id, occurrence);
    assert_eq!(page[0].kind, "cron");
    assert_eq!(page[0].cron_expr.as_deref(), Some("0 3 * * *"));
}

#[sqlx::test(migrations = "./migrations")]
async fn test_concurrent_manual_retries_enqueue_exactly_one_occurrence(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db.queue.enqueue_raw(new_job("j", |_| {})).await.unwrap().unwrap();
    assert!(db.queue.abort_job(id, "make terminal").await.unwrap());

    let mut retries = tokio::task::JoinSet::new();
    for _ in 0..20 {
        let queue = db.queue.clone();
        retries.spawn(async move { queue.retry_job(id, "raced retry").await.unwrap() });
    }
    let mut accepted = 0;
    while let Some(result) = retries.join_next().await {
        accepted += usize::from(result.unwrap());
    }
    assert_eq!(accepted, 1);
    assert_eq!(db.queue.counts().await.unwrap().queued, 1);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_jobs_page_filters_and_paginates(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    for i in 0..3 {
        db.queue.enqueue_raw(new_job("alpha", |j| j.payload = json!(i))).await.unwrap();
    }
    db.queue.enqueue_raw(new_job("beta", |_| {})).await.unwrap();
    db.queue.dequeue(1, Uuid::now_v7()).await.unwrap(); // one starts running

    let all = db.queue.jobs_page(JobFilter::default()).await.unwrap();
    assert_eq!(all.len(), 4);

    let queued = db.queue.jobs_page(JobFilter { status: Some(JobStatus::Queued), ..Default::default() }).await.unwrap();
    assert_eq!(queued.len(), 3);

    let alphas = db.queue.jobs_page(JobFilter { name: Some("alpha".into()), ..Default::default() }).await.unwrap();
    assert_eq!(alphas.len(), 3);

    let page = db
        .queue
        .jobs_page(JobFilter { limit: Some(2), before: Some(JobCursor::from(&all[2])), ..Default::default() })
        .await
        .unwrap();
    assert_eq!(page.len(), 1, "cursor past the first three");
}

#[sqlx::test(migrations = "./migrations")]
async fn test_jobs_page_orders_reused_dedupe_keys_by_latest_enqueue(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let deduplicated = || {
        new_job("deduplicated", |job| {
            job.dedupe_key = Some("key".into());
        })
    };
    let first_id = db.queue.enqueue_raw(deduplicated()).await.unwrap().unwrap();
    let active = db.queue.dequeue(1, Uuid::now_v7()).await.unwrap().remove(0);
    db.queue.finish(&active, JobStatus::Complete, None, None).await.unwrap();

    let newer_id = db.queue.enqueue_raw(new_job("plain", |_| {})).await.unwrap().unwrap();
    sqlx::query(
        "UPDATE pgqueue.jobs SET enqueued_at = now() - interval '1 second' \
         WHERE id IN ($1, $2)",
    )
    .bind(first_id)
    .bind(newer_id)
    .execute(db.queue.pool())
    .await
    .unwrap();
    let latest_id = db.queue.enqueue_raw(deduplicated()).await.unwrap().unwrap();
    assert_ne!(latest_id, first_id);

    let jobs = db.queue.jobs_page(JobFilter::default()).await.unwrap();
    assert_eq!(jobs[0].id, latest_id, "new occurrence is the newest activity");
}

#[sqlx::test(migrations = "./migrations")]
async fn test_counts_split_queued_running_scheduled(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    db.queue.enqueue_raw(new_job("now", |_| {})).await.unwrap();
    db.queue.enqueue_raw(new_job("now2", |_| {})).await.unwrap();
    db.queue
        .enqueue_raw(new_job("later", |job| {
            job.scheduled_at = Some(Timestamp::now() + SignedDuration::from_secs(60));
        }))
        .await
        .unwrap();
    db.queue.dequeue(1, Uuid::now_v7()).await.unwrap();

    let counts = db.queue.counts().await.unwrap();
    assert_eq!((counts.queued, counts.running, counts.scheduled, counts.failed, counts.aborted,), (1, 1, 1, 0, 0));
}

#[sqlx::test(migrations = "./migrations")]
async fn test_consumer_heartbeat_lease_starts_after_a_row_lock_wait(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let worker_id = Uuid::now_v7();
    let consumer = leased_consumer(&db.queue, worker_id).await;
    let mut holder = pool.begin().await.unwrap();
    sqlx::query("SELECT id FROM pgqueue.workers WHERE id = $1 FOR UPDATE")
        .bind(worker_id)
        .fetch_one(&mut *holder)
        .await
        .unwrap();
    let renewal = consumer.clone();
    let heartbeat = tokio::spawn(async move { renewal.heartbeat(json!({}), None, Duration::from_millis(500)).await });
    wait_for_lock_waiter(&db, "%INSERT INTO pgqueue.workers%", "heartbeat did not wait for the worker lock").await;
    tokio::time::sleep(Duration::from_millis(600)).await;
    let released_at = sqlx::query_scalar::<_, jiff_sqlx::Timestamp>("SELECT clock_timestamp()")
        .fetch_one(&pool)
        .await
        .unwrap()
        .to_jiff();
    holder.rollback().await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), heartbeat).await.unwrap().unwrap().unwrap();
    let worker = list_workers(&db.queue).await.remove(0);
    assert!(worker.heartbeat_at >= released_at, "a successful heartbeat must describe the renewed lease");
    assert!(
        worker.expires_at >= released_at + SignedDuration::from_millis(500),
        "the lock wait must not consume the renewed lease"
    );
    db.queue.enqueue_raw(new_job("after-renewal", |_| {})).await.unwrap();
    let attempts = consumer.dequeue(1).await.unwrap();
    assert_eq!(attempts.len(), 1, "a successful renewal must leave intake open");
    assert!(attempts[0].finish(JobStatus::Complete, None, None).await.unwrap());
}

#[sqlx::test(migrations = "./migrations")]
async fn test_worker_info_appears_until_ttl_expires(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let worker = Uuid::now_v7();
    db.queue
        .write_worker_info(worker, json!({"complete": 3}), Some(json!({"host": "test"})), Duration::from_secs(60))
        .await
        .unwrap();

    let workers = list_workers(&db.queue).await;
    assert_eq!(workers.len(), 1);
    assert_eq!(workers[0].id, worker);
    assert_eq!(workers[0].stats, json!({"complete": 3}));
    assert_eq!(workers[0].metadata, Some(json!({"host": "test"})));

    // Re-upsert with zero TTL: immediately expired, hence invisible.
    db.queue.write_worker_info(worker, json!({}), None, Duration::ZERO).await.unwrap();
    assert!(list_workers(&db.queue).await.is_empty());
}

//noinspection SqlWithoutWhere
#[sqlx::test(migrations = "./migrations")]
async fn test_workers_page_is_bounded_and_uses_a_stable_cursor(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let mut expected = Vec::new();
    for index in 0..26 {
        let id = Uuid::now_v7();
        expected.push(id);
        db.queue.write_worker_info(id, json!({"index": index}), None, Duration::from_secs(60)).await.unwrap();
    }
    sqlx::query("UPDATE pgqueue.workers SET started_at = date_trunc('second', clock_timestamp())")
        .execute(&pool)
        .await
        .unwrap();
    expected.sort_unstable();

    assert_eq!(db.queue.workers_page(WorkerFilter::default()).await.unwrap().len(), 25);

    let mut actual = Vec::new();
    let mut after = None;
    loop {
        let page = db.queue.workers_page(WorkerFilter { limit: Some(7), after }).await.unwrap();
        if page.is_empty() {
            break;
        }
        after = page.last().map(WorkerCursor::from);
        actual.extend(page.into_iter().map(|worker| worker.id));
    }
    assert_eq!(actual, expected);

    for limit in [0, 101] {
        assert!(matches!(
            db.queue.workers_page(WorkerFilter { limit: Some(limit), after: None }).await,
            Err(Error::Config(_))
        ));
    }
    let below_range = WorkerCursor { started_at: "-009990-01-01T00:00:00Z".parse().unwrap(), id: Uuid::now_v7() };
    assert!(matches!(
        db.queue.workers_page(WorkerFilter { limit: None, after: Some(below_range) }).await,
        Err(Error::Config(_))
    ));
}

/// The `INSERT` half of the lease upsert has to honour the caller's intake
/// state. A worker whose row the sweeper purged — because it stalled past its
/// TTL — recreates the lease on its next heartbeat, and letting the column
/// default to `accepting` republished a shutting-down worker as live and taking
/// work. A live lease is what suppresses sweeper recovery, so that also stalls
/// recovery of the worker's own abandoned attempts for a whole fresh TTL.
#[sqlx::test(migrations = "./migrations")]
async fn test_worker_lease_is_created_closed_when_the_writer_stopped_taking_work(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let workers = db.queue.pool().clone();
    let accepting = |id: Uuid| {
        let workers = workers.clone();
        async move {
            sqlx::query_scalar::<_, bool>("SELECT accepting FROM pgqueue.workers WHERE id = $1")
                .bind(id)
                .fetch_optional(&workers)
                .await
                .unwrap()
        }
    };
    db.queue.enqueue_raw(new_job("drainable", |_| {})).await.unwrap();

    // No row yet, so this takes the INSERT branch.
    let closed = Uuid::now_v7();
    db.queue.write_worker_lease(closed, json!({}), None, Duration::from_secs(60), false).await.unwrap();
    assert_eq!(accepting(closed).await, Some(false));

    // And a worker that is still taking work creates an accepting lease.
    let open = Uuid::now_v7();
    db.queue.write_worker_lease(open, json!({}), None, Duration::from_secs(60), true).await.unwrap();
    assert_eq!(accepting(open).await, Some(true));

    // A closed lease is live, which is what keeps the sweeper off the attempts
    // the worker is still draining, but it must not be advertised as a worker
    // that will take more work.
    let workers = list_workers(&db.queue).await;
    assert!(workers.iter().any(|worker| worker.id == closed));
    assert!(
        db.queue.consumer(closed).dequeue(1).await.unwrap().is_empty(),
        "a closed lease must not satisfy the intake check"
    );

    // On the UPDATE branch a worker heartbeat leaves the flag alone in both
    // directions: it neither reopens what shutdown closed nor closes a lease a
    // consumer heartbeat reopened.
    db.queue.write_worker_lease(closed, json!({}), None, Duration::from_secs(60), true).await.unwrap();
    assert_eq!(accepting(closed).await, Some(false));
    db.queue.write_worker_lease(open, json!({}), None, Duration::from_secs(60), false).await.unwrap();
    assert_eq!(accepting(open).await, Some(true));

    // Only a consumer heartbeat reopens.
    db.queue.write_worker_info(closed, json!({}), None, Duration::from_secs(60)).await.unwrap();
    assert_eq!(accepting(closed).await, Some(true));
}

#[sqlx::test(migrations = "./migrations")]
async fn test_sweep_purges_expired_jobs_and_workers(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db
        .queue
        .enqueue_raw(with_config("old", |c| c.retention = JobRetention::For(Duration::from_millis(1))))
        .await
        .unwrap()
        .unwrap();
    let active = db.queue.dequeue(1, Uuid::now_v7()).await.unwrap().remove(0);
    db.queue.finish(&active, JobStatus::Complete, None, None).await.unwrap();
    let worker_id = Uuid::now_v7();
    db.queue.write_worker_info(worker_id, json!({}), None, Duration::from_millis(1)).await.unwrap();
    sqlx::query("UPDATE pgqueue.jobs SET expires_at = now() - interval '1 second' WHERE id = $1")
        .bind(id)
        .execute(db.queue.pool())
        .await
        .unwrap();
    crate::expire_worker(&db, worker_id).await;
    sqlx::query(
        "INSERT INTO pgqueue.cron_occurrences (queue, dedupe_key, scheduled_at, expires_at)
         VALUES ($1, 'expired-claim', now() - interval '2 seconds', now() - interval '1 second')",
    )
    .bind(db.queue.name())
    .execute(db.queue.pool())
    .await
    .unwrap();

    let mut sweeper = db.queue.sweeper();
    let report = sweeper.sweep().await.unwrap();
    assert!(report.leader);
    assert_eq!(report.purged_jobs, 1);
    assert!(report.swept.is_empty());
    assert!(db.queue.fetch_job(id).await.unwrap().is_none(), "expired row purged");
    // Read from the table, not through `workers_page`: that lists only unexpired leases, so it was empty whether or not
    // the sweep purged this one.
    let lease_exists = sqlx::query_scalar::<_, bool>("SELECT EXISTS (SELECT 1 FROM pgqueue.workers WHERE id = $1)")
        .bind(worker_id)
        .fetch_one(db.queue.pool())
        .await
        .unwrap();
    assert!(!lease_exists, "expired worker lease was not purged");
    let claim_exists = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (
             SELECT 1 FROM pgqueue.cron_occurrences
             WHERE queue = $1 AND dedupe_key = 'expired-claim'
         )",
    )
    .bind(db.queue.name())
    .fetch_one(db.queue.pool())
    .await
    .unwrap();
    assert!(!claim_exists, "expired cron occurrence claim was not purged");
    sweeper.release().await;
}

#[sqlx::test(migrations = "./migrations")]
async fn test_sweep_purges_only_expired_rows_for_its_queue(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let other = db.another_queue(|builder| builder.name("other")).await;
    let first_job = sqlx::query_scalar::<_, Uuid>(
        r#"
        INSERT INTO pgqueue.jobs (
            queue, name, payload, status, completed_at, expires_at
        ) VALUES ($1, 'expired-first', 'null', 'complete', now(),
                  now() - interval '1 second')
        RETURNING id
        "#,
    )
    .bind(db.queue.name())
    .fetch_one(&pool)
    .await
    .unwrap();
    let other_job = sqlx::query_scalar::<_, Uuid>(
        r#"
        INSERT INTO pgqueue.jobs (
            queue, name, payload, status, completed_at, expires_at
        ) VALUES ($1, 'expired-other', 'null', 'complete', now(),
                  now() - interval '1 second')
        RETURNING id
        "#,
    )
    .bind(other.name())
    .fetch_one(&pool)
    .await
    .unwrap();
    let first_worker = Uuid::now_v7();
    let other_worker = Uuid::now_v7();
    db.queue.write_worker_info(first_worker, json!({}), None, Duration::from_secs(1)).await.unwrap();
    other.write_worker_info(other_worker, json!({}), None, Duration::from_secs(1)).await.unwrap();
    sqlx::query("UPDATE pgqueue.workers SET expires_at = now() - interval '1 second' WHERE id = ANY($1)")
        .bind([first_worker, other_worker])
        .execute(&pool)
        .await
        .unwrap();

    let mut first_sweeper = db.queue.sweeper();
    let report = first_sweeper.sweep().await.unwrap();
    assert!(report.leader);
    assert_eq!(report.purged_jobs, 1);
    assert!(
        !sqlx::query_scalar::<_, bool>(r#"SELECT EXISTS (SELECT 1 FROM pgqueue.jobs WHERE id = $1)"#)
            .bind(first_job)
            .fetch_one(&pool)
            .await
            .unwrap()
    );
    assert!(
        sqlx::query_scalar::<_, bool>(r#"SELECT EXISTS (SELECT 1 FROM pgqueue.jobs WHERE id = $1)"#)
            .bind(other_job)
            .fetch_one(&pool)
            .await
            .unwrap()
    );
    assert!(
        !sqlx::query_scalar::<_, bool>(r#"SELECT EXISTS (SELECT 1 FROM pgqueue.workers WHERE id = $1)"#)
            .bind(first_worker)
            .fetch_one(&pool)
            .await
            .unwrap()
    );
    assert!(
        sqlx::query_scalar::<_, bool>(r#"SELECT EXISTS (SELECT 1 FROM pgqueue.workers WHERE id = $1)"#)
            .bind(other_worker)
            .fetch_one(&pool)
            .await
            .unwrap()
    );
    first_sweeper.release().await;

    let mut other_sweeper = other.sweeper();
    assert_eq!(other_sweeper.sweep().await.unwrap().purged_jobs, 1);
    assert!(
        !sqlx::query_scalar::<_, bool>(r#"SELECT EXISTS (SELECT 1 FROM pgqueue.jobs WHERE id = $1)"#)
            .bind(other_job)
            .fetch_one(&pool)
            .await
            .unwrap()
    );
    assert!(
        !sqlx::query_scalar::<_, bool>(r#"SELECT EXISTS (SELECT 1 FROM pgqueue.workers WHERE id = $1)"#)
            .bind(other_worker)
            .fetch_one(&pool)
            .await
            .unwrap()
    );
    other_sweeper.release().await;
}

#[sqlx::test(migrations = "./migrations")]
async fn test_sweep_bounds_each_purge_batch_and_reports_more_work(pool: PgPool) {
    let db = TestDb::with(pool.clone(), |builder| builder.sweep_batch_size(2)).await;
    sqlx::query(
        r#"
        INSERT INTO pgqueue.jobs (
            queue, name, payload, status, completed_at, expires_at
        )
        SELECT $1, 'expired-batch', 'null'::jsonb, 'complete', now(),
               now() - interval '1 second'
        FROM generate_series(1, 5)
        "#,
    )
    .bind(db.queue.name())
    .execute(&pool)
    .await
    .unwrap();

    let mut sweeper = db.queue.sweeper();
    let first = sweeper.sweep().await.unwrap();
    assert_eq!(first.purged_jobs, 2);
    assert!(first.has_more_work());
    let second = sweeper.sweep().await.unwrap();
    assert_eq!(second.purged_jobs, 2);
    assert!(second.has_more_work());
    let third = sweeper.sweep().await.unwrap();
    assert_eq!(third.purged_jobs, 1);
    assert!(!third.has_more_work());
    sweeper.release().await;

    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            r#"SELECT count(*) FROM pgqueue.jobs WHERE queue = $1 AND name = 'expired-batch'"#
        )
        .bind(db.queue.name())
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn test_sweep_marks_every_stuck_running_job_in_one_pass(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let mut ids = Vec::new();
    for name in ["stuck-a", "stuck-b", "stuck-c"] {
        let id = db
            .queue
            .enqueue_raw(with_config(name, |c| {
                c.timeout = Some(Duration::from_millis(20));
            }))
            .await
            .unwrap()
            .unwrap();
        ids.push(id);
    }
    // With a live lease, so what this pass does to all three is exactly phase
    // one: the owner is still there to abort them cooperatively.
    let owner = Uuid::now_v7();
    db.queue.write_worker_info(owner, json!({}), None, Duration::from_secs(60)).await.unwrap();
    db.queue.dequeue(3, owner).await.unwrap();
    sqlx::query(
        "UPDATE pgqueue.jobs
         SET started_at = now() - interval '100 milliseconds'
         WHERE queue = $1",
    )
    .bind(db.queue.name())
    .execute(db.queue.pool())
    .await
    .unwrap();

    let mut sweeper = db.queue.sweeper();
    let report = sweeper.sweep().await.unwrap();
    let mut cancelling = report.cancelling.clone();
    cancelling.sort();
    ids.sort();
    assert_eq!(cancelling, ids);
    for id in ids {
        let row = db.queue.fetch_job(id).await.unwrap().unwrap();
        assert_eq!(row.status, JobStatus::Aborting);
        assert_eq!(row.error.as_deref(), Some("swept"));
    }
    sweeper.release().await;
}

#[sqlx::test(migrations = "./migrations")]
async fn test_sweep_retries_stuck_retryable_jobs(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db
        .queue
        .enqueue_raw(with_config("stuck", |c| {
            c.max_attempts = 3;
            c.timeout = Some(Duration::from_millis(20));
        }))
        .await
        .unwrap()
        .unwrap();
    // This consumer writes no `pgqueue.workers` row, so its attempt has no
    // owner to cooperate with — see
    // `test_sweep_waits_for_the_live_owner_before_requeueing` for the other
    // half of the split.
    db.queue.dequeue(1, Uuid::now_v7()).await.unwrap();

    sqlx::query(
        "UPDATE pgqueue.jobs SET started_at = now() - interval '100 milliseconds' \
         WHERE id = $1",
    )
    .bind(id)
    .execute(db.queue.pool())
    .await
    .unwrap();

    let mut sweeper = db.queue.sweeper();
    // The row is still marked `aborting` first — every requeue guard keys on
    // the sweeper's marker — but with the owner provably gone nothing waits a
    // second pass for it, so one sweep both marks and requeues.
    let report = sweeper.sweep().await.unwrap();
    assert_eq!(report.cancelling, vec![id]);
    assert_eq!(report.swept, vec![id]);
    assert_eq!(db.queue.stats().retried, 1);
    let row = db.queue.fetch_job(id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Queued, "retryable stuck job requeued");
    sweeper.release().await;
}

#[sqlx::test(migrations = "./migrations")]
async fn test_public_finish_succeeds_through_the_sweeper_grace_window(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db
        .queue
        .enqueue_raw(with_config("slow", |c| {
            c.max_attempts = 3;
            c.timeout = Some(Duration::from_millis(20));
        }))
        .await
        .unwrap()
        .unwrap();
    // Slow, not dead: the lease is live, which is what keeps the row in the
    // cooperative `aborting` window instead of being recovered outright.
    let owner = Uuid::now_v7();
    db.queue.write_worker_info(owner, json!({}), None, Duration::from_secs(60)).await.unwrap();
    let active = db.queue.dequeue(1, owner).await.unwrap().remove(0);
    backdate_job_liveness(&db, id).await;

    let mut sweeper = db.queue.sweeper();
    assert_eq!(sweeper.sweep().await.unwrap().cancelling, vec![id]);
    assert_eq!(db.queue.fetch_job(id).await.unwrap().unwrap().status, JobStatus::Aborting);

    // The low-level consumer was slow, not dead: its successful result must
    // land through the same grace window worker-processed jobs get, instead
    // of being discarded and the job running twice.
    assert!(
        db.queue.finish(&active, JobStatus::Complete, Some(json!("done")), None).await.unwrap(),
        "a swept-but-alive attempt finishes through the grace window"
    );
    let row = db.queue.fetch_job(id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Complete);
    assert_eq!(row.result, Some(json!("done")));
    assert!(sweeper.sweep().await.unwrap().swept.is_empty(), "nothing left to recover");
    sweeper.release().await;
}

#[sqlx::test(migrations = "./migrations")]
async fn test_sweep_waits_for_the_live_owner_before_requeueing(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db
        .queue
        .enqueue_raw(with_config("owned-stuck", |config| {
            config.max_attempts = 2;
            config.timeout = Some(Duration::from_millis(20));
        }))
        .await
        .unwrap()
        .unwrap();
    let worker = Uuid::now_v7();
    db.queue.dequeue(1, worker).await.unwrap();
    db.queue.write_worker_info(worker, json!({}), None, Duration::from_secs(30)).await.unwrap();
    backdate_job_liveness(&db, id).await;

    let mut sweeper = db.queue.sweeper();
    assert_eq!(sweeper.sweep().await.unwrap().cancelling, vec![id]);
    let report = sweeper.sweep().await.unwrap();
    assert!(report.swept.is_empty(), "the owner lease is still live");
    assert_eq!(db.queue.fetch_job(id).await.unwrap().unwrap().status, JobStatus::Aborting);

    db.queue.write_worker_info(worker, json!({}), None, Duration::ZERO).await.unwrap();
    assert_eq!(sweeper.sweep().await.unwrap().swept, vec![id]);
    assert_eq!(db.queue.fetch_job(id).await.unwrap().unwrap().status, JobStatus::Queued);
    sweeper.release().await;
}

#[sqlx::test(migrations = "./migrations")]
async fn test_sweep_keeps_a_keyed_stuck_job_owned_until_the_worker_lease_expires(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db
        .queue
        .enqueue_raw(new_job("stuck", |job| {
            job.dedupe_key = Some("stuck:once".into());
            job.config.max_attempts = 1;
            job.config.timeout = Some(Duration::from_millis(20));
        }))
        .await
        .unwrap()
        .unwrap();
    let worker = Uuid::now_v7();
    db.queue.dequeue(1, worker).await.unwrap();
    db.queue.write_worker_info(worker, json!({}), None, Duration::from_secs(30)).await.unwrap();
    backdate_job_liveness(&db, id).await;

    let mut sweeper = db.queue.sweeper();
    let report = sweeper.sweep().await.unwrap();
    assert_eq!(report.cancelling, vec![id]);
    let report = sweeper.sweep().await.unwrap();
    assert!(report.swept.is_empty(), "the owner lease is still live");
    assert_eq!(db.queue.fetch_job(id).await.unwrap().unwrap().status, JobStatus::Aborting);

    db.queue.write_worker_info(worker, json!({}), None, Duration::ZERO).await.unwrap();
    let report = sweeper.sweep().await.unwrap();
    assert_eq!(report.swept, vec![id]);
    let row = db.queue.fetch_job(id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Aborted);
    assert_eq!(row.error.as_deref(), Some("swept"));
    sweeper.release().await;
}

#[sqlx::test(migrations = "./migrations")]
async fn test_sweep_batch_skips_live_dedupe_blocker_and_recovers_unkeyed_job(pool: PgPool) {
    let db = TestDb::with(pool.clone(), |builder| builder.sweep_batch_size(1)).await;
    let exclusive = db
        .queue
        .enqueue_raw(new_job("exclusive", |job| {
            job.dedupe_key = Some("singleton".into());
            job.config.max_attempts = 1;
            job.config.timeout = Some(Duration::from_millis(20));
        }))
        .await
        .unwrap()
        .unwrap();
    let unkeyed = db
        .queue
        .enqueue_raw(with_config("unkeyed", |config| {
            config.max_attempts = 1;
            config.timeout = Some(Duration::from_millis(20));
        }))
        .await
        .unwrap()
        .unwrap();
    let worker = Uuid::now_v7();
    assert_eq!(db.queue.dequeue(2, worker).await.unwrap().len(), 2);
    db.queue.write_worker_info(worker, json!({}), None, Duration::from_secs(30)).await.unwrap();
    sqlx::query(
        "UPDATE pgqueue.jobs
         SET started_at = now()
             - CASE WHEN id = $1 THEN interval '2 seconds' ELSE interval '1 second' END,
             touched_at = now()
             - CASE WHEN id = $1 THEN interval '2 seconds' ELSE interval '1 second' END
         WHERE id IN ($1, $2)",
    )
    .bind(exclusive)
    .bind(unkeyed)
    .execute(db.queue.pool())
    .await
    .unwrap();

    let mut sweeper = db.queue.sweeper();
    assert_eq!(sweeper.sweep().await.unwrap().cancelling, vec![exclusive]);
    assert_eq!(sweeper.sweep().await.unwrap().cancelling, vec![unkeyed]);
    assert_eq!(sweeper.sweep().await.unwrap().swept, vec![unkeyed]);
    assert_eq!(db.queue.fetch_job(exclusive).await.unwrap().unwrap().status, JobStatus::Aborting);

    db.queue.write_worker_info(worker, json!({}), None, Duration::ZERO).await.unwrap();
    assert_eq!(sweeper.sweep().await.unwrap().swept, vec![exclusive]);
    sweeper.release().await;
}

#[sqlx::test(migrations = "./migrations")]
async fn test_user_abort_reason_cannot_forge_the_sweeper_marker(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db
        .queue
        .enqueue_raw(with_config("ab", |c| {
            c.max_attempts = 5; // retryable, but aborting jobs must not be retried
            c.timeout = Some(Duration::from_millis(20));
        }))
        .await
        .unwrap()
        .unwrap();
    db.queue.dequeue(1, Uuid::now_v7()).await.unwrap();
    db.queue.abort_job(id, "swept").await.unwrap();
    backdate_job_liveness(&db, id).await;

    let mut sweeper = db.queue.sweeper();
    let report = sweeper.sweep().await.unwrap();
    assert_eq!(report.swept, vec![id]);
    let row = db.queue.fetch_job(id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Aborted);
    assert_eq!(row.error.as_deref(), Some("swept"));
    sweeper.release().await;
}

/// The other half of the same forgery. The sweeper's marker is a *pair*, and
/// the test above closes only the `error` half: an abort used to leave a
/// `running` row's `result` exactly as it found it, so a foreign SQL writer that
/// had planted `"pgqueue:swept"` there let any caller complete the pair with
/// `abort_job(id, "swept")` — and the sweeper then read the operator's abort as
/// its own recovery request and requeued the job to run again instead of
/// finishing it. The abort clears `result` unconditionally, which costs nothing
/// (no library path leaves a meaningful `result` on a `queued` or `running` row)
/// and makes the pair unforgeable from the public API.
#[sqlx::test(migrations = "./migrations")]
async fn test_a_planted_result_sentinel_cannot_turn_an_abort_into_a_retry(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db
        .queue
        .enqueue_raw(with_config("planted", |c| {
            c.max_attempts = 5;
            c.timeout = Some(Duration::from_millis(20));
        }))
        .await
        .unwrap()
        .unwrap();
    db.queue.dequeue(1, Uuid::now_v7()).await.unwrap();
    // The half a foreign writer controls.
    sqlx::query(r#"UPDATE pgqueue.jobs SET result = '"pgqueue:swept"' WHERE id = $1"#)
        .bind(id)
        .execute(&pool)
        .await
        .unwrap();
    // The half the public API controls.
    db.queue.abort_job(id, "swept").await.unwrap();
    backdate_job_liveness(&db, id).await;

    let mut sweeper = db.queue.sweeper();
    sweeper.sweep().await.unwrap();
    let row = db.queue.fetch_job(id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Aborted, "a user abort was resurrected as a sweeper retry");
    sweeper.release().await;
}

/// The mirror image of the forgery test above: an abort requested while the
/// row sits in the sweeper-marked `aborting` window claims the row —
/// converting the sweeper's pending retry into a user abort — instead of
/// being dropped for that retry to run the job again.
#[sqlx::test(migrations = "./migrations")]
async fn test_user_abort_claims_a_sweeper_marked_aborting_job(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db
        .queue
        .enqueue_raw(with_config("stuck", |c| {
            c.max_attempts = 3;
            c.timeout = Some(Duration::from_millis(20));
        }))
        .await
        .unwrap()
        .unwrap();
    // A live lease, so phase one leaves the row in the marked `aborting`
    // window for the abort below to claim rather than recovering it outright.
    let owner = Uuid::now_v7();
    db.queue.write_worker_info(owner, json!({}), None, Duration::from_secs(60)).await.unwrap();
    let active = db.queue.dequeue(1, owner).await.unwrap().remove(0);
    backdate_job_liveness(&db, id).await;

    let mut sweeper = db.queue.sweeper();
    // Phase 1: the sweeper marks the stuck row 'aborting' for a retry.
    assert_eq!(sweeper.sweep().await.unwrap().cancelling, vec![id]);

    assert!(
        db.queue.abort_job(id, "operator said stop").await.unwrap(),
        "the abort claims the sweeper-marked row instead of being dropped"
    );
    let row = db.queue.fetch_job(id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Aborting);
    assert_eq!(row.error.as_deref(), Some("operator said stop"));
    assert!(row.result.is_none(), "the sweeper's marker is cleared");

    // A success landing after the claim must not overwrite the abort...
    assert!(!db.queue.finish(&active, JobStatus::Complete, Some(json!("too late")), None).await.unwrap());
    // ...and the next sweep finishes the abort instead of requeueing the job.
    assert_eq!(sweeper.sweep().await.unwrap().swept, vec![id]);
    let row = db.queue.fetch_job(id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Aborted);
    assert_eq!(row.error.as_deref(), Some("operator said stop"));
    assert_eq!(db.queue.stats().retried, 0, "the sweeper retry never ran");
    sweeper.release().await;
}

#[sqlx::test(migrations = "./migrations")]
async fn test_sweep_leadership_is_exclusive_per_queue(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let other = db.another_queue(|b| b).await;

    let mut leader = db.queue.sweeper();
    let report = leader.sweep().await.unwrap();
    assert!(report.leader);
    assert!(leader.is_leader());

    let mut follower = other.sweeper();
    let report = follower.sweep().await.unwrap();
    assert!(!report.leader, "second sweeper must not get the lock");
    assert!(!follower.is_leader());

    // Leadership hands over once released.
    leader.release().await;
    assert!(!leader.is_leader());
    let report = follower.sweep().await.unwrap();
    assert!(report.leader);
    follower.release().await;
}

//noinspection SqlNoDataSourceInspection
#[sqlx::test(migrations = "./migrations")]
async fn test_sweep_leadership_recovers_after_its_backend_is_terminated(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let other = db.another_queue(|builder| builder).await;
    let mut stale = db.queue.sweeper();
    assert!(stale.sweep().await.unwrap().leader);

    let key = pgqueue::__test_support::sweep_lock_key(&db.database, db.queue.name()) as u64;
    let class_id = (key >> 32) as u32 as i64;
    let object_id = key as u32 as i64;
    let pid = sqlx::query_scalar::<_, i32>(
        "SELECT pid FROM pg_locks
         WHERE locktype = 'advisory' AND classid::bigint = $1 AND objid::bigint = $2
           AND objsubid = 1 AND granted",
    )
    .bind(class_id)
    .bind(object_id)
    .fetch_one(db.queue.pool())
    .await
    .unwrap();
    assert!(
        sqlx::query_scalar::<_, bool>("SELECT pg_terminate_backend($1)")
            .bind(pid)
            .fetch_one(db.queue.pool())
            .await
            .unwrap()
    );

    let mut replacement = other.sweeper();
    for _ in 0..20 {
        if replacement.sweep().await.unwrap().leader {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(replacement.is_leader(), "replacement never acquired leadership");

    let report = stale.sweep().await.unwrap();
    assert!(!report.leader, "stale session must revalidate before sweeping");
    assert!(!stale.is_leader());
    replacement.release().await;
}

#[sqlx::test(migrations = "./migrations")]
async fn test_distinct_queue_names_do_not_share_sweep_leadership(pool: PgPool) {
    let db = TestDb::with(pool.clone(), |builder| builder.name("133665")).await;
    let other = db.another_queue(|builder| builder.name("27472")).await;
    let mut first = db.queue.sweeper();
    let mut second = other.sweeper();
    assert!(first.sweep().await.unwrap().leader);
    assert!(second.sweep().await.unwrap().leader);
    first.release().await;
    second.release().await;
}

#[sqlx::test(migrations = "./migrations")]
async fn test_queues_are_isolated_by_name(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let other = db.another_queue(|b| b.name("other")).await;

    let id = db.queue.enqueue_raw(new_job("j", |_| {})).await.unwrap().unwrap();
    assert!(other.dequeue(10, Uuid::now_v7()).await.unwrap().is_empty());
    assert_eq!(other.counts().await.unwrap().queued, 0);
    assert_eq!(db.queue.counts().await.unwrap().queued, 1);

    // UUIDs are not authorization: every id-based operation must remain
    // scoped to the queue handle that owns the row.
    assert!(other.fetch_job(id).await.unwrap().is_none());
    assert!(!other.abort_job(id, "cross-queue").await.unwrap());
    assert!(!other.retry_job(id, "cross-queue").await.unwrap());

    let active = db.queue.dequeue(1, Uuid::now_v7()).await.unwrap().remove(0);
    assert!(matches!(other.retry(&active, "cross-queue").await, Err(Error::Config(_))));
    assert!(matches!(other.finish(&active, JobStatus::Complete, Some(json!(null)), None).await, Err(Error::Config(_))));
    assert_eq!(db.queue.fetch_job(id).await.unwrap().unwrap().status, JobStatus::Running);

    let worker_id = Uuid::now_v7();
    db.queue.write_worker_info(worker_id, json!({"owner": "default"}), None, Duration::from_secs(5)).await.unwrap();
    let err =
        other.write_worker_info(worker_id, json!({"owner": "other"}), None, Duration::from_secs(5)).await.unwrap_err();
    assert!(matches!(err, Error::Config(_)), "{err}");
    assert!(list_workers(&other).await.is_empty());
}

#[sqlx::test(migrations = "./migrations")]
async fn test_builder_accepts_external_pool(pool: PgPool) {
    crate::init_tracing();
    let queue = Queue::builder("ignored-when-pool-is-set")
        .pool(pool.clone())
        .connections(1, 2) // ignored, but exercised
        .connect()
        .await
        .unwrap();
    queue.enqueue_raw(new_job("j", |_| {})).await.unwrap();
    assert_eq!(queue.counts().await.unwrap().queued, 1);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_dropping_a_sweeper_releases_leadership(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let other = db.another_queue(|b| b).await;

    let mut leader = db.queue.sweeper();
    assert!(leader.sweep().await.unwrap().leader);
    drop(leader); // no explicit release: Drop must close the lock connection

    // Mutex-wrapped so the polling closure can re-borrow the sweeper mutably.
    let follower = tokio::sync::Mutex::new(other.sweeper());
    wait_until(Duration::from_secs(5), Duration::from_millis(50), "leadership never released", || async {
        follower.lock().await.sweep().await.unwrap().leader
    })
    .await;
    follower.into_inner().release().await;
}

/// The cooperative abort window is `sweep_grace` measured from the abort mark,
/// not the accident of when the next pass happens to run: a drain that repeats
/// a full batch's pass within milliseconds used to recover rows the previous
/// pass had only just marked, cancelling and re-running attempts whose
/// stalled-but-live owners were still inside the window the mark exists to
/// grant.
#[sqlx::test(migrations = "./migrations")]
async fn test_back_to_back_sweeps_leave_a_marked_owner_its_whole_grace(pool: PgPool) {
    let db = TestDb::with(pool.clone(), |builder| builder.sweep_grace(Duration::from_secs(1))).await;
    let worker_id = Uuid::now_v7();
    db.queue
        .enqueue_raw(with_config("cooperative-window", |config| {
            config.max_attempts = 3;
            config.timeout = Some(Duration::from_millis(20));
        }))
        .await
        .unwrap()
        .unwrap();
    db.queue.write_worker_lease(worker_id, json!({}), None, Duration::from_secs(30), true).await.unwrap();
    let attempt = db.queue.dequeue(1, worker_id).await.unwrap().remove(0);
    backdate_job_liveness(&db, attempt.id).await;
    // The owner's heartbeat stalled: its lease lapsed moments ago, so the
    // attempt is recoverable — but the lease *row* is still on disk (the purge
    // removes one only two graces after expiry), which is exactly the
    // stalled-not-dead state the window exists for.
    sqlx::query("UPDATE pgqueue.workers SET expires_at = now() - interval '100 milliseconds' WHERE id = $1")
        .bind(worker_id)
        .execute(db.queue.pool())
        .await
        .unwrap();

    let mut sweeper = db.queue.sweeper();
    let first = sweeper.sweep().await.unwrap();
    assert_eq!(first.cancelling, [attempt.id], "the first pass asks the attempt to abort");
    assert_eq!(db.queue.fetch_job(attempt.id).await.unwrap().unwrap().status, JobStatus::Aborting);

    // A drain's cadence: the very next pass. The mark is milliseconds old, so
    // the owner keeps the row for the rest of its grace.
    let second = sweeper.sweep().await.unwrap();
    assert!(second.swept.is_empty(), "a back-to-back pass must not take the row inside the grace: {second:?}");
    assert_eq!(db.queue.fetch_job(attempt.id).await.unwrap().unwrap().status, JobStatus::Aborting);

    // Past the grace, the first pass to look recovers it.
    tokio::time::sleep(Duration::from_millis(1300)).await;
    let third = sweeper.sweep().await.unwrap();
    sweeper.release().await;
    assert_eq!(third.swept, [attempt.id]);
    let recovered = db.queue.fetch_job(attempt.id).await.unwrap().unwrap();
    assert_eq!(recovered.status, JobStatus::Queued);
    assert_eq!(recovered.attempts, 1, "recovery requeues rather than spending the attempt");
}

/// A pass wedged on a connection that will never answer is invisible to the
/// consecutive-failure release — that only counts passes that *return* — while
/// the liveness probe on the dedicated leadership connection stays healthy, so
/// sweeping stopped cluster-wide for as long as the socket sat there. The pass
/// deadline abandons it, reports it as a failed pass, and releases leadership
/// so a healthy peer can take over.
#[sqlx::test(migrations = "./migrations")]
async fn test_a_wedged_sweep_pass_releases_leadership_at_its_deadline(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    // Leadership is taken under the *shipped* deadline. The short override
    // below is for the wedged pass; applying it to this healthy one made the
    // test fail on a loaded machine, where an ordinary pass — a pooled acquire
    // plus four statements — can outlast 300ms and be reported as the wedge it
    // is not. The shipped bound is 60s precisely because "a saturated pool
    // spends [30s] before the query even starts".
    let mut sweeper = db.queue.sweeper();
    assert!(sweeper.sweep().await.unwrap().leader);
    assert!(sweeper.is_leader());
    let mut sweeper = pgqueue::__test_support::sweeper_pass_deadline(sweeper, Duration::from_millis(300));

    // Wedge the next pass: an exclusive table lock parks its first statement
    // indefinitely, which is what a black-holed connection looks like from the
    // waiting side.
    let mut wedge = db.pool.begin().await.unwrap();
    sqlx::query("LOCK TABLE pgqueue.jobs IN ACCESS EXCLUSIVE MODE").execute(&mut *wedge).await.unwrap();

    let error = sweeper.sweep().await.unwrap_err();
    assert!(matches!(error, Error::WorkerTask(_)), "{error}");
    assert!(!sweeper.is_leader(), "a wedged pass must surrender leadership");

    // Surrendered means surrendered: once the wedge clears, a peer acquires
    // leadership on its first pass instead of waiting out a phantom leader.
    wedge.rollback().await.unwrap();
    let mut peer = db.another_queue(|builder| builder).await.sweeper();
    assert!(peer.sweep().await.unwrap().leader, "the released lock must be free for a peer");
    peer.release().await;
}

#[sqlx::test(migrations = "./migrations")]
async fn test_queue_accessors_and_debug_reflect_configuration(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    assert_eq!(db.queue.name(), "default");
    let debug = format!("{:?}", db.queue);
    assert!(debug.contains("Queue"));
    assert!(debug.contains("default"));
}

#[sqlx::test(migrations = "./migrations")]
async fn test_stale_attempts_cannot_finalize_newer_ones(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    db.queue
        .enqueue_raw(with_config("dup", |c| {
            c.max_attempts = 3;
            c.timeout = Some(Duration::from_millis(20));
        }))
        .await
        .unwrap()
        .unwrap();
    // Worker A dequeues attempt 1, then goes silent past its timeout.
    let attempt_a = db.queue.dequeue(1, Uuid::now_v7()).await.unwrap().remove(0);
    backdate_job_liveness(&db, attempt_a.id).await;

    // The sweeper recovers the job (two passes) and worker B picks it up.
    let mut sweeper = db.queue.sweeper();
    sweeper.sweep().await.unwrap();
    sweeper.sweep().await.unwrap();
    sweeper.release().await;
    let attempt_b = db.queue.dequeue(1, Uuid::now_v7()).await.unwrap().remove(0);
    assert_eq!(attempt_b.id, attempt_a.id);
    assert_eq!(attempt_b.attempts, 2);

    // Worker A wakes up: its stale attempt must not mutate the row.
    assert!(
        !db.queue.finish(&attempt_a, JobStatus::Complete, Some(json!("stale")), None).await.unwrap(),
        "stale finish must be refused"
    );
    assert!(!db.queue.retry(&attempt_a, "stale").await.unwrap(), "stale retry must be refused");
    let row = db.queue.fetch_job(attempt_a.id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Running, "attempt 2 still owns the row");
    assert_eq!(row.attempts, 2);
    assert!(row.result.is_none());

    // The current attempt finalizes normally.
    assert!(db.queue.finish(&attempt_b, JobStatus::Complete, Some(json!("fresh")), None).await.unwrap());
}

#[sqlx::test(migrations = "./migrations")]
async fn test_failed_attempt_of_an_aborting_job_honors_the_abort(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db.queue.enqueue_raw(with_config("ab", |c| c.max_attempts = 5)).await.unwrap().unwrap();
    let active = db.queue.dequeue(1, Uuid::now_v7()).await.unwrap().remove(0);
    assert!(db.queue.abort_job(id, "user said stop").await.unwrap());

    // The attempt fails while the abort is pending: retry must refuse...
    assert!(!db.queue.retry(&active, "failed: transient").await.unwrap());
    // ...and finishing as aborted (error: None) preserves the abort reason.
    assert!(db.queue.finish(&active, JobStatus::Aborted, None, None).await.unwrap());
    let row = db.queue.fetch_job(id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Aborted);
    assert_eq!(row.error.as_deref(), Some("user said stop"));
}

#[sqlx::test(migrations = "./migrations")]
async fn test_dead_workers_unbounded_jobs_are_recovered(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db
        .queue
        .enqueue_raw(with_config("unbounded", |c| {
            c.max_attempts = 2;
            c.timeout = None; // no timeout, no heartbeat: no self-deadline
        }))
        .await
        .unwrap()
        .unwrap();
    // Dequeued by a worker that never heartbeats (crashed instantly).
    db.queue.dequeue(1, Uuid::now_v7()).await.unwrap();

    let mut sweeper = db.queue.sweeper();
    let report = sweeper.sweep().await.unwrap();
    assert_eq!(report.cancelling, vec![id], "dead-worker job detected as stuck");
    assert_eq!(
        report.swept,
        vec![id],
        "and recovered in the same pass: a worker that crashed instantly has no \
         cooperative abort left to wait a sweep interval for"
    );
    let row = db.queue.fetch_job(id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Queued, "recovered for retry");
    sweeper.release().await;
}

/// Backdates `worker_id`'s lease so it expired `seconds` ago.
async fn expire_worker_lease(db: &TestDb, worker_id: Uuid, seconds: i64) {
    sqlx::query(
        "UPDATE pgqueue.workers
         SET expires_at = now() - ($2::bigint * interval '1 second'),
             heartbeat_at = now() - ($2::bigint * interval '1 second')
         WHERE id = $1",
    )
    .bind(worker_id)
    .bind(seconds)
    .execute(db.queue.pool())
    .await
    .expect("expire worker lease");
}

/// A lapsed lease is not a dead worker, and recovering on that evidence alone
/// ran two handler bodies on one job.
///
/// `pgqueue.job_is_stuck`'s second trigger exists for an attempt whose lease
/// lapsed while it is *still in flight* — a workers-row lock wait, a pool stall,
/// a GC pause or a failover outlasting the lease TTL, which is exactly what
/// `QueueBuilder::sweep_grace` documents. Recovering such a row in the pass that
/// marked it requeued it, cleared its owner and woke every idle fetcher, so a
/// second worker claimed it as the next attempt within milliseconds while the
/// first worker's handler was still running. The row guards refuse the loser's
/// writes; nothing refuses its side effects.
#[sqlx::test(migrations = "./migrations")]
async fn test_a_stalled_workers_lapsed_lease_keeps_its_cooperative_abort_window(pool: PgPool) {
    // A nonzero grace (unlike the harness default) is what sizes the window.
    let db = TestDb::with(pool.clone(), |builder| builder.sweep_grace(Duration::from_secs(30))).await;
    let id = db
        .queue
        .enqueue_raw(with_config("stalled", |c| {
            c.max_attempts = 2;
            c.timeout = None;
        }))
        .await
        .unwrap()
        .unwrap();
    let worker_id = Uuid::now_v7();
    db.queue.dequeue(1, worker_id).await.unwrap();
    db.queue.write_worker_info(worker_id, json!({}), None, Duration::from_secs(30)).await.unwrap();
    backdate_job_liveness(&db, id).await;
    // Expired by one grace: recoverable, which is the whole point of the second
    // trigger — but not by the second grace that separates an owner that is gone
    // from one that is merely slow.
    expire_worker_lease(&db, worker_id, 40).await;

    let mut sweeper = db.queue.sweeper();
    let report = sweeper.sweep().await.unwrap();
    assert_eq!(report.cancelling, vec![id], "the attempt is stuck");
    assert!(
        report.swept.is_empty(),
        "a worker that may still be running the handler must be asked to stop \
         before its attempt is taken away, not in the same breath"
    );
    let row = db.queue.fetch_job(id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Aborting);
    assert_eq!(row.worker_id, Some(worker_id), "the owner still names the row its abort poll looks itself up by");
    assert_eq!(row.attempts, 1, "no second attempt was handed out");

    // The window costs one sweep interval, not the job: once the lease is past
    // the second grace, the next pass recovers it.
    expire_worker_lease(&db, worker_id, 90).await;
    assert_eq!(sweeper.sweep().await.unwrap().swept, vec![id]);
    assert_eq!(db.queue.fetch_job(id).await.unwrap().unwrap().status, JobStatus::Queued);
    sweeper.release().await;
}

/// A pass that fills its stuck-job batch has more work only if it changed something. Marked rows still inside their
/// owner's cooperative window come back from every scan until the window passes, while both recovery batches refuse
/// them, so reporting a full batch of them as unfinished made every drain repeat the whole scan until its budget ran
/// out — and, with a grace longer than a few sweep intervals, report the sweeper as falling behind.
#[sqlx::test(migrations = "./migrations")]
async fn test_a_full_batch_of_attempts_inside_their_abort_window_is_not_more_work(pool: PgPool) {
    let db =
        TestDb::with(pool.clone(), |builder| builder.sweep_grace(Duration::from_secs(30)).sweep_batch_size(2)).await;
    let worker_id = Uuid::now_v7();
    let mut ids = Vec::new();
    for _ in 0..2 {
        ids.push(
            db.queue
                .enqueue_raw(with_config("windowed", |c| {
                    c.max_attempts = 2;
                    c.timeout = None;
                }))
                .await
                .unwrap()
                .unwrap(),
        );
    }
    assert_eq!(db.queue.dequeue(2, worker_id).await.unwrap().len(), 2);
    db.queue.write_worker_info(worker_id, json!({}), None, Duration::from_secs(30)).await.unwrap();
    for id in &ids {
        backdate_job_liveness(&db, *id).await;
    }
    // Recoverable, but with the lease row still on disk: the owner keeps its window.
    expire_worker_lease(&db, worker_id, 40).await;

    let mut sweeper = db.queue.sweeper();
    let marked = sweeper.sweep().await.unwrap();
    assert_eq!(marked.cancelling.len(), 2);
    assert!(marked.has_more_work(), "a full batch that marked rows may have more behind it");

    let waiting = sweeper.sweep_operations(marked.unfinished).await.unwrap();
    assert!(waiting.cancelling.is_empty() && waiting.swept.is_empty(), "the window was not honoured");
    assert!(!waiting.has_more_work(), "a full batch nothing could be done with was reported as more work");
    sweeper.release().await;
}

/// The other side of the same test: a lease that has been expired for longer
/// than the abort window *is* evidence the owner is gone, so the pass that marks
/// the attempt also recovers it. Deferring that cost a SIGKILLed worker's job up
/// to two extra sweep intervals of holding its dedupe key — silently
/// deduplicating every re-enqueue and cron occurrence under it.
///
/// Unlike `test_dead_workers_unbounded_jobs_are_recovered`, the worker here got
/// as far as writing a lease, so the `pgqueue.workers` row is present and its
/// age is what has to be read.
#[sqlx::test(migrations = "./migrations")]
async fn test_a_long_lapsed_lease_is_marked_and_recovered_in_one_pass(pool: PgPool) {
    let db = TestDb::with(pool.clone(), |builder| builder.sweep_grace(Duration::from_secs(30))).await;
    let id = db
        .queue
        .enqueue_raw(with_config("killed", |c| {
            c.max_attempts = 2;
            c.timeout = None;
        }))
        .await
        .unwrap()
        .unwrap();
    let worker_id = Uuid::now_v7();
    db.queue.dequeue(1, worker_id).await.unwrap();
    db.queue.write_worker_info(worker_id, json!({}), None, Duration::from_secs(30)).await.unwrap();
    backdate_job_liveness(&db, id).await;
    expire_worker_lease(&db, worker_id, 90).await;

    let mut sweeper = db.queue.sweeper();
    let report = sweeper.sweep().await.unwrap();
    assert_eq!(report.cancelling, vec![id]);
    assert_eq!(
        report.swept,
        vec![id],
        "an owner that has missed the whole abort window has no cooperative \
         abort left to wait a sweep interval for"
    );
    let row = db.queue.fetch_job(id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Queued, "recovered for retry");
    assert_eq!(row.worker_id, None);
    sweeper.release().await;
}

/// The lease *clock* cannot tell a dead owner from a live one whose heartbeat
/// is blocked, because the two are not blocked by the same things.
/// `write_worker_info` is an `INSERT ... ON CONFLICT DO UPDATE` on
/// `pgqueue.workers`; `Database::aborting_of` is a plain `SELECT` on
/// `pgqueue.jobs`. An operator's transaction left open over
/// `SELECT ... FROM pgqueue.workers ... FOR UPDATE` stops every heartbeat and
/// nothing else, so the lease ages without bound while the worker runs on and
/// reads its abort flag perfectly well — and recovering on that age handed the
/// row to a second worker with zero `WorkerTimers::abort` ticks of warning.
///
/// The lock is what the evidence reads now: `Sweeper::purge_worker_leases` takes
/// its batch `FOR UPDATE SKIP LOCKED`, so a locked lease survives the pass, and
/// a lease that survives is an owner that is not gone.
#[sqlx::test(migrations = "./migrations")]
async fn test_a_row_locked_lease_keeps_its_owners_cooperative_abort_window(pool: PgPool) {
    let db = TestDb::with(pool.clone(), |builder| builder.sweep_grace(Duration::from_secs(30))).await;
    let id = db
        .queue
        .enqueue_raw(with_config("heartbeat-blocked", |c| {
            c.max_attempts = 2;
            c.timeout = None;
        }))
        .await
        .unwrap()
        .unwrap();
    let worker_id = Uuid::now_v7();
    db.queue.dequeue(1, worker_id).await.unwrap();
    db.queue.write_worker_info(worker_id, json!({}), None, Duration::from_secs(30)).await.unwrap();
    backdate_job_liveness(&db, id).await;
    // Well past the purge threshold: on age alone this owner reads as gone.
    expire_worker_lease(&db, worker_id, 90).await;

    // The operator's open transaction, holding the one row the worker's
    // heartbeat writes and none of the rows the sweep reads.
    let mut operator = db.queue.pool().begin().await.unwrap();
    sqlx::query("SELECT id FROM pgqueue.workers WHERE id = $1 FOR UPDATE")
        .bind(worker_id)
        .fetch_one(&mut *operator)
        .await
        .unwrap();

    let mut sweeper = db.queue.sweeper();
    let report = sweeper.sweep().await.unwrap();
    assert_eq!(report.cancelling, vec![id], "the attempt is stuck");
    assert!(
        report.swept.is_empty(),
        "a worker whose heartbeat is blocked on one row lock is alive, and must \
         be asked to stop before its attempt is taken away"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*)::bigint FROM pgqueue.workers WHERE id = $1")
            .bind(worker_id)
            .fetch_one(db.queue.pool())
            .await
            .unwrap(),
        1,
        "SKIP LOCKED left the lease on disk, which is what denied `owner_gone`"
    );
    let row = db.queue.fetch_job(id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Aborting);
    assert_eq!(row.worker_id, Some(worker_id), "the owner still names the row its abort poll looks itself up by");
    assert_eq!(row.attempts, 1, "no second attempt was handed out");

    // The lock costs one sweep interval, not the job: released, the lease is
    // purged and the next pass recovers the attempt.
    operator.rollback().await.unwrap();
    assert_eq!(sweeper.sweep().await.unwrap().swept, vec![id]);
    assert_eq!(db.queue.fetch_job(id).await.unwrap().unwrap().status, JobStatus::Queued);
    sweeper.release().await;
}

/// Worker leases outlive the abort window, because their absence is what the
/// window's second grace is read from: purging at one grace deleted the row that
/// said "this lease lapsed only moments ago" in the very pass that needed it,
/// and a purged lease is indistinguishable from one that never existed.
#[sqlx::test(migrations = "./migrations")]
async fn test_worker_leases_outlive_the_cooperative_abort_window(pool: PgPool) {
    let db = TestDb::with(pool.clone(), |builder| builder.sweep_grace(Duration::from_secs(30))).await;
    let worker_id = Uuid::now_v7();
    db.queue.write_worker_info(worker_id, json!({}), None, Duration::from_secs(30)).await.unwrap();
    expire_worker_lease(&db, worker_id, 40).await;

    let live = || async {
        sqlx::query_scalar::<_, i64>("SELECT count(*)::bigint FROM pgqueue.workers WHERE id = $1")
            .bind(worker_id)
            .fetch_one(db.queue.pool())
            .await
            .unwrap()
    };
    let mut sweeper = db.queue.sweeper();
    sweeper.sweep().await.unwrap();
    assert_eq!(live().await, 1, "one grace past expiry is still on disk");

    expire_worker_lease(&db, worker_id, 90).await;
    sweeper.sweep().await.unwrap();
    assert_eq!(live().await, 0, "past the window it is purged");
    sweeper.release().await;
}

//noinspection SqlNoDataSourceInspection
#[sqlx::test(migrations = "./migrations")]
async fn test_leaseless_unbounded_jobs_are_recovered_after_the_sweep_grace(pool: PgPool) {
    // A nonzero grace (unlike the harness default) makes the no-deadline
    // arm's liveness window observable.
    let db = TestDb::with(pool.clone(), |builder| builder.sweep_grace(Duration::from_secs(30))).await;
    let id = db
        .queue
        .enqueue_raw(with_config("unbounded", |c| {
            c.max_attempts = 2;
            c.timeout = None;
        }))
        .await
        .unwrap()
        .unwrap();
    // Public dequeue: this consumer never writes a workers row.
    db.queue.dequeue(1, Uuid::now_v7()).await.unwrap();

    let backdate_lifecycle = |milliseconds: i64| {
        let pool = db.queue.pool().clone();
        async move {
            sqlx::query(
                "UPDATE pgqueue.jobs
                 SET touched_at = now() - $2::bigint * interval '1 millisecond'
                 WHERE id = $1",
            )
            .bind(id)
            .bind(milliseconds)
            .execute(&pool)
            .await
            .unwrap();
        }
    };

    let mut sweeper = db.queue.sweeper();
    let report = sweeper.sweep().await.unwrap();
    assert!(report.cancelling.is_empty(), "a freshly dequeued no-deadline job is inside the liveness grace");

    // Past the grace with no worker lease, the attempt is recovered.
    backdate_lifecycle(60_000).await;
    assert_eq!(sweeper.sweep().await.unwrap().cancelling, vec![id]);
    sweeper.release().await;
}

#[sqlx::test(migrations = "./migrations")]
async fn test_another_queues_worker_cannot_keep_an_unbounded_job_alive(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let other = db.another_queue(|builder| builder.name("other")).await;
    db.queue
        .enqueue_raw(with_config("unbounded", |config| {
            config.max_attempts = 2;
            config.timeout = None;
        }))
        .await
        .unwrap();
    let worker_id = Uuid::now_v7();
    let active = db.queue.dequeue(1, worker_id).await.unwrap().remove(0);
    other.write_worker_info(worker_id, json!({}), None, Duration::from_secs(30)).await.unwrap();

    let mut sweeper = db.queue.sweeper();
    let report = sweeper.sweep().await.unwrap();
    assert_eq!(report.cancelling, vec![active.id]);
    sweeper.release().await;
}

#[sqlx::test(migrations = "./migrations")]
async fn test_live_workers_unbounded_jobs_are_not_swept(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    db.queue.enqueue_raw(with_config("unbounded", |c| c.timeout = None)).await.unwrap().unwrap();
    let worker_id = Uuid::now_v7();
    db.queue.dequeue(1, worker_id).await.unwrap();
    // The worker has a live heartbeat row: its job is not stuck.
    db.queue.write_worker_info(worker_id, json!({}), None, Duration::from_secs(60)).await.unwrap();

    let mut sweeper = db.queue.sweeper();
    let report = sweeper.sweep().await.unwrap();
    assert!(report.cancelling.is_empty());
    assert!(report.swept.is_empty());
    sweeper.release().await;
}

#[sqlx::test(migrations = "./migrations")]
async fn test_zero_delay_retries_keep_their_queue_position(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db
        .queue
        .enqueue_raw(with_config("j", |c| {
            c.max_attempts = 3;
            c.retry_delay = Duration::ZERO;
        }))
        .await
        .unwrap()
        .unwrap();
    let original = db.queue.fetch_job(id).await.unwrap().unwrap().scheduled_at;

    let active = db.queue.dequeue(1, Uuid::now_v7()).await.unwrap().remove(0);
    assert!(db.queue.retry(&active, "failed: transient").await.unwrap());
    let row = db.queue.fetch_job(id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Queued);
    assert_eq!(row.scheduled_at, original, "zero-delay retry must not lose its place behind the backlog");
}

#[sqlx::test(migrations = "./migrations")]
async fn test_consumer_dequeue_claims_nothing_without_a_live_lease(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    db.queue.enqueue_raw(new_job("leaseless-consumer", |_| {})).await.unwrap().unwrap();

    // No heartbeat: without a lease the sweeper would treat any claim as
    // abandoned and hand the job to someone else mid-flight, so refuse it.
    let attempts = db.queue.consumer(Uuid::now_v7()).dequeue(10).await.unwrap();
    assert!(attempts.is_empty());
    assert_eq!(db.queue.counts().await.unwrap().queued, 1);

    // With the documented heartbeat first, the same call claims the job.
    let attempts = leased_consumer(&db.queue, Uuid::now_v7()).await.dequeue(10).await.unwrap();
    assert_eq!(attempts.len(), 1);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_sweep_recovers_abandoned_jobs_in_one_statement_per_batch(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    // Statement-level triggers fire once per statement, so this counts round
    // trips rather than rows.
    sqlx::raw_sql(sqlx::AssertSqlSafe(
        "CREATE TABLE pgqueue.stmt_counter (n bigint NOT NULL);
         INSERT INTO pgqueue.stmt_counter VALUES (0);
         CREATE FUNCTION pgqueue.bump_stmt_counter() RETURNS trigger
         LANGUAGE plpgsql AS $$
         BEGIN
             UPDATE pgqueue.stmt_counter SET n = n + 1;
             RETURN NULL;
         END
         $$;
         CREATE TRIGGER count_job_updates
         AFTER UPDATE ON pgqueue.jobs
         FOR EACH STATEMENT
         EXECUTE FUNCTION pgqueue.bump_stmt_counter();"
            .to_string(),
    ))
    .execute(&pool)
    .await
    .unwrap();

    // Half retryable (requeued), half exhausted (aborted): both groups must be
    // recovered in one statement each, not one per row.
    sqlx::query(
        "INSERT INTO pgqueue.jobs
             (queue, name, status, kind, attempts, max_attempts, error, result,
              started_at, touched_at, worker_id)
         SELECT 'default', 'swept', 'aborting', 'job', 1,
                CASE WHEN g % 2 = 0 THEN 5 ELSE 1 END,
                'swept', '\"pgqueue:swept\"',
                now() - interval '1 hour', now() - interval '1 hour', gen_random_uuid()
         FROM generate_series(1, 40) AS g",
    )
    .execute(&pool)
    .await
    .unwrap();

    let mut sweeper = db.queue.sweeper();
    let report = sweeper.sweep().await.unwrap();
    sweeper.release().await;
    assert_eq!(report.swept.len(), 40);

    // The table is created dynamically above and exists for the rest of this test.
    let statements =
        sqlx::query_scalar::<_, i64>(concat!("SELECT n FROM pgqueue.", "stmt_counter")).fetch_one(&pool).await.unwrap();
    assert!(
        statements <= 2,
        "40 abandoned jobs should cost one requeue statement and one abort statement, not {statements}"
    );
}

// `finish_with_guards` is the only statement opening with a bare `candidate`
// CTE; the sweeper's near-identical batch abort opens with `WITH requested AS`.

/// The underfilled-batch probe reports availability in terms of the handler
/// names the worker registered, so it is worthless to a consumer that has none
/// — and paying for it would double the statements an idle consumer issues and
/// turn a failure of that diagnostic query into a failed dequeue.
#[sqlx::test(migrations = "./migrations")]
async fn test_consumer_dequeue_does_not_run_the_worker_only_probe(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let Some(mut stats) = Stats::new(&db.database).await else {
        Stats::skipped("test_consumer_dequeue_does_not_run_the_worker_only_probe");
        return;
    };
    let consumer = leased_consumer(&db.queue, Uuid::now_v7()).await;

    let claims = stats.since_now(DEQUEUE_CLAIM).await;
    let probes = stats.since_now(DEQUEUE_PROBE).await;
    for _ in 0..5 {
        assert!(consumer.dequeue(4).await.unwrap().is_empty());
    }

    assert_eq!(stats.delta(&claims).await, 5, "one claim statement per dequeue");
    assert_eq!(stats.delta(&probes).await, 0, "the consumer path must not pay for the worker's availability probe");
}

/// `JobRequest::validate` NUL-checked only `name` and `dedupe_key`, so a NUL in
/// `payload` or `meta` reached `jsonb` and raised `22P05` — and inside
/// `Queue::enqueue_raw_in` that poisons the caller's transaction, taking their
/// whole unit of work down with it. The same applies to an unbounded `at()` or
/// a Jiff timestamp below PostgreSQL's floor.
#[sqlx::test(migrations = "./migrations")]
async fn test_enqueue_in_a_caller_transaction_rejects_bad_input_without_aborting_it(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let horizon = Timestamp::now() + SignedDuration::from_hours(24 * 365 * 200);
    let below_postgres_floor = "-004713-11-23T23:59:59.999999999Z".parse::<Timestamp>().unwrap();

    for (label, job) in [
        (
            "payload",
            new_job("nul_payload", |job| {
                job.payload = json!({ "note": "bad\0value" });
            }),
        ),
        (
            "meta",
            new_job("nul_meta", |job| {
                job.meta = json!({ "trace": "bad\0value" });
            }),
        ),
        (
            "scheduled_at",
            new_job("far_future", |job| {
                job.scheduled_at = Some(horizon);
            }),
        ),
        (
            "scheduled_at_before_postgres_floor",
            new_job("far_past", |job| {
                job.scheduled_at = Some(below_postgres_floor);
            }),
        ),
    ] {
        let mut transaction = pool.begin().await.unwrap();
        // A statement whose result proves the transaction is still usable
        // after the refused enqueue.
        sqlx::query("CREATE TEMPORARY TABLE repro_unit_of_work (n int) ON COMMIT DROP")
            .execute(&mut *transaction)
            .await
            .unwrap();

        let error = db
            .queue
            .enqueue_raw_in(&mut transaction, job)
            .await
            .expect_err(&format!("{label} must be refused before it reaches SQL"));
        assert!(matches!(error, Error::Config(_)), "{label}: {error:?}");

        // The temporary table is created in this transaction immediately above.
        sqlx::query(concat!("INSERT INTO repro_", "unit_of_work VALUES (1)"))
            .execute(&mut *transaction)
            .await
            .unwrap_or_else(|error| panic!("{label} aborted the caller's transaction: {error}"));
        transaction.commit().await.unwrap();
    }

    // And the same input is refused on the non-transactional path.
    for job in [
        new_job("nul_payload", |job| {
            job.payload = json!(["ok", "bad\0"]);
        }),
        new_job("far_future", |job| {
            job.scheduled_at = Some(horizon);
        }),
        new_job("far_past", |job| {
            job.scheduled_at = Some(below_postgres_floor);
        }),
    ] {
        assert!(matches!(db.queue.enqueue_raw(job).await.unwrap_err(), Error::Config(_)));
    }
    assert_eq!(sqlx::query_scalar::<_, i64>("SELECT count(*) FROM pgqueue.jobs").fetch_one(&pool).await.unwrap(), 0);
}

/// A burst of same-key enqueues collapses onto one row only while that row is
/// live: once it completes, the partial dedupe index frees the key and the next
/// enqueue inserts a second row. Publishing the burst inside one transaction is
/// what makes "one row" an invariant rather than a bet on no worker finishing
/// the first row mid-burst — every later insert conflicts with the uncommitted
/// row the transaction itself made, which no worker can dequeue yet.
#[sqlx::test(migrations = "./migrations")]
async fn test_enqueue_in_deduplicates_against_the_uncommitted_row_of_its_own_transaction(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let mut transaction = pool.begin().await.unwrap();

    let mut ids = Vec::new();
    for index in 0..5 {
        let result = db
            .queue
            .enqueue_raw_in(&mut transaction, new_job("burst", |job| job.dedupe_key = Some("singleton".into())))
            .await
            .unwrap();
        assert_eq!(result.is_enqueued(), index == 0, "only the first insert of the burst may create a row");
        ids.push(result.into_job_id());
    }
    assert!(ids.windows(2).all(|pair| pair[0] == pair[1]), "the burst must collapse onto one job id: {ids:?}");

    transaction.commit().await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM pgqueue.jobs WHERE queue = $1 AND dedupe_key = 'singleton'",
        )
        .bind(db.queue.name())
        .fetch_one(&pool)
        .await
        .unwrap(),
        1
    );
}

/// `result_ttl_ms` encodes NULL as "keep forever" and 0 as "delete on finish".
/// A negative value has no encoding, and the column now refuses one rather
/// than leaving a row that decodes as a *live* zero-length retention.
#[sqlx::test(migrations = "./migrations")]
async fn test_result_ttl_column_refuses_a_negative_retention(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let handle = db.queue.enqueue_raw(new_job("retained", |_| {})).await.unwrap();

    let error = sqlx::query("UPDATE pgqueue.jobs SET result_ttl_ms = -1 WHERE id = $1")
        .bind(handle.job_id())
        .execute(&pool)
        .await
        .expect_err("a negative retention has no encoding");
    assert_eq!(
        error.as_database_error().and_then(|error| error.constraint()),
        Some("jobs_result_ttl_ms_check"),
        "{error}"
    );

    // The two encodings that do exist are still accepted.
    for ttl in [None, Some(0i64), Some(1i64)] {
        sqlx::query("UPDATE pgqueue.jobs SET result_ttl_ms = $2 WHERE id = $1")
            .bind(handle.job_id())
            .bind(ttl)
            .execute(&pool)
            .await
            .unwrap();
    }
}

/// `retry_job` returning `false` because the dedupe key already belongs to a
/// live occurrence is documented public behaviour, but only the
/// `retried_at IS NULL` guard had ever been exercised — the keyed
/// `ON CONFLICT ... DO NOTHING` arm had not. It also has to leave the terminal
/// row untouched: the refusal rolls the whole transaction back, so the one
/// retry that row is entitled to survives for when the key is free again.
#[sqlx::test(migrations = "./migrations")]
async fn test_retry_job_refuses_a_terminal_occurrence_while_its_dedupe_key_is_live(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let keyed = |queue: &Queue| {
        let job = new_job("keyed_retry", |job| job.dedupe_key = Some("held".into()));
        let queue = queue.clone();
        async move { queue.enqueue_raw(job).await.unwrap().job_id() }
    };

    let terminal = keyed(&db.queue).await;
    assert!(db.queue.abort_job(terminal, "make it terminal").await.unwrap());
    // A fresh occurrence now owns the key.
    let holder = keyed(&db.queue).await;
    assert_ne!(holder, terminal);

    assert!(
        !db.queue.retry_job(terminal, "manual retry").await.unwrap(),
        "a terminal occurrence must not be retried while its dedupe key is held"
    );
    let retried_at: Option<jiff_sqlx::Timestamp> =
        sqlx::query_scalar("SELECT retried_at FROM pgqueue.jobs WHERE id = $1")
            .bind(terminal)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(retried_at.is_none(), "the refused retry must roll back, leaving the row retryable");
    let keyed_rows: i64 = sqlx::query_scalar("SELECT count(*) FROM pgqueue.jobs WHERE dedupe_key = 'held'")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(keyed_rows, 2, "no occurrence may be inserted for a held key");

    // Once the holder is terminal too, the retry that was refused lands.
    assert!(db.queue.abort_job(holder, "release the key").await.unwrap());
    assert!(db.queue.retry_job(terminal, "manual retry").await.unwrap());
}

/// `counts()` reports the two retained terminal states an operator watches, and
/// both filters had only ever been asserted as `0`.
#[sqlx::test(migrations = "./migrations")]
async fn test_counts_report_retained_failed_and_aborted_jobs(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let aborted = db.queue.enqueue_raw(new_job("counted_abort", |_| {})).await.unwrap();
    assert!(db.queue.abort_job(aborted.job_id(), "counted").await.unwrap());

    let failed = db.queue.enqueue_raw(new_job("counted_failure", |_| {})).await.unwrap();
    let owner = Uuid::now_v7();
    db.queue.write_worker_info(owner, json!({}), None, Duration::from_secs(30)).await.unwrap();
    let claimed = db.queue.dequeue(1, owner).await.unwrap();
    assert_eq!(claimed.len(), 1);
    assert!(db.queue.finish(&claimed[0], JobStatus::Failed, None, Some("counted")).await.unwrap());

    let counts = db.queue.counts().await.unwrap();
    assert_eq!((counts.failed, counts.aborted, counts.queued, counts.running), (1, 1, 0, 0), "{counts:?}");
    assert_eq!(failed.job_id(), claimed[0].id);
}

/// Every one of the five counters is scoped to its own queue and ignores
/// retained `complete` rows. Only `queued` had ever been asserted across two
/// queues, so a counter that lost its `queue` predicate — each carries its own
/// now that they are separate aggregates — would have counted a neighbour's
/// jobs with the whole suite still green.
#[sqlx::test(migrations = "./migrations")]
async fn test_counts_are_scoped_to_one_queue_and_ignore_completed_jobs(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let empty = db.another_queue(|b| b.name("empty")).await;
    let worker = Uuid::now_v7();

    // One retained row per terminal state, then the two live ones. Each job is
    // claimed before the next is enqueued, so every claim is unambiguous.
    for (name, status) in [("done", JobStatus::Complete), ("boom", JobStatus::Failed)] {
        db.queue.enqueue_raw(new_job(name, |_| {})).await.unwrap();
        let claimed = db.queue.dequeue(1, worker).await.unwrap();
        assert_eq!(claimed.len(), 1);
        assert!(db.queue.finish(&claimed[0], status, None, Some("scoped")).await.unwrap());
    }

    let stopping = db.queue.enqueue_raw(new_job("stopping", |_| {})).await.unwrap().unwrap();
    assert_eq!(db.queue.dequeue(1, worker).await.unwrap().len(), 1);
    assert!(db.queue.abort_job(stopping, "scoped").await.unwrap());

    db.queue.enqueue_raw(new_job("busy", |_| {})).await.unwrap();
    assert_eq!(db.queue.dequeue(1, worker).await.unwrap().len(), 1);

    let cancelled = db.queue.enqueue_raw(new_job("cancelled", |_| {})).await.unwrap().unwrap();
    assert!(db.queue.abort_job(cancelled, "scoped").await.unwrap());

    db.queue.enqueue_raw(new_job("ready", |_| {})).await.unwrap();
    db.queue
        .enqueue_raw(new_job("later", |job| {
            job.scheduled_at = Some(Timestamp::now() + SignedDuration::from_secs(60));
        }))
        .await
        .unwrap();

    let counts = db.queue.counts().await.unwrap();
    assert_eq!(
        (counts.queued, counts.running, counts.scheduled, counts.failed, counts.aborted,),
        (1, 2, 1, 1, 1),
        "the completed row counts nowhere and aborting still occupies a worker: {counts:?}"
    );

    let neighbour = empty.counts().await.unwrap();
    assert_eq!(
        (neighbour.queued, neighbour.running, neighbour.scheduled, neighbour.failed, neighbour.aborted,),
        (0, 0, 0, 0, 0),
        "a queue with no rows of its own reports zeros, not its neighbour's \
         jobs and not NULL: {neighbour:?}"
    );
}

/// The version gate is the first thing `Queue::connect` does, and refusing an
/// older server is the whole reason it exists — every statement this library
/// ships assumes PostgreSQL 18 semantics. It had no test because the test
/// cluster is 18: this one shadows `current_setting` in its own database
/// (`pg_catalog` is only searched first when it is not named explicitly) so the
/// connect path sees a 17 server without needing one.
#[tokio::test]
async fn test_queue_connect_refuses_a_server_older_than_postgresql_18() {
    crate::init_tracing();
    let url = crate::fresh_database("pg_version").await;
    let (_, name) = url.rsplit_once('/').expect("database url has a path");

    let mut setup = PgConnection::connect(&url).await.unwrap();
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        r#"CREATE FUNCTION public.current_setting(setting text) RETURNS text
           LANGUAGE sql AS $$
               SELECT CASE WHEN setting = 'server_version_num' THEN '170004'
                           ELSE pg_catalog.current_setting(setting) END
           $$;
           ALTER DATABASE "{name}" SET search_path = public, pg_catalog;"#
    )))
    .execute(&mut setup)
    .await
    .expect("shadow current_setting");
    setup.close().await.unwrap();

    match Queue::connect(&url).await {
        Err(Error::Config(message)) => {
            assert!(
                message.contains("requires PostgreSQL 18+") && message.contains("server_version_num = 170004"),
                "{message}"
            );
        }
        Err(other) => panic!("an old server must be refused as a config error: {other}"),
        Ok(_) => panic!("a PostgreSQL 17 server must be refused"),
    }

    // The refusal comes before the migrator, so nothing was installed.
    let mut check = PgConnection::connect(&url).await.unwrap();
    let installed: bool =
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'pgqueue')")
            .fetch_one(&mut check)
            .await
            .unwrap();
    assert!(!installed, "a refused server must not be migrated");
    check.close().await.unwrap();
}

// ---------------------------------------------------------------------------
// A NUL a handler produces must fail its attempt, not wedge the processor slot
// ---------------------------------------------------------------------------

/// `pgqueue.workers` is keyed by id alone, so the lease upsert guards its update
/// with `WHERE pgqueue.workers.queue = EXCLUDED.queue`: without it, a heartbeat
/// would move another queue's lease — and every attempt owned by it — under a
/// new queue name. A `Worker` mints `Uuid::now_v7()` for itself, so only the
/// low-level [`pgqueue::Consumer`] API, where the caller supplies the id, can
/// collide.
#[sqlx::test(migrations = "./migrations")]
async fn test_consumer_heartbeat_is_refused_when_the_worker_id_belongs_to_another_queue(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let beta = db.another_queue(|builder| builder.name("beta")).await;
    let worker_id = Uuid::now_v7();
    let ttl = Duration::from_secs(30);

    db.queue
        .consumer(worker_id)
        .heartbeat(json!({ "queue": "alpha" }), None, ttl)
        .await
        .expect("the first queue takes the id");

    let error = beta
        .consumer(worker_id)
        .heartbeat(json!({ "queue": "beta" }), None, ttl)
        .await
        .expect_err("a second queue must not adopt a live worker id");
    match error {
        Error::Config(message) => {
            assert!(
                message.contains("already belongs to a different queue") && message.contains(&worker_id.to_string()),
                "{message}"
            );
        }
        other => panic!("a cross-queue worker id must be a config error: {other}"),
    }

    let rows = sqlx::query("SELECT queue, stats FROM pgqueue.workers WHERE id = $1")
        .bind(worker_id)
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<String, _>("queue"), db.queue.name());
    assert_eq!(
        rows[0].get::<serde_json::Value, _>("stats"),
        json!({ "queue": "alpha" }),
        "the refused heartbeat must not have written anything"
    );
}

// ---------------------------------------------------------------------------
// Dashboard authentication: cookie scanning and the guessing budget
// ---------------------------------------------------------------------------

/// The enqueue path takes an advisory lock, reads the live holder of the dedupe
/// key, and then inserts with `ON CONFLICT ... DO NOTHING RETURNING id`. That lock
/// only binds writers that take it, so anything writing `pgqueue.jobs` directly
/// — application SQL, a backfill, an ops script — can commit the key between the
/// two statements and leave the insert nothing to return.
///
/// The contract is that "a dedupe-key collision returns the existing live job's
/// id", but that case returned `Error::Config` telling the caller to retry the
/// transaction or switch to `READ COMMITTED` — advice that fits neither the
/// session (already `READ COMMITTED`) nor `Queue::enqueue_raw` (which owns the
/// transaction). `schedule_cron` has handled the identical race on the identical
/// statement all along by re-reading the holder; this is the enqueue path doing
/// the same.
///
/// A `BEFORE INSERT` trigger parks the library's insert *after* its guarded read
/// and *before* the conflict check, which is what makes the window a fixed point
/// rather than a race the test has to win.
#[sqlx::test(migrations = "./migrations")]
async fn test_enqueue_deduplicates_against_a_holder_committed_by_an_unlocked_writer(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    install_statement_gate(&pool, "enqueue_conflict_gate", 4343, "INSERT", "NEW.name = 'gated'").await;
    let gate = hold_gate(&pool, 4343, &db.database).await;

    let queue = db.queue.clone();
    let enqueue = tokio::spawn(async move {
        queue.enqueue_raw(new_job("gated", |job| job.dedupe_key = Some("contended-key".into()))).await
    });
    wait_for_lock_waiter(&db, "%INSERT INTO pgqueue.jobs%", "the enqueue never reached its insert").await;

    // A writer that never takes the enqueue lock, committing the key in the
    // window the guarded read has already passed.
    let owner: Uuid = sqlx::query_scalar(
        "INSERT INTO pgqueue.jobs (queue, name, dedupe_key) VALUES ($1, 'outsider', $2)
         RETURNING id",
    )
    .bind(db.queue.name())
    .bind("contended-key")
    .fetch_one(db.queue.pool())
    .await
    .expect("outside writer takes the dedupe key");

    gate.rollback().await.unwrap();
    let result = enqueue.await.unwrap().expect("a collision is not an error");
    assert!(result.is_deduplicated(), "the key was taken, so nothing should have been enqueued");
    assert_eq!(result.job_id(), owner, "the collision must name the job that holds the key");
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM pgqueue.jobs WHERE queue = $1 AND dedupe_key = $2")
            .bind(db.queue.name())
            .bind("contended-key")
            .fetch_one(db.queue.pool())
            .await
            .unwrap(),
        1,
        "the dedupe key still admits exactly one live job"
    );
}

// ---------------------------------------------------------------------------
// A superseded attempt must be cancelled, not left running beside its successor
// ---------------------------------------------------------------------------

/// `Consumer::heartbeat` takes the caller's TTL, and `Duration::ZERO` wrote
/// `expires_at = now()` — expired for every later transaction. `dequeue`
/// requires a live lease, so it then returned an empty `Vec` for a queue full
/// of ready work, with no error anywhere to say why. The upper bound was
/// checked all along; only zero slipped through.
#[sqlx::test(migrations = "./migrations")]
async fn test_consumer_heartbeat_refuses_a_zero_lease_ttl(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    db.queue.enqueue_raw(new_job("zero-ttl", |_| {})).await.unwrap();
    let worker_id = Uuid::now_v7();
    let consumer = db.queue.consumer(worker_id);

    let error = consumer.heartbeat(json!({}), None, Duration::ZERO).await.expect_err("a zero TTL must not be accepted");
    match error {
        Error::Config(message) => {
            assert!(message.contains("must be greater than zero"), "{message}");
        }
        other => panic!("a zero lease TTL must be a config error: {other}"),
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM pgqueue.workers WHERE id = $1")
            .bind(worker_id)
            .fetch_one(&pool)
            .await
            .unwrap(),
        0,
        "the refused heartbeat must not have written a lease"
    );

    // The queue was never the problem: a real TTL claims the waiting job.
    consumer.heartbeat(json!({}), None, Duration::from_secs(30)).await.unwrap();
    assert_eq!(consumer.dequeue(10).await.unwrap().len(), 1);
}

// ---------------------------------------------------------------------------
// An idle cron registry must not cost a transaction per cron per tick
// ---------------------------------------------------------------------------

/// The dedupe path opened its transaction *before* validating, so an enqueue
/// whose input can never be accepted first queued for a connection: on a busy
/// pool the caller got `Error::Db(PoolTimedOut)` — a transient failure worth
/// retrying — for input that was permanently invalid. The keyless path
/// validated first and answered `Error::Config` for the very same job.
#[sqlx::test(migrations = "./migrations")]
async fn test_enqueue_rejects_invalid_input_before_taking_a_connection(pool: PgPool) {
    // Generous, because `connect_with` validates the new pool by acquiring a
    // connection under this same timeout: at 250 ms a loaded machine failed the
    // test at pool *creation*, on TCP connect and startup, before the enqueue
    // this is about ever ran. A regression still fails here — the invalid
    // enqueue waits out the timeout for the held connection and reports
    // `PoolTimedOut` instead of `Config` — it just takes longer to say so.
    let starved = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(5))
        .connect_with(pool.connect_options().as_ref().clone())
        .await
        .unwrap();
    let db = TestDb::new(starved.clone()).await;
    // The pool's only connection, held for the rest of the test.
    let _held = starved.acquire().await.unwrap();

    for dedupe_key in [None, Some("held-key".to_string())] {
        let keyed = dedupe_key.is_some();
        let error = db
            .queue
            .enqueue_raw(new_job("bad\0name", |job| job.dedupe_key = dedupe_key))
            .await
            .expect_err("a NUL in a job name is never valid");
        match error {
            Error::Config(message) => {
                assert!(message.contains("must not contain NUL"), "{message}");
            }
            other => panic!("invalid input with dedupe_key={keyed} reported as {other}"),
        }
    }
}

// ---------------------------------------------------------------------------
// One unreadable `backoff` must not strand the batch it rode in on
// ---------------------------------------------------------------------------

/// Collects the `backoff` field of every event emitted while this is the
/// default subscriber. The decoder's warning is the only place an unreadable
/// value is ever named, so the warning is where the name has to be checked.
#[derive(Clone, Default)]
struct RecordedBackoffs(std::sync::Arc<std::sync::Mutex<Vec<String>>>);

impl RecordedBackoffs {
    fn recorded(&self) -> Vec<String> {
        self.0.lock().expect("recorded backoffs").clone()
    }

    fn record(&self, value: String) {
        self.0.lock().expect("recorded backoffs").push(value);
    }
}

impl tracing::field::Visit for RecordedBackoffs {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "backoff" {
            self.record(value.to_string());
        }
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "backoff" {
            self.record(format!("{value:?}"));
        }
    }
}

impl<S: tracing::Subscriber> tracing_subscriber::layer::Layer<S> for RecordedBackoffs {
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: tracing_subscriber::layer::Context<'_, S>) {
        event.record(&mut self.clone());
    }
}

/// `PgValueRef::as_str` hands back the wire bytes verbatim, and binary-format
/// `jsonb` — what every query in this crate uses — puts a one-byte version
/// header in front of the JSON text. `Json::decode` strips it; the warning did
/// not, so the one string identifying the bad value arrived with a stray U+0001
/// glued to its front.
#[sqlx::test(migrations = "./migrations")]
async fn test_unreadable_backoff_warning_reports_the_stored_json(pool: PgPool) {
    use tracing_subscriber::layer::SubscriberExt;

    let db = TestDb::new(pool.clone()).await;
    let handle = db.queue.enqueue_raw(new_job("unreadable-backoff", |_| {})).await.unwrap();
    // Accepted by the column's CHECK — the tag is one this build knows — and
    // still unreadable, because `max_ms` is not a number.
    sqlx::query("UPDATE pgqueue.jobs SET backoff = $2::jsonb WHERE id = $1")
        .bind(handle.job_id())
        .bind(r#"{"type": "exponential", "max_ms": "nope"}"#)
        .execute(&pool)
        .await
        .unwrap();

    let recorded = RecordedBackoffs::default();
    // Thread-local, so it takes precedence over the suite's global subscriber
    // for the read below and nothing else.
    let guard =
        tracing::subscriber::set_default(tracing_subscriber::registry::Registry::default().with(recorded.clone()));
    let row = db
        .queue
        .fetch_job(handle.job_id())
        .await
        .unwrap()
        .expect("the row must decode despite the unreadable strategy");
    drop(guard);

    assert_eq!(row.backoff, JobRetryBackoff::None);
    let reported = recorded.recorded();
    assert_eq!(reported.len(), 1, "the unreadable strategy must be reported exactly once: {reported:?}");
    let reported = &reported[0];
    assert!(
        reported.starts_with('{') && !reported.contains('\u{1}'),
        "the warning must name the stored JSON, not the jsonb wire bytes: {reported:?}"
    );
    assert!(
        reported.contains(r#""max_ms": "nope""#),
        "the warning must name the value that could not be read: {reported:?}"
    );
}

/// The sibling `result_ttl_ms` column refuses a value it has no encoding for,
/// and `backoff` now does the same: the leniency above is for rows that
/// predate the check, not a licence to write new ones.
#[sqlx::test(migrations = "./migrations")]
async fn test_backoff_column_refuses_an_unknown_strategy(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let handle = db.queue.enqueue_raw(new_job("checked", |_| {})).await.unwrap();

    for rejected in [
        r#"{"type":"linear"}"#,
        r#"{"delay":5}"#,
        r#""none""#,
        "null",
    ] {
        let error = sqlx::query("UPDATE pgqueue.jobs SET backoff = $2::jsonb WHERE id = $1")
            .bind(handle.job_id())
            .bind(rejected)
            .execute(&pool)
            .await
            .unwrap_err();
        assert_eq!(
            error.as_database_error().and_then(|error| error.constraint()),
            Some("jobs_backoff_check"),
            "{rejected} must be refused: {error}"
        );
    }

    // Both strategies the crate does write are still accepted.
    for accepted in [
        r#"{"type":"none"}"#,
        r#"{"type":"exponential"}"#,
        r#"{"type":"exponential","max_ms":60000}"#,
    ] {
        sqlx::query("UPDATE pgqueue.jobs SET backoff = $2::jsonb WHERE id = $1")
            .bind(handle.job_id())
            .bind(accepted)
            .execute(&pool)
            .await
            .unwrap_or_else(|error| panic!("{accepted} must be accepted: {error}"));
    }
}

// ---------------------------------------------------------------------------
// `timeout_ms = 0` means unlimited everywhere else; it must here too
// ---------------------------------------------------------------------------

/// And the column refuses one from now on, for the same reason `result_ttl_ms`
/// refuses a negative retention.
#[sqlx::test(migrations = "./migrations")]
async fn test_timeout_column_refuses_a_non_positive_timeout(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let handle = db.queue.enqueue_raw(new_job("bounded", |_| {})).await.unwrap();

    for rejected in [0i64, -1] {
        let error = sqlx::query("UPDATE pgqueue.jobs SET timeout_ms = $2 WHERE id = $1")
            .bind(handle.job_id())
            .bind(rejected)
            .execute(&pool)
            .await
            .unwrap_err();
        assert_eq!(
            error.as_database_error().and_then(|error| error.constraint()),
            Some("jobs_timeout_ms_check"),
            "timeout_ms = {rejected} must be refused: {error}"
        );
    }

    // NULL is how "no timeout" is written, and a positive bound is a timeout.
    for accepted in [None, Some(1i64)] {
        sqlx::query("UPDATE pgqueue.jobs SET timeout_ms = $2 WHERE id = $1")
            .bind(handle.job_id())
            .bind(accepted)
            .execute(&pool)
            .await
            .unwrap();
    }
}

/// The lower bounds are only half of it. `pgqueue.job_is_stuck` adds
/// `timeout_ms` to the sweep grace in `bigint` and is applied to *every* active
/// row of a queue, and finish and abort both compute
/// `now() + result_ttl_ms * interval '1 millisecond'` — so a single row near
/// the type's ceiling answered `22003`/`22008` and took stuck-job recovery down
/// for that whole queue, permanently. Every Rust writer is already held to
/// `MAX_DURATION_MS` by `validate_duration`; these checks are the defence
/// against the writers that are not it.
#[sqlx::test(migrations = "./migrations")]
async fn test_duration_columns_refuse_a_value_past_the_api_maximum(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let handle = db.queue.enqueue_raw(new_job("bounded-duration", |_| {})).await.unwrap();
    let maximum = i64::try_from(pgqueue::__private::MAX_DURATION_MS).unwrap();

    for (column, constraint, statement) in [
        ("timeout_ms", "jobs_timeout_ms_check", "UPDATE pgqueue.jobs SET timeout_ms = $2 WHERE id = $1"),
        ("result_ttl_ms", "jobs_result_ttl_ms_check", "UPDATE pgqueue.jobs SET result_ttl_ms = $2 WHERE id = $1"),
    ] {
        let error = sqlx::query(statement).bind(handle.job_id()).bind(i64::MAX).execute(&pool).await.unwrap_err();
        assert_eq!(
            error.as_database_error().and_then(|error| error.constraint()),
            Some(constraint),
            "{column} = i64::MAX must be refused: {error}"
        );
        // The API's own maximum still is not: the bound is the API's, not a
        // second, stricter policy nothing can reach.
        sqlx::query(statement)
            .bind(handle.job_id())
            .bind(maximum)
            .execute(&pool)
            .await
            .unwrap_or_else(|error| panic!("{column} = {maximum} must be accepted: {error}"));
    }

    // ...and with both at that maximum the arithmetic the sweeper runs over
    // every active row stays in range, which is the whole point of the bound.
    sqlx::query("UPDATE pgqueue.jobs SET started_at = now(), touched_at = now() WHERE id = $1")
        .bind(handle.job_id())
        .execute(&pool)
        .await
        .unwrap();
    let stuck = sqlx::query_scalar::<_, bool>(
        "SELECT pgqueue.job_is_stuck(j, $2::bigint, now()) FROM pgqueue.jobs j WHERE j.id = $1",
    )
    .bind(handle.job_id())
    .bind(maximum)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(!stuck);
}

// ---------------------------------------------------------------------------
// A NUL in a dashboard name filter is a malformed request, not a 500
// ---------------------------------------------------------------------------

/// The public listing's name filter takes the same boundary every other
/// caller-supplied string gets: PostgreSQL `text` cannot hold a NUL, so left
/// to reach the query it came back as a transient-looking `Error::Db` where
/// `Error::Config` says what is actually wrong.
#[sqlx::test(migrations = "./migrations")]
async fn test_jobs_page_refuses_a_nul_name_filter(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let error =
        db.queue.jobs_page(JobFilter { name: Some("bad\0name".into()), ..JobFilter::default() }).await.unwrap_err();
    assert!(matches!(&error, Error::Config(message) if message == "job name filter must not contain NUL"), "{error:?}");
}

/// A page cursor takes the same floor `JobRequest::validate` and the
/// dashboard's cursors hold: `Timestamp` reaches instants `timestamptz`
/// cannot represent, and a deserialized cursor carrying one came back as a
/// transient-looking database range error instead of naming the bad input.
#[sqlx::test(migrations = "./migrations")]
async fn test_jobs_page_refuses_a_cursor_below_the_timestamp_floor(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let cursor = JobCursor { enqueued_at: "-009990-01-01T00:00:00Z".parse().unwrap(), id: Uuid::now_v7() };
    let error = db.queue.jobs_page(JobFilter { before: Some(cursor), ..JobFilter::default() }).await.unwrap_err();
    assert!(
        matches!(&error, Error::Config(message) if message.contains("below PostgreSQL's supported timestamp range")),
        "{error:?}"
    );
}

/// `Queue::abort_job` on a `running` row makes it `aborting` with the operator's
/// reason and no sweeper marker. Both claim-recovery paths — the
/// unacknowledged-commit resolver the `Worker` arms on every dequeue, and
/// `Attempt`'s dropped-capability recovery — used to accept an `aborting` row
/// only while it carried that marker, so a claim a user abort caught mid-flight
/// matched neither the guarded requeue (correctly: a user abort must never come
/// back as a retry) nor the guarded abort that exists to finish what the requeue
/// refuses. The row was then stranded under a live lease: holding its dedupe key
/// against every re-enqueue, answering `false` to every further `abort_job`, and
/// hanging every waiter on it.
///
/// The sweeper-marked claim in the same call is the control, so the difference
/// under test is the *kind* of abort, not the machine.
#[sqlx::test(migrations = "./migrations")]
async fn test_a_user_aborted_claim_is_settled_by_the_resolver(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let worker_id = Uuid::now_v7();
    db.queue.write_worker_lease(worker_id, serde_json::json!({}), None, Duration::from_secs(300), true).await.unwrap();

    let mut ids = Vec::new();
    for tag in ["user", "swept"] {
        ids.push(
            db.queue
                .enqueue_raw(new_job("stranded", |job| {
                    job.dedupe_key = Some(format!("stranded-{tag}"));
                    job.config.max_attempts = 3;
                    job.config.timeout = None;
                }))
                .await
                .unwrap()
                .unwrap(),
        );
    }
    let claimed = db.queue.dequeue(2, worker_id).await.unwrap();
    assert_eq!(claimed.len(), 2);

    assert!(db.queue.abort_job(ids[0], "operator asked").await.unwrap());
    // The control, marked exactly as the sweeper's phase one marks it.
    sqlx::query(
        r#"UPDATE pgqueue.jobs
           SET status = 'aborting', error = 'swept', result = '"pgqueue:swept"', touched_at = now()
           WHERE id = $1"#,
    )
    .bind(ids[1])
    .execute(&pool)
    .await
    .unwrap();

    let claims: Vec<_> = claimed.iter().map(|job| (job.id, job.attempts, job.started_at)).collect();
    pgqueue::__test_support::requeue_unacknowledged_claims(&db.queue, worker_id, &claims).await.unwrap();

    let user = db.queue.fetch_job(ids[0]).await.unwrap().unwrap();
    assert_eq!(user.status, JobStatus::Aborted, "the user-aborted claim was left stranded");
    assert_eq!(user.error.as_deref(), Some("operator asked"), "the operator's reason must survive");
    let control = db.queue.fetch_job(ids[1]).await.unwrap().unwrap();
    assert_eq!(control.status, JobStatus::Queued, "the sweeper-marked claim must still be given back");

    // The consequence that made it matter: the key is free again.
    assert!(
        db.queue
            .enqueue_raw(new_job("stranded", |job| job.dedupe_key = Some("stranded-user".to_string())))
            .await
            .unwrap()
            .is_some(),
        "the settled row must release its dedupe key"
    );
}

/// The claim's `FOR UPDATE ... SKIP LOCKED` relies on READ COMMITTED's
/// EvalPlanQual re-check: `SKIP LOCKED` skips a row another transaction
/// currently *holds*, and says nothing about one already committed. At
/// `repeatable read` PostgreSQL answers that case with `40001` instead, which
/// reaches the fetch loop as an `Error::Db` indistinguishable from the transient
/// failures it is built to retry. An operator can set that default cluster-wide
/// without touching pgqueue at all, so the requirement is checked where the
/// PostgreSQL version is: once, at connect, with a message naming the fix.
#[sqlx::test(migrations = "./migrations")]
async fn test_connect_refuses_a_non_read_committed_default(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let database = &db.database;
    let admin = crate::admin_url();
    let (prefix, _) = admin.rsplit_once('/').expect("url has a path");

    let mut conn = sqlx::PgConnection::connect(&admin).await.unwrap();
    // `read uncommitted` is read committed under another name — PostgreSQL runs it with the same statement snapshots
    // and re-check — so it connects.
    for (isolation, accepted) in [
        ("repeatable read", false),
        ("serializable", false),
        ("read uncommitted", true),
    ] {
        sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
            r#"ALTER DATABASE "{database}" SET default_transaction_isolation = '{isolation}'"#
        )))
        .execute(&mut conn)
        .await
        .unwrap();

        let result = Queue::connect(&format!("{prefix}/{database}")).await;
        if accepted {
            result.unwrap_or_else(|error| panic!("{isolation}: {error}"));
        } else {
            let error = result.expect_err("a hardened default must be refused");
            assert!(matches!(error, Error::Config(_)), "{isolation}: {error}");
            assert!(error.to_string().contains("read committed"), "{isolation}: {error}");
        }
    }

    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(r#"ALTER DATABASE "{database}" RESET default_transaction_isolation"#)))
        .execute(&mut conn)
        .await
        .unwrap();
    Queue::connect(&format!("{prefix}/{database}")).await.expect("a read-committed default still connects");
}

/// The claim's ordered walk under `SKIP LOCKED` is unbounded, so the property
/// is liveness: however many held rows sort ahead, an unlocked row behind them
/// is still claimable — a claim never mistakes a locked prefix for a drained
/// queue.
#[sqlx::test(migrations = "./migrations")]
async fn test_the_dequeue_claim_reaches_past_any_locked_prefix(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    for n in 0..80 {
        db.queue
            .enqueue_raw(new_job("windowed", |job| job.payload = serde_json::json!({ "n": n })))
            .await
            .unwrap()
            .unwrap();
    }
    let worker_id = Uuid::now_v7();

    async fn claim_behind_locks(db: &TestDb, pool: &PgPool, worker_id: Uuid, ahead: i64) -> usize {
        // Reopen intake first: the requeue below is a *shutdown* requeue, which
        // closes it deliberately, and a claim with no open lease returns nothing
        // for a reason that has nothing to do with the locks under test.
        db.queue.write_worker_info(worker_id, serde_json::json!({}), None, Duration::from_secs(60)).await.unwrap();
        let mut lock = pool.begin().await.unwrap();
        sqlx::query(
            "SELECT id FROM pgqueue.jobs
             WHERE queue = $1 AND status = 'queued'
             ORDER BY priority, scheduled_at, id LIMIT $2 FOR UPDATE",
        )
        .bind(db.queue.name())
        .bind(ahead)
        .fetch_all(&mut *lock)
        .await
        .unwrap();
        let claimed = pgqueue::__test_support::dequeue_worker(&db.queue, 1, worker_id).await.unwrap();
        for job in &claimed {
            db.queue.requeue_shutdown(job, "lock probe").await.unwrap();
        }
        lock.rollback().await.unwrap();
        claimed.len()
    }

    // Inside the window, past it, and far past it: all claimable.
    for ahead in [0, 8, 32, 33, 40, 79] {
        assert_eq!(
            claim_behind_locks(&db, &pool, worker_id, ahead).await,
            1,
            "a claim starved behind {ahead} held rows"
        );
    }

    let queued =
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM pgqueue.jobs WHERE queue = $1 AND status = 'queued'")
            .bind(db.queue.name())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(queued, 80, "every row must be back in the queue");
}

/// Exclusivity under simultaneous claims. Every claim is released at the same
/// instant by a barrier, over a ready set smaller than the claimant count so
/// they must collide, and no row may be returned twice.
#[sqlx::test(migrations = "./migrations")]
async fn test_simultaneous_claims_never_return_a_row_twice(pool: PgPool) {
    let db = TestDb::new(crate::pool_with_max(&pool, 32).await).await;
    let workers = (0..24).map(|_| Uuid::now_v7()).collect::<Vec<_>>();
    for worker_id in &workers {
        db.queue.write_worker_info(*worker_id, serde_json::json!({}), None, Duration::from_secs(60)).await.unwrap();
    }

    let mut enqueued = std::collections::HashSet::new();
    let mut claimed = std::collections::HashSet::new();
    for round in 0..10_i64 {
        // Fewer rows than claimants, spread across names and priorities, so
        // the simultaneous claims genuinely collide over the same rows.
        let ids = sqlx::query_scalar::<_, Uuid>(
            "INSERT INTO pgqueue.jobs (queue, name, priority, scheduled_at)
             SELECT $1, 'simultaneous_' || (g % 7), ((g % 5) - 2)::smallint,
                    now() - interval '1 hour' + (($2 * 10 + g) * interval '1 microsecond')
             FROM generate_series(0, 5) AS g
             RETURNING id",
        )
        .bind(db.queue.name())
        .bind(round)
        .fetch_all(db.queue.pool())
        .await
        .unwrap();
        enqueued.extend(ids);

        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(workers.len() + 1));
        let mut claims = tokio::task::JoinSet::new();
        for worker_id in &workers {
            let queue = db.queue.clone();
            let barrier = barrier.clone();
            let worker_id = *worker_id;
            claims.spawn(async move {
                barrier.wait().await;
                pgqueue::__test_support::dequeue_worker(&queue, 1, worker_id).await
            });
        }
        barrier.wait().await;
        while let Some(claim) = claims.join_next().await {
            for job in claim.unwrap().unwrap() {
                assert_eq!(job.attempts, 1, "a claim spent more than one attempt");
                assert!(claimed.insert(job.id), "job {} was returned by two claims", job.id);
            }
        }
    }

    assert_eq!(claimed, enqueued, "every row must be claimed exactly once");
    let malformed = sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM pgqueue.jobs WHERE queue = $1 AND (status <> 'running' OR attempts <> 1)",
    )
    .bind(db.queue.name())
    .fetch_one(db.queue.pool())
    .await
    .unwrap();
    assert_eq!(malformed, 0, "every claimed row must be running on exactly its first attempt");
}

/// A `LISTEN` on one of this queue's channels, on its own connection.
async fn listen(pool: &PgPool, channel: &str) -> sqlx::postgres::PgListener {
    let mut listener = sqlx::postgres::PgListener::connect_with(pool).await.expect("listener connection");
    listener.listen(channel).await.expect("LISTEN");
    listener
}

/// The next notification, or a panic naming what should have sent it.
async fn expect_notification(listener: &mut sqlx::postgres::PgListener, what: &str) -> sqlx::postgres::PgNotification {
    tokio::time::timeout(Duration::from_secs(5), listener.recv())
        .await
        .unwrap_or_else(|_| panic!("{what} did not notify"))
        .expect("listener recv")
}

/// Asserts that nothing arrives for long enough to have arrived.
async fn expect_silence(listener: &mut sqlx::postgres::PgListener, what: &str) {
    assert!(tokio::time::timeout(Duration::from_millis(300), listener.recv()).await.is_err(), "{what} must not notify");
}

async fn mark_awaited(db: &TestDb, id: Uuid) {
    sqlx::query("UPDATE pgqueue.jobs SET awaited = true WHERE id = $1")
        .bind(id)
        .execute(db.queue.pool())
        .await
        .expect("mark awaited");
}

/// Every `NOTIFY` is a cluster-wide serialization point — PostgreSQL commits
/// notifying transactions one at a time — so an enqueue wakes workers only when
/// the wakeup can do anything: for a row that is due now. Workers poll for
/// scheduled work on their own interval, and waking every worker on the queue
/// for a delayed row would only have them claim nothing.
#[sqlx::test(migrations = "./migrations")]
async fn test_enqueue_wakes_workers_only_for_jobs_that_are_due(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let channel = pgqueue::__test_support::notify_channel(db.queue.name());
    let mut wakeups = listen(&pool, &channel).await;
    let later = Timestamp::now() + SignedDuration::from_secs(3600);

    // Both enqueue paths — keyless autocommit and the keyed transaction — stay
    // silent for a delayed row.
    db.queue.enqueue_raw(new_job("later", |job| job.scheduled_at = Some(later))).await.unwrap();
    db.queue
        .enqueue_raw(new_job("later-keyed", |job| {
            job.scheduled_at = Some(later);
            job.dedupe_key = Some("later".into());
        }))
        .await
        .unwrap();
    expect_silence(&mut wakeups, "a delayed enqueue").await;

    // And both wake for a row that is due.
    db.queue.enqueue_raw(new_job("now", |_| {})).await.unwrap();
    let notification = expect_notification(&mut wakeups, "a due enqueue").await;
    assert_eq!(notification.channel(), channel);
    assert_eq!(notification.payload(), "enqueue");
    db.queue.enqueue_raw(new_job("now-keyed", |job| job.dedupe_key = Some("now".into()))).await.unwrap();
    expect_notification(&mut wakeups, "a due keyed enqueue").await;
}

/// Whether a row is due is the server's decision, made when the statement
/// runs: a row scheduled a moment ahead that comes due while its enqueue waits
/// for a pooled connection still wakes workers, rather than being written off
/// by the client's clock before the statement was even sent.
#[sqlx::test(migrations = "./migrations")]
async fn test_enqueue_wakes_workers_for_a_row_that_comes_due_while_it_waits_for_a_connection(pool: PgPool) {
    let query_pool = crate::pool_with_max(&pool, 1).await;
    let db = TestDb::new(query_pool.clone()).await;
    let mut wakeups = listen(&pool, &pgqueue::__test_support::notify_channel(db.queue.name())).await;

    let held = query_pool.acquire().await.unwrap();
    let enqueue = tokio::spawn({
        let queue = db.queue.clone();
        async move {
            queue
                .enqueue_raw(new_job("soon", |job| {
                    job.scheduled_at = Some(Timestamp::now() + SignedDuration::from_millis(200));
                }))
                .await
        }
    });
    tokio::time::sleep(Duration::from_millis(500)).await;
    drop(held);
    enqueue.await.unwrap().unwrap();
    expect_notification(&mut wakeups, "a row that came due while its enqueue waited for a connection").await;
}

/// The same rule for a retry: one scheduled for later wakes nobody.
#[sqlx::test(migrations = "./migrations")]
async fn test_retry_wakes_workers_only_when_the_retry_is_due(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let mut wakeups = listen(&pool, &pgqueue::__test_support::notify_channel(db.queue.name())).await;
    let delayed = db
        .queue
        .enqueue_raw(with_config("delayed", |config| {
            config.max_attempts = 2;
            config.retry_delay = Duration::from_secs(3600);
        }))
        .await
        .unwrap()
        .unwrap();
    let immediate =
        db.queue.enqueue_raw(with_config("immediate", |config| config.max_attempts = 2)).await.unwrap().unwrap();
    // Drain the two enqueue wakeups so only the retries are observed below.
    expect_notification(&mut wakeups, "the first enqueue").await;
    expect_notification(&mut wakeups, "the second enqueue").await;
    let active = db.queue.dequeue(2, Uuid::now_v7()).await.unwrap();
    let delayed_row = active.iter().find(|job| job.id == delayed).unwrap();
    let immediate_row = active.iter().find(|job| job.id == immediate).unwrap();

    assert!(db.queue.retry(delayed_row, "failed: later").await.unwrap());
    expect_silence(&mut wakeups, "a retry scheduled for later").await;
    assert!(db.queue.retry(immediate_row, "failed: now").await.unwrap());
    expect_notification(&mut wakeups, "a due retry").await;
}

/// With a throttle, a handle asks for one notifying commit per window however
/// many enqueues it makes; the rest are carried by the workers' poll interval.
/// The throttle is per handle, a window that has elapsed opens a new slot, and
/// zero disables it.
#[sqlx::test(migrations = "./migrations")]
async fn test_notify_throttle_emits_one_wakeup_per_window(pool: PgPool) {
    let db = TestDb::with(pool.clone(), |builder| builder.notify_throttle(Duration::from_secs(60))).await;
    let mut wakeups = listen(&pool, &pgqueue::__test_support::notify_channel(db.queue.name())).await;

    for n in 0..3 {
        db.queue.enqueue_raw(new_job(&format!("throttled-{n}"), |_| {})).await.unwrap();
    }
    expect_notification(&mut wakeups, "the first enqueue in a window").await;
    expect_silence(&mut wakeups, "an enqueue inside the window").await;
    assert_eq!(db.queue.counts().await.unwrap().queued, 3, "throttling the wakeup never drops the job");

    let unthrottled = db.another_queue(|builder| builder).await;
    unthrottled.enqueue_raw(new_job("other-handle", |_| {})).await.unwrap();
    expect_notification(&mut wakeups, "an unthrottled handle").await;

    let short = db.another_queue(|builder| builder.notify_throttle(Duration::from_millis(500))).await;
    short.enqueue_raw(new_job("short-1", |_| {})).await.unwrap();
    expect_notification(&mut wakeups, "the first enqueue of a short window").await;
    short.enqueue_raw(new_job("short-2", |_| {})).await.unwrap();
    expect_silence(&mut wakeups, "the second enqueue of a short window").await;
    tokio::time::sleep(Duration::from_millis(600)).await;
    short.enqueue_raw(new_job("short-3", |_| {})).await.unwrap();
    expect_notification(&mut wakeups, "an enqueue after the window elapsed").await;

    let disabled = db.another_queue(|builder| builder.notify_throttle(Duration::ZERO)).await;
    disabled.enqueue_raw(new_job("disabled-1", |_| {})).await.unwrap();
    disabled.enqueue_raw(new_job("disabled-2", |_| {})).await.unwrap();
    expect_notification(&mut wakeups, "the first enqueue with the throttle disabled").await;
    expect_notification(&mut wakeups, "the second enqueue with the throttle disabled").await;
}

/// Only the enqueue channel wakes fetchers. A completion never makes a row claimable, yet every awaited completion woke
/// every idle worker on the queue to run a claim — and an availability probe — that could only find nothing.
#[sqlx::test(migrations = "./migrations")]
async fn test_completion_notifications_do_not_wake_fetchers(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let mut wakeups = pgqueue::__test_support::listener_wakeups(&db.queue);
    crate::wait_for_done_listener(&db).await;

    let done = pgqueue::__test_support::done_channel(db.queue.name());
    for _ in 0..10 {
        sqlx::query("SELECT pg_notify($1, $2)")
            .bind(&done)
            .bind(format!(r#"{{"id":"{}","status":"complete"}}"#, Uuid::now_v7()))
            .execute(&pool)
            .await
            .unwrap();
    }
    sqlx::query("SELECT pg_notify($1, 'enqueue')")
        .bind(pgqueue::__test_support::notify_channel(db.queue.name()))
        .execute(&pool)
        .await
        .unwrap();

    // Notifications arrive in commit order, so a completion that woke anything did so ahead of the enqueue, and its
    // wakeup is followed by more.
    tokio::time::timeout(Duration::from_secs(5), wakeups.recv())
        .await
        .expect("the enqueue did not wake fetchers")
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(500), wakeups.recv()).await.is_err(),
        "a completion woke fetchers"
    );
}

/// A completion notification goes out only for a row somebody is waiting on.
/// Every other finish would pay the cluster-wide notify lock for a message no
/// listener has a use for. The rule holds on every path that finishes a row:
/// the owner's finish, a queued abort, and the background recovery of a
/// dropped attempt, which finishes through the batch macro.
#[sqlx::test(migrations = "./migrations")]
async fn test_finishes_notify_only_awaited_jobs(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let mut completions = listen(&pool, &pgqueue::__test_support::done_channel(db.queue.name())).await;
    let payload_of = |notification: sqlx::postgres::PgNotification| -> serde_json::Value {
        serde_json::from_str(notification.payload()).expect("completion payload is JSON")
    };

    let silent = db.queue.enqueue_raw(new_job("silent", |_| {})).await.unwrap().unwrap();
    let awaited = db.queue.enqueue_raw(new_job("awaited", |_| {})).await.unwrap().unwrap();
    mark_awaited(&db, awaited).await;
    let active = db.queue.dequeue(2, Uuid::now_v7()).await.unwrap();
    let silent_row = active.iter().find(|job| job.id == silent).unwrap();
    let awaited_row = active.iter().find(|job| job.id == awaited).unwrap();
    assert!(db.queue.finish(silent_row, JobStatus::Complete, None, None).await.unwrap());
    expect_silence(&mut completions, "finishing a job nobody waits on").await;
    assert!(db.queue.finish(awaited_row, JobStatus::Failed, None, Some("failed: boom")).await.unwrap());
    let payload = payload_of(expect_notification(&mut completions, "finishing an awaited job").await);
    assert_eq!(payload, json!({"id": awaited, "status": "failed"}));

    let silent = db.queue.enqueue_raw(new_job("silent-queued", |_| {})).await.unwrap().unwrap();
    assert!(db.queue.abort_job(silent, "not needed").await.unwrap());
    expect_silence(&mut completions, "aborting a queued job nobody waits on").await;
    let awaited = db.queue.enqueue_raw(new_job("awaited-queued", |_| {})).await.unwrap().unwrap();
    mark_awaited(&db, awaited).await;
    assert!(db.queue.abort_job(awaited, "not needed").await.unwrap());
    let payload = payload_of(expect_notification(&mut completions, "aborting an awaited queued job").await);
    assert_eq!(payload, json!({"id": awaited, "status": "aborted"}));

    let consumer = leased_consumer(&db.queue, Uuid::now_v7()).await;
    let silent =
        db.queue.enqueue_raw(with_config("silent-dropped", |config| config.max_attempts = 1)).await.unwrap().unwrap();
    drop(consumer.dequeue(1).await.unwrap());
    wait_for_some(Duration::from_secs(10), Duration::from_millis(10), "dropped attempt was not aborted", || async {
        db.queue.fetch_job(silent).await.unwrap().filter(|row| row.status.is_terminal())
    })
    .await;
    expect_silence(&mut completions, "recovering a dropped attempt nobody waits on").await;
    let awaited =
        db.queue.enqueue_raw(with_config("awaited-dropped", |config| config.max_attempts = 1)).await.unwrap().unwrap();
    mark_awaited(&db, awaited).await;
    drop(consumer.dequeue(1).await.unwrap());
    let payload = payload_of(expect_notification(&mut completions, "recovering an awaited dropped attempt").await);
    assert_eq!(payload, json!({"id": awaited, "status": "aborted"}));
}

/// Failed and aborted rows expire on their own clock. A result is read within
/// moments of the finish by whoever waited for it; a failure is read by an
/// operator after the fact, so one shared retention purged failures before
/// anyone looked. Each finish path applies the retention of its outcome.
#[sqlx::test(migrations = "./migrations")]
async fn test_failed_and_aborted_rows_keep_the_failure_retention(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let short_result_long_failure = |config: &mut JobConfig| {
        config.retention = JobRetention::For(Duration::from_millis(1));
        config.failed_retention = JobRetention::For(Duration::from_secs(3600));
    };
    let completes = db.queue.enqueue_raw(with_config("completes", short_result_long_failure)).await.unwrap().unwrap();
    let fails = db.queue.enqueue_raw(with_config("fails", short_result_long_failure)).await.unwrap().unwrap();
    let aborts = db.queue.enqueue_raw(with_config("aborts", short_result_long_failure)).await.unwrap().unwrap();
    let aborts_queued =
        db.queue.enqueue_raw(with_config("aborts-queued", short_result_long_failure)).await.unwrap().unwrap();
    let active = db.queue.dequeue(3, Uuid::now_v7()).await.unwrap();
    let row_of = |id: Uuid| active.iter().find(|job| job.id == id).unwrap();
    let soon = Timestamp::now() + SignedDuration::from_secs(60);
    let much_later = Timestamp::now() + SignedDuration::from_secs(1800);

    assert!(db.queue.finish(row_of(completes), JobStatus::Complete, None, None).await.unwrap());
    assert!(db.queue.fetch_job(completes).await.unwrap().unwrap().expires_at.unwrap() < soon, "result retention");
    assert!(db.queue.finish(row_of(fails), JobStatus::Failed, None, Some("failed: boom")).await.unwrap());
    let failed = db.queue.fetch_job(fails).await.unwrap().unwrap();
    assert!(failed.expires_at.unwrap() > much_later, "failure retention");
    assert_eq!(failed.failed_ttl_ms, Some(3_600_000));
    assert_eq!(failed.failed_retention(), JobRetention::For(Duration::from_secs(3600)));
    assert_eq!(failed.retention(), JobRetention::For(Duration::from_millis(1)));
    assert!(db.queue.finish(row_of(aborts), JobStatus::Aborted, None, Some("stop")).await.unwrap());
    assert!(db.queue.fetch_job(aborts).await.unwrap().unwrap().expires_at.unwrap() > much_later, "abort retention");
    assert!(db.queue.abort_job(aborts_queued, "not needed").await.unwrap());
    assert!(
        db.queue.fetch_job(aborts_queued).await.unwrap().unwrap().expires_at.unwrap() > much_later,
        "queued abort retention"
    );

    // Immediate deletion is per outcome too.
    let forgets_failures = |config: &mut JobConfig| {
        config.retention = JobRetention::Forever;
        config.failed_retention = JobRetention::DeleteImmediately;
    };
    let gone = db.queue.enqueue_raw(with_config("gone", forgets_failures)).await.unwrap().unwrap();
    let kept = db.queue.enqueue_raw(with_config("kept", forgets_failures)).await.unwrap().unwrap();
    let active = db.queue.dequeue(2, Uuid::now_v7()).await.unwrap();
    let row_of = |id: Uuid| active.iter().find(|job| job.id == id).unwrap();
    assert!(db.queue.finish(row_of(gone), JobStatus::Failed, None, Some("failed: boom")).await.unwrap());
    assert!(db.queue.fetch_job(gone).await.unwrap().is_none(), "a failure with immediate retention is deleted");
    assert!(db.queue.finish(row_of(kept), JobStatus::Complete, None, None).await.unwrap());
    assert!(db.queue.fetch_job(kept).await.unwrap().unwrap().expires_at.is_none(), "a result kept forever");

    assert_eq!(JobConfig::default().failed_retention, JobRetention::For(Duration::from_secs(7 * 24 * 3600)));

    // The column carries `result_ttl_ms`'s bounds.
    let error = sqlx::query("UPDATE pgqueue.jobs SET failed_ttl_ms = -1 WHERE id = $1")
        .bind(kept)
        .execute(db.queue.pool())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("failed_ttl_ms"), "{error}");
}

/// Counts cannot tell a busy queue from a stalled one; when the oldest ready
/// job became due can, and it moves as the head of the queue is claimed.
#[sqlx::test(migrations = "./migrations")]
async fn test_counts_report_when_the_oldest_ready_job_became_due(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    assert_eq!(db.queue.counts().await.unwrap().oldest_ready_at, None);

    db.queue
        .enqueue_raw(new_job("later", |job| {
            job.scheduled_at = Some(Timestamp::now() + SignedDuration::from_secs(3600))
        }))
        .await
        .unwrap();
    let counts = db.queue.counts().await.unwrap();
    assert_eq!(counts.scheduled, 1);
    assert_eq!(counts.oldest_ready_at, None, "a scheduled job is not ready");

    let older = db
        .queue
        .enqueue_raw(new_job("older", |job| job.scheduled_at = Some(Timestamp::now() - SignedDuration::from_secs(40))))
        .await
        .unwrap()
        .unwrap();
    let old = db
        .queue
        .enqueue_raw(new_job("old", |job| job.scheduled_at = Some(Timestamp::now() - SignedDuration::from_secs(20))))
        .await
        .unwrap()
        .unwrap();
    let due_at = |id: Uuid| {
        let queue = db.queue.clone();
        async move { queue.fetch_job(id).await.unwrap().unwrap().scheduled_at }
    };
    let counts = db.queue.counts().await.unwrap();
    assert_eq!(counts.queued, 2);
    assert_eq!(counts.oldest_ready_at, Some(due_at(older).await));

    let claimed = db.queue.dequeue(1, Uuid::now_v7()).await.unwrap();
    assert_eq!(claimed[0].id, older);
    assert_eq!(db.queue.counts().await.unwrap().oldest_ready_at, Some(due_at(old).await));
    db.queue.dequeue(1, Uuid::now_v7()).await.unwrap();
    assert_eq!(db.queue.counts().await.unwrap().oldest_ready_at, None);
}

/// An operator's purge deletes queued rows of this queue and nothing else:
/// not running attempts, not another queue, not a row a claim is locking.
#[sqlx::test(migrations = "./migrations")]
async fn test_purge_queued_jobs_deletes_only_queued_rows_of_this_queue(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let other = db.another_queue(|builder| builder.name("other")).await;
    let running = db.queue.enqueue_raw(new_job("a", |_| {})).await.unwrap().unwrap();
    let queued_a = db.queue.enqueue_raw(new_job("a", |_| {})).await.unwrap().unwrap();
    let scheduled_a = db
        .queue
        .enqueue_raw(new_job("a", |job| job.scheduled_at = Some(Timestamp::now() + SignedDuration::from_secs(3600))))
        .await
        .unwrap()
        .unwrap();
    for _ in 0..2 {
        db.queue.enqueue_raw(new_job("b", |_| {})).await.unwrap();
    }
    let elsewhere = other.enqueue_raw(new_job("a", |_| {})).await.unwrap().unwrap();
    assert_eq!(db.queue.dequeue(1, Uuid::now_v7()).await.unwrap()[0].id, running);

    assert!(matches!(db.queue.purge_queued_jobs(None, 0).await.unwrap_err(), Error::Config(_)));
    assert_eq!(db.queue.purge_queued_jobs(Some("a"), 1).await.unwrap(), 1, "bounded by the limit");
    assert!(db.queue.fetch_job(queued_a).await.unwrap().is_none(), "the oldest due row goes first");
    assert_eq!(db.queue.purge_queued_jobs(Some("a"), 10).await.unwrap(), 1, "a scheduled job is a queued job");
    assert!(db.queue.fetch_job(scheduled_a).await.unwrap().is_none());
    assert_eq!(
        db.queue.fetch_job(running).await.unwrap().unwrap().status,
        JobStatus::Running,
        "attempts are untouched"
    );
    assert_eq!(db.queue.purge_queued_jobs(None, 10).await.unwrap(), 2, "no name purges every name");
    assert_eq!(db.queue.purge_queued_jobs(None, 10).await.unwrap(), 0);
    assert_eq!(
        other.fetch_job(elsewhere).await.unwrap().unwrap().status,
        JobStatus::Queued,
        "other queues are untouched"
    );

    // A row a claim is locking at that instant is skipped, not waited for.
    let locked = other.enqueue_raw(new_job("b", |_| {})).await.unwrap().unwrap();
    let mut claim = pool.begin().await.unwrap();
    sqlx::query("SELECT id FROM pgqueue.jobs WHERE id = $1 FOR UPDATE")
        .bind(locked)
        .execute(&mut *claim)
        .await
        .unwrap();
    let purged = tokio::time::timeout(Duration::from_secs(5), other.purge_queued_jobs(Some("b"), 10))
        .await
        .expect("purge must not wait on a locked row")
        .unwrap();
    assert_eq!(purged, 0);
    claim.rollback().await.unwrap();
    assert_eq!(other.purge_queued_jobs(Some("b"), 10).await.unwrap(), 1);
}

/// The built-in monitoring role reads every table of the schema and writes
/// none of it, so a metrics exporter needs no grant per deployment.
#[sqlx::test(migrations = "./migrations")]
async fn test_pg_monitor_can_read_every_table_and_write_none(pool: PgPool) {
    let _db = TestDb::new(pool.clone()).await;
    let usage = sqlx::query_scalar::<_, bool>("SELECT has_schema_privilege('pg_monitor', 'pgqueue', 'USAGE')")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(usage, "pg_monitor must have USAGE on the schema");
    for table in [
        "jobs",
        "workers",
        "cron_schedules",
        "cron_occurrences",
        "migrations",
    ] {
        let (select, insert) = sqlx::query_as::<_, (bool, bool)>(
            "SELECT has_table_privilege('pg_monitor', $1, 'SELECT'), has_table_privilege('pg_monitor', $1, 'INSERT')",
        )
        .bind(format!("pgqueue.{table}"))
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(select, "pg_monitor must be able to read pgqueue.{table}");
        assert!(!insert, "pg_monitor must not be able to write pgqueue.{table}");
    }
}

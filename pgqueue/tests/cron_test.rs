//! Durable cron registry, revision, misfire, and publication integration tests.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use jiff::tz::TimeZone;
use jiff::{SignedDuration, Timestamp};
use jiff_sqlx::ToSqlx;
use pgqueue::{
    CronDefinition, CronMisfirePolicy, CronOptions, Error, JobFilter, JobState, JobStatus, JobType, Queue, Worker,
    WorkerBuilder, WorkerComponent, WorkerHealthStatus, WorkerTimers,
};
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;

use crate::{EnqueueResultTestExt, TestDb, pool_with_max, wait_for_some, wait_until};
use crate::{Stats, test_timers};

#[pgqueue::cron("0 * * * *", result_ttl_ms = 3_600_000, revision = 7)]
async fn tick(counter: JobState<Arc<AtomicU32>>) -> anyhow::Result<u32> {
    Ok(counter.0.fetch_add(1, Ordering::SeqCst) + 1)
}

#[pgqueue::cron("0 0 1 1 *")]
async fn yearly(counter: JobState<Arc<AtomicU32>>) -> anyhow::Result<u32> {
    Ok(counter.0.fetch_add(1, Ordering::SeqCst) + 1)
}

#[pgqueue::job(result_ttl_ms = 3_600_000)]
async fn dynamic_tick(_: (), counter: JobState<Arc<AtomicU32>>) -> anyhow::Result<u32> {
    Ok(counter.0.fetch_add(1, Ordering::SeqCst) + 1)
}

fn timers() -> WorkerTimers {
    WorkerTimers { schedule: Duration::from_millis(40), ..test_timers() }
}

fn dynamic_worker_builder(
    queue: Queue,
    expression: &str,
    dedupe_key: &str,
    options: CronOptions,
    counter: Arc<AtomicU32>,
) -> WorkerBuilder {
    Worker::builder(queue)
        .schedule_cron_with_options(expression, dynamic_tick::job(()).dedupe_key(dedupe_key), options)
        .state(counter)
        .timers(timers())
        .poll_interval(Duration::from_millis(20))
        .concurrency(2)
}

fn dynamic_worker(
    queue: Queue,
    expression: &str,
    dedupe_key: &str,
    options: CronOptions,
    counter: Arc<AtomicU32>,
) -> Worker {
    dynamic_worker_builder(queue, expression, dedupe_key, options, counter).build().unwrap()
}

/// The second cron a [`dynamic_worker_with_heartbeat`] registers: a schedule
/// nobody supersedes, sharing the worker's one scheduling loop.
const HEARTBEAT_CRON: &str = "scheduling-heartbeat";

/// A [`dynamic_worker`] that also schedules [`HEARTBEAT_CRON`], so a test can
/// wait for the worker's scheduling loop to have *run* instead of sleeping and
/// hoping it did.
///
/// The supersession assertions are negative — nothing published, the cursor
/// unmoved — and a fixed sleep asserts that over however many ticks a loaded
/// runner happened to deliver, which on a slow one can be none at all: the test
/// then passes without ever reaching the arm it exists for. (The same objection
/// `test_cron_skips_a_locked_schedule_row` records about its own window.)
/// The due-filter query comes from the loop under suspicion, so counting it
/// records the loop's passes without waiting for a cron occurrence.
fn dynamic_worker_with_heartbeat(
    queue: Queue,
    expression: &str,
    dedupe_key: &str,
    options: CronOptions,
    counter: Arc<AtomicU32>,
) -> Worker {
    dynamic_worker_builder(queue, expression, dedupe_key, options, counter)
        .schedule_cron_with_options(
            "0 0 1 1 *",
            dynamic_tick::job(()).dedupe_key(HEARTBEAT_CRON),
            skip_options(1, Duration::from_secs(1)),
        )
        .build()
        .unwrap()
}

/// Waits until the worker's scheduling loop has completed `passes` more due
/// checks. `pg_stat_statements` gives the negative assertions below a real
/// scheduling window without waiting for a minute boundary.
async fn wait_for_scheduling_passes(database: &str, passes: i64) {
    match Stats::new(database).await {
        Some(mut stats) => {
            let filters = stats.since_now(CRON_DUE_FILTER).await;
            stats.wait_for_calls(&filters, passes, "the scheduling loop did not tick").await;
        }
        None => tokio::time::sleep(Duration::from_millis(200)).await,
    }
}

fn skip_options(revision: u64, grace: Duration) -> CronOptions {
    CronOptions { revision, misfire: CronMisfirePolicy::Skip { grace: Some(grace) } }
}

async fn schedule_cursor(pool: &PgPool, queue: &str, dedupe_key: &str) -> Option<Timestamp> {
    sqlx::query_scalar::<_, jiff_sqlx::Timestamp>(
        "SELECT next_run_at FROM pgqueue.cron_schedules WHERE queue = $1 AND dedupe_key = $2",
    )
    .bind(queue)
    .bind(dedupe_key)
    .fetch_optional(pool)
    .await
    .unwrap()
    .map(jiff_sqlx::Timestamp::to_jiff)
}

/// Drags a schedule's durable cursor into the past so the cron is genuinely
/// due, and answers the cursor it wrote — which is what a worker that declines
/// to schedule the cron leaves untouched.
///
/// A cursor still in the future sits still whatever the worker does, so
/// "the cursor has not moved" only says something about supersession once the
/// cursor is one an entitled worker would move.
async fn backdate_schedule(pool: &PgPool, queue: &str, dedupe_key: &str) -> Timestamp {
    sqlx::query_scalar::<_, jiff_sqlx::Timestamp>(
        r#"UPDATE pgqueue.cron_schedules SET next_run_at = now() - interval '5 seconds'
           WHERE queue = $1 AND dedupe_key = $2
           RETURNING next_run_at"#,
    )
    .bind(queue)
    .bind(dedupe_key)
    .fetch_one(pool)
    .await
    .unwrap()
    .to_jiff()
}

async fn cron_jobs_published(pool: &PgPool, queue: &str, dedupe_key: &str) -> i64 {
    sqlx::query_scalar::<_, i64>(
        r#"SELECT count(*) FROM pgqueue.jobs
           WHERE queue = $1 AND dedupe_key = $2 AND kind = 'cron'"#,
    )
    .bind(queue)
    .bind(dedupe_key)
    .fetch_one(pool)
    .await
    .unwrap()
}

/// Runs a worker holding the authoritative revision until it moves the cursor
/// past `due_at`. The positive half of every supersession assertion: it shows
/// the occurrence the superseded worker left alone was there to be taken.
async fn assert_authority_advances(
    db: &TestDb,
    expression: &str,
    dedupe_key: &str,
    options: CronOptions,
    due_at: Timestamp,
) {
    let worker = dynamic_worker(
        db.another_queue(|builder| builder).await,
        expression,
        dedupe_key,
        options,
        Arc::new(AtomicU32::new(0)),
    );
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));
    wait_until(
        Duration::from_secs(5),
        Duration::from_millis(10),
        "the authoritative worker did not take the due occurrence",
        || async { schedule_cursor(&db.pool, db.queue.name(), dedupe_key).await.is_some_and(|cursor| cursor > due_at) },
    )
    .await;
    stop_worker(shutdown, run).await;
}

async fn wait_for_schedule(pool: &PgPool, queue: &str, dedupe_key: &str) -> Timestamp {
    wait_for_some(Duration::from_secs(5), Duration::from_millis(10), "cron schedule was not reconciled", || {
        schedule_cursor(pool, queue, dedupe_key)
    })
    .await
}

async fn stop_worker(shutdown: CancellationToken, run: tokio::task::JoinHandle<Result<(), Error>>) {
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(5), run).await.expect("worker did not stop").unwrap().unwrap();
}

/// A pre-cancelled token stops the worker at its first startup step, which in these tests is the dashboard bind (the
/// `_test` feature always enables the dashboard), so reconciliation is never reached — the property pinned here is that
/// nothing of the cron's is written and the run still ends cleanly. A stop that lands *during* reconciliation is
/// `test_cron_reconciliation_lock_wait_observes_worker_shutdown`.
#[sqlx::test(migrations = "./migrations")]
async fn test_cron_startup_is_skipped_when_shutdown_is_pre_cancelled(pool: PgPool) {
    let constrained = pool_with_max(&pool, 1).await;
    let db = TestDb::new(constrained.clone()).await;
    let worker = dynamic_worker(
        db.queue.clone(),
        "0 0 1 1 *",
        "pre-cancelled-startup",
        CronOptions::default(),
        Arc::new(AtomicU32::new(0)),
    );
    let health = worker.health();
    let connection = constrained.acquire().await.unwrap();
    let shutdown = CancellationToken::new();
    shutdown.cancel();

    tokio::time::timeout(Duration::from_secs(1), worker.run_until(shutdown))
        .await
        .expect("pre-cancelled cron worker should stop promptly")
        .expect("pre-cancelled cron worker should stop cleanly");

    let snapshot = health.snapshot();
    assert_eq!(snapshot.status, WorkerHealthStatus::Stopped);
    assert!(snapshot.failures.is_empty());
    drop(connection);
    assert!(schedule_cursor(&pool, db.queue.name(), "pre-cancelled-startup").await.is_none());
}

async fn register_dynamic_schedule(
    db: &TestDb,
    expression: &str,
    dedupe_key: &str,
    options: CronOptions,
    counter: Arc<AtomicU32>,
) {
    let worker = dynamic_worker(db.queue.clone(), expression, dedupe_key, options, counter);
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));
    wait_for_schedule(&db.pool, db.queue.name(), dedupe_key).await;
    stop_worker(shutdown, run).await;
}

/// A schedule matching one fixed UTC minute of every day, anchored on the minute
/// the fixture is built in, with that minute returned as the occurrence to assert
/// against.
///
/// Daily rather than minutely, because `publication_deadline` is
/// `successor.min(occurrence + grace)`: with `* * * * *` the successor *is* the
/// next minute boundary, so it — not the configured grace — is what bounds
/// publication, and the answer to "which occurrence did the scheduler choose"
/// changes the instant the wall clock crosses it. `skip_catch_up` then prefers the
/// newer occurrence and `FireOnce` recomputes `previous_occurrence` outright, so a
/// run that started late in a minute asserted against an occurrence the scheduler
/// had already, correctly, moved past: reproduced as
/// `left: ...T09:49:00Z, right: ...T09:48:00Z`, and as a `counter == 1` where the
/// test requires 0 on a run that crossed an hour boundary. A successor a day away
/// leaves the grace as the only bound, which is what these tests are actually
/// about, and makes every one of them boundary-immune for a full day rather than
/// for whatever was left of a minute.
async fn daily_schedule(pool: &PgPool) -> (String, Timestamp) {
    let (expression, occurrence) = sqlx::query_as::<_, (String, jiff_sqlx::Timestamp)>(
        "SELECT to_char(anchor AT TIME ZONE 'UTC', 'FMMI FMHH24') || ' * * *', anchor
         FROM (SELECT date_trunc('minute', clock_timestamp()) AS anchor) AS fixture",
    )
    .fetch_one(pool)
    .await
    .unwrap();
    (expression, occurrence.to_jiff())
}

async fn hourly_schedule_half_an_hour_ago(pool: &PgPool) -> (String, Timestamp) {
    let (minute, occurrence) = sqlx::query_as::<_, (i32, jiff_sqlx::Timestamp)>(
        "SELECT extract(minute FROM occurrence)::integer, occurrence
         FROM (
             SELECT date_trunc('minute', clock_timestamp()) - interval '30 minutes' AS occurrence
         ) AS schedule",
    )
    .fetch_one(pool)
    .await
    .unwrap();
    (format!("{minute} * * * *"), occurrence.to_jiff())
}

#[test]
fn test_cron_attribute_exposes_schedule_and_revision() {
    assert_eq!(tick::SCHEDULE, "0 * * * *");
    assert_eq!(tick::CRON_REVISION, 7);
    assert_eq!(tick::NAME, "tick");
}

#[sqlx::test(migrations = "./migrations")]
async fn test_remove_cron_schedule_removes_only_the_named_queue_schedule(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let other = db.another_queue(|builder| builder.name("other")).await;
    let counter = Arc::new(AtomicU32::new(0));
    let worker = Worker::builder(db.queue.clone()).register_cron(tick).state(counter).timers(timers()).build().unwrap();
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));
    wait_for_schedule(&pool, db.queue.name(), "cron:tick").await;
    stop_worker(shutdown, run).await;

    assert!(!other.remove_cron_schedule("cron:tick").await.unwrap());
    assert!(db.queue.remove_cron_schedule("cron:tick").await.unwrap());
    assert_eq!(schedule_cursor(&pool, db.queue.name(), "cron:tick").await, None);
    assert!(!db.queue.remove_cron_schedule("cron:tick").await.unwrap());

    for invalid in ["bad\0key".to_string(), "x".repeat(256)] {
        assert!(matches!(db.queue.remove_cron_schedule(&invalid).await, Err(Error::Config(_))));
    }
}

#[sqlx::test(migrations = "./migrations")]
async fn test_cron_publishes_each_occurrence_once_across_workers(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let counter = Arc::new(AtomicU32::new(0));
    let worker_a = Worker::builder(db.queue.clone())
        .register_cron(tick)
        .state(counter.clone())
        .timers(timers())
        .poll_interval(Duration::from_millis(20))
        .build()
        .unwrap();
    let worker_b = Worker::builder(db.another_queue(|builder| builder).await)
        .register_cron(tick)
        .state(counter.clone())
        .timers(timers())
        .poll_interval(Duration::from_millis(20))
        .build()
        .unwrap();
    let shutdown = CancellationToken::new();
    let run_a = tokio::spawn(worker_a.run_until(shutdown.clone()));
    let run_b = tokio::spawn(worker_b.run_until(shutdown.clone()));

    wait_for_schedule(&pool, db.queue.name(), "cron:tick").await;
    sqlx::query(
        "UPDATE pgqueue.cron_schedules
         SET next_run_at = clock_timestamp()
         WHERE queue = $1 AND dedupe_key = $2",
    )
    .bind(db.queue.name())
    .bind("cron:tick")
    .execute(&pool)
    .await
    .unwrap();

    wait_until(Duration::from_secs(5), Duration::from_millis(10), "cron occurrences did not run", || async {
        counter.load(Ordering::SeqCst) >= 1
    })
    .await;
    shutdown.cancel();
    run_a.await.unwrap().unwrap();
    run_b.await.unwrap().unwrap();

    let fired = counter.load(Ordering::SeqCst);
    // A second publication of one occurrence would run the handler twice, so it
    // moves `published` and `fired` together: only counting the distinct
    // `scheduled_at` values can tell the duplicate apart from the next tick.
    let (published, occurrences, completed) = sqlx::query_as::<_, (i64, i64, i64)>(
        r#"SELECT count(*), count(DISTINCT scheduled_at),
                  count(*) FILTER (WHERE status = 'complete')
           FROM pgqueue.jobs
           WHERE queue = $1 AND name = 'tick' AND kind = 'cron'"#,
    )
    .bind(db.queue.name())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(completed, i64::from(fired));
    assert_eq!(published, occurrences, "two workers published the same occurrence more than once");
}

#[sqlx::test(migrations = "./migrations")]
async fn test_cron_job_can_run_as_a_keyless_one_off(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let counter = Arc::new(AtomicU32::new(0));
    let handle = db.queue.enqueue(yearly::job()).await.unwrap().unwrap();
    let worker = Worker::builder(db.queue.clone())
        .register_cron(yearly)
        .state(counter.clone())
        .timers(timers())
        .poll_interval(Duration::from_millis(20))
        .burst(true)
        .dequeue_timeout(Duration::from_millis(200))
        .build()
        .unwrap();
    worker.run_until(CancellationToken::new()).await.unwrap();

    assert_eq!(handle.wait(Some(Duration::from_secs(2))).await.unwrap(), 1);
    assert!(handle.fetch_job().await.unwrap().dedupe_key.is_none());
}

#[sqlx::test(migrations = "./migrations")]
async fn test_burst_worker_runs_due_cron_before_declaring_queue_drained(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let counter = Arc::new(AtomicU32::new(0));
    let dedupe_key = "burst-due-cron";
    let options = CronOptions { revision: 1, misfire: CronMisfirePolicy::FireOnce };
    // Hourly, not minutely: under `FireOnce` the whole period is the publication
    // grace, so a minutely occurrence stops being publishable at the next minute
    // boundary — and this test spawns a worker, waits on a lock and sleeps, so a
    // slow or instrumented run that started late in a minute crossed it and saw
    // the occurrence correctly discarded as stale. Half an hour of slack removes
    // a wall-clock dependency the behaviour under test does not have.
    let (expression, missed) = hourly_schedule_half_an_hour_ago(&pool).await;
    register_dynamic_schedule(&db, &expression, dedupe_key, options, counter.clone()).await;
    sqlx::query("UPDATE pgqueue.cron_schedules SET next_run_at = $3 WHERE queue = $1 AND dedupe_key = $2")
        .bind(db.queue.name())
        .bind(dedupe_key)
        .bind(missed.to_sqlx())
        .execute(&pool)
        .await
        .unwrap();

    let worker = Worker::builder(db.queue.clone())
        .schedule_cron_with_options(&expression, dynamic_tick::job(()).dedupe_key(dedupe_key), options)
        .state(counter.clone())
        .timers(timers())
        .poll_interval(Duration::from_millis(20))
        .concurrency(1)
        .burst(true)
        .dequeue_timeout(Duration::from_nanos(1))
        .build()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), worker.run_until(CancellationToken::new()))
        .await
        .expect("burst worker did not stop")
        .unwrap();

    assert_eq!(counter.load(Ordering::SeqCst), 1);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            r#"SELECT count(*) FROM pgqueue.jobs
               WHERE queue = $1 AND dedupe_key = $2 AND status = 'complete'"#,
        )
        .bind(db.queue.name())
        .bind(dedupe_key)
        .fetch_one(&pool)
        .await
        .unwrap(),
        1
    );
}

/// A burst's `dequeue_timeout` also bounds its startup reconciliation, and a cron that bound cut off was then scheduled
/// as though it had been reconciled, against a stored row it had never upgraded. A pending revision bump read as a
/// definition conflict: the cron was disabled for the run, its due occurrence went unpublished, and `run_until` still
/// returned `Ok` — run after run, since the same tail of the registry was cut off each time.
#[sqlx::test(migrations = "./migrations")]
async fn test_burst_worker_upgrades_a_cron_its_startup_budget_left_unreconciled(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let counter = Arc::new(AtomicU32::new(0));
    let dedupe_key = "burst-unreconciled-upgrade";
    // Hourly, for the reason `test_burst_worker_runs_due_cron_before_declaring_queue_drained` gives.
    let (expression, missed) = hourly_schedule_half_an_hour_ago(&pool).await;
    let stored = CronOptions { revision: 1, misfire: CronMisfirePolicy::FireOnce };
    register_dynamic_schedule(&db, &expression, dedupe_key, stored, counter.clone()).await;
    sqlx::query("UPDATE pgqueue.cron_schedules SET next_run_at = $3 WHERE queue = $1 AND dedupe_key = $2")
        .bind(db.queue.name())
        .bind(dedupe_key)
        .bind(missed.to_sqlx())
        .execute(&pool)
        .await
        .unwrap();

    // A budget too short for startup reconciliation to settle anything.
    let upgraded = CronOptions { revision: 2, misfire: CronMisfirePolicy::FireOnce };
    let worker = Worker::builder(db.queue.clone())
        .schedule_cron_with_options(&expression, dynamic_tick::job(()).dedupe_key(dedupe_key), upgraded)
        .state(counter.clone())
        .timers(timers())
        .poll_interval(Duration::from_millis(20))
        .concurrency(1)
        .burst(true)
        .dequeue_timeout(Duration::from_nanos(1))
        .build()
        .unwrap();
    let health = worker.health();
    tokio::time::timeout(Duration::from_secs(5), worker.run_until(CancellationToken::new()))
        .await
        .expect("burst worker did not stop")
        .unwrap();

    assert_eq!(counter.load(Ordering::SeqCst), 1, "the due occurrence was not published");
    let revision = sqlx::query_scalar::<_, i64>(
        "SELECT revision FROM pgqueue.cron_schedules WHERE queue = $1 AND dedupe_key = $2",
    )
    .bind(db.queue.name())
    .bind(dedupe_key)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(revision, 2, "the pending revision was never reconciled");
    let failures = health.snapshot().failures;
    assert!(failures.is_empty(), "{failures:?}");
}

#[sqlx::test(migrations = "./migrations")]
async fn test_burst_worker_leaves_cron_occurrences_after_its_start_boundary(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let counter = Arc::new(AtomicU32::new(0));
    let dedupe_key = "burst-cron-boundary";
    let expression = "* * * * *";
    let options = CronOptions { revision: 1, misfire: CronMisfirePolicy::FireOnce };
    register_dynamic_schedule(&db, expression, dedupe_key, options, counter.clone()).await;
    // Registration can straddle a minute boundary. Start the assertion
    // from a clean occurrence ledger and force exactly one occurrence due.
    sqlx::query("DELETE FROM pgqueue.jobs WHERE queue = $1 AND dedupe_key = $2")
        .bind(db.queue.name())
        .bind(dedupe_key)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM pgqueue.cron_occurrences WHERE queue = $1 AND dedupe_key = $2")
        .bind(db.queue.name())
        .bind(dedupe_key)
        .execute(&pool)
        .await
        .unwrap();
    let start_occurrence = sqlx::query_scalar::<_, jiff_sqlx::Timestamp>(
        "UPDATE pgqueue.cron_schedules
         SET next_run_at = date_trunc('minute', clock_timestamp())
         WHERE queue = $1 AND dedupe_key = $2
         RETURNING next_run_at",
    )
    .bind(db.queue.name())
    .bind(dedupe_key)
    .fetch_one(&pool)
    .await
    .unwrap()
    .to_jiff();
    counter.store(0, Ordering::SeqCst);

    let worker = Worker::builder(db.queue.clone())
        .schedule_cron_with_options(expression, dynamic_tick::job(()).dedupe_key(dedupe_key), options)
        .state(counter.clone())
        .timers(WorkerTimers { schedule: Duration::from_millis(20), ..test_timers() })
        .poll_interval(Duration::from_millis(20))
        .concurrency(1)
        .burst(true)
        .dequeue_timeout(Duration::from_millis(1_500))
        .build()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(4), worker.run_until(CancellationToken::new()))
        .await
        .expect("burst worker kept scheduling future cron occurrences")
        .unwrap();

    assert_eq!(counter.load(Ordering::SeqCst), 1);
    // Anchored on the occurrence the burst actually published, not on the minute the fixture set: a boundary crossed
    // between that `UPDATE` and the burst reading its own start moves `FireOnce` to the newer occurrence, as it should,
    // and an assertion pinned to the fixture's minute then failed a burst that had behaved perfectly.
    let published = sqlx::query_scalar::<_, jiff_sqlx::Timestamp>(
        "SELECT scheduled_at FROM pgqueue.jobs WHERE queue = $1 AND dedupe_key = $2",
    )
    .bind(db.queue.name())
    .bind(dedupe_key)
    .fetch_one(&pool)
    .await
    .unwrap()
    .to_jiff();
    assert!(published >= start_occurrence, "the burst published an occurrence before its cursor: {published}");
    // The claim in the test's name: one occurrence published, and the cursor left at its successor rather than run
    // forward through every occurrence that came due while the queue drained — and not left behind it either, which a
    // burst that ignored `through` and never advanced would do.
    let cursor = schedule_cursor(&pool, db.queue.name(), dedupe_key).await;
    assert_eq!(
        cursor,
        Some(published + SignedDuration::from_mins(1)),
        "the burst worker did not leave its cursor at the published occurrence's successor"
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn test_burst_worker_waits_for_a_locked_due_cron_schedule(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let counter = Arc::new(AtomicU32::new(0));
    let blocker_key = "burst-lock-blocker";
    let dedupe_key = "burst-locked-cron";
    let options = CronOptions { revision: 1, misfire: CronMisfirePolicy::FireOnce };
    // Hourly, not minutely: under `FireOnce` the whole period is the publication
    // grace, so a minutely occurrence stops being publishable at the next minute
    // boundary — and this test spawns a worker, waits on a lock and sleeps, so a
    // slow or instrumented run that started late in a minute crossed it and saw
    // the occurrence correctly discarded as stale. Half an hour of slack removes
    // a wall-clock dependency the behaviour under test does not have.
    let (expression, missed) = hourly_schedule_half_an_hour_ago(&pool).await;
    register_dynamic_schedule(&db, &expression, blocker_key, options, counter.clone()).await;
    register_dynamic_schedule(&db, &expression, dedupe_key, options, counter.clone()).await;
    sqlx::query(
        "UPDATE pgqueue.cron_schedules SET next_run_at = $4
         WHERE queue = $1 AND dedupe_key IN ($2, $3)",
    )
    .bind(db.queue.name())
    .bind(blocker_key)
    .bind(dedupe_key)
    .bind(missed.to_sqlx())
    .execute(&pool)
    .await
    .unwrap();

    // Park scheduling of the first cron on its dedupe advisory lock. Once it
    // reaches that lock, reconciliation of the whole registry is complete and
    // the second cron has not yet been visited, giving the test a deterministic
    // window to lock only the schedule path under test.
    let mut blocker = db.queue.pool().begin().await.unwrap();
    db.queue
        .enqueue_in(&mut blocker, dynamic_tick::job(()).dedupe_key(blocker_key).delay(Duration::from_secs(60)))
        .await
        .unwrap();

    let worker = Worker::builder(db.queue.clone())
        .schedule_cron_with_options(&expression, dynamic_tick::job(()).dedupe_key(blocker_key), options)
        .schedule_cron_with_options(&expression, dynamic_tick::job(()).dedupe_key(dedupe_key), options)
        .state(counter.clone())
        .timers(timers())
        .poll_interval(Duration::from_millis(20))
        .concurrency(1)
        .burst(true)
        .dequeue_timeout(Duration::from_millis(100))
        .build()
        .unwrap();
    let run = tokio::spawn(worker.run_until(CancellationToken::new()));
    crate::wait_for_dequeue_lock_waiter(&db.queue, true).await;

    let mut lock = pool.begin().await.unwrap();
    sqlx::query(
        "SELECT next_run_at FROM pgqueue.cron_schedules
         WHERE queue = $1 AND dedupe_key = $2 FOR UPDATE",
    )
    .bind(db.queue.name())
    .bind(dedupe_key)
    .fetch_one(&mut *lock)
    .await
    .unwrap();
    blocker.rollback().await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!run.is_finished(), "a dequeue timeout must not hide a due cron behind row-lock contention");
    lock.rollback().await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .expect("burst worker did not finish after the schedule lock was released")
        .unwrap()
        .unwrap();

    assert_eq!(counter.load(Ordering::SeqCst), 2);
    assert_eq!(cron_jobs_published(&pool, db.queue.name(), dedupe_key).await, 1);
    assert!(schedule_cursor(&pool, db.queue.name(), dedupe_key).await.is_some_and(|cursor| cursor > missed));
}

#[sqlx::test(migrations = "./migrations")]
async fn test_cron_registry_does_not_speculatively_enqueue_future_jobs(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let worker = Worker::builder(db.queue.clone())
        .register_cron(yearly)
        .state(Arc::new(AtomicU32::new(0)))
        .timers(timers())
        .build()
        .unwrap();
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));
    let next = wait_for_schedule(&pool, db.queue.name(), "cron:yearly").await;
    let now =
        sqlx::query_scalar::<_, jiff_sqlx::Timestamp>(r#"SELECT now()"#).fetch_one(&pool).await.unwrap().to_jiff();

    assert!(next > now);
    assert_eq!(db.queue.counts().await.unwrap().scheduled, 0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(r#"SELECT count(*) FROM pgqueue.jobs WHERE queue = $1 AND kind = 'cron'"#)
            .bind(db.queue.name())
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    stop_worker(shutdown, run).await;
}

#[sqlx::test(migrations = "./migrations")]
async fn test_worker_builder_cron_runs_a_dynamic_schedule(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let counter = Arc::new(AtomicU32::new(0));
    let worker = dynamic_worker(
        db.queue.clone(),
        "0 * * * *",
        "dynamic",
        skip_options(0, Duration::from_secs(60)),
        counter.clone(),
    );
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));
    wait_for_schedule(&pool, db.queue.name(), "dynamic").await;
    sqlx::query(
        "UPDATE pgqueue.cron_schedules
         SET next_run_at = clock_timestamp()
         WHERE queue = $1 AND dedupe_key = $2",
    )
    .bind(db.queue.name())
    .bind("dynamic")
    .execute(&pool)
    .await
    .unwrap();
    wait_until(Duration::from_secs(3), Duration::from_millis(10), "dynamic cron did not run", || async {
        counter.load(Ordering::SeqCst) >= 1
    })
    .await;
    stop_worker(shutdown, run).await;
}

#[sqlx::test(migrations = "./migrations")]
async fn test_cron_skip_publishes_a_durable_cursor_within_grace(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let counter = Arc::new(AtomicU32::new(0));
    let (expression, missed) = daily_schedule(&pool).await;
    // Ten minutes of grace on a daily schedule, so publication is bounded by the
    // grace alone and nothing this test does can outrun it. See `daily_schedule`.
    let options = skip_options(1, Duration::from_secs(600));
    register_dynamic_schedule(&db, &expression, "within-grace", options, counter.clone()).await;
    sqlx::query("UPDATE pgqueue.cron_schedules SET next_run_at = $3 WHERE queue = $1 AND dedupe_key = $2")
        .bind(db.queue.name())
        .bind("within-grace")
        .bind(missed.to_sqlx())
        .execute(&pool)
        .await
        .unwrap();

    let worker = dynamic_worker(db.queue.clone(), &expression, "within-grace", options, counter.clone());
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));
    wait_until(Duration::from_secs(3), Duration::from_millis(10), "durable occurrence was not published", || async {
        counter.load(Ordering::SeqCst) == 1
    })
    .await;
    stop_worker(shutdown, run).await;

    let scheduled_at = sqlx::query_scalar::<_, jiff_sqlx::Timestamp>(
        "SELECT scheduled_at FROM pgqueue.jobs
         WHERE queue = $1 AND dedupe_key = $2 AND kind = 'cron'",
    )
    .bind(db.queue.name())
    .bind("within-grace")
    .fetch_one(&pool)
    .await
    .unwrap()
    .to_jiff();
    assert_eq!(scheduled_at, missed);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_cron_skip_advances_a_stale_cursor_without_publishing(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let counter = Arc::new(AtomicU32::new(0));
    let (expression, missed) = hourly_schedule_half_an_hour_ago(&pool).await;
    let options = skip_options(1, Duration::from_secs(1));
    register_dynamic_schedule(&db, &expression, "stale-skip", options, counter.clone()).await;
    sqlx::query("UPDATE pgqueue.cron_schedules SET next_run_at = $3 WHERE queue = $1 AND dedupe_key = $2")
        .bind(db.queue.name())
        .bind("stale-skip")
        .bind(missed.to_sqlx())
        .execute(&pool)
        .await
        .unwrap();

    let worker = dynamic_worker(db.queue.clone(), &expression, "stale-skip", options, counter.clone());
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));
    wait_until(Duration::from_secs(3), Duration::from_millis(10), "stale cursor was not advanced", || async {
        schedule_cursor(&pool, db.queue.name(), "stale-skip").await.is_some_and(|cursor| cursor > missed)
    })
    .await;
    stop_worker(shutdown, run).await;

    assert_eq!(counter.load(Ordering::SeqCst), 0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            r#"SELECT count(*) FROM pgqueue.jobs
               WHERE queue = $1 AND dedupe_key = $2 AND kind = 'cron'"#,
        )
        .bind(db.queue.name())
        .bind("stale-skip")
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            r#"SELECT count(*) FROM pgqueue.cron_occurrences
               WHERE queue = $1 AND dedupe_key = $2 AND scheduled_at = $3"#,
        )
        .bind(db.queue.name())
        .bind("stale-skip")
        .bind(missed.to_sqlx())
        .fetch_one(&pool)
        .await
        .unwrap(),
        1
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn test_cron_fire_once_publishes_only_the_latest_missed_occurrence(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let counter = Arc::new(AtomicU32::new(0));
    let (expression, latest) = daily_schedule(&pool).await;
    let options = CronOptions { revision: 1, misfire: CronMisfirePolicy::FireOnce };
    register_dynamic_schedule(&db, &expression, "fire-once", options, counter.clone()).await;
    // Two whole days back, so `(old_cursor, now]` holds *two* occurrences of this
    // daily schedule rather than one. With a single candidate, `occurrences.len()
    // == 1` is satisfied by any policy that publishes everything since the
    // cursor, and the "only" in this test's name went untested — the minutely
    // fixture this replaced put around 120 in that window.
    let old_cursor = latest - SignedDuration::from_hours(48);
    sqlx::query("UPDATE pgqueue.cron_schedules SET next_run_at = $3 WHERE queue = $1 AND dedupe_key = $2")
        .bind(db.queue.name())
        .bind("fire-once")
        .bind(old_cursor.to_sqlx())
        .execute(&pool)
        .await
        .unwrap();

    let worker = dynamic_worker(db.queue.clone(), &expression, "fire-once", options, counter.clone());
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));
    wait_until(Duration::from_secs(3), Duration::from_millis(10), "fire-once occurrence was not published", || async {
        counter.load(Ordering::SeqCst) == 1
    })
    .await;
    stop_worker(shutdown, run).await;

    let rows =
        db.queue.jobs_page(JobFilter { name: Some("dynamic_tick".into()), ..JobFilter::default() }).await.unwrap();
    let occurrences = rows.iter().filter(|row| row.dedupe_key.as_deref() == Some("fire-once")).collect::<Vec<_>>();
    assert_eq!(occurrences.len(), 1);
    assert_eq!(occurrences[0].scheduled_at, latest);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_cron_template_revision_preserves_a_due_fire_once_cursor(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let counter = Arc::new(AtomicU32::new(0));
    // Hourly, for the reason the burst tests above are: under `FireOnce` the
    // whole period is the grace, so a minutely occurrence stops being
    // publishable at the next minute boundary — and this test runs two workers
    // in sequence, which a slow or instrumented run cannot fit inside the
    // remainder of a minute it started late in.
    let (expression, latest) = hourly_schedule_half_an_hour_ago(&pool).await;
    let dedupe_key = "template-revision-fire-once";
    let initial_options = CronOptions { revision: 1, misfire: CronMisfirePolicy::FireOnce };
    let initial = Worker::builder(db.queue.clone())
        .schedule_cron_with_options(
            &expression,
            dynamic_tick::job(()).dedupe_key(dedupe_key).meta(serde_json::json!({ "template": 1 })),
            initial_options,
        )
        .state(counter.clone())
        .timers(timers())
        .poll_interval(Duration::from_millis(20))
        .build()
        .unwrap();
    let initial_shutdown = CancellationToken::new();
    let initial_run = tokio::spawn(initial.run_until(initial_shutdown.clone()));
    wait_for_schedule(&pool, db.queue.name(), dedupe_key).await;
    stop_worker(initial_shutdown, initial_run).await;

    sqlx::query("UPDATE pgqueue.cron_schedules SET next_run_at = $3 WHERE queue = $1 AND dedupe_key = $2")
        .bind(db.queue.name())
        .bind(dedupe_key)
        .bind(latest.to_sqlx())
        .execute(&pool)
        .await
        .unwrap();

    let revised = Worker::builder(db.queue.clone())
        .schedule_cron_with_options(
            &expression,
            dynamic_tick::job(()).dedupe_key(dedupe_key).meta(serde_json::json!({ "template": 2 })),
            CronOptions { revision: 2, misfire: CronMisfirePolicy::FireOnce },
        )
        .state(counter.clone())
        .timers(timers())
        .poll_interval(Duration::from_millis(20))
        .build()
        .unwrap();
    let revised_shutdown = CancellationToken::new();
    let revised_run = tokio::spawn(revised.run_until(revised_shutdown.clone()));
    wait_until(
        Duration::from_secs(3),
        Duration::from_millis(10),
        "due occurrence was lost during a template-only revision",
        || async { counter.load(Ordering::SeqCst) == 1 },
    )
    .await;
    stop_worker(revised_shutdown, revised_run).await;

    let (scheduled_at, meta) = sqlx::query_as::<_, (jiff_sqlx::Timestamp, serde_json::Value)>(
        "SELECT scheduled_at, meta FROM pgqueue.jobs
             WHERE queue = $1 AND dedupe_key = $2 AND kind = 'cron'",
    )
    .bind(db.queue.name())
    .bind(dedupe_key)
    .fetch_one(&pool)
    .await
    .unwrap();
    let scheduled_at = scheduled_at.to_jiff();
    assert_eq!(scheduled_at, latest);
    assert_eq!(meta, serde_json::json!({ "template": 2 }));
}

/// `reconcile_cron` writes the definition, immediately reads it back, and
/// compared the two with a Rust `!=` — but `jsonb` stores numbers as `numeric`,
/// so `serde_json`'s exponent form is expanded on the way out and re-parses as
/// `Number::PosInt` where it went in as `Number::Float`, and `serde_json`'s
/// `Number: PartialEq` calls those unequal. Any cron whose payload or meta
/// carried a float of 1e16 or larger therefore found the definition it had just
/// written itself to be in conflict, with no competing deploy anywhere:
/// `permanent=true`, so the cron never ran again, and the diagnostic told the
/// operator to bump a revision, which can never help. `jsonb` equality is the
/// only equality this value has, so the comparison belongs in PostgreSQL.
#[sqlx::test(migrations = "./migrations")]
async fn test_cron_reconciles_a_definition_carrying_a_float_jsonb_stores_as_an_integer(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let counter = Arc::new(AtomicU32::new(0));
    let dedupe_key = "float-definition";
    let worker = Worker::builder(db.queue.clone())
        .schedule_cron_with_options(
            "* * * * *",
            dynamic_tick::job(())
                .dedupe_key(dedupe_key)
                // `1e16` survives the round trip as `10000000000000000`, which
                // `jsonb` calls equal and `serde_json` does not. `1e15` renders
                // as `1000000000000000.0` and always compared equal, which is
                // why the control below is the same shape one exponent smaller.
                .meta(serde_json::json!({ "big": 1e16, "small": 1e15 })),
            skip_options(0, Duration::from_secs(60)),
        )
        .state(counter.clone())
        .timers(timers())
        .poll_interval(Duration::from_millis(20))
        .concurrency(2)
        .build()
        .unwrap();
    let health = worker.health();
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));

    wait_for_schedule(&pool, db.queue.name(), dedupe_key).await;
    sqlx::query(
        "UPDATE pgqueue.cron_schedules
         SET next_run_at = date_trunc('minute', clock_timestamp())
         WHERE queue = $1 AND dedupe_key = $2",
    )
    .bind(db.queue.name())
    .bind(dedupe_key)
    .execute(&pool)
    .await
    .unwrap();

    wait_until(Duration::from_secs(5), Duration::from_millis(10), "the cron never published an occurrence", || async {
        cron_jobs_published(&pool, db.queue.name(), dedupe_key).await >= 1
    })
    .await;
    wait_until(Duration::from_secs(5), Duration::from_millis(10), "the cron occurrence never ran", || async {
        counter.load(Ordering::SeqCst) >= 1
    })
    .await;

    // A conflict is reported as a permanent scheduler failure, so a healthy
    // scheduler is what says the definition reconciled rather than merely that
    // some other worker published for it.
    let snapshot = health.snapshot();
    assert_eq!(
        snapshot.status,
        WorkerHealthStatus::Ready,
        "reconciliation reported a conflict against its own definition: {:?}",
        snapshot.failures
    );
    stop_worker(shutdown, run).await;

    // The stored form really is the expanded integer the Rust-side comparison
    // could never match, so this test is exercising the round trip it claims to.
    let stored = sqlx::query_scalar::<_, String>(
        "SELECT definition -> 'meta' ->> 'big' FROM pgqueue.cron_schedules
         WHERE queue = $1 AND dedupe_key = $2",
    )
    .bind(db.queue.name())
    .bind(dedupe_key)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(stored, "10000000000000000");
}

#[sqlx::test(migrations = "./migrations")]
async fn test_cron_equal_revision_rejects_a_different_definition(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let first = dynamic_worker(
        db.queue.clone(),
        "0 * * * *",
        "revision-conflict",
        skip_options(4, Duration::from_secs(1)),
        Arc::new(AtomicU32::new(0)),
    );
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(first.run_until(shutdown.clone()));
    wait_for_schedule(&pool, db.queue.name(), "revision-conflict").await;

    let conflicting = dynamic_worker(
        db.another_queue(|builder| builder).await,
        "30 * * * *",
        "revision-conflict",
        skip_options(4, Duration::from_secs(1)),
        Arc::new(AtomicU32::new(0)),
    );
    // A rejected cron definition disables that cron and degrades scheduler
    // health, but the worker keeps running so unrelated jobs still flow.
    let conflicting_health = conflicting.health();
    let conflicting_shutdown = CancellationToken::new();
    let conflicting_run = tokio::spawn(conflicting.run_until(conflicting_shutdown.clone()));
    wait_until(
        Duration::from_secs(3),
        Duration::from_millis(10),
        "conflicting worker did not report degraded scheduler health",
        || async {
            let snapshot = conflicting_health.snapshot();
            snapshot.status == WorkerHealthStatus::Degraded
                && snapshot.failures.iter().any(|failure| failure.component == WorkerComponent::Scheduler)
        },
    )
    .await;
    // The authority's schedule is untouched by the rejected definition.
    assert_eq!(
        schedule_cursor(&pool, db.queue.name(), "revision-conflict").await.unwrap().to_zoned(TimeZone::UTC).minute(),
        0,
    );
    stop_worker(conflicting_shutdown, conflicting_run).await;
    stop_worker(shutdown, run).await;
}

#[sqlx::test(migrations = "./migrations")]
async fn test_cron_higher_revision_takes_authority_and_degrades_lower_workers(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let lower = dynamic_worker_with_heartbeat(
        db.queue.clone(),
        "0 * * * *",
        "revision-takeover",
        skip_options(1, Duration::from_secs(1)),
        Arc::new(AtomicU32::new(0)),
    );
    let lower_health = lower.health();
    let lower_shutdown = CancellationToken::new();
    let lower_run = tokio::spawn(lower.run_until(lower_shutdown.clone()));
    wait_for_schedule(&pool, db.queue.name(), "revision-takeover").await;

    let higher = dynamic_worker(
        db.another_queue(|builder| builder).await,
        "30 * * * *",
        "revision-takeover",
        skip_options(2, Duration::from_secs(1)),
        Arc::new(AtomicU32::new(0)),
    );
    let higher_shutdown = CancellationToken::new();
    let higher_run = tokio::spawn(higher.run_until(higher_shutdown.clone()));
    wait_until(
        Duration::from_secs(3),
        Duration::from_millis(10),
        "higher cron revision did not take authority",
        || async {
            sqlx::query_scalar::<_, i64>(
                "SELECT revision FROM pgqueue.cron_schedules WHERE queue = $1 AND dedupe_key = $2",
            )
            .bind(db.queue.name())
            .bind("revision-takeover")
            .fetch_optional(&pool)
            .await
            .unwrap()
                == Some(2)
        },
    )
    .await;
    let revised_cursor = schedule_cursor(&pool, db.queue.name(), "revision-takeover").await.unwrap();
    assert_eq!(
        revised_cursor.to_zoned(TimeZone::UTC).minute(),
        30,
        "changing the expression did not reset the durable cursor"
    );
    // Being superseded is normal for a not-yet-upgraded worker during a rolling
    // deploy, but is permanent after a rollback. The worker stays operational
    // and exposes the condition through degraded scheduler health.
    //
    // Retire the authority and drag the cursor into the past first. The takeover
    // left the cursor a minute away, so nothing was due in the window below.
    // Without backdating, the cursor would not move even if the lower worker
    // kept scheduling, so the guard this test asserts would never be reached.
    stop_worker(higher_shutdown, higher_run).await;
    let due_at = backdate_schedule(&pool, db.queue.name(), "revision-takeover").await;
    // Scheduling passes, not wall clock: the assertions below are all negative,
    // and one pass is all it takes for a lower worker that kept scheduling to
    // publish the occurrence now sitting due.
    wait_for_scheduling_passes(&db.database, 2).await;
    let snapshot = lower_health.snapshot();
    assert_eq!(snapshot.status, WorkerHealthStatus::Degraded, "{snapshot:?}");
    assert!(
        snapshot.failures.iter().any(|failure| {
            failure.component == WorkerComponent::Scheduler
                && failure.message.contains("superseded by durable revision 2")
        }),
        "{snapshot:?}"
    );
    // ...and it really has stopped advancing the superseded schedule, having
    // published nothing against a revision that is no longer its own.
    assert_eq!(
        schedule_cursor(&pool, db.queue.name(), "revision-takeover").await,
        Some(due_at),
        "a superseded worker advanced a cursor it no longer owns"
    );
    assert_eq!(
        cron_jobs_published(&pool, db.queue.name(), "revision-takeover").await,
        0,
        "a superseded worker published an occurrence"
    );
    stop_worker(lower_shutdown, lower_run).await;

    // And the occurrence really was there to take.
    assert_authority_advances(&db, "30 * * * *", "revision-takeover", skip_options(2, Duration::from_secs(1)), due_at)
        .await;
}

/// The other half of supersession: a worker that starts *after* a higher
/// revision has taken over is refused at reconciliation, by the UPSERT's
/// `revision < EXCLUDED.revision` guard, and never reaches scheduling at all.
/// That is every not-yet-restarted process in a rolling deploy, so it must stay
/// operational but degraded, leave the authority's revision alone, and — the
/// point of the whole mechanism — publish nothing for a cron that is due right
/// now.
#[sqlx::test(migrations = "./migrations")]
async fn test_cron_reconcile_refuses_a_worker_whose_revision_is_already_superseded(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    register_dynamic_schedule(
        &db,
        "30 * * * *",
        "revision-superseded",
        skip_options(9, Duration::from_secs(1)),
        Arc::new(AtomicU32::new(0)),
    )
    .await;
    let due_at = backdate_schedule(&pool, db.queue.name(), "revision-superseded").await;

    let superseded = dynamic_worker_with_heartbeat(
        db.queue.clone(),
        "30 * * * *",
        "revision-superseded",
        skip_options(8, Duration::from_secs(1)),
        Arc::new(AtomicU32::new(0)),
    );
    let health = superseded.health();
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(superseded.run_until(shutdown.clone()));
    // Scheduling passes, not wall clock: see `wait_for_scheduling_passes`. The
    // refused cron is due right now, so one pass that reached it would publish.
    wait_for_scheduling_passes(&db.database, 2).await;

    let snapshot = health.snapshot();
    assert_eq!(snapshot.status, WorkerHealthStatus::Degraded, "{snapshot:?}");
    assert!(
        snapshot.failures.iter().any(|failure| {
            failure.component == WorkerComponent::Scheduler
                && failure.message.contains("superseded by durable revision 9")
        }),
        "{snapshot:?}"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT revision FROM pgqueue.cron_schedules WHERE queue = $1 AND dedupe_key = $2",
        )
        .bind(db.queue.name())
        .bind("revision-superseded")
        .fetch_one(&pool)
        .await
        .unwrap(),
        9,
        "an older revision overwrote the authority's definition"
    );
    assert_eq!(
        schedule_cursor(&pool, db.queue.name(), "revision-superseded").await,
        Some(due_at),
        "a superseded worker advanced a cursor it never owned"
    );
    assert_eq!(
        cron_jobs_published(&pool, db.queue.name(), "revision-superseded").await,
        0,
        "a superseded worker published an occurrence"
    );
    stop_worker(shutdown, run).await;

    assert_authority_advances(
        &db,
        "30 * * * *",
        "revision-superseded",
        skip_options(9, Duration::from_secs(1)),
        due_at,
    )
    .await;
}

//noinspection SqlNoDataSourceInspection
#[sqlx::test(migrations = "./migrations")]
async fn test_cron_cursor_claim_and_job_insert_roll_back_together(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let counter = Arc::new(AtomicU32::new(0));
    let (expression, missed) = daily_schedule(&pool).await;
    // Ten minutes of grace on a daily schedule, so publication is bounded by the
    // grace alone and nothing this test does can outrun it. See `daily_schedule`.
    let options = skip_options(1, Duration::from_secs(600));
    register_dynamic_schedule(&db, &expression, "atomic-publication", options, counter.clone()).await;
    sqlx::query("UPDATE pgqueue.cron_schedules SET next_run_at = $3 WHERE queue = $1 AND dedupe_key = $2")
        .bind(db.queue.name())
        .bind("atomic-publication")
        .bind(missed.to_sqlx())
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "ALTER TABLE pgqueue.jobs
         ADD CONSTRAINT reject_cron_insert_for_test CHECK (kind <> 'cron') NOT VALID",
    )
    .execute(&pool)
    .await
    .unwrap();

    let worker = dynamic_worker(db.queue.clone(), &expression, "atomic-publication", options, counter.clone());
    let health = worker.health();
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));
    wait_until(
        Duration::from_secs(3),
        Duration::from_millis(10),
        "scheduler insert failure was not reported",
        || async { health.snapshot().status == WorkerHealthStatus::Degraded },
    )
    .await;
    assert_eq!(schedule_cursor(&pool, db.queue.name(), "atomic-publication").await, Some(missed));
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            r#"SELECT count(*) FROM pgqueue.cron_occurrences WHERE queue = $1 AND dedupe_key = $2"#
        )
        .bind(db.queue.name())
        .bind("atomic-publication")
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );

    let drop_insert_constraint = "ALTER TABLE pgqueue.jobs DROP CONSTRAINT reject_cron_insert_for_test".to_string();
    sqlx::query(sqlx::AssertSqlSafe(drop_insert_constraint)).execute(&pool).await.unwrap();
    wait_until(Duration::from_secs(3), Duration::from_millis(10), "rolled-back occurrence was not retried", || async {
        counter.load(Ordering::SeqCst) == 1
    })
    .await;
    wait_until(
        Duration::from_secs(3),
        Duration::from_millis(10),
        "scheduler health did not recover after publication succeeded",
        || async { health.snapshot().status == WorkerHealthStatus::Ready },
    )
    .await;
    stop_worker(shutdown, run).await;
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            r#"SELECT count(*) FROM pgqueue.cron_occurrences
               WHERE queue = $1 AND dedupe_key = $2 AND scheduled_at = $3"#,
        )
        .bind(db.queue.name())
        .bind("atomic-publication")
        .bind(missed.to_sqlx())
        .fetch_one(&pool)
        .await
        .unwrap(),
        1
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn test_cron_foreign_live_holder_claims_and_skips_the_occurrence(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let counter = Arc::new(AtomicU32::new(0));
    let (expression, missed) = daily_schedule(&pool).await;
    // Ten minutes of grace on a daily schedule, so publication is bounded by the
    // grace alone and nothing this test does can outrun it. See `daily_schedule`.
    let options = skip_options(1, Duration::from_secs(600));
    register_dynamic_schedule(&db, &expression, "foreign-holder", options, counter.clone()).await;
    let owner = db
        .queue
        .enqueue(yearly::job().dedupe_key("foreign-holder").delay(Duration::from_secs(60)))
        .await
        .unwrap()
        .unwrap();
    sqlx::query("UPDATE pgqueue.cron_schedules SET next_run_at = $3 WHERE queue = $1 AND dedupe_key = $2")
        .bind(db.queue.name())
        .bind("foreign-holder")
        .bind(missed.to_sqlx())
        .execute(&pool)
        .await
        .unwrap();

    let worker = dynamic_worker(db.queue.clone(), &expression, "foreign-holder", options, counter.clone());
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));
    wait_until(Duration::from_secs(3), Duration::from_millis(10), "held occurrence was not advanced", || async {
        schedule_cursor(&pool, db.queue.name(), "foreign-holder").await.is_some_and(|cursor| cursor > missed)
    })
    .await;

    assert_eq!(counter.load(Ordering::SeqCst), 0);
    assert_eq!(owner.fetch_job().await.unwrap().status, JobStatus::Queued);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            r#"SELECT count(*) FROM pgqueue.cron_occurrences
               WHERE queue = $1 AND dedupe_key = $2 AND scheduled_at = $3"#,
        )
        .bind(db.queue.name())
        .bind("foreign-holder")
        .bind(missed.to_sqlx())
        .fetch_one(&pool)
        .await
        .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            r#"SELECT count(*) FROM pgqueue.jobs
               WHERE queue = $1 AND dedupe_key = $2 AND kind = 'cron'"#,
        )
        .bind(db.queue.name())
        .bind("foreign-holder")
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );
    stop_worker(shutdown, run).await;
}

/// The cron twin of the vanished out-of-band dedupe owner: a foreign writer
/// takes the cron's dedupe key between `schedule_cron`'s holder pre-check and
/// its insert, then releases it again before the holder can be re-read. The
/// occurrence claim rolls back with the error, so a later pass republishes it:
/// the failure is transient, and health must degrade and recover around it.
#[sqlx::test(migrations = "./migrations")]
async fn test_cron_retries_an_occurrence_lost_to_a_vanished_foreign_holder(pool: PgPool) {
    const INSERT_GATE: i32 = 20_574;
    const CONFLICT_GATE: i32 = 20_575;

    let db = TestDb::new(pool.clone()).await;
    let counter = Arc::new(AtomicU32::new(0));
    let (expression, missed) = daily_schedule(&pool).await;
    // Ten minutes of grace on a daily schedule, so publication is bounded by the
    // grace alone and nothing this test does can outrun it. See `daily_schedule`.
    let options = skip_options(1, Duration::from_secs(600));
    register_dynamic_schedule(&db, &expression, "vanishing-holder", options, counter.clone()).await;
    sqlx::query("UPDATE pgqueue.cron_schedules SET next_run_at = $3 WHERE queue = $1 AND dedupe_key = $2")
        .bind(db.queue.name())
        .bind("vanishing-holder")
        .bind(missed.to_sqlx())
        .execute(&pool)
        .await
        .unwrap();
    // Pause the publication between its holder pre-check and its insert...
    crate::install_statement_gate(&pool, "wait_at_cron_insert", INSERT_GATE, "INSERT", "NEW.kind = 'cron'").await;
    // ...and between its conflict decision and its holder re-read.
    crate::install_conflicted_insert_gate(&pool, "wait_at_cron_conflict", CONFLICT_GATE).await;
    let insert_gate = crate::hold_gate(&pool, INSERT_GATE, &db.database).await;
    let conflict_gate = crate::hold_gate(&pool, CONFLICT_GATE, &db.database).await;

    let worker = dynamic_worker(db.queue.clone(), &expression, "vanishing-holder", options, counter.clone());
    let health = worker.health();
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));
    crate::wait_for_lock_waiter(&db, "%WITH inserted AS (%", "cron publication did not reach its insert").await;

    // A row that takes the cron's dedupe key without the enqueue advisory
    // lock...
    let holder = sqlx::query_scalar::<_, uuid::Uuid>(
        r#"INSERT INTO pgqueue.jobs (queue, name, payload, dedupe_key, status, max_attempts)
           VALUES ($1, 'out-of-band', 'null'::jsonb, 'vanishing-holder', 'queued', 1)
           RETURNING id"#,
    )
    .bind(db.queue.name())
    .fetch_one(&pool)
    .await
    .unwrap();
    insert_gate.rollback().await.unwrap();
    crate::wait_for_advisory_waiter(&pool, CONFLICT_GATE, "conflicted cron insert did not reach its holder re-read")
        .await;
    // ...and releases it again before the holder can be named.
    sqlx::query("DELETE FROM pgqueue.jobs WHERE id = $1").bind(holder).execute(&pool).await.unwrap();

    // The degraded state this test asserts on is over almost as soon as it
    // begins: measured, the scheduler reported the lost occurrence and cleared
    // it again by republishing 8 ms later. One 10 ms poll can step over that
    // brief edge entirely, so a level-triggered assertion can miss it. The retry
    // is parked at the insert gate before it can happen, and the degraded state
    // lasts as long as the assertion needs.
    //
    // Queued from another task rather than taken here, and *before* the
    // conflicted publication is released: that publication holds the insert
    // gate's lock for its own transaction, so taking it on this task would wait
    // for a statement that is itself waiting on the gate this task has to roll
    // back. Queued first, PostgreSQL hands the lock to this waiter rather than
    // to the retry that asks for it later.
    let retry_gate = tokio::spawn({
        let pool = pool.clone();
        let database = db.database.clone();
        async move { crate::hold_gate(&pool, INSERT_GATE, &database).await }
    });
    crate::wait_for_advisory_waiter(&pool, INSERT_GATE, "the retry gate never queued behind the failing publication")
        .await;
    conflict_gate.rollback().await.unwrap();

    // The lost occurrence degrades scheduler health with the race error...
    let failure = wait_for_some(
        Duration::from_secs(3),
        Duration::from_millis(10),
        "the lost occurrence did not degrade scheduler health",
        || async {
            health.snapshot().failures.into_iter().find(|failure| {
                failure.component == WorkerComponent::Scheduler && failure.message.contains("lost its dedupe key")
            })
        },
    )
    .await;
    assert!(
        failure.message.contains("dedupe race") && !failure.message.contains("configuration"),
        "a transient dedupe race must not look like a permanent misconfiguration: {}",
        failure.message
    );
    // ...and the rolled-back claim is republished by a later pass.
    retry_gate.await.unwrap().rollback().await.unwrap();
    wait_until(Duration::from_secs(5), Duration::from_millis(10), "the lost occurrence was not retried", || async {
        counter.load(Ordering::SeqCst) == 1
    })
    .await;
    // The `Scheduler` component specifically, not the whole snapshot:
    // `WorkerHealthStatus::Ready` requires *every* component healthy, including
    // `Notification`, whose listener connects outside the pool and whose
    // reconnect backoff climbs to 5s. One LISTEN connection refused while the
    // rest of the suite runs beside this outlasts any budget, and says nothing
    // about the occurrence this test lost and retried.
    wait_until(
        Duration::from_secs(5),
        Duration::from_millis(10),
        "scheduler health did not recover after the retry published",
        || async { !health.snapshot().failures.iter().any(|failure| failure.component == WorkerComponent::Scheduler) },
    )
    .await;
    stop_worker(shutdown, run).await;
    assert_eq!(
        cron_jobs_published(&pool, db.queue.name(), "vanishing-holder").await,
        1,
        "the retried occurrence must be published exactly once"
    );
}

//noinspection SqlNoDataSourceInspection
#[sqlx::test(migrations = "./migrations")]
async fn test_cron_rechecks_skip_grace_after_waiting_for_the_dedupe_lock(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let counter = Arc::new(AtomicU32::new(0));
    // Daily, not hourly: the grace below has to be the only thing bounding
    // publication. See `daily_schedule`.
    let (expression, _) = daily_schedule(&pool).await;
    // Ten seconds, not five: the assertion below requires the scheduler to reach
    // the dedupe lock *before* the grace closes, and everything between the
    // fixture and that point — building a worker, spawning `run_until`, startup
    // reconciliation, the LISTEN connection, the lease write, a scheduler tick,
    // and `wait_for_dequeue_lock_waiter`'s own five-second budget — is setup this
    // test would otherwise be timing. Ten leaves the lock-waiter budget the
    // tighter of the two, so an overrun fails as the timeout it is rather than as
    // a precondition pointed at the scheduler.
    let grace = Duration::from_secs(10);
    let options = skip_options(1, grace);
    register_dynamic_schedule(&db, &expression, "lock-wait", options, counter.clone()).await;

    let mut transaction = db.queue.pool().begin().await.unwrap();
    db.queue
        .enqueue_in(&mut transaction, dynamic_tick::job(()).dedupe_key("lock-wait").delay(Duration::from_secs(60)))
        .await
        .unwrap();
    let occurrence = sqlx::query_scalar::<_, jiff_sqlx::Timestamp>(
        "UPDATE pgqueue.cron_schedules
         SET next_run_at = clock_timestamp()
         WHERE queue = $1 AND dedupe_key = $2
         RETURNING next_run_at",
    )
    .bind(db.queue.name())
    .bind("lock-wait")
    .fetch_one(&pool)
    .await
    .unwrap()
    .to_jiff();
    let worker = dynamic_worker(db.queue.clone(), &expression, "lock-wait", options, counter.clone());
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));

    crate::wait_for_dequeue_lock_waiter(&db.queue, true).await;
    let now = sqlx::query_scalar::<_, jiff_sqlx::Timestamp>("SELECT clock_timestamp()")
        .fetch_one(&pool)
        .await
        .unwrap()
        .to_jiff();
    let deadline = occurrence + SignedDuration::try_from(grace).unwrap();
    assert!(now < deadline, "scheduler reached the lock after grace had already closed");
    let remaining = Duration::try_from(deadline.duration_since(now)).unwrap();
    tokio::time::sleep(remaining + Duration::from_millis(100)).await;
    transaction.rollback().await.unwrap();
    wait_until(
        Duration::from_secs(3),
        Duration::from_millis(10),
        "scheduler did not advance after the dedupe lock was released",
        || async {
            schedule_cursor(&pool, db.queue.name(), "lock-wait").await.is_some_and(|cursor| cursor > occurrence)
        },
    )
    .await;
    stop_worker(shutdown, run).await;

    assert_eq!(counter.load(Ordering::SeqCst), 0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(r#"SELECT count(*) FROM pgqueue.jobs WHERE queue = $1 AND dedupe_key = $2"#)
            .bind(db.queue.name())
            .bind("lock-wait")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn test_cron_lock_wait_observes_worker_shutdown(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let counter = Arc::new(AtomicU32::new(0));
    // Only the expression matters here: this test sets the cursor itself and asserts on shutdown, never on which
    // occurrence was chosen. Yearly, so the schedule comes due only when this test says so: minutely, a boundary
    // crossed after reconciliation let the scheduler lock the schedule row first and park on the dedupe lock this test
    // holds, and the `UPDATE` below then waited on that row lock for ever.
    let expression = "0 0 1 1 *".to_owned();
    let options = skip_options(1, Duration::from_secs(60));
    let mut transaction = db.queue.pool().begin().await.unwrap();
    db.queue
        .enqueue_in(
            &mut transaction,
            dynamic_tick::job(()).dedupe_key("shutdown-lock-wait").delay(Duration::from_secs(60)),
        )
        .await
        .unwrap();

    let worker = dynamic_worker(db.queue.clone(), &expression, "shutdown-lock-wait", options, counter);
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));
    wait_for_schedule(&pool, db.queue.name(), "shutdown-lock-wait").await;
    sqlx::query(
        "UPDATE pgqueue.cron_schedules SET next_run_at = now()
         WHERE queue = $1 AND dedupe_key = $2",
    )
    .bind(db.queue.name())
    .bind("shutdown-lock-wait")
    .execute(&pool)
    .await
    .unwrap();
    crate::wait_for_dequeue_lock_waiter(&db.queue, true).await;

    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(3), run)
        .await
        .expect("worker did not stop while the cron lock remained held")
        .unwrap()
        .unwrap();
    transaction.rollback().await.unwrap();
}

#[sqlx::test(migrations = "./migrations")]
async fn test_cron_reconciliation_lock_wait_observes_worker_shutdown(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let counter = Arc::new(AtomicU32::new(0));
    // Only the expression matters here: this test sets the cursor itself and asserts on shutdown, never on which
    // occurrence was chosen. Yearly, so the schedule comes due only when this test says so: minutely, a boundary
    // crossed after reconciliation let the scheduler lock the schedule row first and park on the dedupe lock this test
    // holds, and the `UPDATE` below then waited on that row lock for ever.
    let expression = "0 0 1 1 *".to_owned();
    let options = skip_options(1, Duration::from_secs(60));
    let mut transaction = db.queue.pool().begin().await.unwrap();
    db.queue
        .enqueue_in(
            &mut transaction,
            dynamic_tick::job(()).dedupe_key("reconcile-shutdown-lock-wait").delay(Duration::from_secs(60)),
        )
        .await
        .unwrap();

    let scheduler =
        dynamic_worker(db.queue.clone(), &expression, "reconcile-shutdown-lock-wait", options, counter.clone());
    let scheduler_shutdown = CancellationToken::new();
    let scheduler_run = tokio::spawn(scheduler.run_until(scheduler_shutdown.clone()));
    wait_for_schedule(&pool, db.queue.name(), "reconcile-shutdown-lock-wait").await;
    sqlx::query(
        "UPDATE pgqueue.cron_schedules SET next_run_at = now()
         WHERE queue = $1 AND dedupe_key = $2",
    )
    .bind(db.queue.name())
    .bind("reconcile-shutdown-lock-wait")
    .execute(&pool)
    .await
    .unwrap();
    crate::wait_for_dequeue_lock_waiter(&db.queue, true).await;

    let starting = dynamic_worker(db.queue.clone(), &expression, "reconcile-shutdown-lock-wait", options, counter);
    let starting_shutdown = CancellationToken::new();
    let starting_run = tokio::spawn(starting.run_until(starting_shutdown.clone()));
    crate::wait_for_lock_waiter(
        &db,
        "%INSERT INTO pgqueue.cron_schedules%",
        "starting worker did not wait on cron reconciliation",
    )
    .await;

    starting_shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(3), starting_run)
        .await
        .expect("starting worker did not stop while cron reconciliation was locked")
        .unwrap()
        .unwrap();
    scheduler_shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(3), scheduler_run)
        .await
        .expect("scheduler did not stop while the cron key remained locked")
        .unwrap()
        .unwrap();
    transaction.rollback().await.unwrap();
}

#[sqlx::test(migrations = "./migrations")]
async fn test_cron_builder_rejects_manual_schedule_overrides(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let error = Worker::builder(db.queue.clone())
        .schedule_cron("* * * * *", dynamic_tick::job(()).delay(Duration::from_secs(1)))
        .build()
        .unwrap_err();
    assert!(error.to_string().contains("cannot use delay"), "{error}");

    let error = Worker::builder(db.queue).schedule_cron("0 * * * * *", dynamic_tick::job(())).build().unwrap_err();
    assert!(error.to_string().contains("expected 5"), "{error}");
}

/// A cursor more than one period stale is correctly refused, but jumping
/// straight to `next_occurrence(now)` silently threw away the *most recent*
/// occurrence even while it was still well inside its own grace — no job row,
/// no claim row, and no `SkippedStale` warning for it. Every catch-up (restart,
/// leader handover, deploy gap) cost one extra occurrence.
#[sqlx::test(migrations = "./migrations")]
async fn test_cron_skip_publishes_the_recent_occurrence_when_the_cursor_is_stale(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let counter = Arc::new(AtomicU32::new(0));
    // Hourly with a 45-minute grace, mirroring the sibling test below. The
    // deadline is `min(successor, occurrence + grace)`, so on a minutely
    // schedule the successor caps the window at the next minute boundary
    // whatever the grace says — leaving a run that started late in a minute
    // only the remainder of it. Here the grace binds instead: the occurrence
    // half an hour ago is still well inside it, and the stored cursor a whole
    // period further back is not.
    let (expression, recent) = hourly_schedule_half_an_hour_ago(&pool).await;
    let options = skip_options(1, Duration::from_secs(45 * 60));
    register_dynamic_schedule(&db, &expression, "stale-catch-up", options, counter.clone()).await;
    // A whole period further back, so the stored cursor is genuinely stale.
    let stale = recent - SignedDuration::from_hours(1);
    sqlx::query("UPDATE pgqueue.cron_schedules SET next_run_at = $3 WHERE queue = $1 AND dedupe_key = $2")
        .bind(db.queue.name())
        .bind("stale-catch-up")
        .bind(stale.to_sqlx())
        .execute(&pool)
        .await
        .unwrap();

    let worker = dynamic_worker(db.queue.clone(), &expression, "stale-catch-up", options, counter.clone());
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));
    wait_until(
        Duration::from_secs(5),
        Duration::from_millis(10),
        "the still-publishable occurrence was discarded",
        || async { counter.load(Ordering::SeqCst) == 1 },
    )
    .await;
    stop_worker(shutdown, run).await;

    let scheduled_at = sqlx::query_scalar::<_, jiff_sqlx::Timestamp>(
        "SELECT scheduled_at FROM pgqueue.jobs
         WHERE queue = $1 AND dedupe_key = $2 AND kind = 'cron'",
    )
    .bind(db.queue.name())
    .bind("stale-catch-up")
    .fetch_one(&pool)
    .await
    .unwrap()
    .to_jiff();
    assert_eq!(scheduled_at, recent, "catch-up must publish the most recent occurrence, not the stale cursor");
    assert_eq!(
        schedule_cursor(&pool, db.queue.name(), "stale-catch-up").await,
        Some(recent + SignedDuration::from_hours(1)),
        "the cursor advances past the occurrence it just published",
    );
}

/// The catch-up fallback is bounded by the same grace as the stored cursor: a
/// recent occurrence that is *also* past its deadline must still be skipped,
/// or `Skip` would degrade into `FireOnce`.
#[sqlx::test(migrations = "./migrations")]
async fn test_cron_skip_discards_a_recent_occurrence_that_is_past_its_own_grace(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let counter = Arc::new(AtomicU32::new(0));
    let (expression, recent) = hourly_schedule_half_an_hour_ago(&pool).await;
    // One second of grace, so the latest hourly occurrence is stale as well.
    let options = skip_options(1, Duration::from_secs(1));
    register_dynamic_schedule(&db, &expression, "stale-both", options, counter.clone()).await;
    let stale = recent - SignedDuration::from_hours(1);
    sqlx::query("UPDATE pgqueue.cron_schedules SET next_run_at = $3 WHERE queue = $1 AND dedupe_key = $2")
        .bind(db.queue.name())
        .bind("stale-both")
        .bind(stale.to_sqlx())
        .execute(&pool)
        .await
        .unwrap();

    let worker = dynamic_worker(db.queue.clone(), &expression, "stale-both", options, counter.clone());
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));
    wait_until(Duration::from_secs(5), Duration::from_millis(10), "stale cursor was not advanced", || async {
        schedule_cursor(&pool, db.queue.name(), "stale-both").await.is_some_and(|cursor| cursor > recent)
    })
    .await;
    stop_worker(shutdown, run).await;

    assert_eq!(counter.load(Ordering::SeqCst), 0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            r#"SELECT count(*) FROM pgqueue.jobs
               WHERE queue = $1 AND dedupe_key = $2 AND kind = 'cron'"#,
        )
        .bind(db.queue.name())
        .bind("stale-both")
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );
}

/// `state.rejected` mixed permanent rejections — a reused revision with a
/// different definition, which disables the cron and is never re-evaluated —
/// with transient ones, and every scheduling pass cleared the whole vector
/// before re-reconciling only the retryable keys. So an *unrelated* cron
/// recovering from a database blip erased the permanent failure and the worker
/// reported itself Ready while a cron stayed silently disabled forever.
#[sqlx::test(migrations = "./migrations")]
async fn test_cron_health_keeps_a_permanent_rejection_when_a_transient_one_recovers(pool: PgPool) {
    let db = TestDb::new(pool_with_max(&pool, 10).await).await;

    // The authority establishes "mixed-permanent" at revision 4.
    let authority = dynamic_worker(
        db.queue.clone(),
        "0 * * * *",
        "mixed-permanent",
        skip_options(4, Duration::from_secs(1)),
        Arc::new(AtomicU32::new(0)),
    );
    let authority_shutdown = CancellationToken::new();
    let authority_run = tokio::spawn(authority.run_until(authority_shutdown.clone()));
    wait_for_schedule(&pool, db.queue.name(), "mixed-permanent").await;
    stop_worker(authority_shutdown, authority_run).await;

    // A database blip that only affects "mixed-transient".
    sqlx::raw_sql(
        "CREATE FUNCTION pgqueue.repro_mixed_outage() RETURNS trigger
         LANGUAGE plpgsql AS $$
         BEGIN RAISE EXCEPTION 'transient: terminating connection due to failover'; END $$;
         CREATE TRIGGER repro_mixed_outage
         BEFORE INSERT OR UPDATE ON pgqueue.cron_schedules
         FOR EACH ROW WHEN (NEW.dedupe_key = 'mixed-transient')
         EXECUTE FUNCTION pgqueue.repro_mixed_outage();",
    )
    .execute(&pool)
    .await
    .unwrap();

    // One worker, two crons: a permanently rejected definition (revision 4
    // reused for a different expression) and a transiently failing one.
    let counter = Arc::new(AtomicU32::new(0));
    let subject = Worker::builder(db.another_queue(|builder| builder).await)
        .schedule_cron_with_options(
            "30 * * * *",
            dynamic_tick::job(()).dedupe_key("mixed-permanent"),
            skip_options(4, Duration::from_secs(1)),
        )
        // Never due, so the subject only ever reconciles: this is a test about
        // health reporting, and publishing on top of it would make shutdown
        // wait on attempts that have nothing to do with the assertion.
        .schedule_cron_with_options(
            "0 0 1 1 *",
            dynamic_tick::job(()).dedupe_key("mixed-transient"),
            skip_options(1, Duration::from_secs(1)),
        )
        .state(counter)
        .timers(timers())
        .poll_interval(Duration::from_millis(20))
        .shutdown_grace(Duration::from_secs(10))
        .build()
        .unwrap();
    let health = subject.health();
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(subject.run_until(shutdown.clone()));

    let scheduler_failures = || {
        health
            .snapshot()
            .failures
            .into_iter()
            .filter(|failure| failure.component == WorkerComponent::Scheduler)
            .map(|failure| failure.message)
            .collect::<Vec<_>>()
    };
    wait_until(
        Duration::from_secs(10),
        Duration::from_millis(20),
        "both cron failures were never reported together",
        || async {
            scheduler_failures()
                .iter()
                .any(|message| message.contains("mixed-permanent") && message.contains("mixed-transient"))
        },
    )
    .await;

    // The blip is over; only the transient cron can recover.
    let drop_outage_trigger = "DROP TRIGGER repro_mixed_outage ON pgqueue.cron_schedules".to_string();
    sqlx::raw_sql(sqlx::AssertSqlSafe(drop_outage_trigger)).execute(&pool).await.unwrap();
    wait_until(
        Duration::from_secs(10),
        Duration::from_millis(20),
        "the transient cron never reconciled after the blip cleared",
        || async { schedule_cursor(&pool, db.queue.name(), "mixed-transient").await.is_some() },
    )
    .await;
    // Give the scheduling loop several passes to (wrongly) report recovery.
    wait_until(
        Duration::from_secs(10),
        Duration::from_millis(20),
        "the transient failure was never dropped from the health report",
        || async { scheduler_failures().iter().all(|message| !message.contains("mixed-transient")) },
    )
    .await;

    let snapshot = health.snapshot();
    assert_eq!(
        snapshot.status,
        WorkerHealthStatus::Degraded,
        "a permanently disabled cron must keep degrading health: {:?}",
        snapshot.failures
    );
    assert!(
        scheduler_failures().iter().any(|message| message.contains("mixed-permanent")),
        "the permanent rejection was erased by an unrelated recovery: {:?}",
        snapshot.failures
    );

    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(20), run).await.expect("subject worker did not stop").unwrap().unwrap();
}

/// Each startup reconciliation needs its own database clock reading. Reusing
/// one reading across a large registry can make later cursors stale. Because
/// five-field schedules only change on minute boundaries, count the clock
/// queries directly instead of making this test wait for a boundary.
#[sqlx::test(migrations = "./migrations")]
async fn test_cron_reconciliation_reads_the_clock_once_per_entry(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let Some(mut stats) = Stats::new(&db.database).await else {
        Stats::skipped("test_cron_reconciliation_reads_the_clock_once_per_entry");
        return;
    };
    let clock_reads = stats.since_now("SELECT now()").await;

    let keys = ["clock-a", "clock-b", "clock-c", "clock-d"];
    let mut builder = Worker::builder(db.queue.clone());
    for key in keys {
        builder = builder.schedule_cron_with_options(
            "* * * * *",
            dynamic_tick::job(()).dedupe_key(key),
            skip_options(1, Duration::from_secs(1)),
        );
    }
    let worker = builder
        .state(Arc::new(AtomicU32::new(0)))
        .timers(timers())
        .poll_interval(Duration::from_millis(20))
        .build()
        .unwrap();
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));
    for key in keys {
        wait_for_schedule(&pool, db.queue.name(), key).await;
    }
    stop_worker(shutdown, run).await;

    assert_eq!(stats.delta(&clock_reads).await, keys.len() as i64);
}

#[pgqueue::cron("0 * * * *")]
async fn repro_ticker() -> anyhow::Result<()> {
    Ok(())
}

/// A pool timeout or a failover during a rolling restart fails startup
/// reconciliation. Reconciliation is one-shot, so without a retry the cron
/// would stay disabled for the lifetime of the process even though the database
/// recovered seconds later.
#[sqlx::test(migrations = "./migrations")]
async fn test_cron_recovers_when_a_transient_reconcile_failure_clears(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;

    sqlx::raw_sql(
        "CREATE FUNCTION pgqueue.repro_cron_outage() RETURNS trigger
         LANGUAGE plpgsql AS $$
         BEGIN RAISE EXCEPTION 'transient: terminating connection due to failover'; END $$;
         CREATE TRIGGER repro_cron_outage
         BEFORE INSERT OR UPDATE ON pgqueue.cron_schedules
         FOR EACH ROW EXECUTE FUNCTION pgqueue.repro_cron_outage();",
    )
    .execute(&pool)
    .await
    .unwrap();

    let shutdown = CancellationToken::new();
    let worker = Worker::builder(db.queue.clone())
        .register_cron(repro_ticker)
        .timers(WorkerTimers { schedule: Duration::from_millis(50), ..test_timers() })
        .build()
        .unwrap();
    let health = worker.health();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));

    wait_until(Duration::from_secs(10), Duration::from_millis(20), "scheduler never degraded", || async {
        health.snapshot().failures.iter().any(|failure| failure.component == WorkerComponent::Scheduler)
    })
    .await;

    // The outage is over.
    let drop_outage_trigger = "DROP TRIGGER repro_cron_outage ON pgqueue.cron_schedules".to_string();
    sqlx::raw_sql(sqlx::AssertSqlSafe(drop_outage_trigger)).execute(&pool).await.unwrap();

    wait_until(
        Duration::from_secs(10),
        Duration::from_millis(20),
        "the cron schedule was not reconciled after the database healed",
        || async { cron_next_run_at(&pool, db.queue.name(), "cron:repro_ticker").await.is_some() },
    )
    .await;
    set_cron_due(&pool, db.queue.name(), "cron:repro_ticker", None).await;

    wait_until(
        Duration::from_secs(10),
        Duration::from_millis(20),
        "the cron never recovered after the database healed",
        || async {
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM pgqueue.jobs WHERE queue = $1")
                .bind(db.queue.name())
                .fetch_one(&pool)
                .await
                .unwrap()
                > 0
        },
    )
    .await;
    wait_until(Duration::from_secs(10), Duration::from_millis(20), "scheduler health never recovered", || async {
        health.snapshot().status == WorkerHealthStatus::Ready
    })
    .await;

    shutdown.cancel();
    // Asserted, not discarded: a worker that ended in `Err` — a background loop
    // that panicked, a shutdown step that timed out — would otherwise pass this
    // test silently.
    assert!(
        matches!(crate::join_worker(run, Duration::from_secs(10)).await, Some(Ok(()))),
        "the worker did not shut down cleanly"
    );
}

// ---------------------------------------------------------------------------
// A refused LISTEN connection at startup
// ---------------------------------------------------------------------------

#[pgqueue::job(name = "repro_cron_tick", max_attempts = 1)]
async fn repro_cron_tick(_: ()) -> anyhow::Result<()> {
    Ok(())
}

/// A worker whose only work is one dynamic cron on `expression`.
fn cron_worker(queue: &Queue, expression: &str, dedupe_key: &str) -> Worker {
    Worker::builder(queue.clone())
        .schedule_cron_with_options(
            expression,
            repro_cron_tick::job(()).dedupe_key(dedupe_key),
            skip_options(0, Duration::from_secs(60)),
        )
        .timers(WorkerTimers { schedule: Duration::from_millis(40), ..test_timers() })
        .poll_interval(Duration::from_millis(20))
        .build()
        .unwrap()
}

async fn cron_job_count(pool: &PgPool, queue: &str, dedupe_key: &str) -> i64 {
    sqlx::query_scalar::<_, i64>("SELECT count(*) FROM pgqueue.jobs WHERE queue = $1 AND dedupe_key = $2")
        .bind(queue)
        .bind(dedupe_key)
        .fetch_one(pool)
        .await
        .unwrap()
}

/// Moves the durable cursor to `due`, which a
/// [`CronMisfirePolicy::Skip`] schedule publishes as its occurrence
/// while it is inside the grace. Returns it so a rewind can name the same
/// instant twice.
async fn set_cron_due(pool: &PgPool, queue: &str, dedupe_key: &str, due: Option<Timestamp>) -> Timestamp {
    sqlx::query_scalar::<_, jiff_sqlx::Timestamp>(
        "UPDATE pgqueue.cron_schedules
         SET next_run_at = COALESCE($3, clock_timestamp())
         WHERE queue = $1 AND dedupe_key = $2
         RETURNING next_run_at",
    )
    .bind(queue)
    .bind(dedupe_key)
    .bind(due.map(|timestamp| timestamp.to_sqlx()))
    .fetch_one(pool)
    .await
    .unwrap()
    .to_jiff()
}

async fn cron_next_run_at(pool: &PgPool, queue: &str, dedupe_key: &str) -> Option<Timestamp> {
    sqlx::query_scalar::<_, jiff_sqlx::Timestamp>(
        "SELECT next_run_at FROM pgqueue.cron_schedules WHERE queue = $1 AND dedupe_key = $2",
    )
    .bind(queue)
    .bind(dedupe_key)
    .fetch_optional(pool)
    .await
    .unwrap()
    .map(jiff_sqlx::Timestamp::to_jiff)
}

/// An occurrence is claimed in `pgqueue.cron_occurrences` before its job row is
/// written, and the claim is what makes publication idempotent across workers.
/// The arm that observes a claim already taken had no test, so nothing pinned
/// "an occurrence is published at most once" — the one guarantee a durable cron
/// registry exists to provide.
#[sqlx::test(migrations = "./migrations")]
async fn test_cron_publishes_an_occurrence_at_most_once_when_the_cursor_is_rewound(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let key = "repro-already-published";
    let worker = cron_worker(&db.queue, "0 3 * * *", key);
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));

    wait_until(Duration::from_secs(15), Duration::from_millis(10), "the cron was never reconciled", || async {
        cron_next_run_at(&pool, db.queue.name(), key).await.is_some()
    })
    .await;

    let due = set_cron_due(&pool, db.queue.name(), key, None).await;
    wait_until(
        Duration::from_secs(15),
        Duration::from_millis(10),
        "the due occurrence was never published",
        || async { cron_job_count(&pool, db.queue.name(), key).await == 1 },
    )
    .await;
    let occurrence = sqlx::query_scalar::<_, jiff_sqlx::Timestamp>(
        "SELECT scheduled_at FROM pgqueue.cron_occurrences WHERE queue = $1 AND dedupe_key = $2",
    )
    .bind(db.queue.name())
    .bind(key)
    .fetch_one(&pool)
    .await
    .unwrap()
    .to_jiff();
    assert_eq!(occurrence, due, "the cursor is what was claimed and published");

    // Remove the published job, so a second publication of the same occurrence
    // would be visible rather than indistinguishable from the first — and so
    // nothing holds the dedupe key, which would refuse the insert for its own
    // reason.
    sqlx::query("DELETE FROM pgqueue.jobs WHERE queue = $1 AND dedupe_key = $2")
        .bind(db.queue.name())
        .bind(key)
        .execute(&pool)
        .await
        .unwrap();

    // The same instant again: within the misfire grace the cursor *is* the
    // occurrence, so this pass recomputes exactly the one already claimed.
    set_cron_due(&pool, db.queue.name(), key, Some(due)).await;
    wait_until(
        Duration::from_secs(15),
        Duration::from_millis(10),
        "the scheduler never ran against the rewound cursor",
        || async { cron_next_run_at(&pool, db.queue.name(), key).await.is_some_and(|next| next > Timestamp::now()) },
    )
    .await;
    assert_eq!(
        cron_job_count(&pool, db.queue.name(), key).await,
        0,
        "an occurrence whose claim already exists must not be published twice"
    );
    let claims: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pgqueue.cron_occurrences
         WHERE queue = $1 AND dedupe_key = $2 AND scheduled_at = $3",
    )
    .bind(db.queue.name())
    .bind(key)
    .bind(occurrence.to_sqlx())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(claims, 1, "the original claim is what refused the republish");

    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(10), run).await.unwrap().unwrap().unwrap();
}

/// The claim `schedule_cron` makes once a schedule is due, and the statement a
/// contended tick comes back empty from. Its call count is how many times the
/// scheduler actually reached a locked row — which
/// [`CRON_SCHEDULE_READ`] does not answer, since a not-yet-due tick rolls back
/// before it.
const CRON_SCHEDULE_CLAIM: &str = "%FROM pgqueue.cron_schedules%FOR UPDATE SKIP LOCKED%";

/// `FOR UPDATE SKIP LOCKED` is what keeps two workers from both publishing the
/// same occurrence: the loser skips the row entirely and must leave the cursor
/// alone, so the occurrence is published by the winner and not lost. A short
/// collision is healthy, but one that persists must degrade scheduler health
/// until the row becomes available again.
#[sqlx::test(migrations = "./migrations")]
async fn test_cron_publishes_nothing_while_another_transaction_holds_the_schedule(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let key = "repro-contended";
    let worker = Worker::builder(db.queue.clone())
        .schedule_cron_with_options(
            "0 * * * *",
            repro_cron_tick::job(()).dedupe_key(key),
            skip_options(0, Duration::from_secs(60)),
        )
        .timers(WorkerTimers { schedule: Duration::from_secs(1), ..test_timers() })
        .poll_interval(Duration::from_millis(20))
        .build()
        .unwrap();
    let health = worker.health();
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));

    wait_until(Duration::from_secs(10), Duration::from_millis(10), "the cron was never reconciled", || async {
        cron_next_run_at(&pool, db.queue.name(), key).await.is_some()
    })
    .await;

    // Arming the schedule and locking it are two transactions, and a scheduler
    // tick fits between them: the tick claims the row first, publishes the
    // occurrence and advances the cursor a whole hour, so the lock lands on a
    // row that is no longer due. The scheduler then rolls back at its due check
    // without ever reaching the claim below, and the wait for that claim times
    // out 30 seconds later — the setup losing a race, reported as the behaviour
    // under test failing. The armed cursor comes back verbatim while nothing
    // else has written the row, so an inequality is exactly "a tick got there
    // first": drop the lock and arm it again.
    let (holder, held) = 'armed: {
        for _ in 0..10 {
            let armed = set_cron_due(&pool, db.queue.name(), key, None).await;
            let mut holder = pool.begin().await.unwrap();
            let held = sqlx::query_scalar::<_, jiff_sqlx::Timestamp>(
                "SELECT next_run_at FROM pgqueue.cron_schedules
                 WHERE queue = $1 AND dedupe_key = $2 FOR UPDATE",
            )
            .bind(db.queue.name())
            .bind(key)
            .fetch_optional(&mut *holder)
            .await
            .unwrap()
            .map(jiff_sqlx::Timestamp::to_jiff)
            .expect("the reconciled schedule row");
            if held == armed {
                break 'armed (holder, Some(held));
            }
            holder.rollback().await.unwrap();
        }
        panic!("the scheduler published the armed occurrence before the lock landed, ten times over");
    };
    let published = cron_job_count(&pool, db.queue.name(), key).await;

    // The window is the scheduler's own passes, not a stretch of wall clock: a
    // fixed sleep asserts "nothing published" over however many ticks a loaded
    // runner happened to deliver, which on a slow one can be none at all — the
    // test then passes without ever exercising the arm it exists for. Waiting
    // for the contended claim to have run several times says what the assertion
    // means. The count is server-side, so no paused clock can stand in for it,
    // and without `pg_stat_statements` there is nothing to count: that build
    // falls back to a fixed window, where the assertion is still sound and just
    // proves less.
    match Stats::new(&db.database).await {
        Some(mut stats) => {
            let attempts = stats.since_now(CRON_SCHEDULE_CLAIM).await;
            stats.wait_for_calls(&attempts, 3, "the scheduler never reached the locked schedule").await;
        }
        None => tokio::time::sleep(Duration::from_secs(4)).await,
    }
    assert_eq!(
        cron_job_count(&pool, db.queue.name(), key).await,
        published,
        "a scheduler that skipped the locked row must not publish"
    );
    assert_eq!(
        cron_next_run_at(&pool, db.queue.name(), key).await,
        held,
        "and it must leave the cursor for the holder, not advance past it"
    );
    wait_until(
        Duration::from_secs(5),
        Duration::from_millis(10),
        "persistent cron-row contention did not degrade scheduler health",
        || async {
            health.snapshot().failures.iter().any(|failure| {
                failure.component == WorkerComponent::Scheduler && failure.message.contains("remained locked")
            })
        },
    )
    .await;

    holder.rollback().await.unwrap();
    wait_until(
        Duration::from_secs(10),
        Duration::from_millis(10),
        "scheduling did not resume once the schedule row was released",
        || async { cron_job_count(&pool, db.queue.name(), key).await > published },
    )
    .await;
    wait_until(
        Duration::from_secs(5),
        Duration::from_millis(10),
        "scheduler health did not recover after the cron row was released",
        || async { !health.snapshot().failures.iter().any(|failure| failure.component == WorkerComponent::Scheduler) },
    )
    .await;

    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(10), run).await.unwrap().unwrap().unwrap();
}

/// `schedule_cron` refuses to invent a schedule row: reconciliation owns the
/// durable definition, and publishing against a row that is not there would
/// mean publishing against no definition at all. The refusal degrades scheduler
/// health so an operator sees it, and that whole arm was untested.
#[sqlx::test(migrations = "./migrations")]
async fn test_cron_scheduling_degrades_health_when_its_schedule_row_disappears(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let key = "repro-unreconciled";
    let worker = cron_worker(&db.queue, "0 3 * * *", key);
    let health = worker.health();
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));

    let scheduler_failure = |snapshot: &pgqueue::WorkerHealthSnapshot| {
        snapshot.failures.iter().find(|failure| failure.component == WorkerComponent::Scheduler).cloned()
    };
    wait_until(Duration::from_secs(15), Duration::from_millis(10), "the cron was never reconciled", || async {
        cron_next_run_at(&pool, db.queue.name(), key).await.is_some()
    })
    .await;
    // Only the scheduler: an unrelated component degrading under load says
    // nothing about the arm under test.
    assert!(scheduler_failure(&health.snapshot()).is_none());

    // The scheduling loop repairs a lost row on its next pass
    // (`test_cron_reschedules_after_its_durable_row_is_removed`), so a single
    // delete races that repair: park the pass and the delete has to land first,
    // delete first and the repair can undo it before the refusal is observed.
    // Either ordering leaves a window, and under a saturated machine the repair
    // won often enough to fail the suite roughly once in twenty runs.
    //
    // Re-deleting on every poll removes the race instead of narrowing it: the
    // scheduler cannot repair its way past a row that keeps disappearing, so
    // some pass is guaranteed to find it missing. The whole snapshot is
    // returned so the aggregate status is read from the *same* observation as
    // the failure, rather than from a fresh one a repair may already have
    // cleared.
    let (failure, snapshot) = wait_for_some(
        Duration::from_secs(15),
        Duration::from_millis(10),
        "a missing schedule row was never reported",
        || async {
            sqlx::query("DELETE FROM pgqueue.cron_schedules WHERE queue = $1 AND dedupe_key = $2")
                .bind(db.queue.name())
                .bind(key)
                .execute(&pool)
                .await
                .unwrap();
            let snapshot = health.snapshot();
            scheduler_failure(&snapshot).map(|failure| (failure, snapshot))
        },
    )
    .await;
    assert!(
        failure.message.contains("was not reconciled"),
        "the operator must be told the schedule row is missing: {}",
        failure.message
    );
    assert_eq!(snapshot.status, WorkerHealthStatus::Degraded);

    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(10), run).await.unwrap().unwrap().unwrap();
}

/// And it repairs itself. `reconcile_crons` runs once, at startup, so the only
/// thing that can rewrite a schedule row a running worker lost is the retry
/// queue the scheduling loop drains first — the error names its own remedy, and
/// routing it anywhere else left the cron silently dead, and the worker
/// degraded, for the lifetime of the process.
#[sqlx::test(migrations = "./migrations")]
async fn test_cron_reschedules_after_its_durable_row_is_removed(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let key = "repro-row-removed";
    let worker = cron_worker(&db.queue, "0 * * * *", key);
    let health = worker.health();
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));

    wait_until(Duration::from_secs(15), Duration::from_millis(10), "the cron was never reconciled", || async {
        cron_next_run_at(&pool, db.queue.name(), key).await.is_some()
    })
    .await;
    set_cron_due(&pool, db.queue.name(), key, None).await;
    wait_until(Duration::from_secs(15), Duration::from_millis(10), "the due cron never published", || async {
        cron_job_count(&pool, db.queue.name(), key).await >= 1
    })
    .await;

    // Stale-definition cleanup, a `TRUNCATE` during an incident, or a restore
    // predating the cron. The delete waits behind any publication already in
    // flight, so the count read after it is a baseline nothing is racing.
    sqlx::query("DELETE FROM pgqueue.cron_schedules WHERE queue = $1 AND dedupe_key = $2")
        .bind(db.queue.name())
        .bind(key)
        .execute(&pool)
        .await
        .unwrap();
    let published = cron_job_count(&pool, db.queue.name(), key).await;

    wait_until(
        Duration::from_secs(15),
        Duration::from_millis(10),
        "the lost schedule row was never reconciled again",
        || async { cron_next_run_at(&pool, db.queue.name(), key).await.is_some() },
    )
    .await;
    // Re-armed on every poll, not once before the wait. A schedule's occurrences
    // never overlap: while the occurrence published above is still live it holds
    // the schedule's dedupe key, so a publication attempt is *correctly* skipped
    // as held — and `SkippedHeld` still advances the cursor to the successor, an
    // hour out, which no later tick brings back. Arming once therefore raced the
    // first occurrence reaching a terminal status, and lost often enough to fail
    // the suite. Re-arming cannot lose: the tick after the holder finishes finds
    // the cron due again.
    wait_until(
        Duration::from_secs(15),
        Duration::from_millis(10),
        "the cron never published again after losing its schedule row",
        || async {
            set_cron_due(&pool, db.queue.name(), key, None).await;
            cron_job_count(&pool, db.queue.name(), key).await > published
        },
    )
    .await;
    assert_eq!(
        health.snapshot().status,
        WorkerHealthStatus::Ready,
        "a cron that repaired itself must stop degrading the worker"
    );

    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(10), run).await.expect("worker did not stop").unwrap().unwrap();
}

/// The enqueue advisory lock only binds writers that take it, and the holder
/// check and the job insert are separate statements in one READ COMMITTED
/// transaction. A `BEFORE INSERT` trigger commits its row inside the
/// scheduler's own insert, which is exactly that interleaving with no timing to
/// arrange: it is the ops script, backfill or application `INSERT` this library
/// cannot stop from writing `pgqueue.jobs` directly. The usurper is scheduled
/// a day out so it holds the dedupe key without the worker claiming it: the
/// worker has no `usurper` handler, and that bounce degrades
/// `WorkerComponent::Dispatch`, racing any assertion on aggregate health.
async fn install_dedupe_usurper(pool: &PgPool) {
    // noinspection SqlResolve
    sqlx::raw_sql(
        "CREATE FUNCTION pgqueue.repro_dedupe_usurper() RETURNS trigger
         LANGUAGE plpgsql AS $$
         BEGIN
             INSERT INTO pgqueue.jobs (queue, name, dedupe_key, status, scheduled_at)
             VALUES (NEW.queue, 'usurper', NEW.dedupe_key, 'queued', now() + interval '1 day')
             ON CONFLICT DO NOTHING;
             RETURN NEW;
         END $$;
         CREATE TRIGGER repro_dedupe_usurper
         BEFORE INSERT ON pgqueue.jobs
         FOR EACH ROW WHEN (NEW.kind = 'cron')
         EXECUTE FUNCTION pgqueue.repro_dedupe_usurper();",
    )
    .execute(pool)
    .await
    .expect("install the dedupe usurper trigger");
}

fn ticker_worker(db: &TestDb) -> Worker {
    Worker::builder(db.queue.clone())
        .register_cron(repro_ticker)
        .timers(WorkerTimers { schedule: Duration::from_millis(50), ..test_timers() })
        .build()
        .expect("build cron worker")
}

/// Scheduling runs in the worker's schedule loop, so treating this conflict as
/// impossible cost the entire worker: the panic surfaced through the background
/// join as `Error::Task` instead of degrading `WorkerComponent::Scheduler`.
#[sqlx::test(migrations = "./migrations")]
async fn test_cron_scheduling_reports_a_dedupe_holder_that_appeared_after_its_check(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    install_dedupe_usurper(&pool).await;

    let shutdown = CancellationToken::new();
    let worker = ticker_worker(&db);
    let health = worker.health();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));

    wait_until(Duration::from_secs(10), Duration::from_millis(20), "the cron was never reconciled", || async {
        cron_next_run_at(&pool, db.queue.name(), "cron:repro_ticker").await.is_some()
    })
    .await;
    set_cron_due(&pool, db.queue.name(), "cron:repro_ticker", None).await;

    wait_until(
        Duration::from_secs(10),
        Duration::from_millis(20),
        "the scheduler never committed an occurrence whose dedupe key was stolen",
        || async {
            assert!(!run.is_finished(), "the worker stopped instead of skipping the stolen occurrence");
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM pgqueue.jobs WHERE queue = $1 AND name = 'usurper'")
                .bind(db.queue.name())
                .fetch_one(&pool)
                .await
                .unwrap()
                > 0
        },
    )
    .await;

    // Skipping a held key is an ordinary outcome, not a failure, and the cursor
    // moved past the occurrence rather than retrying it forever.
    assert_eq!(health.snapshot().status, WorkerHealthStatus::Ready);
    let due: bool = sqlx::query_scalar("SELECT next_run_at > now() FROM pgqueue.cron_schedules WHERE queue = $1")
        .bind(db.queue.name())
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(due, "the cron cursor must advance past a skipped occurrence");

    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(10), run).await.expect("worker did not stop").unwrap().unwrap();
}

/// And when the row that blocked the insert is gone again before it can be
/// named, the scheduler degrades with a diagnosis instead of panicking or
/// reporting a stale-misfire skip that would repeat every tick.
#[sqlx::test(migrations = "./migrations")]
async fn test_cron_scheduling_degrades_when_the_stolen_dedupe_key_is_released_again(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    install_dedupe_usurper(&pool).await;
    // The scheduler's insert is the one that ends with an empty transition
    // table — `ON CONFLICT DO NOTHING` swallowed its only row — so this retires
    // the usurper exactly then: after the conflict, before the holder re-read.
    let release_trigger = "CREATE FUNCTION pgqueue.repro_dedupe_release() RETURNS trigger
         LANGUAGE plpgsql AS $$
         BEGIN
             IF NOT EXISTS (SELECT 1 FROM inserted) THEN
                 UPDATE pgqueue.jobs SET status = 'complete', completed_at = now()
                 WHERE name = 'usurper' AND status = 'queued';
             END IF;
             RETURN NULL;
         END $$;
         CREATE TRIGGER repro_dedupe_release
         AFTER INSERT ON pgqueue.jobs
         REFERENCING NEW TABLE AS inserted
         FOR EACH STATEMENT EXECUTE FUNCTION pgqueue.repro_dedupe_release();"
        .to_string();
    sqlx::raw_sql(sqlx::AssertSqlSafe(release_trigger)).execute(&pool).await.unwrap();

    let shutdown = CancellationToken::new();
    let worker = ticker_worker(&db);
    let health = worker.health();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));

    wait_until(Duration::from_secs(10), Duration::from_millis(20), "the cron was never reconciled", || async {
        cron_next_run_at(&pool, db.queue.name(), "cron:repro_ticker").await.is_some()
    })
    .await;
    set_cron_due(&pool, db.queue.name(), "cron:repro_ticker", None).await;

    let failure = wait_for_some(
        Duration::from_secs(10),
        Duration::from_millis(20),
        "the scheduler never reported the lost dedupe key",
        || async {
            assert!(!run.is_finished(), "the worker stopped instead of degrading its scheduler");
            health.snapshot().failures.into_iter().find(|failure| failure.component == WorkerComponent::Scheduler)
        },
    )
    .await;
    assert!(failure.message.contains("lost its dedupe key"), "{}", failure.message);

    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(10), run).await.expect("worker did not stop").unwrap().unwrap();
}

// ---------------------------------------------------------------------------
// Worker configuration that PostgreSQL will refuse must be refused up front
// ---------------------------------------------------------------------------

/// The `SELECT` that `schedule_cron` opens its transaction with. Identified by
/// `now() AS now`, which only that statement selects — `reconcile_cron` reads
/// the same table with an otherwise near-identical column list.
const CRON_SCHEDULE_READ: &str = "%next_run_at, now() AS now%cron_schedules%";
/// The pooled pre-filter that replaced it on the not-due path.
const CRON_DUE_FILTER: &str = "%LEFT JOIN pgqueue.cron_schedules%";

/// `schedule_cron` opens a transaction, reads the schedule row and — for a cron
/// that is not due, which is nearly every cron on nearly every tick — rolls it
/// back again. Calling it unconditionally for every registered cron cost
/// `BEGIN`/`SELECT`/`ROLLBACK` per cron, per worker, per tick purely to learn
/// there was nothing to do: at the one-second default that is O(crons x
/// workers) transactions a second against an idle registry.
#[sqlx::test(migrations = "./migrations")]
async fn test_cron_scheduling_reads_no_schedule_row_while_nothing_is_due(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let Some(mut stats) = Stats::new(&db.database).await else {
        Stats::skipped("test_cron_scheduling_reads_no_schedule_row_while_nothing_is_due");
        return;
    };
    let key = "repro-idle-cron";
    // Daily at 03:00, so it is due at most once in the window this test runs.
    let worker = cron_worker(&db.queue, "0 3 * * *", key);
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));
    wait_until(Duration::from_secs(15), Duration::from_millis(10), "the cron was never reconciled", || async {
        cron_next_run_at(&pool, db.queue.name(), key).await.is_some()
    })
    .await;

    let reads = stats.since_now(CRON_SCHEDULE_READ).await;
    let filters = stats.since_now(CRON_DUE_FILTER).await;
    // Several scheduling ticks, waited for rather than slept through: at a
    // fixed 500 ms this guard measured how promptly a starved runtime delivers
    // a 40 ms timer, and failed on a loaded machine before the assertion it
    // exists to protect had been evaluated at all.
    stats.wait_for_calls(&filters, 3, "the scheduling loop did not tick").await;
    assert_eq!(stats.delta(&reads).await, 0, "a cron that is not due must cost no transaction of its own");

    // And the pre-filter is only a pre-filter: a cron that becomes due still
    // publishes its occurrence.
    set_cron_due(&pool, db.queue.name(), key, None).await;
    wait_until(Duration::from_secs(15), Duration::from_millis(10), "a due cron was never published", || async {
        cron_job_count(&pool, db.queue.name(), key).await == 1
    })
    .await;
    assert!(stats.delta(&reads).await > 0, "a due cron must still be scheduled through schedule_cron");

    shutdown.cancel();
    run.await.unwrap().unwrap();
}

// ---------------------------------------------------------------------------
// Invalid input is invalid input, with or without a dedupe key
// ---------------------------------------------------------------------------

/// A cron whose priority falls outside the queue's dequeue window publishes an
/// occurrence that can never be claimed — and that occurrence holds the
/// schedule's dedupe key, so every later one is skipped as held. The schedule
/// stops after exactly one occurrence with nothing on health to say so. The
/// builder holds both halves, so it is a configuration error to report.
#[sqlx::test(migrations = "./migrations")]
async fn test_a_cron_priority_outside_the_queue_window_is_refused_at_build(pool: PgPool) {
    let db = TestDb::with(pool.clone(), |builder| builder.priorities(-5, 5)).await;
    let error = Worker::builder(db.queue.clone())
        .schedule_cron_with_options(
            "* * * * *",
            dynamic_tick::job(()).dedupe_key("out-of-window").priority(100),
            CronOptions { revision: 1, misfire: CronMisfirePolicy::FireOnce },
        )
        .state(Arc::new(AtomicU32::new(0)))
        .build()
        .expect_err("a cron that can never be claimed must be refused");
    assert!(matches!(error, Error::Config(_)), "{error}");
    let message = error.to_string();
    assert!(message.contains("outside this queue's dequeue range"), "{message}");

    // Inside the window still builds.
    Worker::builder(db.queue.clone())
        .schedule_cron_with_options(
            "* * * * *",
            dynamic_tick::job(()).dedupe_key("in-window").priority(-5),
            CronOptions { revision: 1, misfire: CronMisfirePolicy::FireOnce },
        )
        .state(Arc::new(AtomicU32::new(0)))
        .build()
        .expect("a claimable priority is accepted");
}

/// A definition mismatch at this worker's own revision is a deploy *mistake*,
/// not a deploy in progress — but the *scheduling loop* reported it as
/// supersession: `cron superseded by a higher revision ... local.revision=1
/// authority.revision=1`, a line the revisions themselves contradict, and it
/// left `Scheduler` health clean while the cron stopped firing for good.
/// Startup reconciliation has always called the same mismatch an
/// `Error::Config` and degraded the scheduler for it; the loop now agrees.
///
/// The edit lands *after* reconciliation has succeeded, which is what makes this
/// the loop's path rather than reconciliation's: a running worker never
/// re-reconciles a cron it has already accepted.
#[sqlx::test(migrations = "./migrations")]
async fn test_an_out_of_band_definition_edit_degrades_scheduler_health(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let counter = Arc::new(AtomicU32::new(0));
    let dedupe_key = "out-of-band-edit";
    let options = CronOptions { revision: 1, misfire: CronMisfirePolicy::FireOnce };
    let (expression, missed) = hourly_schedule_half_an_hour_ago(&pool).await;

    let worker = dynamic_worker(db.queue.clone(), &expression, dedupe_key, options, counter.clone());
    let health = worker.health();
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));
    wait_for_schedule(&pool, db.queue.name(), dedupe_key).await;
    assert!(
        !health.snapshot().failures.iter().any(|failure| failure.component == WorkerComponent::Scheduler),
        "the scheduler must be healthy before the edit"
    );

    // Someone edits the durable options without raising the revision, and the
    // cron comes due.
    sqlx::query(
        "UPDATE pgqueue.cron_schedules
         SET definition = definition || '{\"conflict\":true}'::jsonb, next_run_at = $3
         WHERE queue = $1 AND dedupe_key = $2",
    )
    .bind(db.queue.name())
    .bind(dedupe_key)
    .bind(missed.to_sqlx())
    .execute(&pool)
    .await
    .unwrap();

    wait_until(Duration::from_secs(10), Duration::from_millis(20), "the conflict never reached health", || {
        let health = health.clone();
        async move { health.snapshot().failures.iter().any(|failure| failure.component == WorkerComponent::Scheduler) }
    })
    .await;
    stop_worker(shutdown, run).await;

    assert_eq!(counter.load(Ordering::SeqCst), 0, "a conflicting definition must not publish");
    let failure = health
        .snapshot()
        .failures
        .into_iter()
        .find(|failure| failure.component == WorkerComponent::Scheduler)
        .expect("scheduler failure");
    assert!(failure.message.contains("conflicts with revision"), "{}", failure.message);
}

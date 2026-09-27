//! PostgreSQL persistence shared by queues, workers, and the dashboard.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use jiff::{SignedDuration, Timestamp};
use jiff_sqlx::ToSqlx;
use serde_json::Value;
use sqlx::error::BoxDynError;
use sqlx::migrate::Migrate;
use sqlx::pool::PoolConnection;
use sqlx::postgres::{PgConnection, PgPool, PgPoolOptions, PgTypeInfo, PgValueRef};
use sqlx::{ConnectOptions, Connection, Decode, Postgres, Transaction, Type};
use uuid::Uuid;

use crate::Error;
use crate::job::{
    CronMisfirePolicy, JobCronEntry, JobCursor, JobRequest, JobRetention, JobRetentions, JobRetryBackoff, JobRow,
    JobStatus, duration_to_ms, duration_to_ms_checked, truncate_stored_error, validate_duration,
    validate_json_document, validate_nonzero_duration,
};
use crate::queue::{ListenerProbe, QueueCounters, QueueCounts, QueueNotifyListener, QueueStats};
use crate::sweeper::{SWEPT, Sweeper, is_swept_marked, swept_marker};
use crate::worker::{WorkerCursor, WorkerInfo};

/// SQLx decoder for nullable PostgreSQL `timestamptz` values.
///
/// `jiff-sqlx` deliberately provides wrappers instead of implementing SQLx's
/// foreign traits on Jiff's types. This local wrapper lets `FromRow` convert a
/// nullable database value into the public `Option<Timestamp>` shape.
pub(crate) struct OptionalTimestamp(Option<Timestamp>);

impl Type<Postgres> for OptionalTimestamp {
    fn type_info() -> PgTypeInfo {
        <jiff_sqlx::Timestamp as Type<Postgres>>::type_info()
    }

    fn compatible(ty: &PgTypeInfo) -> bool {
        <jiff_sqlx::Timestamp as Type<Postgres>>::compatible(ty)
    }
}

impl<'r> Decode<'r, Postgres> for OptionalTimestamp {
    fn decode(value: PgValueRef<'r>) -> Result<Self, BoxDynError> {
        let value = <Option<jiff_sqlx::Timestamp> as Decode<Postgres>>::decode(value)?;
        Ok(Self(value.map(jiff_sqlx::Timestamp::to_jiff)))
    }
}

impl From<OptionalTimestamp> for Option<Timestamp> {
    fn from(value: OptionalTimestamp) -> Self {
        value.0
    }
}

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!();

/// PostgreSQL `undefined_table`, raised for a missing schema too.
const UNDEFINED_TABLE: &str = "42P01";

#[derive(sqlx::FromRow)]
struct AppliedMigration {
    version: i64,
    checksum: Vec<u8>,
    success: bool,
}

#[derive(sqlx::FromRow)]
struct DatabaseServer {
    version: i32,
    database: String,
    isolation: String,
    max_locks_per_transaction: i64,
    max_connections: i64,
}

enum MigrationStatus {
    Current,
    Pending,
}

async fn find_applied_migrations(connection: &mut PgConnection) -> Result<Vec<AppliedMigration>, sqlx::Error> {
    sqlx::query_as::<_, AppliedMigration>(
        r#"
        SELECT version, checksum, success
        FROM pgqueue.migrations
        ORDER BY version
        "#,
    )
    .fetch_all(connection)
    .await
}

async fn find_migration_status(connection: &mut PgConnection) -> Result<MigrationStatus, Error> {
    let applied = match find_applied_migrations(connection).await {
        Ok(applied) => applied,
        Err(sqlx::Error::Database(error)) if error.code().as_deref() == Some(UNDEFINED_TABLE) => {
            return Ok(MigrationStatus::Pending);
        }
        Err(error) => return Err(error.into()),
    };

    for row in &applied {
        if !row.success {
            return Err(Error::Migration(sqlx::migrate::MigrateError::Dirty(row.version)));
        }
    }

    let expected =
        MIGRATOR.iter().filter(|migration| !migration.migration_type.is_down_migration()).collect::<Vec<_>>();
    for (index, row) in applied.iter().enumerate() {
        let Some(migration) = expected.get(index) else {
            return Err(Error::Migration(sqlx::migrate::MigrateError::VersionMissing(row.version)));
        };
        if migration.version != row.version {
            if !expected.iter().any(|migration| migration.version == row.version) {
                return Err(Error::Migration(sqlx::migrate::MigrateError::VersionMissing(row.version)));
            }
            return Err(Error::Config(format!(
                "pgqueue migration history is not an applied prefix: expected version {}, found {}",
                migration.version, row.version
            )));
        }
        if migration.checksum.as_ref() != row.checksum.as_slice() {
            return Err(Error::Migration(sqlx::migrate::MigrateError::VersionMismatch(row.version)));
        }
    }
    if applied.len() == expected.len() { Ok(MigrationStatus::Current) } else { Ok(MigrationStatus::Pending) }
}

/// Begins every transaction the queue owns, and in the same round trip tells the server to end the session if the
/// transaction then sits idle for half a minute.
///
/// A transaction's locks last as long as the server keeps its session, and the server ends a session only when it
/// learns the client is gone. A client that exits closes its socket; one whose host loses power, or that a partition
/// cuts off, closes nothing, and the server hears of it only from TCP — after two hours and eleven minutes of keepalive
/// on stock Linux, or some fifteen minutes of retransmission if its last reply went unacknowledged. Every transaction
/// here sends its statements back to back, so a client lost between two of them left what it held locked for that long:
/// a claim's rows, `queued` in every snapshot yet skipped by every other claim and invisible to the sweeper, still
/// holding their dedupe keys — a cron occurrence among them skipped its schedule's every occurrence as held; a
/// resolver's exclusive resolution lock, against which its live worker's every claim came back empty while health
/// stayed ready; a scheduler's schedule row; a keyed enqueue's key. Ended at the bound instead, the session rolls back
/// and its locks go with it, which each of these already survives: a claim whose COMMIT then fails is handed to the
/// resolver, which finds its rows `queued` and settles nothing.
///
/// Half a minute is far beyond the round trip a live client takes between statements, and close to the recovery a
/// crashed worker's attempts already get from lease expiry. `idle_in_transaction_session_timeout` rather than
/// `transaction_timeout`, which would also end the lock waits these transactions make on purpose — a keyed enqueue
/// queued behind a caller's transaction that holds its key, say. Only ever lowered, so an operator's stricter setting
/// stays in force; transaction-local, so the session's own setting is back at commit or rollback, and a caller's
/// transactions, which this crate never begins, keep theirs. `BEGIN` and the bound travel as one simple query, so the
/// bound costs no round trip of its own, only the server's planning of one small statement: some 16 microseconds per
/// transaction, measured on PostgreSQL 18.4.
const BEGIN_BOUNDED_TRANSACTION_SQL: &str = "BEGIN; \
    SELECT set_config('idle_in_transaction_session_timeout', '30s', true) \
    FROM (SELECT current_setting('idle_in_transaction_session_timeout')::interval AS configured) AS setting \
    WHERE configured = interval '0' OR configured > interval '30s'";

struct PoolConnectionGuard {
    connection: PoolConnection<Postgres>,
    close_on_drop: bool,
}

impl PoolConnectionGuard {
    fn new(connection: PoolConnection<Postgres>) -> Self {
        Self { connection, close_on_drop: true }
    }

    fn connection(&mut self) -> &mut PgConnection {
        &mut self.connection
    }

    fn disarm(&mut self) {
        self.close_on_drop = false;
    }

    /// Begins one of the queue's own transactions, bounded as [`BEGIN_BOUNDED_TRANSACTION_SQL`] describes.
    async fn begin_transaction(&mut self) -> Result<Transaction<'_, Postgres>, sqlx::Error> {
        // SQLx 0.9 can miss the rollback when BEGIN is cancelled before it records the transaction depth. Keep the
        // connection out of the pool until BEGIN succeeds and SQLx's transaction guard can perform that rollback. The
        // same covers the bound failing after `BEGIN` succeeded, which SQLx reports before it records the depth.
        self.close_on_drop = true;
        let transaction = self.connection.begin_with(BEGIN_BOUNDED_TRANSACTION_SQL).await?;
        self.close_on_drop = false;
        Ok(transaction)
    }
}

impl Drop for PoolConnectionGuard {
    fn drop(&mut self) {
        if self.close_on_drop {
            self.connection.close_on_drop();
        }
    }
}

#[cfg(test)]
mod connection_guard_tests {
    use std::future::{Future, poll_fn};
    use std::pin::pin;
    use std::task::Poll;

    use super::*;
    use crate::Queue;

    async fn connect_single_connection_queue(pool: &PgPool) -> Queue {
        let shared = PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_secs(5))
            .connect_with(pool.connect_options().as_ref().clone())
            .await
            .unwrap();
        Queue::builder("postgres://unused").pool(shared).connect().await.unwrap()
    }

    async fn poll_once_and_cancel(future: impl Future) {
        let mut future = pin!(future);
        poll_fn(|context| {
            assert!(future.as_mut().poll(context).is_pending(), "the held advisory lock must prevent completion");
            Poll::Ready(())
        })
        .await;
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn test_cancelled_begin_preserves_autocommit_on_the_shared_pool(pool: PgPool) {
        let queue = connect_single_connection_queue(&pool).await;
        let mut connection = PoolConnectionGuard::new(queue.pool().acquire().await.unwrap());
        let original_pid =
            sqlx::query_scalar::<_, i32>("SELECT pg_backend_pid()").fetch_one(connection.connection()).await.unwrap();
        let mut gate = pool.begin().await.unwrap();
        sqlx::raw_sql("SELECT pg_advisory_xact_lock(17293401)").execute(&mut *gate).await.unwrap();

        // Park a statement ahead of BEGIN on this connection. SQLx sends BEGIN but cannot finish reading its response
        // until the gate opens, so cancellation always hits transaction startup regardless of machine speed.
        poll_once_and_cancel(sqlx::raw_sql("SELECT pg_advisory_xact_lock(17293401)").execute(connection.connection()))
            .await;
        poll_once_and_cancel(connection.begin_transaction()).await;
        drop(connection);
        gate.rollback().await.unwrap();

        let worker_id = Uuid::now_v7();
        queue.consumer(worker_id).heartbeat(serde_json::json!({}), None, Duration::from_secs(30)).await.unwrap();
        let visible = sqlx::query_scalar::<_, i64>(
            "-- noinspection SqlResolve
             SELECT count(*) FROM pgqueue.workers WHERE id = $1",
        )
        .bind(worker_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(visible, 1, "a successful heartbeat must commit and be visible from another connection");
        let replacement_pid =
            sqlx::query_scalar::<_, i32>("SELECT pg_backend_pid()").fetch_one(queue.pool()).await.unwrap();
        assert_ne!(replacement_pid, original_pid, "cancelled BEGIN must discard its connection");
        queue.pool().close().await;
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn test_completed_begin_preserves_transaction_outcomes_and_connection_reuse(pool: PgPool) {
        let queue = connect_single_connection_queue(&pool).await;
        let original_pid =
            sqlx::query_scalar::<_, i32>("SELECT pg_backend_pid()").fetch_one(queue.pool()).await.unwrap();

        for outcome in ["commit", "rollback", "drop"] {
            let worker_id = Uuid::now_v7();
            let mut connection = PoolConnectionGuard::new(queue.pool().acquire().await.unwrap());
            let mut transaction = connection.begin_transaction().await.unwrap();
            sqlx::query(
                "-- noinspection SqlResolve
                 INSERT INTO pgqueue.workers (id, queue, expires_at) VALUES ($1, 'default', now())",
            )
            .bind(worker_id)
            .execute(&mut *transaction)
            .await
            .unwrap();
            match outcome {
                "commit" => transaction.commit().await.unwrap(),
                "rollback" => transaction.rollback().await.unwrap(),
                _ => drop(transaction),
            }
            drop(connection);

            let reused_pid =
                sqlx::query_scalar::<_, i32>("SELECT pg_backend_pid()").fetch_one(queue.pool()).await.unwrap();
            assert_eq!(reused_pid, original_pid, "a completed BEGIN must allow connection reuse after {outcome}");
            let visible = sqlx::query_scalar::<_, i64>(
                "-- noinspection SqlResolve
                 SELECT count(*) FROM pgqueue.workers WHERE id = $1",
            )
            .bind(worker_id)
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(visible, i64::from(outcome == "commit"), "incorrect transaction outcome after {outcome}");

            queue.consumer(worker_id).heartbeat(serde_json::json!({}), None, Duration::from_secs(30)).await.unwrap();
            let lease = sqlx::query_scalar::<_, bool>(
                "-- noinspection SqlResolve
                 SELECT expires_at > now() FROM pgqueue.workers WHERE id = $1",
            )
            .bind(worker_id)
            .fetch_one(&pool)
            .await
            .unwrap();
            assert!(lease, "the pooled connection must return to autocommit after {outcome}");
        }
        queue.pool().close().await;
    }
}

async fn set_lock_timeout(connection: &mut PgConnection, value: &str) -> Result<(), sqlx::Error> {
    let _ = sqlx::query_scalar::<_, String>("SELECT set_config('lock_timeout', $1, false)")
        .bind(value)
        .fetch_one(connection)
        .await?;
    Ok(())
}

/// PostgreSQL `lock_not_available`: a lock wait outlasted `lock_timeout`.
const LOCK_NOT_AVAILABLE: &str = "55P03";

/// The shortest wait one attempt at applying migrations allows any single lock;
/// [`migration_attempt_lock_timeout`] lengthens it where the session's deadlock
/// check needs longer.
///
/// A migration that alters an existing table — as any future change to
/// `pgqueue.jobs` will — needs `ACCESS EXCLUSIVE` on it, and PostgreSQL queues every
/// later request for a table behind a request for it that is still waiting. So
/// the whole wait is a stall of everything else that touches the table: the
/// fleet's claims, enqueues and finishes alike, `SKIP LOCKED` included, since
/// that skips row locks only. Waited out in one piece, a `pg_dump` or an open
/// caller transaction on the table froze the running fleet for the entire
/// `migration_lock_timeout`, then failed the connect — and every replica or
/// restart that tried again froze it again. Bounded per attempt, a blocked
/// migration holds the table's queue for at most one attempt's wait before it
/// gives way.
const MIGRATION_ATTEMPT_LOCK_TIMEOUT: Duration = Duration::from_secs(1);

/// How long past its session's `deadlock_timeout` a migration attempt waits for
/// a lock, at least.
///
/// The holder an attempt meets most often is not a long transaction but
/// autovacuum: `pgqueue.jobs` churns, so a regular autovacuum of it is routine,
/// and it holds a lock that `ACCESS EXCLUSIVE` conflicts with. PostgreSQL makes
/// such an autovacuum give way by cancelling it, and only from the waiter's own
/// deadlock check, which runs `deadlock_timeout` into the wait — 1 second by
/// default. Attempts bounded at that or less never let the check act: with both
/// due at the same instant the lock timeout wins, and any earlier there is no
/// check at all. So every attempt timed out behind the autovacuum, each one
/// stalling the fleet for its whole wait, until the connect failed after its
/// whole budget — where a single wait used to land in a second. The margin
/// leaves the cancelled autovacuum time to leave a cost-delay sleep, abort and
/// release its lock. An anti-wraparound autovacuum is never cancelled; only its
/// end frees the table.
const MIGRATION_DEADLOCK_CHECK_MARGIN: Duration = Duration::from_millis(500);

/// The lock wait one migration attempt gets with `remaining` of the budget left,
/// on a session whose deadlock check runs `deadlock_timeout` into a wait:
/// [`MIGRATION_ATTEMPT_LOCK_TIMEOUT`], or [`MIGRATION_DEADLOCK_CHECK_MARGIN`]
/// past the check where that is longer — 1.5 seconds at PostgreSQL's default —
/// and never more than the budget has left. The price of following the server's
/// setting is that a server with a long `deadlock_timeout` stalls the table for
/// proportionally longer per attempt behind a holder no check can move, such as
/// a `pg_dump`.
fn migration_attempt_lock_timeout(remaining: Duration, deadlock_timeout: Duration) -> Duration {
    MIGRATION_ATTEMPT_LOCK_TIMEOUT
        .max(deadlock_timeout.saturating_add(MIGRATION_DEADLOCK_CHECK_MARGIN))
        .min(remaining)
        // At least a millisecond: PostgreSQL reads a zero `lock_timeout` as "wait forever".
        .max(Duration::from_millis(1))
}

/// The `deadlock_timeout` in force on `connection`: how far into a lock wait its
/// deadlock check runs.
async fn session_deadlock_timeout(connection: &mut PgConnection) -> Result<Duration, sqlx::Error> {
    let milliseconds = sqlx::query_scalar::<_, i64>(
        "SELECT (extract(epoch FROM current_setting('deadlock_timeout')::interval) * 1000)::bigint",
    )
    .fetch_one(connection)
    .await?;
    Ok(Duration::from_millis(u64::try_from(milliseconds).unwrap_or(0)))
}

#[cfg(test)]
mod migration_attempt_tests {
    use super::*;

    #[test]
    fn test_a_migration_attempt_outlives_the_deadlock_check() {
        let budget = Duration::from_secs(30);
        let attempt = migration_attempt_lock_timeout(budget, Duration::from_secs(1));
        assert_eq!(attempt, Duration::from_millis(1_500));
        assert!(attempt > Duration::from_secs(1), "the deadlock check must run inside every attempt");
        // The check follows the session's setting, in both directions down to the floor.
        assert_eq!(migration_attempt_lock_timeout(budget, Duration::from_millis(2_500)), Duration::from_secs(3));
        assert_eq!(migration_attempt_lock_timeout(budget, Duration::from_millis(100)), Duration::from_secs(1));
        assert_eq!(migration_attempt_lock_timeout(budget, Duration::MAX), budget);
    }

    #[test]
    fn test_a_migration_attempt_never_outlasts_the_budget_or_reaches_zero() {
        let deadlock_timeout = Duration::from_secs(1);
        assert_eq!(
            migration_attempt_lock_timeout(Duration::from_millis(200), deadlock_timeout),
            Duration::from_millis(200)
        );
        assert_eq!(migration_attempt_lock_timeout(Duration::ZERO, deadlock_timeout), Duration::from_millis(1));
        assert_eq!(migration_attempt_lock_timeout(Duration::ZERO, Duration::ZERO), Duration::from_millis(1));
    }
}

/// The pause after the first attempt refused a lock, doubling per refusal up to
/// [`MIGRATION_RETRY_MAX_PAUSE`] and jittered down by up to half: long enough for
/// the work queued behind the attempt to drain before the next one queues again.
const MIGRATION_RETRY_INITIAL_PAUSE: Duration = Duration::from_millis(50);
const MIGRATION_RETRY_MAX_PAUSE: Duration = Duration::from_millis(500);

fn is_lock_timeout(error: &sqlx::migrate::MigrateError) -> bool {
    match error {
        sqlx::migrate::MigrateError::Execute(sqlx::Error::Database(error))
        | sqlx::migrate::MigrateError::ExecuteMigration(sqlx::Error::Database(error), _) => {
            error.code().as_deref() == Some(LOCK_NOT_AVAILABLE)
        }
        _ => false,
    }
}

/// Applies the pending migrations in attempts whose every lock wait is bounded
/// by [`migration_attempt_lock_timeout`], retrying an attempt refused a lock
/// until `deadline`; every other failure ends it at once. Retrying is safe
/// because the migrator applies each migration and its history row in one
/// transaction — a refused attempt leaves nothing behind, and the next one
/// re-reads the history; a test keeps every migration transactional — and the
/// caller holds the migrator's advisory lock across all of them, so a second
/// replica waits on that lock rather than adding its own queued request for the
/// table. Returns the last refusal once no attempt fits before `deadline`.
async fn run_migrations_until(
    connection: &mut PgConnection,
    migrator: &sqlx::migrate::Migrator,
    deadline: tokio::time::Instant,
) -> Result<(), sqlx::migrate::MigrateError> {
    // Read once, on the connection whose waits it times: the check that cancels an autovacuum is this session's own.
    let deadlock_timeout = session_deadlock_timeout(connection).await?;
    let mut pause = MIGRATION_RETRY_INITIAL_PAUSE;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let attempt = migration_attempt_lock_timeout(remaining, deadlock_timeout);
        set_lock_timeout(connection, &format!("{}ms", attempt.as_millis())).await?;
        let refused = match migrator.run_direct(None, &mut *connection, false).await {
            Err(error) if is_lock_timeout(&error) => error,
            result => return result,
        };
        let jittered = pause.mul_f64(0.5 + rand::random::<f64>() / 2.0);
        if deadline.saturating_duration_since(tokio::time::Instant::now()) <= jittered {
            return Err(refused);
        }
        tokio::time::sleep(jittered).await;
        pause = (pause * 2).min(MIGRATION_RETRY_MAX_PAUSE);
    }
}

/// Brings the schema up to date under one budget, `lock_timeout`, for every
/// lock wait involved. The migrator's advisory lock is waited for under the
/// whole budget, because that wait holds up only the connecting process; the
/// DDL is applied by [`run_migrations_until`] in bounded attempts within what
/// remains, because that wait holds up everyone else using the tables it alters.
async fn ensure_migrations(pool: &PgPool, lock_timeout: Duration) -> Result<(), Error> {
    let mut connection = PoolConnectionGuard::new(pool.acquire().await?);
    let previous_lock_timeout = sqlx::query_scalar::<_, String>("SELECT current_setting('lock_timeout')")
        .fetch_one(connection.connection())
        .await?;
    let timeout_ms =
        duration_to_ms_checked(lock_timeout)
            .filter(|milliseconds| *milliseconds <= i64::from(i32::MAX))
            .ok_or_else(|| Error::Config("migration lock timeout must fit PostgreSQL's integer milliseconds".into()))?;
    // Validated above to fit 24.8 days, so the addition cannot overflow.
    let deadline = tokio::time::Instant::now() + lock_timeout;
    set_lock_timeout(connection.connection(), &format!("{timeout_ms}ms")).await?;

    match find_migration_status(connection.connection()).await {
        Ok(MigrationStatus::Current) => {
            set_lock_timeout(connection.connection(), &previous_lock_timeout).await?;
            connection.disarm();
            return Ok(());
        }
        Ok(MigrationStatus::Pending) => {}
        Err(error) => {
            if set_lock_timeout(connection.connection(), &previous_lock_timeout).await.is_ok() {
                connection.disarm();
            }
            return Err(error);
        }
    }

    if let Err(error) = connection.connection().lock().await {
        if set_lock_timeout(connection.connection(), &previous_lock_timeout).await.is_ok() {
            connection.disarm();
        }
        return Err(Error::Migration(error));
    }

    match find_migration_status(connection.connection()).await {
        Ok(MigrationStatus::Current) => {
            connection.connection().unlock().await?;
            set_lock_timeout(connection.connection(), &previous_lock_timeout).await?;
            connection.disarm();
            return Ok(());
        }
        Ok(MigrationStatus::Pending) => {}
        Err(error) => {
            let unlocked = connection.connection().unlock().await.is_ok();
            let restored = set_lock_timeout(connection.connection(), &previous_lock_timeout).await.is_ok();
            if unlocked && restored {
                connection.disarm();
            }
            return Err(error);
        }
    }

    // The status was rechecked while this connection held SQLx's migration
    // lock, so the migrator must not acquire the same session lock again.
    let mut migrator = sqlx::migrate!();
    migrator.set_locking(false);
    let result = run_migrations_until(connection.connection(), &migrator, deadline).await;
    if let Err(error) = result {
        let unlocked = connection.connection().unlock().await.is_ok();
        let restored = set_lock_timeout(connection.connection(), &previous_lock_timeout).await.is_ok();
        if unlocked && restored {
            connection.disarm();
        }
        return Err(Error::Migration(error));
    }
    connection.connection().unlock().await?;
    set_lock_timeout(connection.connection(), &previous_lock_timeout).await?;
    connection.disarm();
    Ok(())
}

// Advisory locks use distinct two-key namespaces. Hash collisions only add
// serialization; table constraints remain the source of truth.
const DEDUPE_ENQUEUE_LOCK_MASK: i32 = 1 << 29;
const CLAIM_RESOLUTION_LOCK_MASK: i32 = 1 << 28;

/// FNV-1a over a byte stream; the one stable hash used for advisory-lock
/// keys, channel names, and dashboard file fingerprints.
pub(crate) fn stable_hash(bytes: impl IntoIterator<Item = u8>) -> u64 {
    bytes.into_iter().fold(0xcbf2_9ce4_8422_2325, |hash, byte| (hash ^ u64::from(byte)).wrapping_mul(0x100_0000_01b3))
}

pub(crate) fn channel_name(queue: &str, suffix: &str) -> String {
    let full = format!("pgqueue_{queue}{suffix}");
    // Hash the queue and suffix NUL-separated (queue names reject control
    // characters) so a queue named "{x}_done" cannot share a channel with
    // queue "{x}"'s done channel.
    let hash = stable_hash(format!("{queue}\0{suffix}").bytes());
    // PostgreSQL identifiers are at most 63 bytes: 46 bytes, `_`, and 16 hex digits.
    let cut = (0..=46).rev().find(|&index| index <= full.len() && full.is_char_boundary(index)).unwrap_or(0);
    format!("{}_{hash:016x}", &full[..cut])
}

pub(crate) fn done_channel(queue: &str) -> String {
    channel_name(queue, "_done")
}

#[cfg(test)]
mod channel_name_tests {
    use super::*;

    #[test]
    fn test_channel_name_differs_when_queue_name_embeds_done_suffix() {
        assert_ne!(channel_name("jobs_done", ""), channel_name("jobs", "_done"));
    }

    #[test]
    fn test_channel_name_stays_within_postgres_identifier_limit() {
        let name = channel_name(&"q".repeat(300), "_done");
        assert!(name.len() <= 63, "channel name too long: {name}");
    }
}

pub(crate) fn dedupe_enqueue_lock_key(database: &str) -> i32 {
    stable_hash(database.bytes()) as i32 ^ DEDUPE_ENQUEUE_LOCK_MASK
}

/// How many dedupe-key locks one transaction may hold, given the server's settings: half the lock table PostgreSQL
/// sizes for `max_connections` sessions taking `max_locks_per_transaction` locks each, and never less than one full
/// batch's worth. See [`TAKE_DEDUPE_LOCKS_SQL`].
pub(crate) fn dedupe_lock_budget(max_locks_per_transaction: i64, max_connections: i64) -> i64 {
    (max_locks_per_transaction.saturating_mul(max_connections) / 2).max(crate::MAX_ENQUEUE_BATCH_JOBS as i64)
}

/// Takes the dedupe locks of one keyed publish — the keys `$3` of queue `$2`, in namespace `$1` — if the transaction
/// then holds no more than `$4` of them, and answers how many it would hold and whether it took them.
///
/// A transaction holds each of its locks in PostgreSQL's lock table until it ends, and that table is one per server,
/// sized for a few dozen locks per connection and shared by every database on it: about 14,900 entries on a stock
/// server. Uncounted, a caller's transaction that published keyed jobs batch after batch took a thousand more entries
/// per batch. Held open near the table's size, it left every other session on the server failing to take any lock
/// with `out of shared memory`, new connections included, and the batch that went past it failed with that same opaque
/// error. So a transaction's keyed publishes are refused past a budget instead (see [`dedupe_lock_budget`]), before
/// they lock anything: checked, counted and locked in one statement, and refused without an error, so the transaction
/// can still commit what it has.
///
/// The count lives in the transaction-local setting `pgqueue.dedupe_locks`, so every transaction starts from none, and
/// a `ROLLBACK TO SAVEPOINT` takes back exactly the share it releases locks for. It counts each publish's distinct
/// keys, which counts a key the transaction already holds again; only where that would refuse a publish is it replaced
/// by the exact count from `pg_locks`, which is dearer to read and needed only near the budget. So the refusal is
/// exact, and a transaction that publishes the same keys over and over is never refused for it.
///
/// The locks are taken one row at a time in lock-id order, which is what keeps two batches from taking the same two
/// locks in opposite orders: the subquery's `ORDER BY` keeps it from being flattened into the projection, so the lock
/// call runs once per row in lock-id order — sorted for it, or already in that order from the `DISTINCT` — and only
/// after the gate, evaluated once before the scan, admitted the publish.
const TAKE_DEDUPE_LOCKS_SQL: &str = r#"
    WITH request AS MATERIALIZED (
        SELECT DISTINCT hashtext(length($2)::text || ':' || $2 || key) AS lock_id
        FROM unnest($3::text[]) AS keys(key)
    ), tally AS MATERIALIZED (
        SELECT CASE
                   WHEN counted + wanted <= $4 THEN counted + wanted
                   -- Two-key advisory locks list their first key as `classid`, their second as `objid`, both unsigned.
                   ELSE (SELECT count(*) FROM (
                             SELECT objid FROM pg_locks
                             WHERE locktype = 'advisory' AND pid = pg_backend_pid() AND objsubid = 2
                               AND classid = ($1::bigint & 4294967295)::oid
                             UNION
                             SELECT (lock_id::bigint & 4294967295)::oid FROM request
                         ) AS held)
               END AS held
        FROM (
            SELECT coalesce(nullif(current_setting('pgqueue.dedupe_locks', true), ''), '0')::bigint AS counted,
                   (SELECT count(*) FROM request) AS wanted
        ) AS so_far
    ), admission AS MATERIALIZED (
        SELECT held, CASE WHEN held <= $4 THEN set_config('pgqueue.dedupe_locks', held::text, true) END AS counted
        FROM tally
    ), locked AS MATERIALIZED (
        SELECT pg_advisory_xact_lock($1, ordered.lock_id)
        FROM (SELECT lock_id FROM request ORDER BY lock_id) AS ordered
        WHERE (SELECT counted IS NOT NULL FROM admission)
    )
    SELECT held, counted IS NOT NULL AS admitted, (SELECT count(*) FROM locked) AS locked
    FROM admission
"#;

/// The advisory namespace that orders unacknowledged-claim resolution behind the claim transaction it is resolving. The
/// claim takes `(this, hashtext(worker_id))` shared and transaction-scoped inside the claiming statement; the resolver
/// takes the same pair exclusively before it reads anything, so it cannot observe — and settle on — the pre-commit
/// state of a COMMIT that is still in flight.
pub(crate) fn claim_resolution_lock_key(database: &str) -> i32 {
    stable_hash(database.bytes()) as i32 ^ CLAIM_RESOLUTION_LOCK_MASK
}

pub(crate) fn sweep_lock_key(database: &str, queue: &str) -> i64 {
    stable_hash(format!("{database}:sweep:{queue}").bytes()) as i64
}

fn validate_queue_name(queue: &str) -> Result<(), Error> {
    if queue.is_empty() {
        return Err(Error::Config("queue name must not be empty".into()));
    }
    if matches!(queue, "." | "..") {
        return Err(Error::Config("queue name must not be a dot segment (`.` or `..`)".into()));
    }
    if queue.len() > 255 {
        return Err(Error::Config("queue name must not be longer than 255 bytes".into()));
    }
    if queue.chars().any(char::is_control) {
        return Err(Error::Config("queue name must not contain control characters".into()));
    }
    Ok(())
}

/// Refuses a finalization value PostgreSQL can never store, or that this crate
/// could never read back.
///
/// A NUL is permanently invalid, not a transient failure: `jsonb` raises
/// `22P05` and `text` raises `22021`, so the attempt stays `running` and the
/// caller — which [`Attempt::finish`](crate::Attempt::finish) and
/// [`Attempt::retry`](crate::Attempt::retry) explicitly invite to "retry after
/// a transient infrastructure error" — spins forever. Every other writer on
/// this side of the wire already refuses one (see `json_contains_nul`); these
/// are the two the public consumer API reaches.
///
/// Excessive nesting is refused for the mirror-image reason: `jsonb` accepts it
/// and `serde_json` cannot decode it, so the row would be written successfully
/// and then poison every read of the queue it lands in (see
/// `json_exceeds_depth`).
///
/// Refused before a connection is taken, so it cannot be mistaken for pool
/// exhaustion either.
fn validate_finalization(result: Option<&Value>, error: Option<&str>) -> Result<(), Error> {
    if let Some(result) = result {
        validate_json_document("job result", result).map_err(Error::Config)?;
    }
    if error.is_some_and(|error| error.contains('\0')) {
        return Err(Error::Config("job error must not contain NUL".into()));
    }
    Ok(())
}

/// Database state scoped to one named queue.
pub(crate) struct Database {
    pool: PgPool,
    name: String,
    dedupe_enqueue_lock_key: i32,
    /// How many dedupe-key locks one transaction may hold; see [`dedupe_lock_budget`]. Atomic only so this crate's own
    /// tests can lower it, rather than fill a shared server's lock table on the way to the real one.
    dedupe_lock_budget: std::sync::atomic::AtomicI64,
    claim_resolution_lock_key: i32,
    sweep_lock_key: i64,
    priorities: (i16, i16),
    sweep_grace: Duration,
    sweep_batch_size: i64,
    notify_channel: String,
    done_channel: String,
    counters: std::sync::Arc<QueueCounters>,
    notify_listener: std::sync::OnceLock<QueueNotifyListener>,
    /// How the notification listener proves an idle session is still there. The default outside this crate's own
    /// tests, which shorten it with `set_listener_probe` before the listener starts.
    listener_probe: std::sync::OnceLock<ListenerProbe>,
    /// The enqueue wakeup throttle: at most one notification per window from
    /// this handle, or every one when `None`.
    notify_throttle: Option<Duration>,
    /// When the throttle last let a notification through, in milliseconds on
    /// `notify_clock`; [`NEVER_NOTIFIED`] until the first.
    last_notify_ms: AtomicU64,
    notify_clock: Instant,
}

/// The `last_notify_ms` value before any notification has been sent. A zero
/// would read as "sent at the clock's origin" and suppress every wakeup for
/// the first window after connecting.
const NEVER_NOTIFIED: u64 = u64::MAX;

pub(crate) struct DatabaseConnectOptions {
    pub(crate) url: String,
    pub(crate) pool: Option<PgPool>,
    pub(crate) name: String,
    pub(crate) max_connections: u32,
    pub(crate) min_connections: u32,
    pub(crate) priorities: (i16, i16),
    pub(crate) sweep_grace: Duration,
    pub(crate) sweep_batch_size: u32,
    pub(crate) migration_lock_timeout: Duration,
    pub(crate) notify_throttle: Option<Duration>,
}

pub(crate) enum DatabaseEnqueueResult {
    Inserted(Uuid),
    Deduplicated { id: Uuid, name: String, retentions: JobRetentions },
}

/// The one live row holding a dedupe key, as both readers of that rule need it:
/// the enqueue path reports it as the collision winner, and the cron scheduler
/// reports it as the holder an occurrence was skipped for.
///
/// One row shape rather than two queries, because "which live row holds this
/// key" is one rule: split, a change to the live-status set had to be made twice,
/// and whichever copy was missed fell off `jobs_dedupe_key_idx` — whose predicate
/// is that same set — onto a sequential scan, silently.
#[derive(sqlx::FromRow)]
pub(crate) struct DatabaseDedupeHolder {
    pub(crate) id: Uuid,
    pub(crate) name: String,
    pub(crate) result_ttl_ms: Option<i64>,
    pub(crate) failed_ttl_ms: Option<i64>,
    #[sqlx(try_from = "jiff_sqlx::Timestamp")]
    pub(crate) scheduled_at: Timestamp,
    pub(crate) kind: String,
}

impl DatabaseDedupeHolder {
    fn retentions(&self) -> JobRetentions {
        JobRetentions {
            result: JobRetention::from_result_ttl_ms(self.result_ttl_ms),
            failed: JobRetention::from_result_ttl_ms(self.failed_ttl_ms),
        }
    }
}

/// A live dedupe holder together with the key it holds, for the batch enqueue,
/// which reads every holder of a batch's keys in one statement.
#[derive(sqlx::FromRow)]
struct DatabaseKeyedDedupeHolder {
    dedupe_key: String,
    #[sqlx(flatten)]
    holder: DatabaseDedupeHolder,
}

pub(crate) enum DatabaseCronAuthority {
    Active,
    Inactive {
        revision: i64,
    },
    /// Another transaction held the schedule row for all of [`CRON_RECONCILE_LOCK_WAIT`], so nothing was written and
    /// nothing is known: the cron stays unreconciled for a later pass to retry.
    Contended,
}

/// How long reconciling one cron waits for its schedule row's lock before giving the row up until a later pass.
///
/// The upsert locks the row it conflicts with even when its `WHERE` then writes nothing, so it waits for whatever holds
/// the row: a peer's publication for a round trip or two, which this outlasts many times over — or an operator's
/// `SELECT ... FOR UPDATE`, or a session left open in a transaction, for as long as they last. Nothing on the server
/// bounded that second wait. A worker reconciles every cron at startup, so a restart while such a row was held kept the
/// new worker `Starting` for a minute per held row; its scheduling loop retried the reconciliation ahead of every
/// cron, so none of them was published while the row stayed held; and each pass the loop abandoned at its deadline
/// left the wait queued on the server, where the ping sqlx runs before pooling a connection again waited it out, so
/// every pass kept one more pooled connection until the worker had none left for anything.
const CRON_RECONCILE_LOCK_WAIT: Duration = Duration::from_secs(2);

pub(crate) enum DatabaseCronScheduleResult {
    NotDue,
    Contended,
    /// A *higher* revision holds the schedule: the normal state of a worker a
    /// newer release has already superseded.
    Inactive {
        revision: i64,
    },
    /// The stored definition differs at this worker's own revision or below.
    /// Distinct from [`DatabaseCronScheduleResult::Inactive`] because it is not
    /// a deploy in progress but a deploy *mistake*, and reporting it as
    /// supersession produced the self-contradicting
    /// `superseded by a higher revision ... local.revision=1 authority.revision=1`
    /// while leaving health clean — where startup reconciliation calls the same
    /// mismatch an `Error::Config` and degrades the scheduler for it.
    Conflicting {
        revision: i64,
    },
    Published {
        id: Uuid,
        occurrence: Timestamp,
    },
    AlreadyPublished {
        occurrence: Timestamp,
    },
    SkippedStale {
        occurrence: Timestamp,
    },
    SkippedHeld {
        occurrence: Timestamp,
        existing: DatabaseDedupeHolder,
    },
}

pub(crate) struct DatabaseAbortingAttempt {
    pub(crate) id: Uuid,
    pub(crate) attempts: i32,
    pub(crate) reason: Option<String>,
    pub(crate) swept: bool,
}

/// One in-flight attempt as its worker knows it, for [`Database::aborting_of`]
/// to compare against the row.
#[derive(Clone, Copy)]
pub(crate) struct DatabaseAbortClaim {
    pub(crate) id: Uuid,
    pub(crate) attempts: i32,
}

pub(crate) struct DatabaseAbortPoll {
    pub(crate) aborting: Vec<DatabaseAbortingAttempt>,
    /// Claims whose row is gone. Reported as the claim, not just the id, so the
    /// caller can name the one attempt that lost its row: the same id can be
    /// in flight under two attempt numbers at once.
    pub(crate) missing: Vec<DatabaseAbortClaim>,
    /// Claims whose row is still there but no longer theirs.
    pub(crate) superseded: Vec<DatabaseAbortClaim>,
}

#[derive(sqlx::FromRow)]
pub(crate) struct DatabaseStuckJob {
    pub(crate) id: Uuid,
    pub(crate) name: String,
    pub(crate) status: JobStatus,
    pub(crate) attempts: i32,
    pub(crate) refunds: i32,
    pub(crate) max_attempts: i32,
    pub(crate) retry_delay_ms: i64,
    pub(crate) backoff: JobRetryBackoff,
    pub(crate) worker_id: Option<Uuid>,
    pub(crate) error: Option<String>,
    pub(crate) result: Option<Value>,
    /// Whether the attempt's owner is past the cooperative abort window, which
    /// this reads as its `pgqueue.workers` lease row being gone. Deliberately
    /// *not* "holds no live lease", and deliberately not the lease's age either
    /// — see the `owner_gone` comment in
    /// [`Sweeper::recover_stuck_jobs`](Sweeper) for why both are weaker
    /// claims than the row being gone.
    pub(crate) owner_gone: bool,
}

impl DatabaseStuckJob {
    pub(crate) fn is_retryable(&self) -> bool {
        crate::job::has_attempts_remaining(self.attempts, self.max_attempts)
    }

    pub(crate) fn next_retry_delay(&self) -> Duration {
        crate::job::retry_delay_for(
            self.retry_delay_ms,
            &self.backoff,
            crate::job::spent_attempts(self.attempts, self.refunds),
        )
    }
}

#[derive(Clone, Copy)]
struct AttemptGuard {
    id: Uuid,
    attempts: i32,
    worker_id: Option<Uuid>,
}

impl From<&JobRow> for AttemptGuard {
    fn from(job: &JobRow) -> Self {
        Self { id: job.id, attempts: job.attempts, worker_id: job.worker_id }
    }
}

pub(crate) struct DatabaseDequeueBatch {
    pub(crate) jobs: Vec<JobRow>,
    pub(crate) intake_open: bool,
    /// A matching job is still ready after this batch. This remains true for
    /// rows skipped because another transaction currently holds their row
    /// lock, so burst workers cannot mistake transient lock contention for a
    /// drained queue. Only an underfilled batch from the worker fetch loop is
    /// probed; every other batch reports `false` without checking.
    pub(crate) work_available: bool,
}

#[derive(Debug, PartialEq, Eq, sqlx::FromRow)]
struct DatabaseDequeueProbe {
    intake_open: bool,
    work_available: bool,
}

/// The collision answer for a dedupe key an existing live job holds.
fn deduplicated(row: DatabaseDedupeHolder) -> DatabaseEnqueueResult {
    DatabaseEnqueueResult::Deduplicated { id: row.id, retentions: row.retentions(), name: row.name }
}

#[derive(sqlx::FromRow)]
struct CronAuthority {
    name: String,
    expression: String,
    /// Whether the stored `definition` equals the one this worker registered.
    ///
    /// Compared server-side, not in Rust, because `jsonb` equality is the only
    /// equality this value has. `jsonb` stores numbers as `numeric`, so a
    /// `serde_json` float in exponent form comes back expanded and re-parses as
    /// `Number::PosInt` where it went in as `Number::Float` — and `serde_json`'s
    /// `PartialEq` calls those unequal. A cron whose payload or meta carried a
    /// float of 1e16 or larger therefore conflicted with the definition this
    /// same call had just written, and was disabled permanently with a
    /// revision-conflict error no revision bump can clear.
    definition_matches: bool,
    revision: i64,
    misfire_policy: String,
    grace_ms: Option<i64>,
}

#[derive(sqlx::FromRow)]
struct ObservedCron {
    name: String,
    expression: String,
    /// Server-side `jsonb` equality, for the reason on [`CronAuthority`].
    definition_matches: bool,
    revision: i64,
    misfire_policy: String,
    grace_ms: Option<i64>,
    #[sqlx(try_from = "jiff_sqlx::Timestamp")]
    next_run_at: Timestamp,
    #[sqlx(try_from = "jiff_sqlx::Timestamp")]
    now: Timestamp,
}

/// A finished job as a result wait sees it: the status that classifies it, and
/// the two columns that carry the answer.
#[derive(sqlx::FromRow)]
pub(crate) struct DatabaseJobOutcome {
    pub(crate) status: JobStatus,
    pub(crate) result: Option<Value>,
    pub(crate) error: Option<String>,
    /// Whether the row is marked for its completion notification yet. A wait that found the row locked could not mark
    /// it (see [`Database::mark_awaited`]), and reads this back to try again.
    pub(crate) awaited: bool,
}

#[derive(sqlx::FromRow)]
struct AbortPollRow {
    id: Uuid,
    status: JobStatus,
    attempts: i32,
    worker_id: Option<Uuid>,
    error: Option<String>,
    result: Option<Value>,
}

#[derive(sqlx::FromRow)]
struct AbortResult {
    status: String,
}

#[derive(sqlx::FromRow)]
struct FinishResult {
    finished: bool,
}

#[derive(sqlx::FromRow)]
struct RequeueResult {
    requeued: bool,
}

/// What a lease write does to `pgqueue.workers.accepting`.
///
/// The row a heartbeat updates and the row it creates need different answers.
/// A worker's own heartbeat must never reopen intake it already closed, so it
/// leaves an existing flag alone — but it still creates a lease whenever one is
/// missing (its first, or a replacement for one the sweeper purged after the
/// worker stalled past its TTL), and that new row has to start in the state the
/// caller is actually in. Defaulting it to `accepting` republished a
/// shutting-down worker as open for business: `accepting` is read by the two
/// claim paths ([`Database::dequeue_inner`] and its underfilled-batch probe),
/// so the recreated lease let a worker that had already closed intake keep
/// claiming new jobs it would then have to abandon.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LeaseIntake {
    /// Take work: create the lease accepting, and reopen one that was closed.
    /// A [`crate::Consumer`] heartbeat is its request for work, so it reopens.
    Reopen,
    /// Take work, but never undo a close: create the lease accepting and leave
    /// an existing flag as it stands.
    Open,
    /// Stopped taking work: create the lease closed and leave a closed one
    /// closed.
    Closed,
}

impl LeaseIntake {
    /// Whether an existing lease's `accepting` flag is forced back on.
    fn reopens(self) -> bool {
        matches!(self, LeaseIntake::Reopen)
    }

    /// The `accepting` value a lease created by this write starts with.
    fn accepts_when_created(self) -> bool {
        !matches!(self, LeaseIntake::Closed)
    }
}

/// Resolves the probe that ran beside a committed claim. A probe failure with
/// jobs in hand is swallowed: the claim is already durable, so the batch goes
/// to processors under conservative availability rather than orphaning the
/// attempt under lease.
fn resolve_post_commit_probe(
    queue: &str,
    worker_id: Uuid,
    jobs_claimed: usize,
    probe: Result<DatabaseDequeueProbe, sqlx::Error>,
) -> Result<DatabaseDequeueProbe, Error> {
    match probe {
        Ok(probe) => Ok(probe),
        Err(error) if jobs_claimed == 0 => Err(error.into()),
        Err(error) => {
            tracing::warn!(
                queue,
                worker.id = %worker_id,
                job.count = jobs_claimed,
                %error,
                "post-commit dequeue probe failed; returning the committed batch"
            );
            Ok(DatabaseDequeueProbe { intake_open: true, work_available: true })
        }
    }
}

#[cfg(test)]
mod dequeue_probe_tests {
    use super::*;

    #[test]
    fn test_resolve_post_commit_probe_preserves_successful_metadata() {
        let expected = DatabaseDequeueProbe { intake_open: false, work_available: true };

        let actual = resolve_post_commit_probe("default", Uuid::nil(), 0, Ok(expected)).unwrap();

        assert_eq!(actual, DatabaseDequeueProbe { intake_open: false, work_available: true });
    }

    #[test]
    fn test_resolve_post_commit_probe_returns_conservative_metadata_when_jobs_were_claimed() {
        let actual = resolve_post_commit_probe("default", Uuid::nil(), 1, Err(sqlx::Error::PoolClosed)).unwrap();

        assert_eq!(actual, DatabaseDequeueProbe { intake_open: true, work_available: true });
    }

    #[test]
    fn test_resolve_post_commit_probe_propagates_error_when_no_jobs_were_claimed() {
        let error = resolve_post_commit_probe("default", Uuid::nil(), 0, Err(sqlx::Error::PoolClosed)).unwrap_err();

        assert!(matches!(error, Error::Db(sqlx::Error::PoolClosed)));
    }
}

/// Which rows [`Database::requeue_guarded`] may reclaim.
#[derive(Clone, Copy)]
struct DatabaseRequeueGuards {
    /// Reclaim the row while it is still `running`.
    allow_running: bool,
    /// Reclaim an `aborting` row bearing the sweeper's markers.
    allow_swept_abort: bool,
    /// Refund the attempt (`max_attempts + 1`) because it never actually ran.
    refund_attempt: bool,
    /// Close the worker's intake alongside the requeue. The shutdown requeue
    /// wants both writes in one statement; the unacknowledged-claim resolver
    /// refunds a live worker's attempt and must leave its intake open.
    close_intake: bool,
}

/// One claim whose dequeue COMMIT was sent but never acknowledged: the server
/// may have committed it, so the row may be `running` under a worker that
/// never learned it owns it.
pub(crate) struct DatabaseUnacknowledgedClaim {
    pub(crate) id: Uuid,
    pub(crate) attempts: i32,
    /// The `started_at` the claim stamped, which is what names *this* claim. `(id, attempts, worker_id)` alone does
    /// not: a claim whose COMMIT never landed leaves the row `queued` at its old count, so the same worker's next claim
    /// of that row reproduces all three, and a resolver matching on them requeued that live attempt from under the
    /// processor running it. Only a claim writes `started_at` — each with its own transaction's `now()` — every requeue
    /// clears it, and recovery's marks keep it. `None` matches any, and a decoded claim always carries one.
    pub(crate) started_at: Option<Timestamp>,
}

#[derive(Clone)]
struct RecoveryContext {
    pool: PgPool,
    counters: std::sync::Arc<QueueCounters>,
    queue: String,
    notify_channel: String,
    done_channel: String,
    claim_lock_key: i32,
}

/// The `error` stored on a row the resolver reclaims, so the dashboard shows
/// why the occurrence moved back to `queued` without ever reporting a result.
const UNACKNOWLEDGED_CLAIM_ERROR: &str = "dequeue commit was not acknowledged";

/// How long [`Database::requeue_unhandled`] hides a bounced row before the
/// per-bounce jitter. Long enough that a worker missing the handler does not
/// spin reclaiming the same job during a rolling deploy; short enough that
/// the job runs promptly once a worker registering it appears.
const UNHANDLED_REQUEUE_DELAY: Duration = Duration::from_secs(10);

/// The upper bound of the uniform jitter added to each bounce's delay. A fixed
/// delay resynchronizes the fleet: every incapable worker is woken by the same
/// `NOTIFY` when a bounced batch comes due, claims it again in the same
/// instant, and bounces it again — a coordinated burst every cycle in which a
/// capable worker may repeatedly lose the race. Jitter spreads the redelivery
/// so some cycle lands on a worker that can run the job.
const UNHANDLED_REQUEUE_JITTER: Duration = Duration::from_secs(5);

/// The three statements that move an attempt to a terminal state, sharing the
/// one rule they must never disagree about: a row whose retention deletes
/// immediately is `DELETE`d rather than `UPDATE`d, and the completion
/// notification fires for exactly the rows that finished, either way.
///
/// Written once here rather than kept in sync by hand across
/// [`Database::finish_with_guards`], [`Database::abort_stuck_abandoned_batch`]
/// and [`abort_unsettled_claim`]: applying a change to that rule to two of the
/// three leaves the third silently inconsistent. Each of the three has a
/// direct test of the delete branch.
///
/// `$candidates` is the CTE chain ending in a `candidate` that yields
/// `(id, ttl_ms)` — already locked, since every caller reads the row it is
/// about to write; `$set` is the `UPDATE`'s SET list; `$tail` is the final
/// `SELECT` over `finished`. The skeleton itself binds no parameter, so each call
/// site keeps its own numbering. The clocks run after the candidate lock is acquired, so waiting for a row lock
/// cannot consume its retention before the terminal state is written.
macro_rules! finish_rows_sql {
    ($candidates:literal, $set:expr, $tail:expr) => {
        concat!(
            "WITH ",
            $candidates,
            r#",
            deleted AS (
                DELETE FROM pgqueue.jobs
                WHERE id IN (SELECT id FROM candidate WHERE ttl_ms = 0)
                RETURNING id, awaited
            ),
            updated AS (
                UPDATE pgqueue.jobs j
                SET "#,
            $set,
            r#",
                    completed_at = clock_timestamp(), touched_at = clock_timestamp(),
                    expires_at = CASE WHEN c.ttl_ms IS NULL THEN NULL
                                      ELSE clock_timestamp() + (c.ttl_ms * interval '1 millisecond') END
                FROM candidate c
                WHERE j.id = c.id AND c.ttl_ms IS DISTINCT FROM 0
                RETURNING j.id, j.awaited
            ),
            finished AS (
                SELECT id, awaited FROM deleted UNION ALL SELECT id, awaited FROM updated
            )
            "#,
            $tail
        )
    };
}

/// A [`finish_rows_sql!`] tail that returns every finished id and emits one
/// completion notification per awaited row, inside the statement's own
/// transaction. The lateral is a one-row scalar select, so every finished id
/// comes back, awaited or not.
macro_rules! notify_each_finished_sql {
    ($channel:literal, $status:literal) => {
        concat!(
            r#"SELECT finished.id
            FROM finished
            CROSS JOIN LATERAL (
                SELECT CASE WHEN finished.awaited THEN pg_notify("#,
            $channel,
            r#", '{"id":"' || finished.id || '","status":""#,
            $status,
            r#""}') END
            ) AS notified"#
        )
    };
}

/// The answer to a keyless insert that returned no row. Nothing in this crate can cause one: the insert's only conflict
/// target is the partial dedupe-key index, whose predicate excludes keyless rows, and a primary-key collision raises
/// rather than skipping. But a `BEFORE INSERT` trigger on `pgqueue.jobs` that returns `NULL` skips the row silently,
/// and treating that as unreachable panicked in the caller's own task.
fn keyless_insert_skipped() -> Error {
    Error::Config("a keyless job insert returned no row; a trigger on pgqueue.jobs may have skipped it".into())
}

/// Everything an enqueue refuses before it takes a connection, so the two entry
/// points cannot drift and a keyed publish pays for the walks exactly once.
pub(crate) fn validate_enqueue(job: &JobRequest, delay: Option<Duration>) -> Result<(), Error> {
    job.validate()?;
    if let Some(delay) = delay {
        validate_duration("job delay", delay)?;
    }
    Ok(())
}

/// Everything a lease write refuses before it takes a connection.
fn validate_worker_info(stats: &Value, metadata: Option<&Value>, ttl: Duration) -> Result<(), Error> {
    validate_duration("worker info TTL", ttl)?;
    // Guarded here rather than in the builder alone, because `Consumer::
    // heartbeat` is a public writer of both columns and a document nested
    // past what `serde_json` can read back poisons its own row. Keep every
    // public writer inside the same depth and size envelope as job JSON.
    //
    // A NUL is refused for the same reason `validate_finalization` refuses
    // one: `jsonb` cannot hold it, so the write raises `22P05` — an
    // `Error::Db` indistinguishable from the transient failures a heartbeat
    // loop is built to retry. Spinning on it renews nothing, so every
    // attempt the caller has claimed is reclaimed by the sweeper once the
    // lease expires.
    for (field, value) in [("worker stats", Some(stats)), ("worker metadata", metadata)] {
        if let Some(value) = value {
            validate_json_document(field, value).map_err(Error::Config)?;
        }
    }
    Ok(())
}

/// A lease upsert returns no row only when its id is another queue's lease, which it must not take over.
fn worker_info_written(worker_id: Uuid, written: Option<Uuid>) -> Result<(), Error> {
    match written {
        Some(_) => Ok(()),
        None => Err(Error::Config(format!("worker id {worker_id} already belongs to a different queue"))),
    }
}

/// Bounds every lock wait in `transaction` by the time left until `deadline`, unless the session's own
/// `lock_timeout` is already the shorter bound. Returns whether this bound is the one in force, so that a refused
/// lock (`55P03`) can be told apart from the session's setting at work and read as the deadline passing. Already past
/// the deadline, sets nothing and returns `None`, which the caller reports as the deadline it is.
///
/// The client abandoning a statement does not end it on the server. A keyed enqueue dropped at its caller's deadline
/// while it waited for its key's lock — which a caller transaction that enqueued the same key holds until it ends —
/// left that lock call queued, and sqlx pings a connection before pooling it again, so the ping waited it out. Every
/// timed-out call kept a pooled connection for the holder's whole lifetime, and a caller retrying on its `WaitTimeout`
/// held the entire pool within a few tries. A worker retrying a write held up by a row lock did the same, once per
/// retry (see [`WriteDeadline`]), and so did the dashboard's Retry and Abort, once per click cut off at its request
/// deadline (see [`Database::abort_within`]). Bounded here, the server ends the wait at the deadline itself.
///
/// Transaction-local, so the session's setting is back at commit or rollback; and only ever set on a transaction of
/// the queue's own, never a caller's. The bound is the time left when it is set, and each lock wait after it gets that
/// much again: whichever statement is still in flight at the deadline keeps its connection at most that much longer,
/// not for as long as a lock holder stays open.
async fn bound_lock_waits(
    transaction: &mut sqlx::PgTransaction<'_>,
    deadline: tokio::time::Instant,
) -> Result<Option<bool>, Error> {
    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
    if remaining.is_zero() {
        return Ok(None);
    }
    // Whole milliseconds rounded up, so the server never gives up before the deadline and the bound is never zero,
    // which PostgreSQL reads as "wait forever"; and no more than the setting can hold.
    let lock_timeout = format!("{}ms", duration_to_ms(remaining).min(i64::from(i32::MAX)));
    let bounded = sqlx::query_scalar::<_, String>(
        "SELECT set_config('lock_timeout', $1, true)
         FROM (SELECT current_setting('lock_timeout')::interval AS configured) setting
         WHERE configured = interval '0' OR configured >= $1::interval",
    )
    .bind(lock_timeout)
    .fetch_optional(&mut **transaction)
    .await?
    .is_some();
    Ok(Some(bounded))
}

/// Whether `error` is PostgreSQL refusing a lock wait that outlasted `lock_timeout`.
pub(crate) fn is_lock_refusal(error: &Error) -> bool {
    matches!(error, Error::Db(sqlx::Error::Database(error)) if error.code().as_deref() == Some(LOCK_NOT_AVAILABLE))
}

/// The deadline a worker holds one of its writes to, and what it reports for a write that reached it.
///
/// Abandoning a write at its deadline frees the worker's loop, not the write's connection: the statement stays queued
/// on the server, and sqlx pings a connection before pooling it again, so the ping waits for whatever the statement
/// waits for. For a write held up by a row lock that is the lock holder's whole lifetime — an operator's
/// `SELECT ... FOR UPDATE` left open, say — and the worker's loops that retry a write (finalization, the lease
/// heartbeat) parked one more pooled connection per retry, until the pool, and with it every other loop of the worker,
/// was gone. Run under a `WriteDeadline` by [`bounded_write`], a write's lock waits end on the server at the same
/// moment (see [`bound_lock_waits`]), and a wait ended there is reported as the deadline the worker reports itself, so
/// which side gave up first makes no difference.
#[derive(Clone, Copy)]
pub(crate) struct WriteDeadline {
    pub(crate) at: tokio::time::Instant,
    pub(crate) exceeded: &'static str,
}

/// What a worker reports for a lease write that reached its deadline, on either side.
pub(crate) const LEASE_WRITE_DEADLINE_EXCEEDED: &str = "worker lease heartbeat exceeded its deadline";

/// The connection a worker's heartbeat loop renews its lease on, held outside its queue's pool; see
/// [`Database::renew_worker_lease`].
#[derive(Default)]
pub(crate) struct LeaseConnection {
    connection: Option<PgConnection>,
    /// Set while a heartbeat writes on `connection`. Still set when the next one starts, the last was dropped with its
    /// future mid-write, which leaves the connection in a state nothing knows, so it is not used again.
    writing: bool,
    /// Whether the heartbeats since one was last opened have gone to the pool because it could not be, so that a run of
    /// them is warned about once rather than once per heartbeat.
    refused: bool,
}

impl LeaseConnection {
    fn opened(&mut self, queue: &str, worker_id: Uuid) {
        if std::mem::take(&mut self.refused) {
            tracing::info!(
                queue, worker.id = %worker_id,
                "worker lease connection opened; heartbeats no longer go through the pool"
            );
        }
    }

    fn discard(&mut self) {
        if let Some(connection) = self.connection.take() {
            close_lease_connection(connection);
        }
    }

    fn refused(&mut self, queue: &str, worker_id: Uuid, error: &Error) {
        if std::mem::replace(&mut self.refused, true) {
            tracing::debug!(queue, worker.id = %worker_id, %error, "worker lease connection still refused");
        } else {
            tracing::warn!(
                queue, worker.id = %worker_id, %error,
                "worker lease connection could not be opened; heartbeats go through the pool until one can"
            );
        }
    }
}

impl Drop for LeaseConnection {
    fn drop(&mut self) {
        self.discard();
    }
}

/// How long a discarded lease connection is given to close gracefully.
const LEASE_CLOSE_TIMEOUT: Duration = Duration::from_secs(1);

/// Closes a lease connection without awaiting it: gracefully, with a runtime to hand, within [`LEASE_CLOSE_TIMEOUT`] —
/// on a connection that stopped answering, even the `Terminate` it sends can stall once the socket's buffer is full —
/// and otherwise by dropping it, which closes its socket just the same.
fn close_lease_connection(connection: PgConnection) {
    if let Ok(runtime) = tokio::runtime::Handle::try_current() {
        runtime.spawn(async move {
            let _ = tokio::time::timeout(LEASE_CLOSE_TIMEOUT, connection.close()).await;
        });
    }
}

/// The lease upsert as the one statement of a [`bounded_write`] on `connection`, which its caller keeps out of the
/// pool until the write comes back.
async fn write_lease_on(
    connection: &mut PgConnection,
    deadline: WriteDeadline,
    query: sqlx::query::QueryScalar<'_, Postgres, Uuid, sqlx::postgres::PgArguments>,
) -> Result<Option<Uuid>, Error> {
    let transaction = connection.begin_with(BEGIN_BOUNDED_TRANSACTION_SQL).await?;
    bounded_write_in(transaction, deadline, async move |connection| query.fetch_optional(connection).await).await
}

/// Runs `write` as the one statement of a transaction on `connection` whose lock waits end on the server at
/// `deadline`. The transaction costs a `BEGIN`, the bound and a `COMMIT` over the statement's own round trip — the
/// price of a bound only the server can enforce, which is why the hot path's writes do not pay it (see
/// [`Database::finish`]).
async fn bounded_write<T>(
    connection: &mut PoolConnectionGuard,
    deadline: WriteDeadline,
    write: impl AsyncFnOnce(&mut PgConnection) -> Result<T, sqlx::Error>,
) -> Result<T, Error> {
    let transaction = connection.begin_transaction().await?;
    bounded_write_in(transaction, deadline, write).await
}

/// [`bounded_write`] in a transaction its caller began with [`BEGIN_BOUNDED_TRANSACTION_SQL`] itself, for a connection
/// it keeps out of the pool by other means.
async fn bounded_write_in<T>(
    mut transaction: Transaction<'_, Postgres>,
    deadline: WriteDeadline,
    write: impl AsyncFnOnce(&mut PgConnection) -> Result<T, sqlx::Error>,
) -> Result<T, Error> {
    let exceeded = || Error::WorkerTask(deadline.exceeded);
    let bounded = bound_lock_waits(&mut transaction, deadline.at).await?.ok_or_else(exceeded)?;
    let written = match write(&mut transaction).await {
        Ok(written) => transaction.commit().await.map(|()| written),
        Err(error) => Err(error),
    };
    // Under the bound, a refused lock is the deadline reached on the server rather than on the client.
    written.map_err(Error::from).map_err(|error| if bounded && is_lock_refusal(&error) { exceeded() } else { error })
}

#[cfg(test)]
mod lock_wait_bound_tests {
    use super::*;
    use crate::Queue;

    fn keyed_job() -> JobRequest {
        let mut job = JobRequest::new("keyed", serde_json::json!({}));
        job.dedupe_key = Some("held".into());
        job
    }

    /// Called without the client-side timer `Queue::enqueue_and_wait` wraps it in, so only the server's bound can end
    /// the lock wait, and its refusal must read as the deadline it is rather than as a database error.
    #[sqlx::test(migrations = "./migrations")]
    async fn test_a_keyed_enqueue_refused_its_lock_at_the_deadline_reports_a_wait_timeout(pool: PgPool) {
        let queue = Queue::builder("postgres://unused").pool(pool.clone()).connect().await.unwrap();
        let mut holder = pool.begin().await.unwrap();
        queue.database().enqueue_raw_delayed_in_result(&mut holder, keyed_job(), None, false).await.unwrap();

        let deadline = tokio::time::Instant::now() + Duration::from_millis(200);
        let refused = tokio::time::timeout(
            Duration::from_secs(10),
            queue.database().enqueue_raw_delayed_result(keyed_job(), None, true, Some(deadline)),
        )
        .await
        .expect("nothing on the server ended the lock wait at the deadline");
        assert!(matches!(refused, Err(Error::WaitTimeout)), "the refusal must read as the deadline");

        // A deadline already behind it sends no lock call at all.
        let late = tokio::time::timeout(
            Duration::from_secs(10),
            queue.database().enqueue_raw_delayed_result(keyed_job(), None, true, Some(deadline)),
        )
        .await
        .expect("a passed deadline waited for the lock");
        assert!(matches!(late, Err(Error::WaitTimeout)), "a passed deadline must not wait for the lock");
        holder.rollback().await.unwrap();
    }

    /// The dashboard's Abort and Retry, called without the request deadline its limiter enforces on the client, so
    /// only the server's bound can end their lock waits: the abort's on a job row another transaction holds, the
    /// retry's on a dedupe key a caller transaction holds by having enqueued the same key. Each is refused at the
    /// deadline and changes nothing, and a deadline already behind the call sends no statement at all.
    #[sqlx::test(migrations = "./migrations")]
    async fn test_operator_actions_refused_their_lock_at_the_deadline_change_nothing(pool: PgPool) {
        let queue = Queue::builder("postgres://unused").pool(pool.clone()).connect().await.unwrap();
        let database = queue.database();
        let Ok(DatabaseEnqueueResult::Inserted(job)) =
            database.enqueue_raw_delayed_result(keyed_job(), None, false, None).await
        else {
            panic!("the job was not enqueued");
        };

        let mut holder = pool.begin().await.unwrap();
        sqlx::query("SELECT id FROM pgqueue.jobs WHERE id = $1 FOR UPDATE")
            .bind(job)
            .execute(&mut *holder)
            .await
            .unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_millis(200);
        let refused =
            tokio::time::timeout(Duration::from_secs(10), database.abort_within(job, "operator", Some(deadline)))
                .await
                .expect("nothing on the server ended the abort's lock wait at the deadline");
        assert!(refused.is_err_and(|error| is_lock_refusal(&error)), "the abort must be refused the row lock");
        let late =
            tokio::time::timeout(Duration::from_secs(10), database.abort_within(job, "operator", Some(deadline)))
                .await
                .expect("an abort past its deadline waited for the lock");
        assert!(matches!(late, Err(Error::WaitTimeout)), "a passed deadline must not wait for the lock");
        holder.rollback().await.unwrap();
        assert_eq!(database.job(job).await.unwrap().unwrap().status, JobStatus::Queued, "a refused abort landed");

        assert!(database.abort(job, "operator").await.unwrap(), "the retried job has to be terminal");
        let mut holder = pool.begin().await.unwrap();
        database.enqueue_raw_delayed_in_result(&mut holder, keyed_job(), None, false).await.unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_millis(200);
        let refused = tokio::time::timeout(
            Duration::from_secs(10),
            database.retry_job_occurrence_within(job, "operator", Some(deadline)),
        )
        .await
        .expect("nothing on the server ended the retry's lock wait at the deadline");
        assert!(refused.is_err_and(|error| is_lock_refusal(&error)), "the retry must be refused the key's lock");
        let late = tokio::time::timeout(
            Duration::from_secs(10),
            database.retry_job_occurrence_within(job, "operator", Some(deadline)),
        )
        .await
        .expect("a retry past its deadline waited for the lock");
        assert!(matches!(late, Err(Error::WaitTimeout)), "a passed deadline must not wait for the lock");
        holder.rollback().await.unwrap();
        assert_eq!(database.job(job).await.unwrap().unwrap().retried_at, None, "a refused retry marked its source");
        let rows = sqlx::query_scalar::<_, i64>("SELECT count(*) FROM pgqueue.jobs").fetch_one(&pool).await.unwrap();
        assert_eq!(rows, 1, "a refused retry created its occurrence");
    }
}

impl Database {
    pub(crate) async fn connect(options: DatabaseConnectOptions) -> Result<Self, Error> {
        validate_queue_name(&options.name)?;
        if options.priorities.0 > options.priorities.1 {
            return Err(Error::Config("queue priority range must have low <= high".into()));
        }
        // Non-zero: a zero grace collapses the whole recovery cushion this knob
        // exists to size. `job_is_stuck` reduces to "the lease is not in the
        // future", leases are purged at `now()`, and the two-phase cooperative
        // abort window closes in the same pass that opened it — so a worker that
        // misses one heartbeat has its still-running attempts reclaimed at once.
        validate_nonzero_duration("sweep grace", options.sweep_grace)?;
        validate_nonzero_duration("migration lock timeout", options.migration_lock_timeout)?;
        // Zero means "no throttle" rather than "a zero-length window", which
        // would be the same thing spelled as a special case of the CAS below.
        let notify_throttle = options.notify_throttle.filter(|window| !window.is_zero());
        if let Some(window) = notify_throttle {
            validate_duration("notify throttle", window)?;
        }
        if !matches!(
            duration_to_ms_checked(options.migration_lock_timeout),
            Some(milliseconds) if milliseconds <= i64::from(i32::MAX)
        ) {
            return Err(Error::Config("migration lock timeout must fit PostgreSQL's integer milliseconds".into()));
        }
        if options.sweep_batch_size == 0 {
            return Err(Error::Config("sweep batch size must be greater than zero".into()));
        }
        if options.pool.is_none() {
            if options.max_connections == 0 {
                return Err(Error::Config("queue max_connections must be greater than zero".into()));
            }
            if options.min_connections > options.max_connections {
                return Err(Error::Config("queue min_connections must not exceed max_connections".into()));
            }
        }

        let pool = match options.pool {
            Some(pool) => pool,
            None => {
                PgPoolOptions::new()
                    .min_connections(options.min_connections)
                    .max_connections(options.max_connections)
                    .connect(&options.url)
                    .await?
            }
        };

        let server = sqlx::query_as::<_, DatabaseServer>(
            "SELECT current_setting('server_version_num')::int AS version, current_database() AS database,
                    current_setting('default_transaction_isolation') AS isolation,
                    current_setting('max_locks_per_transaction')::bigint AS max_locks_per_transaction,
                    current_setting('max_connections')::bigint AS max_connections",
        )
        .fetch_one(&pool)
        .await?;
        if server.version < 180_000 {
            return Err(Error::Config(format!(
                "pgqueue requires PostgreSQL 18+; server_version_num = {}",
                server.version
            )));
        }
        // Checked here for the same reason the version is: it is a property of
        // the server this queue is about to run against, and finding out later
        // costs far more than finding out now.
        //
        // The claim's `FOR UPDATE ... SKIP LOCKED` relies on READ COMMITTED's
        // EvalPlanQual re-check. `SKIP LOCKED` skips a row another transaction
        // currently *holds*; a row one already committed is a different case,
        // and at `repeatable read` or `serializable` PostgreSQL answers it with
        // `40001` instead of re-reading the row. Every claim that loses that
        // race then fails, and it fails as `Error::Db` — indistinguishable from
        // the pool and network errors the fetch loop is built to retry, so a
        // queue under a hardened `default_transaction_isolation` degrades into
        // intermittent, unexplained dequeue failures rather than stopping.
        // `finish_with_guards`, `requeue_guarded`, the dedupe read in
        // `enqueue_raw_delayed_in_result` and the sweeper's `FOR UPDATE` batches
        // rest on the same re-check.
        //
        // A caller-owned transaction may still use any level it likes — see `Queue::enqueue_raw_in`, which documents
        // what that costs; this is about the level the *queue's own* transactions inherit from the server, database or
        // role default. `read uncommitted` is accepted too: PostgreSQL runs it as read committed, re-check included.
        if !["read committed", "read uncommitted"].iter().any(|level| server.isolation.eq_ignore_ascii_case(level)) {
            return Err(Error::Config(format!(
                "pgqueue requires a `read committed` default_transaction_isolation for its own \
                 transactions; this server reports {:?}. Set it back on the database or role \
                 pgqueue connects as (ALTER DATABASE ... SET default_transaction_isolation = \
                 'read committed')",
                server.isolation
            )));
        }

        ensure_migrations(&pool, options.migration_lock_timeout).await?;

        Ok(Self {
            notify_channel: channel_name(&options.name, ""),
            done_channel: done_channel(&options.name),
            dedupe_enqueue_lock_key: dedupe_enqueue_lock_key(&server.database),
            dedupe_lock_budget: std::sync::atomic::AtomicI64::new(dedupe_lock_budget(
                server.max_locks_per_transaction,
                server.max_connections,
            )),
            claim_resolution_lock_key: claim_resolution_lock_key(&server.database),
            sweep_lock_key: sweep_lock_key(&server.database, &options.name),
            pool,
            name: options.name,
            priorities: options.priorities,
            sweep_grace: options.sweep_grace,
            sweep_batch_size: i64::from(options.sweep_batch_size),
            counters: std::sync::Arc::new(QueueCounters::default()),
            notify_listener: std::sync::OnceLock::new(),
            listener_probe: std::sync::OnceLock::new(),
            notify_throttle,
            last_notify_ms: AtomicU64::new(NEVER_NOTIFIED),
            notify_clock: Instant::now(),
        })
    }

    /// Whether an insert may carry its wakeup, given whether the client clock
    /// reads any of its rows as due now.
    ///
    /// Without a throttle the statement decides on the server clock — a row
    /// that comes due while this enqueue waits for a connection, or that is
    /// due on a server whose clock runs ahead, still wakes workers. Under a
    /// throttle the client's reading gates the slot instead, so a plainly
    /// delayed row spends no window; a row due within a clock skew of now may
    /// then wake nobody, which a throttled handle has already accepted as
    /// poll-interval latency.
    fn authorize_wakeup(&self, due_by_client_clock: bool) -> bool {
        match self.notify_throttle {
            None => true,
            Some(window) => due_by_client_clock && self.take_throttle_slot(window),
        }
    }

    /// Takes the throttle window's one slot when it is free, so concurrent
    /// enqueues agree on a single winner per window.
    ///
    /// The slot is taken before the statement runs, so an enqueue that then
    /// fails, or deduplicates, has spent it: the next enqueue inside the
    /// window is carried by the workers' poll interval instead. That is the
    /// documented cost of the throttle, and cheaper than the alternative of
    /// releasing a slot after the fact, which would need every path out of the
    /// statement to remember to.
    fn take_throttle_slot(&self, window: Duration) -> bool {
        // `validate_duration` bounded the window, so the conversion is exact
        // and non-negative.
        let window_ms = duration_to_ms(window).unsigned_abs();
        let now_ms = u64::try_from(self.notify_clock.elapsed().as_millis()).unwrap_or(NEVER_NOTIFIED - 1);
        let mut last = self.last_notify_ms.load(Ordering::Relaxed);
        loop {
            if last != NEVER_NOTIFIED && now_ms.saturating_sub(last) < window_ms {
                return false;
            }
            match self.last_notify_ms.compare_exchange(last, now_ms, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => return true,
                Err(current) => last = current,
            }
        }
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub(crate) fn sweep_lock_key(&self) -> i64 {
        self.sweep_lock_key
    }

    pub(crate) fn dedupe_lock_budget(&self) -> i64 {
        self.dedupe_lock_budget.load(Ordering::Relaxed)
    }

    /// Lowers the dedupe-lock budget for a test, which can then cross it without holding thousands of locks.
    #[cfg(feature = "_test")]
    pub(crate) fn set_dedupe_lock_budget(&self, budget: i64) {
        self.dedupe_lock_budget.store(budget, Ordering::Relaxed);
    }

    /// The inclusive priority window this queue's claims are restricted to.
    pub(crate) fn priorities(&self) -> (i16, i16) {
        self.priorities
    }

    pub(crate) fn sweep_grace(&self) -> Duration {
        self.sweep_grace
    }

    pub(crate) fn sweep_batch_size(&self) -> i64 {
        self.sweep_batch_size
    }

    pub(crate) fn notify_channel(&self) -> &str {
        &self.notify_channel
    }

    pub(crate) fn done_channel(&self) -> &str {
        &self.done_channel
    }

    pub(crate) fn notify_listener(&self) -> &QueueNotifyListener {
        self.notify_listener.get_or_init(|| QueueNotifyListener::start(self))
    }

    pub(crate) fn listener_probe(&self) -> ListenerProbe {
        self.listener_probe.get().copied().unwrap_or_default()
    }

    /// Shortens the listener's liveness probe for a test. Only before the listener starts: answers `false` once it has.
    #[cfg(feature = "_test")]
    pub(crate) fn set_listener_probe(&self, probe: ListenerProbe) -> bool {
        self.notify_listener.get().is_none() && self.listener_probe.set(probe).is_ok()
    }

    pub(crate) fn sweeper(self: &std::sync::Arc<Self>) -> Sweeper {
        Sweeper::new(std::sync::Arc::clone(self))
    }

    pub(crate) fn stats(&self) -> QueueStats {
        self.counters.snapshot()
    }

    fn recovery_context(&self) -> RecoveryContext {
        RecoveryContext {
            pool: self.pool.clone(),
            counters: std::sync::Arc::clone(&self.counters),
            queue: self.name.clone(),
            notify_channel: self.notify_channel.clone(),
            done_channel: self.done_channel.clone(),
            claim_lock_key: self.claim_resolution_lock_key,
        }
    }

    /// Resolves `claims` in the foreground, retrying while a claim transaction of `worker_id` is still in flight
    /// exactly as the background resolver does.
    #[cfg(feature = "_test")]
    pub(crate) async fn requeue_unacknowledged_claims(
        &self,
        worker_id: Uuid,
        claims: &mut Vec<DatabaseUnacknowledgedClaim>,
    ) -> Result<u64, sqlx::Error> {
        let context = self.recovery_context();
        loop {
            if let Some(requeued) = requeue_unacknowledged_claims(&context, worker_id, claims).await? {
                return Ok(requeued);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// The one place a cross-queue job is refused. Every entry point that hands
    /// a [`JobRow`] to `finish_with_guards` or `requeue_guarded` — `finish`,
    /// `retry`, `retry_swept`, `requeue_shutdown`, `requeue_unhandled` — calls
    /// this first, so those two carry no ownership check of their own. Keep it
    /// that way: an `AttemptGuard` is only constructible from a `JobRow`, and
    /// checking here rather than deeper is what makes `retry` refuse a foreign
    /// job instead of silently reporting "not retryable" for it.
    fn ensure_owns(&self, job: &JobRow) -> Result<(), Error> {
        if job.queue == self.name {
            return Ok(());
        }
        Err(Error::Config(format!("job {} belongs to queue {:?}, not {:?}", job.id, job.queue, self.name)))
    }

    /// Inserts one job. `awaited` marks the row for its completion
    /// notification at insert time, for the enqueue that will wait on it, and
    /// `deadline` is that caller's: a keyed enqueue's lock waits end on the
    /// server there (see [`bound_lock_waits`]), reported as
    /// [`Error::WaitTimeout`].
    pub(crate) async fn enqueue_raw_delayed_result(
        &self,
        job: JobRequest,
        delay: Option<Duration>,
        awaited: bool,
        deadline: Option<tokio::time::Instant>,
    ) -> Result<DatabaseEnqueueResult, Error> {
        // Before a connection is taken, on both branches. Behind `pool.begin()`
        // the dedupe path answered identical invalid input with whatever the
        // pool said — `Error::Db(PoolTimedOut)` under load — while the keyless
        // path answered `Error::Config`, so a permanently invalid job looked
        // retryable purely because it carried a dedupe key.
        validate_enqueue(&job, delay)?;
        if job.dedupe_key.is_some() {
            // Use the validated inner form: `JobRequest::validate` walks both
            // JSON documents, so re-entering through the public entry point
            // would repeat that validation on every keyed publish.
            let mut connection = PoolConnectionGuard::new(self.pool.acquire().await?);
            let mut transaction = connection.begin_transaction().await?;
            let bounded = match deadline {
                Some(deadline) => bound_lock_waits(&mut transaction, deadline).await?.ok_or(Error::WaitTimeout)?,
                None => false,
            };
            let result = match self.enqueue_validated_in(&mut transaction, job, delay, awaited).await {
                Ok(result) => transaction.commit().await.map(|()| result).map_err(Error::from),
                Err(error) => Err(error),
            };
            // Under the bound, a refused lock is the deadline reached on the server rather than on the client.
            return result.map_err(|error| if bounded && is_lock_refusal(&error) { Error::WaitTimeout } else { error });
        }

        // Autocommit, not an explicit transaction: one statement needs no
        // `BEGIN`/`COMMIT` to make `Enqueued(id)` the durability claim it reads
        // as. The wire fact that `RETURNING` puts the `DataRow` on the socket
        // before the implicit transaction commits at `Sync` is real, but it is
        // not observable through this API: `fetch_optional` drains the response
        // stream to `ReadyForQuery` before it returns — it has to, because a
        // deferred constraint can turn a row already yielded into an error — and
        // the server sends `ReadyForQuery` only after the commit. So a returned
        // id is a committed row.
        //
        // What no transaction can remove is the other direction: a future
        // dropped after the statement is flushed may commit an insert its caller
        // never saw. This crate drops such futures by design (`with_db_deadline`,
        // the sweeper's pass deadline, the abort and heartbeat loops,
        // `JobHandle::wait`'s timeout, the shutdown path), and `BEGIN`/`COMMIT`
        // only narrows that window to the commit round trip rather than closing
        // it. At-least-once delivery already requires handlers to tolerate the
        // duplicate, so the two extra round trips bought nothing on the hottest
        // path in the crate. The dedupe branch above keeps its transaction for a
        // different reason: its advisory lock is transaction-scoped.
        let id = self.insert_job(&job, delay, awaited, &self.pool).await?;
        id.map(DatabaseEnqueueResult::Inserted).ok_or_else(keyless_insert_skipped)
    }

    pub(crate) async fn enqueue_raw_delayed_in_result(
        &self,
        transaction: &mut sqlx::PgTransaction<'_>,
        job: JobRequest,
        delay: Option<Duration>,
        awaited: bool,
    ) -> Result<DatabaseEnqueueResult, Error> {
        validate_enqueue(&job, delay)?;
        self.enqueue_validated_in(transaction, job, delay, awaited).await
    }

    /// [`Database::enqueue_raw_delayed_in_result`] for a request the caller has
    /// already validated.
    async fn enqueue_validated_in(
        &self,
        transaction: &mut sqlx::PgTransaction<'_>,
        job: JobRequest,
        delay: Option<Duration>,
        awaited: bool,
    ) -> Result<DatabaseEnqueueResult, Error> {
        if let Some(dedupe_key) = job.dedupe_key.as_deref() {
            self.take_dedupe_locks(transaction, &[dedupe_key]).await?;

            // The advisory transaction lock serializes enqueue decisions. A
            // plain read deliberately avoids pinning the existing row against
            // worker finalization for the caller transaction's lifetime.
            if let Some(row) = self.live_dedupe_holder(dedupe_key, transaction).await? {
                return Ok(deduplicated(row));
            }
        }

        let id = self.insert_job(&job, delay, awaited, &mut **transaction).await?;
        match (id, job.dedupe_key.as_deref()) {
            (Some(id), _) => Ok(DatabaseEnqueueResult::Inserted(id)),
            // The insert's only conflict target is the partial dedupe-key index,
            // and the guarded read above found no such row — but they are two
            // statements, and the advisory lock they run under binds only
            // writers that take it. Anything writing `pgqueue.jobs` directly
            // (application SQL, a backfill, an ops script) can commit a
            // conflicting row in between and leave `DO NOTHING` nothing to
            // return. That is an ordinary dedupe collision as far as the caller
            // is concerned, so re-read the holder and report it as one, exactly
            // as `schedule_cron` does.
            (None, Some(dedupe_key)) => {
                match self.live_dedupe_holder(dedupe_key, transaction).await? {
                    Some(row) => Ok(deduplicated(row)),
                    // The row that blocked the insert left the live statuses
                    // again before it could be named. Nothing here can name a
                    // job to deduplicate against, so the caller retries —
                    // which is why this is `DedupeRace`, not `Config`: the
                    // request itself is valid.
                    None => Err(Error::DedupeRace(format!(
                        "dedupe key {dedupe_key:?} was taken by a writer that did not take the \
                         enqueue lock, and released again before it could be reported; retry the \
                         enqueue"
                    ))),
                }
            }
            (None, None) => Err(keyless_insert_skipped()),
        }
    }

    /// [`Database::enqueue_raw_delayed_result`] for a batch, in one statement.
    /// The caller validates each job and the batch limits while collecting. Results come back in input order.
    ///
    /// A keyless batch runs as one autocommit statement, for the durability
    /// reasoning the single keyless enqueue gives; a batch carrying dedupe keys
    /// runs in a transaction, which its advisory locks need.
    pub(crate) async fn enqueue_batch_validated_result(
        &self,
        batch: Vec<(JobRequest, Option<Duration>)>,
    ) -> Result<Vec<DatabaseEnqueueResult>, Error> {
        if batch.is_empty() {
            return Ok(Vec::new());
        }
        if batch.iter().any(|(job, _)| job.dedupe_key.is_some()) {
            let mut connection = PoolConnectionGuard::new(self.pool.acquire().await?);
            let mut transaction = connection.begin_transaction().await?;
            let results = self.enqueue_batch_validated_in(&mut transaction, batch).await?;
            transaction.commit().await?;
            return Ok(results);
        }
        let ids = batch.iter().map(|_| Uuid::now_v7()).collect::<Vec<_>>();
        let rows = batch.iter().zip(&ids).map(|((job, delay), id)| (*id, job, *delay)).collect::<Vec<_>>();
        let inserted = self.insert_jobs(&rows, &self.pool).await?;
        // A keyless row has no conflict target, and a primary-key collision raises rather than skipping, so only a
        // trigger that skips rows can leave one out; see `keyless_insert_skipped`.
        if inserted.len() != ids.len() {
            return Err(Error::Config(format!(
                "batch insert returned {} of {} keyless rows",
                inserted.len(),
                ids.len()
            )));
        }
        Ok(ids.into_iter().map(DatabaseEnqueueResult::Inserted).collect())
    }

    /// The transactional batch path, with the same validation contract as `enqueue_batch_validated_result`.
    pub(crate) async fn enqueue_batch_validated_in_result(
        &self,
        transaction: &mut sqlx::PgTransaction<'_>,
        batch: Vec<(JobRequest, Option<Duration>)>,
    ) -> Result<Vec<DatabaseEnqueueResult>, Error> {
        if batch.is_empty() {
            return Ok(Vec::new());
        }
        self.enqueue_batch_validated_in(transaction, batch).await
    }

    /// Takes the dedupe locks of `keys` for `transaction`, which holds them until it ends, unless they would carry it
    /// past its budget of them; refused, it takes none and the transaction is untouched. See
    /// [`TAKE_DEDUPE_LOCKS_SQL`].
    async fn take_dedupe_locks(&self, transaction: &mut sqlx::PgTransaction<'_>, keys: &[&str]) -> Result<(), Error> {
        let budget = self.dedupe_lock_budget();
        let (held, admitted, _locked) = sqlx::query_as::<_, (i64, bool, i64)>(TAKE_DEDUPE_LOCKS_SQL)
            .bind(self.dedupe_enqueue_lock_key)
            .bind(&self.name)
            .bind(keys)
            .bind(budget)
            .fetch_one(&mut **transaction)
            .await?;
        if admitted {
            return Ok(());
        }
        Err(Error::Config(format!(
            "publishing these dedupe keys would leave this transaction holding {held} dedupe-key locks, past its \
             budget of {budget}: PostgreSQL keeps each one in its server-wide lock table until the transaction ends. \
             Commit keyed publishes in chunks, or raise max_locks_per_transaction"
        )))
    }

    /// The batch insert under its dedupe locks. Every distinct key is locked
    /// first, and the live holders are read under those locks: a job whose
    /// key is held — by a live row, or by an earlier job of this batch — is
    /// reported as deduplicated against that holder and left out of the
    /// insert. A holder of a different job name refuses the whole batch before
    /// anything is written, where the single enqueue could only refuse it
    /// after the fact.
    ///
    /// The locks are the ones a single keyed enqueue takes, so a batch and a
    /// single enqueue of the same key serialize their decisions rather than
    /// racing to `ON CONFLICT`, and they are taken in the order of their lock
    /// ids rather than of the keys: `hashtext` is 32 bits wide, so two
    /// distinct keys can share a lock, and two batches sorting such keys by
    /// text could take the same two locks in opposite orders.
    async fn enqueue_batch_validated_in(
        &self,
        transaction: &mut sqlx::PgTransaction<'_>,
        batch: Vec<(JobRequest, Option<Duration>)>,
    ) -> Result<Vec<DatabaseEnqueueResult>, Error> {
        let mut keys = batch.iter().filter_map(|(job, _)| job.dedupe_key.as_deref()).collect::<Vec<_>>();
        keys.sort_unstable();
        keys.dedup();
        // Who holds each key: a live row read under the locks, or the first job
        // of this batch to claim it.
        let mut holders: HashMap<String, BatchKeyHolder> = HashMap::new();
        if !keys.is_empty() {
            self.take_dedupe_locks(transaction, &keys).await?;
            for keyed in self.live_dedupe_holders(&keys, transaction).await? {
                holders.insert(keyed.dedupe_key, BatchKeyHolder::from(keyed.holder));
            }
        }

        let ids = batch.iter().map(|_| Uuid::now_v7()).collect::<Vec<_>>();
        let mut results = Vec::with_capacity(batch.len());
        let mut rows = Vec::with_capacity(batch.len());
        for ((job, delay), id) in batch.iter().zip(&ids) {
            let Some(key) = job.dedupe_key.as_deref() else {
                rows.push((*id, job, *delay));
                results.push(DatabaseEnqueueResult::Inserted(*id));
                continue;
            };
            match holders.get(key) {
                Some(holder) if holder.name != job.name => {
                    return Err(Error::Config(format!(
                        "dedupe key {key:?} belongs to job {:?}, not {:?}",
                        holder.name, job.name
                    )));
                }
                Some(holder) => results.push(holder.deduplicated()),
                None => {
                    let holder =
                        BatchKeyHolder { id: *id, name: job.name.clone(), retentions: job.config.retentions() };
                    holders.insert(key.to_string(), holder);
                    rows.push((*id, job, *delay));
                    results.push(DatabaseEnqueueResult::Inserted(*id));
                }
            }
        }

        let inserted = self.insert_jobs(&rows, &mut **transaction).await?.into_iter().collect::<HashSet<_>>();
        if inserted.len() == rows.len() {
            return Ok(results);
        }
        // A keyed row `DO NOTHING` swallowed: a writer that did not take the
        // enqueue lock committed its key in between, exactly as the single
        // enqueue's second read handles. Re-read the holder and report the
        // collision as the ordinary dedupe it is — to *every* job of the batch
        // that carries the key, not only the one whose row was dropped: the
        // later ones were reported as deduplicated against that row's id, and
        // a handle to a row that never landed would fail every wait and fetch
        // as a missing job.
        let mut replacements: HashMap<String, BatchKeyHolder> = HashMap::new();
        for (id, job, _) in &rows {
            if inserted.contains(id) {
                continue;
            }
            let Some(dedupe_key) = job.dedupe_key.as_deref() else {
                return Err(Error::Config(format!("batch insert dropped keyless job {id}")));
            };
            let Some(holder) = self.live_dedupe_holder(dedupe_key, transaction).await? else {
                return Err(Error::DedupeRace(format!(
                    "dedupe key {dedupe_key:?} was taken by a writer that did not take the \
                     enqueue lock, and released again before it could be reported; retry the \
                     enqueue"
                )));
            };
            // The winner is a foreign writer's row, so its name is nobody's
            // guarantee. Refused here, while the transaction can still roll
            // the batch's other rows back, rather than by the typed layer
            // after `enqueue_batch_validated_result` has committed them.
            if holder.name != job.name {
                return Err(Error::Config(format!(
                    "dedupe key {dedupe_key:?} belongs to job {:?}, not {:?}",
                    holder.name, job.name
                )));
            }
            replacements.insert(dedupe_key.to_string(), BatchKeyHolder::from(holder));
        }
        for (result, (job, _)) in results.iter_mut().zip(&batch) {
            if let Some(holder) = job.dedupe_key.as_deref().and_then(|key| replacements.get(key)) {
                *result = holder.deduplicated();
            }
        }
        Ok(results)
    }

    /// Marks a job as awaited, so its finish emits the completion notification. A no-op for a row already marked or
    /// already gone; a wait on a missing job learns that from its first poll, not from here.
    ///
    /// Also a no-op for a row another transaction holds locked: the write never waits for the row's lock, and a wait
    /// that reads the row back unmarked tries again on a later poll (see `JobHandle::wait_inner`). Waiting for it, the
    /// write queued behind whoever held the row — an operator's `SELECT ... FOR UPDATE` left open, a claim whose COMMIT
    /// was lost — for as long as they did. The waiting caller's deadline ended only the client's side of that, and sqlx
    /// pings a connection before pooling it again, so the ping waited the write out: every timed-out wait kept a
    /// pooled connection until the holder ended, and a caller retrying on `WaitTimeout` held the whole pool within a
    /// few tries. Skipped rather than bounded as the keyed enqueue's lock wait is (see [`bound_lock_waits`]), because
    /// a missed registration costs a poll interval, not the wait, and skipping needs no transaction to hold a bound.
    ///
    /// `FOR NO KEY UPDATE` is the lock this `UPDATE` takes on the row anyway, so the row is skipped exactly when the
    /// write would have waited.
    pub(crate) async fn mark_awaited(&self, id: Uuid) -> Result<(), Error> {
        sqlx::query(
            r#"
            UPDATE pgqueue.jobs SET awaited = true
            WHERE id = (
                SELECT id FROM pgqueue.jobs
                WHERE id = $1 AND queue = $2 AND NOT awaited
                FOR NO KEY UPDATE SKIP LOCKED
            )
            "#,
        )
        .bind(id)
        .bind(&self.name)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// The live job holding `dedupe_key` in this queue, if one does.
    ///
    /// The status set is `jobs_dedupe_key_idx`'s own predicate, so this is an
    /// index lookup; see [`DatabaseDedupeHolder`] for why both readers share it.
    async fn live_dedupe_holder(
        &self,
        dedupe_key: &str,
        executor: &mut PgConnection,
    ) -> Result<Option<DatabaseDedupeHolder>, Error> {
        Ok(sqlx::query_as::<_, DatabaseDedupeHolder>(
            r#"
            SELECT id, name, result_ttl_ms, failed_ttl_ms, scheduled_at, kind FROM pgqueue.jobs
            WHERE queue = $1 AND dedupe_key = $2
              AND status IN ('queued', 'running', 'aborting')
            "#,
        )
        .bind(&self.name)
        .bind(dedupe_key)
        .fetch_optional(executor)
        .await?)
    }

    /// [`Database::live_dedupe_holder`] for every key of a batch in one read.
    /// The same live-status set, for the reason that method's row type gives.
    async fn live_dedupe_holders(
        &self,
        dedupe_keys: &[&str],
        executor: &mut PgConnection,
    ) -> Result<Vec<DatabaseKeyedDedupeHolder>, Error> {
        Ok(sqlx::query_as::<_, DatabaseKeyedDedupeHolder>(
            r#"
            SELECT dedupe_key, id, name, result_ttl_ms, failed_ttl_ms, scheduled_at, kind
            FROM pgqueue.jobs
            WHERE queue = $1 AND dedupe_key = ANY($2)
              AND status IN ('queued', 'running', 'aborting')
            "#,
        )
        .bind(&self.name)
        .bind(dedupe_keys)
        .fetch_all(executor)
        .await?)
    }

    pub(crate) async fn reconcile_cron(
        &self,
        entry: &JobCronEntry,
        now: Timestamp,
    ) -> Result<DatabaseCronAuthority, Error> {
        let revision = i64::try_from(entry.options.revision)
            .map_err(|_| Error::Config("cron revision must fit PostgreSQL bigint".into()))?;
        let next_run_at = entry.next_occurrence(now)?;
        let policy = entry.options.misfire.kind();
        let grace_ms = entry.options.misfire.grace_ms();
        let mut connection = PoolConnectionGuard::new(self.pool.acquire().await?);
        let mut tx = connection.begin_transaction().await?;
        bound_lock_waits(&mut tx, tokio::time::Instant::now() + CRON_RECONCILE_LOCK_WAIT).await?;
        let upserted = sqlx::query(
            r#"
            INSERT INTO pgqueue.cron_schedules (
                queue, dedupe_key, name, expression, definition, revision,
                misfire_policy, grace_ms, next_run_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (queue, dedupe_key) DO UPDATE SET
                name = EXCLUDED.name,
                expression = EXCLUDED.expression,
                definition = EXCLUDED.definition,
                revision = EXCLUDED.revision,
                misfire_policy = EXCLUDED.misfire_policy,
                grace_ms = EXCLUDED.grace_ms,
                next_run_at = CASE
                    WHEN pgqueue.cron_schedules.expression = EXCLUDED.expression
                    THEN pgqueue.cron_schedules.next_run_at
                    ELSE EXCLUDED.next_run_at
                END,
                updated_at = now()
            WHERE pgqueue.cron_schedules.revision < EXCLUDED.revision
            "#,
        )
        .bind(&self.name)
        .bind(&entry.dedupe_key)
        .bind(&entry.template.name)
        .bind(&entry.expr)
        .bind(&entry.definition)
        .bind(revision)
        .bind(policy)
        .bind(grace_ms)
        .bind(next_run_at.to_sqlx())
        .execute(&mut *tx)
        .await
        .map_err(Error::from);
        // Whichever lock wait the server refused — this bound's or a shorter one the session already had — the row
        // is held, which is contention to retry rather than a definition to reject.
        if upserted.as_ref().is_err_and(is_lock_refusal) {
            tx.rollback().await?;
            return Ok(DatabaseCronAuthority::Contended);
        }
        upserted?;
        let authority = sqlx::query_as::<_, CronAuthority>(
            r#"
            SELECT name, expression, revision, misfire_policy, grace_ms,
                   definition = $3::jsonb AS definition_matches
            FROM pgqueue.cron_schedules
            WHERE queue = $1 AND dedupe_key = $2
            "#,
        )
        .bind(&self.name)
        .bind(&entry.dedupe_key)
        .bind(&entry.definition)
        .fetch_one(&mut *tx)
        .await?;
        tx.commit().await?;

        if authority.revision > revision {
            return Ok(DatabaseCronAuthority::Inactive { revision: authority.revision });
        }
        if authority.revision != revision
            || authority.name != entry.template.name
            || authority.expression != entry.expr
            || !authority.definition_matches
            || authority.misfire_policy != policy
            || authority.grace_ms != grace_ms
        {
            return Err(Error::Config(format!(
                "cron {:?} revision {} conflicts with the stored definition",
                entry.dedupe_key, revision
            )));
        }
        Ok(DatabaseCronAuthority::Active)
    }

    pub(crate) async fn remove_cron_schedule(&self, dedupe_key: &str) -> Result<bool, Error> {
        Ok(sqlx::query_scalar::<_, bool>(
            "DELETE FROM pgqueue.cron_schedules WHERE queue = $1 AND dedupe_key = $2 RETURNING true",
        )
        .bind(&self.name)
        .bind(dedupe_key)
        .fetch_optional(&self.pool)
        .await?
        .is_some())
    }

    /// The subset of `dedupe_keys` a scheduling pass has anything to do for:
    /// the schedules that are due by `through`, plus every key with no schedule
    /// row at all. `None` uses the database's current time.
    /// A missing row is not skippable — [`Database::schedule_cron`] is where it
    /// becomes the error that degrades the worker's health and queues the key
    /// for reconciliation.
    ///
    /// One pooled statement per tick stands in for one transaction per cron per
    /// tick: `schedule_cron` opens a transaction and, on the overwhelmingly
    /// common `NotDue` path, rolls it straight back, so an idle registry spent
    /// `BEGIN`/`SELECT`/`ROLLBACK` per cron per worker per tick to learn
    /// nothing. This is only a pre-filter — `schedule_cron` re-reads the row
    /// under `FOR UPDATE SKIP LOCKED` and decides for itself, so a key that
    /// stops being due in between is refused there exactly as before.
    pub(crate) async fn due_crons(
        &self,
        dedupe_keys: &[String],
        through: Option<Timestamp>,
    ) -> Result<std::collections::HashSet<String>, Error> {
        Ok(sqlx::query_scalar::<_, String>(
            r#"
            SELECT k.dedupe_key
            FROM unnest($2::text[]) AS k(dedupe_key)
            LEFT JOIN pgqueue.cron_schedules s
                ON s.queue = $1 AND s.dedupe_key = k.dedupe_key
            WHERE COALESCE(s.next_run_at <= COALESCE($3, now()), true)
            "#,
        )
        .bind(&self.name)
        .bind(dedupe_keys)
        .bind(through.map(|timestamp| timestamp.to_sqlx()))
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .collect())
    }

    pub(crate) async fn schedule_cron(
        &self,
        entry: &JobCronEntry,
        through: Option<Timestamp>,
    ) -> Result<DatabaseCronScheduleResult, Error> {
        let revision = i64::try_from(entry.options.revision)
            .map_err(|_| Error::Config("cron revision must fit PostgreSQL bigint".into()))?;
        let policy = entry.options.misfire.kind();
        let grace_ms = entry.options.misfire.grace_ms();
        let mut connection = PoolConnectionGuard::new(self.pool.acquire().await?);
        let mut tx = connection.begin_transaction().await?;
        let observed = sqlx::query_as::<_, ObservedCron>(
            r#"
            SELECT name, expression, revision, misfire_policy, grace_ms,
                   next_run_at, now() AS now,
                   definition = $3::jsonb AS definition_matches
            FROM pgqueue.cron_schedules
            WHERE queue = $1 AND dedupe_key = $2
            "#,
        )
        .bind(&self.name)
        .bind(&entry.dedupe_key)
        .bind(&entry.definition)
        .fetch_optional(&mut *tx)
        .await?;
        let Some(observed) = observed else {
            tx.rollback().await?;
            return Err(Error::Config(format!("cron schedule {:?} was not reconciled", entry.dedupe_key)));
        };
        if observed.revision > revision {
            tx.rollback().await?;
            return Ok(DatabaseCronScheduleResult::Inactive { revision: observed.revision });
        }
        // Everything else that does not match is a definition conflict at this
        // worker's own revision or below — a deploy mistake, not a deploy in
        // progress.
        if observed.revision != revision
            || observed.name != entry.template.name
            || observed.expression != entry.expr
            || !observed.definition_matches
            || observed.misfire_policy != policy
            || observed.grace_ms != grace_ms
        {
            tx.rollback().await?;
            return Ok(DatabaseCronScheduleResult::Conflicting { revision: observed.revision });
        }
        if observed.next_run_at > through.unwrap_or(observed.now) {
            tx.rollback().await?;
            return Ok(DatabaseCronScheduleResult::NotDue);
        }

        // A continuous scheduler must not let one locked row stall every cron,
        // so it skips contention and tries again on its next tick. A burst has
        // a finite scheduling boundary and no later tick: wait for rows that
        // were due at that boundary, then re-check the predicate after the lock
        // is acquired. If another scheduler advanced the cursor while we
        // waited, PostgreSQL re-evaluates the predicate and returns no row.
        let due = if let Some(through) = through {
            sqlx::query_scalar::<_, jiff_sqlx::Timestamp>(
                r#"
                SELECT next_run_at
                FROM pgqueue.cron_schedules
                WHERE queue = $1 AND dedupe_key = $2
                  AND revision = $3 AND definition = $4
                  AND next_run_at <= $5
                FOR UPDATE
                "#,
            )
            .bind(&self.name)
            .bind(&entry.dedupe_key)
            .bind(revision)
            .bind(&entry.definition)
            .bind(through.to_sqlx())
            .fetch_optional(&mut *tx)
            .await?
            .map(jiff_sqlx::Timestamp::to_jiff)
        } else {
            sqlx::query_scalar::<_, jiff_sqlx::Timestamp>(
                r#"
                SELECT next_run_at
                FROM pgqueue.cron_schedules
                WHERE queue = $1 AND dedupe_key = $2
                  AND revision = $3 AND definition = $4
                  AND next_run_at <= now()
                FOR UPDATE SKIP LOCKED
                "#,
            )
            .bind(&self.name)
            .bind(&entry.dedupe_key)
            .bind(revision)
            .bind(&entry.definition)
            .fetch_optional(&mut *tx)
            .await?
            .map(jiff_sqlx::Timestamp::to_jiff)
        };
        let Some(due) = due else {
            // An empty `SKIP LOCKED` claim cannot say why it is empty: the row is locked, or a peer already published
            // this occurrence and advanced the cursor between the read above and the claim. The second is the ordinary
            // outcome of every multi-worker race for a due cron, and reporting it as contention logged "locked by
            // another transaction; occurrence deferred" for an occurrence that had just been published, on most
            // occurrences of every cron with more than one worker. A fresh statement sees the peer's commit, so a row
            // still due here is one somebody holds. A burst waited for the lock instead, so its empty claim is not due.
            let contended = through.is_none()
                && sqlx::query_scalar::<_, bool>(
                    r#"
                    SELECT EXISTS (
                        SELECT 1 FROM pgqueue.cron_schedules
                        WHERE queue = $1 AND dedupe_key = $2
                          AND revision = $3 AND definition = $4
                          AND next_run_at <= now()
                    )
                    "#,
                )
                .bind(&self.name)
                .bind(&entry.dedupe_key)
                .bind(revision)
                .bind(&entry.definition)
                .fetch_one(&mut *tx)
                .await?;
            tx.rollback().await?;
            return Ok(if contended {
                DatabaseCronScheduleResult::Contended
            } else {
                DatabaseCronScheduleResult::NotDue
            });
        };

        let stored_occurrence = due;
        sqlx::query("SELECT pg_advisory_xact_lock($1, hashtext(length($2)::text || ':' || $2 || $3))")
            .bind(self.dedupe_enqueue_lock_key)
            .bind(&self.name)
            .bind(&entry.dedupe_key)
            .execute(&mut *tx)
            .await?;
        // The dedupe-key lock may have been held by a long caller-owned
        // transaction. Use wall-clock database time after that wait so an
        // occurrence cannot be published after its grace or successor.
        let current = sqlx::query_scalar::<_, jiff_sqlx::Timestamp>("SELECT clock_timestamp()")
            .fetch_one(&mut *tx)
            .await?
            .to_jiff();
        // Burst scheduling chooses an occurrence at its fixed boundary. The
        // actual clock still decides whether lock waiting made that occurrence
        // stale; importantly, it never moves the burst forward into a later
        // recurrence.
        let scheduling_time = through.unwrap_or(current);
        let (occurrence, successor, publish) = match entry.options.misfire {
            CronMisfirePolicy::Skip { .. } => self.skip_catch_up(entry, stored_occurrence, scheduling_time)?,
            CronMisfirePolicy::FireOnce => {
                let occurrence = entry.previous_occurrence(scheduling_time)?;
                let successor = entry.next_occurrence(occurrence)?;
                (occurrence, successor, true)
            }
        };
        let publish = publish && current < entry.publication_deadline(occurrence, successor);
        let next_run_at = if publish { successor } else { entry.next_occurrence(scheduling_time)? };
        let claim_expires_at = successor.max(current + SignedDuration::from_secs(1));

        let claimed = sqlx::query_scalar::<_, bool>(
            r#"
            INSERT INTO pgqueue.cron_occurrences (
                queue, dedupe_key, scheduled_at, expires_at
            ) VALUES ($1, $2, $3, $4)
            ON CONFLICT DO NOTHING
            RETURNING true
            "#,
        )
        .bind(&self.name)
        .bind(&entry.dedupe_key)
        .bind(occurrence.to_sqlx())
        .bind(claim_expires_at.to_sqlx())
        .fetch_optional(&mut *tx)
        .await?
        .unwrap_or(false);

        let result = if !claimed {
            DatabaseCronScheduleResult::AlreadyPublished { occurrence }
        } else if !publish {
            DatabaseCronScheduleResult::SkippedStale { occurrence }
        } else if let Some(holder) = self.live_dedupe_holder(&entry.dedupe_key, &mut tx).await? {
            DatabaseCronScheduleResult::SkippedHeld { occurrence, existing: holder }
        } else {
            let job = entry.job_for(occurrence);
            let backoff = serde_json::to_value(job.config.backoff)?;
            let inserted = sqlx::query_scalar::<_, Uuid>(
                r#"
                WITH inserted AS (
                    INSERT INTO pgqueue.jobs (
                        queue, name, payload, dedupe_key, priority,
                        max_attempts, timeout_ms, retry_delay_ms,
                        backoff, result_ttl_ms, failed_ttl_ms, scheduled_at, enqueued_at, meta, kind,
                        cron_expr
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8,
                            $9, $10, $15, $11, clock_timestamp(), $12, 'cron', $13)
                    ON CONFLICT (queue, dedupe_key) WHERE dedupe_key IS NOT NULL
                        AND status IN ('queued', 'running', 'aborting') DO NOTHING
                    RETURNING id
                )
                SELECT id, pg_notify($14, 'enqueue') IS NULL AS notified
                FROM inserted
                "#,
            )
            .bind(&self.name)
            .bind(&job.name)
            .bind(&job.payload)
            .bind(&job.dedupe_key)
            .bind(job.config.priority)
            .bind(job.config.max_attempts as i32)
            .bind(job.config.timeout.map(duration_to_ms))
            .bind(duration_to_ms(job.config.retry_delay))
            .bind(&backoff)
            .bind(job.config.retention.as_result_ttl_ms())
            .bind(occurrence.to_sqlx())
            .bind(&job.meta)
            .bind(&entry.expr)
            .bind(&self.notify_channel)
            .bind(job.config.failed_retention.as_result_ttl_ms())
            .fetch_optional(&mut *tx)
            .await?;
            // The only conflict target is the partial dedupe-key index over
            // `queued`/`running`/`aborting`, and the query just above found no
            // such row — but the two are separate statements in one READ
            // COMMITTED transaction, and the advisory lock they run under binds
            // only writers that take it. Anything writing `pgqueue.jobs`
            // directly (application SQL, a backfill, an ops script) can commit a
            // conflicting row in between, leaving `DO NOTHING` nothing to
            // return. Re-read the holder and report it, exactly as the branch
            // above does: this runs in the worker's schedule loop, where a panic
            // takes the whole worker down instead of degrading the scheduler.
            // Reporting it as `SkippedStale` would point the operator at misfire
            // grace instead of at the live holder, and unlike `SkippedHeld` that
            // warning is not de-duplicated, so it would repeat every tick.
            match inserted {
                Some(id) => DatabaseCronScheduleResult::Published { id, occurrence },
                None => match self.live_dedupe_holder(&entry.dedupe_key, &mut tx).await? {
                    Some(holder) => DatabaseCronScheduleResult::SkippedHeld { occurrence, existing: holder },
                    // The row that blocked the insert left the live statuses
                    // again before it could be named. Rolling back releases this
                    // occurrence's claim too, so the next tick republishes it —
                    // and `DedupeRace`, not `Config`, keeps the scheduler's
                    // "`Config` is permanent" taxonomy intact.
                    None => {
                        tx.rollback().await?;
                        return Err(Error::DedupeRace(format!(
                            "cron {:?} lost its dedupe key to a writer that did not take the \
                             enqueue lock; the occurrence will be retried",
                            entry.dedupe_key
                        )));
                    }
                },
            }
        };

        // `FOR UPDATE` above pinned this row for the rest of the transaction,
        // and it already matched this revision and definition, so the primary
        // key alone identifies it and the update always lands. Re-stating the
        // revision/definition guards here would only add an outcome that cannot
        // occur and so can never be tested.
        sqlx::query(
            r#"
            UPDATE pgqueue.cron_schedules
            SET next_run_at = $3, updated_at = now()
            WHERE queue = $1 AND dedupe_key = $2
            "#,
        )
        .bind(&self.name)
        .bind(&entry.dedupe_key)
        .bind(next_run_at.to_sqlx())
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(result)
    }

    /// Which occurrence a [`CronMisfirePolicy::Skip`] schedule publishes now:
    /// `(occurrence, its successor, whether to publish it)`.
    ///
    /// The durable cursor is the first candidate. When it is more than one
    /// period stale — a restart, a leader handover, or a deploy gap — refusing
    /// it and jumping straight to the next occurrence silently threw away the
    /// *most recent* occurrence even while it was still well inside its own
    /// grace, so every catch-up cost one extra occurrence with no job row, no
    /// claim, and no `SkippedStale` warning. So fall back to that occurrence
    /// when its own publication deadline has not passed.
    ///
    /// This terminates: the fallback is strictly newer than the stored cursor
    /// and its successor is strictly after `current`, and the claim row keeps a
    /// concurrent scheduler from publishing it twice.
    fn skip_catch_up(
        &self,
        entry: &JobCronEntry,
        stored_occurrence: Timestamp,
        current: Timestamp,
    ) -> Result<(Timestamp, Timestamp, bool), Error> {
        let successor = entry.next_occurrence(stored_occurrence)?;
        if current < entry.publication_deadline(stored_occurrence, successor) {
            return Ok((stored_occurrence, successor, true));
        }
        let recent = entry.previous_occurrence(current)?;
        if recent > stored_occurrence {
            let recent_successor = entry.next_occurrence(recent)?;
            if current < entry.publication_deadline(recent, recent_successor) {
                return Ok((recent, recent_successor, true));
            }
        }
        Ok((stored_occurrence, successor, false))
    }

    /// Inserts a plain (non-cron) job and emits its enqueue notification as
    /// one statement, so the insert and its wakeup cost one round trip and
    /// commit together. The keyless caller runs it on the pool directly — see
    /// `enqueue_raw_delayed_result` for why autocommit already backs the
    /// durability `EnqueueResult::Enqueued` claims; the dedupe caller passes its
    /// own transaction, which it needs for the advisory lock rather than for
    /// this insert.
    ///
    /// The wakeup is skipped for a row that is not due yet, and under the
    /// handle's notify throttle. Workers poll on their own interval for
    /// scheduled work, so a notification for a delayed row could only wake
    /// every worker on the queue to claim nothing — while still paying what
    /// every `NOTIFY` costs: PostgreSQL serializes notifying commits
    /// cluster-wide behind one lock (`PreCommit_Notify`), held through the
    /// commit's WAL flush, so under durable commits notifying transactions
    /// cannot overlap at all.
    async fn insert_job<'e>(
        &self,
        job: &JobRequest,
        delay: Option<Duration>,
        awaited: bool,
        executor: impl sqlx::PgExecutor<'e>,
    ) -> Result<Option<Uuid>, Error> {
        let backoff = serde_json::to_value(job.config.backoff)?;
        let notify = self.authorize_wakeup(is_due_now(job, delay));
        let row = sqlx::query_scalar::<_, Uuid>(
            r#"
            WITH inserted AS (
                INSERT INTO pgqueue.jobs (
                    queue, name, payload, dedupe_key, priority, max_attempts,
                    timeout_ms, retry_delay_ms, backoff, result_ttl_ms, failed_ttl_ms,
                    scheduled_at, enqueued_at, meta, kind, cron_expr, awaited
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $15,
                        COALESCE(
                            $11,
                            statement_timestamp() + ($13::bigint * interval '1 millisecond'),
                            statement_timestamp()
                        ),
                        statement_timestamp(), $12, 'job', NULL, $16)
                ON CONFLICT (queue, dedupe_key) WHERE dedupe_key IS NOT NULL
                    AND status IN ('queued', 'running', 'aborting') DO NOTHING
                RETURNING id, scheduled_at
            )
            SELECT id,
                   (CASE WHEN $17::boolean AND scheduled_at <= statement_timestamp()
                         THEN pg_notify($14, 'enqueue') END) IS NULL AS notified
            FROM inserted
            "#,
        )
        .bind(&self.name)
        .bind(&job.name)
        .bind(&job.payload)
        .bind(&job.dedupe_key)
        .bind(job.config.priority)
        .bind(job.config.max_attempts as i32)
        .bind(job.config.timeout.map(duration_to_ms))
        .bind(duration_to_ms(job.config.retry_delay))
        .bind(&backoff)
        .bind(job.config.retention.as_result_ttl_ms())
        .bind(job.scheduled_at.map(|timestamp| timestamp.to_sqlx()))
        .bind(&job.meta)
        .bind(delay.map(duration_to_ms))
        .bind(&self.notify_channel)
        .bind(job.config.failed_retention.as_result_ttl_ms())
        .bind(awaited)
        .bind(notify)
        .fetch_optional(executor)
        .await?;
        Ok(row)
    }

    /// [`Database::insert_job`] for a batch: one statement over unnested
    /// arrays, with client-generated ids so the caller can tell which rows
    /// landed, and at most one wakeup for the whole batch — emitted when any
    /// inserted row is due, and evaluated once because the lateral aggregate
    /// is uncorrelated. Returns the ids that were inserted; a keyed row `DO
    /// NOTHING` swallowed is absent.
    ///
    /// Payloads and metadata travel as `text[]` and are cast to `jsonb` row by row. Bound as `jsonb[]`, each column was
    /// one array datum, which PostgreSQL caps at 1 GiB, and `jsonb` stores an array of small numbers in six times its
    /// JSON text: a batch well inside [`crate::MAX_ENQUEUE_BATCH_BYTES`] failed with `54000` after the server parsed all
    /// of it into memory at once.
    async fn insert_jobs<'e>(
        &self,
        rows: &[(Uuid, &JobRequest, Option<Duration>)],
        executor: impl sqlx::PgExecutor<'e>,
    ) -> Result<Vec<Uuid>, Error> {
        if rows.is_empty() {
            return Ok(Vec::new());
        }
        let mut backoffs = Vec::with_capacity(rows.len());
        let mut payloads = Vec::with_capacity(rows.len());
        let mut metas = Vec::with_capacity(rows.len());
        for (_, job, _) in rows {
            backoffs.push(serde_json::to_value(job.config.backoff)?);
            payloads.push(serde_json::to_string(&job.payload)?);
            metas.push(serde_json::to_string(&job.meta)?);
        }
        let notify = self.authorize_wakeup(rows.iter().any(|(_, job, delay)| is_due_now(job, *delay)));
        let inserted = sqlx::query_scalar::<_, Uuid>(
            r#"
            WITH input AS (
                SELECT *
                FROM unnest($3::uuid[], $4::text[], $5::text[], $6::text[], $7::smallint[], $8::integer[],
                            $9::bigint[], $10::bigint[], $11::jsonb[], $12::bigint[], $13::bigint[],
                            $14::timestamptz[], $15::bigint[], $16::text[])
                    AS t(id, name, payload, dedupe_key, priority, max_attempts,
                         timeout_ms, retry_delay_ms, backoff, result_ttl_ms, failed_ttl_ms,
                         scheduled_at, delay_ms, meta)
            ),
            inserted AS (
                INSERT INTO pgqueue.jobs (
                    id, queue, name, payload, dedupe_key, priority, max_attempts,
                    timeout_ms, retry_delay_ms, backoff, result_ttl_ms, failed_ttl_ms,
                    scheduled_at, enqueued_at, meta, kind, cron_expr, awaited
                )
                SELECT id, $1, name, payload::jsonb, dedupe_key, priority, max_attempts,
                       timeout_ms, retry_delay_ms, backoff, result_ttl_ms, failed_ttl_ms,
                       COALESCE(
                           scheduled_at,
                           statement_timestamp() + (delay_ms * interval '1 millisecond'),
                           statement_timestamp()
                       ),
                       statement_timestamp(), meta::jsonb, 'job', NULL, false
                FROM input
                ON CONFLICT (queue, dedupe_key) WHERE dedupe_key IS NOT NULL
                    AND status IN ('queued', 'running', 'aborting') DO NOTHING
                RETURNING id, scheduled_at
            )
            SELECT inserted.id
            FROM inserted
            CROSS JOIN LATERAL (
                SELECT CASE WHEN $17::boolean AND bool_or(due.scheduled_at <= statement_timestamp())
                            THEN pg_notify($2, 'enqueue') END
                FROM inserted due
            ) AS notified
            "#,
        )
        .bind(&self.name)
        .bind(&self.notify_channel)
        .bind(rows.iter().map(|(id, _, _)| *id).collect::<Vec<_>>())
        .bind(rows.iter().map(|(_, job, _)| job.name.clone()).collect::<Vec<_>>())
        .bind(payloads)
        .bind(rows.iter().map(|(_, job, _)| job.dedupe_key.clone()).collect::<Vec<_>>())
        .bind(rows.iter().map(|(_, job, _)| job.config.priority).collect::<Vec<_>>())
        .bind(rows.iter().map(|(_, job, _)| job.config.max_attempts as i32).collect::<Vec<_>>())
        .bind(rows.iter().map(|(_, job, _)| job.config.timeout.map(duration_to_ms)).collect::<Vec<_>>())
        .bind(rows.iter().map(|(_, job, _)| duration_to_ms(job.config.retry_delay)).collect::<Vec<_>>())
        .bind(backoffs)
        .bind(rows.iter().map(|(_, job, _)| job.config.retention.as_result_ttl_ms()).collect::<Vec<_>>())
        .bind(rows.iter().map(|(_, job, _)| job.config.failed_retention.as_result_ttl_ms()).collect::<Vec<_>>())
        .bind(rows.iter().map(|(_, job, _)| job.scheduled_at.map(|at| at.to_sqlx())).collect::<Vec<_>>())
        .bind(rows.iter().map(|(_, _, delay)| delay.map(duration_to_ms)).collect::<Vec<_>>())
        .bind(metas)
        .bind(notify)
        .fetch_all(executor)
        .await?;
        Ok(inserted)
    }
}

/// Whether a request will be due the moment it is inserted, as far as the
/// client clock can tell. Only a throttled handle's slot accounting reads
/// this; the statement decides on the server clock whether to notify.
fn is_due_now(job: &JobRequest, delay: Option<Duration>) -> bool {
    delay.is_none_or(|delay| delay.is_zero()) && job.scheduled_at.is_none_or(|at| at <= Timestamp::now())
}

/// The holder of one dedupe key as a batch records it: a live row read under
/// the batch's locks, or the first job of the batch to claim the key. Every
/// later job of the batch carrying that key is reported as deduplicated
/// against it.
struct BatchKeyHolder {
    id: Uuid,
    name: String,
    retentions: JobRetentions,
}

impl BatchKeyHolder {
    fn deduplicated(&self) -> DatabaseEnqueueResult {
        DatabaseEnqueueResult::Deduplicated { id: self.id, name: self.name.clone(), retentions: self.retentions }
    }
}

impl From<DatabaseDedupeHolder> for BatchKeyHolder {
    fn from(holder: DatabaseDedupeHolder) -> Self {
        Self { id: holder.id, retentions: holder.retentions(), name: holder.name }
    }
}

/// The abort poll [`Database::aborting_of`] runs. It reads its rows by primary
/// key alone, for the reason [`FINISH_GUARDED_SQL`] gives — and it is the
/// statement where that matters most: a worker runs it once a second for as long
/// as it has attempts in flight, and its ids arrive as an array, so the plan
/// cache never settles on the generic plan the way a single-id statement does
/// after five executions. The generic plan is costed for an array of ten, above
/// a custom plan that reads the queue through a queue-leading index, so a custom
/// plan that took one stayed in use for every poll until an `ANALYZE` sampled the
/// queue.
pub(crate) const ABORT_POLL_SQL: &str = r#"
    SELECT j.id, j.status, j.attempts, j.worker_id,
           -- Only the abort arm of `aborting_of` reads these two, and it is
           -- reached only for an `aborting`/`aborted` row — so a `running` one,
           -- which is the overwhelmingly common case, transfers neither.
           -- `error` is what makes that worth doing: `REQUEUE_GUARDED_SQL`
           -- stores up to 1 MiB of the previous attempt's message there,
           -- and this poll runs once per
           -- `WorkerTimers::abort` (1s by default) for *every* in-flight
           -- attempt — so one handler that failed with a large message
           -- (an HTTP error body, say) had that message re-read and
           -- re-allocated every second for the whole of its next attempt,
           -- on the pool the worker also dequeues and finalizes with.
           CASE WHEN j.status IN ('aborting', 'aborted') THEN j.error END AS error,
           CASE WHEN j.status IN ('aborting', 'aborted') THEN j.result END AS result
    FROM pgqueue.jobs j
    WHERE j.id = ANY($1)
      -- Spelled so that only `id` can reach an index; see `FINISH_GUARDED_SQL`.
      AND j.queue IS NOT DISTINCT FROM $2
"#;

/// [`Queue::jobs_page`](crate::Queue::jobs_page)'s statement.
///
/// The cursor comparison is unconditional, with a missing cursor spelled as a
/// sentinel that admits every row, so that it is an index condition on
/// `jobs_page_idx` under a generic plan. As `($5 IS NULL OR (enqueued_at, id) <
/// ($5, $6))` it was a `Filter` — the planner cannot fold a parameter it does
/// not know — and every page read every row newer than its cursor: 250,001 rows
/// and 6,613 buffers for one 50-row page 250,000 rows deep, against 5 buffers
/// with the bound in the Index Cond. `enqueued_at` is finite by
/// `jobs_timestamps_jiff_range_check`, so every row sorts before `'infinity'`.
pub(crate) const JOBS_PAGE_SQL: &str = r#"
    SELECT id, dedupe_key, queue, name, payload,
           status, priority, attempts, refunds,
           max_attempts, timeout_ms, retry_delay_ms,
           backoff, result_ttl_ms, failed_ttl_ms, scheduled_at,
           enqueued_at, started_at, touched_at, completed_at, expires_at,
           result, error, meta, worker_id, kind, cron_expr, retried_at
    FROM pgqueue.jobs
    WHERE queue = $1
      AND ($2::text IS NULL OR status = $2)
      AND ($3::text IS NULL OR name = $3)
      AND (enqueued_at, id) < (COALESCE($5::timestamptz, 'infinity'),
                               COALESCE($6::uuid, '00000000-0000-0000-0000-000000000000'))
    ORDER BY enqueued_at DESC, id DESC
    LIMIT $4
"#;

/// The purge. The delete matches the doomed rows by id alone, for the reasons
/// the claim's update does (see [`DEQUEUE_CLAIM_SQL`]). Nothing can claim a row
/// between the two steps: `doomed` holds each row it returns `FOR UPDATE` until
/// this statement commits, having re-verified `queued` on its latest version
/// first. Re-checking `status = 'queued'` in the delete only gave the planner
/// the queued-only partial indexes to read its target through, and under
/// statistics that put the queued set at about a row — the moment a runaway
/// producer's backlog lands is exactly that moment — it scanned every queued
/// row of every queue and compared each against the whole doomed batch, on
/// every call of the loop that empties a queue.
pub(crate) const PURGE_QUEUED_JOBS_SQL: &str = r#"
            WITH doomed AS (
                SELECT id FROM pgqueue.jobs
                WHERE queue = $1 AND status = 'queued'
                  AND ($2::text IS NULL OR name = $2)
                ORDER BY scheduled_at, id
                LIMIT $3
                FOR UPDATE SKIP LOCKED
            ),
            deleted AS (
                DELETE FROM pgqueue.jobs j
                WHERE j.id = ANY (ARRAY(SELECT id FROM doomed))
                RETURNING j.id
            )
            SELECT count(*) FROM deleted
            "#;

/// When the oldest job ready in queue `$1` last became ready, as a scalar
/// subquery. [`Database::counts`] and the dashboard's queue signals both report
/// it, so they share it.
///
/// Its due time is not that. A requeue without a delay — a retry under the
/// default zero `retry_delay`, a shutdown, the sweeper taking back an abandoned
/// attempt, an unacknowledged claim resolved — keeps the row's `scheduled_at` on
/// purpose, because that is its place at the head of the queue, and stamps
/// `touched_at` instead. Read as readiness, the due time made a job that failed
/// after a 45-minute attempt and was ready again a moment ago report 45 minutes
/// of backlog latency, and every deploy that put in-flight work back look like a
/// stall. Every insert leaves `touched_at` NULL, the claim stamps it, and both
/// requeue statements stamp it again, so for a `queued` row the later of the two
/// is when it last became ready.
///
/// No index orders a queue by that, and a `min()` over every ready row costs the
/// whole backlog. So this walks `jobs_dashboard_ready_idx` in `(scheduled_at,
/// id)` order carrying the running minimum, and stops at the first row due no
/// earlier than it: no row is ready before it is due, so nothing further on can
/// lower the answer. That reads the requeued rows waiting ahead of the answer and
/// one more — normally a handful, since a requeued row is first in line and
/// claimed next. Never more than 1,001: the last row read stands in with its due
/// time for everything after it, which can only make the answer earlier than the
/// truth, so a queue holding more requeued rows than that reports its latency
/// high rather than hiding a backlog, and the statement stays bounded however the
/// rows are shaped.
macro_rules! oldest_ready_at_sql {
    () => {
        r#"(
                WITH RECURSIVE walk AS (
                    -- `GREATEST` skips a NULL, so a row never put back is ready when it is due.
                    (SELECT j.scheduled_at, j.id, 1 AS rows_read,
                            GREATEST(j.scheduled_at, j.touched_at) AS ready_at
                     FROM pgqueue.jobs j
                     WHERE j.queue = $1 AND j.status = 'queued' AND j.scheduled_at <= now()
                     ORDER BY j.scheduled_at, j.id
                     LIMIT 1)
                    UNION ALL
                    SELECT n.scheduled_at, n.id, walk.rows_read + 1,
                           LEAST(walk.ready_at, CASE WHEN walk.rows_read < 1000
                                                     THEN GREATEST(n.scheduled_at, n.touched_at)
                                                     ELSE n.scheduled_at END)
                    FROM walk
                    CROSS JOIN LATERAL (
                        SELECT j.scheduled_at, j.id, j.touched_at
                        FROM pgqueue.jobs j
                        WHERE j.queue = $1 AND j.status = 'queued' AND j.scheduled_at <= now()
                          AND (j.scheduled_at, j.id) > (walk.scheduled_at, walk.id)
                        ORDER BY j.scheduled_at, j.id
                        LIMIT 1
                    ) n
                    WHERE n.scheduled_at < walk.ready_at
                )
                SELECT min(ready_at) FROM walk
            )"#
    };
}
// For the dashboard's statement; `counts` below reaches the macro textually.
#[cfg(feature = "dashboard")]
pub(crate) use oldest_ready_at_sql;

impl Database {
    pub(crate) async fn jobs_page(
        &self,
        status: Option<&str>,
        name: Option<&str>,
        limit: i64,
        before: Option<JobCursor>,
    ) -> Result<Vec<JobRow>, Error> {
        let (before_enqueued_at, before_id) =
            before.map(|cursor| (Some(cursor.enqueued_at), Some(cursor.id))).unwrap_or((None, None));
        Ok(sqlx::query_as::<_, JobRow>(JOBS_PAGE_SQL)
            .bind(&self.name)
            .bind(status)
            .bind(name)
            .bind(limit)
            .bind(before_enqueued_at.map(|timestamp| timestamp.to_sqlx()))
            .bind(before_id)
            .fetch_all(&self.pool)
            .await?)
    }

    /// Five independent scalar aggregates, not five `FILTER`s over one scan.
    /// A shared `FROM pgqueue.jobs WHERE queue = $1` is a sequential scan of
    /// the queue's whole retained history — overwhelmingly `complete` rows,
    /// which no counter here reports — so its cost grew with throughput times
    /// retention, and was unbounded under `JobRetention::Forever`. Split, each
    /// counter carries its own status predicate and every one of them is served
    /// by an existing index rather than by that scan. Which index is the
    /// planner's choice, not this statement's; measured on PostgreSQL 18.4
    /// under `force_generic_plan` over 150,000 retained rows in one queue
    /// (140,000 `complete`, 6,000 `queued` split evenly between due and future,
    /// 500 `running`, 2,000 `failed`, 1,500 `aborted`, across 50 job names), it
    /// picks `jobs_dequeue_idx` for the ready `queued` half, `jobs_active_idx`
    /// for `running`, `jobs_dashboard_ready_idx` for the future-scheduled half,
    /// `jobs_dashboard_terminal_idx` for both `failed` and `aborted`. One
    /// statement is one snapshot and one `now()`, so the halves still partition
    /// the `queued` rows exactly as the single scan did. `oldest_ready_at` is
    /// [`oldest_ready_at_sql!`]'s bounded walk of `jobs_dashboard_ready_idx`.
    pub(crate) async fn counts(&self) -> Result<QueueCounts, Error> {
        Ok(sqlx::query_as::<_, QueueCounts>(concat!(
            r#"
            SELECT
                (SELECT COUNT(*) FROM pgqueue.jobs
                  WHERE queue = $1 AND status = 'queued'
                    AND scheduled_at <= now()) AS queued,
                (SELECT COUNT(*) FROM pgqueue.jobs
                  WHERE queue = $1 AND status IN ('running', 'aborting')) AS running,
                (SELECT COUNT(*) FROM pgqueue.jobs
                  WHERE queue = $1 AND status = 'queued'
                    AND scheduled_at > now()) AS scheduled,
                (SELECT COUNT(*) FROM pgqueue.jobs
                  WHERE queue = $1 AND status = 'failed') AS failed,
                (SELECT COUNT(*) FROM pgqueue.jobs
                  WHERE queue = $1 AND status = 'aborted') AS aborted,
                "#,
            oldest_ready_at_sql!(),
            r#" AS oldest_ready_at
            "#
        ))
        .bind(&self.name)
        .fetch_one(&self.pool)
        .await?)
    }

    /// Deletes up to `limit` queued jobs of this queue, oldest due first, and
    /// returns how many went. `name` narrows the purge to one job name.
    /// Rows another transaction holds locked — a claim in flight — are skipped
    /// rather than waited for; see [`PURGE_QUEUED_JOBS_SQL`] for why the delete
    /// itself matches by id alone.
    pub(crate) async fn purge_queued_jobs(&self, name: Option<&str>, limit: u32) -> Result<u64, Error> {
        if limit == 0 {
            return Err(Error::Config("purge limit must be at least one job".into()));
        }
        let deleted = sqlx::query_scalar::<_, i64>(PURGE_QUEUED_JOBS_SQL)
            .bind(&self.name)
            .bind(name)
            .bind(i64::from(limit))
            .fetch_one(&self.pool)
            .await?;
        Ok(u64::try_from(deleted).unwrap_or_default())
    }

    pub(crate) async fn workers_page(&self, limit: i64, after: Option<WorkerCursor>) -> Result<Vec<WorkerInfo>, Error> {
        let (after_started_at, after_id) = after.map(|cursor| (cursor.started_at, cursor.id)).unzip();
        Ok(sqlx::query_as::<_, WorkerInfo>(
            r#"
            SELECT id, queue, stats, metadata, started_at, heartbeat_at, expires_at
            FROM pgqueue.workers
            WHERE queue = $1 AND expires_at > now()
              AND ($2::timestamptz IS NULL OR (started_at, id) > ($2, $3))
            ORDER BY started_at, id
            LIMIT $4
            "#,
        )
        .bind(&self.name)
        .bind(after_started_at.map(|timestamp| timestamp.to_sqlx()))
        .bind(after_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?)
    }

    pub(crate) async fn write_worker_info(
        &self,
        worker_id: Uuid,
        stats: Value,
        metadata: Option<Value>,
        ttl: Duration,
        intake: LeaseIntake,
    ) -> Result<(), Error> {
        validate_worker_info(&stats, metadata.as_ref(), ttl)?;
        let written =
            self.worker_info_query(worker_id, stats, metadata, ttl, intake).fetch_optional(&self.pool).await?;
        worker_info_written(worker_id, written)
    }

    /// [`Database::write_worker_info`] as a worker writes its own lease on the pool — at startup, through its
    /// shutdown, and for a heartbeat that could not open a connection of its own (see
    /// [`Database::renew_worker_lease`]): the write is held to `deadline` once it has a connection — on the client,
    /// and on the server for its wait on the lease row's lock (see [`WriteDeadline`]).
    ///
    /// Each part of that answers a way a lease write kept a heartbeat from landing. A deadline counted from before
    /// the pool handed a connection over abandoned a write that was merely queued, and the pool is fair, so the retry
    /// went to the back of the queue: with every acquire waiting longer than the deadline but within the acquire
    /// timeout every other worker operation tolerates, no heartbeat ever landed — so a connection is still waited for
    /// under the pool's `acquire_timeout` alone. But an idle one is taken as it is, never through `acquire`, which
    /// pings it first with nothing but that same 30s timeout to bound the ping: a connection that went dead while
    /// idle never answers one, and held the write for the whole default lease. Taken as it is, it costs the write
    /// its deadline instead, and a write that does not come back closes its connection rather than leaving sqlx to
    /// ping it on the way back into the pool. And a write abandoned on the client alone, behind an operator's
    /// transaction holding the lease row, kept its connection until that transaction ended, so a retrying loop
    /// parked one more pooled connection per retry until none was left.
    pub(crate) async fn write_worker_lease_within(
        &self,
        worker_id: Uuid,
        stats: Value,
        metadata: Option<Value>,
        ttl: Duration,
        intake: LeaseIntake,
        deadline: Duration,
    ) -> Result<(), Error> {
        validate_worker_info(&stats, metadata.as_ref(), ttl)?;
        let query = self.worker_info_query(worker_id, stats, metadata, ttl, intake);
        let pooled = match self.pool.try_acquire() {
            Some(idle) => idle,
            None => self.pool.acquire().await?,
        };
        let mut connection = PoolConnectionGuard::new(pooled);
        let deadline =
            WriteDeadline { at: tokio::time::Instant::now() + deadline, exceeded: LEASE_WRITE_DEADLINE_EXCEEDED };
        let written = tokio::time::timeout_at(deadline.at, write_lease_on(connection.connection(), deadline, query))
            .await
            .unwrap_or(Err(Error::WorkerTask(deadline.exceeded)));
        // Pooled again only once the write has committed. Any other outcome may leave the connection inside a
        // transaction — sqlx forgets a `BEGIN` whose bound then failed — or on one that will never answer the ping sqlx
        // runs before pooling it.
        if written.is_ok() {
            connection.disarm();
        }
        worker_info_written(worker_id, written?)
    }

    /// One periodic heartbeat of a worker's lease: [`Database::write_worker_lease_within`] on `lease`, a connection of
    /// the heartbeat loop's own outside the pool, with opening it counted against `deadline` as well. A heartbeat that
    /// does not land discards the connection, and the next opens a fresh one; see `worker::heartbeat_deadline` for why
    /// the pool could not hold a heartbeat to its deadline.
    ///
    /// Only a connection that cannot be opened — a server or role out of connections, a proxy refusing one more, or
    /// queueing it — sends the heartbeat to the pool instead. The lease is what keeps this worker's attempts its own,
    /// and a worker whose pool still works has to keep it whatever happens to one more connection; the next heartbeat
    /// tries to open one again.
    #[expect(clippy::too_many_arguments, reason = "the lease write's own arguments, and the connection to make it on")]
    pub(crate) async fn renew_worker_lease(
        &self,
        lease: &mut LeaseConnection,
        worker_id: Uuid,
        stats: Value,
        metadata: Option<Value>,
        ttl: Duration,
        intake: LeaseIntake,
        deadline: Duration,
    ) -> Result<(), Error> {
        validate_worker_info(&stats, metadata.as_ref(), ttl)?;
        let bound =
            WriteDeadline { at: tokio::time::Instant::now() + deadline, exceeded: LEASE_WRITE_DEADLINE_EXCEEDED };
        if std::mem::take(&mut lease.writing) {
            lease.discard();
        }
        let connection = match lease.connection.take() {
            Some(connection) => connection,
            // Half the deadline, so that one the server will not serve leaves the pool the rest.
            None => match self.open_lease_connection(deadline / 2).await {
                Ok(connection) => {
                    lease.opened(&self.name, worker_id);
                    connection
                }
                Err(error) => {
                    lease.refused(&self.name, worker_id, &error);
                    return self.write_worker_lease_within(worker_id, stats, metadata, ttl, intake, deadline).await;
                }
            },
        };
        let query = self.worker_info_query(worker_id, stats, metadata, ttl, intake);
        // Back in `lease` while it writes, so that a heartbeat dropped mid-write leaves it for `writing` to find, and
        // for dropping `lease` to close gracefully.
        let connection = lease.connection.insert(connection);
        lease.writing = true;
        let written = tokio::time::timeout_at(bound.at, write_lease_on(connection, bound, query))
            .await
            .unwrap_or(Err(Error::WorkerTask(bound.exceeded)));
        lease.writing = false;
        match written {
            Ok(written) => worker_info_written(worker_id, written),
            Err(error) => {
                lease.discard();
                Err(error)
            }
        }
    }

    /// Opens a connection for [`Database::renew_worker_lease`] from the pool's connect options, and has it answer
    /// once, all within `within`. Accepting a connection is not serving it: a session-mode proxy out of server
    /// connections logs a client in and then queues it at its first statement, which a heartbeat must not find out
    /// only by waiting out its whole deadline.
    async fn open_lease_connection(&self, within: Duration) -> Result<PgConnection, Error> {
        let open = async {
            let mut connection = self.pool.connect_options().connect().await?;
            connection.ping().await?;
            Ok::<_, sqlx::Error>(connection)
        };
        match tokio::time::timeout(within, open).await {
            Ok(opened) => Ok(opened?),
            Err(_) => Err(Error::WorkerTask("opening the worker lease connection exceeded its deadline")),
        }
    }

    /// The lease upsert both lease writers bind.
    fn worker_info_query(
        &self,
        worker_id: Uuid,
        stats: Value,
        metadata: Option<Value>,
        ttl: Duration,
        intake: LeaseIntake,
    ) -> sqlx::query::QueryScalar<'_, Postgres, Uuid, sqlx::postgres::PgArguments> {
        // ON CONFLICT evaluates its SET list after taking the row lock; transaction time would shorten the lease.
        sqlx::query_scalar::<_, Uuid>(
            r#"
            INSERT INTO pgqueue.workers (id, queue, stats, metadata, expires_at, accepting, heartbeat_at)
            VALUES ($1, $2, $3, $5, clock_timestamp() + ($4::bigint * interval '1 millisecond'), $7, clock_timestamp())
            ON CONFLICT (id) DO UPDATE SET
                stats = $3, metadata = $5, heartbeat_at = clock_timestamp(),
                expires_at = clock_timestamp() + ($4::bigint * interval '1 millisecond'),
                accepting = CASE WHEN $6 THEN true ELSE pgqueue.workers.accepting END
            WHERE pgqueue.workers.queue = EXCLUDED.queue
            RETURNING id
            "#,
        )
        .bind(worker_id)
        .bind(&self.name)
        .bind(stats)
        .bind(duration_to_ms(ttl))
        .bind(metadata)
        .bind(intake.reopens())
        .bind(intake.accepts_when_created())
    }

    pub(crate) async fn stop_worker_intake(&self, worker_id: Uuid) -> Result<(), Error> {
        sqlx::query(
            r#"
            UPDATE pgqueue.workers SET accepting = false, heartbeat_at = now()
            WHERE id = $1 AND queue = $2
            "#,
        )
        .bind(worker_id)
        .bind(&self.name)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Reads the rows behind `worker_id`'s in-flight attempts and sorts them
    /// into the three states that end an attempt early.
    ///
    /// A row whose `(attempts, worker_id)` no longer match the claim is
    /// reported as superseded: recovery took the attempt away — by requeueing
    /// the row, which clears `worker_id`, or by letting a later dequeue claim
    /// it with `attempts + 1` — so the row is queued or running for someone
    /// else. That state is neither `aborting` nor missing, and every write the
    /// displaced attempt could still make is guarded out by the same pair, so
    /// it has to be cancelled here or it keeps its processor slot until it
    /// returns on its own — never, when its timeout is disabled.
    ///
    /// Claims are matched against the rows without consuming them: the same id
    /// can arrive under two attempt numbers when this worker re-claimed a row
    /// recovery had taken from it, and the second claim must be answered from
    /// the same row as the first rather than reported missing.
    pub(crate) async fn aborting_of(
        &self,
        claims: &[DatabaseAbortClaim],
        worker_id: Uuid,
    ) -> Result<DatabaseAbortPoll, Error> {
        let ids = claims.iter().map(|claim| claim.id).collect::<Vec<_>>();
        let rows =
            sqlx::query_as::<_, AbortPollRow>(ABORT_POLL_SQL).bind(&ids).bind(&self.name).fetch_all(&self.pool).await?;
        let present = rows.into_iter().map(|row| (row.id, row)).collect::<std::collections::HashMap<_, _>>();
        let mut aborting = Vec::new();
        let mut missing = Vec::new();
        let mut superseded = Vec::new();
        for claim in claims {
            match present.get(&claim.id) {
                None => missing.push(*claim),
                Some(row) if row.attempts != claim.attempts || row.worker_id != Some(worker_id) => {
                    superseded.push(*claim);
                }
                Some(row) if matches!(row.status, JobStatus::Aborting | JobStatus::Aborted) => {
                    aborting.push(DatabaseAbortingAttempt {
                        // A row already *finished* `aborted` under a live attempt was finished by recovery, not by its
                        // owner, and finishing clears the marker pair: read as a user abort, it
                        // bought the handler a cooperative grace on a row that is no longer its own.
                        swept: row.status == JobStatus::Aborted
                            || is_swept_marked(row.error.as_deref(), row.result.as_ref()),
                        id: row.id,
                        attempts: row.attempts,
                        reason: row.error.clone(),
                    });
                }
                // Still running as claimed: nothing to signal.
                Some(_) => {}
            }
        }
        Ok(DatabaseAbortPoll { aborting, missing, superseded })
    }

    pub(crate) async fn now(&self) -> Result<Timestamp, Error> {
        Ok(sqlx::query_scalar::<_, jiff_sqlx::Timestamp>("SELECT now()").fetch_one(&self.pool).await?.to_jiff())
    }

    async fn notify(&self, tx: &mut sqlx::PgTransaction<'_>, channel: &str, payload: &str) -> Result<(), Error> {
        sqlx::query("SELECT pg_notify($1, $2)").bind(channel).bind(payload).execute(&mut **tx).await?;
        Ok(())
    }
}

impl Database {
    /// Requeues an attempt the sweeper marked for abort, on behalf of the
    /// worker that still owns it. The sweeper's own recovery of an abandoned
    /// attempt goes through [`Database::retry_swept_abandoned_batch`], which
    /// carries the extra stuckness and dead-owner guards that path needs.
    ///
    /// `error` is what the attempt ended with, when it ended with something the
    /// operator needs to see: a handler failure that raced the sweeper's abort
    /// is still a real failure, and storing it is what keeps the retry-backoff
    /// window and the next attempt from reporting the sweeper's internal
    /// `swept` marker as the reason. `None` — the attempt the sweeper itself
    /// ended — keeps that marker, which is the accurate reason there.
    ///
    /// `deadline` is a worker's retried finalization's; see [`Database::finish`].
    pub(crate) async fn retry_swept(
        &self,
        job: &JobRow,
        error: Option<&str>,
        deadline: Option<WriteDeadline>,
    ) -> Result<bool, Error> {
        // The same boundary `Database::retry` applies, for the same reason: a
        // NUL in `error` is `22021`, which is permanent, and `finalize` retries
        // a failed requeue once a second forever — pinning the processor slot.
        // Every caller today launders its reason through `JobError::new`, so
        // this holds the invariant where it belongs instead of in three
        // `worker.rs` call sites that must each remember it.
        self.ensure_owns(job)?;
        validate_finalization(None, error)?;
        let error = error.map(truncate_stored_error);
        let guards = DatabaseRequeueGuards {
            allow_running: false,
            allow_swept_abort: true,
            refund_attempt: false,
            close_intake: false,
        };
        let updated = self
            .requeue_guarded(AttemptGuard::from(job), error.as_deref(), job.next_retry_delay(), guards, deadline)
            .await?;
        if updated {
            self.counters.record_retry();
        }
        Ok(updated)
    }

    /// The three columns a result wait reads, and nothing else.
    ///
    /// [`Database::job`] projects 27 columns, `payload` and `meta` included, and the
    /// wait's polling fallback re-reads the row every two seconds for as long as
    /// the notification listener is down — which is its documented, expected state
    /// across a reconnect. A wait on a job with a large payload therefore
    /// re-transferred that payload on every poll, multiplied by however many
    /// waiters `enqueue_and_wait` has outstanding, to look at a status.
    pub(crate) async fn job_outcome(&self, id: Uuid) -> Result<Option<DatabaseJobOutcome>, Error> {
        Ok(sqlx::query_as::<_, DatabaseJobOutcome>(
            r#"
            SELECT status, result, error, awaited FROM pgqueue.jobs WHERE id = $1 AND queue = $2
            "#,
        )
        .bind(id)
        .bind(&self.name)
        .fetch_optional(&self.pool)
        .await?)
    }

    pub(crate) async fn job(&self, id: Uuid) -> Result<Option<JobRow>, Error> {
        Ok(sqlx::query_as::<_, JobRow>(
            r#"
            SELECT id, dedupe_key, queue, name, payload,
                   status, priority, attempts, refunds,
                   max_attempts, timeout_ms, retry_delay_ms,
                   backoff, result_ttl_ms, failed_ttl_ms, scheduled_at,
                   enqueued_at, started_at, touched_at, completed_at, expires_at,
                   result, error, meta, worker_id, kind, cron_expr, retried_at
            FROM pgqueue.jobs WHERE id = $1 AND queue = $2
            "#,
        )
        .bind(id)
        .bind(&self.name)
        .fetch_optional(&self.pool)
        .await?)
    }

    /// A sweeper-marked `aborting` row is claimed too: the sweeper's pending
    /// retry would otherwise run the job again with the abort silently
    /// dropped. Storing the reason and clearing the marker is what converts
    /// that retry intent into a user abort — every downstream requeue guard
    /// keys on the marker pair, so the row can only finish `aborted` from
    /// here. A row already `aborting` for a user abort carries no marker and
    /// is left alone.
    pub(crate) async fn abort(&self, id: Uuid, reason: &str) -> Result<bool, Error> {
        self.abort_within(id, reason, None).await
    }

    /// [`Database::abort`], with its wait for the job's row lock ended on the server at `deadline` when one is given
    /// (see [`bound_lock_waits`]), for a caller that abandons the call there — the dashboard's Abort, cut off at its
    /// request deadline. A wait still going at the deadline is refused with the `55P03` it is, and a deadline already
    /// behind the call is reported as [`Error::WaitTimeout`] without the statement being sent; either way the job is
    /// left as it was.
    pub(crate) async fn abort_within(
        &self,
        id: Uuid,
        reason: &str,
        deadline: Option<tokio::time::Instant>,
    ) -> Result<bool, Error> {
        // The reason lands in the `error` column, which `text` bounds exactly
        // as `validate_finalization` describes: a NUL raises `22021` there, an
        // `Error::Db` indistinguishable from a transient failure, where every
        // other writer of the column answers `Error::Config`.
        validate_finalization(None, Some(reason))?;
        let reason = truncate_stored_error(reason);
        let payload = format!(r#"{{"id":"{id}","status":"aborted"}}"#);
        let abort = sqlx::query_as::<_, AbortResult>(
            r#"
            WITH candidate AS (
                SELECT id FROM pgqueue.jobs
                WHERE id = $1 AND queue = $3
                  AND (status IN ('queued', 'running')
                       OR (status = 'aborting' AND error = $6 AND result = $7))
                FOR UPDATE
            ), updated AS (
                UPDATE pgqueue.jobs j
                SET status = CASE WHEN status = 'queued' THEN 'aborted' ELSE 'aborting' END,
                    error = $2, touched_at = clock_timestamp(),
                    -- Unconditionally, so a `result` this abort did not write can
                    -- never be half of the sweeper's marker pair. Left in place
                    -- for a `running` row, a foreign SQL writer that had planted
                    -- `"pgqueue:swept"` there let any caller complete the pair
                    -- with `abort_job(id, "swept")` — and the sweeper then read
                    -- the operator's abort as its own recovery request and
                    -- requeued the job to run again. No library path leaves a
                    -- meaningful `result` on a `queued` or `running` row: the
                    -- insert leaves it NULL and every requeue clears it, so
                    -- clearing it here costs nothing.
                    result = NULL,
                    completed_at = CASE WHEN status = 'queued' THEN clock_timestamp() ELSE completed_at END,
                    expires_at = CASE WHEN status = 'queued' AND failed_ttl_ms IS NOT NULL
                        THEN clock_timestamp() + (failed_ttl_ms * interval '1 millisecond') ELSE expires_at END
                FROM candidate c
                WHERE j.id = c.id
                RETURNING status, awaited
            )
            SELECT status,
                   (CASE WHEN status = 'aborted' AND awaited THEN pg_notify($4, $5) END) IS NULL
                       AS notify_skipped
            FROM updated
            "#,
        )
        .bind(id)
        .bind(reason.as_ref())
        .bind(&self.name)
        .bind(&self.done_channel)
        .bind(payload)
        .bind(SWEPT)
        .bind(swept_marker());
        let row = match deadline {
            // One statement commits on its own, without the round trips of a transaction.
            None => abort.fetch_optional(&self.pool).await?,
            // In a transaction of its own because the bound is transaction-local. A refused wait rolls it back with
            // nothing written.
            Some(deadline) => {
                let mut connection = PoolConnectionGuard::new(self.pool.acquire().await?);
                let mut transaction = connection.begin_transaction().await?;
                bound_lock_waits(&mut transaction, deadline).await?.ok_or(Error::WaitTimeout)?;
                let row = abort.fetch_optional(&mut *transaction).await?;
                transaction.commit().await?;
                row
            }
        };

        let Some(row) = row else {
            return Ok(false);
        };
        if row.status == "aborted" {
            self.counters.record_abort();
        }
        tracing::debug!(job.id = %id, status = %row.status, queue = %self.name, "abort requested");
        Ok(true)
    }

    pub(crate) async fn retry_job_occurrence(&self, id: Uuid, reason: &str) -> Result<Option<Uuid>, Error> {
        self.retry_job_occurrence_within(id, reason, None).await
    }

    /// [`Database::retry_job_occurrence`], with its lock waits ended on the server at `deadline` when one is given, as
    /// [`Database::abort_within`] ends the abort's and with the same reports. Besides the source row's lock, the retry
    /// waits for its dedupe key's, which a caller transaction that enqueued the same key holds until it ends.
    pub(crate) async fn retry_job_occurrence_within(
        &self,
        id: Uuid,
        reason: &str,
        deadline: Option<tokio::time::Instant>,
    ) -> Result<Option<Uuid>, Error> {
        // The reason is stored in the fresh occurrence's `error` column; see
        // `Database::abort` for why a NUL is refused here rather than left to
        // become a `22021`.
        validate_finalization(None, Some(reason))?;
        let reason = truncate_stored_error(reason);
        // A cron occurrence's dedupe key belongs to the schedule loop's
        // dedupe: carrying it onto a manual retry would collide with the
        // next scheduled occurrence and silently refuse the retry, so cron
        // retries run as keyless one-offs.
        let mut connection = PoolConnectionGuard::new(self.pool.acquire().await?);
        let mut tx = connection.begin_transaction().await?;
        if let Some(deadline) = deadline {
            bound_lock_waits(&mut tx, deadline).await?.ok_or(Error::WaitTimeout)?;
        }
        let new_id = sqlx::query_scalar::<_, Uuid>(
            r#"
            WITH source AS MATERIALIZED (
                UPDATE pgqueue.jobs SET retried_at = now()
                WHERE id = $1 AND queue = $3
                  AND status IN ('complete', 'failed', 'aborted') AND retried_at IS NULL
                  -- The fresh occurrence is inserted with `max_attempts =
                  -- attempts + 1`, and `jobs_attempts_range_check` caps
                  -- `max_attempts` at 2147483646: a source already at that
                  -- ceiling has no room for the one extra attempt a retry
                  -- grants, so it is refused like any other unretryable row.
                  AND attempts < 2147483646
                RETURNING queue, name, payload,
                          CASE WHEN kind = 'cron' THEN NULL
                               ELSE dedupe_key END AS dedupe_key,
                          priority, attempts, refunds, timeout_ms, retry_delay_ms, backoff,
                          result_ttl_ms, failed_ttl_ms, meta, kind, cron_expr
            ), locked AS MATERIALIZED (
                SELECT pg_advisory_xact_lock($4,
                    hashtext(length(queue)::text || ':' || queue || dedupe_key))
                FROM source WHERE dedupe_key IS NOT NULL
            ), wall_clock AS MATERIALIZED (
                SELECT clock_timestamp() AS current
                FROM source LEFT JOIN locked ON true
            )
            INSERT INTO pgqueue.jobs (
                queue, name, payload, dedupe_key, priority, attempts, refunds,
                max_attempts, timeout_ms, retry_delay_ms, backoff,
                result_ttl_ms, failed_ttl_ms, scheduled_at, enqueued_at, meta, error, kind, cron_expr
            )
            -- The refunds come along with the counter they discount, so the
            -- fresh occurrence's backoff and `JobContext::attempt` continue
            -- from the attempts the source actually spent.
            SELECT queue, name, payload, dedupe_key, priority, attempts, refunds,
                   attempts + 1, timeout_ms, retry_delay_ms, backoff,
                   result_ttl_ms, failed_ttl_ms, wall_clock.current, wall_clock.current, meta, $2, kind,
                   cron_expr
            FROM source JOIN wall_clock ON true
            ON CONFLICT (queue, dedupe_key) WHERE dedupe_key IS NOT NULL
                AND status IN ('queued', 'running', 'aborting') DO NOTHING
            RETURNING id
            "#,
        )
        .bind(id)
        .bind(reason.as_ref())
        .bind(&self.name)
        .bind(self.dedupe_enqueue_lock_key)
        .fetch_optional(&mut *tx)
        .await?;
        if new_id.is_some() {
            self.notify(&mut tx, &self.notify_channel, "enqueue").await?;
            tx.commit().await?;
            self.counters.record_retry();
        } else {
            tx.rollback().await?;
        }
        Ok(new_id)
    }
}

impl Database {
    /// Claims jobs for a custom consumer. Like the worker path, this requires a
    /// live, accepting `pgqueue.workers` lease for `worker_id`: without one the
    /// sweeper would treat the claim as abandoned and hand the job to someone
    /// else while it is still running.
    pub(crate) async fn dequeue_consumer(&self, limit: i64, worker_id: Uuid) -> Result<Vec<JobRow>, Error> {
        Ok(self.dequeue_inner(limit, worker_id, true, false).await?.jobs)
    }

    /// Claims jobs without requiring a lease. Only [`crate::__test_support`]
    /// reaches this; every supported entry point goes through a lease-checked
    /// path.
    #[cfg(feature = "_test")]
    pub(crate) async fn dequeue_unleased(&self, limit: i64, worker_id: Uuid) -> Result<Vec<JobRow>, Error> {
        Ok(self.dequeue_inner(limit, worker_id, false, false).await?.jobs)
    }

    pub(crate) async fn dequeue_worker(&self, limit: i64, worker_id: Uuid) -> Result<DatabaseDequeueBatch, Error> {
        self.dequeue_inner(limit, worker_id, true, true).await
    }

    async fn dequeue_inner(
        &self,
        limit: i64,
        worker_id: Uuid,
        require_open_intake: bool,
        probe_on_underfill: bool,
    ) -> Result<DatabaseDequeueBatch, Error> {
        if limit <= 0 {
            return Err(Error::Config("dequeue limit must be greater than zero".into()));
        }

        let mut connection = PoolConnectionGuard::new(self.pool.acquire().await?);
        let mut transaction = connection.begin_transaction().await?;
        let claim = sqlx::query_as::<_, JobRow>(DEQUEUE_CLAIM_SQL)
            .bind(&self.name)
            .bind(self.priorities.0)
            .bind(self.priorities.1)
            .bind(limit)
            .bind(worker_id)
            .bind(require_open_intake)
            .bind(self.claim_resolution_lock_key)
            .fetch_all(&mut *transaction)
            .await;
        let mut jobs = match claim {
            Ok(jobs) => jobs,
            Err(error) => {
                // Return the claim failure, not the rollback's. A claim that
                // failed because the connection broke fails the rollback the
                // same way, and that second error names only the teardown —
                // propagating it would replace the one diagnostic the operator
                // needs with "connection closed". Dropping the transaction
                // rolls it back regardless, so nothing leaks by ignoring this.
                if let Err(rollback) = transaction.rollback().await {
                    tracing::debug!(queue = %self.name, error = %rollback, "dequeue rollback failed");
                }
                return Err(error.into());
            }
        };
        // From the moment the COMMIT is sent until these rows are returned, the
        // claim can be *ours without us knowing it*: the server may commit
        // before its acknowledgement is lost, and the decoded rows here are the
        // only record of which rows that covers. Losing them left rows owned by
        // a live, heartbeating worker that never learned of them — beyond its
        // abort loop (they are in no in-flight registry) and beyond the
        // sweeper's live-owner cooperative window alike, until the process
        // exited. The guard hands the claims to the resolver however that loss
        // happens: a commit that *returns* an error, and equally a future
        // dropped mid-commit or mid-probe — the worker's own operation deadline
        // cancels a wedged dequeue exactly there, and a custom consumer may
        // drop its dequeue future at any await. Only the return of the batch,
        // after which no await remains, disarms it.
        let guard = UnacknowledgedClaimGuard {
            context: self.recovery_context(),
            worker_id,
            claims: jobs
                .iter()
                .map(|job| DatabaseUnacknowledgedClaim {
                    id: job.id,
                    attempts: job.attempts,
                    started_at: job.started_at,
                })
                .collect(),
        };
        transaction.commit().await?;
        drop(connection);

        // The underfilled-batch probe is its own statement, run after the
        // decoded claim commits. It needs no consistency with the batch, and
        // folding it into the statement above would keep its transaction —
        // and the `FOR UPDATE` row locks it holds — open across two more scans
        // before the claim commits.
        //
        // Only the worker fetch loop consumes the probe: it drives demand from
        // `intake_open` and `work_available`. The custom-consumer path would
        // pay a second round trip per dequeue for values it never reads — and,
        // on an empty batch, turn a failure of this purely diagnostic query
        // into a hard error.
        let batch_underfilled = i64::try_from(jobs.len()).is_ok_and(|fetched| fetched < limit);
        let probe = if probe_on_underfill && batch_underfilled {
            let probe = sqlx::query_as::<_, DatabaseDequeueProbe>(
                r#"
                SELECT
                    EXISTS (
                        SELECT 1 FROM pgqueue.workers
                        WHERE id = $2 AND queue = $1
                          AND accepting AND expires_at > now()
                    ) AS intake_open,
                    EXISTS (
                        SELECT 1 FROM pgqueue.jobs job
                        WHERE job.queue = $1 AND job.status = 'queued'
                          AND job.scheduled_at <= now()
                          AND job.priority BETWEEN $3 AND $4
                    ) AS work_available
                "#,
            )
            .bind(&self.name)
            .bind(worker_id)
            .bind(self.priorities.0)
            .bind(self.priorities.1)
            .fetch_one(&self.pool)
            .await;
            resolve_post_commit_probe(&self.name, worker_id, jobs.len(), probe)?
        } else {
            DatabaseDequeueProbe { intake_open: true, work_available: false }
        };

        jobs.sort_by_key(|job| (job.priority, job.scheduled_at, job.id));
        // No await remains between here and the caller receiving the batch, so
        // the committed claim can no longer be lost to a dropped future.
        guard.disarm();
        Ok(DatabaseDequeueBatch { jobs, intake_open: probe.intake_open, work_available: probe.work_available })
    }

    /// Finishes the attempt `job` names, if it still owns its row.
    ///
    /// Without a `deadline` this is the hot path's one autocommit round trip, which waits on the row's lock for as
    /// long as its caller lets it. With one — a worker retrying a finalization that has already failed once — the
    /// statement runs under [`bounded_write`], so a retry its caller abandons behind a row lock gives its connection
    /// back instead of keeping it until the lock holder lets go. The first try does not pay the bound's round trips:
    /// left behind a lock it parks one connection, as a black-holed socket does, and the retries park none.
    pub(crate) async fn finish(
        &self,
        job: &JobRow,
        status: JobStatus,
        result: Option<Value>,
        error: Option<&str>,
        deadline: Option<WriteDeadline>,
    ) -> Result<bool, Error> {
        self.ensure_owns(job)?;
        validate_finalization(result.as_ref(), error)?;
        let error = error.map(truncate_stored_error);
        self.finish_with_guards(AttemptGuard::from(job), status, &result, error.as_deref(), deadline).await
    }

    /// Requeues a batch of abandoned, sweeper-marked attempts in one statement.
    /// The per-row attempt/worker/stuckness guards and retry delays ride along
    /// through `unnest`, exactly as phase one's abort marking does, and so does
    /// its `SKIP LOCKED`: a row another transaction holds locked waits for a
    /// later pass rather than stalling the whole batch (see the sweeper's
    /// `recover_stuck_jobs`).
    pub(crate) async fn retry_swept_abandoned_batch(&self, jobs: &[&DatabaseStuckJob]) -> Result<Vec<Uuid>, Error> {
        if jobs.is_empty() {
            return Ok(Vec::new());
        }
        let ids = jobs.iter().map(|job| job.id).collect::<Vec<_>>();
        let attempts = jobs.iter().map(|job| job.attempts).collect::<Vec<_>>();
        let worker_ids = jobs.iter().map(|job| job.worker_id).collect::<Vec<_>>();
        let delays = jobs.iter().map(|job| duration_to_ms(job.next_retry_delay())).collect::<Vec<_>>();
        let requeued = sqlx::query_scalar::<_, Uuid>(
            r#"
            WITH requested AS (
                SELECT *
                FROM unnest($1::uuid[], $2::integer[], $3::uuid[], $4::bigint[])
                    AS t(id, attempts, worker_id, delay_ms)
            ),
            candidate AS (
                SELECT j.id, r.delay_ms
                FROM pgqueue.jobs j
                JOIN requested r ON r.id = j.id
                WHERE j.queue = $5
                  AND j.status = 'aborting' AND j.error = $6 AND j.result = $7
                  AND j.attempts = r.attempts
                  AND j.worker_id IS NOT DISTINCT FROM r.worker_id
                  AND j.attempts < j.max_attempts
                  -- No `pgqueue.job_is_stuck` here, deliberately: the marker
                  -- pair this WHERE already requires *is* the stuckness
                  -- adjudication, made by the pass that marked the row — and
                  -- that mark stamped `touched_at`, the clock the function's
                  -- second trigger reads, so re-deriving stuckness would hold
                  -- an untimed marked row for a further grace even after its
                  -- owner's lease row was purged. Liveness is the live-lease
                  -- exclusion below; timing is the window clause after it.
                  AND NOT EXISTS (
                      SELECT 1 FROM pgqueue.workers w
                      WHERE w.id = j.worker_id AND w.queue = j.queue
                        AND w.expires_at > now())
                  -- The cooperative window, measured from the abort mark phase
                  -- one stamped into `touched_at`: an owner whose lease row is
                  -- still on disk — lapsed is not gone — keeps the whole
                  -- `sweep_grace` from the mark to end the attempt itself,
                  -- however quickly a drain of a full batch repeats this pass.
                  -- Only a lease row the purge removed (expired at least twice
                  -- the grace ago) or that never existed skips the wait, which
                  -- is the documented owner-gone path.
                  AND (NOT EXISTS (
                          SELECT 1 FROM pgqueue.workers gone
                          WHERE gone.id = j.worker_id AND gone.queue = j.queue)
                       OR j.touched_at + ($8::bigint * interval '1 millisecond') <= now())
                FOR UPDATE OF j SKIP LOCKED
            ),
            requeued AS (
                UPDATE pgqueue.jobs j
                SET status = 'queued',
                    scheduled_at = CASE WHEN c.delay_ms = 0 THEN j.scheduled_at
                        ELSE now() + (c.delay_ms * interval '1 millisecond') END,
                    completed_at = NULL, started_at = NULL,
                    -- The attempt is nobody's from here on. Clearing the owner
                    -- is what tells a presumed-dead worker that is in fact
                    -- still running the handler that the attempt was taken
                    -- from it: `attempts` is unchanged, so `aborting_of` has
                    -- nothing else to see the loss by, and the attempt would
                    -- keep its processor slot — and keep producing side
                    -- effects — until it returned on its own. A queued row
                    -- advertising an owner is wrong for the dashboard too.
                    worker_id = NULL,
                    touched_at = now(), expires_at = NULL, result = NULL
                -- By id alone: `candidate` holds every row it returns,
                -- re-checked on the row's latest version, so none can change
                -- before this writes it.
                FROM candidate c
                WHERE j.id = c.id
                RETURNING j.id, j.scheduled_at
            )
            -- The lateral keeps the wakeup inside this statement's transaction,
            -- so it is emitted exactly when the requeue commits, and only when
            -- a requeued row is due now — a row retried with a delay wakes
            -- nobody, as in `insert_job`. The aggregate is uncorrelated, so the
            -- planner evaluates it once for the whole batch rather than per
            -- row; one wakeup is enough, because every idle fetcher re-polls on
            -- it, and a one-row aggregate keeps every requeued id in the join.
            SELECT requeued.id
            FROM requeued
            CROSS JOIN LATERAL (
                SELECT CASE WHEN bool_or(due.scheduled_at <= now()) THEN pg_notify($9, 'enqueue') END
                FROM requeued due
            ) AS notified
            "#,
        )
        .bind(&ids)
        .bind(&attempts)
        .bind(&worker_ids as &[Option<Uuid>])
        .bind(&delays)
        .bind(&self.name)
        .bind(SWEPT)
        .bind(swept_marker())
        .bind(duration_to_ms(self.sweep_grace))
        .bind(&self.notify_channel)
        .fetch_all(&self.pool)
        .await?;
        for _ in &requeued {
            self.counters.record_retry();
        }
        Ok(requeued)
    }

    /// Aborts a batch of abandoned attempts in one statement. Rows whose
    /// retention deletes immediately are removed instead of updated, matching
    /// the single-row finish path. A row another transaction holds locked is
    /// skipped for a later pass, as [`Database::retry_swept_abandoned_batch`]
    /// skips one.
    pub(crate) async fn abort_stuck_abandoned_batch(&self, jobs: &[&DatabaseStuckJob]) -> Result<Vec<Uuid>, Error> {
        if jobs.is_empty() {
            return Ok(Vec::new());
        }
        let ids = jobs.iter().map(|job| job.id).collect::<Vec<_>>();
        let attempts = jobs.iter().map(|job| job.attempts).collect::<Vec<_>>();
        let worker_ids = jobs.iter().map(|job| job.worker_id).collect::<Vec<_>>();
        let finished = sqlx::query_scalar::<_, Uuid>(finish_rows_sql!(
            r#"requested AS (
                SELECT *
                FROM unnest($1::uuid[], $2::integer[], $3::uuid[])
                    AS t(id, attempts, worker_id)
            ),
            candidate AS (
                SELECT j.id, j.failed_ttl_ms AS ttl_ms
                FROM pgqueue.jobs j
                JOIN requested r ON r.id = j.id
                WHERE j.queue = $4
                  AND j.status IN ('running', 'aborting')
                  AND j.attempts = r.attempts
                  AND j.worker_id IS NOT DISTINCT FROM r.worker_id
                  -- A subquery rather than a join, like the sibling batch
                  -- statements: one index lookup per row of a batch already
                  -- keyed by id, and no outer join for `FOR UPDATE OF j` to
                  -- interact with. A row carrying the sweeper's marker pair
                  -- passes on the marker alone: the mark *is* the stuckness
                  -- adjudication, and it stamped `touched_at` — the clock the
                  -- function's second trigger reads — so re-deriving stuckness
                  -- would hold an untimed marked row for a further grace even
                  -- after its owner's lease row was purged.
                  AND (pgqueue.job_is_stuck(j, $5::bigint, (
                          SELECT lease.expires_at FROM pgqueue.workers AS lease
                          WHERE lease.id = j.worker_id AND lease.queue = j.queue))
                       OR (j.error IS NOT DISTINCT FROM $7 AND j.result IS NOT DISTINCT FROM $8))
                  AND (
                      j.dedupe_key IS NULL
                      OR NOT EXISTS (
                          SELECT 1 FROM pgqueue.workers w
                          WHERE w.id = j.worker_id AND w.queue = j.queue
                            AND w.expires_at > now())
                  )
                  -- The same cooperative window the retry batch grants,
                  -- measured from the abort request in `touched_at` — the
                  -- sweeper's phase-one mark and `Queue::abort_job` both stamp
                  -- it — so an owner whose lease row is still on disk keeps the
                  -- whole `sweep_grace` to finish the abort itself before the
                  -- row is taken away.
                  AND (NOT EXISTS (
                          SELECT 1 FROM pgqueue.workers gone
                          WHERE gone.id = j.worker_id AND gone.queue = j.queue)
                       OR j.touched_at + ($5::bigint * interval '1 millisecond') <= now())
                -- Skipped rather than waited for, as in the requeue batch.
                FOR UPDATE OF j SKIP LOCKED
            )"#,
            // Clear the sweeper's marker pair, as every terminal abort does.
            r#"status = 'aborted', result = NULL"#,
            notify_each_finished_sql!("$6", "aborted")
        ))
        .bind(&ids)
        .bind(&attempts)
        .bind(&worker_ids as &[Option<Uuid>])
        .bind(&self.name)
        .bind(duration_to_ms(self.sweep_grace))
        .bind(&self.done_channel)
        .bind(SWEPT)
        .bind(swept_marker())
        .fetch_all(&self.pool)
        .await?;
        for id in &finished {
            self.counters.record_abort();
            tracing::debug!(job.id = %id, status = "aborted", queue = %self.name, "finished");
        }
        Ok(finished)
    }

    async fn finish_with_guards(
        &self,
        attempt: AttemptGuard,
        status: JobStatus,
        result: &Option<Value>,
        error: Option<&str>,
        deadline: Option<WriteDeadline>,
    ) -> Result<bool, Error> {
        if !status.is_terminal() {
            return Err(Error::Config("finish requires a terminal job status".into()));
        }
        // An owner may still finish an attempt the sweeper marked `aborting`
        // underneath it, so the guard accepts that row too: unconditionally
        // when the owner is itself reporting the abort, and otherwise only
        // while it still carries the sweeper's markers. Folding both into one
        // predicate keeps finishing a swept attempt to a single round trip.
        let owner_reports_abort = status == JobStatus::Aborted;
        let status = status.as_str();
        let payload = format!(r#"{{"id":"{}","status":"{status}"}}"#, attempt.id);

        let query = sqlx::query_as::<_, FinishResult>(FINISH_GUARDED_SQL)
            .bind(attempt.id)
            .bind(status)
            .bind(result)
            .bind(error)
            .bind(attempt.attempts)
            .bind(attempt.worker_id)
            .bind(&self.name)
            .bind(owner_reports_abort)
            .bind(SWEPT)
            .bind(swept_marker())
            .bind(&self.done_channel)
            .bind(payload);
        let row = self.fetch_one_write(query, deadline).await?;
        if !row.finished {
            return Ok(false);
        }

        match status {
            "complete" => self.counters.record_complete(),
            "failed" => self.counters.record_failed(),
            _ => self.counters.record_abort(),
        }
        tracing::debug!(job.id = %attempt.id, status, queue = %self.name, "finished");
        Ok(true)
    }

    /// `deadline` is a worker's retried finalization's; see [`Database::finish`].
    pub(crate) async fn retry(
        &self,
        job: &JobRow,
        error: &str,
        deadline: Option<WriteDeadline>,
    ) -> Result<bool, Error> {
        self.ensure_owns(job)?;
        validate_finalization(None, Some(error))?;
        let error = truncate_stored_error(error);
        if !job.is_retryable() {
            return Ok(false);
        }
        let delay = job.next_retry_delay();
        // `allow_swept_abort` makes this the whole retry story for a consumer
        // holding the attempt capability: a row the sweeper marked for
        // stuck-recovery mid-attempt is requeued exactly as a running one is,
        // converting the recovery request into this retry with the caller's
        // error — the same conversion the worker's own finalization performs.
        // The marker pair is what keeps a *user* abort out of reach: an
        // `aborting` row without it matches nothing here, so a cancellation is
        // never resurrected as a retry.
        let guards = DatabaseRequeueGuards {
            allow_running: true,
            allow_swept_abort: true,
            refund_attempt: false,
            close_intake: false,
        };
        let retried =
            self.requeue_guarded(AttemptGuard::from(job), Some(error.as_ref()), delay, guards, deadline).await?;
        if retried {
            self.counters.record_retry();
            // `attempt` is the spent count `delay` grew from — the number
            // `JobContext::attempt` and the worker's `job.run` span report —
            // and `claim` the raw counter the requeue was fenced on.
            tracing::debug!(
                job.id = %job.id,
                attempt = crate::job::spent_attempts(job.attempts, job.refunds),
                claim = job.attempts,
                delay_ms = duration_to_ms(delay), queue = %self.name,
                "retry scheduled"
            );
        }
        Ok(retried)
    }

    /// Requeues an attempt the worker gave up on at shutdown, refunding the
    /// attempt. `error` is stored so the reason the attempt ended stays visible.
    /// `deadline` is a worker's retried finalization's; see [`Database::finish`].
    pub(crate) async fn requeue_shutdown(
        &self,
        job: &JobRow,
        error: &str,
        deadline: Option<WriteDeadline>,
    ) -> Result<bool, Error> {
        // As in `retry_swept`: refuse a reason PostgreSQL can never store here,
        // at the boundary, rather than let it become a `22021` that the
        // shutdown drain then retries until its budget runs out.
        self.ensure_owns(job)?;
        validate_finalization(None, Some(error))?;
        let error = truncate_stored_error(error);
        let guards = DatabaseRequeueGuards {
            allow_running: true,
            allow_swept_abort: true,
            refund_attempt: true,
            close_intake: true,
        };
        let retried = self
            .requeue_guarded(AttemptGuard::from(job), Some(error.as_ref()), Duration::ZERO, guards, deadline)
            .await?;
        if retried {
            self.counters.record_retry();
        }
        Ok(retried)
    }

    /// Requeues an attempt claimed by a worker with no handler for the job's
    /// name, refunding the attempt: a worker handles every job name in its
    /// queue, so a claim landing here is a contract violation — most often a
    /// rolling deploy, where a new binary enqueues a job type the not-yet
    /// replaced workers do not register. The refund keeps the bounce from
    /// spending the job's attempts (at the default `max_attempts = 1` a burnt
    /// attempt would fail the job outright), and the delay keeps the same
    /// worker from reclaiming the row in a tight loop while the fleet catches
    /// up. The stored error keeps the reason visible on the row until a worker
    /// that registers the handler picks it up. `deadline` is a worker's retried
    /// finalization's; see [`Database::finish`].
    pub(crate) async fn requeue_unhandled(&self, job: &JobRow, deadline: Option<WriteDeadline>) -> Result<bool, Error> {
        self.ensure_owns(job)?;
        // Never NUL: `JobRequest::validate` refuses NUL in names before any
        // row is written, so this error is storable by construction.
        let error = format!("no handler registered for job {:?}", job.name);
        let guards = DatabaseRequeueGuards {
            allow_running: true,
            allow_swept_abort: true,
            refund_attempt: true,
            close_intake: false,
        };
        // The job's own retry delay is not usable here: it defaults to zero,
        // which would respin the claim as fast as the fetch loop can run.
        let delay = UNHANDLED_REQUEUE_DELAY + UNHANDLED_REQUEUE_JITTER.mul_f64(rand::random::<f64>());
        self.requeue_guarded(AttemptGuard::from(job), Some(&error), delay, guards, deadline).await
    }

    /// Puts the job back to `queued` under the given guards, as one
    /// statement: the guarded update, the shutdown intake close, and the
    /// enqueue notification travel together so every requeue on the worker
    /// hot path costs a single round trip. `error` replaces the stored error
    /// when given; a `None` keeps the sweeper's marker in place.
    ///
    /// `refund_attempt` raises `max_attempts` rather than lowering `attempts`,
    /// because `attempts` is what every guard here and in recovery matches a
    /// claim on: decrementing it would let a displaced attempt's writes land on
    /// the row again. The refund is therefore permanent and cumulative, and it
    /// shows. A job configured with three attempts that four rolling restarts
    /// caught mid-flight carries `attempts = 4, max_attempts = 7` afterwards, so
    /// the dashboard renders `4/7` where an untouched job of the same
    /// configuration renders `0/3`. What the pair *means* is unchanged — the
    /// difference is still the three tries the job was given, none of them spent
    /// on a shutdown — but neither numeral is the one it was enqueued with. The
    /// row's `refunds = 4` is what tells the two apart wherever the attempts
    /// *spent* matter: the retry backoff and `JobContext::attempt`.
    async fn requeue_guarded(
        &self,
        attempt: AttemptGuard,
        error: Option<&str>,
        delay: Duration,
        guards: DatabaseRequeueGuards,
        deadline: Option<WriteDeadline>,
    ) -> Result<bool, Error> {
        let query = sqlx::query_as::<_, RequeueResult>(REQUEUE_GUARDED_SQL)
            .bind(attempt.id)
            .bind(duration_to_ms(delay))
            .bind(error)
            .bind(attempt.attempts)
            .bind(attempt.worker_id)
            .bind(&self.name)
            .bind(guards.refund_attempt)
            .bind(guards.allow_running)
            .bind(guards.allow_swept_abort)
            .bind(SWEPT)
            .bind(swept_marker())
            .bind(&self.notify_channel)
            .bind(guards.close_intake)
            .bind(None::<jiff_sqlx::Timestamp>);
        Ok(self.fetch_one_write(query, deadline).await?.requeued)
    }

    /// Runs one of a worker's guarded writes: as one autocommit round trip, or under `deadline` through
    /// [`bounded_write`] (see [`Database::finish`] for which is which).
    async fn fetch_one_write<O>(
        &self,
        query: sqlx::query::QueryAs<'_, Postgres, O, sqlx::postgres::PgArguments>,
        deadline: Option<WriteDeadline>,
    ) -> Result<O, Error>
    where
        O: Send + Unpin + for<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow>,
    {
        let Some(deadline) = deadline else {
            return Ok(query.fetch_one(&self.pool).await?);
        };
        let mut connection = PoolConnectionGuard::new(self.pool.acquire().await?);
        bounded_write(&mut connection, deadline, async move |connection| query.fetch_one(connection).await).await
    }
}

/// The dequeue claim. Every ready row of the queue is a candidate — a worker
/// handles every job name in its queue, and a claimed row with no handler is
/// given back by [`Database::requeue_unhandled`] — so one ordered walk of
/// `jobs_dequeue_idx` under `FOR UPDATE ... SKIP LOCKED` is optimal: it steps
/// over rows another claim currently holds and keeps going until the batch is
/// full. With no name predicate in the statement and no name-leading dequeue
/// index in the schema, this walk is the only *index-ordered* plan; the
/// claim's plan-shape test is what pins the planner to it over a
/// materialize-and-sort alternative under the generic plan.
///
/// The `UPDATE` is keyed by the batch's ids alone, as an array the primary key
/// is probed with. Repeating the walk's predicates there re-checked nothing:
/// `candidates` holds every row it returns `FOR UPDATE`, READ COMMITTED's
/// EvalPlanQual already re-verified them on the row's latest version, and no
/// other transaction can change a locked row before this statement writes it —
/// `queue` is never updated at all. What the repetition did do was hand the
/// planner the queued-only partial indexes, and `queue = $1` alone the
/// queue-leading dashboard ones, as a way to read the update's target. Whenever
/// the statistics put that set at about a row — a healthy queue's steady state,
/// a queue the last `ANALYZE` never sampled, a table never analyzed — it drove
/// the update from a scan of the whole ready backlog and re-scanned the batch
/// once per ready row, O(backlog × batch) for every claim until an `ANALYZE`
/// happened to run. An array rather than a join on `candidates`, because the
/// planner sizes an array parameter at a fixed handful of elements instead of
/// following the `LIMIT`, so a large batch cannot tip the update into hashing a
/// sequential scan of the table. `RETURNING` then yields id order, which costs
/// nothing: [`Database::dequeue_inner`] sorts the batch.
pub(crate) const DEQUEUE_CLAIM_SQL: &str = r#"
    WITH claim_lock AS MATERIALIZED (
        -- Transaction-scoped, and evaluated before any candidate row can qualify: the unacknowledged-claim resolver
        -- takes the same `(namespace, hashtext(worker_id))` pair exclusively at the top of its own transaction, so a
        -- resolver racing this statement's COMMIT is ordered behind the transaction instead of reading the pre-commit
        -- snapshot, matching nothing, and settling claims that commit then makes real — rows `running` under a worker
        -- that never learned of them.
        --
        -- Shared, because claims only need ordering against resolvers, not against each other: `SKIP LOCKED` already
        -- keeps concurrent claims apart. And tried rather than waited for, because a claim that waited here waited on
        -- whatever held the lock. A claim whose session sits idle in transaction after its COMMIT was lost holds it
        -- until the server ends that session — half a minute on, under the bound every transaction of the queue's own
        -- carries (see `BEGIN_BOUNDED_TRANSACTION_SQL`), and hours without it; the resolver for that claim queues
        -- behind it, and PostgreSQL queues every later request behind a waiting one. So every later claim of this
        -- worker waited too, was abandoned at its deadline, and kept its pooled connection, because sqlx pings a
        -- connection before pooling it again and the ping waits out the blocked statement. Refused instead, a claim
        -- takes nothing and reports an empty batch, which every caller already retries — and a resolver's queued
        -- request refusing the claims that would otherwise keep arriving is what lets it in at all; see
        -- `take_claim_resolution_lock`.
        SELECT pg_try_advisory_xact_lock_shared($7, hashtext($5::text)) AS locked
    ),
    candidates AS (
        SELECT job.id FROM pgqueue.jobs job
        WHERE job.queue = $1 AND job.status = 'queued'
          AND job.scheduled_at <= now()
          AND job.priority BETWEEN $2 AND $3
          -- Forces `claim_lock` before the scan, and claims nothing while a resolver of this worker holds or awaits it.
          AND (SELECT locked FROM claim_lock)
        ORDER BY job.priority, job.scheduled_at, job.id
        LIMIT $4
        FOR UPDATE OF job SKIP LOCKED
    ), updated AS (
        UPDATE pgqueue.jobs job
        SET status = 'running', attempts = job.attempts + 1,
            started_at = now(), touched_at = now(), worker_id = $5
        -- By id alone; see the statement's documentation for why nothing else is repeated here.
        WHERE job.id = ANY (ARRAY(SELECT id FROM candidates))
          AND (NOT $6 OR EXISTS (
              SELECT 1 FROM pgqueue.workers worker
              WHERE worker.id = $5 AND worker.queue = $1
                AND worker.accepting AND worker.expires_at > now()
          ))
        RETURNING job.id, job.dedupe_key, job.queue, job.name,
                  job.payload, job.status, job.priority,
                  job.attempts, job.refunds, job.max_attempts, job.timeout_ms,
                  job.retry_delay_ms, job.backoff,
                  job.result_ttl_ms, job.failed_ttl_ms, job.scheduled_at, job.enqueued_at,
                  job.started_at, job.touched_at, job.completed_at,
                  job.expires_at, job.result, job.error, job.meta,
                  job.worker_id, job.kind, job.cron_expr, job.retried_at
    )
    SELECT id, dedupe_key, queue, name, payload,
           status, priority, attempts, refunds,
           max_attempts, timeout_ms, retry_delay_ms,
           backoff, result_ttl_ms, failed_ttl_ms, scheduled_at,
           enqueued_at, started_at, touched_at, completed_at, expires_at,
           result, error, meta, worker_id, kind, cron_expr, retried_at
    FROM updated
"#;

/// The guarded finish [`Database::finish_with_guards`] binds. One statement: the
/// guarded candidate is locked once, rows with an immediate-delete retention are
/// removed instead of updated, and the done notification fires only when a row
/// actually finished.
///
/// The candidate is found by primary key and by nothing else, so `id` is the
/// only guard written in a form an index can serve. The status guard used to be
/// written as plain comparisons, which imply `jobs_active_idx`'s predicate, and
/// whenever the statistics put the queue's in-flight rows at about one — the
/// last `ANALYZE` ran while the queue was idle, or never sampled it — that small
/// partial index was costed below the probe of a unique key, under the generic
/// plan as well as a custom one. The finish then read every in-flight row of
/// the queue and filtered on `id`: 128 buffers against the probe's 4 with 5,000
/// jobs running, on every completion until an `ANALYZE` happened to run. So
/// `status` is tested inside a `CASE`, from which the planner proves nothing.
/// And `queue` is compared with `IS NOT DISTINCT FROM` — `=`, on a column that is
/// never NULL, but in a form no index serves — because with the partial index
/// out of reach, `queue = $7` handed a custom plan for a queue the statistics
/// never sampled to whichever queue-leading index costed lowest, which read
/// every row the queue has. [`REQUEUE_GUARDED_SQL`],
/// [`ABORT_UNSETTLED_CLAIM_SQL`] and the abort poll, [`ABORT_POLL_SQL`], guard
/// the same way, for the same reason.
pub(crate) const FINISH_GUARDED_SQL: &str = finish_rows_sql!(
    // The retention that applies is the outcome's: the result clock
    // for `complete`, the failure clock for `failed` and `aborted`.
    r#"candidate AS (
        SELECT j.id,
               CASE WHEN $2 = 'complete' THEN j.result_ttl_ms ELSE j.failed_ttl_ms END AS ttl_ms
        FROM pgqueue.jobs j
        WHERE j.id = $1
          -- Spelled so that only `id` can reach an index; see `FINISH_GUARDED_SQL`.
          AND j.queue IS NOT DISTINCT FROM $7
          AND CASE j.status WHEN 'running' THEN true
                            WHEN 'aborting' THEN $8 OR (j.error = $9 AND j.result = $10)
                            ELSE false END
          AND j.attempts = $5 AND j.worker_id IS NOT DISTINCT FROM $6
        FOR UPDATE
    )"#,
    r#"status = $2, result = $3,
            error = CASE WHEN $2 = 'complete' THEN $4 ELSE COALESCE($4, j.error) END"#,
    // The one caller that needs a *decision* rather than the ids, and the
    // one whose payload is bound rather than built per row: the status is
    // the caller's, not a literal. The notification goes out only for a
    // row somebody is waiting on.
    r#"SELECT EXISTS (SELECT 1 FROM finished) AS finished,
           (SELECT pg_notify($11, $12) FROM finished WHERE finished.awaited) IS NULL
               AS notify_skipped"#
);

/// The guarded requeue every worker-side "give the attempt back" path binds —
/// [`Database::requeue_guarded`] and the unacknowledged-claim resolver — so the
/// two can never disagree about the guards. It reaches the row by primary key
/// alone, for the reason [`FINISH_GUARDED_SQL`] gives.
pub(crate) const REQUEUE_GUARDED_SQL: &str = r#"
            WITH requeued AS (
                UPDATE pgqueue.jobs j
                SET status = 'queued',
                    -- The cap is 2147483646 (`i32::MAX - 1`), the bound
                    -- `JobConfig::validate` and the schema's
                    -- `jobs_attempts_range_check` hold every writer to, so a
                    -- refunded row still satisfies `attempts < max_attempts`
                    -- while it is queued.
                    max_attempts = CASE WHEN $7
                        THEN LEAST(max_attempts::bigint + 1, 2147483646)::integer
                        ELSE max_attempts END,
                    -- Counted only when it raised `max_attempts`: a refund at
                    -- the ceiling grants nothing, so that claim stays spent.
                    -- `attempts - refunds` is what the retry backoff and
                    -- `JobContext::attempt` count, rather than every claim.
                    refunds = CASE WHEN $7 AND max_attempts < 2147483646
                        THEN refunds + 1
                        ELSE refunds END,
                    scheduled_at = CASE WHEN $2::bigint = 0 THEN scheduled_at
                        ELSE now() + ($2::bigint * interval '1 millisecond') END,
                    error = COALESCE($3, j.error),
                    completed_at = NULL, started_at = NULL,
                    -- The guard below reads the pre-update row, so clearing the
                    -- owner here is safe — and required: a `queued` row that
                    -- still names the worker that gave the attempt up is wrong
                    -- for `JobRow::worker_id` and for the dashboard, which
                    -- renders it as the job's owner. Matches
                    -- `retry_swept_abandoned_batch`.
                    worker_id = NULL,
                    touched_at = now(), expires_at = NULL, result = NULL
                WHERE j.id = $1
                  -- Spelled so that only `id` can reach an index; see `FINISH_GUARDED_SQL`.
                  AND j.queue IS NOT DISTINCT FROM $6
                  AND CASE j.status WHEN 'running' THEN $8
                                    WHEN 'aborting' THEN $9 AND j.error = $10 AND j.result = $11
                                    ELSE false END
                  AND j.attempts = $4 AND j.worker_id IS NOT DISTINCT FROM $5
                  -- The claim's own stamp, for a resolver whose claim may never have committed; see
                  -- `DatabaseUnacknowledgedClaim::started_at`. NULL for an attempt known to be committed, which the
                  -- three guards above already name uniquely.
                  AND ($14::timestamptz IS NULL OR j.started_at = $14)
                  -- A refund at the `max_attempts` ceiling cannot raise it, so
                  -- an attempt counter already there is refused rather than
                  -- requeued as a row whose next claim would violate the range
                  -- check; the callers' abort fallbacks finish such a row.
                  AND (CASE WHEN $7 THEN j.attempts < 2147483646
                       ELSE j.attempts < j.max_attempts END)
                RETURNING j.id, j.scheduled_at
            ),
            intake_closed AS (
                UPDATE pgqueue.workers w
                SET accepting = false, heartbeat_at = now()
                WHERE $13 AND w.id = $5 AND w.queue = $6
                RETURNING w.id
            )
            -- A retry scheduled for later wakes nobody: the fetch loop polls on
            -- its own interval for scheduled work, and every `NOTIFY` is a
            -- cluster-wide commit serialization point (see `insert_job`).
            SELECT EXISTS (SELECT 1 FROM requeued) AS requeued,
                   (SELECT pg_notify($12, 'enqueue') FROM requeued WHERE requeued.scheduled_at <= now())
                       IS NULL AS notify_skipped,
                   EXISTS (SELECT 1 FROM intake_closed) AS intake_closed
            "#;

/// Finishes `aborted` a claim the guarded requeue refused while the row still
/// belongs to it. Two resolvers share it: the unacknowledged-commit resolver
/// reaches it at the `attempts = max_attempts = 2147483646` ceiling, where the
/// refund has nothing left to grant, and the dropped-attempt resolver reaches
/// it for an exhausted final attempt or an abort awaiting acknowledgment. The
/// sweeper's exhausted recovery finishes such rows the same way and for the
/// same reason: nothing here ever saw a handler report an error, so `failed`
/// would be a lie, and leaving the row `running` under a live owner is the
/// exact orphan both resolvers exist to prevent. The guards make it a no-op
/// for every other refusal — a row that is terminal, someone else's, or was
/// never committed matches nothing.
///
/// Every `aborting` row the claim still owns is accepted, not only one bearing
/// the sweeper's marker pair. `REQUEUE_GUARDED_SQL` runs first and takes the
/// marked ones, so what reaches here `aborting` is overwhelmingly a
/// [`Queue::abort_job`](crate::Queue::abort_job) that landed while the claim was
/// unsettled — and restricting this fallback to the marker left exactly that row
/// unreachable. Nothing settled it: the requeue refuses it by design (a user
/// abort must never come back as a retry), this abort refused it too, and the
/// sweeper cannot take it while the owner's lease is live, so it sat `aborting`
/// under a heartbeating worker that never learned it owned it — holding its
/// dedupe key against every re-enqueue and cron occurrence, answering `false` to
/// every further `abort_job`, and hanging every waiter on it, until the process
/// exited.
///
/// The stored reason survives that case. A row the sweeper marked is finished
/// under `reason`, because its marker is internal bookkeeping rather than
/// anything an operator asked for; a row an operator aborted keeps the reason
/// they gave. That is the rule
/// [`Database::finish_with_guards`] already applies to
/// `finish(Aborted, None, None)`.
async fn abort_unsettled_claim(
    transaction: &mut sqlx::PgTransaction<'_>,
    queue: &str,
    done_channel: &str,
    worker_id: Option<Uuid>,
    claim: &DatabaseUnacknowledgedClaim,
    reason: &str,
) -> Result<bool, sqlx::Error> {
    let aborted = sqlx::query_scalar::<_, Uuid>(ABORT_UNSETTLED_CLAIM_SQL)
        .bind(claim.id)
        .bind(queue)
        .bind(claim.attempts)
        .bind(worker_id)
        .bind(SWEPT)
        .bind(swept_marker())
        .bind(reason)
        .bind(done_channel)
        .bind(claim.started_at.map(|started_at| started_at.to_sqlx()))
        .fetch_optional(&mut **transaction)
        .await?;
    Ok(aborted.is_some())
}

/// The statement [`abort_unsettled_claim`] binds. Its candidate is found by
/// primary key alone, for the reason [`FINISH_GUARDED_SQL`] gives.
pub(crate) const ABORT_UNSETTLED_CLAIM_SQL: &str = finish_rows_sql!(
    r#"candidate AS (
        SELECT j.id, j.failed_ttl_ms AS ttl_ms FROM pgqueue.jobs j
        WHERE j.id = $1
          -- Spelled so that only `id` can reach an index; see `FINISH_GUARDED_SQL`.
          AND j.queue IS NOT DISTINCT FROM $2
          AND CASE j.status WHEN 'running' THEN true WHEN 'aborting' THEN true ELSE false END
          AND j.attempts = $3 AND j.worker_id IS NOT DISTINCT FROM $4
          AND ($9::timestamptz IS NULL OR j.started_at = $9)
        FOR UPDATE
    )"#,
    r#"status = 'aborted', result = NULL,
            -- The pre-update row, so this reads the reason as it stands: an
            -- operator's abort keeps it, the sweeper's marker gives way.
            error = CASE WHEN j.status = 'running' OR (j.error = $5 AND j.result = $6)
                         THEN $7 ELSE j.error END"#,
    notify_each_finished_sql!("$8", "aborted")
);

/// How long a resolver whose first try at its lock was refused waits for it
/// before giving up the attempt. Claims commit in milliseconds, and once the
/// resolver is queued no new one starts; behind a claim stuck idle in
/// transaction, this is how long each attempt refuses the worker's claims.
const CLAIM_RESOLUTION_LOCK_WAIT: Duration = Duration::from_millis(250);

/// Takes `(key, hashtext(worker_id))` exclusively for `transaction`, ordering
/// the resolver strictly after every claim transaction of `worker_id` in
/// flight. Returns `false`, holding nothing, if they have not all resolved
/// within [`CLAIM_RESOLUTION_LOCK_WAIT`].
///
/// Why strictly after: the dequeue takes this pair shared and
/// transaction-scoped inside its claiming statement, so it can be taken
/// exclusively here only once an in-flight COMMIT has resolved either way. A
/// pass that skipped this and raced the COMMIT read the pre-commit snapshot,
/// matched neither guarded statement — a snapshot row that fails a predicate is
/// skipped, not waited on — and settled claims the commit then made real: rows
/// `running` under a live worker that never learned of them. The uuid is hashed
/// as its canonical lowercase text, which is what `$5::text` yields in the
/// claim.
///
/// Why tried first and then waited for only briefly: both pure strategies
/// failed. Waiting without bound, the resolver queued behind a claim whose
/// session sat idle in transaction for as long as the server kept it — hours
/// under default TCP keepalives, and still half a minute under the bound every
/// transaction of the queue's own now carries (see
/// [`BEGIN_BOUNDED_TRANSACTION_SQL`]) — and PostgreSQL refuses every later
/// shared try that conflicts with a queued request, so the live worker took
/// nothing for all that time. Only ever trying, it had no
/// priority at all: it got the lock only at an instant when no claim of the
/// worker was in flight, and claims that overlap — a consumer shared between
/// tasks, against a backlog — left no such instant, so the claim it was
/// resolving sat `running` under a live lease that nothing else recovers. A
/// bounded wait has both halves: once queued, the resolver refuses new claims,
/// the ones in flight drain, and it holds the lock within milliseconds; behind
/// a stuck session it gives up after the bound, having held the worker's claims
/// off for at most that long per attempt.
async fn take_claim_resolution_lock(
    transaction: &mut sqlx::PgTransaction<'_>,
    key: i32,
    worker_id: Uuid,
) -> Result<bool, sqlx::Error> {
    let worker = worker_id.to_string();
    let (locked, lock_timeout) = sqlx::query_as::<_, (bool, String)>(
        "SELECT pg_try_advisory_xact_lock($1, hashtext($2)), current_setting('lock_timeout')",
    )
    .bind(key)
    .bind(&worker)
    .fetch_one(&mut **transaction)
    .await?;
    if locked {
        return Ok(true);
    }
    // Transaction-local, so the rollback after a refusal puts the session's own setting back by itself.
    sqlx::query("SELECT set_config('lock_timeout', $1, true)")
        .bind(format!("{}ms", CLAIM_RESOLUTION_LOCK_WAIT.as_millis()))
        .execute(&mut **transaction)
        .await?;
    match sqlx::query("SELECT pg_advisory_xact_lock($1, hashtext($2))")
        .bind(key)
        .bind(&worker)
        .execute(&mut **transaction)
        .await
    {
        Ok(_) => {}
        Err(sqlx::Error::Database(error)) if error.code().as_deref() == Some(LOCK_NOT_AVAILABLE) => return Ok(false),
        Err(error) => return Err(error),
    }
    // The bound is for this wait only; the pass's own row locks wait as they would have after an immediate grant.
    sqlx::query("SELECT set_config('lock_timeout', $1, true)").bind(lock_timeout).execute(&mut **transaction).await?;
    Ok(true)
}

/// Requeues, with an attempt refund, every claim in `claims` that the server
/// really did commit for `worker_id`, clearing the list once the whole pass
/// commits so an interrupted pass retries intact. Returns how many rows were
/// actually reclaimed — a claim whose commit never landed matches no row and
/// counts nothing, and a committed claim whose refund is refused at the
/// attempt ceiling is finished `aborted` instead of being abandoned.
///
/// Returns `None`, having settled nothing, while a claim transaction of
/// `worker_id` is still in flight after [`CLAIM_RESOLUTION_LOCK_WAIT`]: the
/// pass cannot run until it resolves, and the caller tries again.
async fn requeue_unacknowledged_claims(
    context: &RecoveryContext,
    worker_id: Uuid,
    claims: &mut Vec<DatabaseUnacknowledgedClaim>,
) -> Result<Option<u64>, sqlx::Error> {
    if claims.is_empty() {
        return Ok(Some(0));
    }
    let mut requeued = 0;
    let mut aborted = 0;
    let mut connection = PoolConnectionGuard::new(context.pool.acquire().await?);
    let mut transaction = connection.begin_transaction().await?;
    if !take_claim_resolution_lock(&mut transaction, context.claim_lock_key, worker_id).await? {
        transaction.rollback().await?;
        return Ok(None);
    }
    // A caller can lock a committed claim's row independently of the claim transaction. Waiting on that row while
    // holding the exclusive resolution lock would refuse every other claim of this live worker indefinitely. Bound
    // the pass's row waits too: a refusal rolls back without clearing `claims`, releasing the gate until the retry.
    // Transaction-local, and only lowered, so the session's own shorter bound survives and no setting leaks out.
    sqlx::query(
        "SELECT set_config('lock_timeout', $1, true)
         FROM (SELECT current_setting('lock_timeout')::interval AS configured) setting
         WHERE configured = interval '0' OR configured > $1::interval",
    )
    .bind(format!("{}ms", CLAIM_RESOLUTION_LOCK_WAIT.as_millis()))
    .execute(&mut *transaction)
    .await?;
    for claim in claims.iter() {
        let row = sqlx::query_as::<_, RequeueResult>(REQUEUE_GUARDED_SQL)
            .bind(claim.id)
            // The attempt never ran, so its original schedule stands.
            .bind(0i64)
            .bind(Some(UNACKNOWLEDGED_CLAIM_ERROR))
            .bind(claim.attempts)
            .bind(Some(worker_id))
            .bind(&context.queue)
            // Refund: nothing was executed, so nothing was spent.
            .bind(true)
            // The row is `running` if the commit landed, or `aborting` under
            // the sweeper's markers if recovery noticed it first; both are
            // this worker's to give back.
            .bind(true)
            .bind(true)
            .bind(SWEPT)
            .bind(swept_marker())
            .bind(&context.notify_channel)
            // The worker is alive and healthy — the lost acknowledgement was
            // the connection's, not the process's — so its intake stays open.
            .bind(false)
            .bind(claim.started_at.map(|started_at| started_at.to_sqlx()))
            .fetch_one(&mut *transaction)
            .await?;
        if row.requeued {
            requeued += 1;
        } else {
            // A refusal is settled only once it is *explained*: usually the
            // row is terminal, someone else's, or was never committed — all
            // no-ops below — but a refund refused at the attempt ceiling
            // leaves a row this claim still owns, which must finish rather
            // than sit `running` under an owner that never learned of it.
            if abort_unsettled_claim(
                &mut transaction,
                &context.queue,
                &context.done_channel,
                Some(worker_id),
                claim,
                UNACKNOWLEDGED_CLAIM_ERROR,
            )
            .await?
            {
                aborted += 1;
            }
        }
    }
    transaction.commit().await?;
    for _ in 0..aborted {
        context.counters.record_abort();
    }
    claims.clear();
    Ok(Some(requeued))
}

/// Resolves claims whose dequeue COMMIT was sent but never acknowledged.
///
/// The commit outcome is indeterminate: the server may have made the rows
/// `running` under this worker before the acknowledgement was lost. Those rows
/// never reached the intake buffer or the in-flight registry, so the abort
/// loop never polls them — and while this worker keeps heartbeating, the
/// sweeper deliberately leaves an `aborting` row whose owner holds a live lease
/// to that owner. Unresolved, such a row waits for the worker *process* to
/// exit; with its timeout disabled it waits forever. The guarded requeue above
/// settles both outcomes: a committed claim is given back (attempt refunded —
/// it never ran), and one that never landed matches no row.
///
/// Detached, because the fetch loop that hit the commit error may itself be
/// cancelled by shutdown while this is still retrying; a resolver that dies
/// with the process is covered by lease expiry, which is the recovery path a
/// crashed worker already has. A missing runtime — a drop during runtime
/// teardown — is that same process exit, so it only logs: lease expiry is
/// already the answer.
fn spawn_unacknowledged_claim_resolver(
    context: RecoveryContext,
    worker_id: Uuid,
    mut claims: Vec<DatabaseUnacknowledgedClaim>,
) {
    const INITIAL_RETRY_DELAY: Duration = Duration::from_millis(100);
    const MAX_RETRY_DELAY: Duration = Duration::from_secs(5);
    // How long a deferral stays a debug-level detail, and how often a persisting one is reported after that.
    const DEFERRAL_WARN_AFTER: Duration = Duration::from_secs(30);
    const DEFERRAL_WARN_EVERY: Duration = Duration::from_secs(300);
    if claims.is_empty() {
        return;
    }
    tracing::warn!(
        queue = %context.queue,
        worker.id = %worker_id,
        job.count = claims.len(),
        "dequeue commit outcome is unknown; resolving the claims in the background"
    );
    let Ok(runtime) = tokio::runtime::Handle::try_current() else {
        tracing::warn!(
            queue = %context.queue,
            worker.id = %worker_id,
            "no runtime to resolve unacknowledged dequeue claims on; lease expiry will recover them"
        );
        return;
    };
    runtime.spawn(async move {
        let mut delay = INITIAL_RETRY_DELAY;
        let deferred_since = tokio::time::Instant::now();
        let mut deferral_reported_at: Option<tokio::time::Instant> = None;
        loop {
            match requeue_unacknowledged_claims(&context, worker_id, &mut claims).await {
                Ok(Some(requeued)) => {
                    tracing::warn!(
                        queue = %context.queue,
                        worker.id = %worker_id,
                        job.count = requeued,
                        "resolved unacknowledged dequeue claims"
                    );
                    return;
                }
                // A claim of this worker was still in flight past the pass's bounded wait. Not a failure while it is
                // brief: the pass is ordered behind such claims by design. One that persists is a claim stuck open —
                // the server ends one left idle in transaction within half a minute (see
                // `BEGIN_BOUNDED_TRANSACTION_SQL`), but one blocked writing its rows to a client that is gone lasts
                // until TCP gives up on it, many minutes on — and meanwhile the rows being resolved sit `running`
                // under a live lease, holding their dedupe keys, so it is reported, though not on every retry.
                Ok(None) => {
                    let deferred = deferred_since.elapsed();
                    if deferred >= DEFERRAL_WARN_AFTER
                        && deferral_reported_at.is_none_or(|reported| reported.elapsed() >= DEFERRAL_WARN_EVERY)
                    {
                        deferral_reported_at = Some(tokio::time::Instant::now());
                        tracing::warn!(
                            queue = %context.queue,
                            worker.id = %worker_id,
                            job.count = claims.len(),
                            deferred_secs = deferred.as_secs(),
                            "unacknowledged dequeue claims are still unresolved: a dequeue claim of this worker has \
                             stayed open; retrying"
                        );
                    } else {
                        tracing::debug!(
                            queue = %context.queue,
                            worker.id = %worker_id,
                            job.count = claims.len(),
                            "a dequeue claim of this worker is still in flight; retrying its resolution"
                        );
                    }
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(MAX_RETRY_DELAY);
                }
                // A closed pool never reopens: the process is tearing this
                // queue down, and the retry loop would spin against it until
                // exit. Lease expiry is the recovery a dead process already
                // has, and it covers this one.
                Err(sqlx::Error::PoolClosed) => {
                    tracing::warn!(
                        queue = %context.queue,
                        worker.id = %worker_id,
                        job.count = claims.len(),
                        "pool closed before unacknowledged dequeue claims resolved; lease expiry will recover them"
                    );
                    return;
                }
                Err(error) => {
                    tracing::warn!(
                        queue = %context.queue,
                        worker.id = %worker_id,
                        job.count = claims.len(),
                        %error,
                        "failed to resolve unacknowledged dequeue claims; retrying"
                    );
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(MAX_RETRY_DELAY);
                }
            }
        }
    });
}

/// Owns a dequeue's decoded claims across the window where they can be lost
/// without anyone learning of them: from just before the COMMIT is sent until
/// the batch is returned to the caller. Dropping the guard armed — a commit
/// that returned an error unwinding out, or the whole dequeue future dropped
/// mid-commit or mid-probe by the worker's operation deadline or a cancelled
/// custom consumer — hands the claims to the background resolver. Only
/// [`UnacknowledgedClaimGuard::disarm`], called when no await remains between
/// the committed claim and its caller, lets the batch pass without one.
struct UnacknowledgedClaimGuard {
    context: RecoveryContext,
    worker_id: Uuid,
    claims: Vec<DatabaseUnacknowledgedClaim>,
}

impl UnacknowledgedClaimGuard {
    fn disarm(mut self) {
        self.claims.clear();
    }
}

impl Drop for UnacknowledgedClaimGuard {
    fn drop(&mut self) {
        spawn_unacknowledged_claim_resolver(self.context.clone(), self.worker_id, std::mem::take(&mut self.claims));
    }
}

/// The `error` stored on a row recovered from a consumer [`crate::Attempt`]
/// that was dropped without settling, so the dashboard shows
/// why the occurrence moved without its owner reporting anything.
const DROPPED_ATTEMPT_ERROR: &str = "attempt dropped without settlement";

impl Database {
    /// Hands a consumer attempt that was dropped without settling to a
    /// background recovery task. The consumer's own task may have panicked or
    /// been cancelled while its heartbeat loop runs on — and a heartbeat is
    /// the assertion that every claimed attempt is still being worked, so
    /// nothing else would ever reclaim an untimed one. The recovery spends the
    /// attempt (no refund: the handler may have run arbitrarily far) and
    /// requeues it under the job's own retry delay, or finishes it `aborted`
    /// when no attempts remain or an abort landed meanwhile; every transition
    /// carries the standard guards, so a row that already moved on is left
    /// alone.
    pub(crate) fn spawn_dropped_attempt_recovery(&self, row: &JobRow, runtime: Option<tokio::runtime::Handle>) {
        spawn_dropped_attempt_resolver(
            self.recovery_context(),
            row.worker_id,
            duration_to_ms(row.next_retry_delay()),
            DatabaseUnacknowledgedClaim { id: row.id, attempts: row.attempts, started_at: row.started_at },
            runtime,
        );
    }
}

/// One recovery pass for a dropped, unsettled attempt: the guarded requeue
/// (attempt spent, the job's own retry delay applied), falling back to the
/// guarded abort when no retry can be granted.
#[derive(Debug, Clone, Copy)]
enum DroppedAttemptResolution {
    Requeued,
    Aborted,
    Unchanged,
}

async fn resolve_dropped_attempt(
    context: &RecoveryContext,
    worker_id: Option<Uuid>,
    retry_delay_ms: i64,
    claim: &DatabaseUnacknowledgedClaim,
) -> Result<DroppedAttemptResolution, sqlx::Error> {
    let mut connection = PoolConnectionGuard::new(context.pool.acquire().await?);
    let mut transaction = connection.begin_transaction().await?;
    let row = sqlx::query_as::<_, RequeueResult>(REQUEUE_GUARDED_SQL)
        .bind(claim.id)
        .bind(retry_delay_ms)
        .bind(Some(DROPPED_ATTEMPT_ERROR))
        .bind(claim.attempts)
        .bind(worker_id)
        .bind(&context.queue)
        // The attempt was dispatched and may have run arbitrarily far before
        // the drop, so it is spent, not refunded.
        .bind(false)
        .bind(true)
        .bind(true)
        .bind(SWEPT)
        .bind(swept_marker())
        .bind(&context.notify_channel)
        .bind(false)
        .bind(claim.started_at.map(|started_at| started_at.to_sqlx()))
        .fetch_one(&mut *transaction)
        .await?;
    let resolution = if row.requeued {
        DroppedAttemptResolution::Requeued
    } else if abort_unsettled_claim(
        &mut transaction,
        &context.queue,
        &context.done_channel,
        worker_id,
        claim,
        DROPPED_ATTEMPT_ERROR,
    )
    .await?
    {
        DroppedAttemptResolution::Aborted
    } else {
        DroppedAttemptResolution::Unchanged
    };
    transaction.commit().await?;
    Ok(resolution)
}

/// Resolves a consumer attempt dropped without settlement, in the background
/// and with the same retry discipline as the unacknowledged-claim resolver: a
/// resolver that dies with the process is covered by lease expiry, and a
/// closed pool never reopens, so both bail rather than spin.
fn spawn_dropped_attempt_resolver(
    context: RecoveryContext,
    worker_id: Option<Uuid>,
    retry_delay_ms: i64,
    claim: DatabaseUnacknowledgedClaim,
    fallback_runtime: Option<tokio::runtime::Handle>,
) {
    const INITIAL_RETRY_DELAY: Duration = Duration::from_millis(100);
    const MAX_RETRY_DELAY: Duration = Duration::from_secs(5);
    // `claim`, the raw counter this recovery is fenced on, as every log line names it; `attempt` is reserved for the
    // spent count `JobContext::attempt` reports, which excludes refunded claims.
    tracing::warn!(
        queue = %context.queue,
        job.id = %claim.id,
        claim = claim.attempts,
        "attempt dropped without settlement; recovering it in the background"
    );
    let Some(runtime) = tokio::runtime::Handle::try_current().ok().or(fallback_runtime) else {
        tracing::warn!(
            queue = %context.queue,
            job.id = %claim.id,
            "no runtime is available to recover a dropped attempt; recovery waits for worker heartbeats to stop"
        );
        return;
    };
    runtime.spawn(async move {
        let mut delay = INITIAL_RETRY_DELAY;
        loop {
            match resolve_dropped_attempt(
                &context,
                worker_id,
                retry_delay_ms,
                &claim,
            )
            .await
            {
                Ok(resolution) => {
                    match resolution {
                        DroppedAttemptResolution::Requeued => context.counters.record_retry(),
                        DroppedAttemptResolution::Aborted => context.counters.record_abort(),
                        DroppedAttemptResolution::Unchanged => {}
                    }
                    tracing::warn!(queue = %context.queue, job.id = %claim.id, ?resolution, "recovered a dropped attempt");
                    return;
                }
                Err(sqlx::Error::PoolClosed) => {
                    tracing::warn!(
                        queue = %context.queue,
                        job.id = %claim.id,
                        "pool closed before a dropped attempt was recovered; lease expiry will recover it"
                    );
                    return;
                }
                Err(error) => {
                    tracing::warn!(queue = %context.queue, job.id = %claim.id, %error, "failed to recover a dropped attempt; retrying");
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(MAX_RETRY_DELAY);
                }
            }
        }
    });
}

/// Drops an armed [`UnacknowledgedClaimGuard`] for `claims`, so the crate's
/// integration tests can drive the cancellation path without staging a real
/// mid-commit cancellation.
#[cfg(feature = "_test")]
pub(crate) fn drop_armed_claim_guard(database: &Database, worker_id: Uuid, claims: Vec<DatabaseUnacknowledgedClaim>) {
    drop(UnacknowledgedClaimGuard { context: database.recovery_context(), worker_id, claims });
}

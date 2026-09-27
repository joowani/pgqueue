//! Job API, typed macro, enqueue-and-wait, and cron integration tests.

mod enqueue_and_wait {
    //! Request/response tests: `Queue::enqueue_and_wait` and `JobHandle::wait`,
    //! completion-NOTIFY driven with polling fallback.

    use sqlx::PgPool;
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use crate::{
        EnqueueResultTestExt, QueueProtocolTestExt, TestDb, pool_with_max, wait_for_done_listener,
        wait_for_done_listeners,
    };
    use pgqueue::{Error, JobErrorKind, JobRetention, JobState, JobStatus, Queue, Worker, WorkerTimers};
    use tokio_util::sync::CancellationToken;
    use tracing::instrument::WithSubscriber;
    use tracing_subscriber::layer::{Context, Layer, SubscriberExt};

    #[pgqueue::job]
    async fn double(args: u32) -> anyhow::Result<u32> {
        Ok(args * 2)
    }

    #[pgqueue::job(max_attempts = 1)]
    async fn fails_if_odd(args: u32) -> anyhow::Result<u32> {
        anyhow::ensure!(args.is_multiple_of(2), "odd number {args}");
        Ok(args)
    }

    #[pgqueue::job(result_ttl_ms = 0)]
    async fn ephemeral(_: ()) -> anyhow::Result<u32> {
        Ok(7)
    }

    #[pgqueue::job(max_attempts = 1, timeout_ms = 30_000)]
    async fn very_slow(_: ()) -> anyhow::Result<()> {
        std::future::pending().await
    }

    #[pgqueue::job]
    async fn shared(_: (), tag: JobState<String>) -> anyhow::Result<String> {
        Ok(tag.0)
    }

    #[pgqueue::job(failed_ttl_ms = 0)]
    async fn forgets_failures(_: ()) -> anyhow::Result<u32> {
        Ok(1)
    }

    async fn is_awaited(queue: &Queue, id: uuid::Uuid) -> bool {
        sqlx::query_scalar::<_, bool>("SELECT awaited FROM pgqueue.jobs WHERE id = $1")
            .bind(id)
            .fetch_one(queue.pool())
            .await
            .expect("read awaited")
    }

    /// The completion notification is emitted only for a row somebody waits
    /// on, so every wait registers its interest before it subscribes:
    /// `enqueue_and_wait` with the insert itself, `JobHandle::wait` with one
    /// write ahead of its subscription, and a wait that deduplicated onto an
    /// existing row on that row.
    #[sqlx::test(migrations = "./migrations")]
    async fn test_waits_register_interest_in_the_completion_notification(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;

        let plain = db.queue.enqueue(double::job(4)).await.unwrap().unwrap();
        assert!(!is_awaited(&db.queue, plain.id()).await, "a plain enqueue is not awaited");
        let plain_wait = tokio::spawn({
            let plain = plain.clone();
            async move { plain.wait(Some(Duration::from_secs(30))).await }
        });
        crate::wait_until(Duration::from_secs(5), Duration::from_millis(10), "wait did not register", || async {
            is_awaited(&db.queue, plain.id()).await
        })
        .await;

        let keyed = db.queue.enqueue(double::job(5).dedupe_key("keyed")).await.unwrap().unwrap();
        assert!(!is_awaited(&db.queue, keyed.id()).await);
        let deduplicated_wait = tokio::spawn({
            let queue = db.queue.clone();
            async move { queue.enqueue_and_wait(double::job(6).dedupe_key("keyed"), Some(Duration::from_secs(30))).await }
        });
        crate::wait_until(
            Duration::from_secs(5),
            Duration::from_millis(10),
            "dedupe wait did not register",
            || async { is_awaited(&db.queue, keyed.id()).await },
        )
        .await;

        let fresh_wait = tokio::spawn({
            let queue = db.queue.clone();
            async move { queue.enqueue_and_wait(double::job(7).dedupe_key("fresh"), Some(Duration::from_secs(30))).await }
        });
        let fresh = crate::wait_for_some(Duration::from_secs(5), Duration::from_millis(10), "no fresh row", || async {
            sqlx::query_scalar::<_, uuid::Uuid>("SELECT id FROM pgqueue.jobs WHERE dedupe_key = 'fresh'")
                .fetch_optional(db.queue.pool())
                .await
                .unwrap()
        })
        .await;
        assert!(is_awaited(&db.queue, fresh).await, "enqueue_and_wait inserts the row already awaited");

        let (stop, worker) = spawn_worker(db.queue.clone());
        assert_eq!(plain_wait.await.unwrap().unwrap(), 8);
        assert_eq!(deduplicated_wait.await.unwrap().unwrap(), 10, "the existing job's result");
        assert_eq!(fresh_wait.await.unwrap().unwrap(), 14);
        stop.cancel();
        worker.await.unwrap();
    }

    /// A wait does not know which outcome it will get, so it needs a durable
    /// row for either: a failure retention that deletes immediately is refused
    /// exactly as a result retention that does.
    #[sqlx::test(migrations = "./migrations")]
    async fn test_waits_require_durable_retention_for_failures_too(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        let error =
            db.queue.enqueue_and_wait(forgets_failures::job(()), Some(Duration::from_secs(1))).await.unwrap_err();
        assert!(matches!(error, Error::Config(_)), "{error}");
        assert_eq!(db.queue.counts().await.unwrap().queued, 0, "refused before enqueue");

        let handle = db.queue.enqueue(forgets_failures::job(())).await.unwrap().unwrap();
        let error = handle.wait(Some(Duration::from_secs(1))).await.unwrap_err();
        assert!(matches!(error, Error::Config(_)), "{error}");

        // A deduplicated owner that forgets failures is refused the same way,
        // even when this request itself would have kept them.
        db.queue.enqueue(forgets_failures::job(()).dedupe_key("k")).await.unwrap();
        let durable = forgets_failures::job(()).failed_retention(JobRetention::Forever).dedupe_key("k");
        let error = db.queue.enqueue_and_wait(durable, Some(Duration::from_secs(1))).await.unwrap_err();
        assert!(matches!(error, Error::Config(_)), "{error}");
    }

    /// Starts a background worker for the given queue with all test handlers.
    fn spawn_worker(queue: Queue) -> (CancellationToken, tokio::task::JoinHandle<()>) {
        let worker = Worker::builder(queue)
            .register_job(double)
            .register_job(fails_if_odd)
            .register_job(ephemeral)
            .register_job(very_slow)
            .register_job(shared)
            .state("from-state".to_string())
            .timers(WorkerTimers {
                abort: Duration::from_millis(50),
                schedule: Duration::from_millis(200),
                sweep: Duration::from_secs(60),
                worker_info: Duration::from_secs(1),
            })
            .poll_interval(Duration::from_millis(50))
            .concurrency(4)
            .build()
            .unwrap();
        let token = CancellationToken::new();
        let stop = token.clone();
        let handle = tokio::spawn(async move {
            worker.run_until(stop).await.unwrap();
        });
        (token, handle)
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn test_enqueue_and_wait_returns_the_typed_result(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        let (token, run) = spawn_worker(db.queue.clone());

        let result: u32 = db.queue.enqueue_and_wait(double::job(21), Some(Duration::from_secs(10))).await.unwrap();
        assert_eq!(result, 42);

        token.cancel();
        run.await.unwrap();
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn test_enqueue_and_wait_propagates_job_failures(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        let (token, run) = spawn_worker(db.queue.clone());

        let err = db.queue.enqueue_and_wait(fails_if_odd::job(3), Some(Duration::from_secs(10))).await.unwrap_err();
        match err {
            Error::Job(job_error) => {
                assert_eq!(job_error.kind, JobErrorKind::Failed);
                assert_eq!(job_error.message, "odd number 3");
            }
            other => panic!("expected Error::Job, got {other}"),
        }

        token.cancel();
        run.await.unwrap();
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn test_enqueue_and_wait_times_out_when_nothing_processes(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        // No worker running.
        let err = db.queue.enqueue_and_wait(double::job(1), Some(Duration::from_millis(300))).await.unwrap_err();
        assert!(matches!(err, Error::WaitTimeout), "{err}");
    }

    /// The timeout bounds the enqueue as well as the wait. A keyed enqueue waits for its key's lock, which a caller
    /// transaction that enqueued the same key holds until it ends; bounding only the wait parked the caller there for
    /// that transaction's whole life, however short a deadline it had asked for.
    #[sqlx::test(migrations = "./migrations")]
    async fn test_enqueue_and_wait_timeout_bounds_an_enqueue_blocked_on_its_dedupe_key(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        let mut holder = db.queue.pool().begin().await.unwrap();
        db.queue.enqueue_in(&mut holder, double::job(1).dedupe_key("held")).await.unwrap().unwrap();

        let error = tokio::time::timeout(
            Duration::from_secs(5),
            db.queue.enqueue_and_wait(double::job(2).dedupe_key("held"), Some(Duration::from_millis(300))),
        )
        .await
        .expect("the enqueue outlived the caller's timeout")
        .unwrap_err();
        assert!(matches!(error, Error::WaitTimeout), "{error}");
        holder.rollback().await.unwrap();
    }

    /// How many backends in this test's database are queued for an advisory lock.
    async fn advisory_lock_waiters(pool: &PgPool) -> i64 {
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM pg_locks
             WHERE locktype = 'advisory' AND NOT granted
               AND database = (SELECT oid FROM pg_database WHERE datname = current_database())",
        )
        .fetch_one(pool)
        .await
        .expect("inspect advisory lock waiters")
    }

    /// A deadline enforced only by the client does not end the wait it abandons. Dropping a keyed enqueue blocked on
    /// its key's lock left that lock call queued on the server, and sqlx pings a connection before pooling it again,
    /// so the ping waited it out: every timed-out call kept a pooled connection until the transaction holding the key
    /// ended. A caller retrying on `WaitTimeout` soon held the whole pool, and every other operation on the queue — a
    /// co-hosted worker's heartbeat and finish included — waited in `acquire`. The deadline bounds the lock wait on the
    /// server as well, so the connection comes back at the deadline.
    #[sqlx::test(migrations = "./migrations")]
    async fn test_timed_out_keyed_enqueue_and_wait_gives_its_connection_back(pool: PgPool) {
        let db = TestDb::new(pool_with_max(&pool, 3).await).await;
        // On the fixture's pool, so the holder takes none of the queue's three connections.
        let mut holder = pool.begin().await.unwrap();
        db.queue.enqueue_in(&mut holder, double::job(1).dedupe_key("held")).await.unwrap().unwrap();

        // Twice the pool's size, as a caller retrying on every timeout would.
        for _ in 0..6 {
            let error = db
                .queue
                .enqueue_and_wait(double::job(2).dedupe_key("held"), Some(Duration::from_millis(200)))
                .await
                .unwrap_err();
            assert!(matches!(error, Error::WaitTimeout), "{error}");
        }
        tokio::time::timeout(Duration::from_secs(5), db.queue.counts())
            .await
            .expect("timed-out enqueues kept the queue's connections")
            .unwrap();
        // Ended on the server, not merely abandoned by the client: nothing is left queued behind the holder.
        crate::wait_until(
            Duration::from_secs(5),
            Duration::from_millis(10),
            "a lock wait outlived its caller",
            || async { advisory_lock_waiters(&pool).await == 0 },
        )
        .await;
        holder.rollback().await.unwrap();
    }

    /// A queue over connections whose sessions start with `lock_timeout` set, as a role or connection default would.
    async fn queue_with_session_lock_timeout(pool: &PgPool, lock_timeout: &str) -> TestDb {
        let options = pool.connect_options().as_ref().clone().options([("lock_timeout", lock_timeout)]);
        TestDb::new(sqlx::postgres::PgPoolOptions::new().max_connections(2).connect_with(options).await.unwrap()).await
    }

    /// The deadline's bound never lengthens a stricter `lock_timeout` the session already has: that refusal is the
    /// operator's setting at work, not the caller's deadline, and surfaces as the database error it is. A session
    /// setting longer than the time left is tightened to the deadline, exactly as no setting at all is.
    #[sqlx::test(migrations = "./migrations")]
    async fn test_enqueue_and_wait_keeps_a_shorter_session_lock_timeout(pool: PgPool) {
        let mut holder = pool.begin().await.unwrap();
        let strict = queue_with_session_lock_timeout(&pool, "100ms").await;
        strict.queue.enqueue_in(&mut holder, double::job(1).dedupe_key("held")).await.unwrap().unwrap();

        let error = tokio::time::timeout(
            Duration::from_secs(10),
            strict.queue.enqueue_and_wait(double::job(2).dedupe_key("held"), Some(Duration::from_secs(30))),
        )
        .await
        .expect("the session's shorter lock_timeout was lengthened to the deadline")
        .unwrap_err();
        assert!(
            matches!(&error, Error::Db(sqlx::Error::Database(refusal)) if refusal.code().as_deref() == Some("55P03")),
            "{error}"
        );

        let lenient = queue_with_session_lock_timeout(&pool, "60s").await;
        let error = lenient
            .queue
            .enqueue_and_wait(double::job(2).dedupe_key("held"), Some(Duration::from_millis(200)))
            .await
            .unwrap_err();
        assert!(matches!(error, Error::WaitTimeout), "{error}");
        crate::wait_until(
            Duration::from_secs(5),
            Duration::from_millis(10),
            "the session's longer lock_timeout outlasted the deadline",
            || async { advisory_lock_waiters(&pool).await == 0 },
        )
        .await;
        holder.rollback().await.unwrap();
    }

    /// How many backends in this test's database are waiting for a lock of any kind, a row's included.
    async fn lock_waiters(pool: &PgPool) -> i64 {
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock'",
        )
        .fetch_one(pool)
        .await
        .expect("inspect lock waiters")
    }

    /// Opens a transaction on `pool` that holds job `id`'s row `FOR UPDATE`, as an operator's session left open would.
    async fn hold_row_lock(pool: &PgPool, id: uuid::Uuid) -> sqlx::PgTransaction<'static> {
        let mut holder = pool.begin().await.unwrap();
        sqlx::query("SELECT id FROM pgqueue.jobs WHERE id = $1 FOR UPDATE")
            .bind(id)
            .execute(&mut *holder)
            .await
            .unwrap();
        holder
    }

    /// Times `call` out six times — twice the queue's three connections, as a caller retrying on every timeout would —
    /// and then requires the queue to still have a connection to give, with nothing left queued on the server.
    async fn assert_timed_out_calls_give_their_connections_back<T: std::fmt::Debug>(
        db: &TestDb,
        pool: &PgPool,
        mut call: impl AsyncFnMut() -> Result<T, Error>,
    ) {
        for _ in 0..6 {
            let error = call().await.unwrap_err();
            assert!(matches!(error, Error::WaitTimeout), "{error}");
        }
        tokio::time::timeout(Duration::from_secs(5), db.queue.counts())
            .await
            .expect("timed-out calls kept the queue's connections")
            .unwrap();
        crate::wait_until(
            Duration::from_secs(5),
            Duration::from_millis(10),
            "a call left a lock wait behind",
            || async { lock_waiters(pool).await == 0 },
        )
        .await;
    }

    /// A wait registers its interest in the completion notification with a write to the job's row, and that write
    /// queued behind whatever held the row locked — here an operator's `SELECT ... FOR UPDATE` left open. The caller's
    /// deadline ended only the client's side of it, and sqlx pings a connection before pooling it again, so every
    /// timed-out wait kept a pooled connection until the holder ended, and a caller retrying on `WaitTimeout` soon held
    /// the whole pool, and with it every other operation on the queue. The registration never waits for a row lock.
    #[sqlx::test(migrations = "./migrations")]
    async fn test_timed_out_wait_on_a_locked_row_gives_its_connection_back(pool: PgPool) {
        let db = TestDb::new(pool_with_max(&pool, 3).await).await;
        let handle = db.queue.enqueue(double::job(1)).await.unwrap().unwrap();
        // On the fixture's pool, so the holder takes none of the queue's three connections.
        let holder = hold_row_lock(&pool, handle.id()).await;

        assert_timed_out_calls_give_their_connections_back(&db, &pool, async || {
            handle.wait(Some(Duration::from_millis(200))).await
        })
        .await;
        holder.rollback().await.unwrap();
    }

    /// The same for a row held by a dequeue claim whose COMMIT has not landed — still in flight, or lost, which keeps
    /// the claim's session and its row locks until the server ends it. Everybody else still reads the row `queued`.
    #[sqlx::test(migrations = "./migrations")]
    async fn test_timed_out_wait_on_a_claimed_row_gives_its_connection_back(pool: PgPool) {
        let db = TestDb::new(pool_with_max(&pool, 3).await).await;
        let handle = db.queue.enqueue(double::job(1)).await.unwrap().unwrap();
        let mut claim = pool.begin().await.unwrap();
        let claimed = sqlx::query_as::<_, pgqueue::JobRow>(pgqueue::__test_support::dequeue_claim_sql())
            .bind(db.queue.name())
            .bind(i16::MIN)
            .bind(i16::MAX)
            .bind(1_i64)
            .bind(uuid::Uuid::now_v7())
            // Without an intake check, so the claim needs no worker lease.
            .bind(false)
            .bind(pgqueue::__test_support::claim_resolution_lock_key(&db.database))
            .fetch_all(&mut *claim)
            .await
            .unwrap();
        assert_eq!(claimed.iter().map(|job| job.id).collect::<Vec<_>>(), [handle.id()]);

        assert_timed_out_calls_give_their_connections_back(&db, &pool, async || {
            handle.wait(Some(Duration::from_millis(200))).await
        })
        .await;
        claim.rollback().await.unwrap();
    }

    /// The same registration on the path where `enqueue_and_wait` deduplicates onto a live job: somebody else inserted
    /// that row, unmarked, so the call registers on it exactly as a wait on that job's own handle does.
    #[sqlx::test(migrations = "./migrations")]
    async fn test_timed_out_deduplicated_enqueue_and_wait_gives_its_connection_back(pool: PgPool) {
        let db = TestDb::new(pool_with_max(&pool, 3).await).await;
        let existing = db.queue.enqueue(double::job(1).dedupe_key("held")).await.unwrap().unwrap();
        let holder = hold_row_lock(&pool, existing.id()).await;

        assert_timed_out_calls_give_their_connections_back(&db, &pool, async || {
            db.queue.enqueue_and_wait(double::job(2).dedupe_key("held"), Some(Duration::from_millis(200))).await
        })
        .await;
        holder.rollback().await.unwrap();
    }

    /// Waits with no deadline at all hold no connection while their row stays locked either, however many of them
    /// there are, and every one of them still gets the result once the lock is gone.
    #[sqlx::test(migrations = "./migrations")]
    async fn test_waits_on_a_locked_row_hold_no_connection_while_it_stays_locked(pool: PgPool) {
        let db = TestDb::new(pool_with_max(&pool, 3).await).await;
        let handle = db.queue.enqueue(double::job(21)).await.unwrap().unwrap();
        let holder = hold_row_lock(&pool, handle.id()).await;

        // More waits than the queue has connections.
        let waits = (0..4)
            .map(|_| {
                let handle = handle.clone();
                tokio::spawn(async move { handle.wait(None).await })
            })
            .collect::<Vec<_>>();
        // A wait subscribes only once it has tried to register, so the listener is up only once one of them got past
        // that write. Looked for on the fixture's pool, which the waits cannot take.
        wait_for_done_listeners(&pool, 1).await;
        tokio::time::timeout(Duration::from_secs(5), db.queue.counts())
            .await
            .expect("waits on a locked row kept the queue's connections")
            .unwrap();
        assert_eq!(lock_waiters(&pool).await, 0, "a registration is queued behind the row lock");

        holder.rollback().await.unwrap();
        let (stop, worker) = spawn_worker(db.queue.clone());
        for wait in waits {
            let result = tokio::time::timeout(Duration::from_secs(30), wait).await.expect("a wait never resolved");
            assert_eq!(result.unwrap().unwrap(), 42);
        }
        stop.cancel();
        worker.await.unwrap();
    }

    /// A registration the row's lock turned away is a lost registration, not a lost wait, and not lost for good either.
    /// Left at that, the wait would have only its backed-off polls, two seconds apart, for the rest of its life, and a
    /// worker claiming the job just as `wait` follows its `enqueue` would be all it took. A poll that finds the row
    /// live and still unmarked registers again, so the finish is pushed once the lock is gone.
    #[sqlx::test(migrations = "./migrations")]
    async fn test_a_wait_registers_once_its_row_lock_is_released(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        let handle = db.queue.enqueue(double::job(21)).await.unwrap().unwrap();
        let holder = hold_row_lock(&pool, handle.id()).await;
        let wait = {
            let handle = handle.clone();
            tokio::spawn(async move { handle.wait(Some(Duration::from_secs(30))).await })
        };
        // The wait subscribes only once it has tried to register, so from here on only a retry can mark the row.
        wait_for_done_listener(&db).await;
        assert!(!is_awaited(&db.queue, handle.id()).await, "the row was marked through its lock");

        holder.rollback().await.unwrap();
        crate::wait_until(
            Duration::from_secs(10),
            Duration::from_millis(10),
            "the wait never registered once the lock was released",
            || async { is_awaited(&db.queue, handle.id()).await },
        )
        .await;
        let (stop, worker) = spawn_worker(db.queue.clone());
        assert_eq!(wait.await.unwrap().unwrap(), 42);
        stop.cancel();
        worker.await.unwrap();
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn test_shared_pool_remains_available_when_multiple_listeners_start(pool: PgPool) {
        let query_pool = pool_with_max(&pool, 2).await;
        let db = TestDb::new(query_pool).await;
        let other = db.another_queue(|builder| builder).await;
        let first = db.queue.enqueue(double::job(1).delay(Duration::from_secs(60))).await.unwrap().unwrap();
        let second = other.enqueue(double::job(2).delay(Duration::from_secs(60))).await.unwrap().unwrap();
        let first_waiter = tokio::spawn(async move { first.wait_value(None).await });
        let second_waiter = tokio::spawn(async move { second.wait_value(None).await });

        wait_for_done_listeners(&pool, 2).await;
        tokio::time::timeout(Duration::from_secs(1), db.queue.counts())
            .await
            .expect("LISTEN connections must not exhaust the shared query pool")
            .unwrap();

        first_waiter.abort();
        second_waiter.abort();
        let _ = first_waiter.await;
        let _ = second_waiter.await;
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn test_enqueue_and_wait_on_dedupe_hit_waits_on_the_existing_job(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;

        // A slow deduplicated job is already live...
        let existing = db.queue.enqueue(very_slow::job(()).dedupe_key("singleton")).await.unwrap().unwrap();

        // ...so enqueue_and_wait with the same key attaches to it rather than erroring.
        let queue = db.queue.clone();
        let waiter = tokio::spawn(async move {
            queue.enqueue_and_wait(very_slow::job(()).dedupe_key("singleton"), Some(Duration::from_secs(10))).await
        });

        wait_for_done_listener(&db).await;
        assert!(existing.abort("cancelled by test").await.unwrap());

        let err = waiter.await.unwrap().unwrap_err();
        match err {
            Error::Job(job_error) => assert_eq!(job_error.kind, JobErrorKind::Aborted),
            other => panic!("expected Error::Job, got {other}"),
        }
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn test_enqueue_and_wait_revives_a_terminal_deduplicated_job_with_the_same_schedule(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        let (token, run) = spawn_worker(db.queue.clone());

        let first = db.queue.enqueue(double::job(2).dedupe_key("reusable")).await.unwrap().unwrap();
        assert_eq!(first.wait(Some(Duration::from_secs(10))).await.unwrap(), 4);
        let scheduled_at = first.fetch_job().await.unwrap().scheduled_at;

        let second = db
            .queue
            .enqueue_and_wait(double::job(3).dedupe_key("reusable").at(scheduled_at), Some(Duration::from_secs(10)))
            .await
            .unwrap();
        assert_eq!(second, 6, "the terminal row must run again");

        token.cancel();
        run.await.unwrap();
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn test_repeated_dedupe_key_reuse_preserves_every_occurrence_result(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        let worker_id = uuid::Uuid::now_v7();
        let mut occurrence_ids = HashSet::new();
        let mut handles = Vec::new();
        for value in 0..16_u32 {
            let handle = db
                .queue
                .enqueue(double::job(value).dedupe_key("hot-key"))
                .await
                .unwrap()
                .expect("the prior occurrence is terminal");
            assert!(occurrence_ids.insert(handle.id()), "key reuse must create a distinct occurrence");
            let active = db.queue.dequeue(1, worker_id).await.unwrap().remove(0);
            assert_eq!(active.id, handle.id());
            assert!(
                db.queue.finish(&active, JobStatus::Complete, Some(serde_json::json!(value * 2)), None,).await.unwrap()
            );
            handles.push((value, handle));
        }

        let mut waits = tokio::task::JoinSet::new();
        for (value, handle) in handles {
            waits.spawn(async move { (value, handle.wait(Some(Duration::from_secs(5))).await.unwrap()) });
        }
        while let Some(result) = waits.join_next().await {
            let (value, output) = result.unwrap();
            assert_eq!(output, value * 2);
        }
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn test_enqueue_and_wait_rejects_a_dedupe_key_owned_by_another_job_type(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        db.queue.enqueue(very_slow::job(()).dedupe_key("shared-key")).await.unwrap().unwrap();

        let error = db
            .queue
            .enqueue_and_wait(double::job(1).dedupe_key("shared-key"), Some(Duration::from_secs(1)))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("belongs to job"), "{error}");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn test_wait_rejects_delete_immediately_jobs_without_a_durable_result(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        let handle = db.queue.enqueue(ephemeral::job(())).await.unwrap().unwrap();
        let error = handle.wait_value(Some(Duration::from_secs(1))).await.unwrap_err();
        assert!(matches!(error, Error::Config(_)), "{error}");
        assert_eq!(handle.fetch_job().await.unwrap().status, JobStatus::Queued);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn test_wait_timeout_covers_delete_immediately_outcome_read(pool: PgPool) {
        let query_pool = pool_with_max(&pool, 1).await;
        let db = TestDb::new(query_pool.clone()).await;
        let handle = db.queue.enqueue(ephemeral::job(())).await.unwrap().unwrap();
        let _held = query_pool.acquire().await.unwrap();

        let started = tokio::time::Instant::now();
        let error = handle.wait_value(Some(Duration::from_millis(100))).await.unwrap_err();
        assert!(matches!(error, Error::WaitTimeout), "{error}");
        assert!(started.elapsed() < Duration::from_secs(1), "the outcome read escaped the caller's deadline");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn test_enqueue_and_wait_rejects_delete_immediately_before_enqueue(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        let error = db.queue.enqueue_and_wait(ephemeral::job(()), Some(Duration::from_secs(1))).await.unwrap_err();
        assert!(matches!(error, Error::Config(_)), "{error}");
        assert_eq!(db.queue.counts().await.unwrap().queued, 0);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn test_enqueue_and_wait_rejects_a_deduplicated_delete_immediately_owner(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        let owner = db.queue.enqueue(ephemeral::job(()).dedupe_key("ephemeral-owner")).await.unwrap().unwrap();
        let error = db
            .queue
            .enqueue_and_wait(
                ephemeral::job(()).dedupe_key("ephemeral-owner").retention(JobRetention::Forever),
                Some(Duration::from_secs(1)),
            )
            .await
            .unwrap_err();
        assert!(matches!(error, Error::Config(_)), "{error}");
        assert_eq!(owner.fetch_job().await.unwrap().status, JobStatus::Queued);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn test_wait_on_a_missing_job_errors(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        let handle = db.queue.enqueue(double::job(1)).await.unwrap().unwrap();
        // Delete the row out from under the handle.
        sqlx::query("TRUNCATE pgqueue.jobs").execute(db.queue.pool()).await.unwrap();
        let err = handle.wait_value(Some(Duration::from_secs(2))).await.unwrap_err();
        assert!(matches!(err, Error::JobNotFound(_)), "{err}");
    }

    /// The same physical event as the test below — retention deleted a finished
    /// job before the waiter read it — but with the completion notification
    /// lost, which is what a `Lagged` receiver leaves behind and so the normal
    /// case under many concurrent waiters. The polling fallback used to answer
    /// `JobNotFound` here, because a poll that finds no row cannot tell a purged
    /// job from one that never existed. That made the error a caller sees depend
    /// on whether a notification happened to arrive.
    ///
    /// A poll that watched the row run first *can* tell, and that is the whole fix: the wait remembers it. Only a
    /// claimed row counts — see the test below for the row that vanishes while still `queued` — and only when result
    /// retention is short enough to have deleted it since, which is why this job keeps its result for a millisecond.
    #[sqlx::test(migrations = "./migrations")]
    async fn test_a_lost_completion_of_a_purged_row_still_reports_an_expired_result(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        let Some(mut stats) = crate::Stats::new(&db.database).await else {
            return crate::Stats::skipped("test_a_lost_completion_of_a_purged_row_still_reports_an_expired_result");
        };
        let fetches = stats.since_now("%FROM pgqueue.jobs WHERE id = $1 AND queue = $2%").await;

        let short_lived = double::job(21).retention(JobRetention::For(Duration::from_millis(1)));
        let handle = db.queue.enqueue(short_lived).await.unwrap().unwrap();
        let claimed = pgqueue::__test_support::dequeue(&db.queue, 1, uuid::Uuid::now_v7()).await.unwrap();
        assert_eq!(claimed.iter().map(|job| job.id).collect::<Vec<_>>(), [handle.id()]);
        let waiter = {
            let handle = handle.clone();
            tokio::spawn(async move { handle.wait(Some(Duration::from_secs(10))).await })
        };
        wait_for_done_listener(&db).await;
        // The enqueue above issues no fetch, so every call counted here is the waiter's own poll. Two of them prove it
        // saw the row running and went back to waiting rather than merely having subscribed.
        stats.wait_for_calls(&fetches, 2, "waiter never polled the running row").await;

        // Finish and purge the row in one transaction and send no NOTIFY.
        let mut tx = db.queue.pool().begin().await.unwrap();
        sqlx::query(
            "UPDATE pgqueue.jobs SET status = 'complete', result = '42'::jsonb, completed_at = now() WHERE id = $1",
        )
        .bind(handle.id())
        .execute(&mut *tx)
        .await
        .unwrap();
        sqlx::query("DELETE FROM pgqueue.jobs WHERE id = $1").bind(handle.id()).execute(&mut *tx).await.unwrap();
        tx.commit().await.unwrap();

        let error = waiter.await.unwrap().unwrap_err();
        assert!(matches!(error, Error::ResultExpired(id) if id == handle.id()), "{error}");
    }

    /// The same lost notification, but for a job that *failed* and was purged on a short failure retention. Seen
    /// claimed and then gone, it used to be reported as an expired result — a completed job — and a caller trusting
    /// that skipped compensating for work that never succeeded. With the result kept far longer than the time since the
    /// row was seen, retention can explain the deletion only as a failure or abort, which the wait cannot tell apart:
    /// it reports the job missing.
    #[sqlx::test(migrations = "./migrations")]
    async fn test_a_lost_failure_of_a_purged_row_is_not_reported_as_a_completion(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        let Some(mut stats) = crate::Stats::new(&db.database).await else {
            return crate::Stats::skipped("test_a_lost_failure_of_a_purged_row_is_not_reported_as_a_completion");
        };
        let fetches = stats.since_now("%FROM pgqueue.jobs WHERE id = $1 AND queue = $2%").await;

        let short_failures = double::job(21).failed_retention(JobRetention::For(Duration::from_millis(1)));
        let handle = db.queue.enqueue(short_failures).await.unwrap().unwrap();
        let claimed = pgqueue::__test_support::dequeue(&db.queue, 1, uuid::Uuid::now_v7()).await.unwrap();
        assert_eq!(claimed.iter().map(|job| job.id).collect::<Vec<_>>(), [handle.id()]);
        let waiter = {
            let handle = handle.clone();
            tokio::spawn(async move { handle.wait(Some(Duration::from_secs(10))).await })
        };
        wait_for_done_listener(&db).await;
        stats.wait_for_calls(&fetches, 2, "waiter never polled the running row").await;

        // Fail and purge the row in one transaction and send no NOTIFY.
        let mut tx = db.queue.pool().begin().await.unwrap();
        sqlx::query(
            "UPDATE pgqueue.jobs SET status = 'failed', error = 'failed: boom', completed_at = now() WHERE id = $1",
        )
        .bind(handle.id())
        .execute(&mut *tx)
        .await
        .unwrap();
        sqlx::query("DELETE FROM pgqueue.jobs WHERE id = $1").bind(handle.id()).execute(&mut *tx).await.unwrap();
        tx.commit().await.unwrap();

        let error = waiter.await.unwrap().unwrap_err();
        assert!(matches!(error, Error::JobNotFound(id) if id == handle.id()), "{error}");
    }

    /// A row last seen `queued` that then vanishes was not finished by anyone: retention deletes only finished rows, so
    /// what took it is `Queue::purge_queued_jobs` — which documents exactly this answer — or a foreign `DELETE`.
    /// Remembering "seen alive" instead of "seen claimed" reported such a job as completed with its result expired, and
    /// a caller trusting that skips work that never ran.
    #[sqlx::test(migrations = "./migrations")]
    async fn test_wait_on_a_purged_queued_job_reports_it_missing(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        let Some(mut stats) = crate::Stats::new(&db.database).await else {
            return crate::Stats::skipped("test_wait_on_a_purged_queued_job_reports_it_missing");
        };
        let fetches = stats.since_now("%FROM pgqueue.jobs WHERE id = $1 AND queue = $2%").await;

        let handle = db.queue.enqueue(double::job(21)).await.unwrap().unwrap();
        let waiter = {
            let handle = handle.clone();
            tokio::spawn(async move { handle.wait(Some(Duration::from_secs(10))).await })
        };
        wait_for_done_listener(&db).await;
        stats.wait_for_calls(&fetches, 2, "waiter never polled the queued row").await;

        assert_eq!(db.queue.purge_queued_jobs(None, 10).await.unwrap(), 1);

        let error = waiter.await.unwrap().unwrap_err();
        assert!(matches!(error, Error::JobNotFound(id) if id == handle.id()), "{error}");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn test_wait_reports_expired_result_when_completed_row_was_purged(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        let handle = db.queue.enqueue(double::job(21)).await.unwrap().unwrap();
        let waiter = {
            let handle = handle.clone();
            tokio::spawn(async move { handle.wait(Some(Duration::from_secs(5))).await })
        };
        wait_for_done_listener(&db).await;

        // Delete the row and send its completion NOTIFY atomically,
        // reproducing retention purging a completed row before the waiter
        // could re-fetch its result.
        let channel = pgqueue::__test_support::done_channel(db.queue.name());
        let payload = format!(r#"{{"id":"{}","status":"complete"}}"#, handle.id());
        let mut tx = db.queue.pool().begin().await.unwrap();
        sqlx::query("DELETE FROM pgqueue.jobs WHERE id = $1").bind(handle.id()).execute(&mut *tx).await.unwrap();
        sqlx::query("SELECT pg_notify($1, $2)").bind(channel.as_str()).bind(payload).execute(&mut *tx).await.unwrap();
        tx.commit().await.unwrap();

        let err = waiter.await.unwrap().unwrap_err();
        assert!(matches!(err, Error::ResultExpired(id) if id == handle.id()), "{err}");
    }

    /// Records that a wait logged the failure of its event-triggered re-fetch, the one arm that has to keep the
    /// completion event rather than consume it.
    #[derive(Clone, Default)]
    struct FailedRefetch(Arc<AtomicBool>);

    impl<S: tracing::Subscriber> Layer<S> for FailedRefetch {
        fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
            struct Message(bool);
            impl tracing::field::Visit for Message {
                fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                    if field.name() == "message" {
                        self.0 |= format!("{value:?}") == "job completion read failed; falling back to polling";
                    }
                }
            }
            let mut message = Message(false);
            event.record(&mut message);
            if message.0 {
                self.0.store(true, Ordering::SeqCst);
            }
        }
    }

    /// The completion event is the only durable-in-memory record of *which*
    /// terminal state retention is about to erase. A failed immediate re-fetch
    /// must not discard it: after the row is swept, polling alone can report
    /// only a generic expired result.
    #[sqlx::test(migrations = "./migrations")]
    async fn test_wait_preserves_a_failed_event_across_a_read_timeout_and_retention_sweep(pool: PgPool) {
        // One second, not less: the same timeout bounds opening this pool's connection, which a machine busy with
        // the parallel suite can take hundreds of milliseconds to do.
        let query_pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_secs(1))
            .connect_with(pool.connect_options().as_ref().clone())
            .await
            .unwrap();
        let db = TestDb::new(query_pool.clone()).await;
        let control = TestDb::new(pool.clone()).await;
        let Some(mut stats) = crate::Stats::new(&db.database).await else {
            return crate::Stats::skipped(
                "test_wait_preserves_a_failed_event_across_a_read_timeout_and_retention_sweep",
            );
        };
        let fetches = stats.since_now("%FROM pgqueue.jobs WHERE id = $1 AND queue = $2%").await;

        let handle = db
            .queue
            .enqueue(fails_if_odd::job(3).failed_retention(JobRetention::For(Duration::from_millis(1))))
            .await
            .unwrap()
            .unwrap();
        let failed_refetch = FailedRefetch::default();
        let waiter = {
            let handle = handle.clone();
            let subscriber = tracing_subscriber::registry::Registry::default().with(failed_refetch.clone());
            tokio::spawn(async move { handle.wait(Some(Duration::from_secs(10))).await }.with_subscriber(subscriber))
        };
        wait_for_done_listener(&db).await;
        stats.wait_for_calls(&fetches, 1, "waiter never read the live job").await;

        // Occupy the waiter's entire query pool only after its first successful
        // read. The independent LISTEN connection still receives the finish,
        // but the event-triggered re-fetch reaches its acquire timeout.
        let held = query_pool.acquire().await.unwrap();
        let active = control.queue.dequeue(1, uuid::Uuid::now_v7()).await.unwrap().remove(0);
        assert!(control.queue.finish(&active, JobStatus::Failed, None, Some("odd number 3")).await.unwrap());
        // Held until the re-fetch has actually failed, not for a fixed while: released any sooner, the re-fetch
        // acquires the connection after the sweep, finds the row gone, and resolves from the event it is still
        // holding — a wait that discarded the event on a failed read passed that way every time.
        crate::wait_until(
            Duration::from_secs(5),
            Duration::from_millis(10),
            "the event-triggered re-fetch never failed",
            || async { failed_refetch.0.load(Ordering::SeqCst) },
        )
        .await;
        assert!(!waiter.is_finished(), "the wait ended while its query pool was unavailable");

        let mut sweeper = control.queue.sweeper();
        let report = sweeper.sweep().await.unwrap();
        assert_eq!(report.purged_jobs, 1, "the terminal row was not removed by retention");
        drop(held);

        let error = waiter.await.unwrap().unwrap_err();
        assert!(matches!(error, Error::Job(ref job) if job.kind == JobErrorKind::Failed), "{error}");
    }

    //noinspection SqlNoDataSourceInspection
    #[sqlx::test(migrations = "./migrations")]
    async fn test_foreign_notifications_do_not_postpone_fallback_polling(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        let handle = db.queue.enqueue(double::job(21).delay(Duration::from_secs(60))).await.unwrap().unwrap();
        // The deadline is far longer than the fallback poll it is testing: a
        // waiter that lets foreign traffic postpone that poll never polls at
        // all, so it fails here however long the deadline is — while a tight
        // one only asks that the machine be fast, which under `cargo llvm-cov`
        // and a parallel suite it is not.
        let waiter = {
            let handle = handle.clone();
            tokio::spawn(async move { handle.wait(Some(Duration::from_secs(5))).await })
        };
        wait_for_done_listener(&db).await;

        let channel = pgqueue::__test_support::done_channel(db.queue.name());
        let pool = db.queue.pool().clone();
        let (flowing, traffic_started) = tokio::sync::oneshot::channel();
        let notifier = tokio::spawn(async move {
            let mut conn = pool.acquire().await.unwrap();
            let mut flowing = Some(flowing);
            // Foreign traffic for the whole of the waiter's deadline, not just
            // its first second.
            for _ in 0..600 {
                let payload = format!(r#"{{"id":"{}","status":"complete"}}"#, uuid::Uuid::now_v7());
                sqlx::query("SELECT pg_notify($1, $2)")
                    .bind(channel.as_str())
                    .bind(payload)
                    .execute(&mut *conn)
                    .await
                    .unwrap();
                if let Some(flowing) = flowing.take() {
                    let _ = flowing.send(());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        });
        // Traffic first, completion second: completed before the notifier had
        // sent anything, a poll that happened to fall due at once resolved the
        // wait with no foreign traffic to be postponed by.
        traffic_started.await.expect("the notifier died before sending any foreign traffic");

        // Complete the target without NOTIFY, reproducing a notification lost
        // during listener reconnect. The waiter must discover it on its deadline.
        sqlx::query(
            "UPDATE pgqueue.jobs SET status = 'complete', result = '42'::jsonb, \
             completed_at = now() WHERE id = $1",
        )
        .bind(handle.id())
        .execute(db.queue.pool())
        .await
        .unwrap();

        assert_eq!(waiter.await.unwrap().unwrap(), 42);
        notifier.abort();
        // Cancelled or finished, never panicked: a notifier that died early left
        // the waiter no foreign traffic to be postponed by, and the test proved
        // nothing. Finishing is fine — its 600 sends outlast the wait's deadline.
        if let Err(error) = notifier.await {
            assert!(error.is_cancelled(), "the foreign traffic stopped before the wait resolved: {error}");
        }
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn test_enqueue_and_wait_resolves_results_from_state_backed_handlers(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        let (token, run) = spawn_worker(db.queue.clone());

        let out: String = db.queue.enqueue_and_wait(shared::job(()), Some(Duration::from_secs(10))).await.unwrap();
        assert_eq!(out, "from-state");

        token.cancel();
        run.await.unwrap();
    }

    //noinspection SqlNoDataSourceInspection
    #[sqlx::test(migrations = "./migrations")]
    async fn test_malformed_done_notifications_are_tolerated(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        let Some(mut stats) = crate::Stats::new(&db.database).await else {
            return crate::Stats::skipped("test_malformed_done_notifications_are_tolerated");
        };
        let fetches = stats.since_now("%FROM pgqueue.jobs WHERE id = $1 AND queue = $2%").await;

        // Blast garbage onto the done channel while a waiter is subscribed. The listener must log and continue, and
        // still deliver the real completion queued behind it.
        let handle = db.queue.enqueue(double::job(5)).await.unwrap().unwrap();
        let waiter = {
            let handle = handle.clone();
            tokio::spawn(async move { handle.wait(Some(Duration::from_secs(10))).await })
        };
        wait_for_done_listener(&db).await;
        stats.wait_for_calls(&fetches, 1, "waiter never polled the row").await;
        let done_channel = pgqueue::__test_support::done_channel(db.queue.name());
        for _ in 0..100 {
            sqlx::query("SELECT pg_notify($1, $2)")
                .bind(done_channel.as_str())
                .bind("not json at all")
                .execute(&pool)
                .await
                .unwrap();
        }

        // The real completion, of a row already gone when anything reads it. Polling alone can only report that row
        // missing — the waiter and the worker both poll, which is how a listener that died on the garbage used to go
        // unnoticed here — so only a listener that survived can say it failed.
        let mut transaction = pool.begin().await.unwrap();
        sqlx::query("DELETE FROM pgqueue.jobs WHERE id = $1")
            .bind(handle.id())
            .execute(&mut *transaction)
            .await
            .unwrap();
        sqlx::query("SELECT pg_notify($1, $2)")
            .bind(done_channel.as_str())
            .bind(format!(r#"{{"id":"{}","status":"failed"}}"#, handle.id()))
            .execute(&mut *transaction)
            .await
            .unwrap();
        transaction.commit().await.unwrap();

        match waiter.await.unwrap() {
            Err(Error::Job(error)) => assert_eq!(error.kind, JobErrorKind::Failed, "{error}"),
            other => panic!("the completion queued behind the garbage was not delivered: {other:?}"),
        }
    }
}

mod typed {
    //! End-to-end tests of the `#[pgqueue::job]` macro output: typed enqueue,
    //! config propagation, and the generated helpers.

    use sqlx::PgPool;
    use std::time::Duration;

    use crate::{EnqueueResultTestExt, TestDb};
    use pgqueue::{
        EnqueueResult, Error, JobConfig, JobErrorKind, JobRetention, JobRetryBackoff, JobState, JobStatus, JobType,
    };
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct SendEmail {
        to: String,
        body: String,
    }

    /// Sends an email (test fixture).
    #[pgqueue::job(
        max_attempts = 3,
        timeout_ms = 30_000,
        result_ttl_ms = 3_600_000,
        failed_ttl_ms = 86_400_000,
        retry_delay_ms = 250,
        max_backoff_ms = 60_000
    )]
    async fn send_email(args: SendEmail) -> anyhow::Result<String> {
        Ok(format!("sent to {}", args.to))
    }

    #[pgqueue::job(
        name = "cleanup_v2",
        timeout_ms = 0,
        result_ttl_ms = 0,
        priority = -5
    )]
    async fn cleanup(_: ()) -> anyhow::Result<u64> {
        Ok(42)
    }

    #[pgqueue::job]
    async fn with_state(args: u32, state: JobState<String>) -> Result<String, std::io::Error> {
        Ok(format!("{}-{args}", state.0))
    }

    #[test]
    fn test_job_macro_generates_name_and_config() {
        assert_eq!(send_email::NAME, "send_email");
        let config = send_email::config();
        assert_eq!(config.max_attempts, 3);
        assert_eq!(config.timeout, Some(Duration::from_secs(30)));
        assert_eq!(config.retention, JobRetention::For(Duration::from_secs(3600)));
        assert_eq!(config.failed_retention, JobRetention::For(Duration::from_secs(86_400)));
        assert_eq!(config.retry_delay, Duration::from_millis(250));
        assert_eq!(config.backoff, JobRetryBackoff::Exponential { max: Some(Duration::from_secs(60)) });
        assert_eq!(config.priority, 0);

        assert_eq!(cleanup::NAME, "cleanup_v2", "name attribute overrides the fn name");
        let config = cleanup::config();
        assert_eq!(config.timeout, None);
        assert_eq!(config.retention, JobRetention::DeleteImmediately);
        assert_eq!(config.failed_retention, JobConfig::default().failed_retention, "unset attributes keep the default");
        assert_eq!(config.priority, -5);

        // No attributes: pure defaults.
        assert_eq!(with_state::config(), JobConfig::default());

        // The generated struct is Copy/Clone/Debug.
        let job = send_email;
        #[allow(clippy::clone_on_copy)]
        let _ = job.clone();
        assert_eq!(format!("{job:?}"), "send_email");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn test_generated_call_invokes_the_original_function(_pool: PgPool) {
        let out = send_email::call(SendEmail { to: "a@b.c".into(), body: "hi".into() }).await.unwrap();
        assert_eq!(out, "sent to a@b.c");
        assert_eq!(cleanup::call(()).await.unwrap(), 42);
    }

    #[test]
    fn test_erased_handler_carries_name_and_config() {
        let handler = send_email::erased();
        assert_eq!(handler.name(), "send_email");
        assert_eq!(handler.config().max_attempts, 3);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn test_typed_enqueue_round_trips_payload_and_config(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        let result =
            db.queue.enqueue(send_email::job(SendEmail { to: "a@b.c".into(), body: "hello".into() })).await.unwrap();
        assert!(result.is_enqueued());
        let id = result.job_id();
        let handle = result.into_job_handle();
        assert_eq!(handle.id(), id);

        let row = handle.fetch_job().await.unwrap();
        assert_eq!(row.name, "send_email");
        assert_eq!(row.status, JobStatus::Queued);
        assert_eq!(row.max_attempts, 3);
        assert_eq!(row.timeout(), Some(Duration::from_secs(30)));
        assert_eq!(row.retry_delay_ms, 250);
        let payload: SendEmail = serde_json::from_value(row.payload).unwrap();
        assert_eq!(payload, SendEmail { to: "a@b.c".into(), body: "hello".into() });
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn test_typed_enqueue_in_commits_with_the_caller_transaction(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        let mut transaction = db.queue.pool().begin().await.unwrap();
        let result = db
            .queue
            .enqueue_in(
                &mut transaction,
                send_email::job(SendEmail { to: "tx@example.com".into(), body: "hello".into() }),
            )
            .await
            .unwrap();
        let handle = result.into_job_handle();
        assert!(matches!(handle.fetch_job().await, Err(Error::JobNotFound(_))));
        transaction.commit().await.unwrap();
        assert_eq!(handle.fetch_job().await.unwrap().name, "send_email");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn test_typed_enqueue_in_anchors_delay_to_the_statement_in_an_aged_transaction(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        let mut transaction = db.queue.pool().begin().await.unwrap();
        sqlx::query("SELECT pg_sleep(0.3)").execute(&mut *transaction).await.unwrap();
        let enqueue_started = sqlx::query_scalar::<_, jiff_sqlx::Timestamp>("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await
            .unwrap()
            .to_jiff();

        let handle = db
            .queue
            .enqueue_in(&mut transaction, cleanup::job(()).delay(Duration::from_secs(1)))
            .await
            .unwrap()
            .unwrap();
        let (scheduled_at, enqueued_at) = sqlx::query_as::<_, (jiff_sqlx::Timestamp, jiff_sqlx::Timestamp)>(
            "SELECT scheduled_at, enqueued_at FROM pgqueue.jobs WHERE id = $1",
        )
        .bind(handle.id())
        .fetch_one(&mut *transaction)
        .await
        .unwrap();
        let scheduled_at = scheduled_at.to_jiff();
        let enqueued_at = enqueued_at.to_jiff();

        assert!(enqueued_at >= enqueue_started);
        assert!(
            scheduled_at >= enqueue_started + jiff::SignedDuration::from_millis(950),
            "delay was anchored before enqueue: {} < {}",
            scheduled_at,
            enqueue_started,
        );
        transaction.rollback().await.unwrap();
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn test_typed_enqueue_reports_dedupe_and_rejects_a_foreign_owner(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        let existing = db
            .queue
            .enqueue(
                send_email::job(SendEmail { to: "owner@example.com".into(), body: "first".into() })
                    .dedupe_key("typed-owner"),
            )
            .await
            .unwrap()
            .unwrap();

        let mut transaction = db.queue.pool().begin().await.unwrap();
        let duplicate = db
            .queue
            .enqueue_in(
                &mut transaction,
                send_email::job(SendEmail { to: "ignored@example.com".into(), body: "ignored".into() })
                    .dedupe_key("typed-owner"),
            )
            .await
            .unwrap();
        assert_eq!(duplicate.job_id(), existing.id());
        assert!(matches!(
            duplicate,
            EnqueueResult::Deduplicated(ref handle) if handle.id() == existing.id()
        ));
        transaction.rollback().await.unwrap();

        let error = db.queue.enqueue(cleanup::job(()).dedupe_key("typed-owner")).await.unwrap_err();
        assert!(error.to_string().contains("belongs to job"), "{error}");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn test_aborting_delete_immediately_job_resolves_as_a_job_result(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        let handle = db.queue.enqueue(cleanup::job(())).await.unwrap().expect("enqueued");

        assert!(handle.abort("not needed").await.unwrap());
        let error = handle.wait_value(Some(Duration::from_secs(1))).await.unwrap_err();
        assert!(matches!(error, Error::Job(ref job) if job.kind == JobErrorKind::Aborted), "{error}");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn test_typed_job_builder_overrides_attribute_config(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        let handle = db
            .queue
            .enqueue(
                send_email::job(SendEmail { to: "x".into(), body: "y".into() })
                    .max_attempts(9)
                    .timeout(Duration::from_secs(5))
                    .retention(JobRetention::Forever)
                    .retry_delay(Duration::from_millis(10))
                    .backoff(JobRetryBackoff::None)
                    .priority(4)
                    .meta(serde_json::json!({"req": 1})),
            )
            .await
            .unwrap()
            .unwrap();

        let row = handle.fetch_job().await.unwrap();
        assert_eq!(row.max_attempts, 9);
        assert_eq!(row.timeout(), Some(Duration::from_secs(5)));
        assert_eq!(row.retention(), JobRetention::Forever);
        assert_eq!(row.retry_delay_ms, 10);
        assert_eq!(row.backoff, JobRetryBackoff::None);
        assert_eq!(row.priority, 4);
        assert_eq!(row.meta, serde_json::json!({"req": 1}));
    }

    /// `timeout()` could only ever *set* a timeout, so a job whose attribute
    /// declares one had no per-enqueue route back to the unlimited state that
    /// `#[pgqueue::job(timeout_ms = 0)]` expresses at the type level.
    #[sqlx::test(migrations = "./migrations")]
    async fn test_typed_job_builder_can_remove_the_attributes_timeout(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        let untimed = db
            .queue
            .enqueue(send_email::job(SendEmail { to: "x".into(), body: "y".into() }).no_timeout())
            .await
            .unwrap()
            .unwrap();

        let row = untimed.fetch_job().await.unwrap();
        assert_eq!(row.timeout(), None);
        assert_eq!(row.timeout_ms, None, "and no timeout is stored on the row");

        // The attribute's own timeout is still what an untouched enqueue gets.
        let timed =
            db.queue.enqueue(send_email::job(SendEmail { to: "x".into(), body: "y".into() })).await.unwrap().unwrap();
        assert_eq!(timed.fetch_job().await.unwrap().timeout(), Some(Duration::from_secs(30)));
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn test_typed_job_builder_applies_dedupe_and_scheduling(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        let first =
            db.queue.enqueue(cleanup::job(()).dedupe_key("cron:cleanup").delay(Duration::from_secs(60))).await.unwrap();
        assert!(first.is_some());
        let row = first.unwrap().fetch_job().await.unwrap();
        assert_eq!(
            row.scheduled_at.duration_since(row.enqueued_at).as_micros(),
            60_000_000,
            "relative delay and enqueue time must share the same database clock"
        );

        // Same dedupe key while live: dedupe.
        let second = db.queue.enqueue(cleanup::job(()).dedupe_key("cron:cleanup")).await.unwrap();
        assert!(second.is_none());

        // `at` pins an absolute schedule.
        let when = jiff::Timestamp::now() + jiff::SignedDuration::from_secs(120);
        let handle = db.queue.enqueue(cleanup::job(()).at(when)).await.unwrap().unwrap();
        let row = handle.fetch_job().await.unwrap();
        assert!(row.scheduled_at.duration_since(when).as_millis().abs() < 5);

        let error = db.queue.enqueue(cleanup::job(()).delay(Duration::MAX)).await.unwrap_err();
        use Error::Config;
        assert!(matches!(error, Config(_)), "{error}");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn test_job_handle_aborts_and_refreshes(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        let handle = db.queue.enqueue(cleanup::job(())).await.unwrap().unwrap();
        assert_ne!(handle.id(), uuid::Uuid::nil());
        assert!(handle.abort("changed my mind").await.unwrap());
        assert_eq!(handle.fetch_job().await.unwrap().status, JobStatus::Aborted);
        assert!(format!("{handle:?}").contains("JobHandle"));
    }
}

mod batch {
    //! `Queue::enqueue_batch`: one statement, one wakeup, typed handles in
    //! input order.

    use sqlx::PgPool;
    use std::cell::Cell;
    use std::time::Duration;

    use crate::{EnqueueResultTestExt, TestDb, hold_gate, install_statement_gate, wait_for_advisory_waiter};
    use pgqueue::{
        EnqueueResult, Error, JobBuilder, JobStatus, JobType, MAX_ENQUEUE_BATCH_BYTES, MAX_ENQUEUE_BATCH_JOBS, Worker,
    };
    use tokio_util::sync::CancellationToken;

    /// The advisory namespace a test gate parks the batch insert on.
    const BATCH_INSERT_GATE: i32 = 7_401_002;

    #[pgqueue::job]
    async fn twice(args: u32) -> anyhow::Result<u32> {
        Ok(args * 2)
    }

    #[pgqueue::job]
    async fn other(args: u32) -> anyhow::Result<u32> {
        Ok(args)
    }

    #[pgqueue::job]
    async fn echo(args: String) -> anyhow::Result<usize> {
        Ok(args.len())
    }

    async fn wakeups(db: &TestDb) -> sqlx::postgres::PgListener {
        let mut listener = sqlx::postgres::PgListener::connect_with(db.queue.pool()).await.unwrap();
        listener.listen(&pgqueue::__test_support::notify_channel(db.queue.name())).await.unwrap();
        listener
    }

    async fn queued(db: &TestDb) -> i64 {
        db.queue.counts().await.unwrap().queued
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn test_enqueue_batch_inserts_every_job_in_order_with_one_wakeup(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        let mut wakeups = wakeups(&db).await;

        let results = db.queue.enqueue_batch((0..25).map(twice::job)).await.unwrap();
        assert_eq!(results.len(), 25);
        for (n, result) in results.iter().enumerate() {
            assert!(result.is_enqueued());
            let row = result.job_handle().fetch_job().await.unwrap();
            assert_eq!(row.payload, serde_json::json!(n));
            assert_eq!(row.name, twice::NAME);
            assert_eq!(row.status, JobStatus::Queued);
        }
        // Ids are time-ordered, so the batch dequeues in input order.
        let ids = results.iter().map(|result| result.job_id()).collect::<Vec<_>>();
        let mut sorted = ids.clone();
        sorted.sort();
        assert_eq!(ids, sorted);
        assert_eq!(queued(&db).await, 25);
        tokio::time::timeout(Duration::from_secs(5), wakeups.recv())
            .await
            .expect("a batch with due rows wakes workers")
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(300), wakeups.recv()).await.is_err(),
            "one wakeup per batch"
        );

        // The handles are the ordinary kind: a worker runs the jobs and each
        // handle yields its result.
        let worker = Worker::builder(db.queue.clone())
            .register_job(twice)
            .timers(crate::test_timers())
            .poll_interval(Duration::from_millis(50))
            .concurrency(4)
            .build()
            .unwrap();
        let shutdown = CancellationToken::new();
        let run = tokio::spawn(worker.run_until(shutdown.clone()));
        for (n, result) in results.into_iter().enumerate() {
            let value = result.into_job_handle().wait(Some(Duration::from_secs(10))).await.unwrap();
            assert_eq!(value, u32::try_from(n).unwrap() * 2);
        }
        shutdown.cancel();
        run.await.unwrap().unwrap();

        // An empty batch is a no-op, and a batch of delayed rows wakes nobody.
        assert!(db.queue.enqueue_batch(std::iter::empty::<JobBuilder<twice>>()).await.unwrap().is_empty());
        let delayed = (0..3).map(|n| twice::job(n).delay(Duration::from_secs(3600)));
        assert_eq!(db.queue.enqueue_batch(delayed).await.unwrap().len(), 3);
        assert!(
            tokio::time::timeout(Duration::from_millis(300), wakeups.recv()).await.is_err(),
            "delayed rows wake nobody"
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn test_enqueue_batch_deduplicates_against_live_jobs_and_within_itself(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        let live = db.queue.enqueue(twice::job(1).dedupe_key("k1")).await.unwrap().unwrap();

        let results = db
            .queue
            .enqueue_batch([
                twice::job(2).dedupe_key("k1"),
                twice::job(3).dedupe_key("k2"),
                twice::job(4).dedupe_key("k2"),
                twice::job(5),
                twice::job(6).dedupe_key("k1"),
            ])
            .await
            .unwrap();
        assert!(matches!(&results[0], EnqueueResult::Deduplicated(handle) if handle.id() == live.id()));
        let EnqueueResult::Enqueued(k2) = &results[1] else { panic!("k2's first job inserts") };
        assert!(matches!(&results[2], EnqueueResult::Deduplicated(handle) if handle.id() == k2.id()));
        assert!(results[3].is_enqueued());
        assert!(matches!(&results[4], EnqueueResult::Deduplicated(handle) if handle.id() == live.id()));
        assert_eq!(queued(&db).await, 3);

        // In a transaction the same rules apply, visible only after commit.
        let mut transaction = pool.begin().await.unwrap();
        let in_transaction =
            db.queue.enqueue_batch_in(&mut transaction, [twice::job(7).dedupe_key("k3"), twice::job(8)]).await.unwrap();
        assert!(in_transaction.iter().all(EnqueueResult::is_enqueued));
        assert_eq!(queued(&db).await, 3, "invisible before commit");
        transaction.commit().await.unwrap();
        assert_eq!(queued(&db).await, 5);
    }

    /// The batch takes the dedupe lock a single keyed enqueue takes, so the
    /// two serialize their decisions instead of racing to `ON CONFLICT`.
    #[sqlx::test(migrations = "./migrations")]
    async fn test_enqueue_batch_takes_the_single_enqueue_dedupe_lock(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        let namespace = pgqueue::__test_support::dedupe_enqueue_lock_key(&db.database);
        // Held by a real single keyed enqueue, in a transaction left open: its dedupe lock is transaction-scoped, so it
        // lasts until the rollback. A copy of the lock's formula held here instead would stay green if the single
        // enqueue's own key drifted from the batch's.
        let mut single = pool.begin().await.unwrap();
        assert!(db.queue.enqueue_in(&mut single, twice::job(0).dedupe_key("shared")).await.unwrap().is_enqueued());

        let batch = tokio::spawn({
            let queue = db.queue.clone();
            async move { queue.enqueue_batch([twice::job(1).dedupe_key("shared"), twice::job(2)]).await }
        });
        wait_for_advisory_waiter(&pool, namespace, "the batch did not wait for the single enqueue's dedupe lock").await;
        assert!(!batch.is_finished(), "the batch must not decide the key while a single enqueue holds it");
        single.rollback().await.unwrap();
        let results = batch.await.unwrap().unwrap();
        assert!(results.iter().all(EnqueueResult::is_enqueued));
    }

    /// Parks the batch's insert on a gate, and while it waits takes its key
    /// with a row that never took the enqueue lock, exactly as a foreign SQL
    /// writer would. `name` is the foreign row's job name.
    async fn insert_foreign_holder_during_batch(
        db: &TestDb,
        name: &str,
        batch: impl IntoIterator<Item = JobBuilder<twice>>,
    ) -> (uuid::Uuid, Result<Vec<EnqueueResult<pgqueue::JobHandle<twice>>>, Error>) {
        install_statement_gate(
            db.queue.pool(),
            "wait_at_batch_insert",
            BATCH_INSERT_GATE,
            "INSERT",
            "NEW.meta ->> 'gate' = 'batch'",
        )
        .await;
        let gate = hold_gate(db.queue.pool(), BATCH_INSERT_GATE, &db.database).await;
        let batch = batch.into_iter().map(|job| job.meta(serde_json::json!({"gate": "batch"}))).collect::<Vec<_>>();
        let enqueue = tokio::spawn({
            let queue = db.queue.clone();
            async move { queue.enqueue_batch(batch).await }
        });
        crate::wait_for_lock_waiter(db, "%WITH input AS (%", "the batch did not reach its insert").await;
        let foreign = sqlx::query_scalar::<_, uuid::Uuid>(
            r#"INSERT INTO pgqueue.jobs (queue, name, payload, dedupe_key, status, max_attempts)
               VALUES ($1, $2, 'null'::jsonb, 'raced', 'queued', 1)
               RETURNING id"#,
        )
        .bind(db.queue.name())
        .bind(name)
        .fetch_one(db.queue.pool())
        .await
        .unwrap();
        gate.rollback().await.unwrap();
        (foreign, enqueue.await.unwrap())
    }

    /// A key whose predicted holder loses to a foreign writer is remapped for
    /// every job of the batch that carries it: the first, whose row was
    /// dropped, and the later ones, which were deduplicated against that
    /// row's id and would otherwise hold a handle to a job that never landed.
    #[sqlx::test(migrations = "./migrations")]
    async fn test_enqueue_batch_reports_every_duplicate_against_a_racing_foreign_holder(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        let batch = [
            twice::job(1).dedupe_key("raced"),
            twice::job(2).dedupe_key("raced"),
            twice::job(3),
        ];
        let (foreign, results) = insert_foreign_holder_during_batch(&db, twice::NAME, batch).await;
        let results = results.unwrap();
        assert!(matches!(&results[0], EnqueueResult::Deduplicated(handle) if handle.id() == foreign));
        assert!(matches!(&results[1], EnqueueResult::Deduplicated(handle) if handle.id() == foreign));
        assert!(results[2].is_enqueued());
        for result in &results {
            result.job_handle().fetch_job().await.expect("every reported handle names a row that exists");
        }
        assert_eq!(queued(&db).await, 2);
    }

    /// The same race, but the foreign holder is of another job type: the batch
    /// is refused while its transaction can still roll back, so none of its
    /// rows are published without a handle.
    #[sqlx::test(migrations = "./migrations")]
    async fn test_enqueue_batch_refuses_a_racing_foreign_holder_of_another_type_without_publishing(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        let batch = [twice::job(1), twice::job(2).dedupe_key("raced")];
        let (foreign, results) = insert_foreign_holder_during_batch(&db, other::NAME, batch).await;
        let error = results.unwrap_err();
        assert!(matches!(error, Error::Config(_)), "{error}");
        assert!(error.to_string().contains(other::NAME), "{error}");
        assert_eq!(queued(&db).await, 1, "only the foreign row remains");
        assert_eq!(db.queue.fetch_job(foreign).await.unwrap().unwrap().name, other::NAME);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn test_enqueue_batch_refuses_a_key_of_another_job_type_before_inserting(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        db.queue.enqueue(other::job(1).dedupe_key("shared")).await.unwrap();
        let error = db.queue.enqueue_batch([twice::job(1), twice::job(2).dedupe_key("shared")]).await.unwrap_err();
        assert!(matches!(error, Error::Config(_)), "{error}");
        assert!(error.to_string().contains("shared"), "{error}");
        assert_eq!(queued(&db).await, 1, "nothing from the refused batch was inserted");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn test_enqueue_batch_refuses_oversized_batches_before_sending(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        for transactional in [false, true] {
            let mut transaction = pool.begin().await.unwrap();
            let consumed = Cell::new(0);
            let too_many_jobs = (0..MAX_ENQUEUE_BATCH_JOBS + 2).map(|_| {
                consumed.set(consumed.get() + 1);
                twice::job(1)
            });
            let error = if transactional {
                db.queue.enqueue_batch_in(&mut transaction, too_many_jobs).await.unwrap_err()
            } else {
                db.queue.enqueue_batch(too_many_jobs).await.unwrap_err()
            };
            assert!(matches!(error, Error::Config(_)), "{error}");
            assert!(error.to_string().contains(&MAX_ENQUEUE_BATCH_JOBS.to_string()), "{error}");
            assert_eq!(consumed.get(), MAX_ENQUEUE_BATCH_JOBS + 1, "stop reading when the count exceeds the limit");

            // Each document fits, but collection must stop as soon as their combined size exceeds the batch limit.
            let payload = "x".repeat(1_000_000);
            let fitting = MAX_ENQUEUE_BATCH_BYTES / payload.len();
            consumed.set(0);
            let too_many_bytes = (0..fitting + 2).map(|_| {
                consumed.set(consumed.get() + 1);
                echo::job(payload.clone())
            });
            let error = if transactional {
                db.queue.enqueue_batch_in(&mut transaction, too_many_bytes).await.unwrap_err()
            } else {
                db.queue.enqueue_batch(too_many_bytes).await.unwrap_err()
            };
            assert!(matches!(error, Error::Config(_)), "{error}");
            assert!(error.to_string().contains("bytes"), "{error}");
            assert_eq!(consumed.get(), fitting + 1, "stop reading when the bytes exceed the limit");
            transaction.commit().await.unwrap();
        }

        // Per-job validation is `enqueue`'s: one bad job refuses the batch.
        let error = db.queue.enqueue_batch([twice::job(1), twice::job(2).max_attempts(0)]).await.unwrap_err();
        assert!(matches!(error, Error::Config(_)), "{error}");
        assert_eq!(queued(&db).await, 0);
    }

    /// The advisory locks this transaction holds, as the server counts them.
    async fn advisory_locks_held(transaction: &mut sqlx::PgTransaction<'_>) -> i64 {
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND pid = pg_backend_pid()",
        )
        .fetch_one(&mut **transaction)
        .await
        .unwrap()
    }

    /// Every distinct dedupe key a transaction publishes holds an entry of PostgreSQL's lock table until the
    /// transaction ends, and that table is one per server, sized for a few dozen locks per connection and shared by
    /// every database on it: about 14,900 entries at stock settings. A backfill publishing keyed jobs batch after batch
    /// in one transaction took a thousand more per batch, and held open near full it left every session on the server
    /// failing with `out of shared memory`, new connections included. A transaction now holds at most a budget of
    /// them. Past it a keyed publish is refused before it locks anything, and the transaction stays usable to commit
    /// what it has.
    #[sqlx::test(migrations = "./migrations")]
    async fn test_a_transaction_holds_a_bounded_number_of_dedupe_locks(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        // The shipped budget is half the lock table the server sizes for its connections, and at least a batch...
        let (per_transaction, connections) = sqlx::query_as::<_, (i64, i64)>(
            "SELECT current_setting('max_locks_per_transaction')::bigint, current_setting('max_connections')::bigint",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            pgqueue::__test_support::dedupe_lock_budget(&db.queue),
            (per_transaction * connections / 2).max(MAX_ENQUEUE_BATCH_JOBS as i64)
        );
        // ...and this one keeps the test from taking thousands of locks on a server the suite shares.
        pgqueue::__test_support::set_dedupe_lock_budget(&db.queue, 2_500);
        let keyed = |numbers: std::ops::Range<u32>| numbers.map(|n| twice::job(n).dedupe_key(format!("budget:{n}")));

        let mut transaction = pool.begin().await.unwrap();
        db.queue.enqueue_batch_in(&mut transaction, keyed(0..1_000)).await.unwrap();
        db.queue.enqueue_batch_in(&mut transaction, keyed(1_000..2_000)).await.unwrap();
        assert_eq!(advisory_locks_held(&mut transaction).await, 2_000);
        // Keys the transaction already holds take no new lock, so they publish however near the budget it is.
        let again = db.queue.enqueue_batch_in(&mut transaction, keyed(0..1_000)).await.unwrap();
        assert!(again.iter().all(EnqueueResult::is_deduplicated));
        assert_eq!(advisory_locks_held(&mut transaction).await, 2_000);

        // A batch that would carry it past the budget is refused before it locks or inserts anything.
        let error = db.queue.enqueue_batch_in(&mut transaction, keyed(2_000..3_000)).await.unwrap_err();
        assert!(matches!(&error, Error::Config(message) if message.contains("2500")), "{error}");
        assert_eq!(advisory_locks_held(&mut transaction).await, 2_000);

        // A savepoint's locks go with it, and so does its share of the budget.
        sqlx::query("SAVEPOINT keyed").execute(&mut *transaction).await.unwrap();
        db.queue.enqueue_batch_in(&mut transaction, keyed(2_000..2_500)).await.unwrap();
        assert_eq!(advisory_locks_held(&mut transaction).await, 2_500);
        let error = db.queue.enqueue_in(&mut transaction, twice::job(0).dedupe_key("budget:last")).await.unwrap_err();
        assert!(matches!(error, Error::Config(_)), "{error}");
        sqlx::query("ROLLBACK TO SAVEPOINT keyed").execute(&mut *transaction).await.unwrap();
        assert_eq!(advisory_locks_held(&mut transaction).await, 2_000);
        let last = db.queue.enqueue_in(&mut transaction, twice::job(0).dedupe_key("budget:last")).await.unwrap();
        assert!(last.is_enqueued());
        assert_eq!(advisory_locks_held(&mut transaction).await, 2_001);

        // Neither refusal cost the transaction anything it had published.
        transaction.commit().await.unwrap();
        assert_eq!(queued(&db).await, 2_001);
    }
}

mod macros {
    //! Compile-pass and compile-fail tests for `#[pgqueue::job]` diagnostics.

    #[test]
    fn test_job_macro_cases_compile_as_expected() {
        let t = trybuild::TestCases::new();
        t.pass("tests/macros/pass.rs");
        t.pass("tests/macros/pass_cfg.rs");
        t.pass("tests/macros/pass_deprecated.rs");
        t.pass("tests/macros/pass_forbid_style.rs");
        t.pass("tests/macros/pass_hygiene.rs");
        t.pass("tests/macros/pass_inner_docs.rs");
        t.pass("tests/macros/pass_inner_lint_attrs.rs");
        t.pass("tests/macros/pass_lint_attrs.rs");
        t.pass("tests/macros/pass_macro_rules.rs");
        t.pass("tests/macros/pass_non_ascii_names.rs");
        t.pass("tests/macros/pass_static_extractor.rs");
        t.compile_fail("tests/macros/fail.rs");
        t.compile_fail("tests/macros/fail_deprecated.rs");
        t.compile_fail("tests/macros/fail_emoji_name.rs");
        t.compile_fail("tests/macros/fail_forbid.rs");
        t.compile_fail("tests/macros/fail_inner_lint_attrs.rs");
        t.compile_fail("tests/macros/fail_registration.rs");
    }
}

mod macro_telemetry {
    //! `#[tracing::instrument]` is the motivating example for leaving non-lint
    //! attributes on the hidden function, and it takes its span name from the
    //! identifier. Renaming that function to a private placeholder labelled
    //! every job's telemetry `__pgqueue_inner`, so the handler name was lost
    //! across all of it.

    use std::sync::{Arc, Mutex};

    use tracing_subscriber::layer::{Context, Layer, SubscriberExt};
    use tracing_subscriber::registry::Registry;

    /// Records the name of every span opened while it is the default.
    #[derive(Clone, Default)]
    struct RecordedSpans(Arc<Mutex<Vec<String>>>);

    impl<S: tracing::Subscriber> Layer<S> for RecordedSpans {
        fn on_new_span(&self, attrs: &tracing::span::Attributes<'_>, _id: &tracing::span::Id, _ctx: Context<'_, S>) {
            if let Ok(mut names) = self.0.lock() {
                names.push(attrs.metadata().name().to_string());
            }
        }
    }

    #[pgqueue::job(name = "instrumented_handler")]
    #[tracing::instrument]
    async fn instrumented_handler(_: ()) -> anyhow::Result<()> {
        Ok(())
    }

    #[test]
    fn test_instrumented_job_reports_the_handler_name_as_its_span() {
        let recorded = RecordedSpans::default();
        let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
        // Thread-local, so it takes precedence over the suite's global
        // subscriber without disturbing tests running on other threads.
        tracing::subscriber::with_default(Registry::default().with(recorded.clone()), || {
            runtime.block_on(instrumented_handler::call(())).expect("handler");
        });

        let names = recorded.0.lock().expect("recorded spans").clone();
        assert!(
            names.iter().any(|name| name == "instrumented_handler"),
            "job telemetry lost the handler name: {names:?}"
        );
        assert!(
            !names.iter().any(|name| name.contains("pgqueue_inner")),
            "the expansion's private placeholder leaked into telemetry: {names:?}"
        );
    }
}

mod wait_without_notifications {
    use std::time::Duration;

    use pgqueue::{Queue, Worker};
    use tokio_util::sync::CancellationToken;

    #[pgqueue::job(name = "wait_without_listener", max_attempts = 1)]
    async fn wait_without_listener(_: ()) -> anyhow::Result<u32> {
        Ok(42)
    }

    /// `wait` subscribes to completion notifications, but that needs a
    /// connection outside the query pool. Losing it must not fail a caller
    /// whose job runs and completes normally — the backing-off poll in
    /// `wait_inner` covers it.
    #[tokio::test]
    async fn test_enqueue_and_wait_falls_back_to_polling_when_the_listener_cannot_connect() {
        crate::init_tracing();
        let url = crate::fresh_database("wait_polling").await;
        let admin_queue = Queue::connect(&url).await.unwrap();
        let client_url = crate::limited_role_url(&url, -1).await;

        // A warm pool that can no longer open new connections: the caller can talk
        // to the database, but cannot start a LISTEN.
        let pool = sqlx::postgres::PgPoolOptions::new()
            .min_connections(2)
            .max_connections(2)
            .connect(&client_url)
            .await
            .unwrap();
        crate::warm_pool(&pool, 2).await;
        crate::revoke_connect(&url, &client_url).await;
        let client_queue = Queue::builder(&client_url).pool(pool).connect().await.unwrap();

        let shutdown = CancellationToken::new();
        let worker = Worker::builder(admin_queue.clone())
            .register_job(wait_without_listener)
            .timers(crate::test_timers())
            .build()
            .unwrap();
        let run = tokio::spawn(worker.run_until(shutdown.clone()));

        let value = client_queue
            .enqueue_and_wait(wait_without_listener::job(()), Some(Duration::from_secs(30)))
            .await
            .expect("wait must poll instead of surfacing the listener failure");
        assert_eq!(value, 42);

        shutdown.cancel();
        run.await.unwrap().unwrap();
    }

    /// The same promise for the *query* pool, not just the listener. A poll that
    /// cannot reach the database has learned nothing about the job, so it must
    /// not end the wait: propagating it abandoned every in-flight caller on the
    /// first `PoolTimedOut` of an outage — about thirty seconds in, whatever
    /// deadline they gave — while the job itself ran to completion and a later
    /// wait on the same id returned its result.
    #[tokio::test]
    async fn test_wait_polls_through_a_database_outage_instead_of_abandoning_the_caller() {
        crate::init_tracing();
        let url = crate::fresh_database("wait_outage").await;
        let admin_queue = Queue::connect(&url).await.unwrap();
        let client_url = crate::limited_role_url(&url, -1).await;
        let pool = sqlx::postgres::PgPoolOptions::new()
            .min_connections(2)
            .max_connections(2)
            .connect(&client_url)
            .await
            .unwrap();
        let client_queue = Queue::builder(&client_url).pool(pool).connect().await.unwrap();

        let handle = client_queue
            .enqueue(wait_without_listener::job(()))
            .await
            .expect("enqueue before the outage")
            .into_job_handle();

        // Sever the client completely: no new connections, and the warm ones
        // terminated. Every `fetch_job` this queue issues now fails.
        crate::revoke_connect(&url, &client_url).await;
        let role = client_url.split_once("://").and_then(|(_, rest)| rest.split_once(':')).unwrap().0.to_string();
        sqlx::query(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity
             WHERE datname = current_database() AND usename = $1",
        )
        .bind(&role)
        .execute(admin_queue.pool())
        .await
        .unwrap();

        let waiting = tokio::spawn(async move { handle.wait(Some(Duration::from_secs(60))).await });

        // Long enough for several failed polls at the backing-off interval. The
        // wait must still be outstanding rather than resolved into an error.
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert!(!waiting.is_finished(), "the wait ended on a transient database error");

        // The job runs and finishes while the caller cannot see the database.
        let shutdown = CancellationToken::new();
        let worker = Worker::builder(admin_queue.clone())
            .register_job(wait_without_listener)
            .timers(crate::test_timers())
            .build()
            .unwrap();
        let run = tokio::spawn(worker.run_until(shutdown.clone()));

        // Restore the client, and the wait it never gave up on resolves.
        sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
            r#"GRANT CONNECT ON DATABASE "{}" TO "{role}""#,
            url.rsplit_once('/').unwrap().1
        )))
        .execute(admin_queue.pool())
        .await
        .unwrap();

        let value = tokio::time::timeout(Duration::from_secs(60), waiting)
            .await
            .expect("the wait outlived its own deadline")
            .unwrap()
            .expect("the wait must resolve once the database is reachable again");
        assert_eq!(value, 42);

        shutdown.cancel();
        run.await.unwrap().unwrap();
    }
}

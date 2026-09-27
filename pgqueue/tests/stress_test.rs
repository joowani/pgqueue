//! Sustained-load tests against the Docker Compose PostgreSQL.
//!
//! These are `#[ignore]`d: they run for seconds to tens of seconds each, need a
//! multi-threaded runtime to produce real contention, and each one creates a
//! database of its own. Run them with `CI=1 shell/scripts/stress`.
//!
//! Every test here asserts an *invariant* under load rather than a throughput
//! number, so a pass means the same thing on a busy laptop as on idle CI. The
//! invariants are the library's own promises: at-least-once delivery, one
//! winner per dedupe key, no job left unterminated, and no state transition that
//! the schema forbids.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pgqueue::{Dashboard, JobFilter, JobState, JobStatus, Queue, QueueBuilder, Worker, WorkerTimers};
use sqlx::Connection;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{EnqueueResultTestExt, QueueProtocolTestExt, fresh_database, wait_until};

/// Records every completion so a test can tell "ran at least once" from "ran
/// exactly once" — at-least-once delivery permits the latter to be false, and a
/// test that asserted a bare count would be asserting the wrong promise.
#[derive(Default)]
struct Ledger {
    runs: Mutex<HashMap<u64, u32>>,
}

impl Ledger {
    fn record(&self, id: u64) {
        *self.runs.lock().expect("ledger poisoned").entry(id).or_insert(0) += 1;
    }

    fn distinct(&self) -> usize {
        self.runs.lock().expect("ledger poisoned").len()
    }

    fn total(&self) -> u32 {
        self.runs.lock().expect("ledger poisoned").values().sum()
    }

    fn ids(&self) -> HashSet<u64> {
        self.runs.lock().expect("ledger poisoned").keys().copied().collect()
    }
}

/// A queue on a database of its own. `tag` names the database, so it must be
/// unique per test; `fresh_database` drops any previous one, so repeated runs
/// reuse it rather than accumulating databases.
///
/// The pool ceiling is a budget, not a throughput need: a worker peaks at
/// `max + 2` server connections (pool plus the dedicated listener and sweep
/// leadership connections), these tests run at cargo's full parallelism, and
/// Docker Compose configures `max_connections = 300`. At 12 per pool, every
/// test running at once on a 16-17-core host stays comfortably under that;
/// nothing here holds a connection across a handler, so a smaller pool only
/// queues acquires rather than deadlocking.
async fn stress_queue(tag: &str, customize: impl FnOnce(QueueBuilder) -> QueueBuilder) -> (Queue, String) {
    crate::init_tracing();
    let url = fresh_database(tag).await;
    let queue = customize(Queue::builder(&url).connections(2, 12)).connect().await.expect("stress queue connect");
    (queue, url)
}

/// The worker timers the load tests run on: fast enough that a test does not
/// spend its runtime waiting on a poll interval, slow enough that the polling
/// itself is not the load under test.
fn stress_timers() -> WorkerTimers {
    WorkerTimers {
        abort: Duration::from_millis(50),
        schedule: Duration::from_millis(100),
        sweep: Duration::from_millis(250),
        worker_info: Duration::from_millis(100),
    }
}

fn stress_worker(queue: Queue) -> pgqueue::WorkerBuilder {
    Worker::builder(queue)
        .timers(stress_timers())
        .poll_interval(Duration::from_millis(25))
        .abort_grace(Duration::from_millis(100))
        .shutdown_grace(Duration::from_secs(10))
}

/// `QueueCounts` deliberately has no `complete` gauge — completed rows are the
/// bulk of retained history and counting them is not on the dashboard's hot
/// path — so a load test that wants "how many finished" asks the table.
async fn complete_count(queue: &Queue) -> i64 {
    sqlx::query_scalar::<_, i64>("SELECT count(*) FROM pgqueue.jobs WHERE queue = $1 AND status = 'complete'")
        .bind(queue.name())
        .fetch_one(queue.pool())
        .await
        .expect("count complete rows")
}

/// Asserts the queue reached a state where nothing is outstanding: every row is
/// terminal, and the gauges agree with the rows.
async fn assert_fully_drained(queue: &Queue, expected_complete: i64, what: &str) {
    let counts = queue.counts().await.expect("counts");
    let complete = complete_count(queue).await;
    assert_eq!(counts.queued, 0, "{what}: {} still queued, counts={counts:?}", counts.queued);
    assert_eq!(counts.running, 0, "{what}: {} still running, counts={counts:?}", counts.running);
    assert_eq!(counts.scheduled, 0, "{what}: {} still scheduled, counts={counts:?}", counts.scheduled);
    assert_eq!(complete, expected_complete, "{what}: complete rows disagree, counts={counts:?}");
}

// ---------------------------------------------------------------------------
// Batch size
// ---------------------------------------------------------------------------

#[pgqueue::job(name = "stress_mask", max_attempts = 1)]
async fn stress_mask(mask: Vec<u8>) -> anyhow::Result<usize> {
    Ok(mask.len())
}

/// A batch inside `MAX_ENQUEUE_BATCH_BYTES` has to land. Its payloads were bound as one `jsonb[]` parameter — one array
/// datum, which PostgreSQL caps at 1 GiB — and `jsonb` stores an array of small numbers in six times its JSON text, so
/// 200 masks of a mebibyte each, well inside the bound, failed with `54000` after the server had parsed every one of
/// them into memory at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "stress test"]
async fn test_stress_a_batch_inside_the_byte_bound_lands_however_jsonb_expands_it() {
    const JOBS: usize = 200;
    let (queue, _url) = stress_queue("stress_batch_bytes", |b| b).await;
    // Serialized as `[0,0,...]`: two bytes of JSON per element, twelve of `jsonb`, and just under the 1 MiB a single
    // payload may take.
    let mask = vec![0u8; 524_287];
    let results = queue
        .enqueue_batch((0..JOBS).map(|_| stress_mask::job(mask.clone())))
        .await
        .expect("a batch inside the byte bound must land");
    assert_eq!(results.len(), JOBS);
    assert!(results.iter().all(|result| result.is_enqueued()));
    let stored = sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM pgqueue.jobs WHERE queue = $1 AND jsonb_array_length(payload) = $2",
    )
    .bind(queue.name())
    .bind(i32::try_from(mask.len()).unwrap())
    .fetch_one(queue.pool())
    .await
    .unwrap();
    assert_eq!(stored, i64::try_from(JOBS).unwrap(), "every payload must be stored whole");
}

// ---------------------------------------------------------------------------
// Throughput
// ---------------------------------------------------------------------------

#[pgqueue::job(name = "stress_unit", max_attempts = 1, timeout_ms = 30_000)]
async fn stress_unit(args: u64, ledger: JobState<Arc<Ledger>>) -> anyhow::Result<u64> {
    ledger.0.record(args);
    Ok(args)
}

/// Three workers, twenty-four processors between them, against a queue filled
/// by eight concurrent producers. The promise under test is at-least-once: every
/// job runs, none is skipped, and every row ends `complete` exactly once however
/// many times its handler ran.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "stress test"]
async fn test_stress_multi_worker_throughput_loses_no_jobs() {
    const JOBS: u64 = 900;
    let (queue, _url) = stress_queue("stress_throughput", |b| b).await;

    let mut producers = Vec::new();
    for shard in 0..8u64 {
        let queue = queue.clone();
        producers.push(tokio::spawn(async move {
            let mut ids = Vec::new();
            let mut n = shard;
            while n < JOBS {
                ids.push(queue.enqueue(stress_unit::job(n)).await.expect("enqueue").unwrap().id());
                n += 8;
            }
            ids
        }));
    }
    let mut enqueued = Vec::new();
    for producer in producers {
        enqueued.extend(producer.await.expect("producer"));
    }
    assert_eq!(enqueued.len() as u64, JOBS);

    let ledger = Arc::new(Ledger::default());
    let shutdown = CancellationToken::new();
    let mut workers = Vec::new();
    for _ in 0..3 {
        let worker = stress_worker(queue.clone())
            .register_job(stress_unit)
            .state(ledger.clone())
            .concurrency(8)
            .build()
            .expect("build worker");
        workers.push(tokio::spawn(worker.run_until(shutdown.clone())));
    }

    wait_until(
        Duration::from_secs(120),
        Duration::from_millis(100),
        "jobs did not all complete under multi-worker load",
        || async { complete_count(&queue).await == JOBS as i64 },
    )
    .await;
    shutdown.cancel();
    for worker in workers {
        worker.await.expect("worker join").expect("worker run");
    }

    // At-least-once: the handler may have run more than once for a job whose
    // attempt was recovered, but every job must have run and every row must be
    // terminal exactly once.
    assert_eq!(ledger.distinct() as u64, JOBS, "some job never ran");
    assert!(ledger.total() as u64 >= JOBS, "ledger total below job count: {}", ledger.total());
    assert_fully_drained(&queue, JOBS as i64, "throughput").await;
    for id in enqueued {
        let job = queue.fetch_job(id).await.expect("fetch").expect("row");
        assert_eq!(job.status, JobStatus::Complete, "job {id} not complete");
    }
}

// ---------------------------------------------------------------------------
// Retries
// ---------------------------------------------------------------------------

/// Fails until its attempt count reaches the threshold carried in the payload,
/// so a single definition covers "retries then succeeds" and "exhausts and
/// fails".
#[pgqueue::job(name = "stress_flaky", max_attempts = 4, timeout_ms = 30_000)]
async fn stress_flaky(succeed_on: u32, ctx: pgqueue::JobContext, ledger: JobState<Arc<Ledger>>) -> anyhow::Result<u32> {
    let attempt = ctx.attempt();
    ledger.0.record(u64::from(attempt));
    if attempt >= succeed_on { Ok(attempt) } else { anyhow::bail!("stress failure on attempt {attempt}") }
}

/// Retry accounting has to hold when many jobs are retrying at once against the
/// same rows the workers are claiming from: a job that should succeed on its
/// third attempt must end `complete` with exactly three attempts, and one that
/// can never succeed must end `failed` having spent all four.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "stress test"]
async fn test_stress_retries_account_exactly_under_load() {
    let (queue, _url) = stress_queue("stress_retries", |b| b).await;

    let mut succeeds = Vec::new();
    let mut exhausts = Vec::new();
    for _ in 0..60 {
        succeeds.push(
            queue
                .enqueue(stress_flaky::job(3).retry_delay(Duration::from_millis(10)))
                .await
                .expect("enqueue")
                .unwrap()
                .id(),
        );
        // Unreachable within `max_attempts = 4`, so this one exhausts.
        exhausts.push(
            queue
                .enqueue(stress_flaky::job(99).retry_delay(Duration::from_millis(10)))
                .await
                .expect("enqueue")
                .unwrap()
                .id(),
        );
    }

    let ledger = Arc::new(Ledger::default());
    let shutdown = CancellationToken::new();
    let mut workers = Vec::new();
    for _ in 0..2 {
        let worker = stress_worker(queue.clone())
            .register_job(stress_flaky)
            .state(ledger.clone())
            .concurrency(6)
            .build()
            .expect("build worker");
        workers.push(tokio::spawn(worker.run_until(shutdown.clone())));
    }

    wait_until(Duration::from_secs(120), Duration::from_millis(100), "retrying jobs did not settle", || async {
        let counts = queue.counts().await.expect("counts");
        complete_count(&queue).await == 60 && counts.failed == 60
    })
    .await;
    shutdown.cancel();
    for worker in workers {
        worker.await.expect("worker join").expect("worker run");
    }

    for id in succeeds {
        let job = queue.fetch_job(id).await.expect("fetch").expect("row");
        assert_eq!(job.status, JobStatus::Complete, "job {id}");
        assert_eq!(job.attempts, 3, "job {id} attempts");
    }
    for id in exhausts {
        let job = queue.fetch_job(id).await.expect("fetch").expect("row");
        assert_eq!(job.status, JobStatus::Failed, "job {id}");
        assert_eq!(job.attempts, 4, "job {id} attempts");
        assert!(job.error.is_some(), "job {id} kept no error");
    }
}

// ---------------------------------------------------------------------------
// Deduplication
// ---------------------------------------------------------------------------

/// Races 24 producers onto one key and returns how many were admitted.
async fn race_one_key(queue: &Queue, key: &str, payload: u64) -> usize {
    let mut racers = Vec::new();
    for _ in 0..24 {
        let queue = queue.clone();
        let key = key.to_string();
        racers.push(tokio::spawn(async move {
            queue.enqueue(stress_unit::job(payload).dedupe_key(key)).await.expect("enqueue").is_some()
        }));
    }
    let mut winners = 0;
    for racer in racers {
        if racer.await.expect("racer") {
            winners += 1;
        }
    }
    winners
}

/// `jobs_dedupe_key_idx` is partial over `queued`/`running`/`aborting`, so the
/// promise is one *live* job per key — not one row ever. Both halves of that
/// need holding under contention: concurrent producers must be admitted one at
/// a time while a job is live, and the key must become available again once it
/// is not.
///
/// The races run with no worker attached, so "one winner" is deterministic
/// rather than a race against the completion that releases the key; the worker
/// then drains, and the same keys are raced again to prove the release.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "stress test"]
async fn test_stress_dedupe_admits_one_live_job_per_key_then_releases_it() {
    const KEYS: u64 = 40;
    let (queue, _url) = stress_queue("stress_dedupe", |b| b).await;

    for round in 0..KEYS {
        let winners = race_one_key(&queue, &format!("stress-key-{round}"), round).await;
        assert_eq!(winners, 1, "round {round} admitted {winners} live jobs");
    }

    let ledger = Arc::new(Ledger::default());
    let shutdown = CancellationToken::new();
    let worker = stress_worker(queue.clone())
        .register_job(stress_unit)
        .state(ledger.clone())
        .concurrency(4)
        .build()
        .expect("build worker");
    let running = tokio::spawn(worker.run_until(shutdown.clone()));
    wait_until(Duration::from_secs(60), Duration::from_millis(100), "deduplicated jobs did not drain", || async {
        complete_count(&queue).await == KEYS as i64
    })
    .await;
    shutdown.cancel();
    running.await.expect("worker join").expect("worker run");

    // Completion took every key out of the partial index, so the same keys must
    // be admissible again — once each.
    for round in 0..KEYS {
        let winners = race_one_key(&queue, &format!("stress-key-{round}"), round).await;
        assert_eq!(winners, 1, "round {round} readmitted {winners} live jobs after release");
    }
    let rows =
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM pgqueue.jobs WHERE queue = $1 AND dedupe_key IS NOT NULL")
            .bind(queue.name())
            .fetch_one(queue.pool())
            .await
            .expect("count deduped rows");
    assert_eq!(rows, (KEYS * 2) as i64, "one row per key per live window");
}

// ---------------------------------------------------------------------------
// enqueue_and_wait
// ---------------------------------------------------------------------------

/// Every waiter must be woken with its own result: under two hundred concurrent waits, a completion routed to the wrong
/// waiter shows up here as a wrong answer. A *lost* notification does not — the polling fallback resolves the wait
/// regardless, only later — so delivery itself is pinned by the tests that only a live listener can pass, such as
/// `test_malformed_done_notifications_are_tolerated`.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "stress test"]
async fn test_stress_enqueue_and_wait_wakes_every_waiter() {
    const WAITERS: u64 = 200;
    let (queue, _url) = stress_queue("stress_wait", |b| b).await;
    let ledger = Arc::new(Ledger::default());
    let shutdown = CancellationToken::new();
    let mut workers = Vec::new();
    for _ in 0..2 {
        let worker = stress_worker(queue.clone())
            .register_job(stress_unit)
            .state(ledger.clone())
            .concurrency(8)
            .build()
            .expect("build worker");
        workers.push(tokio::spawn(worker.run_until(shutdown.clone())));
    }

    let mut waiters = Vec::new();
    for n in 0..WAITERS {
        let queue = queue.clone();
        waiters.push(tokio::spawn(async move {
            let value: u64 = queue
                .enqueue_and_wait(stress_unit::job(n), Some(Duration::from_secs(90)))
                .await
                .expect("enqueue_and_wait");
            (n, value)
        }));
    }
    for waiter in waiters {
        let (asked, got) = waiter.await.expect("waiter");
        assert_eq!(asked, got, "waiter {asked} received another job's result");
    }

    shutdown.cancel();
    for worker in workers {
        worker.await.expect("worker join").expect("worker run");
    }
    assert_eq!(ledger.distinct() as u64, WAITERS);
}

// ---------------------------------------------------------------------------
// Priority
// ---------------------------------------------------------------------------

#[pgqueue::job(name = "stress_ordered", max_attempts = 1, timeout_ms = 30_000)]
async fn stress_ordered(args: u64, order: JobState<Arc<Mutex<Vec<u64>>>>) -> anyhow::Result<()> {
    order.0.lock().expect("order poisoned").push(args);
    Ok(())
}

/// With one processor, the dequeue order is observable. Low-priority work is enqueued first and in bulk, so a queue
/// that ignored priority would run it first; the high-priority tail must overtake all of it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "stress test"]
async fn test_stress_priority_overtakes_a_backlog() {
    let (queue, _url) = stress_queue("stress_priority", |b| b).await;
    for n in 0..200u64 {
        queue.enqueue(stress_ordered::job(n).priority(10)).await.expect("enqueue low");
    }
    for n in 1000..1030u64 {
        queue.enqueue(stress_ordered::job(n).priority(-10)).await.expect("enqueue high");
    }

    let order: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    let shutdown = CancellationToken::new();
    let worker = stress_worker(queue.clone())
        .register_job(stress_ordered)
        .state(order.clone())
        .concurrency(1)
        .build()
        .expect("build worker");
    let running = tokio::spawn(worker.run_until(shutdown.clone()));

    wait_until(Duration::from_secs(120), Duration::from_millis(100), "priority backlog did not drain", || async {
        complete_count(&queue).await == 230
    })
    .await;
    shutdown.cancel();
    running.await.expect("worker join").expect("worker run");

    let order = order.lock().expect("order poisoned").clone();
    assert_eq!(order.len(), 230);
    let last_high = order.iter().rposition(|n| *n >= 1000).expect("no high-priority job ran");
    let low_before_last_high = order[..last_high].iter().filter(|n| **n < 1000).count();
    // Every row is enqueued before the worker starts, and its one processor claims one row at a time in
    // `(priority, scheduled_at, id)` order, so no low-priority job can run ahead of any high-priority one. A tolerance
    // here let a partial loss of priority ordering pass.
    assert_eq!(
        low_before_last_high, 0,
        "{low_before_last_high} low-priority jobs ran before the last high-priority one: {order:?}"
    );
}

// ---------------------------------------------------------------------------
// Scheduling
// ---------------------------------------------------------------------------

/// A delayed job must not be claimable before its time, under load or otherwise.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "stress test"]
async fn test_stress_delayed_jobs_do_not_run_early() {
    let (queue, _url) = stress_queue("stress_delay", |b| b).await;
    let ledger = Arc::new(Ledger::default());

    let mut delayed = Vec::new();
    for n in 0..100u64 {
        delayed.push(
            queue.enqueue(stress_unit::job(n).delay(Duration::from_secs(3))).await.expect("enqueue").unwrap().id(),
        );
    }
    // Immediate work alongside it, so the workers are busy rather than idle
    // while the delay elapses.
    for n in 1000..1100u64 {
        queue.enqueue(stress_unit::job(n)).await.expect("enqueue");
    }

    let shutdown = CancellationToken::new();
    let worker = stress_worker(queue.clone())
        .register_job(stress_unit)
        .state(ledger.clone())
        .concurrency(8)
        .build()
        .expect("build worker");
    let running = tokio::spawn(worker.run_until(shutdown.clone()));

    wait_until(Duration::from_secs(60), Duration::from_millis(100), "delayed jobs never became due", || async {
        complete_count(&queue).await == 200
    })
    .await;
    shutdown.cancel();
    running.await.expect("worker join").expect("worker run");
    assert_fully_drained(&queue, 200, "delay").await;

    // "Did not run early" is asserted from row timestamps rather than by
    // probing the queue while the delay elapses: a probe holds only if the
    // immediate batch drains and is observed within the delay window, which a
    // loaded host cannot promise. The dequeue stamps `started_at` with the
    // same database `now()` it compared `scheduled_at` against, so a claim
    // before the delay elapsed shows up as `started_at < scheduled_at` no
    // matter how slowly the test itself ran.
    for id in &delayed {
        let job = queue.fetch_job(*id).await.expect("fetch").expect("row");
        assert_eq!(job.status, JobStatus::Complete, "delayed job {id}");
        let started = job.started_at.unwrap_or_else(|| panic!("delayed job {id} completed without started_at"));
        assert!(
            started >= job.scheduled_at,
            "delayed job {id} was claimed at {started}, before its scheduled {}",
            job.scheduled_at
        );
        // The delay must actually have pushed `scheduled_at` out; otherwise the
        // check above would pass trivially for a delay the enqueue dropped.
        // Both timestamps come from the same `statement_timestamp()` in the
        // enqueue's INSERT, so the delay separates them exactly; asserting a
        // second short of it just keeps this from depending on that detail.
        assert!(
            job.scheduled_at >= job.enqueued_at + jiff::SignedDuration::from_secs(2),
            "delayed job {id} was scheduled at {}, not ~3s after its enqueue at {}",
            job.scheduled_at,
            job.enqueued_at
        );
    }
}

// ---------------------------------------------------------------------------
// Shutdown and recovery
// ---------------------------------------------------------------------------

/// Sleeps far longer than the 50ms shutdown grace the restart test gives its
/// generations, so a generation cancelled with attempts in hand can never
/// finish them gracefully — the cancel must interrupt them.
#[pgqueue::job(name = "stress_slow", max_attempts = 5, timeout_ms = 30_000)]
async fn stress_slow(args: u64, ledger: JobState<Arc<Ledger>>) -> anyhow::Result<u64> {
    tokio::time::sleep(Duration::from_millis(400)).await;
    ledger.0.record(args);
    Ok(args)
}

/// Workers are cancelled mid-flight, repeatedly, while the queue is full. Every
/// job must survive: an attempt interrupted by shutdown is requeued rather than
/// lost, so the run eventually drains with every job completed.
///
/// The interruption is forced, not left to timing: each generation is cancelled
/// only once it verifiably holds running attempts, and its 50ms shutdown grace
/// cannot wait out the job's 400ms sleep, so force-stop — and with it the
/// shutdown requeue — fires every time. The `max_attempts` assertion after the
/// generations proves it did.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "stress test"]
async fn test_stress_rolling_restarts_lose_no_jobs() {
    // 96 rather than a few hundred: the job sleeps 400ms so restarts reliably
    // catch attempts mid-flight, and the final drain pays that cost per job.
    const JOBS: u64 = 96;
    let (queue, _url) = stress_queue("stress_restart", |b| b).await;
    let mut ids = Vec::new();
    for n in 0..JOBS {
        ids.push(queue.enqueue(stress_slow::job(n)).await.expect("enqueue").unwrap().id());
    }

    let ledger = Arc::new(Ledger::default());
    // Six generations of workers, each killed while it still has work in hand:
    // the cancel waits for every processor to hold a running attempt rather
    // than for a wall-clock instant, so the state is reached on any machine
    // speed. The youngest such attempt is at most a poll interval or two into
    // its 400ms sleep when the 50ms grace expires, leaving ~300ms of margin
    // between "force-stop fires" and "the handler could have finished".
    for generation in 0..6 {
        let shutdown = CancellationToken::new();
        let worker = stress_worker(queue.clone())
            .register_job(stress_slow)
            .state(ledger.clone())
            .concurrency(6)
            // Far below the job's 400ms sleep, overriding `stress_worker`'s
            // 10s: graceful shutdown must not be able to finish the in-flight
            // attempts, or the test degenerates to start/stop/drain.
            .shutdown_grace(Duration::from_millis(50))
            .build()
            .expect("build worker");
        let running = tokio::spawn(worker.run_until(shutdown.clone()));
        wait_until(
            Duration::from_secs(30),
            Duration::from_millis(25),
            "generation never claimed a full complement of work",
            || async { queue.counts().await.expect("counts").running >= 6 },
        )
        .await;
        shutdown.cancel();
        running.await.expect("worker join").unwrap_or_else(|error| panic!("generation {generation} failed: {error}"));
    }

    // The restarts must actually have interrupted attempts, or the drain below
    // proves nothing about recovery. A shutdown requeue refunds the attempt by
    // raising `max_attempts` above the declared 5 — never by lowering
    // `attempts` — and the refund is permanent, so the interruptions stay
    // visible on the rows from here to the end of the run.
    let refunded =
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM pgqueue.jobs WHERE queue = $1 AND max_attempts > 5")
            .bind(queue.name())
            .fetch_one(queue.pool())
            .await
            .expect("count refunded rows");
    assert!(refunded > 0, "no attempt was interrupted by the rolling restarts");

    // A final generation with no interruption, to drain whatever is left.
    let shutdown = CancellationToken::new();
    let worker = stress_worker(queue.clone())
        .register_job(stress_slow)
        .state(ledger.clone())
        .concurrency(8)
        .build()
        .expect("build worker");
    let running = tokio::spawn(worker.run_until(shutdown.clone()));
    wait_until(
        Duration::from_secs(120),
        Duration::from_millis(100),
        "rolling restarts left jobs undrained",
        || async { complete_count(&queue).await == JOBS as i64 },
    )
    .await;
    shutdown.cancel();
    running.await.expect("worker join").expect("worker run");

    assert_eq!(ledger.distinct() as u64, JOBS, "a job was lost across restarts");
    assert_fully_drained(&queue, JOBS as i64, "rolling restart").await;
    for id in ids {
        let job = queue.fetch_job(id).await.expect("fetch").expect("row");
        assert_eq!(job.status, JobStatus::Complete, "job {id}");
    }
}

/// Never actually runs here: the point is the row it leaves behind when a
/// phantom worker claims it and dies. `max_attempts` exceeds the number of
/// times it is stranded, so recovery requeues rather than exhausts.
#[pgqueue::job(name = "stress_abandoned", max_attempts = 8, timeout_ms = 60_000)]
async fn stress_abandoned(args: u64, ledger: JobState<Arc<Ledger>>) -> anyhow::Result<u64> {
    ledger.0.record(args);
    Ok(args)
}

/// A worker that died before it could finish — or even heartbeat — leaves its
/// attempt `running` under a lease that nothing will renew. Only the sweeper
/// can return those rows, so this strands a large batch of them and asserts
/// every one comes back and completes while a live worker drains beside it.
///
/// The claims go through the raw unleased dequeue rather than by killing a
/// worker task: [`Worker::run_until`] documents that *dropping* it starts a
/// graceful shutdown in the background precisely so in-flight jobs are not
/// abandoned, so a cancelled task cannot produce this state. A claim with a
/// worker id that never writes a `pgqueue.workers` row is exactly a process that
/// died between claiming and its first heartbeat.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "stress test"]
async fn test_stress_sweeper_recovers_abandoned_attempts() {
    const JOBS: i64 = 120;
    const PHANTOMS: i64 = 6;
    const PER_PHANTOM: i64 = 15;
    let (queue, _url) = stress_queue("stress_sweep", |b| b.sweep_grace(Duration::from_millis(200))).await;
    let ledger = Arc::new(Ledger::default());

    for n in 0..JOBS {
        queue.enqueue(stress_abandoned::job(n as u64)).await.expect("enqueue");
    }

    // Strand attempts across several dead owners at once, so recovery has to
    // batch rather than handle one crashed worker at a time.
    let mut stranded = Vec::new();
    for _ in 0..PHANTOMS {
        let claimed = queue.dequeue(PER_PHANTOM, Uuid::now_v7()).await.expect("phantom claim");
        assert_eq!(claimed.len() as i64, PER_PHANTOM, "phantom worker could not claim a full batch");
        stranded.extend(claimed.into_iter().map(|job| job.id));
    }
    assert_eq!(stranded.len() as i64, PHANTOMS * PER_PHANTOM);
    assert_eq!(
        queue.counts().await.expect("counts").running,
        PHANTOMS * PER_PHANTOM,
        "stranded attempts are not sitting running"
    );

    // A live worker plus its sweeper must recover every one of them.
    let shutdown = CancellationToken::new();
    let worker = stress_worker(queue.clone())
        .register_job(stress_abandoned)
        .state(ledger.clone())
        .concurrency(8)
        .build()
        .expect("build worker");
    let running = tokio::spawn(worker.run_until(shutdown.clone()));

    wait_until(
        Duration::from_secs(180),
        Duration::from_millis(200),
        "sweeper did not recover every abandoned attempt",
        || async { complete_count(&queue).await == JOBS },
    )
    .await;
    shutdown.cancel();
    running.await.expect("worker join").expect("worker run");

    assert_fully_drained(&queue, JOBS, "sweeper recovery").await;
    assert_eq!(ledger.distinct() as i64, JOBS, "a recovered job never ran to completion");
    // Every stranded row had already spent an attempt on its dead owner, so a
    // recovered-and-rerun row is one with more than one attempt against it.
    let recovered = sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM pgqueue.jobs WHERE queue = $1 AND id = ANY($2) AND attempts > 1",
    )
    .bind(queue.name())
    .bind(&stranded)
    .fetch_one(queue.pool())
    .await
    .expect("count recovered rows");
    assert_eq!(
        recovered,
        stranded.len() as i64,
        "only {recovered} of {} stranded rows show a second attempt",
        stranded.len()
    );
}

// ---------------------------------------------------------------------------
// Cron
// ---------------------------------------------------------------------------

#[pgqueue::cron("0 * * * *", name = "stress_tick", result_ttl_ms = 3_600_000)]
async fn stress_tick(ticks: JobState<Arc<AtomicU64>>) -> anyhow::Result<u64> {
    Ok(ticks.0.fetch_add(1, Ordering::SeqCst) + 1)
}

/// Four workers all scheduling the same minutely cron: no occurrence may be
/// enqueued twice however many workers race to schedule it.
///
/// What racing workers exercise here is the shared durable cursor —
/// `cron_schedules.next_run_at`, advanced under `FOR UPDATE SKIP LOCKED` in
/// the same transaction that publishes — plus the live dedupe-key holder
/// check. The `pgqueue.cron_occurrences` claim table only decides when that
/// cursor is *rewound* past an already-published occurrence, which live
/// workers never do, so this test asserts nothing about it; that path is
/// covered deterministically in `cron_test`
/// (`test_cron_publishes_an_occurrence_at_most_once_when_the_cursor_is_rewound`).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "stress test"]
async fn test_stress_cron_enqueues_each_occurrence_once_across_workers() {
    let (queue, _url) = stress_queue("stress_cron", |b| b).await;
    let ticks = Arc::new(AtomicU64::new(0));
    let shutdown = CancellationToken::new();
    let mut workers = Vec::new();
    for _ in 0..4 {
        let worker = stress_worker(queue.clone())
            .register_cron(stress_tick)
            .state(ticks.clone())
            .concurrency(4)
            .build()
            .expect("build worker");
        workers.push(tokio::spawn(worker.run_until(shutdown.clone())));
    }

    wait_until(Duration::from_secs(15), Duration::from_millis(20), "cron schedule was not reconciled", || async {
        sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (
                    SELECT 1 FROM pgqueue.cron_schedules
                    WHERE queue = $1 AND dedupe_key = 'cron:stress_tick'
                )",
        )
        .bind(queue.name())
        .fetch_one(queue.pool())
        .await
        .expect("inspect cron schedule")
    })
    .await;
    sqlx::query(
        "UPDATE pgqueue.cron_schedules
         SET next_run_at = clock_timestamp()
         WHERE queue = $1 AND dedupe_key = 'cron:stress_tick'",
    )
    .bind(queue.name())
    .execute(queue.pool())
    .await
    .expect("make cron due");
    wait_until(Duration::from_secs(15), Duration::from_millis(20), "cron did not fire", || async {
        ticks.load(Ordering::SeqCst) >= 1
    })
    .await;
    shutdown.cancel();
    for worker in workers {
        worker.await.expect("worker join").expect("worker run");
    }

    // A cron job's `dedupe_key` is the *schedule* identity (`cron:{name}`),
    // constant across occurrences — an occurrence is identified by its
    // `scheduled_at`. So the cluster-safety promise is one row per instant
    // however many workers raced to schedule it.
    let rows = queue
        .jobs_page(JobFilter { name: Some("stress_tick".to_string()), limit: Some(1000), ..Default::default() })
        .await
        .expect("jobs page");
    assert!(!rows.is_empty(), "cron never fired");
    let mut per_occurrence: HashMap<jiff::Timestamp, u32> = HashMap::new();
    for row in &rows {
        assert_eq!(
            row.dedupe_key.as_deref(),
            Some("cron:stress_tick"),
            "cron row {} carries an unexpected schedule key",
            row.id
        );
        *per_occurrence.entry(row.scheduled_at).or_insert(0) += 1;
    }
    for (occurrence, count) in &per_occurrence {
        assert_eq!(*count, 1, "occurrence {occurrence} was enqueued {count} times across workers");
    }
    assert_eq!(per_occurrence.len(), rows.len(), "duplicate cron occurrences");
    // The claim table records every published occurrence, but claims are
    // deliberately transient — they only outlive the scheduler's backfill
    // grace, and this worker sweeps every 250ms — so the surviving ones are a
    // subset of the occurrences, never more of them.
    let claims = sqlx::query_scalar::<_, i64>(
        "SELECT count(DISTINCT scheduled_at) FROM pgqueue.cron_occurrences WHERE queue = $1",
    )
    .bind(queue.name())
    .fetch_one(queue.pool())
    .await
    .expect("count occurrence claims");
    assert!(
        claims <= per_occurrence.len() as i64,
        "more occurrence claims ({claims}) than job rows ({})",
        per_occurrence.len()
    );
}

// ---------------------------------------------------------------------------
// Control plane
// ---------------------------------------------------------------------------

#[pgqueue::job(name = "stress_long", max_attempts = 2, timeout_ms = 30_000)]
async fn stress_long(_: (), ctx: pgqueue::JobContext) -> anyhow::Result<()> {
    // Cooperative: ends promptly when the attempt is aborted, and otherwise
    // stays running long enough to be a target for abort and retry.
    let cancelled = ctx.cancellation();
    tokio::select! {
        _ = cancelled.cancelled() => Ok(()),
        _ = tokio::time::sleep(Duration::from_secs(30)) => Ok(()),
    }
}

/// Aborts every id at once, so the requests land against rows in every state
/// the worker is moving them through.
async fn abort_every(queue: &Queue, ids: &[Uuid]) {
    let mut aborts = Vec::new();
    for id in ids {
        let queue = queue.clone();
        let id = *id;
        aborts.push(tokio::spawn(async move { queue.abort_job(id, "stress abort").await }));
    }
    for abort in aborts {
        abort.await.expect("abort task").expect("abort");
    }
}

/// `abort_job` and `retry_job` are the operator's controls, and the dashboard
/// exposes them. Driving them concurrently against jobs that are actively being
/// claimed must never leave a row in a non-terminal state or panic a worker.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "stress test"]
async fn test_stress_abort_and_retry_settle_every_row() {
    const JOBS: usize = 80;
    let (queue, _url) = stress_queue("stress_control", |b| b).await;
    let mut ids = Vec::new();
    for _ in 0..JOBS {
        ids.push(queue.enqueue(stress_long::job(())).await.expect("enqueue").unwrap().id());
    }

    let shutdown = CancellationToken::new();
    let worker = stress_worker(queue.clone()).register_job(stress_long).concurrency(8).build().expect("build worker");
    let running = tokio::spawn(worker.run_until(shutdown.clone()));

    // Abort every job from many tasks at once, while the worker is claiming
    // them: some are queued, some running, some already finished. `stress_long`
    // only ends when its attempt is aborted, so nothing settles on its own and
    // every terminal row here is one the abort produced.
    let settled = async || {
        let counts = queue.counts().await.expect("counts");
        counts.running == 0 && counts.queued == 0 && counts.scheduled == 0
    };

    abort_every(&queue, &ids).await;
    wait_until(Duration::from_secs(90), Duration::from_millis(100), "aborted jobs did not settle", settled).await;

    // Retry every terminal row and abort the lot again: the operator loop is
    // abort, inspect, retry, and a row must survive being cycled through it
    // while workers are claiming from underneath.
    //
    // `retry_job` re-enqueues a terminal row as a *fresh occurrence* with an id
    // of its own, so the retried work is those new ids — aborting the originals
    // a second time would be a no-op against rows that are already terminal.
    let mut retried = Vec::new();
    for id in &ids {
        let occurrence = queue
            .retry_job_occurrence(*id, "stress retry")
            .await
            .expect("retry")
            .unwrap_or_else(|| panic!("terminal job {id} refused a retry"));
        retried.push(occurrence);
    }
    assert_eq!(retried.len(), JOBS);

    abort_every(&queue, &retried).await;
    wait_until(Duration::from_secs(90), Duration::from_millis(100), "retried jobs did not settle", settled).await;
    ids.extend(retried);
    shutdown.cancel();
    running.await.expect("worker join").expect("worker run");

    for id in &ids {
        let job = queue.fetch_job(*id).await.expect("fetch").expect("row");
        assert!(job.status.is_terminal(), "job {id} left in {:?}", job.status);
    }
}

// ---------------------------------------------------------------------------
// Dashboard
// ---------------------------------------------------------------------------

/// The dashboard reads the same pool the worker runs on, and its data routes
/// are polled continuously by every open tab. Under a request flood beside a
/// busy worker, no data route may fail and the worker must still drain the
/// queue. `/health` has a deliberate two-second availability bound, so probe it
/// outside the synthetic flood rather than treating its documented overload
/// response as an internal failure.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "stress test"]
async fn test_stress_dashboard_serves_under_load_beside_a_worker() {
    const JOBS: u64 = 400;
    let (queue, _url) = stress_queue("stress_dashboard", |b| b).await;
    for n in 0..JOBS {
        queue.enqueue(stress_unit::job(n)).await.expect("enqueue");
    }

    let router = Dashboard::new([queue.clone()]).allow_unauthenticated().router().expect("dashboard router");
    let ledger = Arc::new(Ledger::default());
    let shutdown = CancellationToken::new();
    let worker = stress_worker(queue.clone())
        .register_job(stress_unit)
        .state(ledger.clone())
        .concurrency(8)
        .build()
        .expect("build worker");
    let running = tokio::spawn(worker.run_until(shutdown.clone()));

    let (status, body) = crate::dashboard_test::request(&router, "GET", "/health", None).await;
    assert!(status.is_success(), "dashboard health before load answered {status}: {body:?}");

    let name = queue.name().to_string();
    let paths = [
        "/api/queues".to_string(),
        format!("/api/queues/{name}/jobs?limit=50"),
        format!("/api/queues/{name}/jobs?status=complete&limit=25"),
        format!("/api/queues/{name}/job-names?prefix=stress"),
        format!("/api/queues/{name}/workers?limit=25"),
    ];

    let failures = Arc::new(AtomicU32::new(0));
    let stop = CancellationToken::new();
    let mut hammers = Vec::new();
    for path in paths {
        for _ in 0..4 {
            let router = router.clone();
            let path = path.clone();
            let failures = failures.clone();
            let stop = stop.clone();
            hammers.push(tokio::spawn(async move {
                while !stop.is_cancelled() {
                    let (status, body) = crate::dashboard_test::request(&router, "GET", &path, None).await;
                    if status.is_server_error() {
                        failures.fetch_add(1, Ordering::SeqCst);
                        eprintln!("dashboard data route {path} answered {status}: {body:?}");
                    }
                    tokio::task::yield_now().await;
                }
            }));
        }
    }

    wait_until(
        Duration::from_secs(120),
        Duration::from_millis(100),
        "worker could not drain the queue under dashboard load",
        || async { complete_count(&queue).await == JOBS as i64 },
    )
    .await;
    stop.cancel();
    for hammer in hammers {
        hammer.await.expect("hammer");
    }

    let (status, body) = crate::dashboard_test::request(&router, "GET", "/health", None).await;
    assert!(status.is_success(), "dashboard health after load answered {status}: {body:?}");

    shutdown.cancel();
    running.await.expect("worker join").expect("worker run");

    assert_eq!(failures.load(Ordering::SeqCst), 0, "dashboard data route answered 5xx under load");
    assert_fully_drained(&queue, JOBS as i64, "dashboard").await;
}

// ---------------------------------------------------------------------------
// Multi-queue isolation
// ---------------------------------------------------------------------------

/// Two named queues in one database. Work enqueued on one must never be claimed
/// by a worker on the other, whatever the contention.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "stress test"]
async fn test_stress_queues_stay_isolated() {
    const PER_QUEUE: u64 = 200;
    let (alpha, url) = stress_queue("stress_isolation", |b| b.name("alpha")).await;
    let beta = Queue::builder(&url).name("beta").connections(2, 12).connect().await.expect("beta connect");

    for n in 0..PER_QUEUE {
        alpha.enqueue(stress_unit::job(n)).await.expect("alpha");
        beta.enqueue(stress_unit::job(n + 10_000)).await.expect("beta");
    }

    let alpha_ledger = Arc::new(Ledger::default());
    let beta_ledger = Arc::new(Ledger::default());
    let shutdown = CancellationToken::new();
    let mut workers = Vec::new();
    for (queue, ledger) in [
        (alpha.clone(), alpha_ledger.clone()),
        (beta.clone(), beta_ledger.clone()),
    ] {
        let worker =
            stress_worker(queue).register_job(stress_unit).state(ledger).concurrency(8).build().expect("build worker");
        workers.push(tokio::spawn(worker.run_until(shutdown.clone())));
    }

    wait_until(Duration::from_secs(120), Duration::from_millis(100), "isolated queues did not drain", || async {
        complete_count(&alpha).await == PER_QUEUE as i64 && complete_count(&beta).await == PER_QUEUE as i64
    })
    .await;
    shutdown.cancel();
    for worker in workers {
        worker.await.expect("worker join").expect("worker run");
    }

    let alpha_ids = alpha_ledger.ids();
    let beta_ids = beta_ledger.ids();
    assert!(alpha_ids.iter().all(|n| *n < 10_000), "alpha's worker ran beta's jobs");
    assert!(beta_ids.iter().all(|n| *n >= 10_000), "beta's worker ran alpha's jobs");
    assert_eq!(alpha_ids.len() as u64, PER_QUEUE);
    assert_eq!(beta_ids.len() as u64, PER_QUEUE);
}

// ---------------------------------------------------------------------------
// Burst mode
// ---------------------------------------------------------------------------

/// Burst workers drain what is due and return, and `max_burst_jobs` caps how
/// much any one of them takes. Several running at once against a backlog must
/// each return `Ok` and must not, between them, process more than their budgets
/// allow — the budget is what a cron or CI invocation sizes its run with.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "stress test"]
async fn test_stress_burst_workers_respect_their_budgets() {
    const JOBS: u64 = 400;
    const BURSTS: usize = 4;
    const BUDGET: usize = 25;
    let (queue, _url) = stress_queue("stress_burst", |b| b).await;
    for n in 0..JOBS {
        queue.enqueue(stress_unit::job(n)).await.expect("enqueue");
    }

    let ledger = Arc::new(Ledger::default());
    let mut bursts = Vec::new();
    for _ in 0..BURSTS {
        let worker = stress_worker(queue.clone())
            .register_job(stress_unit)
            .state(ledger.clone())
            .concurrency(4)
            .burst(true)
            .max_burst_jobs(BUDGET)
            .dequeue_timeout(Duration::from_millis(500))
            .build()
            .expect("build burst worker");
        bursts.push(tokio::spawn(worker.run_until(CancellationToken::new())));
    }
    for burst in bursts {
        burst.await.expect("burst join").expect("burst run");
    }

    let done = complete_count(&queue).await;
    assert!(done > 0, "burst workers processed nothing");
    assert!(
        done <= (BURSTS * BUDGET) as i64,
        "burst workers processed {done}, over the {} the budgets allow",
        BURSTS * BUDGET
    );
    // The rest is untouched and still due, not lost or left running.
    let counts = queue.counts().await.expect("counts");
    assert_eq!(counts.running, 0, "burst left an attempt running");
    assert_eq!(counts.queued, JOBS as i64 - done, "backlog does not account for what the bursts took: {counts:?}");

    // An unbudgeted burst drains the remainder and returns on its own.
    let worker = stress_worker(queue.clone())
        .register_job(stress_unit)
        .state(ledger.clone())
        .concurrency(8)
        .burst(true)
        .dequeue_timeout(Duration::from_millis(500))
        .build()
        .expect("build draining burst");
    worker.run_until(CancellationToken::new()).await.expect("draining burst");
    assert_fully_drained(&queue, JOBS as i64, "burst").await;
}

// ---------------------------------------------------------------------------
// Timeouts
// ---------------------------------------------------------------------------

/// Sleeps well past its own timeout, and ignores cancellation, so the only
/// thing that can end the attempt is the worker's timeout.
#[pgqueue::job(name = "stress_overrun", max_attempts = 2, timeout_ms = 300)]
async fn stress_overrun(_: (), ledger: JobState<Arc<Ledger>>) -> anyhow::Result<()> {
    ledger.0.record(0);
    tokio::time::sleep(Duration::from_secs(20)).await;
    Ok(())
}

/// Timed-out attempts must not wedge the processor that ran them: a queue of
/// jobs that all overrun has to end with every row `failed` on its attempts,
/// and ordinary work mixed in beside it has to finish regardless.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "stress test"]
async fn test_stress_timeouts_fail_attempts_without_wedging_the_worker() {
    const OVERRUNS: usize = 24;
    const NORMAL: u64 = 100;
    let (queue, _url) = stress_queue("stress_timeout", |b| b).await;
    let mut overrun_ids = Vec::new();
    for _ in 0..OVERRUNS {
        overrun_ids.push(
            queue
                .enqueue(stress_overrun::job(()).retry_delay(Duration::from_millis(10)))
                .await
                .expect("enqueue")
                .unwrap()
                .id(),
        );
    }
    for n in 0..NORMAL {
        queue.enqueue(stress_unit::job(n)).await.expect("enqueue");
    }

    let ledger = Arc::new(Ledger::default());
    let shutdown = CancellationToken::new();
    let mut workers = Vec::new();
    for _ in 0..2 {
        let worker = stress_worker(queue.clone())
            .register_job(stress_overrun)
            .register_job(stress_unit)
            .state(ledger.clone())
            .concurrency(6)
            .build()
            .expect("build worker");
        workers.push(tokio::spawn(worker.run_until(shutdown.clone())));
    }

    wait_until(
        Duration::from_secs(120),
        Duration::from_millis(100),
        "timed-out jobs did not exhaust their attempts",
        || async {
            let counts = queue.counts().await.expect("counts");
            counts.failed == OVERRUNS as i64 && complete_count(&queue).await == NORMAL as i64
        },
    )
    .await;
    shutdown.cancel();
    for worker in workers {
        worker.await.expect("worker join").expect("worker run");
    }

    for id in overrun_ids {
        let job = queue.fetch_job(id).await.expect("fetch").expect("row");
        assert_eq!(job.status, JobStatus::Failed, "job {id}");
        assert_eq!(job.attempts, 2, "job {id} did not spend both attempts");
    }
}

// ---------------------------------------------------------------------------
// Retention
// ---------------------------------------------------------------------------

/// `JobRetention::DeleteImmediately` removes the row as the worker finishes it,
/// and a finite retention leaves it for the sweeper. Under load both have to
/// hold at once without taking any *other* job's row with them.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "stress test"]
async fn test_stress_retention_purges_only_what_it_should() {
    const EACH: u64 = 150;
    let (queue, _url) = stress_queue("stress_retention", |b| b).await;
    let mut kept = Vec::new();
    for n in 0..EACH {
        queue
            .enqueue(stress_unit::job(n).retention(pgqueue::JobRetention::DeleteImmediately))
            .await
            .expect("enqueue ephemeral");
        kept.push(
            queue
                .enqueue(stress_unit::job(n + 10_000).retention(pgqueue::JobRetention::Forever))
                .await
                .expect("enqueue kept")
                .unwrap()
                .id(),
        );
    }

    let ledger = Arc::new(Ledger::default());
    let shutdown = CancellationToken::new();
    let worker = stress_worker(queue.clone())
        .register_job(stress_unit)
        .state(ledger.clone())
        .concurrency(8)
        .build()
        .expect("build worker");
    let running = tokio::spawn(worker.run_until(shutdown.clone()));

    wait_until(
        Duration::from_secs(120),
        Duration::from_millis(100),
        "retention run did not finish every job",
        || async { ledger.distinct() as u64 == EACH * 2 },
    )
    .await;
    shutdown.cancel();
    running.await.expect("worker join").expect("worker run");

    // Every job ran; only the retained half is still on disk.
    wait_until(Duration::from_secs(60), Duration::from_millis(100), "ephemeral rows were not removed", || async {
        complete_count(&queue).await == EACH as i64
    })
    .await;
    for id in &kept {
        assert!(queue.fetch_job(*id).await.expect("fetch").is_some(), "retained job {id} was purged");
    }
}

// ---------------------------------------------------------------------------
// Runtime cron
// ---------------------------------------------------------------------------

/// `schedule_cron` registers a schedule at runtime rather than at compile time.
/// It rides the same durable schedule rows — the shared cursor advanced under
/// the publishing transaction's row lock, as in the compile-time cron test
/// above — so the same cluster-safety promise has to hold across workers that
/// were each handed the schedule separately.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "stress test"]
async fn test_stress_runtime_cron_is_cluster_safe() {
    let (queue, _url) = stress_queue("stress_runtime_cron", |b| b).await;
    let ledger = Arc::new(Ledger::default());
    let shutdown = CancellationToken::new();
    let mut workers = Vec::new();
    for _ in 0..4 {
        let worker = stress_worker(queue.clone())
            .schedule_cron("0 * * * *", stress_unit::job(7))
            .state(ledger.clone())
            .concurrency(4)
            .build()
            .expect("build worker");
        workers.push(tokio::spawn(worker.run_until(shutdown.clone())));
    }
    wait_until(
        Duration::from_secs(15),
        Duration::from_millis(20),
        "runtime cron schedule was not reconciled",
        || async {
            sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS (
                    SELECT 1 FROM pgqueue.cron_schedules
                    WHERE queue = $1 AND dedupe_key = 'cron:stress_unit'
                )",
            )
            .bind(queue.name())
            .fetch_one(queue.pool())
            .await
            .expect("inspect runtime cron schedule")
        },
    )
    .await;
    sqlx::query(
        "UPDATE pgqueue.cron_schedules
         SET next_run_at = clock_timestamp()
         WHERE queue = $1 AND dedupe_key = 'cron:stress_unit'",
    )
    .bind(queue.name())
    .execute(queue.pool())
    .await
    .expect("make runtime cron due");
    wait_until(Duration::from_secs(15), Duration::from_millis(20), "runtime cron did not fire", || async {
        ledger.distinct() >= 1
    })
    .await;
    shutdown.cancel();
    for worker in workers {
        worker.await.expect("worker join").expect("worker run");
    }

    let rows = queue
        .jobs_page(JobFilter { name: Some("stress_unit".to_string()), limit: Some(1000), ..Default::default() })
        .await
        .expect("jobs page");
    assert!(!rows.is_empty(), "runtime cron never fired");
    let mut per_occurrence: HashMap<jiff::Timestamp, u32> = HashMap::new();
    for row in &rows {
        *per_occurrence.entry(row.scheduled_at).or_insert(0) += 1;
    }
    for (occurrence, count) in &per_occurrence {
        assert_eq!(*count, 1, "runtime occurrence {occurrence} was enqueued {count} times across workers");
    }
}

// ---------------------------------------------------------------------------
// Dashboard mutations
// ---------------------------------------------------------------------------

/// The dashboard's retry and abort buttons are `POST` routes onto the same
/// operator controls. Driven concurrently against a working queue they must
/// answer without a 5xx and leave every row terminal.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "stress test"]
async fn test_stress_dashboard_mutations_stay_consistent() {
    const JOBS: usize = 60;
    let (queue, _url) = stress_queue("stress_dash_mutate", |b| b).await;
    let mut ids = Vec::new();
    for _ in 0..JOBS {
        ids.push(queue.enqueue(stress_long::job(())).await.expect("enqueue").unwrap().id());
    }

    let router = Dashboard::new([queue.clone()]).allow_unauthenticated().router().expect("dashboard router");
    let shutdown = CancellationToken::new();
    let worker = stress_worker(queue.clone()).register_job(stress_long).concurrency(8).build().expect("build worker");
    let running = tokio::spawn(worker.run_until(shutdown.clone()));

    let name = queue.name().to_string();
    let mut posts = Vec::new();
    for id in &ids {
        let router = router.clone();
        let path = format!("/api/queues/{name}/jobs/{id}/abort");
        posts.push(tokio::spawn(async move { crate::dashboard_test::request(&router, "POST", &path, None).await }));
    }
    for post in posts {
        let (status, body) = post.await.expect("abort post");
        assert!(!status.is_server_error(), "dashboard abort answered {status}: {body:?}");
    }

    wait_until(
        Duration::from_secs(90),
        Duration::from_millis(100),
        "dashboard-aborted jobs did not settle",
        || async {
            let counts = queue.counts().await.expect("counts");
            counts.running == 0 && counts.queued == 0 && counts.scheduled == 0
        },
    )
    .await;

    // Retry through the dashboard, then abort the fresh occurrences the same
    // way, so the whole operator loop runs over HTTP.
    let mut retries = Vec::new();
    for id in &ids {
        let router = router.clone();
        let path = format!("/api/queues/{name}/jobs/{id}/retry");
        retries.push(tokio::spawn(async move { crate::dashboard_test::request(&router, "POST", &path, None).await }));
    }
    for retry in retries {
        let (status, body) = retry.await.expect("retry post");
        assert!(!status.is_server_error(), "dashboard retry answered {status}: {body:?}");
    }

    wait_until(
        Duration::from_secs(90),
        Duration::from_millis(100),
        "dashboard-retried jobs never became live",
        || async {
            let counts = queue.counts().await.expect("counts");
            counts.running > 0 || counts.queued > 0
        },
    )
    .await;

    let live = queue
        .jobs_page(JobFilter { limit: Some(1000), ..Default::default() })
        .await
        .expect("jobs page")
        .into_iter()
        .filter(|row| !row.status.is_terminal())
        .map(|row| row.id)
        .collect::<Vec<_>>();
    abort_every(&queue, &live).await;

    wait_until(
        Duration::from_secs(90),
        Duration::from_millis(100),
        "dashboard-retried jobs did not settle",
        || async {
            let counts = queue.counts().await.expect("counts");
            counts.running == 0 && counts.queued == 0 && counts.scheduled == 0
        },
    )
    .await;
    shutdown.cancel();
    running.await.expect("worker join").expect("worker run");

    for row in queue.jobs_page(JobFilter { limit: Some(1000), ..Default::default() }).await.expect("jobs page") {
        assert!(row.status.is_terminal(), "job {} left in {:?}", row.id, row.status);
    }
}

// ---------------------------------------------------------------------------
// Claim exclusivity under contention
// ---------------------------------------------------------------------------

#[pgqueue::job(name = "stress_reg_a", max_attempts = 1, timeout_ms = 30_000)]
async fn stress_reg_a(args: u64, ledger: JobState<Arc<Ledger>>) -> anyhow::Result<u64> {
    ledger.0.record(args);
    Ok(args)
}

#[pgqueue::job(name = "stress_reg_b", max_attempts = 1, timeout_ms = 30_000)]
async fn stress_reg_b(args: u64, ledger: JobState<Arc<Ledger>>) -> anyhow::Result<u64> {
    ledger.0.record(args);
    Ok(args)
}

/// Claim exclusivity where it has to hold: four workers contending over a
/// small ready set whose ordered prefix another transaction keeps locked. The
/// claim's `SKIP LOCKED` walk must reach past the held rows (liveness) without
/// two claims ever returning the same row (exclusivity).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "stress test"]
async fn test_stress_contended_claims_stay_exclusive() {
    const HANDLED: u64 = 400;
    let (queue, url) = stress_queue("stress_contended", |b| b).await;

    for n in 0..HANDLED {
        if n % 2 == 0 {
            queue.enqueue(stress_reg_a::job(n)).await.expect("enqueue a").unwrap();
        } else {
            queue.enqueue(stress_reg_b::job(n)).await.expect("enqueue b").unwrap();
        }
    }

    // Hold the ordered prefix of the ready rows, so every claim walks past
    // rows it cannot take before reaching one it can — the path where two
    // claims reaching the same row would show up as a job running twice.
    const HELD: i64 = 48;
    let mut holder = sqlx::PgConnection::connect(&url).await.expect("holder connect");
    let mut held = holder.begin().await.expect("hold prefix");
    let locked = sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM (
             SELECT id FROM pgqueue.jobs
             WHERE queue = 'default' AND status = 'queued'
             ORDER BY priority, scheduled_at, id
             LIMIT $1 FOR UPDATE
         ) prefix",
    )
    .bind(HELD)
    .fetch_one(&mut *held)
    .await
    .expect("lock prefix");
    assert_eq!(locked, HELD, "the prefix to hold was not there to hold");

    let ledger = Arc::new(Ledger::default());
    let shutdown = CancellationToken::new();
    let mut workers = Vec::new();
    for _ in 0..4 {
        let worker = stress_worker(queue.clone())
            .register_job(stress_reg_a)
            .register_job(stress_reg_b)
            .state(ledger.clone())
            .concurrency(6)
            .build()
            .expect("build worker");
        workers.push(tokio::spawn(worker.run_until(shutdown.clone())));
    }

    // Everything behind the held prefix drains while it is still held, so the
    // walk really does step past the locked rows rather than starving.
    wait_until(
        Duration::from_secs(120),
        Duration::from_millis(100),
        "claims did not reach past the held prefix",
        || async { complete_count(&queue).await == HANDLED as i64 - HELD },
    )
    .await;
    held.rollback().await.expect("release prefix");

    wait_until(
        Duration::from_secs(120),
        Duration::from_millis(100),
        "claims did not drain the queue after the prefix was released",
        || async { complete_count(&queue).await == HANDLED as i64 },
    )
    .await;
    shutdown.cancel();
    for worker in workers {
        worker.await.expect("worker join").expect("worker run");
    }

    // Every handled job ran, and every row is terminal exactly once. `total`
    // is the assertion that two claims never reached the same row: nothing here
    // fails or times out, so at-least-once has no licence to run one twice, and
    // `distinct` alone would count a double dispatch as a pass.
    assert_eq!(ledger.distinct() as u64, HANDLED, "a handled job never ran");
    assert_eq!(ledger.total() as u64, HANDLED, "a handled job ran more than once");
    assert_eq!(ledger.ids(), (0..HANDLED).collect::<HashSet<_>>(), "the wrong set of jobs ran");
    assert_eq!(complete_count(&queue).await, HANDLED as i64);
}

//! Adversarial input, encoding boundaries, randomized round-trips, and
//! connection accounting.
//!
//! These are not load tests — they run in the normal suite because they are
//! fast and deterministic. What they have in common is that each drives the
//! library with values a *caller* controls but a developer rarely types: SQL
//! and identifier metacharacters, multibyte text at byte-counted limits, and
//! randomly shaped JSON. The queue name is the sharpest of these, because it is
//! the one caller value that reaches a PostgreSQL *identifier* — `LISTEN` takes
//! no bind parameter — rather than a bind parameter.

use std::collections::HashSet;
use std::time::Duration;

use pgqueue::{Error, JobRow, JobStatus, Queue};
use serde_json::{Value, json};
use sqlx::PgPool;

use crate::{EnqueueResultTestExt, QueueProtocolTestExt, TestDb, list_workers, new_job};

/// Values a caller can supply that carry meaning to SQL, to PostgreSQL's
/// identifier quoting, or to `LIKE`/`starts_with` patterns.
///
/// `validate_queue_name` rejects only empty names, dot segments, names over 255
/// bytes, and control characters — so every string here is a legal queue name,
/// job name and dedupe key, and each must survive a full round trip.
const HOSTILE: &[(&str, &str)] = &[
    ("double_quote", "a\"b"),
    ("single_quote", "it's"),
    ("semicolon", "a;DROP TABLE pgqueue.jobs;--"),
    ("sql_comment", "a--b"),
    ("backslash", "a\\b"),
    ("percent", "100%_off"),
    ("dollar_quote", "a$$b"),
    ("brace", "{\"not\":\"json\"}"),
    ("newline_escape", "a\\nb"),
    ("unicode_quote", "a\u{2019}b"),
    ("emoji", "job-\u{1F600}"),
    ("rtl", "\u{202E}reversed"),
];

// ---------------------------------------------------------------------------
// Adversarial identifiers
// ---------------------------------------------------------------------------

/// The queue name reaches a channel *identifier*, not a bind parameter:
/// `Database::channel_name` interpolates it into `pgqueue_{queue}_done_{hash}`
/// and `PgListener` issues `LISTEN` on the result. A name carrying a quote or a
/// semicolon therefore has to survive identifier quoting rather than parameter
/// binding, which is a different code path with a different failure mode.
///
/// `enqueue_and_wait` alone does *not* establish that. `QueueNotifyListener::start`
/// is documented "never fails" — a refused `LISTEN` only sets the health watch and
/// logs — and `JobHandle::wait` has a polling fallback that resolves inside the
/// waiter's budget regardless, so a derived identifier PostgreSQL rejected would
/// still let every assertion here pass. The explicit `PgListener` below is what
/// pins the claim: it `LISTEN`s on exactly the identifier the crate derives, and
/// then requires the completion `NOTIFY` to arrive on it, with no fallback to hide
/// behind. The end-to-end wait is kept beside it because it covers the rest of the
/// path.
#[sqlx::test(migrations = "./migrations")]
async fn test_hostile_queue_names_survive_the_listen_notify_path(pool: PgPool) {
    // Plus one name long enough to be *truncated*. `channel_name` cuts to 46 bytes
    // on a char boundary before appending the hash, and every name above is far
    // too short to reach that path — so a cut that produced an over-long or
    // otherwise illegal identifier would have gone unnoticed here.
    // `test_channel_names_survive_multibyte_queue_names` asserts the length in
    // Rust; nothing asked PostgreSQL whether the result is a legal identifier.
    let long_multibyte = "\u{e9}".repeat(80);
    let cases = HOSTILE.iter().copied().chain(std::iter::once(("long_multibyte", long_multibyte.as_str())));
    for (label, hostile) in cases {
        let hostile = &hostile;
        let db = TestDb::with(pool.clone(), |builder| builder.name(*hostile)).await;
        assert_eq!(db.queue.name(), *hostile, "{label}: queue renamed itself");

        let waiting = db.queue.clone();
        let waiter =
            tokio::spawn(async move { waiting.enqueue_and_wait(probe::job(()), Some(Duration::from_secs(30))).await });

        // Claim and complete the row the waiter enqueued. The waiter can only
        // return once the `NOTIFY` this publishes reaches its listener.
        let claimed = crate::wait_for_some(
            Duration::from_secs(30),
            Duration::from_millis(10),
            "hostile-queue job never became claimable",
            || async {
                let batch = db.queue.dequeue(1, uuid::Uuid::now_v7()).await.unwrap();
                (!batch.is_empty()).then_some(batch)
            },
        )
        .await;
        // Subscribed before the finish, or the notification it publishes is gone
        // before anything can observe it.
        let channel = pgqueue::__test_support::done_channel(hostile);
        let mut listener = sqlx::postgres::PgListener::connect_with(&pool)
            .await
            .unwrap_or_else(|error| panic!("{label}: listener connect: {error}"));
        listener
            .listen(&channel)
            .await
            .unwrap_or_else(|error| panic!("{label}: LISTEN refused the derived channel {channel:?}: {error}"));

        assert!(
            db.queue
                .finish(&claimed[0], JobStatus::Complete, Some(json!(7u32)), None)
                .await
                .unwrap_or_else(|error| panic!("{label}: finish failed: {error}"))
        );

        let notification = tokio::time::timeout(Duration::from_secs(30), listener.recv())
            .await
            .unwrap_or_else(|_| panic!("{label}: no completion NOTIFY on the derived channel {channel:?}"))
            .unwrap_or_else(|error| panic!("{label}: listener error: {error}"));
        assert!(
            notification.payload().contains(&claimed[0].id.to_string()),
            "{label}: completion arrived on {channel:?} for another job: {}",
            notification.payload()
        );

        let value: u32 = waiter
            .await
            .unwrap_or_else(|error| panic!("{label}: waiter task: {error}"))
            .unwrap_or_else(|error| panic!("{label}: notification never arrived: {error}"));
        assert_eq!(value, 7, "{label}: waiter got the wrong result");

        let row = db.queue.fetch_job(claimed[0].id).await.unwrap().expect("row");
        assert_eq!(row.status, JobStatus::Complete, "{label}");
        assert_eq!(row.queue, *hostile, "{label}: queue column mangled");
    }
}

/// Typed so `enqueue_and_wait` above has something to deserialize; the value is
/// arbitrary, only its safe arrival matters.
#[pgqueue::job(name = "probe", max_attempts = 1)]
async fn probe(_: ()) -> anyhow::Result<u32> {
    Ok(7)
}

/// The same values as job names and dedupe keys, where they reach bind
/// parameters, a `LIKE`-adjacent prefix search, and a unique index.
#[sqlx::test(migrations = "./migrations")]
async fn test_hostile_job_names_and_dedupe_keys_round_trip(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    for (label, hostile) in HOSTILE {
        let id = db
            .queue
            .enqueue_raw(new_job(hostile, |job| {
                job.dedupe_key = Some((*hostile).to_string());
                job.payload = json!({ "value": hostile });
            }))
            .await
            .unwrap_or_else(|error| panic!("{label}: enqueue failed: {error}"))
            .unwrap();

        let row = db.queue.fetch_job(id).await.unwrap().expect("row");
        assert_eq!(row.name, *hostile, "{label}: name mangled");
        assert_eq!(row.dedupe_key.as_deref(), Some(*hostile), "{label}: key");
        assert_eq!(row.payload, json!({ "value": hostile }), "{label}: payload");

        // The dedupe key is a live-uniqueness constraint, so the same hostile
        // string must deduplicate against itself rather than escaping into a
        // second row.
        assert!(
            db.queue
                .enqueue_raw(new_job(hostile, |job| {
                    job.dedupe_key = Some((*hostile).to_string());
                }))
                .await
                .unwrap()
                .is_none(),
            "{label}: hostile dedupe key did not deduplicate"
        );
    }

    // `%` and `_` are `LIKE` wildcards; the listing's name filter is an
    // equality, so a literal `100%_off` must not match `100XYoff`.
    db.queue.enqueue_raw(new_job("100XYoff", |_| {})).await.unwrap().unwrap();
    let matched = db
        .queue
        .jobs_page(pgqueue::JobFilter { name: Some("100%_off".to_string()), limit: Some(100), ..Default::default() })
        .await
        .unwrap();
    assert_eq!(matched.len(), 1, "wildcards were interpreted as a pattern");
    assert_eq!(matched[0].name, "100%_off");

    // The dashboard's job-name typeahead is the prefix search: every hostile
    // name must be found under itself, and `%` must stay a literal there too,
    // or the prefix `100%` would also offer `100XYoff`.
    let router = pgqueue::Dashboard::new([db.queue.clone()]).allow_unauthenticated().router().unwrap();
    let suggest = async |prefix: &str| {
        let path = format!("/api/queues/default/job-names?kind=job&prefix={}", percent_encode(prefix));
        let (status, body) = crate::dashboard_test::request(&router, "GET", &path, None).await;
        assert_eq!(status, axum::http::StatusCode::OK, "{prefix:?}: {body}");
        body["names"].as_array().unwrap().iter().map(|name| name.as_str().unwrap().to_string()).collect::<Vec<_>>()
    };
    for (label, hostile) in HOSTILE {
        assert!(suggest(hostile).await.iter().any(|name| name == hostile), "{label}: not suggested under itself");
    }
    assert_eq!(suggest("100%").await, ["100%_off"], "the prefix search interpreted `%` as a wildcard");
}

/// Percent-encodes every byte outside RFC 3986's unreserved set, so a hostile
/// value reaches a query string as exactly the bytes it is.
fn percent_encode(value: &str) -> String {
    value
        .bytes()
        .map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => char::from(byte).to_string(),
            _ => format!("%{byte:02X}"),
        })
        .collect()
}

/// After every hostile value above, the schema must be exactly as migrated —
/// the `DROP TABLE` payloads are only interesting if they were never executed.
#[sqlx::test(migrations = "./migrations")]
async fn test_hostile_values_leave_the_schema_intact(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    for (_, hostile) in HOSTILE {
        db.queue
            .enqueue_raw(new_job(hostile, |job| {
                job.dedupe_key = Some((*hostile).to_string());
            }))
            .await
            .unwrap()
            .unwrap();
    }

    let tables = sqlx::query_scalar::<_, String>(
        "SELECT tablename FROM pg_tables WHERE schemaname = 'pgqueue' ORDER BY tablename",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    // Exact equality rather than a presence check: an injected `CREATE TABLE`
    // would show up as an extra entry. `migrations` is the migrator's own
    // bookkeeping table, which lives in this schema too.
    assert_eq!(
        tables,
        vec![
            "cron_occurrences",
            "cron_schedules",
            "jobs",
            "migrations",
            "workers"
        ],
        "schema changed while hostile values were stored"
    );
    let rows = sqlx::query_scalar::<_, i64>("SELECT count(*) FROM pgqueue.jobs").fetch_one(&pool).await.unwrap();
    assert_eq!(rows, HOSTILE.len() as i64, "row count does not match");
}

// ---------------------------------------------------------------------------
// Encoding boundaries
// ---------------------------------------------------------------------------

/// The limits are byte counts (`queue.len() > 255`, `octet_length(...) <= 255`
/// in the schema), while the control-character rule is per character. Every
/// existing limit test spells its input with ASCII, where the two coincide;
/// these use 3- and 4-byte characters, where they do not.
#[sqlx::test(migrations = "./migrations")]
async fn test_length_limits_are_measured_in_bytes_not_characters(pool: PgPool) {
    // U+1F600 is 4 bytes: 63 of them are 252 bytes, 64 are 256.
    let within = "\u{1F600}".repeat(63);
    let beyond = "\u{1F600}".repeat(64);
    assert_eq!(within.len(), 252);
    assert_eq!(beyond.len(), 256);

    let db = TestDb::with(pool.clone(), |builder| builder.name(&within)).await;
    let id = db
        .queue
        .enqueue_raw(new_job(&within, |job| {
            job.dedupe_key = Some(within.clone());
        }))
        .await
        .expect("252 bytes of emoji is within every limit")
        .unwrap();
    let row = db.queue.fetch_job(id).await.unwrap().expect("row");
    assert_eq!(row.name, within);
    assert_eq!(row.queue, within);

    // 64 characters is only 64 characters, but 256 bytes — over the limit.
    let error = Queue::builder("postgres://unused")
        .pool(pool.clone())
        .name(&beyond)
        .connect()
        .await
        .expect_err("256 bytes must be refused even as 64 characters");
    assert!(error.to_string().contains("255"), "unexpected error: {error}");
    let error = db.queue.enqueue_raw(new_job(&beyond, |_| {})).await.expect_err("a 256-byte job name must be refused");
    assert!(error.to_string().contains("255"), "unexpected error: {error}");
}

/// A queue name longer than the 46 bytes a channel name keeps is truncated to
/// make room for the hash, and the cut has to land on a character boundary or
/// slicing it panics.
///
/// The two fixtures are chosen to need different step-backs. `channel_name`
/// builds `pgqueue_{queue}{suffix}`, and `pgqueue_` is eight bytes, so byte 46
/// sits 38 bytes into the queue name. That is two bytes into a four-byte
/// character for a bare emoji run, so its cut steps back two, to 44; one leading
/// ASCII byte makes it one byte into a two-byte character, so that cut steps
/// back one, to 45. `46.min(full.len())` fails both, and a search that stepped
/// back a fixed amount rather than to the nearest boundary fails at least one.
///
/// Two queues whose names share their first 46 bytes must also stay on separate
/// channels, or a completion on one would wake a waiter on the other.
#[sqlx::test(migrations = "./migrations")]
async fn test_channel_names_survive_multibyte_queue_names(pool: PgPool) {
    let shared = "\u{1F600}".repeat(20); // 80 bytes, cut steps back two, to 44
    let first = format!("{shared}-one");
    let second = format!("{shared}-two");

    let one = TestDb::with(pool.clone(), |builder| builder.name(&first)).await;
    let two = TestDb::with(pool.clone(), |builder| builder.name(&second)).await;
    let (channel_one, channel_two) =
        (pgqueue::__test_support::done_channel(&first), pgqueue::__test_support::done_channel(&second));
    // PostgreSQL truncates identifiers at 63 bytes, so a longer channel name is
    // not merely ugly: two names that differ only past byte 63 become the same
    // channel server-side, and a completion on one queue would wake a waiter on
    // the other. Asserting inequality in Rust alone would miss exactly that.
    for (name, channel) in [(&first, &channel_one), (&second, &channel_two)] {
        assert!(channel.len() <= 63, "channel for an {}-byte name is {} bytes: {channel}", name.len(), channel.len());
    }
    assert_ne!(channel_one, channel_two, "queues sharing a 46-byte prefix collided on one channel");

    // The two-byte run as well, whose cut steps back one byte rather than two: a
    // search that stepped back by a fixed amount rather than to the nearest
    // boundary would pass for one width and not the other.
    let two_byte = format!("x{}", "\u{e9}".repeat(80));
    let two_byte_channel = pgqueue::__test_support::done_channel(&two_byte);
    assert!(two_byte_channel.len() <= 63, "channel is {} bytes: {two_byte_channel}", two_byte_channel.len());
    let three = TestDb::with(pool.clone(), |builder| builder.name(&two_byte)).await;
    let id = three.queue.enqueue_raw(new_job("probe", |_| {})).await.unwrap().unwrap();
    let claimed = three.queue.dequeue(1, uuid::Uuid::now_v7()).await.unwrap();
    three.queue.finish(&claimed[0], JobStatus::Complete, Some(json!(1)), None).await.unwrap();
    assert_eq!(three.queue.fetch_job(id).await.unwrap().unwrap().status, JobStatus::Complete);

    // Both must actually work end to end: an invalid identifier would fail at LISTEN rather than at any assertion
    // above. So `LISTEN` on exactly the identifier the crate derives, and require the completion to arrive on it: a
    // finish notifies only an awaited row, so without both, nothing here ever asked PostgreSQL about the step-back cut.
    for (db, channel) in [(&one, &channel_one), (&two, &channel_two)] {
        let mut listener = sqlx::postgres::PgListener::connect_with(&pool).await.unwrap();
        listener.listen(channel).await.unwrap_or_else(|error| panic!("LISTEN refused {channel:?}: {error}"));
        let id = db.queue.enqueue_raw(new_job("probe", |_| {})).await.unwrap().unwrap();
        sqlx::query("UPDATE pgqueue.jobs SET awaited = true WHERE id = $1").bind(id).execute(&pool).await.unwrap();
        let claimed = db.queue.dequeue(1, uuid::Uuid::now_v7()).await.unwrap();
        db.queue.finish(&claimed[0], JobStatus::Complete, Some(json!(1)), None).await.unwrap();
        let notification = tokio::time::timeout(Duration::from_secs(10), listener.recv())
            .await
            .unwrap_or_else(|_| panic!("no completion NOTIFY on the derived channel {channel:?}"))
            .unwrap();
        assert!(notification.payload().contains(&id.to_string()), "{}", notification.payload());
        assert_eq!(db.queue.fetch_job(id).await.unwrap().unwrap().status, JobStatus::Complete);
    }
}

/// Text that survives a `jsonb` round trip only if nothing along the way
/// re-encodes it: astral-plane characters, combining marks, zero-width joiners,
/// bidi overrides, and the escapes JSON itself uses.
#[sqlx::test(migrations = "./migrations")]
async fn test_unicode_payloads_round_trip_byte_for_byte(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let payload = json!({
        "astral": "\u{1F469}\u{200D}\u{1F4BB}",
        "combining": "e\u{0301}\u{0327}",
        "bidi": "\u{202E}txet\u{202C}",
        "cjk": "\u{6F22}\u{5B57}",
        "escapes": "quote\" backslash\\ slash/ tab\t newline\n",
        "keys are values too": { "\u{1F600}": "\u{1F600}" },
    });

    let id = db
        .queue
        .enqueue_raw(new_job("unicode", |job| {
            job.payload = payload.clone();
            job.meta = payload.clone();
        }))
        .await
        .unwrap()
        .unwrap();

    let claimed = db.queue.dequeue(1, uuid::Uuid::now_v7()).await.unwrap();
    assert_eq!(claimed[0].payload, payload, "payload changed on the way out");
    db.queue.finish(&claimed[0], JobStatus::Complete, Some(payload.clone()), None).await.unwrap();

    let row = db.queue.fetch_job(id).await.unwrap().expect("row");
    assert_eq!(row.payload, payload, "payload changed in storage");
    assert_eq!(row.meta, payload, "meta changed in storage");
    assert_eq!(row.result, Some(payload), "result changed in storage");
}

// ---------------------------------------------------------------------------
// Randomized round-trips
// ---------------------------------------------------------------------------

/// xorshift64*, so the shapes below are random but the run is reproducible from
/// the seed printed in any failure. A dependency-free PRNG keeps this a test
/// helper rather than a new crate in the tree.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }
}

/// A random JSON value. `depth` bounds nesting well under the writers' depth
/// guard so this exercises encoding, not the guard.
fn random_json(rng: &mut Rng, depth: u32) -> Value {
    match rng.below(if depth == 0 { 5 } else { 7 }) {
        0 => Value::Null,
        1 => json!(rng.below(2) == 1),
        // Both integer and fractional numbers: `serde_json` stores them
        // differently and `jsonb` normalizes numerics.
        2 => json!(rng.next() as i64 / 1_000),
        3 => json!((rng.below(1_000_000) as f64) / 1_024.0),
        4 => Value::String(random_string(rng)),
        5 => Value::Array((0..rng.below(4)).map(|_| random_json(rng, depth - 1)).collect()),
        _ => Value::Object((0..rng.below(4)).map(|_| (random_string(rng), random_json(rng, depth - 1))).collect()),
    }
}

/// Draws from an alphabet of characters that are awkward for one layer or
/// another: JSON escapes, SQL quoting, and multibyte UTF-8.
fn random_string(rng: &mut Rng) -> String {
    const ALPHABET: &[char] = &[
        'a',
        'Z',
        '0',
        ' ',
        '"',
        '\'',
        '\\',
        '/',
        '\n',
        '\t',
        '%',
        '_',
        '$',
        '{',
        '}',
        '\u{00E9}',
        '\u{6F22}',
        '\u{1F600}',
        '\u{200D}',
        '\u{202E}',
    ];
    (0..rng.below(12)).map(|_| ALPHABET[rng.below(ALPHABET.len() as u64) as usize]).collect()
}

/// Randomly shaped payloads, results and metadata must come back exactly as
/// they went in. A fixed seed keeps the run reproducible; the value is in the
/// shapes a hand-written fixture would never think to include.
#[sqlx::test(migrations = "./migrations")]
async fn test_random_json_payloads_round_trip_exactly(pool: PgPool) {
    const SEED: u64 = 0x5EED_1234_ABCD_0001;
    const CASES: usize = 200;
    let db = TestDb::new(pool.clone()).await;
    let mut rng = Rng(SEED);

    for case in 0..CASES {
        let payload = random_json(&mut rng, 3);
        let meta = match random_json(&mut rng, 2) {
            // `meta` is `jsonb NOT NULL DEFAULT '{}'`; the column accepts any
            // JSON, but the request's own default is an object, so keep the
            // shape a caller would actually send.
            Value::Object(map) => Value::Object(map),
            other => json!({ "value": other }),
        };
        let result = random_json(&mut rng, 3);

        let id = db
            .queue
            .enqueue_raw(new_job("random", |job| {
                job.payload = payload.clone();
                job.meta = meta.clone();
            }))
            .await
            .unwrap_or_else(|error| panic!("seed {SEED:#x} case {case}: enqueue: {error}"))
            .unwrap();

        let claimed = db.queue.dequeue(1, uuid::Uuid::now_v7()).await.unwrap();
        assert_eq!(claimed[0].payload, payload, "seed {SEED:#x} case {case}: payload changed on dequeue");
        db.queue.finish(&claimed[0], JobStatus::Complete, Some(result.clone()), None).await.unwrap();

        let row = db.queue.fetch_job(id).await.unwrap().expect("row");
        assert_eq!(row.payload, payload, "seed {SEED:#x} case {case}: payload changed in storage");
        assert_eq!(row.meta, meta, "seed {SEED:#x} case {case}: meta changed in storage");
        assert_eq!(row.result, Some(result), "seed {SEED:#x} case {case}: result changed in storage");
    }
}

/// The same generator against the error column, which is `text` rather than
/// `jsonb`: PostgreSQL refuses a NUL in `text`, and the library is expected to
/// reject that at the boundary rather than raise `22021` from the driver.
#[sqlx::test(migrations = "./migrations")]
async fn test_random_error_strings_are_stored_or_refused_cleanly(pool: PgPool) {
    const SEED: u64 = 0x5EED_1234_ABCD_0002;
    let db = TestDb::new(pool.clone()).await;
    let mut rng = Rng(SEED);

    for case in 0..60 {
        let id = db.queue.enqueue_raw(new_job("random-error", |_| {})).await.unwrap().unwrap();
        let claimed = db.queue.dequeue(1, uuid::Uuid::now_v7()).await.unwrap();
        let mut message = random_string(&mut rng);
        // The shared alphabet carries no NUL, because `jsonb` refuses one as `text` does and the payload round trip
        // draws from it too, so every fifth message gets one here.
        if case % 5 == 0 {
            let at = rng.below(message.chars().count() as u64 + 1) as usize;
            let index = message.char_indices().nth(at).map_or(message.len(), |(index, _)| index);
            message.insert(index, '\0');
        }

        let finished = db.queue.finish(&claimed[0], JobStatus::Failed, None, Some(&message)).await;
        let row = db.queue.fetch_job(id).await.unwrap().expect("row");
        if message.contains('\0') {
            assert!(
                matches!(&finished, Err(Error::Config(reason)) if reason == "job error must not contain NUL"),
                "seed {SEED:#x} case {case}: {finished:?}"
            );
            assert_eq!(row.status, JobStatus::Running, "seed {SEED:#x} case {case}: a refused finish moved the row");
        } else {
            finished.unwrap_or_else(|error| panic!("seed {SEED:#x} case {case}: finish: {error}"));
            assert_eq!(
                row.error.as_deref(),
                Some(message.as_str()),
                "seed {SEED:#x} case {case}: error text changed in storage"
            );
        }
    }
}

#[sqlx::test(migrations = "./migrations")]
async fn test_oversized_errors_are_truncated_before_storage(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db.queue.enqueue_raw(new_job("oversized-error", |_| {})).await.unwrap().unwrap();
    let message = "é".repeat(600_000);

    assert!(db.queue.abort_job(id, &message).await.unwrap());
    let stored = db.queue.fetch_job(id).await.unwrap().expect("aborted job").error.expect("stored error");

    assert!(stored.len() <= 1_048_576);
    assert!(stored.len() >= 1_048_573);
    assert!(stored.ends_with("… [truncated]"));
}

// ---------------------------------------------------------------------------
// Connection accounting
// ---------------------------------------------------------------------------

/// Every queue operation borrows a pooled connection and must give it back. A
/// leak does not fail anything immediately — it shows up much later as
/// `acquire_timeout` on a pool that looks idle — so the check is that a pool of
/// two connections can serve far more operations than it has connections, and
/// that the server-side backend count does not grow while it does.
#[sqlx::test(migrations = "./migrations")]
async fn test_pooled_connections_are_returned_across_many_operations(pool: PgPool) {
    let db = TestDb::new(crate::pool_with_max(&pool, 2).await).await;
    let backends = || async {
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM pg_stat_activity
             WHERE datname = current_database() AND pid <> pg_backend_pid()",
        )
        .fetch_one(&pool)
        .await
        .unwrap()
    };

    // Warm the pool so the baseline is the steady state, not a cold start.
    for _ in 0..4 {
        db.queue.counts().await.unwrap();
    }
    let baseline = backends().await;

    // Two hundred full lifecycles through a two-connection pool: this can only
    // complete if every borrow is returned.
    for round in 0..200 {
        let id = db
            .queue
            .enqueue_raw(new_job("cycle", |_| {}))
            .await
            .unwrap_or_else(|error| panic!("round {round}: enqueue: {error}"))
            .unwrap();
        let claimed = db.queue.dequeue(1, uuid::Uuid::now_v7()).await.unwrap();
        db.queue.finish(&claimed[0], JobStatus::Complete, Some(json!(round)), None).await.unwrap();
        db.queue.fetch_job(id).await.unwrap().expect("row");
        db.queue.counts().await.unwrap();
    }

    let after = backends().await;
    assert!(after <= baseline + 1, "backend count grew from {baseline} to {after} across 200 lifecycles");
}

/// A pool with one connection has to serialize, not deadlock: a single
/// operation must never need two connections at once. Bounded so a regression
/// that does need two fails the test instead of hanging the suite.
#[sqlx::test(migrations = "./migrations")]
async fn test_no_operation_needs_two_connections_at_once(pool: PgPool) {
    let db = TestDb::new(crate::pool_with_max(&pool, 1).await).await;
    let work = async {
        let id = db.queue.enqueue_raw(new_job("single", |_| {})).await.unwrap().unwrap();
        let claimed = db.queue.dequeue(1, uuid::Uuid::now_v7()).await.unwrap();
        db.queue.finish(&claimed[0], JobStatus::Complete, Some(json!(1)), None).await.unwrap();
        db.queue.counts().await.unwrap();
        list_workers(&db.queue).await;
        db.queue.fetch_job(id).await.unwrap().expect("row");
        db.queue.jobs_page(pgqueue::JobFilter::default()).await.unwrap();
    };
    tokio::time::timeout(Duration::from_secs(30), work)
        .await
        .expect("an operation deadlocked on a single-connection pool");
}

// ---------------------------------------------------------------------------
// Fault injection
// ---------------------------------------------------------------------------

/// A pooled connection killed out from under the queue must not corrupt state:
/// the operation either fails cleanly or succeeds on a fresh connection, and
/// the queue keeps working afterwards. `pg_terminate_backend` is the closest
/// thing to a failover this suite can stage.
#[sqlx::test(migrations = "./migrations")]
async fn test_the_queue_recovers_when_its_backends_are_terminated(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let mut committed = HashSet::new();

    for round in 0..20 {
        // Kill everything this database has open, mid-stream.
        sqlx::query(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity
             WHERE datname = current_database() AND pid <> pg_backend_pid()",
        )
        .execute(&pool)
        .await
        .ok();

        // The enqueue either fails (the connection was killed under it) or
        // commits. Both are acceptable; silently losing a committed row is not.
        if let Ok(result) = db.queue.enqueue_raw(new_job("survivor", |_| {})).await {
            committed.insert(result.job_id());
        }
        // And the queue must be usable again immediately afterwards.
        let id = wait_until_enqueued(&db.queue, round).await;
        committed.insert(id);
    }

    for id in &committed {
        assert!(
            db.queue.fetch_job(*id).await.unwrap().is_some(),
            "job {id} reported as enqueued but is not in the table"
        );
    }
    let stored = sqlx::query_scalar::<_, i64>("SELECT count(*) FROM pgqueue.jobs").fetch_one(&pool).await.unwrap();
    assert_eq!(stored, committed.len() as i64, "the table holds rows the queue never reported enqueuing");
}

/// A keyless insert has no conflict target, so nothing in this crate can make it return no row, but a `BEFORE INSERT`
/// trigger on the table that returns `NULL` skips the row silently. Both single enqueues treated that as unreachable
/// and panicked in the caller's own task, where the batch already answered it with an error.
#[sqlx::test(migrations = "./migrations")]
async fn test_an_insert_a_trigger_skips_is_an_error_rather_than_a_panic(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    sqlx::raw_sql(
        "CREATE FUNCTION pgqueue.skip_insert() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$;
         CREATE TRIGGER skip_insert BEFORE INSERT ON pgqueue.jobs
             FOR EACH ROW EXECUTE FUNCTION pgqueue.skip_insert();",
    )
    .execute(&pool)
    .await
    .unwrap();
    let is_skipped = |error: &Error| matches!(error, Error::Config(message) if message.contains("trigger"));

    let error = db.queue.enqueue_raw(new_job("skipped", |_| {})).await.map(|result| result.job_id()).unwrap_err();
    assert!(is_skipped(&error), "{error}");

    let mut transaction = pool.begin().await.unwrap();
    let error = db
        .queue
        .enqueue_raw_in(&mut transaction, new_job("skipped", |_| {}))
        .await
        .map(|result| result.job_id())
        .unwrap_err();
    assert!(is_skipped(&error), "{error}");
    transaction.rollback().await.unwrap();

    let stored = sqlx::query_scalar::<_, i64>("SELECT count(*) FROM pgqueue.jobs").fetch_one(&pool).await.unwrap();
    assert_eq!(stored, 0);
}

/// Retries an enqueue until the pool has replaced its terminated connections.
/// Bounded, so a queue that never recovers fails here instead of hanging.
async fn wait_until_enqueued(queue: &Queue, round: usize) -> uuid::Uuid {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        match queue.enqueue_raw(new_job("recovered", |_| {})).await {
            Ok(result) => return result.job_id(),
            Err(error) => {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "round {round}: queue never became usable again after its backends \
                     were terminated: {error}"
                );
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Indeterminate dequeue commits
// ---------------------------------------------------------------------------

/// The dequeue claim commits server-side before the client learns of it, so a
/// COMMIT whose acknowledgement is lost can leave rows `running` under a live,
/// heartbeating worker that never received them — in no in-flight registry, so
/// its abort loop never polls them, and inside the sweeper's live-owner
/// cooperative window, which waits for an owner that cannot answer. The
/// resolver the commit error spawns must settle both outcomes of the lost
/// acknowledgement: a claim the server committed is requeued with the attempt
/// refunded (it never ran), the owner cleared and the worker's intake left
/// open, while a claim that never landed matches no row and settles as done.
#[sqlx::test(migrations = "./migrations")]
async fn test_unacknowledged_dequeue_claims_are_requeued_with_the_attempt_refunded(pool: PgPool) {
    let db = TestDb::new(pool).await;
    let worker_id = uuid::Uuid::now_v7();
    db.queue.write_worker_info(worker_id, json!({}), None, Duration::from_secs(30)).await.unwrap();

    let committed = db
        .queue
        .enqueue_raw(crate::with_config("unacked_committed", |config| config.max_attempts = 3))
        .await
        .unwrap()
        .job_id();
    let uncommitted = db.queue.enqueue_raw(new_job("unacked_pending", |_| {})).await.unwrap().job_id();

    // The committed half of the race: this claim really did land server-side.
    let claimed = pgqueue::__test_support::dequeue_worker(&db.queue, 1, worker_id).await.unwrap();
    assert_eq!(claimed.len(), 1);
    let claimed = &claimed[0];
    assert_eq!(claimed.attempts, 1);

    // The uncommitted half never claimed anything, but the resolver only holds
    // the decoded rows, which carry the attempt number a claim *would* have.
    let resolved = pgqueue::__test_support::requeue_unacknowledged_claims(
        &db.queue,
        worker_id,
        &[
            (claimed.id, claimed.attempts, claimed.started_at),
            (uncommitted, 1, None),
        ],
    )
    .await
    .unwrap();
    assert_eq!(resolved, 1, "only the committed claim matches a row");
    assert_eq!(db.queue.stats().retried, 0, "an attempt that never ran is refunded, not retried");
    assert_eq!(db.queue.stats().aborted, 0);

    let requeued = db.queue.fetch_job(committed).await.unwrap().expect("committed row is retained");
    assert_eq!(requeued.status, JobStatus::Queued);
    assert_eq!(requeued.attempts, 1, "the spent counter stays, so the old claim's writes remain fenced out");
    assert_eq!(requeued.max_attempts, 4, "the attempt that never ran is refunded");
    assert_eq!(requeued.worker_id, None);
    assert_eq!(requeued.error.as_deref(), Some("dequeue commit was not acknowledged"));
    assert_eq!(requeued.scheduled_at, claimed.scheduled_at, "an attempt that never ran earns no retry delay");

    let untouched = db.queue.fetch_job(uncommitted).await.unwrap().expect("uncommitted row is retained");
    assert_eq!(untouched.status, JobStatus::Queued);
    assert_eq!(untouched.attempts, 0);
    assert_eq!(untouched.error, None);

    // The worker is alive and healthy, so resolution must not close its intake:
    // the requeued row is immediately claimable again by the same worker.
    let accepting = sqlx::query_scalar::<_, bool>("SELECT accepting FROM pgqueue.workers WHERE id = $1")
        .bind(worker_id)
        .fetch_one(db.queue.pool())
        .await
        .unwrap();
    assert!(accepting, "resolving unacknowledged claims must not close a live worker's intake");
    let reclaimed = pgqueue::__test_support::dequeue_worker(&db.queue, 1, worker_id).await.unwrap();
    assert_eq!(reclaimed.len(), 1);
    assert_eq!(reclaimed[0].id, committed);
    assert_eq!(reclaimed[0].attempts, 2);
}

/// A claim the sweeper marked `aborting` before the resolver ran is still this
/// worker's to give back: the marker-guarded arm requeues it exactly as the
/// owner's own `retry_swept` would, clearing the marker pair, instead of
/// leaving the row to a cooperative owner that never learned of the attempt.
#[sqlx::test(migrations = "./migrations")]
async fn test_unacknowledged_claim_resolution_reclaims_a_sweeper_marked_row(pool: PgPool) {
    let db = TestDb::new(pool).await;
    let worker_id = uuid::Uuid::now_v7();
    db.queue.write_worker_info(worker_id, json!({}), None, Duration::from_secs(30)).await.unwrap();
    db.queue.enqueue_raw(crate::with_config("unacked_swept", |config| config.max_attempts = 2)).await.unwrap();
    let claimed = pgqueue::__test_support::dequeue_worker(&db.queue, 1, worker_id).await.unwrap();
    let claimed = &claimed[0];

    // The sweeper's phase one: mark the stuck-looking attempt for abort.
    sqlx::query(
        r#"UPDATE pgqueue.jobs
           SET status = 'aborting', error = 'swept', result = '"pgqueue:swept"'::jsonb
           WHERE id = $1"#,
    )
    .bind(claimed.id)
    .execute(db.queue.pool())
    .await
    .unwrap();

    let resolved = pgqueue::__test_support::requeue_unacknowledged_claims(
        &db.queue,
        worker_id,
        &[(claimed.id, claimed.attempts, claimed.started_at)],
    )
    .await
    .unwrap();
    assert_eq!(resolved, 1);
    let requeued = db.queue.fetch_job(claimed.id).await.unwrap().expect("row is retained");
    assert_eq!(requeued.status, JobStatus::Queued);
    assert_eq!(requeued.attempts, 1);
    assert_eq!(requeued.max_attempts, 3);
    assert_eq!(requeued.result, None, "the sweeper's marker pair must not survive the requeue");
    assert_eq!(requeued.error.as_deref(), Some("dequeue commit was not acknowledged"));
}

// ---------------------------------------------------------------------------
// Attempt counter bounds
// ---------------------------------------------------------------------------

/// `jobs_attempts_range_check`, in `0001_migration.sql` for the same reason as
/// its other bounds: foreign SQL writers are supported, and the claim
/// computes `attempts + 1` in `integer` over whichever queued rows sort first.
/// A hand-written row at `i32::MAX` raised `22003` from inside every claim
/// batch that selected it — and, sorted to the front, was selected by every
/// retry, blocking all matching work behind it until manual repair.
#[sqlx::test(migrations = "./migrations")]
async fn test_attempt_counter_bounds_refuse_rows_that_would_poison_the_claim(pool: PgPool) {
    let db = TestDb::new(pool).await;
    for (label, status, attempts, max_attempts) in [
        ("attempts at i32::MAX", "queued", 2147483647i32, 2147483647i32),
        ("max_attempts at i32::MAX", "queued", 0, 2147483647),
        ("negative attempts", "queued", -1, 1),
        ("zero max_attempts", "queued", 0, 0),
        ("attempts past max_attempts", "complete", 5, 3),
        ("queued with no attempt left", "queued", 3, 3),
    ] {
        let error = sqlx::query(
            "INSERT INTO pgqueue.jobs (queue, name, status, attempts, max_attempts)
             VALUES ($1, 'poison', $2, $3, $4)",
        )
        .bind(db.queue.name())
        .bind(status)
        .bind(attempts)
        .bind(max_attempts)
        .execute(db.queue.pool())
        .await
        .expect_err(label);
        let code = match &error {
            sqlx::Error::Database(database_error) => database_error.code().map(|code| code.to_string()),
            other => panic!("{label}: expected a check violation, got {other}"),
        };
        assert_eq!(code.as_deref(), Some("23514"), "{label}: {error}");
    }

    // The boundary a legitimate claim reaches: the last allowed attempt at the
    // cap claims cleanly to `attempts = max_attempts = 2147483646`, is refused
    // a further retry, and finishes.
    db.queue.write_worker_info(uuid::Uuid::nil(), json!({}), None, Duration::from_secs(30)).await.unwrap();
    sqlx::query(
        "INSERT INTO pgqueue.jobs (queue, name, status, attempts, max_attempts)
         VALUES ($1, 'boundary', 'queued', 2147483645, 2147483646)",
    )
    .bind(db.queue.name())
    .execute(db.queue.pool())
    .await
    .unwrap();
    let batch = pgqueue::__test_support::dequeue_worker(&db.queue, 1, uuid::Uuid::nil()).await.unwrap();
    assert_eq!(batch.len(), 1, "the boundary row must be claimable");
    assert_eq!(batch[0].attempts, 2147483646);
    assert!(!db.queue.retry(&batch[0], "failed: boom").await.unwrap(), "no attempts remain at the cap");
    assert!(db.queue.finish(&batch[0], JobStatus::Failed, None, Some("failed: boom")).await.unwrap());
}

/// At the `max_attempts` ceiling there is no attempt left to grant, so the two
/// paths that would grant one refuse instead of writing a row whose next claim
/// the range check would refuse: the shutdown requeue's refund, and the manual
/// retry's fresh occurrence with `max_attempts = attempts + 1`.
#[sqlx::test(migrations = "./migrations")]
async fn test_attempt_refunds_and_manual_retries_stop_at_the_max_attempts_ceiling(pool: PgPool) {
    let db = TestDb::new(pool).await;
    let worker_id = uuid::Uuid::now_v7();
    db.queue.write_worker_info(worker_id, json!({}), None, Duration::from_secs(30)).await.unwrap();

    let running = sqlx::query_scalar::<_, uuid::Uuid>(
        "INSERT INTO pgqueue.jobs (queue, name, status, attempts, max_attempts, worker_id, started_at, touched_at)
         VALUES ($1, 'ceiling_running', 'running', 2147483646, 2147483646, $2, now(), now())
         RETURNING id",
    )
    .bind(db.queue.name())
    .bind(worker_id)
    .fetch_one(db.queue.pool())
    .await
    .unwrap();
    let row = db.queue.fetch_job(running).await.unwrap().expect("running row is retained");
    assert!(!db.queue.requeue_shutdown(&row, "cancelled").await.unwrap(), "nothing can be refunded at the ceiling");
    let unchanged = db.queue.fetch_job(running).await.unwrap().expect("running row is retained");
    assert_eq!(unchanged.status, JobStatus::Running, "a refused refund leaves the row alone");

    let failed = sqlx::query_scalar::<_, uuid::Uuid>(
        "INSERT INTO pgqueue.jobs (queue, name, status, attempts, max_attempts, completed_at)
         VALUES ($1, 'ceiling_failed', 'failed', 2147483646, 2147483646, now())
         RETURNING id",
    )
    .bind(db.queue.name())
    .fetch_one(db.queue.pool())
    .await
    .unwrap();
    assert_eq!(db.queue.retry_job_occurrence(failed, "manual retry").await.unwrap(), None);
    let source = db.queue.fetch_job(failed).await.unwrap().expect("failed row is retained");
    assert_eq!(source.retried_at, None, "a refused retry must not consume the row's one retry");
}

/// The commit guard's other trigger: not a commit that *returned* an error,
/// but a dequeue future dropped while the outcome was unknown — the worker's
/// operation deadline cancelling a wedged dequeue, or a custom consumer
/// dropping its call mid-await. The cancelled future cannot report anything,
/// so the guard's drop is what hands the claims to the background resolver.
#[sqlx::test(migrations = "./migrations")]
async fn test_a_cancelled_dequeue_commit_hands_its_claims_to_the_resolver(pool: PgPool) {
    let db = TestDb::new(pool).await;
    let worker_id = uuid::Uuid::now_v7();
    db.queue.write_worker_info(worker_id, json!({}), None, Duration::from_secs(30)).await.unwrap();
    let committed = db
        .queue
        .enqueue_raw(crate::with_config("unacked_cancelled", |config| config.max_attempts = 2))
        .await
        .unwrap()
        .job_id();
    let claimed = pgqueue::__test_support::dequeue_worker(&db.queue, 1, worker_id).await.unwrap();
    let claimed = &claimed[0];

    pgqueue::__test_support::drop_armed_dequeue_claim_guard(
        &db.queue,
        worker_id,
        &[(claimed.id, claimed.attempts, claimed.started_at)],
    );

    // The resolver runs detached on this runtime; poll for its effect.
    crate::wait_until(
        Duration::from_secs(10),
        Duration::from_millis(20),
        "the dropped guard never handed the claim back",
        || async { db.queue.fetch_job(committed).await.unwrap().is_some_and(|row| row.status == JobStatus::Queued) },
    )
    .await;
    let row = db.queue.fetch_job(committed).await.unwrap().expect("requeued row is retained");
    assert_eq!(row.attempts, 1);
    assert_eq!(row.max_attempts, 3, "the attempt that never ran is refunded");
    assert_eq!(row.worker_id, None);
    assert_eq!(row.error.as_deref(), Some("dequeue commit was not acknowledged"));
}

/// A committed claim whose refund is refused — `attempts = max_attempts` at
/// the 2147483646 ceiling has nothing left to grant — must not be popped as
/// settled while its row sits `running` under an owner that never learned of
/// it. It finishes `aborted` instead, the sweeper's own answer for a recovered
/// attempt with no attempts left, with the unacknowledged-commit reason on the
/// row for the operator.
#[sqlx::test(migrations = "./migrations")]
async fn test_unacknowledged_claim_at_the_attempt_ceiling_is_finished_aborted(pool: PgPool) {
    let db = TestDb::new(pool).await;
    let worker_id = uuid::Uuid::now_v7();
    db.queue.write_worker_info(worker_id, json!({}), None, Duration::from_secs(30)).await.unwrap();
    let ceiling = sqlx::query_scalar::<_, uuid::Uuid>(
        "INSERT INTO pgqueue.jobs (queue, name, status, attempts, max_attempts, worker_id, started_at, touched_at)
         VALUES ($1, 'unacked_ceiling', 'running', 2147483646, 2147483646, $2, now(), now())
         RETURNING id",
    )
    .bind(db.queue.name())
    .bind(worker_id)
    .fetch_one(db.queue.pool())
    .await
    .unwrap();

    let resolved =
        pgqueue::__test_support::requeue_unacknowledged_claims(&db.queue, worker_id, &[(ceiling, 2147483646, None)])
            .await
            .unwrap();

    assert_eq!(resolved, 0, "nothing was requeued; the ceiling row finished instead");
    let row = db.queue.fetch_job(ceiling).await.unwrap().expect("aborted row is retained");
    assert_eq!(row.status, JobStatus::Aborted);
    assert_eq!(row.error.as_deref(), Some("dequeue commit was not acknowledged"));
    assert_eq!(row.result, None);
    assert_eq!(db.queue.stats().aborted, 1);
    assert_eq!(db.queue.stats().retried, 0);
}

/// Resolution is ordered strictly behind the claim transaction it resolves:
/// the claim takes a transaction-scoped advisory lock inside its claiming
/// statement, and the resolver takes the same lock before reading anything. A
/// resolver that skipped the ordering and raced an in-flight COMMIT read the
/// pre-commit snapshot, matched neither guarded statement — a snapshot row
/// that fails a predicate is skipped, not waited on — and settled claims the
/// commit then made real: rows `running` under a live worker that never
/// learned of them.
#[sqlx::test(migrations = "./migrations")]
async fn test_unacknowledged_claim_resolution_waits_for_the_claim_commit(pool: PgPool) {
    let db = TestDb::new(pool).await;
    let worker_id = uuid::Uuid::now_v7();
    db.queue.write_worker_info(worker_id, json!({}), None, Duration::from_secs(30)).await.unwrap();
    let job_id = db
        .queue
        .enqueue_raw(crate::with_config("unacked_ordered", |config| config.max_attempts = 2))
        .await
        .unwrap()
        .job_id();

    // The claim transaction, still in flight: the shipped claiming statement, so the resolution lock held here is
    // exactly the one a claim takes, and its claim is not yet visible.
    let mut claim = db.queue.pool().begin().await.unwrap();
    let claimed = claim_in(&mut claim, &db, worker_id).await;
    assert_eq!(claimed.len(), 1);
    let claimed = (claimed[0].id, claimed[0].attempts, claimed[0].started_at);
    assert_eq!(claimed.0, job_id);

    // The resolver for that claim, racing the commit: it must wait, not settle
    // on the pre-commit snapshot in which the row is still `queued`.
    let resolver_queue = db.queue.clone();
    let resolver = tokio::spawn(async move {
        pgqueue::__test_support::requeue_unacknowledged_claims(&resolver_queue, worker_id, &[claimed]).await
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!resolver.is_finished(), "resolution must be ordered behind the in-flight claim transaction");
    assert_eq!(
        db.queue.fetch_job(job_id).await.unwrap().unwrap().status,
        JobStatus::Queued,
        "nothing may be settled while the claim is unresolved"
    );

    claim.commit().await.unwrap();
    let requeued = tokio::time::timeout(Duration::from_secs(10), resolver).await.unwrap().unwrap().unwrap();
    assert_eq!(requeued, 1, "once the commit is visible the claim matches and is given back");
    let row = db.queue.fetch_job(job_id).await.unwrap().expect("requeued row is retained");
    assert_eq!(row.status, JobStatus::Queued);
    assert_eq!(row.attempts, 1);
    assert_eq!(row.max_attempts, 3, "the attempt that never ran is refunded");
}

/// `(id, attempts, worker_id)` does not name one claim. A claim whose COMMIT never landed leaves its row `queued` at
/// the old count, so the same worker's next claim of that row carries all three again — and a resolver matching on them
/// took that live attempt away from the processor running it, refunding an attempt that did run. The stamp each claim
/// writes into `started_at` is what tells the two apart.
#[sqlx::test(migrations = "./migrations")]
async fn test_unacknowledged_claim_resolution_leaves_a_later_claim_of_the_same_row_alone(pool: PgPool) {
    let db = TestDb::new(pool).await;
    let worker_id = uuid::Uuid::now_v7();
    db.queue.write_worker_info(worker_id, json!({}), None, Duration::from_secs(30)).await.unwrap();
    let job_id = db
        .queue
        .enqueue_raw(crate::with_config("unacked_reclaimed", |config| config.max_attempts = 2))
        .await
        .unwrap()
        .job_id();

    // The lost claim: the shipped claiming statement, whose COMMIT never landed.
    let mut lost = db.queue.pool().begin().await.unwrap();
    let lost_claim = claim_in(&mut lost, &db, worker_id).await;
    lost.rollback().await.unwrap();
    assert_eq!(lost_claim.len(), 1);
    let lost_claim = &lost_claim[0];

    // The same worker's next claim of the row, which is live.
    let live = pgqueue::__test_support::dequeue_worker(&db.queue, 1, worker_id).await.unwrap();
    assert_eq!(live.len(), 1);
    let live = &live[0];
    assert_eq!(
        (live.id, live.attempts, live.worker_id),
        (lost_claim.id, lost_claim.attempts, lost_claim.worker_id),
        "the two claims share every guard but their stamp"
    );
    assert_ne!(live.started_at, lost_claim.started_at);

    let resolved = pgqueue::__test_support::requeue_unacknowledged_claims(
        &db.queue,
        worker_id,
        &[(lost_claim.id, lost_claim.attempts, lost_claim.started_at)],
    )
    .await
    .unwrap();

    assert_eq!(resolved, 0, "a claim that never landed matches no row");
    let row = db.queue.fetch_job(job_id).await.unwrap().expect("row is retained");
    assert_eq!(row.status, JobStatus::Running, "the live claim was taken away or aborted");
    assert_eq!(row.worker_id, Some(worker_id));
    assert_eq!(row.started_at, live.started_at);
    assert_eq!(row.max_attempts, 2, "an attempt that is running was refunded");
    assert_eq!(row.error, None);
}

/// A claim takes its resolution lock shared and without waiting, so a resolver holding it — or queued for it behind a
/// claim whose session is stuck idle in transaction — makes a claim take nothing at once. Waiting for it instead parked
/// every later dequeue of the worker until its deadline, and each one abandoned there kept its pooled connection until
/// the lock was released.
#[sqlx::test(migrations = "./migrations")]
async fn test_a_claim_takes_nothing_rather_than_wait_for_its_resolution_lock(pool: PgPool) {
    let db = TestDb::new(pool).await;
    let worker_id = uuid::Uuid::now_v7();
    db.queue.write_worker_info(worker_id, json!({}), None, Duration::from_secs(30)).await.unwrap();
    let job_id = db.queue.enqueue_raw(new_job("unacked_resolving", |_| {})).await.unwrap().job_id();

    // A resolver pass for this worker, in flight.
    let mut resolver = db.queue.pool().begin().await.unwrap();
    sqlx::query("SELECT pg_advisory_xact_lock($1, hashtext($2))")
        .bind(pgqueue::__test_support::claim_resolution_lock_key(&db.database))
        .bind(worker_id.to_string())
        .execute(&mut *resolver)
        .await
        .unwrap();

    let claimed =
        tokio::time::timeout(Duration::from_secs(5), pgqueue::__test_support::dequeue_worker(&db.queue, 1, worker_id))
            .await
            .expect("the claim waited for the resolution lock")
            .unwrap();
    assert!(claimed.is_empty(), "a claim ran while its worker's claims were being resolved");
    assert_eq!(db.queue.fetch_job(job_id).await.unwrap().unwrap().status, JobStatus::Queued);

    // Other workers are not held up at all, and this one claims again as soon as the resolution ends.
    let other = uuid::Uuid::now_v7();
    db.queue.write_worker_info(other, json!({}), None, Duration::from_secs(30)).await.unwrap();
    let second = db.queue.enqueue_raw(new_job("unacked_resolving", |_| {})).await.unwrap().job_id();
    let claimed = pgqueue::__test_support::dequeue_worker(&db.queue, 1, other).await.unwrap();
    assert_eq!(claimed.iter().map(|job| job.id).collect::<Vec<_>>(), [job_id]);
    resolver.rollback().await.unwrap();
    let claimed = pgqueue::__test_support::dequeue_worker(&db.queue, 1, worker_id).await.unwrap();
    assert_eq!(claimed.iter().map(|job| job.id).collect::<Vec<_>>(), [second]);
}

/// A resolver ordered behind a claim whose session is stuck idle in transaction — its COMMIT lost on a connection
/// that died on one side only — must leave the worker claiming. PostgreSQL queues a *waiting* exclusive request ahead
/// of every later shared one, so a resolver that blocked on the lock without bound had each later claim of its worker
/// refused: the worker took nothing, for as long as the server took to reap the stuck session, while it kept
/// heartbeating. Its wait is bounded now, so a claim landing inside one is still refused — by design, which is what
/// gets a resolver past overlapping claims — and the worker claims again as soon as the wait gives up.
#[sqlx::test(migrations = "./migrations")]
async fn test_a_resolver_behind_a_stuck_claim_leaves_its_worker_claiming(pool: PgPool) {
    let db = TestDb::new(pool).await;
    let worker_id = uuid::Uuid::now_v7();
    db.queue.write_worker_info(worker_id, json!({}), None, Duration::from_secs(30)).await.unwrap();
    let stuck_id = db
        .queue
        .enqueue_raw(crate::with_config("unacked_stuck", |config| config.max_attempts = 2))
        .await
        .unwrap()
        .job_id();

    // The stuck claim: the shipped claiming statement, its transaction left open.
    let mut stuck = db.queue.pool().begin().await.unwrap();
    let held = claim_in(&mut stuck, &db, worker_id).await;
    assert_eq!(held.iter().map(|job| job.id).collect::<Vec<_>>(), [stuck_id]);
    pgqueue::__test_support::drop_armed_dequeue_claim_guard(
        &db.queue,
        worker_id,
        &[(held[0].id, held[0].attempts, held[0].started_at)],
    );
    // Long enough for the detached resolver to have tried the lock several times.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let next = db.queue.enqueue_raw(new_job("unacked_stuck", |_| {})).await.unwrap().job_id();
    // Each dequeue is bounded on its own, so one that waited — rather than being refused — still fails here.
    let claimed = crate::wait_for_some(
        Duration::from_secs(10),
        Duration::from_millis(20),
        "the worker stopped claiming while its resolver waited on a stuck claim",
        || async {
            let claimed = tokio::time::timeout(
                Duration::from_secs(5),
                pgqueue::__test_support::dequeue_worker(&db.queue, 1, worker_id),
            )
            .await
            .expect("the claim waited behind the resolver")
            .unwrap();
            (!claimed.is_empty()).then_some(claimed)
        },
    )
    .await;
    assert_eq!(claimed.iter().map(|job| job.id).collect::<Vec<_>>(), [next]);

    // Once the stuck claim resolves, the resolver settles it: committed, so given back with the attempt refunded.
    stuck.commit().await.unwrap();
    crate::wait_until(
        Duration::from_secs(15),
        Duration::from_millis(20),
        "the resolver never settled the claim once it committed",
        || async { db.queue.fetch_job(stuck_id).await.unwrap().is_some_and(|row| row.status == JobStatus::Queued) },
    )
    .await;
    let row = db.queue.fetch_job(stuck_id).await.unwrap().expect("requeued row is retained");
    assert_eq!(row.max_attempts, 3, "the attempt that never ran is refunded");
}

/// A committed claim's row can be locked independently of its claim transaction. Waiting for that row while holding
/// the exclusive resolution lock must not stop the worker from claiming unrelated jobs.
#[sqlx::test(migrations = "./migrations")]
async fn test_a_resolver_behind_a_locked_row_leaves_its_worker_claiming(pool: PgPool) {
    let db = TestDb::new(pool).await;
    let worker_id = uuid::Uuid::now_v7();
    db.queue.write_worker_info(worker_id, json!({}), None, Duration::from_secs(30)).await.unwrap();
    let stuck_id = db
        .queue
        .enqueue_raw(crate::with_config("unacked_locked_row", |config| config.max_attempts = 2))
        .await
        .unwrap()
        .job_id();
    let held = pgqueue::__test_support::dequeue_worker(&db.queue, 1, worker_id).await.unwrap();
    assert_eq!(held.iter().map(|job| job.id).collect::<Vec<_>>(), [stuck_id]);

    let mut row_lock = db.queue.pool().begin().await.unwrap();
    sqlx::query("SELECT id FROM pgqueue.jobs WHERE id = $1 FOR UPDATE")
        .bind(stuck_id)
        .execute(&mut *row_lock)
        .await
        .unwrap();
    pgqueue::__test_support::drop_armed_dequeue_claim_guard(
        &db.queue,
        worker_id,
        &[(held[0].id, held[0].attempts, held[0].started_at)],
    );
    crate::wait_until(
        Duration::from_secs(5),
        Duration::from_millis(10),
        "the resolver never took its exclusive resolution lock",
        || async {
            !sqlx::query_scalar::<_, bool>("SELECT pg_try_advisory_xact_lock_shared($1, hashtext($2))")
                .bind(pgqueue::__test_support::claim_resolution_lock_key(&db.database))
                .bind(worker_id.to_string())
                .fetch_one(db.queue.pool())
                .await
                .unwrap()
        },
    )
    .await;

    let next = db.queue.enqueue_raw(new_job("unacked_locked_row", |_| {})).await.unwrap().job_id();
    let claimed = crate::wait_for_some(
        Duration::from_secs(5),
        Duration::from_millis(20),
        "the worker stopped claiming while its resolver waited on a locked row",
        || async {
            let claimed = pgqueue::__test_support::dequeue_worker(&db.queue, 1, worker_id).await.unwrap();
            (!claimed.is_empty()).then_some(claimed)
        },
    )
    .await;
    assert_eq!(claimed.iter().map(|job| job.id).collect::<Vec<_>>(), [next]);
    assert_eq!(db.queue.fetch_job(stuck_id).await.unwrap().unwrap().status, JobStatus::Running);

    row_lock.rollback().await.unwrap();
    crate::wait_until(
        Duration::from_secs(15),
        Duration::from_millis(20),
        "the resolver never settled the claim once its row lock was released",
        || async { db.queue.fetch_job(stuck_id).await.unwrap().is_some_and(|row| row.status == JobStatus::Queued) },
    )
    .await;
    let row = db.queue.fetch_job(stuck_id).await.unwrap().unwrap();
    assert_eq!(row.max_attempts, 3, "the attempt that never ran is refunded once");
    assert_eq!(row.refunds, 1);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_a_resolver_row_lock_timeout_rolls_back_the_whole_batch(pool: PgPool) {
    let db = TestDb::new(pool).await;
    let worker_id = uuid::Uuid::now_v7();
    db.queue.write_worker_info(worker_id, json!({}), None, Duration::from_secs(30)).await.unwrap();
    for _ in 0..2 {
        db.queue
            .enqueue_raw(crate::with_config("unacked_batch_rollback", |config| config.max_attempts = 2))
            .await
            .unwrap();
    }
    let held = pgqueue::__test_support::dequeue_worker(&db.queue, 2, worker_id).await.unwrap();
    assert_eq!(held.len(), 2);
    let claims = held.iter().map(|row| (row.id, row.attempts, row.started_at)).collect::<Vec<_>>();
    let mut row_lock = db.queue.pool().begin().await.unwrap();
    sqlx::query("SELECT id FROM pgqueue.jobs WHERE id = $1 FOR UPDATE")
        .bind(held[1].id)
        .execute(&mut *row_lock)
        .await
        .unwrap();

    // The first refund is made before the pass reaches the locked second row, and must roll back with that refusal.
    let error = tokio::time::timeout(
        Duration::from_secs(5),
        pgqueue::__test_support::requeue_unacknowledged_claims(&db.queue, worker_id, &claims),
    )
    .await
    .expect("the resolver waited indefinitely on a locked row")
    .unwrap_err();
    assert!(matches!(error, Error::Db(sqlx::Error::Database(error)) if error.code().as_deref() == Some("55P03")));
    for claim in &held {
        let row = db.queue.fetch_job(claim.id).await.unwrap().unwrap();
        assert_eq!(row.status, JobStatus::Running);
        assert_eq!(row.max_attempts, 2, "a failed pass granted a partial refund");
        assert_eq!(row.refunds, 0);
    }
    assert!(
        sqlx::query_scalar::<_, bool>("SELECT pg_try_advisory_xact_lock_shared($1, hashtext($2))")
            .bind(pgqueue::__test_support::claim_resolution_lock_key(&db.database))
            .bind(worker_id.to_string())
            .fetch_one(db.queue.pool())
            .await
            .unwrap(),
        "a failed pass kept the worker's exclusive resolution lock"
    );

    row_lock.rollback().await.unwrap();
    assert_eq!(pgqueue::__test_support::requeue_unacknowledged_claims(&db.queue, worker_id, &claims).await.unwrap(), 2);
    for claim in &held {
        let row = db.queue.fetch_job(claim.id).await.unwrap().unwrap();
        assert_eq!(row.status, JobStatus::Queued);
        assert_eq!(row.max_attempts, 3, "a retry must refund each claim exactly once");
        assert_eq!(row.refunds, 1);
    }
}

/// The other half of the shared lock: claims need ordering only against a resolver, so two claims of one worker in
/// flight together both take work, and `SKIP LOCKED` keeps them off each other's rows. An exclusive lock refused the
/// second claim, or made it wait for the first to commit.
#[sqlx::test(migrations = "./migrations")]
async fn test_concurrent_claims_of_one_worker_both_take_work(pool: PgPool) {
    let db = TestDb::new(pool).await;
    let worker_id = uuid::Uuid::now_v7();
    db.queue.write_worker_info(worker_id, json!({}), None, Duration::from_secs(30)).await.unwrap();
    let first = db.queue.enqueue_raw(new_job("unacked_resolving", |_| {})).await.unwrap().job_id();
    let second = db.queue.enqueue_raw(new_job("unacked_resolving", |_| {})).await.unwrap().job_id();

    // A claim of this worker whose COMMIT is still in flight.
    let mut in_flight = db.queue.pool().begin().await.unwrap();
    let held = claim_in(&mut in_flight, &db, worker_id).await;
    assert_eq!(held.iter().map(|job| job.id).collect::<Vec<_>>(), [first]);

    let claimed =
        tokio::time::timeout(Duration::from_secs(5), pgqueue::__test_support::dequeue_worker(&db.queue, 1, worker_id))
            .await
            .expect("the claim waited for its own worker's claim to commit")
            .unwrap();
    assert_eq!(
        claimed.iter().map(|job| job.id).collect::<Vec<_>>(),
        [second],
        "the claim was refused while its own worker's claim was in flight"
    );
    in_flight.rollback().await.unwrap();
}

/// What the shared lock costs a resolver that only ever *tried* its exclusive side: nothing gives a try priority, so
/// it got the lock only at an instant when no claim of its worker was in flight — and a custom consumer shared between
/// tasks against a backlog keeps one in flight at almost every instant. Refused indefinitely, it left the claim it was
/// resolving `running` under a live lease nothing else recovers, holding its dedupe key, hanging its waiters. Here two
/// transactions hand the claim side of the lock to each other, so it is never free; the resolver has to queue for it,
/// which refuses the next claim's try and lets the ones in flight drain.
#[sqlx::test(migrations = "./migrations")]
async fn test_a_resolver_is_not_starved_by_overlapping_claims_of_its_worker(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let worker_id = uuid::Uuid::now_v7();
    db.queue.write_worker_info(worker_id, json!({}), None, Duration::from_secs(30)).await.unwrap();
    let job_id = db
        .queue
        .enqueue_raw(crate::with_config("unacked_overlapped", |config| config.max_attempts = 2))
        .await
        .unwrap()
        .job_id();

    // The claim whose acknowledgement was lost: the shipped claiming statement, committed.
    let mut lost = db.queue.pool().begin().await.unwrap();
    let claimed = claim_in(&mut lost, &db, worker_id).await;
    lost.commit().await.unwrap();
    assert_eq!(claimed.iter().map(|job| job.id).collect::<Vec<_>>(), [job_id]);

    // The worker's other claims, back to back: each takes the claim side of the lock — tried, as a claim does, so
    // refused once a resolver is queued — before the one before it lets go. Its own pool, so the fixture's stays free
    // for the resolver.
    let stop = tokio_util::sync::CancellationToken::new();
    let (holding_tx, holding_rx) = tokio::sync::oneshot::channel();
    let relay = tokio::spawn({
        let relay_pool = crate::pool_with_max(&pool, 2).await;
        let key = pgqueue::__test_support::claim_resolution_lock_key(&db.database);
        let stop = stop.clone();
        async move {
            let mut holding = relay_pool.begin().await.unwrap();
            let held = try_claim_side_of_resolution_lock(&mut holding, key, worker_id).await;
            holding_tx.send(held).unwrap();
            while !stop.is_cancelled() {
                let mut next = relay_pool.begin().await.unwrap();
                try_claim_side_of_resolution_lock(&mut next, key, worker_id).await;
                holding.commit().await.unwrap();
                holding = next;
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            holding.commit().await.unwrap();
        }
    });
    assert!(holding_rx.await.unwrap(), "nothing else holds the lock yet, so the first claim must take it");

    // Only now, so the resolver's very first try already finds a claim in flight.
    pgqueue::__test_support::drop_armed_dequeue_claim_guard(
        &db.queue,
        worker_id,
        &[(claimed[0].id, claimed[0].attempts, claimed[0].started_at)],
    );
    crate::wait_until(
        Duration::from_secs(15),
        Duration::from_millis(20),
        "the resolver was starved by the overlapping claims of its own worker",
        || async { db.queue.fetch_job(job_id).await.unwrap().is_some_and(|row| row.status == JobStatus::Queued) },
    )
    .await;
    stop.cancel();
    relay.await.unwrap();
    let row = db.queue.fetch_job(job_id).await.unwrap().expect("requeued row is retained");
    assert_eq!(row.max_attempts, 3, "the attempt that never ran is refunded");
    assert_eq!(row.worker_id, None);
}

/// Takes the claim side of `worker_id`'s resolution lock in `transaction` the way the claim does — shared, tried, and
/// refused without waiting while a resolver holds or awaits it — and reports whether it got it.
async fn try_claim_side_of_resolution_lock(
    transaction: &mut sqlx::PgTransaction<'_>,
    key: i32,
    worker_id: uuid::Uuid,
) -> bool {
    sqlx::query_scalar::<_, bool>("SELECT pg_try_advisory_xact_lock_shared($1, hashtext($2))")
        .bind(key)
        .bind(worker_id.to_string())
        .fetch_one(&mut **transaction)
        .await
        .unwrap()
}

/// Runs the shipped dequeue claim for `worker_id` inside `transaction`, which the caller keeps open — a claim whose
/// COMMIT is still in flight — or rolls back, for one whose COMMIT never landed.
async fn claim_in(transaction: &mut sqlx::PgTransaction<'_>, db: &TestDb, worker_id: uuid::Uuid) -> Vec<JobRow> {
    sqlx::query_as::<_, JobRow>(pgqueue::__test_support::dequeue_claim_sql())
        .bind(db.queue.name())
        .bind(i16::MIN)
        .bind(i16::MAX)
        .bind(1_i64)
        .bind(worker_id)
        .bind(true)
        .bind(pgqueue::__test_support::claim_resolution_lock_key(&db.database))
        .fetch_all(&mut **transaction)
        .await
        .unwrap()
}

// ---------------------------------------------------------------------------
// Sessions whose client is gone
// ---------------------------------------------------------------------------

#[pgqueue::job(name = "idle_bound_batched")]
async fn idle_bound_batched(_: u32) -> anyhow::Result<()> {
    Ok(())
}

/// Asserts that every write recorded since the last read ran under the queue's own idle bound, and that there was one.
async fn assert_idle_bounded(pool: &PgPool, what: &str) {
    let seen = crate::idle_bounds_seen(pool).await;
    assert!(!seen.is_empty(), "{what} wrote nothing");
    assert!(
        seen.iter().all(|(_, setting)| setting == crate::QUEUE_TRANSACTION_IDLE_BOUND),
        "{what} ran in a transaction the server would keep idle for as long as TCP takes to notice a dead client: \
         {seen:?}"
    );
}

/// A transaction's locks last as long as the server keeps its session, and a client whose host lost power, or that a
/// partition cut off, closes nothing: the server learns of it from TCP, after two hours and eleven minutes of
/// keepalive on stock settings. A worker lost between its claim and the claim's COMMIT left the claimed rows locked and
/// `queued` all that time — skipped by every other claim, invisible to the sweeper, still holding their dedupe keys — a
/// resolver lost that way refused every claim of its live worker, and a keyed enqueue held its key against every
/// other. So every transaction the queue owns tells the server, from its first statement, to end the session once it
/// sits idle for half a minute. A caller's own transaction is left as its session has it, and the session's own
/// setting is back once each of the queue's transactions ends.
#[sqlx::test(migrations = "./migrations")]
async fn test_the_queues_own_transactions_bound_how_long_they_may_sit_idle(pool: PgPool) {
    crate::record_idle_bounds(&pool).await;
    // One connection, so the session read back at the end is the one every transaction ran on.
    let db = TestDb::new(crate::pool_with_max(&pool, 1).await).await;
    let worker_id = uuid::Uuid::now_v7();
    db.queue.write_worker_info(worker_id, json!({}), None, Duration::from_secs(30)).await.unwrap();
    let keyless = db.queue.enqueue_raw(new_job("idle_bound", |_| {})).await.unwrap().job_id();
    let seen = crate::idle_bounds_seen(&pool).await;
    assert!(
        seen.iter().all(|(_, setting)| setting == "0"),
        "a single statement is no transaction of the queue's own: {seen:?}"
    );

    db.queue.enqueue_raw(new_job("idle_bound", |job| job.dedupe_key = Some("idle:single".into()))).await.unwrap();
    assert_idle_bounded(&pool, "a keyed enqueue").await;
    db.queue.enqueue_batch([idle_bound_batched::job(1).dedupe_key("idle:batch")]).await.unwrap();
    assert_idle_bounded(&pool, "a keyed batch").await;

    let claimed = pgqueue::__test_support::dequeue_worker(&db.queue, 1, worker_id).await.unwrap();
    assert_eq!(claimed.iter().map(|job| job.id).collect::<Vec<_>>(), [keyless]);
    assert_idle_bounded(&pool, "a dequeue claim").await;
    let claims = [(claimed[0].id, claimed[0].attempts, claimed[0].started_at)];
    assert_eq!(pgqueue::__test_support::requeue_unacknowledged_claims(&db.queue, worker_id, &claims).await.unwrap(), 1);
    assert_idle_bounded(&pool, "an unacknowledged-claim resolution").await;
    pgqueue::__test_support::write_worker_lease_within(
        &db.queue,
        worker_id,
        Duration::from_secs(30),
        Duration::from_secs(10),
    )
    .await
    .unwrap();
    assert_idle_bounded(&pool, "a lease write held to a deadline").await;

    assert!(db.queue.abort_job(keyless, "terminal").await.unwrap());
    crate::idle_bounds_seen(&pool).await;
    assert!(db.queue.retry_job(keyless, "again").await.unwrap());
    assert_idle_bounded(&pool, "a manual retry").await;

    let consumer = crate::leased_consumer(&db.queue, uuid::Uuid::now_v7()).await;
    let attempt = consumer.dequeue(1).await.unwrap().into_iter().next().expect("a queued job to claim");
    let dropped = attempt.job().id;
    crate::idle_bounds_seen(&pool).await;
    drop(attempt);
    crate::wait_until(
        Duration::from_secs(10),
        Duration::from_millis(10),
        "the dropped attempt was not recovered",
        || async { db.queue.fetch_job(dropped).await.unwrap().is_some_and(|row| row.status != JobStatus::Running) },
    )
    .await;
    assert_idle_bounded(&pool, "a dropped attempt's recovery").await;

    let mut caller = db.queue.pool().begin().await.unwrap();
    db.queue
        .enqueue_raw_in(&mut caller, new_job("idle_bound", |job| job.dedupe_key = Some("idle:caller".into())))
        .await
        .unwrap();
    caller.commit().await.unwrap();
    assert_eq!(
        crate::idle_bounds_seen(&pool).await,
        [("INSERT jobs".to_owned(), "0".to_owned())],
        "a caller's transaction must be left as its session has it"
    );
    let session = sqlx::query_scalar::<_, String>("SHOW idle_in_transaction_session_timeout")
        .fetch_one(db.queue.pool())
        .await
        .unwrap();
    assert_eq!(session, "0", "the session's own setting must be back once the queue's transactions end");
}

/// The bound only ever tightens what the session already has: an operator's stricter setting stays in force inside
/// the queue's transactions, and a looser one is tightened only for as long as each of them lasts.
#[sqlx::test(migrations = "./migrations")]
async fn test_the_queues_idle_bound_only_tightens_the_sessions_own(pool: PgPool) {
    crate::record_idle_bounds(&pool).await;
    for (session, within) in [
        ("20s", "20s"),
        ("1min", crate::QUEUE_TRANSACTION_IDLE_BOUND),
    ] {
        let options =
            pool.connect_options().as_ref().clone().options([("idle_in_transaction_session_timeout", session)]);
        let db =
            TestDb::new(sqlx::postgres::PgPoolOptions::new().max_connections(1).connect_with(options).await.unwrap())
                .await;
        db.queue
            .enqueue_raw(new_job("idle_bound", |job| job.dedupe_key = Some(format!("idle:{session}"))))
            .await
            .unwrap();
        assert_eq!(
            crate::idle_bounds_seen(&pool).await,
            [("INSERT jobs".to_owned(), within.to_owned())],
            "a session set to {session}"
        );
        let after = sqlx::query_scalar::<_, String>("SHOW idle_in_transaction_session_timeout")
            .fetch_one(db.queue.pool())
            .await
            .unwrap();
        assert_eq!(after, session, "the session's own setting must be back once the queue's transaction ends");
    }
}

// ---------------------------------------------------------------------------
// Numeric fidelity across the `jsonb` round trip
// ---------------------------------------------------------------------------

/// `jsonb` stores every number as `numeric` and prints it back as a full decimal
/// expansion, so a float's text form on the way out is not the one `serde_json`
/// wrote on the way in. Without `serde_json`'s `float_roundtrip` feature its
/// parser is only best-effort over that expansion, which cost two things: about
/// a quarter of all `f64` values came back a ULP different — silent corruption
/// of job input and output, the very failure `encode_json`'s `RejectNonFinite`
/// pass exists to prevent — and `f64::MAX` came back as *nothing at all*.
///
/// `f64::MAX` prints as a 309-digit integer, which the fast parser reconstructs
/// as `17976931348623157.0 * 1e292` and rounds to infinity: `number out of
/// range`. That is not confined to its own row. The dequeue decodes its batch
/// inside the claiming transaction, so the decode failure rolls the claim back,
/// the next claim re-selects the same row, and **no job in the queue runs
/// again** — reachable from a plain `queue.enqueue(job(f64::MAX))`, with no
/// foreign SQL writer involved.
#[sqlx::test(migrations = "./migrations")]
async fn test_float_payloads_and_results_round_trip_bit_for_bit(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    // The sentinels, a subnormal, and two ordinary application-scale decimals
    // that the fast parser got wrong.
    let probes: [f64; 8] = [
        f64::MAX,
        f64::MIN,
        f64::MIN_POSITIVE,
        5e-324,
        -1.5e-300,
        -102_187.004_377_093_63,
        0.1,
        1e308,
    ];

    for (index, value) in probes.into_iter().enumerate() {
        let payload = json!({ "v": value });
        // PostgreSQL expands exponent notation. With arbitrary_precision, Number equality compares that spelling;
        // compare the decoded bits to check the actual floating-point round trip under either feature configuration.
        let assert_same_bits = |document: &Value, stage: &str| {
            assert_eq!(document["v"].as_f64().unwrap().to_bits(), value.to_bits(), "{value:e} changed {stage}");
        };
        let id = db
            .queue
            .enqueue_raw(new_job("float", |job| {
                job.payload = payload.clone();
                job.meta = payload.clone();
            }))
            .await
            .unwrap_or_else(|error| panic!("enqueue {value:e} ({index}): {error}"))
            .unwrap();

        // The claim is the path that matters: it decodes inside its own
        // transaction, so a value it cannot read takes the whole batch with it.
        let claimed = db
            .queue
            .dequeue(1, uuid::Uuid::now_v7())
            .await
            .unwrap_or_else(|error| panic!("dequeue after enqueueing {value:e}: {error}"));
        assert_eq!(claimed.len(), 1, "{value:e} was not claimable");
        assert_same_bits(&claimed[0].payload, "between enqueue and claim");

        db.queue.finish(&claimed[0], JobStatus::Complete, Some(payload.clone()), None).await.unwrap();
        let row = db.queue.fetch_job(id).await.unwrap().expect("row");
        assert_same_bits(&row.payload, "in storage");
        assert_same_bits(&row.meta, "in meta");
        assert_same_bits(row.result.as_ref().unwrap(), "as a result");
    }
}

/// One unreadable value must not be able to stop the queue: with the round trip
/// exact, a queue holding `f64::MAX` keeps claiming everything behind it.
#[sqlx::test(migrations = "./migrations")]
async fn test_a_sentinel_float_does_not_wedge_the_claim(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    db.queue.enqueue_raw(new_job("sentinel", |job| job.payload = json!({ "v": f64::MAX }))).await.unwrap().unwrap();
    db.queue.enqueue_raw(new_job("healthy", |job| job.payload = json!({ "v": 1 }))).await.unwrap().unwrap();

    let claimed = db.queue.dequeue(10, uuid::Uuid::now_v7()).await.expect("the claim must not fail to decode");
    assert_eq!(claimed.len(), 2, "the sentinel row blocked the job queued behind it");
}

#[pgqueue::job]
async fn numeric_payload(_: Value) {}

#[sqlx::test(migrations = "./migrations")]
async fn test_unrepresentable_json_numbers_preserve_enqueue_transactions(pool: PgPool) {
    let db = TestDb::new(pool).await;
    for number in ["1e200000", "1e-20000"] {
        let value: Value = serde_json::from_str(number).unwrap();
        for field in ["job payload", "job meta"] {
            let mut tx = db.queue.pool().begin().await.unwrap();
            let request = if field == "job payload" {
                numeric_payload::job(json!({"nested": [value.clone()]}))
            } else {
                numeric_payload::job(Value::Null).meta(json!({"nested": [value.clone()]}))
            };
            let error = db.queue.enqueue_in(&mut tx, request).await.unwrap_err();
            assert!(matches!(&error, Error::Config(message) if message.contains(field)), "{error}");
            // Refusal must happen before PostgreSQL aborts the application's transaction.
            db.queue.enqueue_in(&mut tx, numeric_payload::job(json!(42))).await.unwrap();
            tx.commit().await.unwrap();
        }

        let mut tx = db.queue.pool().begin().await.unwrap();
        let error = db
            .queue
            .enqueue_batch_in(
                &mut tx,
                [
                    numeric_payload::job(json!(42)),
                    numeric_payload::job(value.clone()),
                ],
            )
            .await
            .unwrap_err();
        assert!(matches!(error, Error::Config(_)), "{error}");
        assert_eq!(sqlx::query_scalar::<_, i32>("SELECT 1").fetch_one(&mut *tx).await.unwrap(), 1);
        tx.rollback().await.unwrap();

        let error = pgqueue::Worker::builder(db.queue.clone())
            .schedule_cron("* * * * *", numeric_payload::job(value))
            .build()
            .unwrap_err();
        assert!(matches!(error, Error::Config(_)), "{error}");
    }
    assert_eq!(db.queue.counts().await.unwrap().queued, 4);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_unrepresentable_json_numbers_are_refused_by_consumers(pool: PgPool) {
    let db = TestDb::new(pool).await;
    let consumer = crate::leased_consumer(&db.queue, uuid::Uuid::now_v7()).await;
    db.queue.enqueue(numeric_payload::job(Value::Null)).await.unwrap();
    let attempt = consumer.dequeue(1).await.unwrap().pop().unwrap();
    let value: Value = serde_json::from_str("1e-20000").unwrap();
    for (stats, metadata, field) in [
        (value.clone(), None, "worker stats"),
        (json!({}), Some(value.clone()), "worker metadata"),
    ] {
        let error = consumer.heartbeat(stats, metadata, Duration::from_secs(30)).await.unwrap_err();
        assert!(matches!(&error, Error::Config(message) if message.contains(field)), "{error}");
    }
    let error = attempt.finish(JobStatus::Complete, Some(value), None).await.unwrap_err();
    assert!(matches!(&error, Error::Config(message) if message.contains("job result")), "{error}");
    assert!(attempt.finish(JobStatus::Complete, Some(json!(42)), None).await.unwrap());
}

/// A number is written only if PostgreSQL's `numeric` holds it *and* the decimal
/// PostgreSQL prints back stays within `f64`'s range. The second bound is for
/// mixed builds: `arbitrary_precision` is per build, serde_json without it
/// refuses a number past `f64::MAX`, and the dequeue claim decodes its rows
/// before it commits — so one such number from a producer that has the feature
/// stopped a worker that does not on the same row for ever, and every job
/// queued behind it with it.
///
/// PostgreSQL's own answer is `pg_input_is_valid`. The `f64` answer is the
/// printed decimal parsed the way a reader without the feature parses it, which
/// checks the validator's spelling-based cut-off against what is actually read.
#[sqlx::test(migrations = "./migrations")]
async fn test_json_numeric_validation_matches_postgres_and_f64_boundaries(pool: PgPool) {
    let db = TestDb::new(pool).await;
    let numbers = [
        // `f64::MAX`, a spelling that still rounds down to it, and the first
        // that rounds up to infinity instead.
        "1.7976931348623157e308".to_string(),
        "-1.7976931348623158e308".into(),
        "1.7976931348623159e308".into(),
        "1e308".into(),
        "-1e309".into(),
        "1e400".into(),
        "-1e131071".into(),
        "1e131072".into(),
        "1e-16383".into(),
        "1e-16384".into(),
        "1.0e-16383".into(),
        "0e131072".into(),
        "0e-16384".into(),
        "0e1073741823".into(),
        "0e1073741824".into(),
        "0e99999999999999999999".into(),
        "0.0001e131075".into(),
        "0.0001e131076".into(),
        "9".repeat(131_072),
        "9".repeat(131_073),
        format!("0.{}", "0".repeat(16_383)),
        format!("0.{}", "0".repeat(16_384)),
    ];
    let mut refused_for_f64 = 0;
    for number in numbers {
        let postgres = sqlx::query_scalar::<_, bool>("SELECT pg_input_is_valid($1, 'jsonb')")
            .bind(&number)
            .fetch_one(db.queue.pool())
            .await
            .unwrap();
        let readable = postgres
            && sqlx::query_scalar::<_, String>("SELECT $1::jsonb::text")
                .bind(&number)
                .fetch_one(db.queue.pool())
                .await
                .unwrap()
                .parse::<f64>()
                .is_ok_and(f64::is_finite);
        let value: Value = serde_json::from_str(&number).unwrap();
        let result = db.queue.enqueue(numeric_payload::job(value)).await;
        let label = format!("number of {} bytes ({}…)", number.len(), &number[..number.len().min(24)]);
        if readable {
            let handle = result.unwrap_or_else(|error| panic!("{label}: {error}")).into_job_handle();
            assert!(handle.fetch_job().await.is_ok(), "{label}");
        } else if postgres {
            refused_for_f64 += 1;
            assert!(
                matches!(&result, Err(Error::Config(message)) if message.contains("past f64's range")),
                "{label}: {result:?}"
            );
        } else {
            assert!(
                matches!(&result, Err(Error::Config(message)) if message.contains("outside PostgreSQL's numeric range")),
                "{label}: {result:?}"
            );
        }
    }
    // The new bound is exercised, not just tolerated: PostgreSQL accepts every one of these, `f64` none.
    assert_eq!(refused_for_f64, 6, "1.7976931348623159e308, -1e309, 1e400, -1e131071, 0.0001e131075, 9×131,072");
}

/// `jsonb` prints a number back as its full decimal expansion, so the eight
/// bytes of `1e-16383` are read back as 16,385. The `f64` bound does not catch
/// it — a build without `arbitrary_precision` reads it as zero — but measured
/// by spelling, 70,000 of them are a 630 KB document that passes the size limit
/// and is stored in 1.1 MB, and then no read can print the row:
/// `string buffer exceeds maximum allowed length (1073741823 bytes)`, `54000`.
/// The dequeue claim is such a read, and with the row at the head of the queue,
/// so was every claim after it.
///
/// Each spelling below is placed in a document that reaches the limit exactly,
/// as whichever of its spelling and its printed form is longer: that document
/// is accepted and reads back no larger than the limit, and one byte more is
/// refused.
#[sqlx::test(migrations = "./migrations")]
async fn test_json_number_expansion_counts_toward_the_document_limit(pool: PgPool) {
    let db = TestDb::new(pool).await;
    let max = pgqueue::__test_support::max_json_document_bytes();

    let wedge: Value = serde_json::from_str(&format!("[{}1]", "1e-16383,".repeat(70_000))).unwrap();
    let error = db.queue.enqueue(numeric_payload::job(wedge)).await.unwrap_err();
    assert!(
        matches!(&error, Error::Config(message) if message.contains("job payload") && message.contains("expands its numbers")),
        "{error}"
    );

    for spelling in [
        "1e308",
        "-1.7976931348623157e308",
        "0.0001e312",
        "1e-16383",
        "-1e-16383",
        "-0.0e-5",
        "12.5e-3",
        "-2.5E-10",
        "1.500e2",
        "0e5",
    ] {
        let number: Value = serde_json::from_str(spelling).unwrap();
        let spelled = serde_json::to_string(&number).unwrap().len();
        let printed = sqlx::query_scalar::<_, String>("SELECT $1::jsonb::text")
            .bind(spelling)
            .fetch_one(db.queue.pool())
            .await
            .unwrap()
            .len();
        let document = |pad: usize| json!([number.clone(), "x".repeat(pad)]);
        let pad = max - spelled.max(printed) - r#"[,""]"#.len();

        let handle = db
            .queue
            .enqueue(numeric_payload::job(document(pad)))
            .await
            .unwrap_or_else(|error| panic!("{spelling} at the limit: {error}"))
            .into_job_handle();
        let stored = handle.fetch_job().await.unwrap().payload;
        assert_eq!(serde_json::to_string(&stored).unwrap().len(), max - spelled.max(printed) + printed, "{spelling}");

        for request in [
            numeric_payload::job(document(pad + 1)),
            numeric_payload::job(json!(1)).meta(document(pad + 1)),
        ] {
            let error = db.queue.enqueue(request).await.unwrap_err();
            assert!(
                matches!(&error, Error::Config(message) if message.contains("must not exceed")),
                "{spelling}: {error}"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// The clocks recovery reads
// ---------------------------------------------------------------------------

/// `pgqueue.job_is_stuck` reads `started_at` on its timeout trigger and
/// `COALESCE(touched_at, started_at)` on its liveness one, so an active row
/// carrying neither answers the whole predicate NULL — never TRUE — and the
/// sweeper's scan can only select rows it answers TRUE for. Such a row is
/// unrecoverable: it stays `running` for ever and its dedupe key silently
/// deduplicates every later enqueue, with nothing on health to say so.
///
/// `started_at` has no default, so a foreign SQL writer that names only the
/// columns it knows about produces exactly that shape. The schema refuses it,
/// as it refuses every other value such a writer could poison a queue through.
#[sqlx::test(migrations = "./migrations")]
async fn test_an_active_row_must_carry_the_clock_recovery_reads(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    for status in ["running", "aborting"] {
        let error = sqlx::query(
            "INSERT INTO pgqueue.jobs (queue, name, status, attempts, max_attempts, worker_id)
             VALUES ($1, 'stranded', $2, 1, 3, gen_random_uuid())",
        )
        .bind(db.queue.name())
        .bind(status)
        .execute(&pool)
        .await
        .expect_err("an active row with no started_at must be refused");
        let database_error = error.as_database_error().expect("a constraint violation");
        assert_eq!(database_error.code().as_deref(), Some("23514"), "{error}");
        assert_eq!(
            database_error.constraint(),
            Some("jobs_active_started_at_check"),
            "refused by the wrong constraint: {error}"
        );
    }

    // The same row *with* the clock is accepted, and recovery reaches it — so
    // the check refuses only the shape that cannot be recovered.
    sqlx::query(
        "INSERT INTO pgqueue.jobs
             (queue, name, dedupe_key, status, attempts, max_attempts, worker_id, started_at, touched_at)
         VALUES ($1, 'stranded', 'stranded-key', 'running', 1, 3, gen_random_uuid(),
                 now() - interval '1 hour', now() - interval '1 hour')",
    )
    .bind(db.queue.name())
    .execute(&pool)
    .await
    .expect("an active row carrying its clocks is accepted");

    let mut sweeper = db.queue.sweeper();
    let marked = sweeper.sweep().await.expect("sweep");
    assert_eq!(marked.cancelling.len(), 1, "the recoverable row must be marked: {marked:?}");
}

/// The ceiling is enforced on the *serialized* document — structure, object
/// keys and escapes count as much as string content — so this test measures
/// the exact boundary rather than restating it. Why a too-large document must
/// be refused on this side of the wire is documented on
/// `MAX_JSON_DOCUMENT_BYTES`; the test below proves every call site actually
/// runs the check.
#[test]
fn test_oversized_json_documents_are_refused_before_they_reach_postgres() {
    use pgqueue::__test_support::{json_exceeds_bytes, max_json_document_bytes};

    let document = json!({ "v": "x".repeat(64) });
    let serialized = serde_json::to_string(&document).unwrap().len();
    assert!(!json_exceeds_bytes(&document, serialized), "a document exactly at the limit is storable");
    assert!(json_exceeds_bytes(&document, serialized - 1), "one byte over must be refused");
    // Escapes count at their serialized width, not their in-memory one.
    let escaped = json!({ "v": "\"" });
    let escaped_serialized = serde_json::to_string(&escaped).unwrap().len();
    assert!(!json_exceeds_bytes(&escaped, escaped_serialized), "an escaped document at the limit is storable");
    assert!(json_exceeds_bytes(&escaped, escaped_serialized - 1), "the escaping backslash must count");
    // Object keys count, wrapped in their own quotes and structure.
    assert!(json_exceeds_bytes(&json!({ "x".repeat(64): 1 }), 64), "keys must count toward the budget");
    // A document with no strings at all still has a serialized size.
    assert!(json_exceeds_bytes(&json!([1, true, null, 2.5]), 0), "non-strings must count too");
    // The shipped ceiling is a policy choice, far under PostgreSQL's own
    // 268435455-byte `jsonb` limits — which this cap makes unreachable.
    assert_eq!(max_json_document_bytes(), 1_048_576);
}

/// The check above is only useful where it is actually *called*, and it was
/// once wired into result finalization alone. Every other unbounded `jsonb` a
/// caller controls — a job's `payload` and `meta`, and a worker lease's
/// `stats` and `metadata` — reached PostgreSQL unchecked, and at the server's
/// own limits came back as `54000`.
///
/// That is the failure the NUL guard beside it exists to prevent, with the same
/// two consequences. Inside `Queue::enqueue_in`/`enqueue_raw_in` the error
/// aborts the *caller's* transaction, so an application that enqueues alongside
/// its own writes loses the whole unit of work — and its `commit()` then reports
/// success, having rolled everything back. On a worker lease it is worse than a
/// lost write: `write_worker_info` is a heartbeat, so it retries a write that
/// can never land, the lease lapses, and the sweeper reclaims every attempt the
/// worker is still running.
#[sqlx::test(migrations = "./migrations")]
async fn test_every_unbounded_jsonb_writer_refuses_an_oversized_document(pool: PgPool) {
    /// One byte past the document ceiling: the string is sized so that string
    /// plus structure lands exactly one over.
    fn oversized() -> Value {
        let max = pgqueue::__test_support::max_json_document_bytes();
        let structure = serde_json::to_string(&json!({ "v": "" })).expect("serialize empty document").len();
        json!({ "v": "x".repeat(max - structure + 1) })
    }
    fn refused(error: pgqueue::Error, what: &str) {
        let message = error.to_string();
        assert!(
            matches!(error, pgqueue::Error::Config(_)) && message.contains("must not exceed"),
            "{what} must be refused as a configuration error, got: {message}"
        );
    }

    let db = TestDb::new(pool).await;

    refused(
        db.queue.enqueue_raw(new_job("oversized_payload", |job| job.payload = oversized())).await.unwrap_err(),
        "an oversized payload",
    );
    refused(
        db.queue.enqueue_raw(new_job("oversized_meta", |job| job.meta = oversized())).await.unwrap_err(),
        "oversized meta",
    );

    // The caller's transaction must stay usable, which is the whole point of
    // refusing before a connection is taken.
    let mut tx = db.queue.pool().begin().await.unwrap();
    let error = db
        .queue
        .enqueue_raw_in(&mut tx, new_job("oversized_in_tx", |job| job.payload = oversized()))
        .await
        .unwrap_err();
    refused(error, "an oversized payload inside a caller transaction");
    let alive: i32 = sqlx::query_scalar("SELECT 1").fetch_one(&mut *tx).await.expect("caller transaction survives");
    assert_eq!(alive, 1);
    tx.commit().await.expect("caller transaction commits");

    let worker_id = uuid::Uuid::now_v7();
    let consumer = db.queue.consumer(worker_id);
    refused(
        consumer.heartbeat(oversized(), None, Duration::from_secs(30)).await.unwrap_err(),
        "oversized worker stats",
    );
    refused(
        consumer.heartbeat(json!({}), Some(oversized()), Duration::from_secs(30)).await.unwrap_err(),
        "oversized worker metadata",
    );

    // A handler is registered first: `build()` refuses an empty registry before
    // it looks at metadata, so without one this would assert the wrong refusal.
    refused(
        pgqueue::Worker::builder(db.queue.clone()).register_job(probe).metadata(oversized()).build().unwrap_err(),
        "oversized builder metadata",
    );
}

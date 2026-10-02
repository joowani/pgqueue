# PGQueue

Async and cron jobs for Rust, backed by PostgreSQL 18+.

## Features

- Turn async functions into background jobs with `#[pgqueue::job]`.
- Schedule cron jobs with `#[pgqueue::cron]`.
- Retry, delay, prioritize, deduplicate, batch, and wait for jobs.
- View queues, workers, and jobs in the built-in dashboard.

## Requirements

- Rust 1.98.1 or newer.
- PostgreSQL 18 or newer, with `fsync` and `synchronous_commit` on to guarantee delivery.
- A direct connection or a session-pooling proxy. PgBouncer transaction and statement pooling are unsupported.

## Quick Start

Add PGQueue to an application with:

```toml
[dependencies]
anyhow = "1"
pgqueue = "0.2"
serde = { version = "1", features = ["derive"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

## Enqueueing Jobs

Define a job and start a worker:

```rust
use pgqueue::{Queue, Worker};
use serde::{Deserialize, Serialize};

// The job's input, taken as the handler function's first parameter (must be JSON-serializable).
#[derive(Serialize, Deserialize)]
pub struct Email {
    pub address: String,
}

// The job's output, returned by the handler function (must be JSON-serializable).
#[derive(Serialize, Deserialize)]
pub struct Receipt {
    pub address: String,
}

// Define a background job.
#[pgqueue::job(
    // Job name, at most 255 bytes (optional; default: function name).
    name = "deliver_email",
    // Total attempts including the initial run (optional; default: 1).
    max_attempts = 5,
    // Max duration of each attempt in milliseconds (optional; default: 10,000; 0 disables timeout).
    timeout_ms = 30_000,
    // Result retention in milliseconds (optional; default: 600,000; 0 deletes immediately).
    result_ttl_ms = 3_600_000,
    // Retention of a failed or aborted job in milliseconds (optional; default: 604,800,000; 0 deletes immediately).
    failed_ttl_ms = 86_400_000,
    // Base retry delay in milliseconds (optional; default: 0).
    retry_delay_ms = 500,
    // Max exponential backoff in milliseconds (optional; default: disabled).
    // Backoff applies full jitter: each retry waits a uniformly random duration
    // up to the bound, so the delays above are ceilings rather than fixed waits.
    max_backoff_ms = 60_000,
    // Dequeue priority; lower values run first (optional; default: 0).
    priority = -10,
)]
pub async fn send_email(args: Email) -> anyhow::Result<Receipt> {
    println!("emailing {}", args.address);
    Ok(Receipt { address: args.address })
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let database_url = std::env::var("DATABASE_URL")?;
    let queue = Queue::connect(&database_url).await?;

    Worker::builder(queue)
        .register_job(send_email)
        .run()
        .await?;

    Ok(())
}
```

In another process, enqueue the job:

```rust
use std::time::Duration;

use crate::{send_email, Email, Receipt};
use pgqueue::{EnqueueResult, JobHandle, Queue};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let database_url = std::env::var("DATABASE_URL")?;
    let queue = Queue::connect(&database_url).await?;

    // Enqueue without waiting for the job to finish.
    let job1 = send_email::job(Email { address: "user1@example.com".into() })
        .dedupe_key("welcome:user1@example.com")
        .delay(Duration::from_secs(5));
    let result: EnqueueResult<JobHandle<send_email>> = queue.enqueue(job1).await?;
    println!("job id: {}", result.job_id());

    // Enqueue and wait for the job to finish.
    let job2 = send_email::job(Email { address: "user2@example.com".into() });
    let receipt: Receipt = queue
        .enqueue_and_wait(job2, Some(Duration::from_secs(30)))
        .await?;
    println!("receipt for: {}", receipt.address);

    // Enqueue many jobs in one statement, with one worker wakeup for the whole batch.
    let batch = (3..=5).map(|n| send_email::job(Email { address: format!("user{n}@example.com") }));
    let results = queue.enqueue_batch(batch).await?;
    println!("enqueued {} jobs", results.len());

    Ok(())
}
```

`Queue::connect` applies database migrations, which needs permission to create or update the `pgqueue` schema. A
migration that alters a table also needs a moment when no other transaction holds it, so upgrade outside long
transactions such as a `pg_dump`. Every worker on a queue must register every job. Use separate queues for workers with
different jobs.

`Queue::enqueue_in` and `Queue::enqueue_batch_in` publish inside your own transaction, which holds each dedupe key's
lock until it ends. Every such lock takes an entry of PostgreSQL's lock table, which all sessions on the server share,
so one transaction may hold half of the table that `max_locks_per_transaction` × `max_connections` sizes, and at least
1,000. Past that, a keyed publish fails with `Error::Config`; commit large keyed publishes in chunks.

## Cron Jobs

Define cron jobs to run on a recurring schedule:

```rust
use pgqueue::{JobContext, Queue, Worker};

// Cron jobs have no payload. Parameters are context extractors.
#[pgqueue::cron(
    // Five-field crontab schedule (`minute hour day-of-month month day-of-week`) in UTC. Sunday is `0` or `7`.
    "0 * * * *",
    // Revision; increment after changes, highest wins across workers (optional; default: 0).
    // The stored schedule is compared verbatim, so *any* textual edit needs a bump — including a
    // semantically equivalent one like `SUN` to `0`, or a whitespace change. Editing the expression
    // without bumping the revision leaves workers disagreeing and disables the cron.
    revision = 1,
    // Job name, at most 250 bytes due to the cron dedupe key (optional; default: function name).
    name = "collect_hourly_metrics",
    // Total attempts including the initial run (optional; default: 1).
    max_attempts = 2,
    // Max duration of each attempt in milliseconds (optional; default: 10,000; 0 disables timeout).
    timeout_ms = 120_000,
    // Result retention in milliseconds (optional; default: 600,000; 0 deletes immediately).
    result_ttl_ms = 604_800_000,
    // Retention of a failed or aborted job in milliseconds (optional; default: 604,800,000; 0 deletes immediately).
    failed_ttl_ms = 2_592_000_000,
    // Base retry delay in milliseconds (optional; default: 0).
    retry_delay_ms = 1_000,
    // Max exponential backoff in milliseconds (optional; default: disabled).
    // Backoff applies full jitter, as above.
    max_backoff_ms = 60_000,
    // Dequeue priority; lower values run first (optional; default: 0).
    priority = 10,
)]
async fn collect_hourly_metrics(ctx: JobContext) -> anyhow::Result<()> {
    let queued = ctx.queue().counts().await?.queued;
    println!("{queued} job(s) queued");
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let database_url = std::env::var("DATABASE_URL")?;
    let queue = Queue::connect(&database_url).await?;

    Worker::builder(queue)
        .register_cron(collect_hourly_metrics)
        .run()
        .await?;

    Ok(())
}
```

For schedules loaded at runtime, define a regular `#[pgqueue::job]` and use `WorkerBuilder::schedule_cron`:

```rust
use pgqueue::{Queue, Worker};

#[pgqueue::job]
async fn cleanup(_: ()) -> anyhow::Result<()> {
    println!("cleaning up");
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let database_url = std::env::var("DATABASE_URL")?;
    let queue = Queue::connect(&database_url).await?;

    Worker::builder(queue)
        .schedule_cron("0 3 * * *", cleanup::job(()))
        .run()
        .await?;

    Ok(())
}
```

## Dashboard

Enable the `dashboard` feature to use the built-in web dashboard:

```toml
pgqueue = { version = "0.2", features = ["dashboard"] }
```

The dashboard shows your queues, workers, and jobs. Run it as a standalone server behind a reverse proxy:

```rust
use pgqueue::{Dashboard, Queue};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let database_url = std::env::var("DATABASE_URL")?;
    let queue = Queue::connect(&database_url).await?;

    Dashboard::new([queue])
        .basic_auth("admin", std::env::var("PGQUEUE_DASHBOARD_PASSWORD")?)
        .allowed_hosts(["queues.example.com"]) // the name clients reach the proxy by
        .trusted_proxy_hops(1) // one proxy in front, appending each client's address to X-Forwarded-For
        .serve_on("localhost", 8080) // reachable only through the proxy on this machine
        .run()
        .await?;

    Ok(())
}
```

The server speaks plain HTTP and sets `Secure` session cookies, so serve it through a reverse proxy that terminates
TLS. Have the proxy forward the client's `Host` header and append the client's address to `X-Forwarded-For`; nginx
does neither by default:

```nginx
proxy_set_header Host $host;
proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
```

`trusted_proxy_hops` is the number of such proxies in front of the dashboard. The dashboard throttles password guesses
per client, and reads each client's address from the entries those proxies appended. At the default of `0` it uses the
connection's address instead, which behind a proxy is the proxy's for every request, so a flood of wrong passwords from
anywhere locks everyone out, the operator included. Keep `0` for a dashboard that clients can connect to directly,
since they can write anything into the header.

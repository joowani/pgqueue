//! The README's "Enqueueing Jobs" producer process, compiled with the crate so the quickstart
//! cannot drift from the API. The README's producer imports `send_email` from its own crate; as a
//! standalone example binary this one carries the same definition (it must match the worker's).
//! Run it against the Docker Compose Postgres while `--example worker` is processing:
//!
//! `DATABASE_URL=postgres://pgqueue:pgqueue@localhost:3532/pgqueue cargo run --example enqueue`

use std::time::Duration;

use pgqueue::{EnqueueResult, JobHandle, Queue};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct Email {
    pub address: String,
}

#[derive(Serialize, Deserialize)]
pub struct Receipt {
    pub address: String,
}

#[pgqueue::job(
    name = "deliver_email",
    max_attempts = 5,
    timeout_ms = 30_000,
    result_ttl_ms = 3_600_000,
    failed_ttl_ms = 86_400_000,
    retry_delay_ms = 500,
    max_backoff_ms = 60_000,
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

    // Enqueue without waiting for the job to finish.
    let job1 = send_email::job(Email { address: "user1@example.com".into() })
        .dedupe_key("welcome:user1@example.com")
        .delay(Duration::from_secs(5));
    let result: EnqueueResult<JobHandle<send_email>> = queue.enqueue(job1).await?;
    println!("job id: {}", result.job_id());

    // Enqueue and wait for the job to finish.
    let job2 = send_email::job(Email { address: "user2@example.com".into() });
    let receipt: Receipt = queue.enqueue_and_wait(job2, Some(Duration::from_secs(30))).await?;
    println!("receipt for: {}", receipt.address);

    // Enqueue many jobs in one statement, with one worker wakeup for the whole batch.
    let batch = (3..=5).map(|n| send_email::job(Email { address: format!("user{n}@example.com") }));
    let results = queue.enqueue_batch(batch).await?;
    println!("enqueued {} jobs", results.len());

    Ok(())
}

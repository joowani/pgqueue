//! The README's "Enqueueing Jobs" worker process, compiled with the crate so the quickstart cannot
//! drift from the API. Run it against the Docker Compose Postgres:
//!
//! `DATABASE_URL=postgres://pgqueue:pgqueue@localhost:3532/pgqueue cargo run --example worker`

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

    Worker::builder(queue).register_job(send_email).run().await?;

    Ok(())
}

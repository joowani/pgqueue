//! Applies the embedded PGQueue database migrations and exits.

use anyhow::Context;
use pgqueue::Queue;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let database_url = std::env::var("DATABASE_URL").context("DATABASE_URL is not set")?;
    Queue::connect(&database_url).await?;
    println!("PGQueue migrations are current");
    Ok(())
}

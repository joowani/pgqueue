//! The README's standalone dashboard server, compiled with the crate so the quickstart cannot
//! drift from the API. Run it against the Docker Compose Postgres:
//!
//! `DATABASE_URL=postgres://pgqueue:pgqueue@localhost:3532/pgqueue \
//!  PGQUEUE_DASHBOARD_PASSWORD=local-password cargo run --features dashboard --example dashboard`

use pgqueue::{Dashboard, Queue};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let database_url = std::env::var("DATABASE_URL")?;
    let queue = Queue::connect(&database_url).await?;

    Dashboard::new([queue])
        .basic_auth("admin", std::env::var("PGQUEUE_DASHBOARD_PASSWORD")?)
        .secure_cookies(false) // only for direct HTTP on a trusted network
        .serve_on("localhost", 8080)
        .run()
        .await?;

    Ok(())
}

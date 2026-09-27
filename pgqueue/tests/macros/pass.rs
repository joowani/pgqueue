use pgqueue::{CronDefinition, JobContext, JobState, JobType};

#[derive(Clone)]
struct Db;

#[derive(serde::Serialize, serde::Deserialize)]
struct Payload {
    value: u32,
}

#[pgqueue::job(max_attempts = 3, timeout_ms = 30_000, failed_ttl_ms = 86_400_000, priority = -1)]
async fn work(args: Payload, db: JobState<Db>, ctx: JobContext) -> anyhow::Result<u32> {
    let (_, _) = (db, ctx);
    Ok(args.value)
}

/// `syn` only folds a leading `-` into a negative literal when the literal is
/// the attribute's last token, so a negative priority written anywhere else
/// arrives as a unary negation — and `-32768` has no positive counterpart to
/// negate, which is the one magnitude that path has to special-case.
#[pgqueue::job(priority = -32768, max_attempts = 2)]
async fn lowest_priority(_: ()) {}

#[pgqueue::cron("*/5 * * * *", max_attempts = 2, revision = 3)]
async fn cleanup(db: JobState<Db>) {
    let _ = db;
}

fn main() {
    assert_eq!(work::NAME, "work");
    assert_eq!(cleanup::SCHEDULE, "*/5 * * * *");
    assert_eq!(cleanup::CRON_REVISION, 3);
    assert_eq!(lowest_priority::config().priority, i16::MIN);
    let _ = work::job(Payload { value: 1 });
    let _ = lowest_priority::job(());
    let _ = cleanup::job();
}

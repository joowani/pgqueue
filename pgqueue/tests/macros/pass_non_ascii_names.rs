//! A function name outside ASCII is never a keyword, so the job's struct keeps the user's own token instead of a raw
//! identifier rebuilt from its string, which `fail_emoji_name.rs` shows is not safe for every name rustc hands over.
//! The struct still has to read as the expansion's own to the naming lints, as an ASCII one does, and a raw spelling
//! still names the same job.
#![forbid(nonstandard_style)]
#![deny(warnings)]

#[pgqueue::job]
async fn émail(_: ()) {}

#[pgqueue::cron("* * * * *")]
async fn 日次() {}

#[pgqueue::job]
async fn r#émoi(_: ()) {}

fn main() {
    assert_eq!(<émail as pgqueue::JobType>::NAME, "émail");
    assert_eq!(<日次 as pgqueue::JobType>::NAME, "日次");
    assert_eq!(<émoi as pgqueue::JobType>::NAME, "émoi");
    let _ = émail::job(());
    let _ = 日次::job();
    let _ = r#émoi::job(());
}

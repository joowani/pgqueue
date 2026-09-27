//! The `#[allow(deprecated)]` the expansion needs for a `#[deprecated]` job
//! type must not extend over the handler body the user wrote, or a crate
//! migrating off a deprecated API gets no signal from inside any job.
//!
//! Only a job that is itself `#[deprecated]` gets that allow, so that is the one that has to report the call in its
//! body; the other two are the controls for jobs the allow never reaches.
#![deny(deprecated)]

#[deprecated(note = "do not call")]
pub fn old_helper() -> u32 {
    1
}

#[pgqueue::job]
async fn uses_deprecated(_: ()) -> anyhow::Result<u32> {
    Ok(old_helper())
}

#[pgqueue::cron("* * * * *")]
async fn cron_uses_deprecated() -> anyhow::Result<u32> {
    Ok(old_helper())
}

#[deprecated(note = "retiring")]
#[pgqueue::job]
async fn deprecated_job_uses_deprecated(_: ()) -> anyhow::Result<u32> {
    Ok(old_helper())
}

fn control() -> u32 {
    old_helper()
}

fn main() {
    let _ = uses_deprecated::job(());
    let _ = cron_uses_deprecated::job();
    #[allow(deprecated)]
    let _ = deprecated_job_uses_deprecated::job(());
    let _ = control();
}

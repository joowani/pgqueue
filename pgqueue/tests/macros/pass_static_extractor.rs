//! Extractors may borrow. The expansion only re-emits an extractor's type as a `call()` parameter and inside a function
//! body, where a reference is as legal as in the handler itself, so `impl FromJobContext for &'static AppConfig` works
//! like any other extractor — written `&'static AppConfig` or with the lifetime elided — and so does a reference inside
//! one, as `JobState<&AppConfig>`. Every borrowed parameter used to be refused as if it were the payload, "built per
//! attempt", which a `'static` reference is not.
//!
//! The payload is the one parameter that cannot borrow, and only a lifetime left to elision is refused inside it: a
//! named one can be legitimate, as `Cow<'static, str>` is.
#![deny(warnings)]

use std::borrow::Cow;

use pgqueue::{FromJobContext, JobContext, JobError, JobState};

pub struct AppConfig {
    pub name: &'static str,
}

static CONFIG: AppConfig = AppConfig { name: "app" };

impl FromJobContext for &'static AppConfig {
    fn from_context(_: &JobContext) -> Result<Self, JobError> {
        Ok(&CONFIG)
    }
}

#[pgqueue::job]
async fn uses_static(_: (), config: &'static AppConfig) {
    let _ = config.name;
}

#[pgqueue::job]
async fn uses_elided(_: (), config: &AppConfig) {
    let _ = config.name;
}

#[pgqueue::cron("* * * * *")]
async fn tick(config: &'static AppConfig) {
    let _ = config.name;
}

#[pgqueue::job]
async fn state_of_a_reference(_: (), elided: JobState<&AppConfig>, explicit: JobState<&'static AppConfig>) {
    let _ = (elided.0.name, explicit.0.name);
}

#[pgqueue::job]
async fn static_cow(text: Cow<'static, str>) -> anyhow::Result<Cow<'static, str>> {
    Ok(text)
}

#[allow(dead_code)]
async fn register(queue: pgqueue::Queue) {
    let _ = pgqueue::Worker::builder(queue)
        .state::<&'static AppConfig>(&CONFIG)
        .register_job(uses_static)
        .register_job(uses_elided)
        .register_cron(tick)
        .register_job(state_of_a_reference)
        .register_job(static_cow);
    uses_static::call((), &CONFIG).await;
    uses_elided::call((), &CONFIG).await;
    tick::call(&CONFIG).await;
}

fn main() {
    let _ = uses_static::job(());
    let _ = uses_elided::job(());
    let _ = tick::job();
    let _ = state_of_a_reference::job(());
    let _ = static_cow::job(Cow::Borrowed("text"));
}

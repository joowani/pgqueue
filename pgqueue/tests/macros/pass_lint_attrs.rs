//! A lint attribute the user wrote has to reach the generated `impl` blocks,
//! not just the struct and the hidden function.
//!
//! Both impls re-mention the payload, the extractor and the return type with
//! the *user's* spans, so a lint fires there and is not written off as
//! external-macro code. With the user's `#[allow(deprecated)]` routed only to
//! the struct and the function, the identical plain `async fn` below compiled
//! while the job did not.
#![deny(warnings)]
// A crate is free to forbid the lint that reports an unfulfilled expectation.
// The expansion may therefore never write `#[allow(unfulfilled_lint_expectations)]`
// of its own: `forbid` cannot be overridden later in the same crate, so that
// `allow` is `error[E0453]` and `#[expect(...)]` on a job stopped compiling
// while the equivalent plain function was fine.
#![forbid(unfulfilled_lint_expectations)]

use pgqueue::JobState;

#[deprecated(note = "use NewPayload")]
#[derive(serde::Serialize, serde::Deserialize)]
pub struct OldPayload;

#[deprecated(note = "use NewOutput")]
#[derive(serde::Serialize, serde::Deserialize)]
pub struct OldOutput;

#[deprecated(note = "use NewState")]
#[derive(Clone)]
pub struct OldState;

/// The control: a plain function naming the same deprecated types under the
/// same attribute compiles.
#[allow(deprecated)]
pub async fn plain(_: OldPayload, state: JobState<OldState>) -> anyhow::Result<OldOutput> {
    let _ = state;
    Ok(OldOutput)
}

#[pgqueue::job]
#[allow(deprecated)]
pub async fn as_job(_: OldPayload, state: JobState<OldState>) -> anyhow::Result<OldOutput> {
    let _ = state;
    Ok(OldOutput)
}

#[pgqueue::cron("* * * * *")]
#[allow(deprecated)]
pub async fn as_cron(state: JobState<OldState>) -> anyhow::Result<OldOutput> {
    let _ = state;
    Ok(OldOutput)
}

/// `#[expect(...)]` is lowered to an `allow` on every item the expansion
/// writes, including the hidden handler. One written item becomes several and
/// the lint fires on only one of them, so any copy that kept the expectation
/// would report as unfulfilled through no fault of the user's.
#[pgqueue::job]
#[expect(deprecated)]
pub async fn expecting(_: OldPayload) -> anyhow::Result<()> {
    Ok(())
}

/// A `#[forbid(...)]` reaches the impls lowered to `deny`: the expansion has to
/// be able to write its own `#[allow(deprecated)]` there for a deprecated job.
///
/// The job is `#[deprecated]` on purpose. Without it `allow_deprecated` expands
/// to nothing, so there is no `#[allow(deprecated)]` on the impls for a
/// verbatim `#[forbid(deprecated)]` to collide with, and the case compiles with
/// or without the lowering — pinning nothing. Pairing the two is what makes
/// this `error[E0453]` the moment the lowering is dropped.
#[pgqueue::job]
#[forbid(deprecated)]
#[deprecated(note = "use as_job")]
pub async fn forbidding(_: ()) -> anyhow::Result<()> {
    Ok(())
}

/// A job that is itself deprecated keeps working while carrying a lint
/// attribute of its own.
#[pgqueue::job]
#[deprecated(note = "use as_job")]
#[allow(deprecated)]
pub async fn legacy(_: OldPayload) -> anyhow::Result<()> {
    Ok(())
}

fn main() {
    #[allow(deprecated)]
    {
        let _ = plain;
        let _ = as_job::job(OldPayload);
        let _ = as_cron::job();
        let _ = expecting::job(OldPayload);
        let _ = forbidding::job(());
        let _ = legacy::job(OldPayload);
    }
}

/// The same routing, conditionally: a `cfg_attr` whose tail is lint control
/// reaches the generated impls too — without the copies, this job failed under
/// this file's `deny(warnings)` on the impls' own mentions of the deprecated
/// types while the handler compiled fine. `not(any())` is the always-true
/// predicate, so the enabled branch is what compiles here.
#[pgqueue::job]
#[cfg_attr(not(any()), allow(deprecated))]
pub async fn conditionally_allowed(_: OldPayload, state: JobState<OldState>) -> anyhow::Result<OldOutput> {
    let _ = state;
    Ok(OldOutput)
}

#[pgqueue::cron("* * * * *")]
#[cfg_attr(not(any()), allow(deprecated))]
pub async fn conditionally_allowed_cron(state: JobState<OldState>) -> anyhow::Result<OldOutput> {
    let _ = state;
    Ok(OldOutput)
}

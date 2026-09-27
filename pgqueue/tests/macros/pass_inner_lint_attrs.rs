//! An attribute at the top of a handler's body belongs to the function item itself, signature included, exactly as its
//! outer spelling does — so it has to reach the items a job expands to the same way `pass_lint_attrs.rs` pins for the
//! outer one. Left on the hidden function alone, an inner `#![allow(missing_docs)]` never reached the struct that lint
//! fires on, an inner `#![allow(deprecated)]` never reached the impls that re-mention the user's types, an inner
//! `#![expect(...)]` was left unfulfilled, and an inner `#![deprecated]` deprecated the hidden function — so every
//! build warned about the expansion's own calls of it — instead of the job. The `plain_*` functions are the controls.
#![deny(warnings)]
#![deny(missing_docs)]
#![forbid(unfulfilled_lint_expectations)]

use pgqueue::JobState;

/// A deprecated payload.
#[deprecated(note = "use NewPayload")]
#[derive(serde::Serialize, serde::Deserialize)]
pub struct OldPayload;

/// A deprecated output.
#[deprecated(note = "use NewOutput")]
#[derive(serde::Serialize, serde::Deserialize)]
pub struct OldOutput;

/// Deprecated worker state.
#[deprecated(note = "use NewState")]
#[derive(Clone)]
pub struct OldState;

pub async fn plain_allowed(_: ()) {
    #![allow(missing_docs)]
}

#[pgqueue::job]
pub async fn allowed(_: ()) {
    #![allow(missing_docs)]
}

#[pgqueue::cron("* * * * *")]
pub async fn allowed_cron() {
    #![allow(missing_docs)]
}

pub async fn plain_expected(_: ()) {
    #![expect(missing_docs)]
}

#[pgqueue::job]
pub async fn expected(_: ()) {
    #![expect(missing_docs)]
}

/// The control: a plain function naming the deprecated types under the inner attribute.
pub async fn plain_deprecated_types(_: OldPayload, state: JobState<OldState>) -> anyhow::Result<OldOutput> {
    #![allow(deprecated)]
    let _ = state;
    Ok(OldOutput)
}

/// Names the deprecated types in the signature the impls re-mention.
#[pgqueue::job]
pub async fn deprecated_types(_: OldPayload, state: JobState<OldState>) -> anyhow::Result<OldOutput> {
    #![allow(deprecated)]
    let _ = state;
    Ok(OldOutput)
}

/// The same for the cron expansion.
#[pgqueue::cron("* * * * *")]
pub async fn deprecated_types_cron(state: JobState<OldState>) -> anyhow::Result<OldOutput> {
    #![allow(deprecated)]
    let _ = state;
    Ok(OldOutput)
}

/// Forbids `deprecated` and is deprecated itself, so its impls carry the expansion's own `#[allow(deprecated)]`: the
/// forbid has to reach them lowered to `deny`, as the outer one does, or the pair is `error[E0453]`.
#[pgqueue::job]
pub async fn forbidding(_: ()) -> anyhow::Result<()> {
    #![forbid(deprecated)]
    #![deprecated(note = "use deprecated_types")]
    Ok(())
}

/// Deprecated from inside its body. The only use is under an allow, so this compiles only while the deprecation sits on
/// the struct the caller names rather than on the function the expansion calls.
#[pgqueue::job]
pub async fn retired(_: ()) {
    #![deprecated(note = "use deprecated_types")]
}

/// The same for the cron expansion.
#[pgqueue::cron("* * * * *")]
pub async fn retired_cron() {
    #![deprecated(note = "use deprecated_types_cron")]
}

fn main() {
    let _ = (plain_allowed, plain_expected);
    let _ = allowed::job(());
    let _ = allowed_cron::job();
    let _ = expected::job(());
    #[allow(deprecated)]
    {
        let _ = plain_deprecated_types;
        let _ = deprecated_types::job(OldPayload);
        let _ = deprecated_types_cron::job();
        let _ = forbidding::job(());
        let _ = retired::job(());
        let _ = retired_cron::job();
    }
}

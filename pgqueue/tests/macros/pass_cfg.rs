//! rustc evaluates `#[cfg]` and `#[cfg_attr]` *before* it invokes an attribute macro, so a
//! configured-out job never reaches the expansion: the item is gone, and with it the struct, the
//! impls and the handler, all together. That is what makes both configurations compile, and it is
//! why the expansion carries no `cfg` routing of its own — `#[pgqueue::job(bogus_key = 1)]
//! #[cfg(any())] async fn f(_: ()) {}` compiles clean, because nothing ever parsed the attribute.
//! This file pins that: an enabled job must be fully usable, and a disabled one must leave nothing
//! behind that could reference a handler the configuration removed.
//!
//! `any()` is the always-false predicate and `not(any())` the always-true one; a made-up feature
//! name would trip `unexpected_cfgs` under the `deny(warnings)` this file compiles with.
#![deny(warnings)]

// Disabled: nothing of this job may survive, or the build fails on the orphaned
// generated items referencing the removed handler.
#[pgqueue::job]
#[cfg(any())]
async fn configured_out(_: ()) -> anyhow::Result<()> {
    Ok(())
}

// Enabled: the whole expansion must still be there to enqueue.
#[pgqueue::job]
#[cfg(not(any()))]
async fn configured_in(_: ()) -> anyhow::Result<()> {
    Ok(())
}

// A `cfg` in cron mode takes the same route through the expansion.
#[pgqueue::cron("*/5 * * * *")]
#[cfg(any())]
async fn cron_configured_out() -> anyhow::Result<()> {
    Ok(())
}

// A `cfg_attr` that cannot yield a `cfg` resolves to an attribute or to
// nothing; the function exists either way, so it stays on the handler and both
// predicate outcomes compile.
#[pgqueue::job]
#[cfg_attr(any(), deprecated)]
async fn conditionally_linted(_: ()) -> anyhow::Result<()> {
    Ok(())
}

#[pgqueue::job]
#[cfg_attr(not(any()), allow(dead_code))]
async fn conditionally_linted_enabled(_: ()) -> anyhow::Result<()> {
    Ok(())
}

// A `cfg_attr` that *does* yield a `cfg` removes the item it lands on, so it
// has to gate the whole expansion — with the predicate true (the item is gone)
// and with it false (everything is present) alike.
#[pgqueue::job]
#[cfg_attr(not(any()), cfg(any()))]
async fn conditionally_configured_out(_: ()) -> anyhow::Result<()> {
    Ok(())
}

#[pgqueue::job]
#[cfg_attr(any(), cfg(any()))]
async fn conditionally_configured_in(_: ()) -> anyhow::Result<()> {
    Ok(())
}

fn main() {
    let _ = configured_in::job(());
    let _ = conditionally_linted::job(());
    let _ = conditionally_linted_enabled::job(());
    let _ = conditionally_configured_in::job(());
}

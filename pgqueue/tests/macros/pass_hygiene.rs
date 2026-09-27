//! Hygiene and lint-attribute routing for the job macros.
//!
//! The expansion splits one written function into a public struct and a hidden
//! function, so a lint attribute the user wrote has to reach whichever of the
//! two the lint actually fires on. `missing_docs` fires on the struct.
#![deny(missing_docs)]
#![deny(warnings)]

/// Jobs in a module that denies undocumented public items.
pub mod documented {
    /// Carries its own documentation, so no lint attribute is needed.
    #[pgqueue::job]
    pub async fn described(_: ()) {}

    #[pgqueue::job]
    #[allow(missing_docs)]
    pub async fn allowed(_: ()) {}

    #[pgqueue::job]
    #[expect(missing_docs)]
    pub async fn expected(_: ()) {}
}

/// Every path the expansion emits is absolute, so it compiles with neither the
/// standard nor the extern prelude in scope.
#[no_implicit_prelude]
pub mod noprelude {
    /// A job configured through the attribute, so `JobConfig::default` and the
    /// config setters are all exercised.
    #[::pgqueue::job(
        max_attempts = 3,
        timeout_ms = 30_000,
        retry_delay_ms = 500,
        max_backoff_ms = 60_000
    )]
    pub async fn np_job(_: ()) {}

    /// The same, for the cron expansion.
    #[::pgqueue::cron("*/5 * * * *", revision = 2)]
    pub async fn np_cron() {}
}

/// Every name the expansion binds is a *pattern*, so an in-scope unit struct or
/// const of that name silently reinterprets it as a path pattern — `E0530` for
/// a const, `E0308` for a unit struct — with a diagnostic that points at the
/// attribute and names nothing in the user's source.
pub mod hand_written_shadows {
    /// The name the config binding used to take, in the value namespace.
    #[allow(non_upper_case_globals)]
    pub const config: u32 = 1;

    /// The name `job(args: ...)`'s parameter pattern used to take.
    #[allow(non_camel_case_types)]
    pub struct args;

    /// Compiles only while the expansion binds neither name.
    #[pgqueue::job(max_attempts = 3, priority = 1)]
    pub async fn neighbour(_: u32) {
        let _ = (config, args);
    }

    /// The cron expansion binds the same names.
    #[::pgqueue::cron("* * * * *", max_attempts = 2)]
    pub async fn cron_neighbour() {}
}

/// The realistic trigger is the macro itself: these generate `struct config;`
/// and `struct args;` at module level, which are then in scope for every other
/// job in the same module.
pub mod generated_shadows {
    /// Generates the module-level `struct config;`.
    #[pgqueue::job]
    pub async fn config(_: ()) {}

    /// Generates the module-level `struct args;`.
    #[pgqueue::job]
    pub async fn args(_: u32) {}

    /// The neighbour that used to stop compiling.
    #[pgqueue::job(max_attempts = 2, timeout_ms = 1_000)]
    pub async fn neighbour(_: u32) {}

    /// And its cron equivalent.
    #[::pgqueue::cron("* * * * *", max_attempts = 2)]
    pub async fn cron_neighbour() {}
}

/// An unconditional `#[allow(unused_mut)]` in the expansion is itself an
/// `error[E0453]` here, so `mut` has to be emitted only when it is used.
#[forbid(unused_mut)]
pub mod forbids_unused_mut {
    /// No attributes, so nothing assigns to the config binding.
    #[pgqueue::job]
    pub async fn bare(_: ()) {}

    /// Attributes, so the config binding has to be mutable.
    #[pgqueue::job(max_attempts = 2)]
    pub async fn configured(_: ()) {}
}

/// `syn` hoists a handler body's *inner* attributes into `ItemFn::attrs`.
/// Re-emitting one in front of the generated struct re-emits its leading `!`
/// too — "an inner attribute is not permitted in this context" — and the doc
/// strip deleted an inner `//!` outright. Both stay in the body: the crate's
/// `#![deny(warnings)]` turns the unused binding below into an error unless the
/// inner `allow` survives on the function the user wrote. The inner doc also
/// documents the job, as it documents any function it sits in.
pub mod inner_attributes {
    /// Carries an inner lint level and inner documentation.
    #[pgqueue::job]
    pub async fn inner_attrs(_: ()) -> anyhow::Result<()> {
        #![allow(unused_variables)]
        //! Documents the job, as an inner doc comment documents any function.
        let unused = 1;
        Ok(())
    }

    /// The same for the cron expansion.
    #[::pgqueue::cron("* * * * *")]
    pub async fn inner_attrs_cron() {
        #![allow(unused_variables)]
        //! Documents the job, as an inner doc comment documents any function.
        let unused = 1;
    }
}

/// A `#[forbid(...)]` copied verbatim onto the generated struct met the
/// `#[allow(non_camel_case_types)]` the expansion used to write there, because
/// the struct is named after the user's snake_case function: `error[E0453]:
/// allow(non_camel_case_types) incompatible with previous forbid`. The struct's
/// name no longer needs an allow (see `pass_forbid_style.rs`), but the copy is
/// still lowered to `deny` for the allows the impls of a deprecated job carry.
pub mod forbidden_levels {
    /// Forbids exactly the lint the generated struct used to allow.
    #[pgqueue::job]
    #[forbid(non_camel_case_types)]
    pub async fn forbidding(_: ()) {}

    /// The same for the cron expansion.
    #[::pgqueue::cron("* * * * *")]
    #[forbid(non_camel_case_types)]
    pub async fn forbidding_cron() {}

    /// A forbid the struct genuinely has to honour compiles, lowered to `deny`. That it still reaches the struct cannot
    /// show here, where the crate denies `missing_docs` anyway; `fail_forbid.rs` pins it.
    #[pgqueue::job]
    #[forbid(missing_docs)]
    pub async fn documented_by_forbid(_: ()) {}
}

/// The expansion binds the handler's value before encoding it, and that `let` carries the handler body's span, so rustc
/// lints it as the user's own statement. Unascribed, a handler returning `()` tripped the allow-by-default
/// `unit_bindings` on a binding the user never wrote.
#[deny(unit_bindings)]
pub mod unit_results {
    /// Returns `()` implicitly.
    #[pgqueue::job]
    pub async fn implicit_unit(_: ()) {}

    /// Returns `()` explicitly.
    #[pgqueue::job]
    pub async fn explicit_unit(_: ()) -> () {}

    /// The same for the cron expansion.
    #[::pgqueue::cron("* * * * *")]
    pub async fn unit_cron() {}
}

fn main() {
    let _ = unit_results::implicit_unit::job(());
    let _ = unit_results::explicit_unit::job(());
    let _ = unit_results::unit_cron::job();
    let _ = inner_attributes::inner_attrs::job(());
    let _ = inner_attributes::inner_attrs_cron::job();
    let _ = forbidden_levels::forbidding::job(());
    let _ = forbidden_levels::forbidding_cron::job();
    let _ = forbidden_levels::documented_by_forbid::job(());
    let _ = documented::allowed::job(());
    let _ = documented::described::job(());
    let _ = documented::expected::job(());
    let _ = noprelude::np_job::job(());
    let _ = noprelude::np_cron::job();
    let _ = hand_written_shadows::neighbour::job(1);
    let _ = hand_written_shadows::cron_neighbour::job();
    let _ = generated_shadows::config::job(());
    let _ = generated_shadows::args::job(1);
    let _ = generated_shadows::neighbour::job(1);
    let _ = generated_shadows::cron_neighbour::job();
    let _ = forbids_unused_mut::bare::job(());
    let _ = forbids_unused_mut::configured::job(());
}

//! Jobs defined by a `macro_rules!` wrapper.
//!
//! A metavariable substitution arrives inside an invisible (`Delimiter::None`)
//! group, and syn only flattens it when it is the last token of the attribute.
//! Every value below is therefore written in a position *other* than the last,
//! which is exactly what used to be rejected — `timeout_ms = $ms,
//! max_attempts = 3` failed while `max_attempts = 3, timeout_ms = $ms`
//! compiled.

use std::time::Duration;

use pgqueue::{CronDefinition, JobType};

macro_rules! define_job {
    ($name:literal, $ms:literal, $priority:literal, $attempts:expr) => {
        #[pgqueue::job(
            timeout_ms = $ms,
            name = $name,
            priority = -$priority,
            max_attempts = $attempts,
            result_ttl_ms = $ms,
            retry_delay_ms = $ms,
            max_backoff_ms = $ms,
        )]
        async fn generated(_: ()) {}
    };
}

macro_rules! define_cron {
    ($schedule:literal, $revision:expr, $name:literal, $priority:expr) => {
        #[pgqueue::cron(
            $schedule,
            revision = $revision,
            priority = $priority,
            name = $name,
        )]
        async fn generated_cron() {}
    };
}

// A payload type substituted from the *caller's* syntax context, which the
// expansion spans the payload decode on so the obligation lands on the type the
// user wrote. Nothing the expansion binds may travel out of its own context on
// that span, or the job stops compiling for a reason named nowhere in this file.
macro_rules! define_typed_job {
    ($payload:ty) => {
        #[pgqueue::job(name = "generated_typed", max_attempts = 2)]
        async fn generated_typed(value: $payload) {
            let _ = value;
        }
    };
}

// The *return* type travels the same way, and the expansion spans the result
// encode on it so a handler returning a bare value blames its own `->` rather
// than the attribute. A `ty` fragment arrives inside an invisible group, so it
// carries no context of its own; `tt` and `ident` do not, and a generated
// binding referenced through such a span lands in the caller's context while its
// `let` stays in the expansion's. That broke the job with `cannot find value
// `__result` in this scope` — an identifier written nowhere in this file.
macro_rules! define_returning_job {
    ($name:ident, $($ret:tt)+) => {
        #[pgqueue::job(name = "generated_returning", max_attempts = 2)]
        async fn $name(_: ()) -> $($ret)+ {
            Ok(1)
        }
    };
}

type AliasedResult = anyhow::Result<u32>;

macro_rules! define_aliased_job {
    ($ret:ident) => {
        #[pgqueue::job(name = "generated_aliased", max_attempts = 2)]
        async fn generated_aliased(_: ()) -> $ret {
            Ok(2)
        }
    };
}

// An optional bound list splices a `where` with no predicates on the zero-bound
// invocation. That signature is identical to one carrying no clause at all —
// and `fn f<>(...)`, the equally empty generics form, was already accepted — yet
// the attribute refused it as "generic", with the diagnostic collapsed onto the
// attribute because an empty `WhereClause` tokenizes to nothing for
// `new_spanned` to point at.
macro_rules! define_bounded_job {
    ($name:ident, $label:literal, $($ty:ty: $bound:path),*) => {
        #[pgqueue::job(name = $label, max_attempts = 2)]
        async fn $name(_: ()) where $($ty: $bound),* {}
    };
}

macro_rules! define_returning_cron {
    ($($ret:tt)+) => {
        #[pgqueue::cron("*/5 * * * *", name = "generated_returning_cron")]
        async fn generated_returning_cron() -> $($ret)+ {
            Ok(())
        }
    };
}

define_job!("generated_job", 30_000, 5, 3);
define_cron!("*/5 * * * *", 7, "generated_cron", 9);
define_typed_job!(u32);
define_returning_job!(generated_returning, anyhow::Result<u32>);
define_aliased_job!(AliasedResult);
define_returning_cron!(anyhow::Result<()>);
define_bounded_job!(generated_bounded, "generated_bounded",);

fn main() {
    assert_eq!(generated::NAME, "generated_job");
    let config = generated::config();
    assert_eq!(config.timeout, Some(Duration::from_millis(30_000)));
    assert_eq!(config.retry_delay, Duration::from_millis(30_000));
    assert_eq!(config.priority, -5);
    assert_eq!(config.max_attempts, 3);

    assert_eq!(generated_cron::NAME, "generated_cron");
    assert_eq!(generated_cron::SCHEDULE, "*/5 * * * *");
    assert_eq!(generated_cron::CRON_REVISION, 7);
    assert_eq!(generated_cron::config().priority, 9);

    assert_eq!(generated_typed::NAME, "generated_typed");
    assert_eq!(generated_typed::config().max_attempts, 2);

    assert_eq!(generated_returning::NAME, "generated_returning");
    assert_eq!(generated_aliased::NAME, "generated_aliased");
    assert_eq!(generated_returning_cron::SCHEDULE, "*/5 * * * *");

    assert_eq!(generated_bounded::NAME, "generated_bounded");
}

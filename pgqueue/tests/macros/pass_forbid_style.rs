//! A crate that forbids the naming lints outright — `#![forbid(nonstandard_style)]`, a `[lints]` table, `-F` on the
//! command line — must still compile its jobs. The struct a job expands to is named after the snake_case function, and
//! the expansion used to `#[allow(non_camel_case_types)]` it: an enclosing `forbid` refuses that as `error[E0453]`, and
//! nothing written on the job could lower it. A forbidden lint *group* fared no better, as a `forbidden_lint_groups`
//! warning that `deny(warnings)` turns into an error.
#![forbid(nonstandard_style)]
#![deny(warnings)]

#[pgqueue::job]
async fn snake_case_job(_: ()) {}

#[pgqueue::cron("* * * * *")]
async fn snake_case_cron() {}

mod forbidding_module {
    #![forbid(non_camel_case_types)]

    #[pgqueue::job]
    pub async fn inner_job(_: ()) {}
}

fn main() {
    let _ = snake_case_job::job(());
    let _ = snake_case_cron::job();
    let _ = forbidding_module::inner_job::job(());
}

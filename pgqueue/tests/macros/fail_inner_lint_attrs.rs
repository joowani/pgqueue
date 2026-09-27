//! A lint level at the top of a handler's body applies to the job exactly as its outer spelling does, so it has to
//! reach the struct the job expands to. `missing_docs` fires on that struct and is at its default `allow` here, so
//! these jobs are refused only if their own inner `deny` or `forbid` gets there; on the hidden function it never fired.
//! An inner `#![deprecated]` deprecates the job's struct, so it is enqueueing the job that is refused — not the
//! expansion's own calls of the handler.
//!
//! A file of its own, because `missing_docs` is a late lint: rustc runs none of those once type checking has failed, so
//! in `fail.rs` it could never fire.
#![deny(deprecated)]

pub mod inner_levels_reach_the_struct {
    #[pgqueue::job]
    pub async fn denying(_: ()) {
        #![deny(missing_docs)]
    }

    #[pgqueue::cron("* * * * *")]
    pub async fn forbidding() {
        #![forbid(missing_docs)]
    }
}

#[pgqueue::job]
async fn retired(_: ()) {
    #![deprecated(note = "enqueue its replacement")]
}

fn main() {
    let _ = inner_levels_reach_the_struct::denying::job(());
    let _ = inner_levels_reach_the_struct::forbidding::job();
    let _ = retired::job(());
}

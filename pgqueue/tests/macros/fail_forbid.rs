//! `missing_docs` fires on the struct the expansion generates, and it is at its default `allow` here, so this job is
//! refused only if its own `forbid` reaches that struct — lowered to `deny`, since the expansion writes an `allow` of
//! its own there. `pass_hygiene.rs` can only show that the lowering compiles.
//!
//! A file of its own, because `missing_docs` is a late lint: rustc runs none of those once type checking has failed, so
//! in `fail.rs` it could never fire.

pub mod forbid_reaches_the_struct {
    #[pgqueue::job]
    #[forbid(missing_docs)]
    pub async fn undocumented(_: ()) {}
}

fn main() {
    let _ = forbid_reaches_the_struct::undocumented::job(());
}

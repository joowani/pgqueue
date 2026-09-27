//! `//!` inside a function documents that function exactly as `///` above it does, so a job documented that way must
//! hand the text to the public struct it expands to. Left on the hidden function, the struct was undocumented: rustdoc
//! showed the job without its docs, and `missing_docs` failed a handler that compiled as a plain `fn`.
#![deny(missing_docs)]

/// The control: the same documentation on a plain function.
pub async fn plain(_: ()) {}

#[pgqueue::job]
pub async fn documented_inside(_: ()) {
    //! Documented from inside its body.
}

fn main() {
    let _ = documented_inside::job(());
}

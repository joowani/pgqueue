//! rustc still expands an attribute on a function whose name its lexer rejected: it reports "identifiers cannot
//! contain emoji", recovers, and hands the attribute that name all the same. Its report has to be the only one. The
//! job's struct name used to be rebuilt from its string through `Ident::new_raw`, which panics on anything that is not
//! an identifier, so `custom attribute panicked` came first and named nothing the user wrote.
#![allow(uncommon_codepoints)]

#[pgqueue::job]
pub async fn send_🦀(_: ()) {}

#[pgqueue::cron("* * * * *")]
pub async fn tick_🦀() {}

fn main() {}

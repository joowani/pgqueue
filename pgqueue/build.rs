//! Makes the embedded migrations a build input.
//!
//! `sqlx::migrate!()` expands to one `include_str!` per file that existed when
//! the crate was last compiled, so an *added* migration is not a tracked input:
//! `cargo build` after writing `0002_*.sql` does nothing, and the binary keeps
//! the old migration set. Meanwhile `shell/scripts/migrate` reads the directory at
//! runtime and applies the new file, so the two disagree — and which way that
//! disagreement lands decides whether the next `cargo test` fails with
//! "previously applied but is missing in the resolved migrations" or passes
//! against the old schema while proving nothing about the new one. CI never
//! sees it, because CI builds cold; only a warm local tree does.
//!
//! `sqlx.toml` is the same input under another name. The `sqlx-toml` feature makes
//! `sqlx::migrate!()` read `create-schemas` and `table-name` from it at expansion time, while
//! `shell/scripts/migrate` passes `--config` and reads it at runtime — and naming any `rerun-if-changed`
//! at all replaces cargo's default "rerun on any change in the package", so leaving it untracked
//! left a warm tree validating the *old* history table against a database the migrator had just
//! written the new one to, which surfaces as "database is missing pgqueue migrations".
fn main() {
    println!("cargo:rerun-if-changed=migrations");
    println!("cargo:rerun-if-changed=sqlx.toml");
}

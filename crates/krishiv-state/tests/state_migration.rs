//! State schema migration tests (R16 S4.3).

// Integration-test crate: helpers run outside `#[test]` fns, so clippy.toml's
// `allow-unwrap-in-tests` does not reach them. A panic is the failure signal here.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::print_stdout,
    clippy::print_stderr
)]
use std::sync::Arc;

use krishiv_state::migration::StateMigrationRegistry;

#[test]
fn state_migration_chained_on_restore() {
    let mut reg = StateMigrationRegistry::new();
    reg.register(
        1,
        2,
        Arc::new(|b| {
            let mut v = b.to_vec();
            v.extend_from_slice(b"v2");
            Ok(v)
        }),
    );
    reg.register(
        2,
        3,
        Arc::new(|b| {
            let mut v = b.to_vec();
            v.extend_from_slice(b"v3");
            Ok(v)
        }),
    );
    let out = reg.migrate(1, 3, b"base").unwrap();
    assert_eq!(out, b"basev2v3");
}

#[test]
fn state_migration_missing_returns_error() {
    let reg = StateMigrationRegistry::new();
    assert!(reg.migrate(1, 2, b"x").is_err());
}

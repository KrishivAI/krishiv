#![forbid(unsafe_code)]
//! Upgrade compatibility tests for persisted Krishiv metadata families.
//!
//! Each test simulates writing metadata at schema_version N and reading
//! it with the current reader to verify forward-compatible decode.

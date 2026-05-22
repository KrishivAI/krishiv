#![forbid(unsafe_code)]

//! Policy-enforcing SQL engine: wraps [`krishiv_sql::SqlEngine`] with
//! authentication and column-masking.

pub use krishiv_sql::PolicyEnforcingSqlEngine;

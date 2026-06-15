#![forbid(unsafe_code)]
//! JDBC/ODBC-oriented SQL gateway facade.
//!
//! This crate is intentionally separate from [`krishiv_api`] so wire-protocol
//! drivers can evolve on their own release cadence while sharing the same
//! session semantics and SQLSTATE error mapping.

mod error;
mod session;

pub use error::{GatewayError, GatewayResult};
pub use session::GatewaySession;

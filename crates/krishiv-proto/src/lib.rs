#![forbid(unsafe_code)]

//! R2/R3 control-plane contracts for Krishiv.

mod ids;
mod domain;
pub mod wire;

pub use ids::*;
pub use domain::*;

#[cfg(test)]
mod proto_tests;

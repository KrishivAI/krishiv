#![forbid(unsafe_code)]
//! In-process SQLSTATE-mapping facade over [`BlockingSession`].
//!
//! ## API-12: This is NOT a JDBC/ODBC wire protocol server
//!
//! Despite the historical name "JDBC/ODBC gateway", this crate does **not**
//! serve a wire protocol. It is a library that wraps
//! `krishiv_api::blocking::BlockingSession` with SQLSTATE error mapping,
//! connection pooling, and a gateway-style API surface.
//!
//! **External JDBC/ODBC drivers must connect via Arrow Flight SQL**
//! (`krishiv_flight_sql`), which is the actual wire-protocol ingress.
//!
//! ## Architecture
//!
//! ```text
//! JDBC Driver (Arrow Flight SQL)
//!   → Flight SQL wire protocol (DoGet / DoPut)
//!   → krishiv-flight-sql (tonic server)
//!   → krishiv-sql-gateway (SQLSTATE mapping, connection pooling)
//!   → krishiv-api (Session, DataFrame)
//!   → krishiv-sql / DataFusion (planning + execution)
//! ```
//!
//! ## JDBC Connection String
//!
//! JDBC drivers use the Arrow Flight SQL JDBC driver:
//!
//! ```text
//! jdbc:arrow-flight-sql://localhost:50051?useEncryption=false
//! ```
//!
//! For Flight SQL with TLS:
//!
//! ```text
//! jdbc:arrow-flight-sql://coordinator.example.com:50051?useEncryption=true
//! ```
//!
//! ## Python (ADBC / Flight SQL)
//!
//! ```python
//! from adbc_driver_flightsql import dbapi
//! conn = dbapi.connect("grpc://localhost:50051")
//! cursor = conn.cursor()
//! cursor.execute("SELECT * FROM my_table")
//! ```
//!
//! ## dbt Integration
//!
//! Use `dbt-flight-sql` adapter with a profiles.yml:
//!
//! ```yaml
//! krishiv:
//!   target: dev
//!   outputs:
//!     dev:
//!       type: flight_sql
//!       host: localhost
//!       port: 50051
//!       use_encryption: false
//! ```
//!
//! ## Programmatic (Rust)
//!
//! This crate can be used as a library for tools that embed Krishiv:
//!
//! ```rust,ignore
//! use krishiv_sql_gateway::{GatewaySession, GatewayQueryResult};
//!
//! let session = GatewaySession::embedded()?;
//! let result = session.execute_sql("SELECT 42 AS answer")?;
//! println!("{}", result.result.pretty_table().unwrap());
//! ```

mod error;
mod session;

pub use error::{GatewayError, GatewayResult};
pub use session::{GatewayQueryResult, GatewaySession, PooledSession, SessionPool};

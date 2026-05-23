#![forbid(unsafe_code)]

//! Spark Connect gRPC server and plan translation (R15 S3).

mod matrix;
mod server;
mod translate;

pub use matrix::SparkConnectCompatMatrix;
pub use server::{serve_spark_connect, SparkConnectConfig, SparkConnectServiceImpl};
pub use translate::{relation_to_sql, SparkTranslateError};

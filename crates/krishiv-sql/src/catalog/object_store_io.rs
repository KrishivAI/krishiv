//! Object-store-backed Iceberg [`Storage`] for shared warehouses (S3 / MinIO).
//!
//! The implementation now lives in `krishiv-connectors`
//! (`krishiv_connectors::lakehouse::object_store_io`) so the streaming Iceberg
//! sink (`iceberg_native.rs`, in that crate) can share the *same*
//! `#[typetag::serde]`-registered `Storage` bridge as the batch/REST-catalog
//! path here. Two identically-tagged impls linked into one binary would collide
//! at typetag registration, so there is exactly one — this module re-exports it.
//!
//! Gated on the features that pull `krishiv-connectors/iceberg` (which is where
//! the bridge is compiled): `iceberg`, `postgres-catalog`, `rest-catalog`,
//! `iceberg-datafusion`. `local-catalog` uses `LocalFsStorageFactory` directly
//! and never references this bridge.

#![cfg(any(
    feature = "iceberg",
    feature = "postgres-catalog",
    feature = "rest-catalog",
    feature = "iceberg-datafusion"
))]

pub use krishiv_connectors::lakehouse::object_store_io::{KrishivStorage, KrishivStorageFactory};

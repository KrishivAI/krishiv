//! Krishiv engine-core: the three-engine spine.
//!
//! Krishiv runs **three distinct compute engines** that share one execution
//! contract:
//!
//! - [`EngineKind::Batch`] — bounded, run-to-completion SQL (Spark-style).
//! - [`EngineKind::Incremental`] — change-driven incremental view maintenance
//!   (Feldera/DBSP-style).
//! - [`EngineKind::Streaming`] — event-time, watermark-driven streaming with
//!   keyed state and checkpoints (Flink-style).
//!
//! The **engine** (compute model), the **placement** (embedded / single-node /
//! distributed), and the **API surface** (SQL / Python / Rust) are three
//! independent axes. Every front-end compiles to a single [`CompiledJob`]; a
//! [`ComputeEngine`] runs it using placement-provided services carried by
//! [`EngineRuntime`]. The same engine code therefore runs unchanged from an
//! embedded in-process call to a distributed cluster — only the injected
//! services differ.
//!
//! This crate is the bottom of the engine stack: it depends only on
//! `krishiv-common`, `krishiv-proto`, and Arrow, so every engine and front-end
//! crate can depend on it without a cycle.

mod changelog;
pub mod consolidate;
pub mod durable;
mod engine;
mod error;
mod job;
mod kind;
pub mod mem;
mod runtime;
pub mod upsert;

pub use changelog::{ChangelogBatch, RowKind};
pub use consolidate::ConsolidatingSinkProvider;
pub use durable::DurableCheckpointService;
pub use engine::{ComputeEngine, JobHandle, JobStatus};
pub use error::{EngineError, EngineResult};
pub use job::{CompiledJob, DeliveryContract, SinkSpec, SourceSpec, StatePolicy};
pub use kind::{EngineKind, UnknownEngine};
pub use runtime::{
    BatchOutputStream, CheckpointPayload, CheckpointService, Clock, DataNotify, EngineRuntime,
    KeyedState, Placement, QueryExecutor, ShuffleService, SinkProvider, SinkWriter, SourceProvider,
    SourceReader, StateBackendFactory, SystemClock, decode_batch_ipc, encode_batch_ipc,
};
pub use upsert::UpsertSinkProvider;

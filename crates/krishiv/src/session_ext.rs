use std::path::Path;

use krishiv_api::{Session, StreamBatch};

use crate::Result;
use crate::relation::{EmitMode, Relation, RelationKind, StreamingChain};

/// Extension trait that adds unified batch+streaming entry points to [`Session`].
///
/// Import this trait to call `.relation()`, `.from_parquet()`, `.from_source()`,
/// and `.from_bounded_stream()` on a `Session`.
#[allow(clippy::wrong_self_convention)]
pub trait SessionExt {
    /// Create a batch [`Relation`] from an arbitrary SQL query.
    fn relation(&self, query: impl AsRef<str>) -> Result<Relation>;

    /// Create a batch [`Relation`] by reading a local Parquet file.
    fn from_parquet(&self, path: impl AsRef<Path>) -> Result<Relation>;

    /// Create an unbounded streaming [`Relation`] backed by the named source.
    fn from_source(&self, name: impl Into<String>) -> Relation;

    /// Create a bounded streaming [`Relation`] from in-memory batches.
    fn from_bounded_stream(&self, name: impl Into<String>, batches: Vec<StreamBatch>) -> Relation;
}

impl SessionExt for Session {
    fn relation(&self, query: impl AsRef<str>) -> Result<Relation> {
        let df = self.sql(query.as_ref())?;
        Ok(Relation {
            kind: RelationKind::Batch(df),
        })
    }

    fn from_parquet(&self, path: impl AsRef<Path>) -> Result<Relation> {
        let df = self.read_parquet(path)?;
        Ok(Relation {
            kind: RelationKind::Batch(df),
        })
    }

    fn from_source(&self, name: impl Into<String>) -> Relation {
        let name = name.into();
        Relation {
            kind: RelationKind::Stream(StreamingChain {
                session: self.clone(),
                source_name: name,
                batches: Vec::new(),
                bounded: false,
                key_column: None,
                event_time_column: None,
                watermark_lag_ms: 0,
                window: None,
                emit_mode: EmitMode::default(),
                agg_exprs: None,
            }),
        }
    }

    fn from_bounded_stream(&self, name: impl Into<String>, batches: Vec<StreamBatch>) -> Relation {
        let name = name.into();
        Relation {
            kind: RelationKind::Stream(StreamingChain {
                session: self.clone(),
                source_name: name,
                batches,
                bounded: true,
                key_column: None,
                event_time_column: None,
                watermark_lag_ms: 0,
                window: None,
                emit_mode: EmitMode::default(),
                agg_exprs: None,
            }),
        }
    }
}

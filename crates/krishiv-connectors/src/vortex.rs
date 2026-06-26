#![deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
#![cfg(feature = "vortex")]
//! Vortex columnar format connector (feature = "vortex").
//!
//! [Vortex](https://github.com/spiraldb/vortex) is a next-generation columnar
//! format with zero-copy Arrow interoperability, advanced compression codecs
//! (BtrBlocks, Alp, Dictionary, …), and native filter/projection pushdown.
//!
//! # API
//!
//! - [`read_vortex_file`]  — read a `.vortex` file into Arrow [`RecordBatch`]es
//! - [`write_vortex_file`] — write Arrow [`RecordBatch`]es to a `.vortex` file
//!
//! # Feature gate
//!
//! ```toml
//! krishiv-connectors = { features = ["vortex"] }
//! ```
//!
//! # Example
//!
//! ```no_run
//! use std::path::Path;
//! use arrow::record_batch::RecordBatch;
//! use krishiv_connectors::vortex::{read_vortex_file, write_vortex_file};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let path = Path::new("data.vortex");
//! let batches: Vec<RecordBatch> = read_vortex_file(path).await?;
//! write_vortex_file(path, &batches).await?;
//! # Ok(())
//! # }
//! ```

use std::path::Path;
use std::sync::Arc;

use arrow::array::{ArrayRef as ArrowArrayRef, StructArray as ArrowStructArray};
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use thiserror::Error;
use vortex::VortexSessionDefault;
use vortex::array::arrow::{ArrowSessionExt, FromArrowArray};
use vortex::array::dtype::arrow::FromArrowType;
use vortex::array::stream::ArrayStreamAdapter;
use vortex::array::{ArrayRef, VortexSessionExecute};
use vortex::dtype::DType;
use vortex::file::{OpenOptionsSessionExt, WriteOptionsSessionExt};
use vortex::session::VortexSession;

/// Errors produced by the Vortex connector.
#[derive(Debug, Error)]
pub enum VortexError {
    #[error("Vortex error: {0}")]
    Vortex(#[from] vortex::error::VortexError),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("empty batch list: schema cannot be inferred for writing")]
    EmptyBatches,
}

/// Read a `.vortex` file and return its contents as Arrow [`RecordBatch`]es.
///
/// Uses the default Vortex session with all standard encodings registered.
/// The file is scanned in row-group chunks; each chunk is converted to an
/// Arrow `RecordBatch` via the Vortex Arrow execution kernel.
pub async fn read_vortex_file(path: &Path) -> Result<Vec<RecordBatch>, VortexError> {
    let session = VortexSession::default();
    let file = session.open_options().open_path(path).await?;
    let mut stream = file.scan()?.into_array_stream()?;

    let mut batches = Vec::new();
    while let Some(result) = stream.next().await {
        let array = result?;
        let batch = array_to_record_batch(array, &session)?;
        batches.push(batch);
    }
    Ok(batches)
}

/// Write Arrow [`RecordBatch`]es to a new `.vortex` file.
///
/// All batches must share the same schema.  The file is created (truncating
/// any existing content). Uses the default Vortex session and the standard
/// table layout strategy.
///
/// # Errors
///
/// Returns [`VortexError::EmptyBatches`] if `batches` is empty.
pub async fn write_vortex_file(path: &Path, batches: &[RecordBatch]) -> Result<(), VortexError> {
    let first = batches.first().ok_or(VortexError::EmptyBatches)?;
    let dtype = DType::from_arrow(first.schema());

    let arrays: Result<Vec<ArrayRef>, VortexError> = batches
        .iter()
        .map(|b| ArrayRef::from_arrow(b, false).map_err(VortexError::Vortex))
        .collect();
    let arrays = arrays?;

    let stream = ArrayStreamAdapter::new(
        dtype,
        futures::stream::iter(arrays.into_iter().map(Ok::<_, vortex::error::VortexError>)),
    );

    let file = tokio::fs::File::create(path).await?;
    VortexSession::default()
        .write_options()
        .write(file, stream)
        .await?;
    Ok(())
}

/// Convert a Vortex `ArrayRef` (a top-level struct from a file scan) to a `RecordBatch`.
fn array_to_record_batch(
    array: ArrayRef,
    session: &VortexSession,
) -> Result<RecordBatch, VortexError> {
    let mut ctx = session.create_execution_ctx();
    let arrow: ArrowArrayRef = session.arrow().execute_arrow(array, None, &mut ctx)?;
    let struct_arr = arrow
        .as_any()
        .downcast_ref::<ArrowStructArray>()
        .ok_or_else(|| {
            arrow::error::ArrowError::CastError(format!(
                "expected StructArray from vortex scan, got {:?}",
                arrow.data_type()
            ))
        })?;
    Ok(RecordBatch::from(struct_arr))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    fn make_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrowArrayRef,
                Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrowArrayRef,
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn round_trip_vortex_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.vortex");

        write_vortex_file(&path, &[make_batch()]).await.unwrap();

        let batches = read_vortex_file(&path).await.unwrap();
        assert!(!batches.is_empty(), "must read at least one batch");

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3, "expected 3 rows, got {total_rows}");
    }

    #[tokio::test]
    async fn write_empty_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.vortex");
        let result = write_vortex_file(&path, &[]).await;
        assert!(matches!(result, Err(VortexError::EmptyBatches)));
    }
}

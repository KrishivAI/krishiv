#![forbid(unsafe_code)]

//! Spill-to-disk support for memory-bounded operators.
//!
//! Operators that hit their [`krishiv_common::MemoryBudget`] write Arrow IPC
//! files into the OS temp directory via [`SpillFile`].  Files are removed on
//! drop, so abandoning an operator never leaks disk space.

use std::fs::File;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow::datatypes::Schema;
use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;

use crate::{ExecError, ExecResult};

/// Monotonic counter so concurrent operators in one process never collide on
/// spill file names.
static SPILL_SEQ: AtomicU64 = AtomicU64::new(0);

/// A temporary file holding Arrow IPC batches; removed from disk on drop.
pub(crate) struct SpillFile {
    path: PathBuf,
}

impl SpillFile {
    /// Write `batches` to a fresh spill file in the OS temp directory.
    ///
    /// Returns the file guard plus the number of bytes written to disk.  The
    /// guard removes the file when dropped, including on a failed write.
    pub(crate) fn write(
        prefix: &str,
        schema: &Schema,
        batches: &[RecordBatch],
    ) -> ExecResult<(Self, u64)> {
        let seq = SPILL_SEQ.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "krishiv-spill-{prefix}-{pid}-{seq}.arrow",
            pid = std::process::id()
        ));
        let file = File::create(&path)
            .map_err(|e| ExecError::Spill(format!("create {}: {e}", path.display())))?;
        // Construct the guard before writing so a failed write still removes
        // the partially written file.
        let spill = Self { path };
        let mut writer = FileWriter::try_new(file, schema).map_err(|e| {
            ExecError::Spill(format!("open ipc writer {}: {e}", spill.path.display()))
        })?;
        for batch in batches {
            writer
                .write(batch)
                .map_err(|e| ExecError::Spill(format!("write {}: {e}", spill.path.display())))?;
        }
        writer
            .finish()
            .map_err(|e| ExecError::Spill(format!("finish {}: {e}", spill.path.display())))?;
        drop(writer);
        let bytes = std::fs::metadata(&spill.path)
            .map_err(|e| ExecError::Spill(format!("stat {}: {e}", spill.path.display())))?
            .len();
        Ok((spill, bytes))
    }

    /// Read every batch back from this spill file.
    pub(crate) fn read(&self) -> ExecResult<Vec<RecordBatch>> {
        let file = File::open(&self.path)
            .map_err(|e| ExecError::Spill(format!("open {}: {e}", self.path.display())))?;
        let reader = FileReader::try_new(file, None).map_err(|e| {
            ExecError::Spill(format!("open ipc reader {}: {e}", self.path.display()))
        })?;
        reader
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| ExecError::Spill(format!("read {}: {e}", self.path.display())))
    }

    /// Path of the spill file on disk (test inspection only).
    #[cfg(test)]
    pub(crate) fn path(&self) -> &std::path::Path {
        &self.path
    }
}

impl Drop for SpillFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field};

    use super::*;

    fn batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))]).unwrap()
    }

    #[test]
    fn write_read_roundtrip_and_cleanup() {
        let b = batch();
        let (spill, bytes) = SpillFile::write("test", &b.schema(), &[b.clone()]).unwrap();
        assert!(bytes > 0);
        let path = spill.path().to_path_buf();
        assert!(path.exists());
        let back = spill.read().unwrap();
        assert_eq!(back.len(), 1);
        assert_eq!(back[0], b);
        drop(spill);
        assert!(!path.exists());
    }
}

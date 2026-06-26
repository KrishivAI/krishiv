//! Hive-style partition discovery and column injection for file/object sources.
//!
//! Parquet datasets are often laid out on disk or in object stores using the
//! Hive partition convention:
//!
//!   `root/year=2024/month=01/day=01/part-0.parquet`
//!
//! This module provides:
//! - [`list_parquet_files`] — sorted list of `.parquet` files under a local
//!   directory (optionally recursive).
//! - [`discover_hive_partitions`] — parse `key=value` segments out of the
//!   relative path between `root` and `file`.
//! - [`inject_partition_columns`] — append Hive partition columns (as
//!   `Utf8` arrays) to an Arrow `RecordBatch`.

use std::path::{Path, PathBuf};

use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::error::{ConnectorError, ConnectorResult};

// ---------------------------------------------------------------------------
// File listing
// ---------------------------------------------------------------------------

/// Return a sorted list of all `.parquet` files found under `dir`.
///
/// When `recursive` is `true` the entire sub-tree is scanned; otherwise only
/// the immediate entries of `dir` are returned.
pub fn list_parquet_files(dir: &Path, recursive: bool) -> ConnectorResult<Vec<PathBuf>> {
    if !dir.is_dir() {
        return Err(ConnectorError::Parquet(format!(
            "partition listing: '{}' is not a directory",
            dir.display()
        )));
    }
    let mut files = Vec::new();
    collect_parquet_files(dir, recursive, &mut files)?;
    files.sort();
    Ok(files)
}

fn collect_parquet_files(
    dir: &Path,
    recursive: bool,
    out: &mut Vec<PathBuf>,
) -> ConnectorResult<()> {
    let entries = std::fs::read_dir(dir).map_err(ConnectorError::Io)?;
    let mut subdirs = Vec::new();
    for entry in entries {
        let entry = entry.map_err(ConnectorError::Io)?;
        let path = entry.path();
        if path.is_dir() {
            if recursive {
                subdirs.push(path);
            }
        } else if path.extension().and_then(|e| e.to_str()) == Some("parquet") {
            out.push(path);
        }
    }
    // Sort sub-directories for deterministic traversal order.
    subdirs.sort();
    for subdir in subdirs {
        collect_parquet_files(&subdir, recursive, out)?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Partition discovery
// ---------------------------------------------------------------------------

/// Parse Hive-style `key=value` segments from the path between `root` and
/// `file`, returning them as `(key, value)` pairs in directory order.
///
/// Non-conforming path segments (those without exactly one `=`) are silently
/// skipped so that plain directory structures (`/data/2024/01/`) do not cause
/// errors — they simply produce no partition columns.
pub fn discover_hive_partitions(root: &Path, file: &Path) -> Vec<(String, String)> {
    let relative = match file.strip_prefix(root) {
        Ok(r) => r,
        Err(_) => return vec![],
    };
    let mut parts = Vec::new();
    // Walk all components except the last (the filename itself).
    let components: Vec<_> = relative.components().collect();
    for component in components.iter().take(components.len().saturating_sub(1)) {
        let segment = component.as_os_str().to_string_lossy();
        if let Some((key, value)) = segment.split_once('=')
            && !key.is_empty()
            && !value.is_empty()
        {
            parts.push((key.to_owned(), value.to_owned()));
        }
    }
    parts
}

// ---------------------------------------------------------------------------
// Column injection
// ---------------------------------------------------------------------------

/// Append Hive partition columns to `batch` as `Utf8` (string) columns.
///
/// Each `(key, value)` pair in `partitions` becomes a new column at the right
/// of the schema.  All rows in the batch receive the same constant value.
/// Columns whose names already exist in `batch.schema()` are silently skipped
/// so the caller does not need to filter first.
pub fn inject_partition_columns(
    batch: RecordBatch,
    partitions: &[(String, String)],
) -> ConnectorResult<RecordBatch> {
    if partitions.is_empty() {
        return Ok(batch);
    }
    let schema = batch.schema();
    let existing_names: std::collections::HashSet<&str> =
        schema.fields().iter().map(|f| f.name().as_str()).collect();

    let mut new_fields: Vec<Field> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    let mut new_columns: Vec<arrow::array::ArrayRef> = batch.columns().to_vec();

    for (key, value) in partitions {
        if existing_names.contains(key.as_str()) {
            continue;
        }
        let arr = StringArray::from(vec![value.as_str(); batch.num_rows()]);
        new_fields.push(Field::new(key, DataType::Utf8, false));
        new_columns.push(std::sync::Arc::new(arr));
    }

    let schema = std::sync::Arc::new(Schema::new(new_fields));
    RecordBatch::try_new(schema, new_columns).map_err(|e| ConnectorError::Parquet(e.to_string()))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray as ArrowStringArray};
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    fn minimal_batch(n: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(
                (0..n).map(|i| i as i32).collect::<Vec<_>>(),
            ))],
        )
        .unwrap()
    }

    #[test]
    fn discover_hive_partitions_parses_key_value_segments() {
        let root = Path::new("/data");
        let file = Path::new("/data/year=2024/month=01/day=15/part-0.parquet");
        let parts = discover_hive_partitions(root, file);
        assert_eq!(parts.len(), 3);
        assert_eq!(parts[0], ("year".into(), "2024".into()));
        assert_eq!(parts[1], ("month".into(), "01".into()));
        assert_eq!(parts[2], ("day".into(), "15".into()));
    }

    #[test]
    fn discover_hive_partitions_skips_plain_segments() {
        let root = Path::new("/data");
        let file = Path::new("/data/2024/01/part-0.parquet");
        let parts = discover_hive_partitions(root, file);
        assert!(
            parts.is_empty(),
            "plain segments should not produce partition columns"
        );
    }

    #[test]
    fn inject_partition_columns_appends_utf8_columns() {
        let batch = minimal_batch(3);
        let parts = vec![
            ("year".into(), "2024".into()),
            ("month".into(), "01".into()),
        ];
        let out = inject_partition_columns(batch, &parts).unwrap();
        assert_eq!(out.num_columns(), 3);
        assert_eq!(out.schema().field(1).name(), "year");
        assert_eq!(out.schema().field(2).name(), "month");
        let year_col = out
            .column(1)
            .as_any()
            .downcast_ref::<ArrowStringArray>()
            .unwrap();
        assert_eq!(year_col.value(0), "2024");
        assert_eq!(year_col.value(2), "2024");
    }

    #[test]
    fn inject_partition_columns_skips_existing_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("year", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(ArrowStringArray::from(vec!["2023"])),
            ],
        )
        .unwrap();
        let parts = vec![("year".into(), "2024".into())];
        let out = inject_partition_columns(batch, &parts).unwrap();
        assert_eq!(
            out.num_columns(),
            2,
            "existing 'year' column must not be duplicated"
        );
        let year_col = out
            .column(1)
            .as_any()
            .downcast_ref::<ArrowStringArray>()
            .unwrap();
        assert_eq!(
            year_col.value(0),
            "2023",
            "original value must be preserved"
        );
    }

    #[test]
    fn list_parquet_files_finds_files_in_flat_dir() {
        let dir = tempfile::tempdir().unwrap();
        let p1 = dir.path().join("a.parquet");
        let p2 = dir.path().join("b.parquet");
        let _other = dir.path().join("c.csv");
        std::fs::write(&p1, b"dummy").unwrap();
        std::fs::write(&p2, b"dummy").unwrap();
        std::fs::write(_other, b"dummy").unwrap();

        let files = list_parquet_files(dir.path(), false).unwrap();
        assert_eq!(files.len(), 2);
        assert!(files.iter().any(|f| f.ends_with("a.parquet")));
        assert!(files.iter().any(|f| f.ends_with("b.parquet")));
    }

    #[test]
    fn list_parquet_files_recursive_finds_nested_files() {
        let dir = tempfile::tempdir().unwrap();
        let sub = dir.path().join("year=2024").join("month=01");
        std::fs::create_dir_all(&sub).unwrap();
        let p1 = dir.path().join("root.parquet");
        let p2 = sub.join("part-0.parquet");
        std::fs::write(&p1, b"dummy").unwrap();
        std::fs::write(&p2, b"dummy").unwrap();

        let files = list_parquet_files(dir.path(), true).unwrap();
        assert_eq!(files.len(), 2);
    }

    #[test]
    fn list_parquet_files_non_recursive_ignores_subdirs() {
        let dir = tempfile::tempdir().unwrap();
        let sub = dir.path().join("year=2024");
        std::fs::create_dir_all(&sub).unwrap();
        let p_nested = sub.join("part-0.parquet");
        let p_root = dir.path().join("root.parquet");
        std::fs::write(&p_nested, b"dummy").unwrap();
        std::fs::write(&p_root, b"dummy").unwrap();

        let files = list_parquet_files(dir.path(), false).unwrap();
        assert_eq!(files.len(), 1);
        assert!(files[0].ends_with("root.parquet"));
    }
}

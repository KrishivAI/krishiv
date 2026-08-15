//! Apache Delta Lake read/write (local `_delta_log` + Parquet, R18 S1).

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use super::local_delta;
use super::{LakehouseError, LakehouseResult};

/// Write mode for Delta tables.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeltaWriteMode {
    Append,
    Overwrite,
    Merge,
}

/// Handle to a local Delta table directory.
#[derive(Clone, Debug)]
pub struct DeltaTableHandle {
    path: String,
    version: Option<i64>,
}

impl DeltaTableHandle {
    pub async fn open(path: impl Into<String>, version: Option<i64>) -> LakehouseResult<Self> {
        let path = path.into();
        // Validate by resolving the snapshot's file list from the log only —
        // reading the actual data here materialized the whole table on every
        // open (Phase 52 #194) while surfacing the same missing/corrupt-log
        // errors.
        let _ = local_delta::list_table_data_files(&path, version.map(|v| v as u64))?;
        Ok(Self { path, version })
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn version(&self) -> Option<i64> {
        self.version
    }

    pub async fn schema(&self) -> LakehouseResult<SchemaRef> {
        local_delta::table_schema(&self.path, self.version.map(|v| v as u64))
    }

    pub async fn scan_batches(&self) -> LakehouseResult<Vec<RecordBatch>> {
        local_delta::read_table(&self.path, self.version.map(|v| v as u64))
    }

    pub async fn with_version(self, version: i64) -> LakehouseResult<Self> {
        Self::open(&self.path, Some(version)).await
    }
}

pub async fn write_delta(
    path: impl Into<String>,
    batches: Vec<RecordBatch>,
    mode: DeltaWriteMode,
    _schema_evolution: bool,
) -> LakehouseResult<()> {
    let path = path.into();
    if batches.is_empty() {
        return Ok(());
    }
    // Merge is a keyed operation; silently treating it as Overwrite would
    // replace the whole table instead of merging rows.
    if matches!(mode, DeltaWriteMode::Merge) {
        return Err(LakehouseError::Io(
            "merge mode requires merge_delta with key columns; write_delta only supports append/overwrite".to_string(),
        ));
    }
    let overwrite = matches!(mode, DeltaWriteMode::Overwrite);
    tokio::task::spawn_blocking(move || local_delta::write_table(&path, batches, overwrite))
        .await
        .map_err(|e| LakehouseError::Io(e.to_string()))?
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MergeDeltaResult {
    pub rows_inserted: u64,
    pub rows_updated: u64,
    pub rows_deleted: u64,
}

pub async fn merge_delta(
    target_path: &str,
    source_batches: Vec<RecordBatch>,
    merge_key: &str,
    when_matched_update: bool,
    when_not_matched_insert: bool,
) -> LakehouseResult<MergeDeltaResult> {
    use std::collections::HashSet;

    if source_batches.is_empty() {
        return Ok(MergeDeltaResult::default());
    }
    let target_batches = local_delta::read_table(target_path, None)?;
    let target = concat_batches(&target_batches)?;
    let source = concat_batches(&source_batches)?;
    let key = merge_key.to_string();

    let source_col = source
        .column_by_name(&key)
        .ok_or_else(|| LakehouseError::Io(format!("merge key {key} missing in source")))?;
    let target_col = target
        .column_by_name(&key)
        .ok_or_else(|| LakehouseError::Io(format!("merge key {key} missing in target")))?;

    // Build a set of typed keys from source rows so we can classify each
    // target row as matched or unmatched.  Keys are type-prefixed to prevent
    // cross-type false matches (e.g. Int64(1) must not match String("1")).
    let source_keys: HashSet<String> = keys_set(source_col.as_ref())?;

    // Keep target rows whose key does NOT appear in source. When matched
    // rows are not being updated (insert-only merge), matched target rows
    // must survive unchanged, so every target row is kept.
    let keep_indices: Vec<u32> = if when_matched_update {
        target_keys_indices(target_col.as_ref(), &source_keys)?
    } else {
        (0..target.num_rows() as u32).collect()
    };

    let mut merged_batches = Vec::new();
    if !keep_indices.is_empty() {
        merged_batches.push(take_rows(&target, &keep_indices)?);
    }

    // Split source rows into updates (key matched target) and inserts (key
    // did not match target).  Build the target-key set once for O(1) checks.
    let target_key_set: HashSet<String> = keys_set(target_col.as_ref())?;
    let (update_indices, insert_indices): (Vec<u32>, Vec<u32>) =
        (0..source.num_rows()).map(|i| i as u32).partition(|&i| {
            typed_key(source_col.as_ref(), i as usize)
                .unwrap_or(None)
                .is_some_and(|k| target_key_set.contains(&k))
        });

    let rows_updated = if when_matched_update && !update_indices.is_empty() {
        merged_batches.push(take_rows(&source, &update_indices)?);
        update_indices.len() as u64
    } else {
        0
    };

    let rows_inserted = if when_not_matched_insert && !insert_indices.is_empty() {
        merged_batches.push(take_rows(&source, &insert_indices)?);
        insert_indices.len() as u64
    } else {
        0
    };

    let merged = concat_batches(&merged_batches)?;
    write_delta(target_path, vec![merged], DeltaWriteMode::Overwrite, false).await?;
    Ok(MergeDeltaResult {
        rows_inserted,
        rows_updated,
        rows_deleted: 0,
    })
}

// ── ObjectStore-backed Delta reader ────────────────────────────────────────

/// ObjectStore-backed Delta Lake reader.
///
/// Reads the `_delta_log/` directory from any `ObjectStore` implementation
/// (S3, GCS, Azure, or in-memory for tests). Compatible with tables written
/// by [`write_delta`] via a local filesystem layout or any other Delta writer
/// that produces the standard `_delta_log/*.json` structure.
pub struct DeltaObjectStoreReader {
    store: std::sync::Arc<dyn object_store::ObjectStore>,
    prefix: String,
}

impl DeltaObjectStoreReader {
    /// Create a reader targeting `prefix` within `store`.
    ///
    /// `prefix` is the root of the Delta table (the directory that contains
    /// `_delta_log/`).
    pub fn new(
        store: std::sync::Arc<dyn object_store::ObjectStore>,
        prefix: impl Into<String>,
    ) -> Self {
        Self {
            store,
            prefix: prefix.into(),
        }
    }

    /// List available Delta log versions sorted ascending.
    async fn list_versions(&self) -> LakehouseResult<Vec<u64>> {
        use futures::StreamExt as _;
        let log_prefix = object_store::path::Path::from(format!("{}/_delta_log", self.prefix));
        let mut stream = self.store.list(Some(&log_prefix));
        let mut versions = Vec::new();
        while let Some(meta) = stream.next().await {
            let meta = meta.map_err(|e| LakehouseError::Io(e.to_string()))?;
            let name = meta.location.filename().unwrap_or("").to_string();
            if name.ends_with(".json")
                && let Ok(v) = name.trim_end_matches(".json").parse::<u64>()
            {
                versions.push(v);
            }
        }
        versions.sort_unstable();
        Ok(versions)
    }

    /// Read the add and remove file paths from a single Delta log entry.
    ///
    /// Returns `(add_paths, remove_paths)`. Callers must subtract removes from
    /// the accumulated add set to get the correct snapshot — ignoring remove
    /// entries causes double-counting after `DeltaWriteMode::Overwrite`.
    async fn parquet_paths_from_log_entry(
        &self,
        version: u64,
    ) -> LakehouseResult<(Vec<String>, Vec<String>)> {
        let log_path = object_store::path::Path::from(format!(
            "{}/_delta_log/{:020}.json",
            self.prefix, version
        ));
        let result = self
            .store
            .get_opts(&log_path, Default::default())
            .await
            .map_err(|e| LakehouseError::Io(e.to_string()))?;
        let bytes = result
            .bytes()
            .await
            .map_err(|e| LakehouseError::Io(e.to_string()))?;
        let mut add_paths = Vec::new();
        let mut remove_paths = Vec::new();
        for line in std::str::from_utf8(&bytes)
            .map_err(|e| LakehouseError::Io(e.to_string()))?
            .lines()
        {
            if line.trim().is_empty() {
                continue;
            }
            let v: serde_json::Value =
                serde_json::from_str(line).map_err(|e| LakehouseError::Io(e.to_string()))?;
            // Delta add action: {"add": {"path": "part-xxx.parquet", ...}}
            if let Some(path) = v
                .get("add")
                .and_then(|a| a.get("path"))
                .and_then(|p| p.as_str())
            {
                add_paths.push(path.to_string());
            }
            // Delta remove action: {"remove": {"path": "part-xxx.parquet", ...}}
            if let Some(path) = v
                .get("remove")
                .and_then(|r| r.get("path"))
                .and_then(|p| p.as_str())
            {
                remove_paths.push(path.to_string());
            }
        }
        Ok((add_paths, remove_paths))
    }

    /// Scan all Parquet files in the latest Delta snapshot and return all record batches.
    ///
    /// Correctly handles remove (tombstone) actions: files removed by an overwrite or
    /// delete operation are excluded from the result even if they appeared in an earlier
    /// add action.
    pub async fn scan_batches(&self) -> LakehouseResult<Vec<arrow::record_batch::RecordBatch>> {
        let versions = self.list_versions().await?;
        if versions.is_empty() {
            return Ok(Vec::new());
        }

        // Replay add/remove actions in version order so a path removed in one
        // version and re-added in a later version stays readable (matches the
        // local `_delta_log` replay in local_delta::active_data_file_paths).
        let mut active: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        for version in &versions {
            let (add_paths, remove_paths) = self.parquet_paths_from_log_entry(*version).await?;
            for p in remove_paths {
                active.remove(&p);
            }
            for p in add_paths {
                active.insert(p);
            }
        }

        let mut out = Vec::new();
        for rel_path in &active {
            let obj_path = object_store::path::Path::from(format!("{}/{}", self.prefix, rel_path));
            // A get failure on an active (non-tombstoned) file means silent
            // row loss if swallowed — propagate it.
            let get_result = self
                .store
                .get_opts(&obj_path, Default::default())
                .await
                .map_err(|e| LakehouseError::Io(e.to_string()))?;
            let data = get_result
                .bytes()
                .await
                .map_err(|e| LakehouseError::Io(e.to_string()))?;
            use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
            let reader = ParquetRecordBatchReader::try_new(data, 1024)
                .map_err(|e| LakehouseError::Io(e.to_string()))?;
            for batch in reader {
                out.push(batch.map_err(|e| LakehouseError::Io(e.to_string()))?);
            }
        }
        Ok(out)
    }
}

use arrow::array::Array;
use arrow::util::display::{ArrayFormatter, FormatOptions};

fn concat_batches(batches: &[RecordBatch]) -> LakehouseResult<RecordBatch> {
    if batches.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::new(
            arrow::datatypes::Schema::empty(),
        )));
    }
    let schema = batches
        .first()
        .ok_or_else(|| LakehouseError::Io("empty batches".to_string()))?
        .schema();
    let mut columns: Vec<Vec<Arc<dyn Array>>> = vec![Vec::new(); schema.fields().len()];
    for batch in batches {
        for (i, col) in batch.columns().iter().enumerate() {
            if let Some(v) = columns.get_mut(i) {
                v.push(col.clone());
            }
        }
    }
    let arrays: Vec<Arc<dyn Array>> = columns
        .into_iter()
        .map(|parts| {
            arrow::compute::concat(&parts.iter().map(|p| p.as_ref()).collect::<Vec<_>>())
                .map_err(|e| LakehouseError::Io(e.to_string()))
        })
        .collect::<LakehouseResult<_>>()?;
    RecordBatch::try_new(schema, arrays).map_err(|e| LakehouseError::Io(e.to_string()))
}

fn take_rows(batch: &RecordBatch, indices: &[u32]) -> LakehouseResult<RecordBatch> {
    use arrow::array::UInt32Array;
    let idx = UInt32Array::from(indices.to_vec());
    let cols: Vec<Arc<dyn Array>> = (0..batch.num_columns())
        .map(|c| {
            arrow::compute::take(batch.column(c), &idx, None)
                .map_err(|e| LakehouseError::Io(e.to_string()))
        })
        .collect::<LakehouseResult<_>>()?;
    RecordBatch::try_new(batch.schema(), cols).map_err(|e| LakehouseError::Io(e.to_string()))
}

/// Build a set of typed key strings for a column, using type-prefixed
/// formatting so that Int64(1) and String("1") do not collide.
fn keys_set(array: &dyn Array) -> LakehouseResult<std::collections::HashSet<String>> {
    (0..array.len())
        .filter_map(|row| typed_key(array, row).transpose())
        .collect::<LakehouseResult<std::collections::HashSet<_>>>()
}

/// Return target-row indices whose typed key is NOT in the source key set.
fn target_keys_indices(
    array: &dyn Array,
    source_keys: &std::collections::HashSet<String>,
) -> LakehouseResult<Vec<u32>> {
    let mut result = Vec::new();
    for i in 0..array.len() {
        let k = typed_key(array, i)?;
        if k.is_none_or(|key| !source_keys.contains(&key)) {
            result.push(i as u32);
        }
    }
    Ok(result)
}

/// Format a single cell as a type-prefixed string key for hash-join.
/// The prefix prevents cross-type collisions (e.g. Int32 1 vs String "1").
fn typed_key(array: &dyn Array, row: usize) -> Result<Option<String>, LakehouseError> {
    use arrow::datatypes::DataType;
    if array.is_null(row) {
        return Ok(None);
    }
    let prefix = match array.data_type() {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => "I",
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => "U",
        DataType::Float16 | DataType::Float32 | DataType::Float64 => "F",
        DataType::Utf8 | DataType::LargeUtf8 => "S",
        DataType::Boolean => "B",
        DataType::Date32 | DataType::Date64 => "D",
        dt => {
            return Ok(Some(format!(
                "O:{}:{}",
                dt,
                format_value_as_string(array, row)?
            )));
        }
    };
    Ok(Some(format!(
        "{}:{}",
        prefix,
        format_value_as_string(array, row)?
    )))
}

fn format_value_as_string(array: &dyn Array, row: usize) -> LakehouseResult<String> {
    let formatter = ArrayFormatter::try_new(array, &FormatOptions::default())
        .map_err(|e| LakehouseError::Io(e.to_string()))?;
    Ok(formatter.value(row).to_string())
}

/// Remove a column by name from a RecordBatch.
pub fn remove_merge_key_column(
    batch: &RecordBatch,
    key_field: &str,
) -> LakehouseResult<RecordBatch> {
    use arrow::datatypes::Schema;
    let pos = batch
        .schema()
        .index_of(key_field)
        .map_err(|e| LakehouseError::Io(format!("column '{key_field}' not found: {e}")))?;
    let new_schema = Arc::new(Schema::new(
        batch
            .schema()
            .fields()
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != pos)
            .map(|(_, f)| f.as_ref().clone())
            .collect::<Vec<_>>(),
    ));
    let new_columns: Vec<Arc<dyn Array>> = batch
        .columns()
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != pos)
        .map(|(_, c)| c.clone())
        .collect();
    RecordBatch::try_new(new_schema, new_columns).map_err(|e| LakehouseError::Io(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{BooleanArray, Float64Array, Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use tempfile::tempdir;

    fn sample_batch(ids: &[i64], names: &[&str]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids.to_vec())),
                Arc::new(StringArray::from(names.to_vec())),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn delta_roundtrip_local_table() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_string_lossy().to_string();
        let batch = sample_batch(&[1, 2], &["a", "b"]);
        write_delta(&path, vec![batch], DeltaWriteMode::Overwrite, false)
            .await
            .unwrap();
        let handle = DeltaTableHandle::open(&path, None).await.unwrap();
        let read = handle.scan_batches().await.unwrap();
        let rows: usize = read.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 2);
    }

    // ------------------------------------------------------------------
    // typed_key type expansion tests
    // ------------------------------------------------------------------

    fn tk(array: &dyn Array, row: usize) -> Option<String> {
        typed_key(array, row).unwrap()
    }

    #[test]
    fn typed_key_int32() {
        let arr = Int32Array::from(vec![42, -1]);
        assert_eq!(tk(&arr, 0), Some("I:42".into()));
        assert_eq!(tk(&arr, 1), Some("I:-1".into()));
    }

    #[test]
    fn typed_key_float64() {
        let arr = Float64Array::from(vec![3.15, -2.5]);
        assert_eq!(tk(&arr, 0), Some("F:3.15".into()));
        assert_eq!(tk(&arr, 1), Some("F:-2.5".into()));
    }

    #[test]
    fn typed_key_bool() {
        let arr = BooleanArray::from(vec![true, false]);
        assert_eq!(tk(&arr, 0), Some("B:true".into()));
        assert_eq!(tk(&arr, 1), Some("B:false".into()));
    }

    #[test]
    fn typed_key_date32() {
        use arrow::array::Date32Array;
        let arr = Date32Array::from(vec![0, 1]); // epoch, 1970-01-02
        assert_eq!(tk(&arr, 0), Some("D:1970-01-01".into()));
        assert_eq!(tk(&arr, 1), Some("D:1970-01-02".into()));
    }

    #[test]
    fn typed_key_utf8() {
        let arr = StringArray::from(vec!["hello", "world"]);
        assert_eq!(tk(&arr, 0), Some("S:hello".into()));
        assert_eq!(tk(&arr, 1), Some("S:world".into()));
    }

    #[test]
    fn typed_key_int64() {
        let arr = Int64Array::from(vec![1, 2]);
        assert_eq!(tk(&arr, 0), Some("I:1".into()));
        assert_eq!(tk(&arr, 1), Some("I:2".into()));
    }

    #[test]
    fn typed_key_null() {
        let arr = Int64Array::from(vec![Some(1), None]);
        assert_eq!(tk(&arr, 0), Some("I:1".into()));
        assert!(tk(&arr, 1).is_none());
    }

    /// Cross-type collision test: same numeric value in different types must
    /// produce distinct typed keys.
    #[test]
    fn typed_key_cross_type_no_collision() {
        let i32_arr = Int32Array::from(vec![1]);
        let i64_arr = Int64Array::from(vec![1]);
        let f64_arr = Float64Array::from(vec![1.0]);
        let s_arr = StringArray::from(vec!["1"]);

        let i32_key = tk(&i32_arr, 0).unwrap();
        let i64_key = tk(&i64_arr, 0).unwrap();
        let f64_key = tk(&f64_arr, 0).unwrap();
        let s_key = tk(&s_arr, 0).unwrap();

        assert_eq!(i32_key, i64_key, "I:1 == I:1"); // same prefix family
        assert_ne!(i32_key, f64_key, "I:1 != F:1");
        assert_ne!(i32_key, s_key, "I:1 != S:1");
        assert_ne!(f64_key, s_key, "F:1 != S:1");
    }

    #[test]
    fn typed_key_unsigned() {
        use arrow::array::UInt32Array;
        let arr = UInt32Array::from(vec![100u32, 200u32]);
        assert_eq!(tk(&arr, 0), Some("U:100".into()));
        assert_eq!(tk(&arr, 1), Some("U:200".into()));
    }

    // ------------------------------------------------------------------
    // remove_merge_key_column tests
    // ------------------------------------------------------------------

    #[test]
    fn remove_key_column_by_name() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )
        .unwrap();

        let stripped = remove_merge_key_column(&batch, "id").unwrap();
        assert_eq!(stripped.num_columns(), 1);
        assert_eq!(stripped.schema().field(0).name(), "name");
        assert_eq!(stripped.num_rows(), 2);
    }

    #[test]
    fn remove_key_column_not_found_error() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
        let err = remove_merge_key_column(&batch, "nonexistent").unwrap_err();
        assert!(
            matches!(err, LakehouseError::Io(_)),
            "expected Io error, got: {err:?}"
        );
    }

    #[test]
    fn remove_key_column_keeps_remaining_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, false),
            Field::new("c", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["x"])),
                Arc::new(Float64Array::from(vec![3.5])),
            ],
        )
        .unwrap();

        let stripped = remove_merge_key_column(&batch, "b").unwrap();
        assert_eq!(stripped.num_columns(), 2);
        assert_eq!(stripped.schema().field(0).name(), "a");
        assert_eq!(stripped.schema().field(1).name(), "c");
    }

    // ------------------------------------------------------------------
    // merge_delta / write_delta behaviour tests
    // ------------------------------------------------------------------

    /// Insert-only merge (`WHEN NOT MATCHED THEN INSERT` without a matched
    /// clause) must keep matched target rows unchanged, not delete them.
    #[tokio::test]
    async fn merge_delta_insert_only_keeps_matched_target_rows() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_string_lossy().to_string();
        write_delta(
            &path,
            vec![sample_batch(&[1, 2], &["a", "b"])],
            DeltaWriteMode::Overwrite,
            false,
        )
        .await
        .unwrap();

        // Source matches key 1 and brings new key 3; matched update disabled.
        let result = merge_delta(
            &path,
            vec![sample_batch(&[1, 3], &["a-new", "c"])],
            "id",
            false,
            true,
        )
        .await
        .unwrap();
        assert_eq!(result.rows_inserted, 1);
        assert_eq!(result.rows_updated, 0);

        let handle = DeltaTableHandle::open(&path, None).await.unwrap();
        let read = handle.scan_batches().await.unwrap();
        let merged = concat_batches(&read).unwrap();
        assert_eq!(merged.num_rows(), 3, "matched target row must survive");
        let ids = merged
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let names = merged
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let mut rows: Vec<(i64, String)> = (0..merged.num_rows())
            .map(|i| (ids.value(i), names.value(i).to_string()))
            .collect();
        rows.sort();
        assert_eq!(
            rows,
            vec![
                (1, "a".to_string()), // unchanged, NOT "a-new"
                (2, "b".to_string()),
                (3, "c".to_string()),
            ]
        );
    }

    /// `write_delta` with Merge mode must refuse instead of silently
    /// overwriting the table.
    #[tokio::test]
    async fn write_delta_merge_mode_is_explicit_error() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_string_lossy().to_string();
        write_delta(
            &path,
            vec![sample_batch(&[1, 2], &["a", "b"])],
            DeltaWriteMode::Overwrite,
            false,
        )
        .await
        .unwrap();

        let err = write_delta(
            &path,
            vec![sample_batch(&[9], &["z"])],
            DeltaWriteMode::Merge,
            false,
        )
        .await
        .unwrap_err();
        assert!(
            err.to_string().contains("merge_delta"),
            "unexpected error: {err}"
        );

        // Table untouched.
        let handle = DeltaTableHandle::open(&path, None).await.unwrap();
        let rows: usize = handle
            .scan_batches()
            .await
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(rows, 2, "merge-mode error must not modify the table");
    }

    #[tokio::test]
    async fn write_delta_empty_batches_is_noop() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_string_lossy().to_string();
        write_delta(&path, vec![], DeltaWriteMode::Append, false)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn delta_write_mode_overwrite_replaces_data() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_string_lossy().to_string();
        let batch1 = sample_batch(&[1, 2, 3], &["a", "b", "c"]);
        write_delta(&path, vec![batch1], DeltaWriteMode::Overwrite, false)
            .await
            .unwrap();
        let batch2 = sample_batch(&[10], &["x"]);
        write_delta(&path, vec![batch2], DeltaWriteMode::Overwrite, false)
            .await
            .unwrap();
        let handle = DeltaTableHandle::open(&path, None).await.unwrap();
        let read = handle.scan_batches().await.unwrap();
        let rows: usize = read.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 1);
    }

    #[tokio::test]
    async fn delta_write_mode_append_adds_data() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_string_lossy().to_string();
        write_delta(
            &path,
            vec![sample_batch(&[1], &["a"])],
            DeltaWriteMode::Append,
            false,
        )
        .await
        .unwrap();
        write_delta(
            &path,
            vec![sample_batch(&[2], &["b"])],
            DeltaWriteMode::Append,
            false,
        )
        .await
        .unwrap();
        let handle = DeltaTableHandle::open(&path, None).await.unwrap();
        let read = handle.scan_batches().await.unwrap();
        let rows: usize = read.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 2);
    }

    // ── DeltaObjectStoreReader tests ──────────────────────────────────────────

    fn make_inmemory_delta_store() -> std::sync::Arc<dyn object_store::ObjectStore> {
        std::sync::Arc::new(object_store::memory::InMemory::new())
    }

    async fn write_delta_log_entry(
        store: &dyn object_store::ObjectStore,
        prefix: &str,
        version: u64,
        parquet_path: &str,
    ) {
        let log_path =
            object_store::path::Path::from(format!("{prefix}/_delta_log/{version:020}.json"));
        let log_entry = format!(r#"{{"add":{{"path":"{parquet_path}","size":100}}}}"#);
        store
            .put_opts(
                &log_path,
                bytes::Bytes::from(log_entry).into(),
                Default::default(),
            )
            .await
            .unwrap();
    }

    async fn write_parquet_to_store(
        store: &dyn object_store::ObjectStore,
        prefix: &str,
        rel_path: &str,
        batch: &arrow::record_batch::RecordBatch,
    ) {
        use parquet::arrow::ArrowWriter;
        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), None).unwrap();
        writer.write(batch).unwrap();
        writer.close().unwrap();
        let obj_path = object_store::path::Path::from(format!("{prefix}/{rel_path}"));
        store
            .put_opts(
                &obj_path,
                bytes::Bytes::from(buf).into(),
                Default::default(),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn delta_object_store_reader_roundtrip() {
        let store = make_inmemory_delta_store();
        let batch = sample_batch(&[1, 2, 3], &["a", "b", "c"]);

        // Write a Delta log entry pointing at a Parquet file.
        let rel_parquet = "part-0.parquet";
        write_parquet_to_store(store.as_ref(), "tbl", rel_parquet, &batch).await;
        write_delta_log_entry(store.as_ref(), "tbl", 0, rel_parquet).await;

        let reader = DeltaObjectStoreReader::new(std::sync::Arc::clone(&store), "tbl");
        let batches = reader.scan_batches().await.unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 3, "object-store Delta roundtrip must return all rows");
    }

    #[tokio::test]
    async fn delta_object_store_reader_empty_log_returns_empty() {
        let store = make_inmemory_delta_store();
        let reader = DeltaObjectStoreReader::new(std::sync::Arc::clone(&store), "empty_tbl");
        let batches = reader.scan_batches().await.unwrap();
        assert!(batches.is_empty(), "no log entries → no batches");
    }

    #[tokio::test]
    async fn delta_object_store_overwrite_returns_only_new_data() {
        // Verify tombstone handling: an overwrite writes a remove for the old
        // file and an add for the new file. The reader must return only the
        // new file's rows, not both old and new (the pre-fix bug).
        let store = make_inmemory_delta_store();

        // Version 0: add part-0 (3 rows)
        let old_batch = sample_batch(&[1, 2, 3], &["a", "b", "c"]);
        write_parquet_to_store(store.as_ref(), "ow", "part-0.parquet", &old_batch).await;
        let log_v0 = r#"{"add":{"path":"part-0.parquet","size":100}}"#;
        let log_v0_path = object_store::path::Path::from("ow/_delta_log/00000000000000000000.json");
        store
            .as_ref()
            .put_opts(
                &log_v0_path,
                bytes::Bytes::from(log_v0).into(),
                Default::default(),
            )
            .await
            .unwrap();

        // Version 1: remove part-0, add part-1 (1 row) — simulates an overwrite
        let new_batch = sample_batch(&[99], &["new"]);
        write_parquet_to_store(store.as_ref(), "ow", "part-1.parquet", &new_batch).await;
        let log_v1 = "{\"remove\":{\"path\":\"part-0.parquet\",\"dataChange\":true}}\n{\"add\":{\"path\":\"part-1.parquet\",\"size\":50}}";
        let log_v1_path = object_store::path::Path::from("ow/_delta_log/00000000000000000001.json");
        store
            .as_ref()
            .put_opts(
                &log_v1_path,
                bytes::Bytes::from(log_v1).into(),
                Default::default(),
            )
            .await
            .unwrap();

        let reader = DeltaObjectStoreReader::new(std::sync::Arc::clone(&store), "ow");
        let batches = reader.scan_batches().await.unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            rows, 1,
            "overwrite must return only the new file's rows (tombstone removes old file)"
        );
    }

    /// A path removed in one version and re-added in a later version must be
    /// readable again — removes apply in version order, not globally.
    #[tokio::test]
    async fn delta_object_store_remove_then_readd_is_readable() {
        let store = make_inmemory_delta_store();
        let batch = sample_batch(&[1, 2], &["a", "b"]);
        write_parquet_to_store(store.as_ref(), "ra", "part-0.parquet", &batch).await;

        // v0: add part-0
        write_delta_log_entry(store.as_ref(), "ra", 0, "part-0.parquet").await;
        // v1: remove part-0
        let log_v1 = r#"{"remove":{"path":"part-0.parquet","dataChange":true}}"#;
        let log_v1_path = object_store::path::Path::from("ra/_delta_log/00000000000000000001.json");
        store
            .as_ref()
            .put_opts(
                &log_v1_path,
                bytes::Bytes::from(log_v1).into(),
                Default::default(),
            )
            .await
            .unwrap();
        // v2: re-add part-0
        write_delta_log_entry(store.as_ref(), "ra", 2, "part-0.parquet").await;

        let reader = DeltaObjectStoreReader::new(std::sync::Arc::clone(&store), "ra");
        let batches = reader.scan_batches().await.unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 2, "re-added file must be readable after remove");
    }

    /// A get failure on an active file must surface as an error, not silent
    /// row loss.
    #[tokio::test]
    async fn delta_object_store_missing_active_file_is_error() {
        let store = make_inmemory_delta_store();
        // Log references a parquet file that was never written.
        write_delta_log_entry(store.as_ref(), "gone", 0, "part-0.parquet").await;
        let reader = DeltaObjectStoreReader::new(std::sync::Arc::clone(&store), "gone");
        assert!(
            reader.scan_batches().await.is_err(),
            "missing active file must be an error, not an empty result"
        );
    }

    #[tokio::test]
    async fn delta_object_store_reader_multiple_versions() {
        let store = make_inmemory_delta_store();

        let b1 = sample_batch(&[1], &["a"]);
        let b2 = sample_batch(&[2], &["b"]);
        write_parquet_to_store(store.as_ref(), "mv", "part-0.parquet", &b1).await;
        write_parquet_to_store(store.as_ref(), "mv", "part-1.parquet", &b2).await;
        write_delta_log_entry(store.as_ref(), "mv", 0, "part-0.parquet").await;
        write_delta_log_entry(store.as_ref(), "mv", 1, "part-1.parquet").await;

        let reader = DeltaObjectStoreReader::new(std::sync::Arc::clone(&store), "mv");
        let batches = reader.scan_batches().await.unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 2, "both versions must be readable");
    }

    #[tokio::test]
    async fn delta_table_handle_with_version() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_string_lossy().to_string();
        write_delta(
            &path,
            vec![sample_batch(&[1, 2], &["a", "b"])],
            DeltaWriteMode::Overwrite,
            false,
        )
        .await
        .unwrap();
        let handle = DeltaTableHandle::open(&path, None).await.unwrap();
        let v0 = handle.clone();
        let v1 = handle.with_version(0).await.unwrap();
        assert_eq!(v1.version(), Some(0));
        assert_eq!(v1.path(), v0.path());
    }
}

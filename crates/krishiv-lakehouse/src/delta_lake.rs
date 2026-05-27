//! Apache Delta Lake read/write (local `_delta_log` + Parquet, R18 S1).

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use crate::local_delta;
use crate::{LakehouseError, LakehouseResult};

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
        let _ = local_delta::read_table(&path, version.map(|v| v as u64))?;
        Ok(Self { path, version })
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn version(&self) -> Option<i64> {
        self.version
    }

    pub async fn schema(&self) -> LakehouseResult<SchemaRef> {
        local_delta::table_schema(&self.path)
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
    let overwrite = matches!(mode, DeltaWriteMode::Overwrite | DeltaWriteMode::Merge);
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
    let source_keys: HashSet<String> = keys_set(source_col.as_ref());

    // Keep target rows whose key does NOT appear in source.
    let keep_indices: Vec<u32> = target_keys_indices(target_col.as_ref(), &source_keys);

    let mut merged_batches = Vec::new();
    if !keep_indices.is_empty() {
        merged_batches.push(take_rows(&target, &keep_indices)?);
    }

    // Split source rows into updates (key matched target) and inserts (key
    // did not match target).  Build the target-key set once for O(1) checks.
    let target_key_set: HashSet<String> = keys_set(target_col.as_ref());
    let (update_indices, insert_indices): (Vec<u32>, Vec<u32>) =
        (0..source.num_rows()).map(|i| i as u32).partition(|&i| {
            typed_key(source_col.as_ref(), i as usize).is_some_and(|k| target_key_set.contains(&k))
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

use arrow::array::Array;
use arrow::util::display::{ArrayFormatter, FormatOptions};

fn concat_batches(batches: &[RecordBatch]) -> LakehouseResult<RecordBatch> {
    if batches.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::new(
            arrow::datatypes::Schema::empty(),
        )));
    }
    let schema = batches[0].schema();
    let mut columns: Vec<Vec<Arc<dyn Array>>> = vec![Vec::new(); schema.fields().len()];
    for batch in batches {
        for (i, col) in batch.columns().iter().enumerate() {
            columns[i].push(col.clone());
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
fn keys_set(array: &dyn Array) -> std::collections::HashSet<String> {
    (0..array.len())
        .filter_map(|row| typed_key(array, row))
        .collect()
}

/// Return target-row indices whose typed key is NOT in the source key set.
fn target_keys_indices(
    array: &dyn Array,
    source_keys: &std::collections::HashSet<String>,
) -> Vec<u32> {
    (0..array.len())
        .filter(|&i| {
            let k = typed_key(array, i);
            k.is_none() || !source_keys.contains(&k.unwrap())
        })
        .map(|i| i as u32)
        .collect()
}

/// Format a single cell as a type-prefixed string key for hash-join.
/// The prefix prevents cross-type collisions (e.g. Int32 1 vs String "1").
fn typed_key(array: &dyn Array, row: usize) -> Option<String> {
    use arrow::datatypes::DataType;
    if array.is_null(row) {
        return None;
    }
    let prefix = match array.data_type() {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => "I",
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => "U",
        DataType::Float16 | DataType::Float32 | DataType::Float64 => "F",
        DataType::Utf8 | DataType::LargeUtf8 => "S",
        DataType::Boolean => "B",
        DataType::Date32 | DataType::Date64 => "D",
        dt => return Some(format!("O:{}:{}", dt, format_value_as_string(array, row))),
    };
    Some(format!("{}:{}", prefix, format_value_as_string(array, row)))
}

fn format_value_as_string(array: &dyn Array, row: usize) -> String {
    let formatter = ArrayFormatter::try_new(array, &FormatOptions::default())
        .expect("ArrayFormatter creation should succeed for valid arrays");
    formatter.value(row).to_string()
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

    #[test]
    fn typed_key_int32() {
        let arr = Int32Array::from(vec![42, -1]);
        assert_eq!(typed_key(&arr, 0), Some("I:42".into()));
        assert_eq!(typed_key(&arr, 1), Some("I:-1".into()));
    }

    #[test]
    fn typed_key_float64() {
        let arr = Float64Array::from(vec![3.14, -2.5]);
        assert_eq!(typed_key(&arr, 0), Some("F:3.14".into()));
        assert_eq!(typed_key(&arr, 1), Some("F:-2.5".into()));
    }

    #[test]
    fn typed_key_bool() {
        let arr = BooleanArray::from(vec![true, false]);
        assert_eq!(typed_key(&arr, 0), Some("B:true".into()));
        assert_eq!(typed_key(&arr, 1), Some("B:false".into()));
    }

    #[test]
    fn typed_key_date32() {
        use arrow::array::Date32Array;
        let arr = Date32Array::from(vec![0, 1]); // epoch, 1970-01-02
        assert_eq!(typed_key(&arr, 0), Some("D:1970-01-01".into()));
        assert_eq!(typed_key(&arr, 1), Some("D:1970-01-02".into()));
    }

    #[test]
    fn typed_key_utf8() {
        let arr = StringArray::from(vec!["hello", "world"]);
        assert_eq!(typed_key(&arr, 0), Some("S:hello".into()));
        assert_eq!(typed_key(&arr, 1), Some("S:world".into()));
    }

    #[test]
    fn typed_key_int64() {
        let arr = Int64Array::from(vec![1, 2]);
        assert_eq!(typed_key(&arr, 0), Some("I:1".into()));
        assert_eq!(typed_key(&arr, 1), Some("I:2".into()));
    }

    #[test]
    fn typed_key_null() {
        let arr = Int64Array::from(vec![Some(1), None]);
        assert_eq!(typed_key(&arr, 0), Some("I:1".into()));
        assert!(typed_key(&arr, 1).is_none());
    }

    /// Cross-type collision test: same numeric value in different types must
    /// produce distinct typed keys.
    #[test]
    fn typed_key_cross_type_no_collision() {
        let i32_arr = Int32Array::from(vec![1]);
        let i64_arr = Int64Array::from(vec![1]);
        let f64_arr = Float64Array::from(vec![1.0]);
        let s_arr = StringArray::from(vec!["1"]);

        let i32_key = typed_key(&i32_arr, 0).unwrap();
        let i64_key = typed_key(&i64_arr, 0).unwrap();
        let f64_key = typed_key(&f64_arr, 0).unwrap();
        let s_key = typed_key(&s_arr, 0).unwrap();

        assert_eq!(i32_key, i64_key, "I:1 == I:1"); // same prefix family
        assert_ne!(i32_key, f64_key, "I:1 != F:1");
        assert_ne!(i32_key, s_key, "I:1 != S:1");
        assert_ne!(f64_key, s_key, "F:1 != S:1");
    }

    #[test]
    fn typed_key_unsigned() {
        use arrow::array::UInt32Array;
        let arr = UInt32Array::from(vec![100u32, 200u32]);
        assert_eq!(typed_key(&arr, 0), Some("U:100".into()));
        assert_eq!(typed_key(&arr, 1), Some("U:200".into()));
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
}

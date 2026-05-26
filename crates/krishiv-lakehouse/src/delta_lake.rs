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
    use std::collections::HashMap;

    if source_batches.is_empty() {
        return Ok(MergeDeltaResult::default());
    }
    let target_batches = local_delta::read_table(target_path, None)?;
    let target = concat_batches(&target_batches)?;
    let source = concat_batches(&source_batches)?;
    let key = merge_key.to_string();

    let source_keys = keys_column(
        source
            .column_by_name(&key)
            .ok_or_else(|| LakehouseError::Io(format!("merge key {key} missing in source")))?,
    );
    let mut source_map: HashMap<String, usize> = HashMap::new();
    for (i, k) in source_keys.iter().enumerate() {
        if let Some(k) = k {
            source_map.insert(k.clone(), i);
        }
    }

    let target_keys = keys_column(
        target
            .column_by_name(&key)
            .ok_or_else(|| LakehouseError::Io(format!("merge key {key} missing in target")))?,
    );
    let mut keep_indices = Vec::new();
    let mut updated = 0u64;
    for (i, k) in target_keys.iter().enumerate() {
        if let Some(k) = k
            && source_map.contains_key(k)
        {
            if when_matched_update {
                updated += 1;
            }
            continue;
        }
        keep_indices.push(i as u32);
    }

    let mut merged_batches = Vec::new();
    if !keep_indices.is_empty() {
        merged_batches.push(take_rows(&target, &keep_indices)?);
    }
    let mut inserted = 0u64;
    if when_not_matched_insert {
        inserted = source.num_rows() as u64;
        merged_batches.push(source);
    }
    let merged = concat_batches(&merged_batches)?;
    write_delta(target_path, vec![merged], DeltaWriteMode::Overwrite, false).await?;
    Ok(MergeDeltaResult {
        rows_inserted: inserted,
        rows_updated: updated,
        rows_deleted: 0,
    })
}

use arrow::array::{Array, Int64Array, StringArray};

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

fn keys_column(array: &dyn Array) -> Vec<Option<String>> {
    (0..array.len())
        .map(|row| value_as_string(array, row))
        .collect()
}

fn value_as_string(array: &dyn Array, row: usize) -> Option<String> {
    if let Some(a) = array.as_any().downcast_ref::<StringArray>() {
        if a.is_null(row) {
            None
        } else {
            Some(a.value(row).to_string())
        }
    } else if let Some(a) = array.as_any().downcast_ref::<Int64Array>() {
        if a.is_null(row) {
            None
        } else {
            Some(a.value(row).to_string())
        }
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
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
}

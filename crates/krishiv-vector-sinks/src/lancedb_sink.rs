//! Lance-compatible local sink: Parquet fragments + merge_insert on `id` (no lancedb crate dep).

use std::collections::HashMap;
use std::sync::Arc;
use std::path::{Path, PathBuf};
use std::sync::RwLock;

use arrow::array::{FixedSizeListArray, Float32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

use crate::batch::EmbeddingBatch;
use crate::id::point_id_from_doc_epoch;
use crate::memory::InMemoryVectorSink;
use crate::traits::{
    PayloadFilter, ScoredChunk, VectorSink, VectorSinkError, VectorSinkResult,
};

/// Lance-style local sink: persists Parquet under `uri` and serves queries from an in-memory index.
#[derive(Debug)]
pub struct LanceDbSink {
    uri: PathBuf,
    table_name: String,
    vector_dim: usize,
    index: InMemoryVectorSink,
    manifest: RwLock<HashMap<String, PathBuf>>,
}

impl LanceDbSink {
    /// Open or create a Lance-compatible table directory.
    pub async fn open(uri: impl AsRef<Path>, table_name: &str, vector_dim: usize) -> VectorSinkResult<Self> {
        let uri = uri.as_ref().to_path_buf();
        std::fs::create_dir_all(&uri).map_err(|e| VectorSinkError::Connection(e.to_string()))?;
        Ok(Self {
            uri,
            table_name: table_name.to_string(),
            vector_dim,
            index: InMemoryVectorSink::new(),
            manifest: RwLock::new(HashMap::new()),
        })
    }

    fn fragment_path(&self, id: &str) -> PathBuf {
        self.uri
            .join(&self.table_name)
            .join(format!("{id}.parquet"))
    }

    fn write_fragment(&self, id: &str, batch: &RecordBatch) -> VectorSinkResult<()> {
        let path = self.fragment_path(id);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| VectorSinkError::Upsert(e.to_string()))?;
        }
        let file = std::fs::File::create(&path).map_err(|e| VectorSinkError::Upsert(e.to_string()))?;
        let props = WriterProperties::builder().build();
        let mut writer =
            ArrowWriter::try_new(file, batch.schema(), Some(props)).map_err(|e| {
                VectorSinkError::Upsert(e.to_string())
            })?;
        writer
            .write(batch)
            .map_err(|e| VectorSinkError::Upsert(e.to_string()))?;
        writer
            .close()
            .map_err(|e| VectorSinkError::Upsert(e.to_string()))?;
        self.manifest
            .write()
            .map_err(|e| VectorSinkError::Upsert(e.to_string()))?
            .insert(id.to_string(), path);
        Ok(())
    }

    fn batch_to_arrow(batch: &EmbeddingBatch, vector_dim: usize) -> VectorSinkResult<RecordBatch> {
        let n = batch.len();
        let mut ids = Vec::with_capacity(n);
        let mut doc_ids = Vec::with_capacity(n);
        let mut epochs = Vec::with_capacity(n);
        let mut flat = Vec::with_capacity(n * vector_dim);
        for ((doc_id, vector), _) in batch
            .doc_ids
            .iter()
            .zip(batch.vectors.iter())
            .zip(batch.payloads.iter())
        {
            ids.push(point_id_from_doc_epoch(doc_id, batch.epoch));
            doc_ids.push(doc_id.clone());
            epochs.push(batch.epoch as i64);
            if vector.len() != vector_dim {
                return Err(VectorSinkError::SchemaConflict(format!(
                    "vector dim mismatch: expected {vector_dim}, got {}",
                    vector.len()
                )));
            }
            flat.extend_from_slice(vector);
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(
                    Arc::new(Field::new("item", DataType::Float32, true)),
                    vector_dim as i32,
                ),
                false,
            ),
            Field::new("doc_id", DataType::Utf8, false),
            Field::new("epoch", DataType::Int64, false),
        ]));
        let id_array = StringArray::from(ids);
        let doc_id_array = StringArray::from(doc_ids);
        let epoch_array = Int64Array::from(epochs);
        let values = Float32Array::from(flat);
        let vector_array = FixedSizeListArray::new(
            Arc::new(Field::new("item", DataType::Float32, true)),
            vector_dim as i32,
            Arc::new(values),
            None,
        );
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(id_array),
                Arc::new(vector_array),
                Arc::new(doc_id_array),
                Arc::new(epoch_array),
            ],
        )
        .map_err(|e| VectorSinkError::Upsert(e.to_string()))
    }
}

#[async_trait]
impl VectorSink for LanceDbSink {
    fn sink_name(&self) -> &str {
        "lancedb"
    }

    async fn upsert_batch(&self, batch: &EmbeddingBatch) -> VectorSinkResult<()> {
        self.index.upsert_batch(batch).await?;
        let record = Self::batch_to_arrow(batch, self.vector_dim)?;
        for row in 0..batch.len() {
            let id = point_id_from_doc_epoch(&batch.doc_ids[row], batch.epoch);
            let slice = record.slice(row, 1);
            self.write_fragment(&id, &slice)?;
        }
        Ok(())
    }

    async fn delete_by_ids(&self, ids: &[String]) -> VectorSinkResult<()> {
        self.index.delete_by_ids(ids).await?;
        let mut guard = self
            .manifest
            .write()
            .map_err(|e| VectorSinkError::Upsert(e.to_string()))?;
        for id in ids {
            if let Some(path) = guard.remove(id) {
                let _ = std::fs::remove_file(path);
            }
        }
        Ok(())
    }

    async fn query_nearest(
        &self,
        vector: &[f32],
        top_k: usize,
        filter: Option<&PayloadFilter>,
    ) -> VectorSinkResult<Vec<ScoredChunk>> {
        self.index.query_nearest(vector, top_k, filter).await
    }
}

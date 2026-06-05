//! LanceDB-compatible local sink (R17 S1.4).
//!
//! Uses Parquet fragments under `{uri}/{table}/` with per-point `merge_insert` on `id`
//! (`hash(doc_id || epoch)`). The upstream `lancedb` Rust crate is not linked here because
//! its `chrono` dependency conflicts with DataFusion 53 in this workspace; the idempotent
//! upsert contract from ADR-R17.3 is preserved via the same point-id scheme as other sinks.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
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
use crate::traits::{PayloadFilter, ScoredChunk, VectorSink, VectorSinkError, VectorSinkResult};

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
    pub async fn open(
        uri: impl AsRef<Path>,
        table_name: &str,
        vector_dim: usize,
    ) -> VectorSinkResult<Self> {
        let uri = uri.as_ref().to_path_buf();
        tokio::task::spawn_blocking({
            let uri = uri.clone();
            move || std::fs::create_dir_all(&uri)
        })
        .await
        .map_err(|e| VectorSinkError::Connection(e.to_string()))?
        .map_err(|e| VectorSinkError::Connection(e.to_string()))?;
        let mut sink = Self {
            uri: uri.clone(),
            table_name: table_name.to_string(),
            vector_dim,
            index: InMemoryVectorSink::new(),
            manifest: RwLock::new(HashMap::new()),
        };
        sink.load_existing_fragments().await?;
        Ok(sink)
    }

    /// Reload Parquet fragments written in prior runs (P2-9).
    async fn load_existing_fragments(&mut self) -> VectorSinkResult<()> {
        let table_dir = self.uri.join(&self.table_name);
        let exists = tokio::task::spawn_blocking({
            let dir = table_dir.clone();
            move || dir.is_dir()
        })
        .await
        .map_err(|e| VectorSinkError::Connection(e.to_string()))?;
        if !exists {
            return Ok(());
        }
        let entries: Vec<_> = tokio::task::spawn_blocking({
            let dir = table_dir.clone();
            move || -> VectorSinkResult<Vec<PathBuf>> {
                std::fs::read_dir(&dir)
                    .map_err(|e| VectorSinkError::Connection(e.to_string()))
                    .map(|iter| {
                        iter.filter_map(|e| e.ok())
                            .map(|e| e.path())
                            .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("parquet"))
                            .collect()
                    })
            }
        })
        .await
        .map_err(|e| VectorSinkError::Connection(e.to_string()))??;
        for path in entries {
            let batch = tokio::task::spawn_blocking({
                let path = path.clone();
                move || -> VectorSinkResult<Vec<RecordBatch>> {
                    let file = std::fs::File::open(&path)
                        .map_err(|e| VectorSinkError::Query(e.to_string()))?;
                    let reader =
                        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
                            file,
                        )
                        .map_err(|e| VectorSinkError::Query(e.to_string()))?
                        .build()
                        .map_err(|e| VectorSinkError::Query(e.to_string()))?;
                    reader
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(|e| VectorSinkError::Query(e.to_string()))
                }
            })
            .await
            .map_err(|e| VectorSinkError::Query(e.to_string()))??;
            for batch in batch {
                let restored = Self::arrow_batch_to_embedding(&batch, self.vector_dim)?;
                self.index.upsert_batch(&restored).await?;
                if let Some(id) = path.file_stem().and_then(|s| s.to_str()) {
                    self.manifest
                        .write()
                        .map_err(|e| VectorSinkError::Upsert(e.to_string()))?
                        .insert(id.to_string(), path.clone());
                }
            }
        }
        Ok(())
    }

    fn fragment_path(&self, id: &str) -> PathBuf {
        self.uri
            .join(&self.table_name)
            .join(format!("{id}.parquet"))
    }

    async fn write_fragment(&self, id: &str, batch: &RecordBatch) -> VectorSinkResult<()> {
        let path = self.fragment_path(id);
        let batch = batch.clone();
        let path2 = path.clone();
        let id = id.to_string();
        tokio::task::spawn_blocking(move || -> VectorSinkResult<()> {
            if let Some(parent) = path2.parent() {
                std::fs::create_dir_all(parent)
                    .map_err(|e| VectorSinkError::Upsert(e.to_string()))?;
            }
            let file = std::fs::File::create(&path2)
                .map_err(|e| VectorSinkError::Upsert(e.to_string()))?;
            let props = WriterProperties::builder().build();
            let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))
                .map_err(|e| VectorSinkError::Upsert(e.to_string()))?;
            writer
                .write(&batch)
                .map_err(|e| VectorSinkError::Upsert(e.to_string()))?;
            writer
                .close()
                .map_err(|e| VectorSinkError::Upsert(e.to_string()))?;
            Ok(())
        })
        .await
        .map_err(|e| VectorSinkError::Upsert(e.to_string()))??;
        self.manifest
            .write()
            .map_err(|e| VectorSinkError::Upsert(e.to_string()))?
            .insert(id, path);
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

    fn arrow_batch_to_embedding(
        batch: &RecordBatch,
        vector_dim: usize,
    ) -> VectorSinkResult<EmbeddingBatch> {
        use arrow::array::Array;
        let doc_ids = batch
            .column_by_name("doc_id")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| VectorSinkError::SchemaConflict("missing doc_id".into()))?;
        let epochs = batch
            .column_by_name("epoch")
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
            .ok_or_else(|| VectorSinkError::SchemaConflict("missing epoch".into()))?;
        let vectors = batch
            .column_by_name("vector")
            .and_then(|c| c.as_any().downcast_ref::<FixedSizeListArray>())
            .ok_or_else(|| VectorSinkError::SchemaConflict("missing vector".into()))?;
        let mut out = EmbeddingBatch {
            doc_ids: Vec::new(),
            vectors: Vec::new(),
            payloads: vec![HashMap::new(); batch.num_rows()],
            epoch: epochs.value(0) as u64,
        };
        for row in 0..batch.num_rows() {
            out.doc_ids.push(doc_ids.value(row).to_string());
            let list = vectors.value(row);
            let floats = list
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| VectorSinkError::SchemaConflict("vector not float32".into()))?;
            if floats.len() != vector_dim {
                return Err(VectorSinkError::SchemaConflict(
                    "vector dim mismatch".into(),
                ));
            }
            out.vectors.push(floats.values().to_vec());
        }
        Ok(out)
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
        // Write the entire batch as a single Parquet fragment instead of one
        // file per row, which was catastrophic for filesystem overhead.
        let batch_id = point_id_from_doc_epoch("batch", batch.epoch);
        self.write_fragment(&batch_id, &record).await?;
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
                // Filesystem deletes can fail transiently (file held open by
                // a reader, NFS hiccup). Log at warn so the failure is
                // observable; the in-memory index is already updated, so
                // the next query will return the right results.
                if let Err(e) = std::fs::remove_file(&path) {
                    tracing::warn!(
                        sink = "lancedb",
                        id = %id,
                        path = %path.display(),
                        error = %e,
                        "failed to remove Parquet fragment during delete_by_ids"
                    );
                }
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

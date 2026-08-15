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

use super::batch::EmbeddingBatch;
use super::id::point_id_from_doc_epoch;
use super::memory::InMemoryVectorSink;
use super::traits::{PayloadFilter, ScoredChunk, VectorSink, VectorSinkError, VectorSinkResult};

/// A persisted Parquet fragment and the point ids it contains.
#[derive(Debug, Clone)]
struct FragmentMeta {
    path: PathBuf,
    point_ids: Vec<String>,
}

/// Lance-style local sink: persists Parquet under `uri` and serves queries from an in-memory index.
#[derive(Debug)]
pub struct LanceDbSink {
    uri: PathBuf,
    table_name: String,
    vector_dim: usize,
    index: InMemoryVectorSink,
    manifest: RwLock<HashMap<String, FragmentMeta>>,
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
            let batches = Self::read_fragment_batches(&path).await?;
            for batch in batches {
                if batch.num_rows() == 0 {
                    continue;
                }
                let restored = Self::arrow_batch_to_embedding(&batch, self.vector_dim)?;
                let point_ids: Vec<String> = restored
                    .doc_ids
                    .iter()
                    .map(|doc_id| point_id_from_doc_epoch(doc_id, restored.epoch))
                    .collect();
                self.index.upsert_batch(&restored).await?;
                if let Some(id) = path.file_stem().and_then(|s| s.to_str()) {
                    self.manifest
                        .write()
                        .map_err(|e| VectorSinkError::Upsert(e.to_string()))?
                        .insert(
                            id.to_string(),
                            FragmentMeta {
                                path: path.clone(),
                                point_ids,
                            },
                        );
                }
            }
        }
        Ok(())
    }

    async fn read_fragment_batches(path: &Path) -> VectorSinkResult<Vec<RecordBatch>> {
        let path = path.to_path_buf();
        tokio::task::spawn_blocking(move || -> VectorSinkResult<Vec<RecordBatch>> {
            let file =
                std::fs::File::open(&path).map_err(|e| VectorSinkError::Query(e.to_string()))?;
            let reader =
                parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
                    .map_err(|e| VectorSinkError::Query(e.to_string()))?
                    .build()
                    .map_err(|e| VectorSinkError::Query(e.to_string()))?;
            reader
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| VectorSinkError::Query(e.to_string()))
        })
        .await
        .map_err(|e| VectorSinkError::Query(e.to_string()))?
    }

    fn fragment_path(&self, id: &str) -> PathBuf {
        self.uri
            .join(&self.table_name)
            .join(format!("{id}.parquet"))
    }

    async fn write_fragment(
        &self,
        id: &str,
        batch: &RecordBatch,
        point_ids: Vec<String>,
    ) -> VectorSinkResult<()> {
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
            .insert(id, FragmentMeta { path, point_ids });
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
            epoch: if batch.num_rows() == 0 {
                0
            } else {
                epochs.value(0) as u64
            },
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
        // Zero-row fragments would break reload (no epoch row to read), and
        // there is nothing to persist anyway.
        if batch.is_empty() {
            return Ok(());
        }
        self.index.upsert_batch(batch).await?;
        let record = Self::batch_to_arrow(batch, self.vector_dim)?;
        // Write the entire batch as a single Parquet fragment instead of one
        // file per row, which was catastrophic for filesystem overhead. The
        // fragment id is derived from the batch content (point ids), so
        // distinct batches at the same epoch land in distinct fragments while
        // an identical re-upsert idempotently overwrites its own fragment.
        let point_ids: Vec<String> = batch
            .doc_ids
            .iter()
            .map(|doc_id| point_id_from_doc_epoch(doc_id, batch.epoch))
            .collect();
        let mut parts: Vec<&[u8]> = point_ids.iter().map(|id| id.as_bytes()).collect();
        let epoch_bytes = batch.epoch.to_le_bytes();
        parts.push(&epoch_bytes);
        let digest = krishiv_common::hash::sha256_bytes_multi(&parts);
        let batch_id: String = digest.iter().take(16).map(|b| format!("{b:02x}")).collect();
        self.write_fragment(&batch_id, &record, point_ids).await?;
        Ok(())
    }

    async fn delete_by_ids(&self, ids: &[String]) -> VectorSinkResult<()> {
        self.index.delete_by_ids(ids).await?;
        // Find fragments containing any of the deleted point ids.
        let affected: Vec<(String, FragmentMeta)> = {
            let guard = self
                .manifest
                .read()
                .map_err(|e| VectorSinkError::Delete(e.to_string()))?;
            guard
                .iter()
                .filter(|(_, meta)| meta.point_ids.iter().any(|p| ids.contains(p)))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        };
        for (fragment_id, meta) in affected {
            let batches = Self::read_fragment_batches(&meta.path).await?;
            let mut remaining: Option<EmbeddingBatch> = None;
            for batch in batches {
                if batch.num_rows() == 0 {
                    continue;
                }
                let restored = Self::arrow_batch_to_embedding(&batch, self.vector_dim)?;
                let keep = remaining.get_or_insert_with(|| EmbeddingBatch {
                    doc_ids: Vec::new(),
                    vectors: Vec::new(),
                    payloads: Vec::new(),
                    epoch: restored.epoch,
                });
                for (doc_id, vector) in restored.doc_ids.iter().zip(restored.vectors.iter()) {
                    let point_id = point_id_from_doc_epoch(doc_id, restored.epoch);
                    if !ids.contains(&point_id) {
                        keep.doc_ids.push(doc_id.clone());
                        keep.vectors.push(vector.clone());
                        keep.payloads.push(HashMap::new());
                    }
                }
            }
            match remaining {
                Some(keep) if !keep.is_empty() => {
                    // Rewrite the fragment without the deleted points, keeping
                    // its id and path stable.
                    let record = Self::batch_to_arrow(&keep, self.vector_dim)?;
                    let point_ids: Vec<String> = keep
                        .doc_ids
                        .iter()
                        .map(|doc_id| point_id_from_doc_epoch(doc_id, keep.epoch))
                        .collect();
                    self.write_fragment(&fragment_id, &record, point_ids)
                        .await?;
                }
                _ => {
                    // Every point in the fragment was deleted: drop the file.
                    // Filesystem deletes can fail transiently (file held open
                    // by a reader, NFS hiccup). Log at warn so the failure is
                    // observable; the in-memory index is already updated, so
                    // the next query will return the right results.
                    self.manifest
                        .write()
                        .map_err(|e| VectorSinkError::Delete(e.to_string()))?
                        .remove(&fragment_id);
                    if let Err(e) = std::fs::remove_file(&meta.path) {
                        tracing::warn!(
                            sink = "lancedb",
                            fragment = %fragment_id,
                            path = %meta.path.display(),
                            error = %e,
                            "failed to remove Parquet fragment during delete_by_ids"
                        );
                    }
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

#![forbid(unsafe_code)]
//! IVF-accelerated nearest-neighbor search over a registered table
//! (Phase 36 leg b(3), gap G19): the query-side that ties the IVF index
//! [`crate::vector_index`] to real embedding columns and re-ranks across
//! however many record batches a table (or Iceberg file set) spans.
//!
//! [`SqlEngine::ann_search`] is the governed entry point: it reads the
//! `(id, embedding)` projection through the normal SQL path (so grants
//! and every other governed-scan property already apply), extracts the
//! embeddings once, builds an IVF index, and probes. The results-identical
//! contract is a test invariant here — with `nprobe >= nlist` the ANN
//! result is byte-identical to `ORDER BY <metric>(emb, q) LIMIT k`, so the
//! index is only ever a speedup.
//!
//! Multi-file re-rank falls out for free: batches are concatenated into
//! one row space before indexing, so a query spanning N Iceberg files
//! re-ranks the union of their candidates exactly as a single-file query
//! re-ranks one file's.

use arrow::array::{Array, Float32Array, Float64Array};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;

use crate::vector_index::IvfIndex;
use crate::vector_metric::VectorMetric;
use crate::{SqlError, SqlResult};

/// One ANN hit: the id column's value rendered as text, and the distance.
#[derive(Debug, Clone, PartialEq)]
pub struct AnnHit {
    /// The id column value for the row (display-formatted).
    pub id: String,
    /// Distance under the search metric (lower is nearer).
    pub distance: f32,
}

/// Parameters for [`crate::SqlEngine::ann_search`].
#[derive(Debug, Clone)]
pub struct AnnSearchParams<'a> {
    /// Table to search (governed name; grants apply).
    pub table: &'a str,
    /// Identifier column returned for each hit.
    pub id_col: &'a str,
    /// Embedding column (`List`/`FixedSizeList` of floats).
    pub embedding_col: &'a str,
    /// Top-k to return.
    pub k: usize,
    /// Cells to probe; `>= nlist` (e.g. `usize::MAX`) is exhaustive =
    /// identical to the brute-force `ORDER BY <metric> LIMIT k`.
    pub nprobe: usize,
    /// Voronoi cells; `0` auto-sizes to `~sqrt(rows)`.
    pub nlist: usize,
    /// Distance metric (must match how a footer index was built).
    pub metric: VectorMetric,
}

/// Extract a `List`/`FixedSizeList` of `Float32`/`Float64` column into a
/// flat row-major `f32` buffer and its per-row dimension. Every row must
/// share one dimension and carry no NULLs — a ragged or NULL-bearing
/// embedding column is corrupt for search and errors rather than ranking
/// garbage.
fn embeddings_to_f32(
    column: &dyn Array,
    column_name: &str,
) -> SqlResult<(Vec<f32>, usize)> {
    use arrow::array::{FixedSizeListArray, ListArray};
    let err = |m: String| SqlError::DataFusion { message: m };

    let mut flat = Vec::new();
    let mut dim: Option<usize> = None;
    let mut push_row = |values: &dyn Array| -> SqlResult<()> {
        let downcast_err = || err(format!("ann_search: '{column_name}' element downcast failed"));
        let row: Vec<f32> = match values.data_type() {
            DataType::Float32 => values
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(downcast_err)?
                .values()
                .to_vec(),
            DataType::Float64 => values
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(downcast_err)?
                .values()
                .iter()
                .map(|&v| v as f32)
                .collect(),
            other => {
                return Err(err(format!(
                    "ann_search: embedding column '{column_name}' elements must be \
                     Float32/Float64, got {other}"
                )));
            }
        };
        if values.null_count() > 0 {
            return Err(err(format!(
                "ann_search: embedding column '{column_name}' has NULL elements — \
                 clean it before indexing, or NULL would score as 0"
            )));
        }
        match dim {
            Some(d) if d != row.len() => {
                return Err(err(format!(
                    "ann_search: embedding column '{column_name}' is ragged \
                     ({d} vs {} dims) — a vector index needs one dimension",
                    row.len()
                )));
            }
            _ => dim = Some(row.len()),
        }
        flat.extend_from_slice(&row);
        Ok(())
    };

    if let Some(fixed) = column.as_any().downcast_ref::<FixedSizeListArray>() {
        for i in 0..fixed.len() {
            if fixed.is_null(i) {
                return Err(err(format!(
                    "ann_search: embedding column '{column_name}' has a NULL vector at row {i}"
                )));
            }
            push_row(fixed.value(i).as_ref())?;
        }
    } else if let Some(list) = column.as_any().downcast_ref::<ListArray>() {
        for i in 0..list.len() {
            if list.is_null(i) {
                return Err(err(format!(
                    "ann_search: embedding column '{column_name}' has a NULL vector at row {i}"
                )));
            }
            push_row(list.value(i).as_ref())?;
        }
    } else {
        return Err(err(format!(
            "ann_search: column '{column_name}' is {}, not a List/FixedSizeList of floats",
            column.data_type()
        )));
    }

    Ok((flat, dim.unwrap_or(0)))
}

impl crate::SqlEngine {
    /// IVF-accelerated top-`k` nearest neighbors of `query` in
    /// `table`.`embedding_col`, identified by `id_col`.
    ///
    /// `nprobe` cells are probed and their union exact-re-ranked; pass
    /// `nprobe >= nlist` (e.g. `usize::MAX`) for an exhaustive search
    /// identical to the brute-force SQL. `nlist` is auto-sized to
    /// `~sqrt(rows)` (the IVF rule of thumb) when `nlist` is 0.
    pub async fn ann_search(
        &self,
        params: &AnnSearchParams<'_>,
        query: &[f32],
    ) -> SqlResult<Vec<AnnHit>> {
        let AnnSearchParams {
            table,
            id_col,
            embedding_col,
            k,
            nprobe,
            nlist,
            metric,
        } = *params;
        // Governed read: grants, catalog resolution, everything the scan
        // path enforces applies here — ann_search holds no side door.
        let batches = self
            .sql(format!("SELECT {id_col}, {embedding_col} FROM {table}"))
            .await?
            .collect()
            .await?;

        let mut all_embeddings: Vec<f32> = Vec::new();
        let mut ids: Vec<String> = Vec::new();
        let mut dim = 0usize;
        for batch in &batches {
            let id_idx = column_index(batch, id_col)?;
            let emb_idx = column_index(batch, embedding_col)?;
            let (flat, batch_dim) =
                embeddings_to_f32(batch.column(emb_idx).as_ref(), embedding_col)?;
            if batch_dim != 0 {
                if dim != 0 && dim != batch_dim {
                    return Err(SqlError::DataFusion {
                        message: format!(
                            "ann_search: embedding dim {batch_dim} differs from {dim} \
                             across batches — one table, one dimension"
                        ),
                    });
                }
                dim = batch_dim;
            }
            all_embeddings.extend_from_slice(&flat);
            let id_fmt = arrow::util::display::ArrayFormatter::try_new(
                batch.column(id_idx).as_ref(),
                &arrow::util::display::FormatOptions::default(),
            )
            .map_err(|e| SqlError::DataFusion {
                message: e.to_string(),
            })?;
            for r in 0..batch.num_rows() {
                ids.push(id_fmt.value(r).to_string());
            }
        }

        if ids.is_empty() {
            return Ok(Vec::new());
        }
        if query.len() != dim {
            return Err(SqlError::DataFusion {
                message: format!(
                    "ann_search: query dim {} != table embedding dim {dim}",
                    query.len()
                ),
            });
        }

        let n = ids.len();
        let nlist = if nlist == 0 {
            (n as f64).sqrt().ceil() as usize
        } else {
            nlist
        };
        let index = IvfIndex::build(&all_embeddings, dim, nlist, 12, metric)
            .map_err(|m| SqlError::DataFusion { message: m })?;
        let hits = index
            .search(&all_embeddings, query, k, nprobe)
            .map_err(|m| SqlError::DataFusion { message: m })?;
        Ok(hits
            .into_iter()
            .filter_map(|(off, distance)| {
                ids.get(off as usize).map(|id| AnnHit {
                    id: id.clone(),
                    distance,
                })
            })
            .collect())
    }
}

fn column_index(batch: &RecordBatch, name: &str) -> SqlResult<usize> {
    batch
        .schema()
        .index_of(name)
        .map_err(|e| SqlError::DataFusion {
            message: format!("ann_search: column '{name}' not found: {e}"),
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, FixedSizeListBuilder, Float32Builder, StringArray};
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    /// A table of `n` 4-d embeddings on a small lattice, ids `row-000…`.
    async fn seed(engine: &crate::SqlEngine, n: usize) {
        let mut emb = FixedSizeListBuilder::new(Float32Builder::new(), 4);
        let mut ids = Vec::new();
        for i in 0..n {
            let base = (i % 8) as f32 * 10.0 + (i / 8) as f32 * 0.1;
            for v in [base, base + 1.0, base - 1.0, base + 0.5] {
                emb.values().append_value(v);
            }
            emb.append(true);
            ids.push(format!("row-{i:03}"));
        }
        let emb: ArrayRef = Arc::new(emb.finish());
        let id_arr: ArrayRef = Arc::new(StringArray::from(ids));
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new(
                "emb",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 4),
                false,
            ),
        ]));
        let batch = RecordBatch::try_new(schema, vec![id_arr, emb]).unwrap();
        crate::lakehouse::register_scan_batches(engine.session_context(), "docs", vec![batch])
            .await
            .unwrap();
    }

    async fn brute_force_ids(engine: &crate::SqlEngine, q: &str, k: usize) -> Vec<String> {
        let batches = engine
            .sql(&format!(
                "SELECT id FROM docs ORDER BY cosine_distance(emb, {q}) LIMIT {k}"
            ))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let mut out = Vec::new();
        for b in &batches {
            let ids = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
            for i in 0..b.num_rows() {
                out.push(ids.value(i).to_string());
            }
        }
        out
    }

    #[tokio::test]
    async fn ann_exhaustive_equals_brute_force_sql() {
        let engine = crate::SqlEngine::new();
        seed(&engine, 120).await;
        let q = [20.0f32, 21.0, 19.0, 20.5]; // near cluster 2
        // nprobe = usize::MAX → exhaustive → must equal ORDER BY … LIMIT k.
        let hits = engine
            .ann_search(
                &AnnSearchParams {
                    table: "docs",
                    id_col: "id",
                    embedding_col: "emb",
                    k: 5,
                    nprobe: usize::MAX,
                    nlist: 0,
                    metric: VectorMetric::Cosine,
                },
                &q,
            )
            .await
            .unwrap();
        let ann_ids: Vec<String> = hits.into_iter().map(|h| h.id).collect();
        let bf = brute_force_ids(&engine, "[20.0, 21.0, 19.0, 20.5]", 5).await;
        assert_eq!(ann_ids, bf, "exhaustive ANN must equal brute-force SQL");
    }

    #[tokio::test]
    async fn ann_probe_recovers_exact_self_match() {
        let engine = crate::SqlEngine::new();
        seed(&engine, 120).await;
        // row-016 is base 20.0 (i%8==0, i/8==2 → 20.2 actually); query it exactly.
        let batches = engine
            .sql("SELECT emb FROM docs WHERE id = 'row-016'")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let (flat, _) = embeddings_to_f32(batches[0].column(0).as_ref(), "emb").unwrap();
        let hits = engine
            .ann_search(
                &AnnSearchParams {
                    table: "docs",
                    id_col: "id",
                    embedding_col: "emb",
                    k: 1,
                    nprobe: 1,
                    nlist: 0,
                    metric: VectorMetric::L2,
                },
                &flat,
            )
            .await
            .unwrap();
        assert_eq!(hits[0].id, "row-016", "nprobe=1 finds the exact self-match");
        assert!(hits[0].distance.abs() < 1e-4);
    }

    #[tokio::test]
    async fn ragged_and_null_embeddings_error() {
        use arrow::array::ListBuilder;
        let engine = crate::SqlEngine::new();
        // A List column with rows of different lengths.
        let mut b = ListBuilder::new(Float32Builder::new());
        b.values().append_value(1.0);
        b.values().append_value(2.0);
        b.append(true);
        b.values().append_value(3.0);
        b.append(true); // length-1 row → ragged
        let list: ArrayRef = Arc::new(b.finish());
        let ids: ArrayRef = Arc::new(StringArray::from(vec!["a", "b"]));
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new(
                "emb",
                DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
                false,
            ),
        ]));
        let batch = RecordBatch::try_new(schema, vec![ids, list]).unwrap();
        crate::lakehouse::register_scan_batches(engine.session_context(), "ragged", vec![batch])
            .await
            .unwrap();
        let err = engine
            .ann_search(
                &AnnSearchParams {
                    table: "ragged",
                    id_col: "id",
                    embedding_col: "emb",
                    k: 1,
                    nprobe: 1,
                    nlist: 0,
                    metric: VectorMetric::L2,
                },
                &[1.0, 2.0],
            )
            .await
            .err()
            .expect("ragged must error");
        assert!(err.to_string().contains("ragged"), "{err}");
    }
}

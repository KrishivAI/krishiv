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

use std::collections::HashMap;
use std::sync::Arc;

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
pub(crate) fn embeddings_to_f32(
    column: &dyn Array,
    column_name: &str,
) -> SqlResult<(Vec<f32>, usize)> {
    use arrow::array::{FixedSizeListArray, ListArray};
    let err = |m: String| SqlError::DataFusion { message: m };

    let mut flat = Vec::new();
    let mut dim: Option<usize> = None;
    let mut push_row = |values: &dyn Array| -> SqlResult<()> {
        let downcast_err = || {
            err(format!(
                "ann_search: '{column_name}' element downcast failed"
            ))
        };
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

/// Concatenate one embedding column across `batches` into a single flat
/// row-major `f32` buffer, enforcing one dimension across every batch.
pub(crate) fn concat_embedding_batches(
    batches: &[RecordBatch],
    col_idx: usize,
    column_name: &str,
) -> SqlResult<(Vec<f32>, usize)> {
    let mut flat = Vec::new();
    let mut dim = 0usize;
    for batch in batches {
        let column = batch
            .columns()
            .get(col_idx)
            .ok_or_else(|| SqlError::DataFusion {
                message: format!("embedding column index {col_idx} out of range"),
            })?;
        let (rows, batch_dim) = embeddings_to_f32(column.as_ref(), column_name)?;
        if batch_dim != 0 {
            if dim != 0 && dim != batch_dim {
                return Err(SqlError::DataFusion {
                    message: format!(
                        "embedding column '{column_name}': dim {batch_dim} differs from \
                         {dim} across batches — one table, one dimension"
                    ),
                });
            }
            dim = batch_dim;
        }
        flat.extend_from_slice(&rows);
    }
    Ok((flat, dim))
}

/// The SQL distance function a plain-SQL kNN sorts by. Distances the rule
/// computes at plan time must be in THIS function's units (the injected
/// `Filter(distance <= τ)` compares against the SQL function's output), so
/// the enum names the SQL surface, not the index metric.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SqlDistanceFn {
    /// `cosine_distance` / `array_cosine_distance`: `1 − cos θ`.
    CosineDistance,
    /// DataFusion's `array_distance`: TRUE Euclidean (with the sqrt) —
    /// not [`VectorMetric::L2`]'s squared form. Same ordering, different
    /// units; τ is computed in these units.
    Euclidean,
}

impl SqlDistanceFn {
    /// The index metric whose Voronoi cells are valid for this function.
    pub(crate) fn index_metric(self) -> VectorMetric {
        match self {
            SqlDistanceFn::CosineDistance => VectorMetric::Cosine,
            SqlDistanceFn::Euclidean => VectorMetric::L2,
        }
    }
}

/// `f64` mirror of the SQL `cosine_distance` built-in (same accumulation
/// order over f32-widened values). `None` where SQL yields NULL (zero
/// magnitude).
fn cosine_f64(a: &[f32], b: &[f32]) -> Option<f64> {
    let (mut dot, mut na, mut nb) = (0.0f64, 0.0f64, 0.0f64);
    for (x, y) in a.iter().zip(b) {
        let (x, y) = (*x as f64, *y as f64);
        dot += x * y;
        na += x * x;
        nb += y * y;
    }
    let denom = na.sqrt() * nb.sqrt();
    (denom > 0.0).then(|| 1.0 - dot / denom)
}

/// True Euclidean distance in `f64` (the units of DataFusion's
/// `array_distance`).
fn euclid_f64(a: &[f32], b: &[f32]) -> f64 {
    a.iter()
        .zip(b)
        .map(|(x, y)| {
            let d = *x as f64 - *y as f64;
            d * d
        })
        .sum::<f64>()
        .sqrt()
}

/// `f64` with a total order, for the bounded max-heap in the exact
/// traversal.
#[derive(PartialEq)]
struct OrdF64(f64);
impl Eq for OrdF64 {}
impl PartialOrd for OrdF64 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for OrdF64 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.total_cmp(&other.0)
    }
}

/// Keep the `k` smallest values seen: a max-heap capped at `k`.
struct SmallestK {
    k: usize,
    heap: std::collections::BinaryHeap<OrdF64>,
}

impl SmallestK {
    fn new(k: usize) -> Self {
        Self {
            k,
            heap: std::collections::BinaryHeap::with_capacity(k.saturating_add(1)),
        }
    }
    fn push(&mut self, d: f64) {
        if self.heap.len() < self.k {
            self.heap.push(OrdF64(d));
        } else if self.heap.peek().is_some_and(|top| d < top.0) {
            self.heap.pop();
            self.heap.push(OrdF64(d));
        }
    }
    /// The current k-th smallest, once `k` values have been kept.
    fn kth(&self) -> Option<f64> {
        (self.heap.len() == self.k)
            .then(|| self.heap.peek().map(|t| t.0))
            .flatten()
    }
}

/// A cached per-table vector index: the IVF cells plus the exact row data
/// they were built over, retained so the plan-time τ computation and a
/// repeat `ann_search` never re-read or re-cluster the table.
///
/// Staleness discipline: entries are invalidated by every engine DML /
/// re-registration path (`SqlEngine::invalidate_vector_indexes`), so an
/// entry present at plan time describes the table's current rows.
pub(crate) struct TableVectorIndex {
    pub(crate) index: IvfIndex,
    /// The indexed rows, flat row-major (`n_rows * dim`), f32.
    pub(crate) embeddings: Vec<f32>,
    pub(crate) dim: usize,
    pub(crate) n_rows: usize,
    /// Per-cell max Euclidean member↔centroid distance, for the exact
    /// triangle-inequality cell pruning (valid in true-Euclidean space).
    radii_euclid: Vec<f64>,
    /// Rows with a nonzero norm — the rows whose SQL `cosine_distance` is
    /// non-NULL. The rewrite only fires when at least `k` of these exist,
    /// so filtered-out NULL-distance rows can never have been in the top-k.
    finite_cosine_rows: usize,
    /// Largest |component| over the data — scales the τ safety slack that
    /// covers Float64-source columns narrowed to f32 in this cache.
    max_abs: f64,
}

impl std::fmt::Debug for TableVectorIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The row data would flood any log line; summarize.
        f.debug_struct("TableVectorIndex")
            .field("n_rows", &self.n_rows)
            .field("dim", &self.dim)
            .field("nlist", &self.index.nlist())
            .field("metric", &self.index.metric)
            .finish_non_exhaustive()
    }
}

impl TableVectorIndex {
    pub(crate) fn new(index: IvfIndex, embeddings: Vec<f32>, dim: usize) -> Self {
        let n_rows = if dim == 0 { 0 } else { embeddings.len() / dim };
        let mut radii_euclid = vec![0.0f64; index.nlist()];
        for ((members, centroid), radius) in index
            .cells()
            .iter()
            .zip(index.centroids().chunks_exact(dim.max(1)))
            .zip(radii_euclid.iter_mut())
        {
            for &m in members {
                let start = (m as usize).saturating_mul(dim);
                if let Some(row) = embeddings.get(start..start + dim) {
                    *radius = radius.max(euclid_f64(centroid, row));
                }
            }
        }
        let finite_cosine_rows = embeddings
            .chunks_exact(dim.max(1))
            .filter(|row| row.iter().any(|v| *v != 0.0))
            .count();
        let max_abs = embeddings
            .iter()
            .fold(0.0f64, |acc, v| acc.max((*v as f64).abs()));
        Self {
            index,
            embeddings,
            dim,
            n_rows,
            radii_euclid,
            finite_cosine_rows,
            max_abs,
        }
    }

    /// The EXACT k-th smallest SQL-units distance from `query` to the
    /// cached rows, or `None` when fewer than `k` rows have a defined
    /// distance (the rewrite must then decline).
    ///
    /// Euclidean uses the IVF cells for exact triangle-inequality pruning:
    /// every row `x` in cell `c` satisfies `d(q,x) ≥ d(q,centroid_c) −
    /// radius_c`, so cells are visited in ascending lower bound and the
    /// traversal stops when the next bound exceeds the current k-th best —
    /// exact, not approximate. Cosine does a flat pass (a pruned cosine
    /// traversal needs a normalized-space index — a later I/O tightening
    /// that cannot change this function's answer).
    pub(crate) fn exact_kth_sql_distance(
        &self,
        query: &[f32],
        k: usize,
        f: SqlDistanceFn,
    ) -> Option<f64> {
        if k == 0 || query.len() != self.dim || self.dim == 0 {
            return None;
        }
        let mut best = SmallestK::new(k);
        match f {
            SqlDistanceFn::CosineDistance => {
                if self.finite_cosine_rows < k {
                    return None;
                }
                for row in self.embeddings.chunks_exact(self.dim) {
                    if let Some(d) = cosine_f64(query, row) {
                        best.push(d);
                    }
                }
            }
            SqlDistanceFn::Euclidean => {
                if self.n_rows < k {
                    return None;
                }
                let mut cells: Vec<(f64, usize)> = self
                    .index
                    .centroids()
                    .chunks_exact(self.dim)
                    .zip(&self.radii_euclid)
                    .map(|(centroid, radius)| (euclid_f64(query, centroid) - radius).max(0.0))
                    .enumerate()
                    .map(|(i, lb)| (lb, i))
                    .collect();
                cells.sort_by(|a, b| a.0.total_cmp(&b.0));
                for (lower_bound, cell) in cells {
                    if let Some(kth) = best.kth()
                        && lower_bound > kth
                    {
                        break; // no remaining cell can hold a closer row
                    }
                    let Some(members) = self.index.cells().get(cell) else {
                        continue;
                    };
                    for &m in members {
                        let start = (m as usize).saturating_mul(self.dim);
                        if let Some(row) = self.embeddings.get(start..start + self.dim) {
                            best.push(euclid_f64(query, row));
                        }
                    }
                }
            }
        }
        best.kth()
    }

    /// Safety slack added to τ before it becomes the filter bound.
    ///
    /// τ is computed over this cache's f32 copies; the SQL side computes
    /// over the table's original values (exact for Float32 sources, up to
    /// f32-narrowing error for Float64 sources) with its own f64
    /// accumulation order. The slack over-covers both, and over-inclusion
    /// is harmless: extra rows pass the filter and the untouched Sort
    /// still ranks them exactly. Only under-inclusion could move an
    /// answer, so the bounds are deliberately loose.
    pub(crate) fn tau_slack(&self, f: SqlDistanceFn) -> f64 {
        match f {
            // Distances live in [0, 2]; normalization keeps errors tiny.
            SqlDistanceFn::CosineDistance => 1e-4,
            // Per-component narrowing error ≤ max_abs · ε_f32, accumulated
            // across dim under a sqrt — 16× a √dim linearization bound.
            SqlDistanceFn::Euclidean => {
                16.0 * self.max_abs * (f32::EPSILON as f64) * (self.dim as f64).sqrt() + 1e-9
            }
        }
    }
}

/// The per-engine cache: `table␟column` → its index. Shared with the
/// `ann_rewrite` optimizer rule, which holds a clone of this `Arc`.
pub(crate) type VectorIndexCache = Arc<std::sync::RwLock<HashMap<String, Arc<TableVectorIndex>>>>;

/// Normalize a table reference for cache keying: the bare table segment,
/// unquoted, lowercased. Schema-qualified spellings of one table collapse
/// to one key; collisions across schemas only over-invalidate (safe).
pub(crate) fn normalize_table_ref(table: &str) -> String {
    table
        .rsplit('.')
        .next()
        .unwrap_or(table)
        .trim_matches('"')
        .to_ascii_lowercase()
}

/// Cache key for a table's embedding column.
pub(crate) fn vector_cache_key(table: &str, column: &str) -> String {
    format!(
        "{}\u{1f}{}",
        normalize_table_ref(table),
        column.trim_matches('"').to_ascii_lowercase()
    )
}

/// Statistics returned by [`crate::SqlEngine::build_vector_index`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorIndexStats {
    /// Rows indexed.
    pub rows: usize,
    /// Embedding dimensionality.
    pub dim: usize,
    /// Voronoi cells actually built (after clamping / auto-sizing).
    pub nlist: usize,
}

/// Whether `sql` starts like a statement that mutates nothing (SELECT, WITH,
/// EXPLAIN, SHOW, …). Everything else is treated as a mutation for cache
/// invalidation — over-invalidation only costs a re-plan.
pub(crate) fn statement_is_read_shaped(sql: &str) -> bool {
    let first_word = sql
        .trim_start()
        .split(|c: char| c.is_whitespace() || c == '(' || c == ';')
        .next()
        .unwrap_or("")
        .to_ascii_uppercase();
    const READ_SHAPED: &[&str] = &[
        "SELECT", "WITH", "VALUES", "EXPLAIN", "SHOW", "DESCRIBE", "DESC", "TABLE", "ANALYZE",
    ];
    READ_SHAPED.contains(&first_word.as_str())
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
        // A cached index for this (table, column) skips the re-cluster when
        // it still describes the current rows exactly; the cache is
        // invalidated by every DML / re-registration path, and the row-count
        // + dim + metric check here is the belt to that suspenders. `nlist`
        // is advisory on a hit (probe clamps to the cached cell count, so
        // `nprobe >= nlist` stays exhaustive either way).
        let key = vector_cache_key(table, embedding_col);
        let cached = self
            .vector_indexes
            .read()
            .ok()
            .and_then(|cache| cache.get(&key).cloned())
            .filter(|e| e.n_rows == n && e.dim == dim && e.index.metric == metric);
        let hits = match cached {
            Some(entry) => entry.index.search(&all_embeddings, query, k, nprobe),
            None => {
                let nlist = if nlist == 0 {
                    (n as f64).sqrt().ceil() as usize
                } else {
                    nlist
                };
                let index = IvfIndex::build(&all_embeddings, dim, nlist, 12, metric)
                    .map_err(|m| SqlError::DataFusion { message: m })?;
                let hits = index.search(&all_embeddings, query, k, nprobe);
                // Populate the cache so the next call — and the plain-SQL
                // `ORDER BY distance LIMIT k` rewrite — reuses this build.
                if let Ok(mut cache) = self.vector_indexes.write() {
                    cache.insert(
                        key,
                        Arc::new(TableVectorIndex::new(index, all_embeddings.clone(), dim)),
                    );
                }
                hits
            }
        }
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

impl crate::SqlEngine {
    /// Build (or rebuild) the vector index for `table`.`embedding_col` and
    /// cache it engine-side — the explicit indexing entry point the embed
    /// tick calls after landing new rows. The read is governed (the same
    /// SQL path as any scan), the build is deterministic, and both
    /// [`Self::ann_search`] and the plain-SQL `ORDER BY distance LIMIT k`
    /// rewrite reuse the result until the table changes.
    pub async fn build_vector_index(
        &self,
        table: &str,
        embedding_col: &str,
        metric: VectorMetric,
        nlist: usize,
    ) -> SqlResult<VectorIndexStats> {
        let batches = self
            .sql(format!("SELECT {embedding_col} FROM {table}"))
            .await?
            .collect()
            .await?;
        let (rows, dim) = concat_embedding_batches(&batches, 0, embedding_col)?;
        if dim == 0 {
            return Err(SqlError::DataFusion {
                message: format!("build_vector_index: '{table}' has no rows to index"),
            });
        }
        let n = rows.len() / dim;
        let nlist = if nlist == 0 {
            (n as f64).sqrt().ceil() as usize
        } else {
            nlist
        };
        let index = IvfIndex::build(&rows, dim, nlist, 12, metric)
            .map_err(|m| SqlError::DataFusion { message: m })?;
        let stats = VectorIndexStats {
            rows: n,
            dim,
            nlist: index.nlist(),
        };
        if let Ok(mut cache) = self.vector_indexes.write() {
            cache.insert(
                vector_cache_key(table, embedding_col),
                Arc::new(TableVectorIndex::new(index, rows, dim)),
            );
        }
        Ok(stats)
    }

    /// Register a Parquet file as `table_name` and load the IVF index its
    /// footer carries for `embedding_col` (written by
    /// [`crate::vector_footer::write_parquet_with_vector_index`]) into the
    /// engine cache. Returns whether an index was loaded — `false` (with
    /// the table still registered and fully queryable) when the footer has
    /// no index, a foreign payload, or an index that does not describe the
    /// file's rows.
    pub async fn register_parquet_with_vector_index(
        &self,
        table_name: impl AsRef<str>,
        path: impl AsRef<std::path::Path>,
        embedding_col: &str,
    ) -> SqlResult<bool> {
        let table_name = table_name.as_ref();
        let path = path.as_ref();
        self.register_parquet(table_name, path).await?;
        let Some(index) = crate::vector_footer::read_vector_index_footer(path, embedding_col)
        else {
            return Ok(false);
        };
        // The governed read both fetches the rows the cache retains and
        // validates the footer against them: a mismatched index (wrong dim
        // or row count) is dropped, not trusted.
        let batches = self
            .sql(format!("SELECT {embedding_col} FROM {table_name}"))
            .await?
            .collect()
            .await?;
        let Ok((rows, dim)) = concat_embedding_batches(&batches, 0, embedding_col) else {
            return Ok(false);
        };
        if dim == 0 || index.dim != dim || index.len() != rows.len() / dim {
            return Ok(false);
        }
        if let Ok(mut cache) = self.vector_indexes.write() {
            cache.insert(
                vector_cache_key(table_name, embedding_col),
                Arc::new(TableVectorIndex::new(index, rows, dim)),
            );
        }
        Ok(true)
    }

    /// The cached index entry for `table`.`column`, if any (test-only
    /// observability; production code goes through the cache directly).
    #[cfg(test)]
    pub(crate) fn vector_index_entry(
        &self,
        table: &str,
        column: &str,
    ) -> Option<Arc<TableVectorIndex>> {
        self.vector_indexes
            .read()
            .ok()
            .and_then(|cache| cache.get(&vector_cache_key(table, column)).cloned())
    }

    /// Front-door statement guard: any statement that is not read-shaped
    /// clears the WHOLE vector-index cache. Coarse on purpose — memory DDL
    /// (`CREATE OR REPLACE TABLE … AS`) executes inside DataFusion where
    /// no per-table hook sees it, and a stale index behind the τ rewrite
    /// is the one path to a wrong answer. Over-invalidation only costs a
    /// rebuild; the serving pattern (index once, SELECT many) never pays.
    pub(crate) fn invalidate_vector_indexes_for_statement(&self, sql: &str) {
        // The plan cache is protected regardless of the vector cache: this
        // fast path used to return before `invalidate_plan_cache`, so with no
        // vector index built (the normal case) `CREATE OR REPLACE TABLE` /
        // `DROP TABLE` left cached plans pinning the replaced provider, and
        // identical SELECT text served the old rows for the cache lifetime.
        if !statement_is_read_shaped(sql) {
            self.invalidate_plan_cache();
        }
        // Fast path: nothing cached, nothing to protect.
        if self
            .vector_indexes
            .read()
            .map(|cache| cache.is_empty())
            .unwrap_or(true)
        {
            return;
        }
        // CREATE VECTOR INDEX mutates no rows — it POPULATES this cache;
        // clearing other tables' indexes for it would be pure loss.
        if crate::statement_completion::parse_create_vector_index(sql).is_some() {
            return;
        }
        if !statement_is_read_shaped(sql) {
            if let Ok(mut cache) = self.vector_indexes.write() {
                cache.clear();
            }
            self.invalidate_plan_cache();
        }
    }

    /// Drop every cached vector index for `table_ref` — called from every
    /// DML and re-registration path, so a cache entry present at plan time
    /// always describes the table's current rows. When an entry was
    /// actually dropped the plan cache goes with it: an optimized plan may
    /// embed a τ bound computed from the dropped entry.
    pub(crate) fn invalidate_vector_indexes(&self, table_ref: &str) {
        let prefix = format!("{}\u{1f}", normalize_table_ref(table_ref));
        let mut dropped = false;
        if let Ok(mut cache) = self.vector_indexes.write() {
            let before = cache.len();
            cache.retain(|key, _| !key.starts_with(&prefix));
            dropped = cache.len() != before;
        }
        if dropped {
            self.invalidate_plan_cache();
        }
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
            .sql(format!(
                "SELECT id FROM docs ORDER BY cosine_distance(emb, {q}) LIMIT {k}"
            ))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let mut out = Vec::new();
        for b in &batches {
            // Parquet scans hand back Utf8View, memory tables Utf8 — cast
            // rather than downcast so the helper serves both.
            let ids = arrow::compute::cast(b.column(0), &DataType::Utf8).unwrap();
            let ids = ids.as_any().downcast_ref::<StringArray>().unwrap();
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
    async fn ann_search_populates_and_reuses_the_cache() {
        let engine = crate::SqlEngine::new();
        seed(&engine, 120).await;
        assert!(engine.vector_index_entry("docs", "emb").is_none());
        let q = [20.0f32, 21.0, 19.0, 20.5];
        let params = AnnSearchParams {
            table: "docs",
            id_col: "id",
            embedding_col: "emb",
            k: 5,
            nprobe: usize::MAX,
            nlist: 0,
            metric: VectorMetric::Cosine,
        };
        let first = engine.ann_search(&params, &q).await.unwrap();
        assert!(
            engine.vector_index_entry("docs", "emb").is_some(),
            "the first search must leave the index cached"
        );
        // The second call takes the cached-index path; results identical.
        let second = engine.ann_search(&params, &q).await.unwrap();
        assert_eq!(first, second, "cache hit must not move an answer");
        let bf = brute_force_ids(&engine, "[20.0, 21.0, 19.0, 20.5]", 5).await;
        let ids: Vec<String> = second.into_iter().map(|h| h.id).collect();
        assert_eq!(ids, bf);
    }

    #[tokio::test]
    async fn build_vector_index_reports_stats_and_serves_ann_search() {
        let engine = crate::SqlEngine::new();
        seed(&engine, 120).await;
        let stats = engine
            .build_vector_index("docs", "emb", VectorMetric::Cosine, 0)
            .await
            .unwrap();
        assert_eq!(
            stats,
            VectorIndexStats {
                rows: 120,
                dim: 4,
                nlist: 11, // ceil(sqrt(120))
            }
        );
        // Exhaustive search off the prebuilt index equals brute force.
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
                &[20.0, 21.0, 19.0, 20.5],
            )
            .await
            .unwrap();
        let ids: Vec<String> = hits.into_iter().map(|h| h.id).collect();
        assert_eq!(
            ids,
            brute_force_ids(&engine, "[20.0, 21.0, 19.0, 20.5]", 5).await
        );
    }

    #[tokio::test]
    async fn register_parquet_with_footer_index_roundtrip() {
        use arrow::array::{FixedSizeListBuilder, Float32Builder};
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("docs.parquet");
        // Same lattice as seed(), landed as a footer-indexed Parquet file.
        let mut emb = FixedSizeListBuilder::new(Float32Builder::new(), 4);
        let mut ids = Vec::new();
        for i in 0..120usize {
            let base = (i % 8) as f32 * 10.0 + (i / 8) as f32 * 0.1;
            for v in [base, base + 1.0, base - 1.0, base + 0.5] {
                emb.values().append_value(v);
            }
            emb.append(true);
            ids.push(format!("row-{i:03}"));
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new(
                "emb",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 4),
                false,
            ),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(ids)) as ArrayRef,
                Arc::new(emb.finish()) as ArrayRef,
            ],
        )
        .unwrap();
        crate::vector_footer::write_parquet_with_vector_index(
            &path,
            &[batch],
            "emb",
            VectorMetric::Cosine,
            0,
        )
        .unwrap();

        let engine = crate::SqlEngine::new();
        let loaded = engine
            .register_parquet_with_vector_index("docs", &path, "emb")
            .await
            .unwrap();
        assert!(loaded, "footer index must load");
        assert!(engine.vector_index_entry("docs", "emb").is_some());
        // The registered table answers exactly like brute-force SQL.
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
                &[20.0, 21.0, 19.0, 20.5],
            )
            .await
            .unwrap();
        let ann_ids: Vec<String> = hits.into_iter().map(|h| h.id).collect();
        assert_eq!(
            ann_ids,
            brute_force_ids(&engine, "[20.0, 21.0, 19.0, 20.5]", 5).await
        );

        // A plain file (no footer) registers fine but loads no index.
        let plain = dir.path().join("plain.parquet");
        std::fs::copy(&path, &plain).unwrap();
        let engine2 = crate::SqlEngine::new();
        // Strip the footer by rewriting through a plain writer.
        let file = std::fs::File::open(&plain).unwrap();
        let reader =
            datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
                file,
            )
            .unwrap()
            .build()
            .unwrap();
        let batches: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
        let stripped = dir.path().join("stripped.parquet");
        let out = std::fs::File::create(&stripped).unwrap();
        let mut writer =
            datafusion::parquet::arrow::ArrowWriter::try_new(out, batches[0].schema(), None)
                .unwrap();
        for b in &batches {
            writer.write(b).unwrap();
        }
        writer.close().unwrap();
        let loaded = engine2
            .register_parquet_with_vector_index("docs", &stripped, "emb")
            .await
            .unwrap();
        assert!(!loaded, "no footer, no index — but the table still works");
        assert!(engine2.vector_index_entry("docs", "emb").is_none());
        assert_eq!(
            brute_force_ids(&engine2, "[20.0, 21.0, 19.0, 20.5]", 5)
                .await
                .len(),
            5
        );
    }

    #[test]
    fn exact_kth_distance_matches_brute_force_both_functions() {
        // The τ source must be EXACT: compare the pruned Euclidean
        // traversal and the flat cosine pass against a plain full scan.
        let dim = 4;
        let mut rows = Vec::new();
        for i in 0..300usize {
            let base = (i % 9) as f32 * 7.0 + (i / 9) as f32 * 0.13;
            rows.extend_from_slice(&[base, base * 0.5 + 1.0, -base, base + 0.25]);
        }
        // A few zero vectors: NULL under cosine, ordinary under Euclidean.
        for _ in 0..3 {
            rows.extend_from_slice(&[0.0, 0.0, 0.0, 0.0]);
        }
        let index = IvfIndex::build(&rows, dim, 17, 12, VectorMetric::L2).unwrap();
        let entry = TableVectorIndex::new(index, rows.clone(), dim);
        let q = [23.0f32, 12.0, -20.0, 22.0];

        for k in [1usize, 5, 40] {
            // Brute-force k-th, Euclidean.
            let mut euclid: Vec<f64> = rows.chunks_exact(dim).map(|r| euclid_f64(&q, r)).collect();
            euclid.sort_by(|a, b| a.total_cmp(b));
            let expected = euclid[k - 1];
            let got = entry
                .exact_kth_sql_distance(&q, k, SqlDistanceFn::Euclidean)
                .unwrap();
            assert!(
                (got - expected).abs() < 1e-12,
                "euclid k={k}: {got} vs {expected}"
            );

            // Brute-force k-th, cosine (zero vectors excluded, as SQL NULLs).
            let mut cos: Vec<f64> = rows
                .chunks_exact(dim)
                .filter_map(|r| cosine_f64(&q, r))
                .collect();
            cos.sort_by(|a, b| a.total_cmp(b));
            let expected = cos[k - 1];
            let got = entry
                .exact_kth_sql_distance(&q, k, SqlDistanceFn::CosineDistance)
                .unwrap();
            assert!(
                (got - expected).abs() < 1e-12,
                "cosine k={k}: {got} vs {expected}"
            );
        }

        // More rows requested than defined distances → decline.
        assert!(
            entry
                .exact_kth_sql_distance(&q, 400, SqlDistanceFn::Euclidean)
                .is_none()
        );
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
            .expect_err("ragged must error");
        assert!(err.to_string().contains("ragged"), "{err}");
    }
}

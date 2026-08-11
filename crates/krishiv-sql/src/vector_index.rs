#![forbid(unsafe_code)]
//! IVF (inverted-file) vector index — Phase 36 leg b(2/3), gap G19.
//!
//! An IVF index partitions a file's embeddings into `nlist` Voronoi cells
//! by k-means centroids; a query probes the `nprobe` nearest cells and
//! exact-re-ranks only their members instead of scanning every row. This
//! is the "pq-vector in the Parquet footer" approach: the centroids and
//! per-cell membership serialize to ~0.3% overhead in the file's user
//! metadata, standard readers ignore them, and the embed tick writes them
//! so freshness is by construction.
//!
//! This module is the pure, deterministic core: build, serialize, probe.
//! The physical-plan rewrite (`ORDER BY distance LIMIT k` → probe) and the
//! Parquet-footer read/write wire it into the scan path; both rest on the
//! results-identical property proven here — **`nprobe >= nlist` returns
//! exactly the brute-force top-k**, so the index is an accelerator that
//! can never change an answer, only the work done to reach it.

use crate::vector_metric::VectorMetric;

/// Serialization tag — bump only on an incompatible layout change so a
/// stale footer index is ignored (rebuilt by the next embed tick) rather
/// than misread.
const IVF_MAGIC: u32 = 0x49_56_46_31; // "IVF1"

/// An IVF index over one file's embeddings.
#[derive(Debug, Clone, PartialEq)]
pub struct IvfIndex {
    /// Embedding dimensionality.
    pub dim: usize,
    /// The distance metric the cells were built for. Probing under a
    /// different metric is refused — cells partitioned for cosine mean
    /// nothing for L2.
    pub metric: VectorMetric,
    /// `nlist` centroids, row-major (`nlist * dim` floats).
    centroids: Vec<f32>,
    /// Inverted lists: for each centroid, the row offsets assigned to it.
    /// Row offsets are into the FILE's embedding column (0-based).
    inverted: Vec<Vec<u32>>,
}

impl IvfIndex {
    /// Number of Voronoi cells (`nlist`).
    pub fn nlist(&self) -> usize {
        self.inverted.len()
    }

    /// Total indexed rows across all cells.
    pub fn len(&self) -> usize {
        self.inverted.iter().map(Vec::len).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Build an IVF index over `rows` (`n * dim` row-major embeddings) with
    /// `nlist` cells and `iters` Lloyd iterations.
    ///
    /// Deterministic: initial centroids are strided picks over the input
    /// (no RNG — reproducible index for the same data, which is what makes
    /// the footer bytes content-addressable), and empty cells re-seed to
    /// the point farthest from its assigned centroid so `nlist` is always
    /// honored. `nlist` is clamped to `[1, n]`.
    pub fn build(
        rows: &[f32],
        dim: usize,
        nlist: usize,
        iters: usize,
        metric: VectorMetric,
    ) -> Result<Self, String> {
        if dim == 0 {
            return Err("IVF build: dim must be > 0".into());
        }
        if !rows.len().is_multiple_of(dim) {
            return Err(format!(
                "IVF build: {} values not divisible by dim {dim}",
                rows.len()
            ));
        }
        let n = rows.len() / dim;
        if n == 0 {
            return Err("IVF build: no rows".into());
        }
        let nlist = nlist.clamp(1, n);

        // Strided deterministic init: evenly spaced picks so the seeds
        // span the input rather than clustering at the front.
        let mut centroids = vec![0.0f32; nlist * dim];
        for (c, dst) in centroids.chunks_exact_mut(dim).enumerate() {
            let src = (c * n) / nlist;
            if let Some(seed) = rows.chunks_exact(dim).nth(src) {
                dst.copy_from_slice(seed);
            }
        }

        let mut assign = vec![0u32; n];
        for _ in 0..iters.max(1) {
            // Assignment step.
            let mut changed = false;
            for (a, r) in assign.iter_mut().zip(rows.chunks_exact(dim)) {
                let best = nearest_centroid(r, &centroids, dim, metric) as u32;
                if *a != best {
                    *a = best;
                    changed = true;
                }
            }
            // Update step: mean of each cell's members.
            let mut sums = vec![0.0f32; nlist * dim];
            let mut counts = vec![0u32; nlist];
            for (&c, r) in assign.iter().zip(rows.chunks_exact(dim)) {
                let c = c as usize;
                if let (Some(cnt), Some(s)) =
                    (counts.get_mut(c), sums.chunks_exact_mut(dim).nth(c))
                {
                    *cnt += 1;
                    for (sd, &rd) in s.iter_mut().zip(r) {
                        *sd += rd;
                    }
                }
            }
            for (c, (cnt, centroid)) in counts
                .iter()
                .zip(centroids.chunks_exact_mut(dim))
                .enumerate()
            {
                if *cnt == 0 {
                    // Re-seed an empty cell to a point stolen from the
                    // most-populated region so nlist is honored and the next
                    // assignment can populate it. Deterministic: the lowest
                    // row offset among the largest cell's members.
                    if let Some(worst) = donor_row(&assign, nlist) {
                        if let Some(seed) = rows.chunks_exact(dim).nth(worst) {
                            centroid.copy_from_slice(seed);
                        }
                        if let Some(a) = assign.get_mut(worst) {
                            *a = c as u32;
                        }
                    }
                    continue;
                }
                let inv = 1.0 / *cnt as f32;
                if let Some(s) = sums.chunks_exact(dim).nth(c) {
                    for (cd, &sd) in centroid.iter_mut().zip(s) {
                        *cd = sd * inv;
                    }
                }
            }
            if !changed {
                break;
            }
        }

        // Final membership under the converged centroids.
        let mut inverted = vec![Vec::new(); nlist];
        for (i, r) in rows.chunks_exact(dim).enumerate() {
            let c = nearest_centroid(r, &centroids, dim, metric);
            if let Some(cell) = inverted.get_mut(c) {
                cell.push(i as u32);
            }
        }
        Ok(Self {
            dim,
            metric,
            centroids,
            inverted,
        })
    }

    /// Candidate row offsets for a query: union of the `nprobe` nearest
    /// cells' members. `nprobe >= nlist` returns every row (the
    /// brute-force set). The caller exact-re-ranks these by true distance.
    pub fn probe(&self, query: &[f32], nprobe: usize) -> Result<Vec<u32>, String> {
        if query.len() != self.dim {
            return Err(format!(
                "IVF probe: query dim {} != index dim {}",
                query.len(),
                self.dim
            ));
        }
        let nprobe = nprobe.clamp(1, self.nlist());
        // Rank cells by centroid distance, take the nearest nprobe.
        let mut cell_dist: Vec<(usize, f32)> = self
            .centroids
            .chunks_exact(self.dim)
            .map(|centroid| self.metric.distance(query, centroid))
            .enumerate()
            .collect();
        cell_dist.sort_by(|a, b| a.1.total_cmp(&b.1));
        let mut candidates = Vec::new();
        for (c, _) in cell_dist.into_iter().take(nprobe) {
            if let Some(cell) = self.inverted.get(c) {
                candidates.extend_from_slice(cell);
            }
        }
        candidates.sort_unstable();
        Ok(candidates)
    }

    /// Top-`k` `(row_offset, distance)` for `query`, probing `nprobe`
    /// cells and exact-re-ranking the candidates. Ties break by lower row
    /// offset for a stable, reproducible order.
    pub fn search(
        &self,
        rows: &[f32],
        query: &[f32],
        k: usize,
        nprobe: usize,
    ) -> Result<Vec<(u32, f32)>, String> {
        let candidates = self.probe(query, nprobe)?;
        let mut scored: Vec<(u32, f32)> = candidates
            .into_iter()
            .filter_map(|off| {
                let start = (off as usize).checked_mul(self.dim)?;
                let emb = rows.get(start..start + self.dim)?;
                Some((off, self.metric.distance(query, emb)))
            })
            .collect();
        scored.sort_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)));
        scored.truncate(k);
        Ok(scored)
    }

    /// Serialize to bytes for the Parquet footer user metadata.
    /// Layout: magic, metric tag, dim, nlist, centroids (f32 LE), then per
    /// cell a u32 length + its u32 row offsets.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&IVF_MAGIC.to_le_bytes());
        out.extend_from_slice(&(self.metric.tag() as u32).to_le_bytes());
        out.extend_from_slice(&(self.dim as u32).to_le_bytes());
        out.extend_from_slice(&(self.nlist() as u32).to_le_bytes());
        for &f in &self.centroids {
            out.extend_from_slice(&f.to_le_bytes());
        }
        for cell in &self.inverted {
            out.extend_from_slice(&(cell.len() as u32).to_le_bytes());
            for &off in cell {
                out.extend_from_slice(&off.to_le_bytes());
            }
        }
        out
    }

    /// Parse footer bytes back into an index. Returns `None` (not an
    /// error) for a wrong magic or a truncated buffer — a stale or foreign
    /// footer must degrade to a brute-force scan, never fail the query.
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        let mut r = ByteReader::new(bytes);
        if r.u32()? != IVF_MAGIC {
            return None;
        }
        let metric = VectorMetric::from_tag(r.u32()? as u8)?;
        let dim = r.u32()? as usize;
        let nlist = r.u32()? as usize;
        if dim == 0 || nlist == 0 {
            return None;
        }
        let mut centroids = Vec::with_capacity(nlist * dim);
        for _ in 0..nlist * dim {
            centroids.push(r.f32()?);
        }
        let mut inverted = Vec::with_capacity(nlist);
        for _ in 0..nlist {
            let len = r.u32()? as usize;
            let mut cell = Vec::with_capacity(len);
            for _ in 0..len {
                cell.push(r.u32()?);
            }
            inverted.push(cell);
        }
        Some(Self {
            dim,
            metric,
            centroids,
            inverted,
        })
    }
}

fn nearest_centroid(v: &[f32], centroids: &[f32], dim: usize, metric: VectorMetric) -> usize {
    centroids
        .chunks_exact(dim)
        .map(|c| metric.distance(v, c))
        .enumerate()
        .min_by(|(_, a), (_, b)| a.total_cmp(b))
        .map(|(i, _)| i)
        .unwrap_or(0)
}

/// A row to steal for re-seeding an empty cell: the lowest-offset member of
/// the currently most-populated cell (deterministic, and stealing from the
/// largest cell is the split that most reduces the worst-case cell size).
fn donor_row(assign: &[u32], nlist: usize) -> Option<usize> {
    let mut counts = vec![0u32; nlist];
    for &c in assign {
        if let Some(cnt) = counts.get_mut(c as usize) {
            *cnt += 1;
        }
    }
    let (biggest, _) = counts.iter().enumerate().max_by_key(|&(_, c)| *c)?;
    assign.iter().position(|&c| c as usize == biggest)
}

struct ByteReader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> ByteReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }
    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let end = self.pos.checked_add(n)?;
        let slice = self.bytes.get(self.pos..end)?;
        self.pos = end;
        Some(slice)
    }
    fn u32(&mut self) -> Option<u32> {
        Some(u32::from_le_bytes(self.take(4)?.try_into().ok()?))
    }
    fn f32(&mut self) -> Option<f32> {
        Some(f32::from_le_bytes(self.take(4)?.try_into().ok()?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Deterministic clustered fixture: `clusters` groups of `per` points
    /// spread around integer lattice centers, dim 4.
    fn fixture(clusters: usize, per: usize) -> (Vec<f32>, usize) {
        let dim = 4;
        let mut rows = Vec::new();
        for c in 0..clusters {
            let base = (c * 10) as f32;
            for p in 0..per {
                let jitter = (p as f32) * 0.01;
                rows.extend_from_slice(&[base + jitter, base - jitter, base, base + 0.5]);
            }
        }
        (rows, dim)
    }

    fn brute_force(rows: &[f32], dim: usize, q: &[f32], k: usize, m: VectorMetric) -> Vec<u32> {
        let n = rows.len() / dim;
        let mut scored: Vec<(u32, f32)> = (0..n)
            .map(|i| (i as u32, m.distance(q, &rows[i * dim..(i + 1) * dim])))
            .collect();
        scored.sort_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)));
        scored.truncate(k);
        scored.into_iter().map(|(o, _)| o).collect()
    }

    #[test]
    fn full_probe_equals_brute_force() {
        // The load-bearing property: nprobe >= nlist is exactly brute force,
        // for both metrics, so the index can only save work, never move an
        // answer.
        for metric in [VectorMetric::L2, VectorMetric::Cosine] {
            let (rows, dim) = fixture(8, 25);
            let index = IvfIndex::build(&rows, dim, 8, 10, metric).unwrap();
            for qi in [0usize, 37, 199] {
                let q = &rows[qi * dim..(qi + 1) * dim];
                let ann = index.search(&rows, q, 5, index.nlist()).unwrap();
                let ann_ids: Vec<u32> = ann.into_iter().map(|(o, _)| o).collect();
                let bf = brute_force(&rows, dim, q, 5, metric);
                assert_eq!(ann_ids, bf, "metric={metric:?} qi={qi}");
            }
        }
    }

    #[test]
    fn partial_probe_finds_the_true_neighbor_for_clustered_data() {
        // With well-separated clusters, probing even 1 cell recovers the
        // exact nearest neighbor (it shares the query's cell). This is the
        // recall the accelerator buys.
        let (rows, dim) = fixture(8, 25);
        let index = IvfIndex::build(&rows, dim, 8, 10, VectorMetric::L2).unwrap();
        let q = &rows[50 * dim..51 * dim]; // a point in cluster 2
        let top1 = index.search(&rows, q, 1, 1).unwrap();
        assert_eq!(top1[0].0, 50, "nprobe=1 still finds the exact self-match");
        assert!(top1[0].1.abs() < 1e-6);
    }

    #[test]
    fn nlist_honored_even_with_degenerate_data() {
        // All-identical rows would collapse cells; re-seeding keeps nlist.
        let rows = vec![1.0f32; 4 * 20];
        let index = IvfIndex::build(&rows, 4, 5, 10, VectorMetric::L2).unwrap();
        assert_eq!(index.nlist(), 5);
        assert_eq!(index.len(), 20, "every row indexed exactly once");
    }

    #[test]
    fn serialize_roundtrip_is_exact() {
        let (rows, dim) = fixture(6, 15);
        let index = IvfIndex::build(&rows, dim, 6, 8, VectorMetric::Cosine).unwrap();
        let bytes = index.to_bytes();
        let back = IvfIndex::from_bytes(&bytes).expect("roundtrip");
        assert_eq!(index, back);
        // ~0.3% overhead claim sanity: index bytes are a small multiple of
        // nlist*dim floats + row offsets, far under the raw embedding bytes.
        assert!(bytes.len() < rows.len() * 4, "index smaller than the vectors");
    }

    #[test]
    fn foreign_or_truncated_footer_is_none_not_panic() {
        assert!(IvfIndex::from_bytes(b"not an ivf footer").is_none());
        let (rows, dim) = fixture(4, 10);
        let good = IvfIndex::build(&rows, dim, 4, 5, VectorMetric::L2).unwrap().to_bytes();
        assert!(IvfIndex::from_bytes(&good[..good.len() - 3]).is_none(), "truncated");
    }

    #[test]
    fn build_is_deterministic() {
        let (rows, dim) = fixture(5, 20);
        let a = IvfIndex::build(&rows, dim, 5, 12, VectorMetric::L2).unwrap();
        let b = IvfIndex::build(&rows, dim, 5, 12, VectorMetric::L2).unwrap();
        assert_eq!(a, b, "same data → identical index (content-addressable footer)");
    }
}

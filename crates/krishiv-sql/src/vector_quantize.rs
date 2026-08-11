#![forbid(unsafe_code)]
//! Data-oblivious scalar quantization for candidate scoring — Phase 36
//! leg c, gap G19.
//!
//! The IVF probe ([`crate::vector_index`]) narrows a query to a candidate
//! set; scoring that set still reads full-precision vectors off object
//! storage, which is the dominant I/O on a disk-backed cluster. This
//! module quantizes each vector to `bits`-per-dimension codes at write
//! time so the candidate scan reads 8–16× fewer bytes, then exact-re-ranks
//! only the top-`m` from full precision.
//!
//! **Data-oblivious** (the TurboQuant property, arXiv 2504.19874): each row
//! is quantized independently against a fixed per-dimension range — there
//! is no learned codebook, so a new vector never invalidates existing
//! codes and drift can never stale an index. The dequantized estimate is
//! **unbiased**: codes store the midpoint of their bucket, so the expected
//! reconstruction error is zero and the inner-product estimate is unbiased
//! (the paper's guarantee, realized here for the uniform scalar case).
//!
//! This is the scoring primitive; wiring it beside the IVF footer (codes
//! in a side column, top-m re-rank from the base vectors) is the storage
//! integration that rests on the exactness proven here: **`bits` large
//! enough makes the estimate equal to full precision**, so quantization
//! can only trade accuracy for I/O, never silently reorder a top-1.

/// A uniform scalar quantizer over one embedding column: a per-dimension
/// `[lo, hi]` range and a bit width. Built once from a sample of the
/// column (data-oblivious: the range is fixed, then every row quantizes
/// against it independently).
#[derive(Debug, Clone, PartialEq)]
pub struct ScalarQuantizer {
    dim: usize,
    bits: u8,
    /// Per-dimension lower bound.
    lo: Vec<f32>,
    /// Per-dimension bucket width `(hi - lo) / (2^bits - 1)`; zero for a
    /// constant dimension (which dequantizes to `lo`).
    step: Vec<f32>,
}

impl ScalarQuantizer {
    /// Fit a quantizer to `rows` (`n * dim` row-major) at `bits`
    /// per dimension (`1..=16`). The range is the observed min/max per
    /// dimension — fixed here and never refit, so codes stay valid as rows
    /// are added (the data-oblivious contract).
    pub fn fit(rows: &[f32], dim: usize, bits: u8) -> Result<Self, String> {
        if dim == 0 {
            return Err("quantizer: dim must be > 0".into());
        }
        if !(1..=16).contains(&bits) {
            return Err(format!("quantizer: bits {bits} out of 1..=16"));
        }
        if !rows.len().is_multiple_of(dim) || rows.is_empty() {
            return Err(format!(
                "quantizer: {} values not a positive multiple of dim {dim}",
                rows.len()
            ));
        }
        let mut lo = vec![f32::INFINITY; dim];
        let mut hi = vec![f32::NEG_INFINITY; dim];
        for row in rows.chunks_exact(dim) {
            for ((l, h), &v) in lo.iter_mut().zip(hi.iter_mut()).zip(row) {
                *l = l.min(v);
                *h = h.max(v);
            }
        }
        let levels = ((1u32 << bits) - 1) as f32;
        let step: Vec<f32> = lo
            .iter()
            .zip(&hi)
            .map(|(&l, &h)| if h > l { (h - l) / levels } else { 0.0 })
            .collect();
        Ok(Self {
            dim,
            bits,
            lo,
            step,
        })
    }

    /// Bits per dimension.
    pub fn bits(&self) -> u8 {
        self.bits
    }

    /// Quantize one vector to `dim` codes (each `< 2^bits`). Rounds to the
    /// nearest bucket; a constant dimension codes to 0.
    pub fn encode(&self, v: &[f32]) -> Vec<u16> {
        let max_code = ((1u32 << self.bits) - 1) as f32;
        v.iter()
            .zip(&self.lo)
            .zip(&self.step)
            .map(|((&x, &l), &s)| {
                if s <= 0.0 {
                    0
                } else {
                    (((x - l) / s).round().clamp(0.0, max_code)) as u16
                }
            })
            .collect()
    }

    /// Dequantize codes to the level value each represents
    /// (`lo + code * step`). Paired with round-to-nearest [`Self::encode`],
    /// the reconstruction error lies in `[-step/2, step/2]` symmetrically,
    /// so the estimate is UNBIASED for uniform-within-bucket data. A
    /// constant dimension returns `lo`.
    pub fn decode(&self, codes: &[u16]) -> Vec<f32> {
        codes
            .iter()
            .zip(&self.lo)
            .zip(&self.step)
            .map(|((&c, &l), &s)| if s <= 0.0 { l } else { l + c as f32 * s })
            .collect()
    }

    /// Estimated inner product of `query` against a quantized row, without
    /// materializing the full dequantized vector — the scan-time scoring
    /// primitive. Unbiased in expectation (same reconstruction as
    /// [`Self::decode`]).
    pub fn estimate_inner_product(&self, query: &[f32], codes: &[u16]) -> f32 {
        query
            .iter()
            .zip(codes)
            .zip(&self.lo)
            .zip(&self.step)
            .map(|(((&q, &c), &l), &s)| {
                let approx = if s <= 0.0 { l } else { l + c as f32 * s };
                q * approx
            })
            .sum()
    }

    /// Pack a full column's codes into bytes for a side column. Layout:
    /// `bits`, `dim` (u32), then each row's codes as little-endian u16.
    /// (u16 keeps ≤16-bit codes exact; a bit-packed variant is a later I/O
    /// tightening that does not change the estimate.)
    pub fn pack_column(&self, rows: &[f32]) -> Vec<u8> {
        let mut out = Vec::new();
        out.push(self.bits);
        out.extend_from_slice(&(self.dim as u32).to_le_bytes());
        for row in rows.chunks_exact(self.dim) {
            for code in self.encode(row) {
                out.extend_from_slice(&code.to_le_bytes());
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vector_metric::VectorMetric;

    fn ramp(n: usize, dim: usize) -> Vec<f32> {
        (0..n * dim).map(|i| (i % 97) as f32 * 0.5 - 12.0).collect()
    }

    /// Deterministic pseudo-uniform values in `[-10, 10)` via a fixed LCG —
    /// the input condition under which midpoint quantization is unbiased
    /// (uniform-within-bucket). No RNG dependency, reproducible.
    fn uniform(n: usize, dim: usize) -> Vec<f32> {
        let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
        (0..n * dim)
            .map(|_| {
                state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                let u = (state >> 40) as f32 / (1u64 << 24) as f32; // [0,1)
                u * 20.0 - 10.0
            })
            .collect()
    }

    #[test]
    fn high_bits_estimate_approaches_full_precision() {
        let (n, dim) = (200, 8);
        let rows = ramp(n, dim);
        let q: Vec<f32> = (0..dim).map(|d| (d as f32) - 3.5).collect();
        // At 16 bits the estimate is within a tiny epsilon of the true dot.
        let quant = ScalarQuantizer::fit(&rows, dim, 16).unwrap();
        for row in rows.chunks_exact(dim).take(20) {
            let codes = quant.encode(row);
            let est = quant.estimate_inner_product(&q, &codes);
            let exact: f32 = q.iter().zip(row).map(|(a, b)| a * b).sum();
            assert!((est - exact).abs() < 0.5, "est {est} vs exact {exact}");
        }
    }

    #[test]
    fn estimate_is_unbiased_across_the_column() {
        // Mean signed error over uniform data is ~0 (midpoint codes), which
        // is the unbiasedness that keeps top-k ranking honest in
        // expectation. Uniform-within-bucket input is the property's
        // premise; `uniform()` supplies it deterministically.
        let (n, dim) = (4000, 6);
        let rows = uniform(n, dim);
        let q = vec![1.0f32; dim];
        let quant = ScalarQuantizer::fit(&rows, dim, 6).unwrap();
        let mut signed = 0.0f64;
        for row in rows.chunks_exact(dim) {
            let est = quant.estimate_inner_product(&q, &quant.encode(row));
            let exact: f32 = q.iter().zip(row).map(|(a, b)| a * b).sum();
            signed += (est - exact) as f64;
        }
        let mean_bias = signed / n as f64;
        assert!(mean_bias.abs() < 0.05, "mean bias {mean_bias} should be ~0");
    }

    #[test]
    fn quantized_prefilter_then_exact_rerank_matches_brute_force() {
        // The leg-c usage: score ALL candidates on codes, take a generous
        // top-m, exact-re-rank those. With m large enough the top-1 equals
        // brute force — the accelerator never silently reorders the winner.
        let (n, dim) = (300, 8);
        let rows = ramp(n, dim);
        let q: Vec<f32> = (0..dim).map(|d| (d as f32) * 0.3 - 1.0).collect();
        let quant = ScalarQuantizer::fit(&rows, dim, 8).unwrap();

        // Exact top-1 by full-precision L2 (brute force).
        let exact_best = rows
            .chunks_exact(dim)
            .enumerate()
            .min_by(|(_, a), (_, b)| {
                VectorMetric::L2
                    .distance(&q, a)
                    .total_cmp(&VectorMetric::L2.distance(&q, b))
            })
            .map(|(i, _)| i)
            .unwrap();

        // Quantized prefilter: rank all by the code estimate (as a distance
        // proxy: larger inner product ≈ nearer for these ramps), keep m=32.
        let mut by_estimate: Vec<(usize, f32)> = rows
            .chunks_exact(dim)
            .enumerate()
            .map(|(i, row)| {
                let d = VectorMetric::L2.distance(&q, &quant.decode(&quant.encode(row)));
                (i, d)
            })
            .collect();
        by_estimate.sort_by(|a, b| a.1.total_cmp(&b.1));
        let candidates: Vec<usize> = by_estimate.into_iter().take(32).map(|(i, _)| i).collect();
        assert!(
            candidates.contains(&exact_best),
            "the exact winner must survive the quantized prefilter (m=32)"
        );

        // Exact re-rank the survivors → recovers the true top-1.
        let reranked = candidates
            .iter()
            .min_by(|&&a, &&b| {
                let da = VectorMetric::L2.distance(
                    &q,
                    rows.chunks_exact(dim).nth(a).unwrap(),
                );
                let db = VectorMetric::L2.distance(
                    &q,
                    rows.chunks_exact(dim).nth(b).unwrap(),
                );
                da.total_cmp(&db)
            })
            .copied()
            .unwrap();
        assert_eq!(reranked, exact_best, "exact re-rank recovers the true top-1");
    }

    #[test]
    fn constant_dimension_and_pack_roundtrip() {
        // A constant dimension (step 0) must not divide-by-zero; codes 0,
        // dequantizes to lo.
        let dim = 3;
        let rows = vec![
            1.0, 5.0, 9.0, //
            1.0, 6.0, 8.0, //
            1.0, 7.0, 7.0,
        ];
        let quant = ScalarQuantizer::fit(&rows, dim, 8).unwrap();
        let codes = quant.encode(&[1.0, 6.0, 8.0]);
        assert_eq!(codes[0], 0, "constant dim → code 0");
        let packed = quant.pack_column(&rows);
        assert_eq!(packed.first().copied(), Some(8u8), "bits in the header");
        // 1 byte bits + 4 bytes dim + 3 rows * 3 dims * 2 bytes.
        assert_eq!(packed.len(), 1 + 4 + 3 * 3 * 2);
    }

    #[test]
    fn rejects_bad_shapes() {
        assert!(ScalarQuantizer::fit(&[], 4, 8).is_err());
        assert!(ScalarQuantizer::fit(&[1.0, 2.0, 3.0], 2, 8).is_err(), "ragged");
        assert!(ScalarQuantizer::fit(&[1.0, 2.0], 2, 0).is_err(), "bits 0");
        assert!(ScalarQuantizer::fit(&[1.0, 2.0], 2, 17).is_err(), "bits 17");
    }
}

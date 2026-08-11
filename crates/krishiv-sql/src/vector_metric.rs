#![forbid(unsafe_code)]
//! The distance metric shared by the vector distance built-ins
//! (`vector_functions`) and the IVF index (`vector_index`) — one
//! definition so a query's `ORDER BY cosine_distance(...)` and the index
//! that accelerates it can never disagree on what "nearest" means.

/// A vector distance metric. Lower is nearer for both.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorMetric {
    /// Squared Euclidean (monotonic in Euclidean, cheaper — ordering
    /// identical, which is all a nearest-neighbor search needs).
    L2,
    /// Cosine distance `1 − cos θ` in `[0, 2]`; a zero-magnitude vector
    /// has undefined direction and yields the max distance so it never
    /// ranks as a neighbor.
    Cosine,
}

impl VectorMetric {
    /// Distance between two equal-length vectors. Callers guarantee the
    /// lengths match (the index stores one dim; the built-ins check per
    /// row); a mismatch here zips to the shorter, never panics.
    pub fn distance(self, a: &[f32], b: &[f32]) -> f32 {
        match self {
            VectorMetric::L2 => a
                .iter()
                .zip(b)
                .map(|(x, y)| {
                    let d = x - y;
                    d * d
                })
                .sum(),
            VectorMetric::Cosine => {
                let (mut dot, mut na, mut nb) = (0.0f32, 0.0f32, 0.0f32);
                for (x, y) in a.iter().zip(b) {
                    dot += x * y;
                    na += x * x;
                    nb += y * y;
                }
                let denom = na.sqrt() * nb.sqrt();
                if denom > 0.0 {
                    1.0 - dot / denom
                } else {
                    2.0 // undefined direction → farthest, never a neighbor
                }
            }
        }
    }

    /// Stable one-byte tag for footer serialization.
    pub fn tag(self) -> u8 {
        match self {
            VectorMetric::L2 => 1,
            VectorMetric::Cosine => 2,
        }
    }

    /// Inverse of [`Self::tag`]; `None` for an unknown tag (a foreign
    /// footer degrades to brute force rather than misinterpreting cells).
    pub fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            1 => Some(VectorMetric::L2),
            2 => Some(VectorMetric::Cosine),
            _ => None,
        }
    }
}

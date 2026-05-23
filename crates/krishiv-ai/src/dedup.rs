use std::collections::HashSet;

/// Semantic deduplication strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DedupStrategy {
    KeepFirst,
    KeepLast,
    KeepHighestScore,
}

/// Semantic dedup configuration.
#[derive(Debug, Clone)]
pub struct SemanticDedupConfig {
    pub threshold: f32,
    pub strategy: DedupStrategy,
}

/// Cosine-similarity semantic deduplication operator.
#[derive(Debug, Clone)]
pub struct SemanticDedup {
    pub config: SemanticDedupConfig,
}

impl SemanticDedup {
    /// Create a semantic dedup operator.
    pub fn new(config: SemanticDedupConfig) -> Self {
        Self { config }
    }

    fn cosine(a: &[f32], b: &[f32]) -> f32 {
        let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
        let na = a.iter().map(|x| x * x).sum::<f32>().sqrt();
        let nb = b.iter().map(|x| x * x).sum::<f32>().sqrt();
        if na == 0.0 || nb == 0.0 {
            0.0
        } else {
            dot / (na * nb)
        }
    }

    fn band_hash(slice: &[f32]) -> u64 {
        use std::hash::Hasher;
        let mut hasher = twox_hash::XxHash64::with_seed(0);
        for v in slice {
            hasher.write(&v.to_le_bytes());
        }
        hasher.finish()
    }

    fn lsh_candidates(embeddings: &[Vec<f32>]) -> Vec<(usize, usize)> {
        let bands = 10usize;
        let width = 8usize.max(1);
        let mut buckets: std::collections::HashMap<u64, Vec<usize>> =
            std::collections::HashMap::new();
        for (idx, emb) in embeddings.iter().enumerate() {
            for band in 0..bands {
                let start = (band * width).min(emb.len());
                let end = (start + width).min(emb.len());
                if start >= end {
                    continue;
                }
                let key = Self::band_hash(&emb[start..end]);
                buckets.entry(key).or_default().push(idx);
            }
        }
        let mut pairs = Vec::new();
        for members in buckets.values() {
            for i in 0..members.len() {
                for j in (i + 1)..members.len() {
                    pairs.push((members[i], members[j]));
                }
            }
        }
        pairs
    }

    /// Return indices to keep after deduplication.
    pub fn dedup_indices(&self, embeddings: &[Vec<f32>], scores: &[f32]) -> Vec<usize> {
        if embeddings.is_empty() {
            return Vec::new();
        }
        let pairs = if embeddings.len() < 1000 {
            let mut pairs = Vec::new();
            for i in 0..embeddings.len() {
                for j in (i + 1)..embeddings.len() {
                    if Self::cosine(&embeddings[i], &embeddings[j]) >= self.config.threshold {
                        pairs.push((i, j));
                    }
                }
            }
            pairs
        } else {
            Self::lsh_candidates(embeddings)
                .into_iter()
                .filter(|(i, j)| Self::cosine(&embeddings[*i], &embeddings[*j]) >= self.config.threshold)
                .collect()
        };
        let mut drop: HashSet<usize> = HashSet::new();
        for (i, j) in pairs {
            match self.config.strategy {
                DedupStrategy::KeepFirst => {
                    drop.insert(j);
                }
                DedupStrategy::KeepLast => {
                    drop.insert(i);
                }
                DedupStrategy::KeepHighestScore => {
                    let si = scores.get(i).copied().unwrap_or(0.0);
                    let sj = scores.get(j).copied().unwrap_or(0.0);
                    if si >= sj {
                        drop.insert(j);
                    } else {
                        drop.insert(i);
                    }
                }
            }
        }
        (0..embeddings.len())
            .filter(|idx| !drop.contains(idx))
            .collect()
    }

    /// Backward-compatible wrapper when scores are unavailable.
    pub fn dedup_indices_unscored(&self, embeddings: &[Vec<f32>]) -> Vec<usize> {
        let scores = vec![0.0f32; embeddings.len()];
        self.dedup_indices(embeddings, &scores)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identical_embeddings_deduplicated() {
        let dedup = SemanticDedup::new(SemanticDedupConfig {
            threshold: 0.99,
            strategy: DedupStrategy::KeepFirst,
        });
        let embeddings = vec![vec![1.0, 0.0], vec![1.0, 0.0]];
        let kept = dedup.dedup_indices_unscored(&embeddings);
        assert_eq!(kept.len(), 1);
    }

    #[test]
    fn below_threshold_kept() {
        let dedup = SemanticDedup::new(SemanticDedupConfig {
            threshold: 0.99,
            strategy: DedupStrategy::KeepFirst,
        });
        let embeddings = vec![vec![1.0, 0.0], vec![0.0, 1.0]];
        let kept = dedup.dedup_indices_unscored(&embeddings);
        assert_eq!(kept.len(), 2);
    }
}

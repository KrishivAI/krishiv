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

    fn lsh_candidates(embeddings: &[Vec<f32>]) -> Vec<(usize, usize)> {
        let mut pairs = Vec::new();
        let bands = 10usize;
        let functions = 5usize;
        for i in 0..embeddings.len() {
            for j in (i + 1)..embeddings.len() {
                let mut same = 0usize;
                for b in 0..bands {
                    let hi = (b * functions).min(embeddings[i].len());
                    let lo = hi.saturating_sub(functions);
                    let sig_i: f32 = embeddings[i][lo..hi].iter().sum();
                    let sig_j: f32 = embeddings[j][lo..hi].iter().sum();
                    if (sig_i - sig_j).abs() < 0.01 {
                        same += 1;
                    }
                }
                if same >= bands / 2 {
                    pairs.push((i, j));
                }
            }
        }
        pairs
    }

    /// Return indices to keep after deduplication.
    pub fn dedup_indices(&self, embeddings: &[Vec<f32>]) -> Vec<usize> {
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
                    drop.insert(j);
                }
            }
        }
        (0..embeddings.len())
            .filter(|idx| !drop.contains(idx))
            .collect()
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
        let kept = dedup.dedup_indices(&embeddings);
        assert_eq!(kept.len(), 1);
    }

    #[test]
    fn below_threshold_kept() {
        let dedup = SemanticDedup::new(SemanticDedupConfig {
            threshold: 0.99,
            strategy: DedupStrategy::KeepFirst,
        });
        let embeddings = vec![vec![1.0, 0.0], vec![0.0, 1.0]];
        let kept = dedup.dedup_indices(&embeddings);
        assert_eq!(kept.len(), 2);
    }
}

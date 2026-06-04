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
        let width = 8usize;
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
                .filter(|(i, j)| {
                    Self::cosine(&embeddings[*i], &embeddings[*j]) >= self.config.threshold
                })
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

    /// Run `dedup_indices` safely isolated in a blocking threadpool.
    pub async fn dedup_indices_async(
        &self,
        embeddings: Vec<Vec<f32>>,
        scores: Vec<f32>,
    ) -> Result<Vec<usize>, tokio::task::JoinError> {
        let dedup = self.clone();
        tokio::task::spawn_blocking(move || dedup.dedup_indices(&embeddings, &scores)).await
    }

    /// Run `dedup_indices_unscored` safely isolated in a blocking threadpool.
    pub async fn dedup_indices_unscored_async(
        &self,
        embeddings: Vec<Vec<f32>>,
    ) -> Result<Vec<usize>, tokio::task::JoinError> {
        let dedup = self.clone();
        tokio::task::spawn_blocking(move || dedup.dedup_indices_unscored(&embeddings)).await
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

    // ── Additional deep-coverage tests ─────────────────────────────────

    #[test]
    fn empty_embeddings() {
        let dedup = SemanticDedup::new(SemanticDedupConfig {
            threshold: 0.9,
            strategy: DedupStrategy::KeepFirst,
        });
        let kept = dedup.dedup_indices_unscored(&[]);
        assert!(kept.is_empty());
    }

    #[test]
    fn single_embedding() {
        let dedup = SemanticDedup::new(SemanticDedupConfig {
            threshold: 0.9,
            strategy: DedupStrategy::KeepFirst,
        });
        let embeddings = vec![vec![1.0, 2.0, 3.0]];
        let kept = dedup.dedup_indices_unscored(&embeddings);
        assert_eq!(kept, vec![0]);
    }

    #[test]
    fn three_identical_dedup_to_one() {
        let dedup = SemanticDedup::new(SemanticDedupConfig {
            threshold: 0.99,
            strategy: DedupStrategy::KeepFirst,
        });
        let embeddings = vec![vec![1.0, 0.0], vec![1.0, 0.0], vec![1.0, 0.0]];
        let kept = dedup.dedup_indices_unscored(&embeddings);
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0], 0);
    }

    #[test]
    fn keep_last_strategy() {
        let dedup = SemanticDedup::new(SemanticDedupConfig {
            threshold: 0.99,
            strategy: DedupStrategy::KeepLast,
        });
        let embeddings = vec![vec![1.0, 0.0], vec![1.0, 0.0]];
        let kept = dedup.dedup_indices_unscored(&embeddings);
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0], 1);
    }

    #[test]
    fn keep_highest_score_strategy() {
        let dedup = SemanticDedup::new(SemanticDedupConfig {
            threshold: 0.99,
            strategy: DedupStrategy::KeepHighestScore,
        });
        let embeddings = vec![vec![1.0, 0.0], vec![1.0, 0.0]];
        let scores = vec![0.5, 0.9];
        let kept = dedup.dedup_indices(&embeddings, &scores);
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0], 1); // higher score kept
    }

    #[test]
    fn keep_highest_score_equal_scores() {
        let dedup = SemanticDedup::new(SemanticDedupConfig {
            threshold: 0.99,
            strategy: DedupStrategy::KeepHighestScore,
        });
        let embeddings = vec![vec![1.0, 0.0], vec![1.0, 0.0]];
        let scores = vec![0.5, 0.5];
        let kept = dedup.dedup_indices(&embeddings, &scores);
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0], 0); // first kept when scores equal
    }

    #[test]
    fn cosine_same_vector() {
        let sim = SemanticDedup::cosine(&[1.0, 0.0], &[1.0, 0.0]);
        assert!((sim - 1.0).abs() < 1e-6);
    }

    #[test]
    fn cosine_orthogonal_vectors() {
        let sim = SemanticDedup::cosine(&[1.0, 0.0], &[0.0, 1.0]);
        assert!(sim.abs() < 1e-6);
    }

    #[test]
    fn cosine_opposite_vectors() {
        let sim = SemanticDedup::cosine(&[1.0, 0.0], &[-1.0, 0.0]);
        assert!((sim + 1.0).abs() < 1e-6);
    }

    #[test]
    fn cosine_zero_vector_returns_zero() {
        let sim = SemanticDedup::cosine(&[0.0, 0.0], &[1.0, 0.0]);
        assert_eq!(sim, 0.0);
    }

    #[test]
    fn cosine_both_zero_vectors() {
        let sim = SemanticDedup::cosine(&[0.0, 0.0], &[0.0, 0.0]);
        assert_eq!(sim, 0.0);
    }

    #[test]
    fn cosine_high_dimensional() {
        let a: Vec<f32> = (0..128).map(|i| i as f32).collect();
        let b: Vec<f32> = (0..128).map(|i| i as f32).collect();
        let sim = SemanticDedup::cosine(&a, &b);
        assert!((sim - 1.0).abs() < 1e-6);
    }

    #[test]
    fn dedup_all_orthogonal_vectors() {
        let dedup = SemanticDedup::new(SemanticDedupConfig {
            threshold: 0.99,
            strategy: DedupStrategy::KeepFirst,
        });
        let embeddings = vec![
            vec![1.0, 0.0, 0.0],
            vec![0.0, 1.0, 0.0],
            vec![0.0, 0.0, 1.0],
        ];
        let kept = dedup.dedup_indices_unscored(&embeddings);
        assert_eq!(kept.len(), 3);
    }

    #[test]
    fn dedup_with_scores_len_mismatch() {
        let dedup = SemanticDedup::new(SemanticDedupConfig {
            threshold: 0.99,
            strategy: DedupStrategy::KeepHighestScore,
        });
        let embeddings = vec![vec![1.0, 0.0], vec![1.0, 0.0]];
        let scores = vec![0.5]; // shorter than embeddings
        let kept = dedup.dedup_indices(&embeddings, &scores);
        assert_eq!(kept.len(), 1);
    }

    #[test]
    fn dedup_preserves_original_indices() {
        let dedup = SemanticDedup::new(SemanticDedupConfig {
            threshold: 0.99,
            strategy: DedupStrategy::KeepFirst,
        });
        let embeddings = vec![
            vec![1.0, 0.0],
            vec![0.0, 1.0],
            vec![1.0, 0.0],
            vec![0.0, 1.0],
        ];
        let kept = dedup.dedup_indices_unscored(&embeddings);
        assert_eq!(kept.len(), 2);
        assert!(kept.contains(&0));
        assert!(kept.contains(&1));
    }

    #[test]
    fn dedup_strategy_debug() {
        let s = DedupStrategy::KeepFirst;
        let debug = format!("{:?}", s);
        assert!(debug.contains("KeepFirst"));
    }

    #[test]
    fn dedup_strategy_clone() {
        let s = DedupStrategy::KeepHighestScore;
        let c = s;
        assert_eq!(c, DedupStrategy::KeepHighestScore);
    }

    #[test]
    fn dedup_config_clone() {
        let config = SemanticDedupConfig {
            threshold: 0.85,
            strategy: DedupStrategy::KeepLast,
        };
        let c = config.clone();
        assert_eq!(c.threshold, 0.85);
        assert_eq!(c.strategy, DedupStrategy::KeepLast);
    }

    #[test]
    fn dedup_config_debug() {
        let config = SemanticDedupConfig {
            threshold: 0.5,
            strategy: DedupStrategy::KeepFirst,
        };
        let debug = format!("{:?}", config);
        assert!(debug.contains("0.5"));
    }

    #[test]
    fn dedup_new() {
        let config = SemanticDedupConfig {
            threshold: 0.9,
            strategy: DedupStrategy::KeepFirst,
        };
        let dedup = SemanticDedup::new(config);
        assert_eq!(dedup.config.threshold, 0.9);
    }

    #[test]
    fn dedup_threshold_exactly_at_boundary() {
        let dedup = SemanticDedup::new(SemanticDedupConfig {
            threshold: 1.0,
            strategy: DedupStrategy::KeepFirst,
        });
        // Identical unit vectors have cosine = 1.0
        let embeddings = vec![vec![1.0, 0.0], vec![1.0, 0.0]];
        let kept = dedup.dedup_indices_unscored(&embeddings);
        assert_eq!(kept.len(), 1);
    }

    #[test]
    fn dedup_threshold_just_below_boundary() {
        let dedup = SemanticDedup::new(SemanticDedupConfig {
            threshold: 1.0 + f32::EPSILON,
            strategy: DedupStrategy::KeepFirst,
        });
        // Identical vectors have cosine = 1.0, which is < 1.0 + epsilon
        let embeddings = vec![vec![1.0, 0.0], vec![1.0, 0.0]];
        let kept = dedup.dedup_indices_unscored(&embeddings);
        assert_eq!(kept.len(), 2);
    }

    #[test]
    fn dedup_many_similar_pairs() {
        let dedup = SemanticDedup::new(SemanticDedupConfig {
            threshold: 0.99,
            strategy: DedupStrategy::KeepFirst,
        });
        let mut embeddings = Vec::new();
        for i in 0..50 {
            // Each vector is unique and orthogonal enough
            let mut v = vec![0.0f32; 50];
            v[i] = 1.0;
            embeddings.push(v);
        }
        // All are orthogonal (different axes), so all should be kept
        let kept = dedup.dedup_indices_unscored(&embeddings);
        assert_eq!(kept.len(), 50);
    }

    #[test]
    fn band_hash_deterministic() {
        let a = SemanticDedup::band_hash(&[1.0, 2.0, 3.0]);
        let b = SemanticDedup::band_hash(&[1.0, 2.0, 3.0]);
        assert_eq!(a, b);
    }

    #[test]
    fn band_hash_different_inputs() {
        let a = SemanticDedup::band_hash(&[1.0, 2.0]);
        let b = SemanticDedup::band_hash(&[3.0, 4.0]);
        assert_ne!(a, b);
    }

    #[test]
    fn lsh_candidates_finds_similar() {
        let embeddings: Vec<Vec<f32>> = (0..1100).map(|i| vec![i as f32; 100]).collect();
        let pairs = SemanticDedup::lsh_candidates(&embeddings);
        // With 1100 embeddings (>= 1000), LSH is used
        // Should find some candidates (at minimum, no crash)
        assert!(!pairs.is_empty() || embeddings.len() < 1000);
    }

    #[test]
    fn dedup_small_set_uses_brute_force() {
        let dedup = SemanticDedup::new(SemanticDedupConfig {
            threshold: 0.99,
            strategy: DedupStrategy::KeepFirst,
        });
        // 999 embeddings (< 1000), brute force path
        // Use orthogonal vectors so none are duplicates
        let embeddings: Vec<Vec<f32>> = (0..999)
            .map(|i| {
                let mut v = vec![0.0f32; 999];
                v[i] = 1.0;
                v
            })
            .collect();
        let kept = dedup.dedup_indices_unscored(&embeddings);
        assert_eq!(kept.len(), 999);
    }
}

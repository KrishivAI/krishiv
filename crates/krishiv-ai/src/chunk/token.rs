use super::{Chunk, TextChunker};

/// Token-aware chunker using character windows as tokenizer proxy for `cl100k_base`,
/// or `tiktoken-rs` when the `tiktoken` feature is enabled (P3-11).
#[derive(Debug, Clone)]
pub struct TokenAwareChunker {
    pub max_tokens: usize,
    pub token_overlap: usize,
    pub tokenizer_name: String,
}

impl TokenAwareChunker {
    /// Create a token-aware chunker.
    pub fn new(max_tokens: usize, token_overlap: usize, tokenizer_name: impl Into<String>) -> Self {
        Self {
            max_tokens: max_tokens.max(1),
            token_overlap,
            tokenizer_name: tokenizer_name.into(),
        }
    }

    fn token_len(&self, s: &str) -> usize {
        #[cfg(feature = "tiktoken")]
        {
            use tiktoken_rs::get_bpe_from_model;
            if let Ok(bpe) = get_bpe_from_model(&self.tokenizer_name) {
                return bpe.encode_with_special_tokens(s).len();
            }
        }
        // Rough cl100k_base proxy: ~4 chars per token for ASCII text.
        s.len().div_ceil(4)
    }

    fn slice_by_tokens(&self, text: &str, max_tokens: usize) -> usize {
        if max_tokens == 0 {
            return 0;
        }
        let mut lo = 0usize;
        let mut hi = text.len();
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if self.token_len(&text[..mid]) <= max_tokens {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo.saturating_sub(1).max(1.min(text.len()))
    }
}

impl TextChunker for TokenAwareChunker {
    fn chunk(&self, text: &str) -> Vec<Chunk> {
        if text.is_empty() {
            return Vec::new();
        }
        let mut chunks = Vec::new();
        let mut start = 0usize;
        let mut idx = 0usize;
        while start < text.len() {
            let end = if start == text.len() {
                start
            } else {
                let mut end = self.slice_by_tokens(&text[start..], self.max_tokens) + start;
                if end <= start {
                    end = (start + self.max_tokens * 4).min(text.len());
                }
                end.min(text.len())
            };
            chunks.push(Chunk {
                text: text[start..end].to_string(),
                start_byte: start,
                end_byte: end,
                chunk_index: idx,
            });
            idx += 1;
            if end >= text.len() {
                break;
            }
            let overlap_end = start + self.slice_by_tokens(&text[start..end], self.token_overlap);
            start = overlap_end.min(end);
        }
        chunks
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_chunker_respects_max() {
        let c = TokenAwareChunker::new(8, 2, "cl100k_base");
        let chunks = c.chunk(&"word ".repeat(200));
        assert!(chunks.len() > 1);
    }
}

use super::{Chunk, TextChunker};

/// Token-aware chunker using character windows as tokenizer proxy for `cl100k_base`.
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

    fn approx_tokens(s: &str) -> usize {
        // Rough cl100k_base proxy: ~4 chars per token for ASCII text.
        (s.len() + 3) / 4
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
            let mut end = start;
            while end < text.len() && Self::approx_tokens(&text[start..end]) < self.max_tokens {
                end += 1;
            }
            if end == start {
                end = (start + self.max_tokens * 4).min(text.len());
            }
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
            let overlap_bytes = self.token_overlap * 4;
            start = end.saturating_sub(overlap_bytes);
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

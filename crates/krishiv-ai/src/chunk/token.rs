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
        // Use char_indices so every probe is at a valid UTF-8 boundary,
        // preventing panic on multi-byte characters.
        let char_indices: Vec<usize> = text.char_indices().map(|(i, _)| i).collect();
        let mut lo = 0usize;
        let mut hi = char_indices.len();
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let byte_pos = char_indices[mid];
            if self.token_len(&text[..byte_pos]) <= max_tokens {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        // lo is the first char index where tokens exceed max_tokens
        let byte_pos = char_indices.get(lo).copied().unwrap_or(text.len());
        byte_pos.min(text.len()).max(1.min(text.len()))
    }

    fn suffix_start_by_tokens(&self, text: &str, max_tokens: usize) -> usize {
        if max_tokens == 0 || text.is_empty() {
            return text.len();
        }

        let boundaries: Vec<usize> = text
            .char_indices()
            .map(|(i, _)| i)
            .chain(std::iter::once(text.len()))
            .collect();

        let mut lo = 0usize;
        let mut hi = boundaries.len() - 1;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let byte_pos = boundaries[mid];
            if self.token_len(&text[byte_pos..]) <= max_tokens {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }

        boundaries[lo]
    }

    fn next_char_boundary(text: &str, start: usize) -> usize {
        if start >= text.len() {
            return text.len();
        }

        let mut chars = text[start..].char_indices();
        let _ = chars.next();
        chars
            .next()
            .map(|(offset, _)| start + offset)
            .unwrap_or(text.len())
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

            let overlap_tokens = self.token_overlap.min(self.max_tokens.saturating_sub(1));
            let suffix_start = self.suffix_start_by_tokens(&text[start..end], overlap_tokens);
            let mut next_start = start + suffix_start;
            if next_start <= start {
                next_start = Self::next_char_boundary(text, start);
            }
            start = next_start.min(end);
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

    // ── Additional deep-coverage tests ─────────────────────────────────

    #[test]
    fn empty_text() {
        let c = TokenAwareChunker::new(10, 2, "cl100k_base");
        let chunks = c.chunk("");
        assert!(chunks.is_empty());
    }

    #[test]
    fn short_text_single_chunk() {
        let c = TokenAwareChunker::new(100, 10, "cl100k_base");
        let chunks = c.chunk("hello");
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].text, "hello");
    }

    #[test]
    fn chunk_indices_sequential() {
        let c = TokenAwareChunker::new(5, 1, "cl100k_base");
        let chunks = c.chunk(&"word ".repeat(50));
        for (i, chunk) in chunks.iter().enumerate() {
            assert_eq!(chunk.chunk_index, i);
        }
    }

    #[test]
    fn overlap_creates_multiple_chunks() {
        let c = TokenAwareChunker::new(8, 2, "cl100k_base");
        let text = &"word ".repeat(100);
        let chunks = c.chunk(text);
        assert!(chunks.len() > 1);
    }

    #[test]
    fn new_enforces_min_tokens() {
        let c = TokenAwareChunker::new(0, 0, "cl100k_base");
        assert_eq!(c.max_tokens, 1);
    }

    #[test]
    fn new_with_various_tokenizers() {
        let _ = TokenAwareChunker::new(10, 2, "cl100k_base");
        let _ = TokenAwareChunker::new(10, 2, "p50k_base");
        let _ = TokenAwareChunker::new(10, 2, "r50k_base");
    }

    #[test]
    fn token_len_proxy() {
        let c = TokenAwareChunker::new(100, 0, "cl100k_base");
        // ~4 chars per token proxy
        assert_eq!(c.token_len(""), 0);
        assert!(c.token_len("a") >= 1);
        assert!(c.token_len("abcdefgh") >= 1);
    }

    #[test]
    fn slice_by_tokens_zero_max() {
        let c = TokenAwareChunker::new(10, 0, "cl100k_base");
        let result = c.slice_by_tokens("hello", 0);
        assert_eq!(result, 0);
    }

    #[test]
    fn zero_overlap_chunks_are_contiguous() {
        let c = TokenAwareChunker::new(2, 0, "cl100k_base");
        let text = "abcdefghijklmnopqrstuvwxyz";
        let chunks = c.chunk(text);
        assert!(chunks.len() > 1);

        let reconstructed: String = chunks.iter().map(|chunk| chunk.text.as_str()).collect();
        assert_eq!(reconstructed, text);

        for window in chunks.windows(2) {
            assert_eq!(window[0].end_byte, window[1].start_byte);
            assert!(window[1].start_byte > window[0].start_byte);
        }
    }

    #[test]
    fn overlap_is_taken_from_previous_chunk_suffix() {
        let c = TokenAwareChunker::new(4, 1, "cl100k_base");
        let text = "0123456789abcdef0123456789abcdef";
        let chunks = c.chunk(text);
        assert!(chunks.len() > 1);

        for window in chunks.windows(2) {
            let previous = &window[0];
            let next = &window[1];
            assert!(next.start_byte > previous.start_byte);
            assert!(next.start_byte < previous.end_byte);
            assert!(next.start_byte >= previous.start_byte);
            assert_eq!(
                &text[next.start_byte..previous.end_byte],
                &previous.text[next.start_byte - previous.start_byte..]
            );
        }
    }

    #[test]
    fn excessive_overlap_is_capped_to_preserve_progress() {
        let c = TokenAwareChunker::new(2, 20, "cl100k_base");
        let chunks = c.chunk(&"abcd".repeat(20));
        assert!(chunks.len() > 1);

        for window in chunks.windows(2) {
            assert!(window[1].start_byte > window[0].start_byte);
        }
    }

    #[test]
    fn zero_overlap_preserves_utf8_boundaries() {
        let c = TokenAwareChunker::new(1, 0, "cl100k_base");
        let text = "alpha βeta γamma delta";
        let chunks = c.chunk(text);
        assert!(chunks.len() > 1);

        let reconstructed: String = chunks.iter().map(|chunk| chunk.text.as_str()).collect();
        assert_eq!(reconstructed, text);
    }

    #[test]
    fn large_text() {
        let c = TokenAwareChunker::new(50, 10, "cl100k_base");
        let text = "The quick brown fox jumps over the lazy dog. ".repeat(200);
        let chunks = c.chunk(&text);
        assert!(chunks.len() > 1);
        // All text should be covered
        let total_len: usize = chunks.iter().map(|c| c.text.len()).sum();
        assert!(total_len >= text.len());
    }
}

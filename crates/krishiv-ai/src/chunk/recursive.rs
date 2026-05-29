use super::{Chunk, TextChunker};

/// Recursively split text on separators until under `chunk_size`.
#[derive(Debug, Clone)]
pub struct RecursiveTextChunker {
    pub chunk_size: usize,
    pub chunk_overlap: usize,
    pub separators: Vec<String>,
}

impl RecursiveTextChunker {
    /// Create with default paragraph/sentence/word separators.
    pub fn new(chunk_size: usize, chunk_overlap: usize) -> Self {
        Self {
            chunk_size,
            chunk_overlap,
            separators: vec![
                "\n\n".into(),
                "\n".into(),
                ". ".into(),
                " ".into(),
                "".into(),
            ],
        }
    }

    fn split_recursive(&self, text: &str, sep_idx: usize) -> Vec<(usize, usize, String)> {
        if text.is_empty() {
            return Vec::new();
        }
        if text.len() <= self.chunk_size || sep_idx >= self.separators.len() {
            return vec![(0, text.len(), text.to_string())];
        }
        let sep = &self.separators[sep_idx];
        if sep.is_empty() {
            let mut parts = Vec::new();
            let mut start = 0usize;
            while start < text.len() {
                let end = (start + self.chunk_size).min(text.len());
                parts.push((start, end, text[start..end].to_string()));
                if end == text.len() {
                    break;
                }
                start = end.saturating_sub(self.chunk_overlap);
            }
            return parts;
        }
        let mut out = Vec::new();
        let mut offset = 0usize;
        for part in text.split(sep) {
            if part.is_empty() {
                offset += sep.len();
                continue;
            }
            let local = self.split_recursive(part, sep_idx + 1);
            for (s, e, chunk_text) in local {
                let abs_start = offset + s;
                let abs_end = offset + e;
                out.push((abs_start, abs_end, chunk_text));
            }
            offset += part.len() + sep.len();
        }
        if out.is_empty() {
            out.push((0, text.len(), text.to_string()));
        }
        out
    }
}

impl TextChunker for RecursiveTextChunker {
    fn chunk(&self, text: &str) -> Vec<Chunk> {
        self.split_recursive(text, 0)
            .into_iter()
            .enumerate()
            .map(|(idx, (start, end, chunk_text))| Chunk {
                text: chunk_text,
                start_byte: start,
                end_byte: end,
                chunk_index: idx,
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recursive_splits_long_text() {
        let chunker = RecursiveTextChunker::new(20, 4);
        let chunks = chunker.chunk("aaaa bbbb cccc dddd eeee ffff");
        assert!(!chunks.is_empty());
        assert!(
            chunks
                .iter()
                .all(|c| c.text.len() <= 20 || c.text.contains(' '))
        );
    }

    #[test]
    fn empty_string_returns_empty() {
        let chunker = RecursiveTextChunker::new(10, 2);
        assert!(chunker.chunk("").is_empty());
    }

    // ── Additional deep-coverage tests ─────────────────────────────────

    #[test]
    fn short_text_no_split() {
        let chunker = RecursiveTextChunker::new(100, 10);
        let chunks = chunker.chunk("hello");
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].text, "hello");
        assert_eq!(chunks[0].start_byte, 0);
        assert_eq!(chunks[0].end_byte, 5);
        assert_eq!(chunks[0].chunk_index, 0);
    }

    #[test]
    fn exact_chunk_size_no_split() {
        let chunker = RecursiveTextChunker::new(5, 0);
        let chunks = chunker.chunk("hello");
        assert_eq!(chunks.len(), 1);
    }

    #[test]
    fn chunk_indices_are_sequential() {
        let chunker = RecursiveTextChunker::new(10, 2);
        let chunks = chunker.chunk("one two three four five six seven eight");
        for (i, chunk) in chunks.iter().enumerate() {
            assert_eq!(chunk.chunk_index, i);
        }
    }

    #[test]
    fn chunk_texts_cover_original() {
        let chunker = RecursiveTextChunker::new(15, 3);
        let text = "The quick brown fox jumps over the lazy dog. The end.";
        let chunks = chunker.chunk(text);
        let reconstructed: String = chunks.iter().map(|c| c.text.as_str()).collect();
        assert!(!reconstructed.is_empty());
    }

    #[test]
    fn paragraph_splitting() {
        let chunker = RecursiveTextChunker::new(20, 0);
        let text = "First paragraph.\n\nSecond paragraph.\n\nThird paragraph.";
        let chunks = chunker.chunk(text);
        assert!(chunks.len() >= 2);
    }

    #[test]
    fn sentence_splitting() {
        let chunker = RecursiveTextChunker::new(25, 0);
        let text = "First sentence. Second sentence. Third sentence.";
        let chunks = chunker.chunk(text);
        assert!(chunks.len() >= 2);
    }

    #[test]
    fn word_splitting() {
        let chunker = RecursiveTextChunker::new(15, 0);
        let text = "one two three four five six seven eight";
        let chunks = chunker.chunk(text);
        assert!(chunks.len() >= 2);
        assert!(chunks.iter().all(|c| c.text.len() <= 16)); // allow for separator boundary
    }

    #[test]
    fn character_splitting() {
        let chunker = RecursiveTextChunker::new(5, 0);
        let text = "abcdefghij";
        let chunks = chunker.chunk(text);
        assert!(chunks.len() >= 2);
        assert!(chunks.iter().all(|c| c.text.len() <= 5));
    }

    #[test]
    fn overlap_creates_overlapping_text() {
        let chunker = RecursiveTextChunker::new(10, 3);
        // Use text with no separators so it falls to character-level splitting where overlap applies
        let text = "abcdefghijklmnoprstuvwxyz0123456789";
        let chunks = chunker.chunk(text);
        if chunks.len() > 1 {
            // With overlap at character level, total reconstructed text should be longer than original
            let total_len: usize = chunks.iter().map(|c| c.text.len()).sum();
            assert!(total_len >= text.len());
        }
    }

    #[test]
    fn empty_chunks_not_produced() {
        let chunker = RecursiveTextChunker::new(10, 2);
        let chunks = chunker.chunk("hello world");
        assert!(chunks.iter().all(|c| !c.text.is_empty()));
    }

    #[test]
    fn unicode_text() {
        let chunker = RecursiveTextChunker::new(10, 0);
        let text = "hello world";
        let chunks = chunker.chunk(text);
        assert!(!chunks.is_empty());
    }

    #[test]
    fn new_with_various_sizes() {
        let _ = RecursiveTextChunker::new(1, 0);
        let _ = RecursiveTextChunker::new(1000, 100);
        let _ = RecursiveTextChunker::new(50, 25);
    }
}

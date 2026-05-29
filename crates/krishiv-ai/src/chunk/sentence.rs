use super::{Chunk, TextChunker};

/// Sentence-boundary chunker.
#[derive(Debug, Clone)]
pub struct SentenceChunker {
    pub max_sentences_per_chunk: usize,
    pub sentence_overlap: usize,
}

impl SentenceChunker {
    /// Create a sentence chunker.
    pub fn new(max_sentences_per_chunk: usize, sentence_overlap: usize) -> Self {
        Self {
            max_sentences_per_chunk: max_sentences_per_chunk.max(1),
            sentence_overlap,
        }
    }

    fn sentences(text: &str) -> Vec<(usize, usize, String)> {
        let mut out = Vec::new();
        let mut start = 0usize;
        let bytes = text.as_bytes();
        let mut i = 0usize;
        while i < bytes.len() {
            let b = bytes[i];
            if (b == b'.' || b == b'!' || b == b'?')
                && i + 1 < bytes.len()
                && bytes[i + 1].is_ascii_whitespace()
            {
                let end = i + 1;
                out.push((start, end, text[start..=i].trim().to_string()));
                while i + 1 < bytes.len() && bytes[i + 1].is_ascii_whitespace() {
                    i += 1;
                }
                start = i + 1;
            }
            i += 1;
        }
        if start < text.len() {
            out.push((start, text.len(), text[start..].trim().to_string()));
        }
        if out.is_empty() && !text.is_empty() {
            out.push((0, text.len(), text.to_string()));
        }
        out
    }
}

impl TextChunker for SentenceChunker {
    fn chunk(&self, text: &str) -> Vec<Chunk> {
        let sentences = Self::sentences(text);
        if sentences.is_empty() {
            return Vec::new();
        }
        let step = self
            .max_sentences_per_chunk
            .saturating_sub(self.sentence_overlap)
            .max(1);
        let mut chunks = Vec::new();
        let mut idx = 0usize;
        let mut i = 0usize;
        while i < sentences.len() {
            let end = (i + self.max_sentences_per_chunk).min(sentences.len());
            let slice = &sentences[i..end];
            let start_byte = slice.first().map(|s| s.0).unwrap_or(0);
            let end_byte = slice.last().map(|s| s.1).unwrap_or(text.len());
            let combined = slice
                .iter()
                .map(|(_, _, s)| s.as_str())
                .collect::<Vec<_>>()
                .join(" ");
            chunks.push(Chunk {
                text: combined,
                start_byte,
                end_byte,
                chunk_index: idx,
            });
            idx += 1;
            if end == sentences.len() {
                break;
            }
            i += step;
        }
        chunks
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sentence_chunker_groups() {
        let c = SentenceChunker::new(2, 0);
        let chunks = c.chunk("Hello world. Second sentence! Third?");
        assert!(!chunks.is_empty());
    }

    // ── Additional deep-coverage tests ─────────────────────────────────

    #[test]
    fn empty_text() {
        let c = SentenceChunker::new(2, 0);
        let chunks = c.chunk("");
        assert!(chunks.is_empty());
    }

    #[test]
    fn single_sentence() {
        let c = SentenceChunker::new(2, 0);
        let chunks = c.chunk("Just one sentence.");
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].text, "Just one sentence.");
    }

    #[test]
    fn two_sentences_max_one() {
        let c = SentenceChunker::new(1, 0);
        let chunks = c.chunk("First. Second.");
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].text, "First.");
        assert_eq!(chunks[1].text, "Second.");
    }

    #[test]
    fn two_sentences_max_two() {
        let c = SentenceChunker::new(2, 0);
        let chunks = c.chunk("First. Second.");
        assert_eq!(chunks.len(), 1);
        assert!(chunks[0].text.contains("First"));
        assert!(chunks[0].text.contains("Second"));
    }

    #[test]
    fn sentences_with_exclamation() {
        let c = SentenceChunker::new(2, 0);
        let chunks = c.chunk("Hello! World!");
        assert_eq!(chunks.len(), 1);
    }

    #[test]
    fn sentences_with_question() {
        let c = SentenceChunker::new(2, 0);
        let chunks = c.chunk("Who? What?");
        assert_eq!(chunks.len(), 1);
    }

    #[test]
    fn mixed_punctuation() {
        let c = SentenceChunker::new(3, 0);
        let chunks = c.chunk("One. Two! Three? Four.");
        assert_eq!(chunks.len(), 2); // 4 sentences / 2 per chunk = 2 chunks
    }

    #[test]
    fn chunk_indices_sequential() {
        let c = SentenceChunker::new(1, 0);
        let chunks = c.chunk("A. B. C. D.");
        for (i, chunk) in chunks.iter().enumerate() {
            assert_eq!(chunk.chunk_index, i);
        }
    }

    #[test]
    fn no_overlap_step_is_max_sentences() {
        let c = SentenceChunker::new(2, 0);
        let chunks = c.chunk("A. B. C. D. E. F.");
        // step = 2 - 0 = 2
        assert!(chunks.len() >= 2);
    }

    #[test]
    fn overlap_reduces_step() {
        let c = SentenceChunker::new(3, 1);
        let chunks = c.chunk("A. B. C. D. E. F. G. H.");
        // step = 3 - 1 = 2
        assert!(chunks.len() >= 3);
    }

    #[test]
    fn new_enforces_min_one() {
        let c = SentenceChunker::new(0, 0);
        assert_eq!(c.max_sentences_per_chunk, 1);
    }

    #[test]
    fn text_without_sentence_endings() {
        let c = SentenceChunker::new(2, 0);
        let chunks = c.chunk("no punctuation here");
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].text, "no punctuation here");
    }

    #[test]
    fn sentence_boundaries_preserved() {
        let c = SentenceChunker::new(1, 0);
        let chunks = c.chunk("Hello world. Goodbye world.");
        assert_eq!(chunks[0].text, "Hello world.");
        assert_eq!(chunks[1].text, "Goodbye world.");
    }
}

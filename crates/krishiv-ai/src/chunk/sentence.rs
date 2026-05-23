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
}

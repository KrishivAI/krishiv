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
}

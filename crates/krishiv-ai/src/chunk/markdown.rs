use super::recursive::RecursiveTextChunker;
use super::{Chunk, TextChunker};

/// Markdown heading-aware chunker.
#[derive(Debug, Clone)]
pub struct MarkdownSectionChunker {
    pub min_heading_level: u8,
    pub max_chunk_size: Option<usize>,
}

impl MarkdownSectionChunker {
    /// Create a markdown section chunker.
    pub fn new(min_heading_level: u8, max_chunk_size: Option<usize>) -> Self {
        Self {
            min_heading_level: min_heading_level.clamp(1, 6),
            max_chunk_size,
        }
    }

    fn heading_level(line: &str) -> Option<u8> {
        let trimmed = line.trim_start();
        if !trimmed.starts_with('#') {
            return None;
        }
        let hashes = trimmed.chars().take_while(|c| *c == '#').count();
        if hashes == 0 || hashes > 6 {
            return None;
        }
        let rest = trimmed[hashes..].trim_start();
        if rest.is_empty() || !trimmed.as_bytes().get(hashes).is_some_and(|b| *b == b' ') {
            return None;
        }
        Some(hashes as u8)
    }
}

impl TextChunker for MarkdownSectionChunker {
    fn chunk(&self, text: &str) -> Vec<Chunk> {
        let mut sections: Vec<(usize, usize, String)> = Vec::new();
        let mut current_start = 0usize;
        let mut current = String::new();
        for (line_start, line) in text.lines().enumerate() {
            let byte_offset = text
                .lines()
                .take(line_start)
                .map(|l| l.len() + 1)
                .sum::<usize>();
            if let Some(level) = Self::heading_level(line) {
                if level >= self.min_heading_level && !current.is_empty() {
                    sections.push((current_start, byte_offset, std::mem::take(&mut current)));
                    current_start = byte_offset;
                }
            }
            if !current.is_empty() {
                current.push('\n');
            }
            current.push_str(line);
        }
        if !current.is_empty() {
            sections.push((current_start, text.len(), current));
        }
        if sections.is_empty() {
            sections.push((0, text.len(), text.to_string()));
        }
        let mut out = Vec::new();
        let mut chunk_index = 0usize;
        for (start, end, section_text) in sections {
            if let Some(max) = self.max_chunk_size {
                let sub = RecursiveTextChunker::new(max, max / 8);
                for sub_chunk in sub.chunk(&section_text) {
                    out.push(Chunk {
                        text: sub_chunk.text,
                        start_byte: start + sub_chunk.start_byte,
                        end_byte: start + sub_chunk.end_byte,
                        chunk_index,
                    });
                    chunk_index += 1;
                }
            } else {
                out.push(Chunk {
                    text: section_text,
                    start_byte: start,
                    end_byte: end,
                    chunk_index,
                });
                chunk_index += 1;
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn markdown_splits_on_headings() {
        let c = MarkdownSectionChunker::new(2, None);
        let md = "# Title\n\n## Section\n\nBody text.";
        let chunks = c.chunk(md);
        assert!(!chunks.is_empty());
    }
}

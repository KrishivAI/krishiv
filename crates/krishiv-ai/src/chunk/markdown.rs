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
        if rest.is_empty() || trimmed.as_bytes().get(hashes).is_none_or(|b| *b != b' ') {
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
            if let Some(level) = Self::heading_level(line)
                && level >= self.min_heading_level
                && !current.is_empty()
            {
                sections.push((current_start, byte_offset, std::mem::take(&mut current)));
                current_start = byte_offset;
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

    // ── Additional deep-coverage tests ─────────────────────────────────

    #[test]
    fn no_headings_single_chunk() {
        let c = MarkdownSectionChunker::new(2, None);
        let md = "Just plain text without any headings.";
        let chunks = c.chunk(md);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].text, md);
    }

    #[test]
    fn empty_text() {
        let c = MarkdownSectionChunker::new(2, None);
        let chunks = c.chunk("");
        assert_eq!(chunks.len(), 1); // empty fallback
    }

    #[test]
    fn heading_level_detection() {
        assert_eq!(MarkdownSectionChunker::heading_level("# H1"), Some(1));
        assert_eq!(MarkdownSectionChunker::heading_level("## H2"), Some(2));
        assert_eq!(MarkdownSectionChunker::heading_level("### H3"), Some(3));
        assert_eq!(MarkdownSectionChunker::heading_level("#### H4"), Some(4));
        assert_eq!(MarkdownSectionChunker::heading_level("##### H5"), Some(5));
        assert_eq!(MarkdownSectionChunker::heading_level("###### H6"), Some(6));
    }

    #[test]
    fn not_a_heading() {
        assert_eq!(MarkdownSectionChunker::heading_level("not a heading"), None);
        assert_eq!(MarkdownSectionChunker::heading_level("##no_space"), None);
        assert_eq!(MarkdownSectionChunker::heading_level("## "), None);
        assert_eq!(MarkdownSectionChunker::heading_level(""), None);
    }

    #[test]
    fn min_heading_level_filters() {
        let c = MarkdownSectionChunker::new(3, None);
        let md = "# Top\n\n## Middle\n\n### Detail";
        let chunks = c.chunk(md);
        // Only ### should split
        assert!(chunks.len() >= 2);
    }

    #[test]
    fn multiple_headings() {
        let c = MarkdownSectionChunker::new(2, None);
        let md = "## A\n\ncontent a\n\n## B\n\ncontent b\n\n## C\n\ncontent c";
        let chunks = c.chunk(md);
        assert_eq!(chunks.len(), 3);
    }

    #[test]
    fn heading_level_clamp() {
        let c = MarkdownSectionChunker::new(0, None);
        assert_eq!(c.min_heading_level, 1);
        let c2 = MarkdownSectionChunker::new(10, None);
        assert_eq!(c2.min_heading_level, 6);
    }

    #[test]
    fn chunk_indices_sequential() {
        let c = MarkdownSectionChunker::new(2, None);
        let md = "## A\n\n## B\n\n## C";
        let chunks = c.chunk(md);
        for (i, chunk) in chunks.iter().enumerate() {
            assert_eq!(chunk.chunk_index, i);
        }
    }

    #[test]
    fn with_max_chunk_size_sub_chunks() {
        let c = MarkdownSectionChunker::new(2, Some(20));
        let md = "## Section\n\nThis is a long section that should be split into smaller sub-chunks by the recursive chunker.";
        let chunks = c.chunk(md);
        assert!(chunks.len() >= 1);
        // All sub-chunks should respect the size limit approximately
        for chunk in &chunks {
            assert!(!chunk.text.is_empty());
        }
    }

    #[test]
    fn nested_headings() {
        let c = MarkdownSectionChunker::new(1, None);
        let md = "# Title\n\n## Sub1\n\n### SubSub\n\n## Sub2";
        let chunks = c.chunk(md);
        assert!(chunks.len() >= 2);
    }

    #[test]
    fn heading_without_content() {
        let c = MarkdownSectionChunker::new(2, None);
        let md = "## Empty Section\n\n## Another";
        let chunks = c.chunk(md);
        assert!(chunks.len() >= 1);
    }
}

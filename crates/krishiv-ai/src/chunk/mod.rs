mod markdown;
mod recursive;
mod sentence;
mod token;

pub use markdown::MarkdownSectionChunker;
pub use recursive::RecursiveTextChunker;
pub use sentence::SentenceChunker;
pub use token::TokenAwareChunker;

/// One text chunk with byte offsets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Chunk {
    pub text: String,
    pub start_byte: usize,
    pub end_byte: usize,
    pub chunk_index: usize,
}

/// Text chunking strategy.
pub trait TextChunker: Send + Sync {
    fn chunk(&self, text: &str) -> Vec<Chunk>;
}

mod huggingface;
mod openai;
mod registry;

pub use openai::OpenAiEmbeddingModel;
pub use registry::{
    EmbeddingDevice, EmbeddingError, EmbeddingModel, EmbeddingModelRegistry, ModelKey,
};

#[cfg(feature = "fastembed-local")]
pub use huggingface::HuggingFaceEmbeddingModel;

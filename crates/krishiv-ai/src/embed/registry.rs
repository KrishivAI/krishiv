use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex, OnceLock};

use async_trait::async_trait;

/// Device selection for local embedding models.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum EmbeddingDevice {
    Cpu,
    Gpu(u8),
    Mps,
}

/// Registry key including model name and device (ADR-R17.2).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ModelKey {
    pub model_name: String,
    pub device: EmbeddingDevice,
}

/// Embedding errors.
#[derive(Debug)]
pub enum EmbeddingError {
    Load(String),
    Inference(String),
    RateLimit(String),
    Http(String),
}

impl fmt::Display for EmbeddingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Load(m) => write!(f, "embedding load error: {m}"),
            Self::Inference(m) => write!(f, "embedding inference error: {m}"),
            Self::RateLimit(m) => write!(f, "embedding rate limit: {m}"),
            Self::Http(m) => write!(f, "embedding http error: {m}"),
        }
    }
}

impl std::error::Error for EmbeddingError {}

/// Embedding model contract.
#[async_trait]
pub trait EmbeddingModel: Send + Sync {
    fn model_name(&self) -> &str;
    fn embedding_dim(&self) -> usize;
    async fn embed_batch(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, EmbeddingError>;
}

static REGISTRY: OnceLock<Mutex<HashMap<ModelKey, Arc<dyn EmbeddingModel>>>> = OnceLock::new();

/// Process-level embedding model registry (ADR-R17.2).
pub struct EmbeddingModelRegistry;

impl EmbeddingModelRegistry {
    fn map() -> &'static Mutex<HashMap<ModelKey, Arc<dyn EmbeddingModel>>> {
        REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
    }

    /// Get or insert a model singleton.
    pub fn get_or_load<F>(
        key: ModelKey,
        loader: F,
    ) -> Result<Arc<dyn EmbeddingModel>, EmbeddingError>
    where
        F: FnOnce() -> Result<Arc<dyn EmbeddingModel>, EmbeddingError>,
    {
        let mut guard = Self::map()
            .lock()
            .map_err(|e| EmbeddingError::Load(e.to_string()))?;
        if let Some(model) = guard.get(&key) {
            return Ok(Arc::clone(model));
        }
        let model = loader()?;
        guard.insert(key, Arc::clone(&model));
        Ok(model)
    }

    /// Register a pre-built model (tests / Python bindings).
    pub fn register(key: ModelKey, model: Arc<dyn EmbeddingModel>) -> Result<(), EmbeddingError> {
        let mut guard = Self::map()
            .lock()
            .map_err(|e| EmbeddingError::Load(e.to_string()))?;
        guard.insert(key, model);
        Ok(())
    }
}

use std::collections::HashMap;
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
#[derive(Debug, thiserror::Error)]
pub enum EmbeddingError {
    #[error("embedding load error: {0}")]
    Load(String),
    #[error("embedding inference error: {0}")]
    Inference(String),
    #[error("embedding rate limit: {0}")]
    RateLimit(String),
    #[error("embedding http error: {0}")]
    Http(String),
}

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

#[cfg(test)]
mod tests {
    use super::*;

    struct DummyModel {
        name: String,
        dim: usize,
    }

    #[async_trait]
    impl EmbeddingModel for DummyModel {
        fn model_name(&self) -> &str {
            &self.name
        }
        fn embedding_dim(&self) -> usize {
            self.dim
        }
        async fn embed_batch(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, EmbeddingError> {
            Ok(texts.iter().map(|_| vec![0.0; self.dim]).collect())
        }
    }

    #[test]
    fn embedding_error_display() {
        let e1 = EmbeddingError::Load("bad model".into());
        assert!(e1.to_string().contains("load error"));
        assert!(e1.to_string().contains("bad model"));

        let e2 = EmbeddingError::Inference("runtime fault".into());
        assert!(e2.to_string().contains("inference error"));

        let e3 = EmbeddingError::RateLimit("too many".into());
        assert!(e3.to_string().contains("rate limit"));

        let e4 = EmbeddingError::Http("500".into());
        assert!(e4.to_string().contains("http error"));
    }

    #[test]
    fn embedding_error_is_std_error() {
        let err: Box<dyn std::error::Error> = Box::new(EmbeddingError::Load("test".into()));
        assert!(!err.to_string().is_empty());
    }

    #[test]
    fn model_key_equality() {
        let k1 = ModelKey {
            model_name: "m".into(),
            device: EmbeddingDevice::Cpu,
        };
        let k2 = ModelKey {
            model_name: "m".into(),
            device: EmbeddingDevice::Cpu,
        };
        assert_eq!(k1, k2);
    }

    #[test]
    fn model_key_inequality() {
        let k1 = ModelKey {
            model_name: "m1".into(),
            device: EmbeddingDevice::Cpu,
        };
        let k2 = ModelKey {
            model_name: "m2".into(),
            device: EmbeddingDevice::Cpu,
        };
        assert_ne!(k1, k2);
    }

    #[test]
    fn model_key_different_device() {
        let k1 = ModelKey {
            model_name: "m".into(),
            device: EmbeddingDevice::Cpu,
        };
        let k2 = ModelKey {
            model_name: "m".into(),
            device: EmbeddingDevice::Gpu(0),
        };
        assert_ne!(k1, k2);
    }

    #[test]
    fn model_key_hashable() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        let k1 = ModelKey {
            model_name: "m".into(),
            device: EmbeddingDevice::Cpu,
        };
        let k2 = ModelKey {
            model_name: "m".into(),
            device: EmbeddingDevice::Gpu(0),
        };
        set.insert(k1);
        set.insert(k2);
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn embedding_device_variants() {
        let cpu = EmbeddingDevice::Cpu;
        let gpu = EmbeddingDevice::Gpu(1);
        let mps = EmbeddingDevice::Mps;
        assert_eq!(cpu, EmbeddingDevice::Cpu);
        assert_eq!(gpu, EmbeddingDevice::Gpu(1));
        assert_eq!(mps, EmbeddingDevice::Mps);
    }

    #[test]
    fn embedding_device_debug() {
        let debug = format!("{:?}", EmbeddingDevice::Gpu(2));
        assert!(debug.contains("Gpu"));
        assert!(debug.contains("2"));
    }

    #[test]
    fn embedding_device_clone() {
        let d = EmbeddingDevice::Gpu(3);
        let c = d.clone();
        assert_eq!(c, EmbeddingDevice::Gpu(3));
    }

    #[test]
    fn model_key_debug() {
        let k = ModelKey {
            model_name: "test".into(),
            device: EmbeddingDevice::Cpu,
        };
        let debug = format!("{:?}", k);
        assert!(debug.contains("test"));
    }

    #[test]
    fn model_key_clone() {
        let k = ModelKey {
            model_name: "m".into(),
            device: EmbeddingDevice::Mps,
        };
        let c = k.clone();
        assert_eq!(c.model_name, "m");
        assert_eq!(c.device, EmbeddingDevice::Mps);
    }

    #[test]
    fn registry_returns_same_instance() {
        let key = ModelKey {
            model_name: "dummy_singleton".into(),
            device: EmbeddingDevice::Cpu,
        };
        let a = EmbeddingModelRegistry::get_or_load(key.clone(), || {
            Ok(Arc::new(DummyModel {
                name: "dummy_singleton".into(),
                dim: 8,
            }) as Arc<dyn EmbeddingModel>)
        })
        .unwrap();
        let b = EmbeddingModelRegistry::get_or_load(key, || {
            Err(EmbeddingError::Load("should not reload".into()))
        })
        .unwrap();
        assert!(Arc::ptr_eq(&a, &b));
    }

    #[test]
    fn registry_register_and_get() {
        let key = ModelKey {
            model_name: "registered_model".into(),
            device: EmbeddingDevice::Cpu,
        };
        let model: Arc<dyn EmbeddingModel> = Arc::new(DummyModel {
            name: "registered_model".into(),
            dim: 16,
        });
        EmbeddingModelRegistry::register(key.clone(), model).unwrap();
        let got = EmbeddingModelRegistry::get_or_load(key, || {
            Err(EmbeddingError::Load("should not be called".into()))
        })
        .unwrap();
        assert_eq!(got.model_name(), "registered_model");
        assert_eq!(got.embedding_dim(), 16);
    }
}

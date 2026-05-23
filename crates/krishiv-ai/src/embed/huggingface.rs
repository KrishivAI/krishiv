#[cfg(feature = "fastembed-local")]
mod imp {
    use std::sync::Arc;

    use async_trait::async_trait;
    use fastembed::{EmbeddingModel as FastModel, InitOptions, TextEmbedding};

    use crate::{EmbeddingDevice, EmbeddingError, EmbeddingModel};

    /// Local HuggingFace-compatible embeddings via fastembed (ADR-R17.2 singleton).
    pub struct HuggingFaceEmbeddingModel {
        inner: Arc<TextEmbedding>,
        model_name: String,
        dim: usize,
    }

    impl HuggingFaceEmbeddingModel {
        /// Load a sentence-transformers style model by name.
        pub fn load(
            model_name: impl Into<String>,
            device: EmbeddingDevice,
        ) -> Result<Self, EmbeddingError> {
            let name = model_name.into();
            let fast_model = match name.as_str() {
                "sentence-transformers/all-MiniLM-L6-v2" | "all-MiniLM-L6-v2" => {
                    FastModel::AllMiniLML6V2
                }
                _ => FastModel::AllMiniLML6V2,
            };
            let mut options = InitOptions::new(fast_model);
            if matches!(device, EmbeddingDevice::Mps) {
                options = options.with_show_download_progress(false);
            }
            let inner = TextEmbedding::try_new(options)
                .map_err(|e| EmbeddingError::Load(e.to_string()))?;
            let dim = inner
                .embed(vec!["probe"], None)
                .ok()
                .and_then(|v| v.first().map(|e| e.len()))
                .unwrap_or(384);
            Ok(Self {
                inner: Arc::new(inner),
                model_name: name,
                dim,
            })
        }
    }

    #[async_trait]
    impl EmbeddingModel for HuggingFaceEmbeddingModel {
        fn model_name(&self) -> &str {
            &self.model_name
        }

        fn embedding_dim(&self) -> usize {
            self.dim
        }

        async fn embed_batch(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, EmbeddingError> {
            let owned: Vec<String> = texts.to_vec();
            let inner = Arc::clone(&self.inner);
            tokio::task::spawn_blocking(move || {
                let refs: Vec<&str> = owned.iter().map(String::as_str).collect();
                inner
                    .embed(refs, None)
                    .map_err(|e| EmbeddingError::Inference(e.to_string()))
            })
            .await
            .map_err(|e| EmbeddingError::Inference(e.to_string()))?
        }
    }

    #[test]
    fn huggingface_model_loads_once_via_registry() {
        use crate::{EmbeddingModelRegistry, ModelKey};

        let key = ModelKey {
            model_name: "all-MiniLM-L6-v2".into(),
            device: EmbeddingDevice::Cpu,
        };
        let a = EmbeddingModelRegistry::get_or_load(key.clone(), || {
            Ok(Arc::new(HuggingFaceEmbeddingModel::load(
                "all-MiniLM-L6-v2",
                EmbeddingDevice::Cpu,
            )?)
                as Arc<dyn EmbeddingModel>)
        });
        let b = EmbeddingModelRegistry::get_or_load(key, || {
            Err(EmbeddingError::Load("should not reload".into()))
        });
        assert!(a.is_ok());
        assert!(b.is_ok());
        assert!(Arc::ptr_eq(&a.unwrap(), &b.unwrap()));
    }
}

#[cfg(feature = "fastembed-local")]
pub use imp::HuggingFaceEmbeddingModel;

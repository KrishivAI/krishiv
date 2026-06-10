//! R17 plan types: RAG index specs and hybrid feature store.

use serde::{Deserialize, Serialize};

use crate::PlanError;

/// Data source reference for RAG / feature store plans.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataSource {
    pub name: String,
    pub format: String,
    pub path: Option<String>,
}

impl DataSource {
    pub fn new(name: impl Into<String>, format: impl Into<String>) -> Result<Self, PlanError> {
        let src = Self {
            name: name.into(),
            format: format.into(),
            path: None,
        };
        src.validate()?;
        Ok(src)
    }

    pub fn with_path(mut self, path: impl Into<String>) -> Self {
        self.path = Some(path.into());
        self
    }

    pub fn validate(&self) -> Result<(), PlanError> {
        if self.name.trim().is_empty() {
            return Err(PlanError::Validation(String::from(
                "DataSource name must not be empty",
            )));
        }
        if self.format.trim().is_empty() {
            return Err(PlanError::Validation(String::from(
                "DataSource format must not be empty",
            )));
        }
        Ok(())
    }
}

/// Chunker configuration for RAG indexing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "strategy", rename_all = "snake_case")]
pub enum ChunkerConfig {
    RecursiveText {
        chunk_size: usize,
        overlap: usize,
    },
    Sentence {
        max_sentences: usize,
    },
    TokenAware {
        max_tokens: usize,
        tokenizer: String,
    },
    MarkdownSection {
        min_level: u8,
    },
}

impl ChunkerConfig {
    pub fn validate(&self) -> Result<(), PlanError> {
        match self {
            Self::RecursiveText {
                chunk_size,
                overlap,
            } => {
                if *chunk_size == 0 {
                    return Err(PlanError::Validation(String::from(
                        "RecursiveText chunk_size must be greater than zero",
                    )));
                }
                if *overlap >= *chunk_size {
                    return Err(PlanError::Validation(String::from(
                        "RecursiveText overlap must be less than chunk_size",
                    )));
                }
            }
            Self::Sentence { max_sentences } => {
                if *max_sentences == 0 {
                    return Err(PlanError::Validation(String::from(
                        "Sentence max_sentences must be greater than zero",
                    )));
                }
            }
            Self::TokenAware {
                max_tokens,
                tokenizer,
            } => {
                if *max_tokens == 0 {
                    return Err(PlanError::Validation(String::from(
                        "TokenAware max_tokens must be greater than zero",
                    )));
                }
                if tokenizer.trim().is_empty() {
                    return Err(PlanError::Validation(String::from(
                        "TokenAware tokenizer must not be empty",
                    )));
                }
            }
            Self::MarkdownSection { .. } => {}
        }
        Ok(())
    }
}

/// Embedder configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EmbedderConfig {
    pub model: String,
    pub api_key_env: Option<String>,
}

impl EmbedderConfig {
    pub fn new(model: impl Into<String>) -> Result<Self, PlanError> {
        let cfg = Self {
            model: model.into(),
            api_key_env: None,
        };
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn with_api_key_env(mut self, env_var: impl Into<String>) -> Self {
        self.api_key_env = Some(env_var.into());
        self
    }

    pub fn validate(&self) -> Result<(), PlanError> {
        if self.model.trim().is_empty() {
            return Err(PlanError::Validation(String::from(
                "EmbedderConfig model must not be empty",
            )));
        }
        Ok(())
    }
}

/// Vector sink configuration (delegates to krishiv-ai::vector_sinks JSON).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VectorSinkPlanConfig {
    pub sink_type: String,
    pub options: serde_json::Value,
}

impl VectorSinkPlanConfig {
    pub fn new(
        sink_type: impl Into<String>,
        options: serde_json::Value,
    ) -> Result<Self, PlanError> {
        let cfg = Self {
            sink_type: sink_type.into(),
            options,
        };
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn validate(&self) -> Result<(), PlanError> {
        if self.sink_type.trim().is_empty() {
            return Err(PlanError::Validation(String::from(
                "VectorSinkPlanConfig sink_type must not be empty",
            )));
        }
        Ok(())
    }
}

/// RAG refresh policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RefreshPolicy {
    Manual,
    Schedule { cron: String },
    Continuous,
}

impl RefreshPolicy {
    pub fn validate(&self) -> Result<(), PlanError> {
        if let Self::Schedule { cron } = self && cron.trim().is_empty() {
            return Err(PlanError::Validation(String::from(
                "RefreshPolicy::Schedule cron expression must not be empty",
            )));
        }
        Ok(())
    }
}

/// RAG index job specification.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RagIndexSpec {
    pub source: DataSource,
    pub chunker: ChunkerConfig,
    pub embedder: EmbedderConfig,
    pub vector_store: VectorSinkPlanConfig,
    pub refresh: RefreshPolicy,
}

impl RagIndexSpec {
    pub fn new(
        source: DataSource,
        chunker: ChunkerConfig,
        embedder: EmbedderConfig,
        vector_store: VectorSinkPlanConfig,
        refresh: RefreshPolicy,
    ) -> Result<Self, PlanError> {
        source.validate()?;
        chunker.validate()?;
        embedder.validate()?;
        vector_store.validate()?;
        refresh.validate()?;
        Ok(Self {
            source,
            chunker,
            embedder,
            vector_store,
            refresh,
        })
    }
}

/// Feature definition for the hybrid feature store.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeatureDef {
    pub name: String,
    /// Arrow data type tag: `"int32"`, `"int64"`, `"float64"`, `"utf8"`, `"bool"`.
    pub dtype: String,
    pub ttl_ms: Option<u64>,
}

const VALID_DTYPES: &[&str] = &["int32", "int64", "float64", "utf8", "bool"];

impl FeatureDef {
    pub fn new(
        name: impl Into<String>,
        dtype: impl Into<String>,
        ttl_ms: Option<u64>,
    ) -> Result<Self, PlanError> {
        let def = Self {
            name: name.into(),
            dtype: dtype.into(),
            ttl_ms,
        };
        def.validate()?;
        Ok(def)
    }

    pub fn validate(&self) -> Result<(), PlanError> {
        if self.name.trim().is_empty() {
            return Err(PlanError::Validation(String::from(
                "FeatureDef name must not be empty",
            )));
        }
        if !VALID_DTYPES.contains(&self.dtype.as_str()) {
            return Err(PlanError::Validation(format!(
                "FeatureDef dtype '{}' is not valid; expected one of: {}",
                self.dtype,
                VALID_DTYPES.join(", ")
            )));
        }
        if self.ttl_ms == Some(0) {
            return Err(PlanError::Validation(String::from(
                "FeatureDef ttl_ms must be greater than zero when set",
            )));
        }
        Ok(())
    }
}

/// Feature schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeatureSchema {
    pub features: Vec<FeatureDef>,
    pub entity_key: Vec<String>,
}

impl FeatureSchema {
    pub fn new(features: Vec<FeatureDef>, entity_key: Vec<String>) -> Result<Self, PlanError> {
        let schema = Self {
            features,
            entity_key,
        };
        schema.validate()?;
        Ok(schema)
    }

    pub fn validate(&self) -> Result<(), PlanError> {
        if self.entity_key.is_empty() {
            return Err(PlanError::Validation(String::from(
                "FeatureSchema entity_key must not be empty",
            )));
        }
        for key in &self.entity_key {
            if key.trim().is_empty() {
                return Err(PlanError::Validation(String::from(
                    "FeatureSchema entity_key contains an empty key",
                )));
            }
        }
        for feature in &self.features {
            feature.validate()?;
        }
        Ok(())
    }
}

/// Feature store plan object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeatureStore {
    pub name: String,
    pub batch_source: DataSource,
    pub stream_source: Option<DataSource>,
    pub feature_schema: FeatureSchema,
}

impl FeatureStore {
    pub fn new(
        name: impl Into<String>,
        batch_source: DataSource,
        feature_schema: FeatureSchema,
    ) -> Result<Self, PlanError> {
        let store = Self {
            name: name.into(),
            batch_source,
            stream_source: None,
            feature_schema,
        };
        store.validate()?;
        Ok(store)
    }

    pub fn with_stream_source(mut self, source: DataSource) -> Self {
        self.stream_source = Some(source);
        self
    }

    pub fn validate(&self) -> Result<(), PlanError> {
        if self.name.trim().is_empty() {
            return Err(PlanError::Validation(String::from(
                "FeatureStore name must not be empty",
            )));
        }
        self.batch_source.validate()?;
        if let Some(src) = &self.stream_source {
            src.validate()?;
        }
        self.feature_schema.validate()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_source() -> DataSource {
        DataSource::new("my_source", "parquet").unwrap()
    }

    fn valid_embedder() -> EmbedderConfig {
        EmbedderConfig::new("text-embedding-3-small").unwrap()
    }

    fn valid_chunker() -> ChunkerConfig {
        ChunkerConfig::RecursiveText {
            chunk_size: 512,
            overlap: 64,
        }
    }

    fn valid_sink() -> VectorSinkPlanConfig {
        VectorSinkPlanConfig::new("pinecone", serde_json::json!({"index": "my-index"})).unwrap()
    }

    fn valid_feature_def() -> FeatureDef {
        FeatureDef::new("revenue", "float64", None).unwrap()
    }

    fn valid_schema() -> FeatureSchema {
        FeatureSchema::new(vec![valid_feature_def()], vec!["user_id".into()]).unwrap()
    }

    #[test]
    fn data_source_rejects_empty_name() {
        assert!(DataSource::new("", "parquet").is_err());
    }

    #[test]
    fn data_source_rejects_empty_format() {
        assert!(DataSource::new("src", "").is_err());
    }

    #[test]
    fn data_source_with_path() {
        let src = DataSource::new("s", "parquet").unwrap().with_path("/data");
        assert_eq!(src.path.as_deref(), Some("/data"));
    }

    #[test]
    fn chunker_recursive_text_rejects_zero_chunk_size() {
        let c = ChunkerConfig::RecursiveText {
            chunk_size: 0,
            overlap: 0,
        };
        assert!(c.validate().is_err());
    }

    #[test]
    fn chunker_recursive_text_rejects_overlap_gte_chunk_size() {
        let c = ChunkerConfig::RecursiveText {
            chunk_size: 100,
            overlap: 100,
        };
        assert!(c.validate().is_err());
    }

    #[test]
    fn embedder_rejects_empty_model() {
        assert!(EmbedderConfig::new("").is_err());
    }

    #[test]
    fn vector_sink_rejects_empty_sink_type() {
        assert!(VectorSinkPlanConfig::new("", serde_json::json!({})).is_err());
    }

    #[test]
    fn refresh_policy_schedule_rejects_empty_cron() {
        let p = RefreshPolicy::Schedule {
            cron: String::new(),
        };
        assert!(p.validate().is_err());
    }

    #[test]
    fn rag_index_spec_valid() {
        let spec = RagIndexSpec::new(
            valid_source(),
            valid_chunker(),
            valid_embedder(),
            valid_sink(),
            RefreshPolicy::Continuous,
        );
        assert!(spec.is_ok());
    }

    #[test]
    fn feature_def_rejects_invalid_dtype() {
        let err = FeatureDef::new("rev", "decimal", None).unwrap_err();
        assert!(err.to_string().contains("dtype"));
    }

    #[test]
    fn feature_def_rejects_zero_ttl() {
        assert!(FeatureDef::new("rev", "float64", Some(0)).is_err());
    }

    #[test]
    fn feature_schema_rejects_empty_entity_key() {
        let err = FeatureSchema::new(vec![valid_feature_def()], vec![]).unwrap_err();
        assert!(err.to_string().contains("entity_key"));
    }

    #[test]
    fn feature_store_valid() {
        let store = FeatureStore::new("my_store", valid_source(), valid_schema());
        assert!(store.is_ok());
    }

    #[test]
    fn feature_store_rejects_empty_name() {
        assert!(FeatureStore::new("", valid_source(), valid_schema()).is_err());
    }

    #[test]
    fn r17_types_serde_roundtrip() {
        let spec = RagIndexSpec::new(
            valid_source(),
            valid_chunker(),
            valid_embedder(),
            valid_sink(),
            RefreshPolicy::Manual,
        )
        .unwrap();
        let json = serde_json::to_string(&spec).unwrap();
        let decoded: RagIndexSpec = serde_json::from_str(&json).unwrap();
        assert_eq!(spec, decoded);
    }
}

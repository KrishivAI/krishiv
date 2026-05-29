mod openai;
mod rate_limit;

pub use openai::OpenAiLlmUdf;
pub use rate_limit::LlmRateLimiter;

use std::fmt;

use async_trait::async_trait;

/// LLM UDF configuration.
#[derive(Debug, Clone)]
pub struct LlmUdfConfig {
    pub model: String,
    pub max_tokens: u32,
    pub temperature: f32,
    pub cache: bool,
    pub rate_limit: RateLimitConfig,
}

/// Rate limit settings for LLM calls.
#[derive(Debug, Clone)]
pub struct RateLimitConfig {
    pub requests_per_minute: u32,
    pub tokens_per_minute: u64,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            requests_per_minute: 60,
            tokens_per_minute: 90_000,
        }
    }
}

/// One LLM response row.
#[derive(Debug, Clone, PartialEq)]
pub struct LlmResponse {
    pub text: String,
    pub finish_reason: String,
    pub tokens_used: u32,
}

/// LLM errors.
#[derive(Debug)]
pub enum LlmError {
    Http(String),
    RateLimit(String),
    Parse(String),
}

impl fmt::Display for LlmError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Http(m) => write!(f, "llm http error: {m}"),
            Self::RateLimit(m) => write!(f, "llm rate limit: {m}"),
            Self::Parse(m) => write!(f, "llm parse error: {m}"),
        }
    }
}

impl std::error::Error for LlmError {}

/// LLM UDF trait.
#[async_trait]
pub trait LlmUdf: Send + Sync {
    async fn call_batch(&self, prompts: &[String]) -> Result<Vec<LlmResponse>, LlmError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn llm_udf_config_debug() {
        let config = LlmUdfConfig {
            model: "gpt-4o".into(),
            max_tokens: 256,
            temperature: 0.7,
            cache: true,
            rate_limit: RateLimitConfig::default(),
        };
        let debug = format!("{:?}", config);
        assert!(debug.contains("gpt-4o"));
        assert!(debug.contains("256"));
    }

    #[test]
    fn llm_udf_config_clone() {
        let config = LlmUdfConfig {
            model: "gpt-4".into(),
            max_tokens: 128,
            temperature: 0.0,
            cache: false,
            rate_limit: RateLimitConfig {
                requests_per_minute: 30,
                tokens_per_minute: 40_000,
            },
        };
        let cloned = config.clone();
        assert_eq!(cloned.model, "gpt-4");
        assert_eq!(cloned.max_tokens, 128);
        assert!(!cloned.cache);
        assert_eq!(cloned.rate_limit.requests_per_minute, 30);
    }

    #[test]
    fn llm_response_variants() {
        let r = LlmResponse {
            text: String::new(),
            finish_reason: String::new(),
            tokens_used: 0,
        };
        assert!(r.text.is_empty());
        assert!(r.finish_reason.is_empty());
        assert_eq!(r.tokens_used, 0);
    }

    #[test]
    fn rate_limit_config_all_fields() {
        let config = RateLimitConfig {
            requests_per_minute: 200,
            tokens_per_minute: 200_000,
        };
        assert_eq!(config.requests_per_minute, 200);
        assert_eq!(config.tokens_per_minute, 200_000);
    }
}

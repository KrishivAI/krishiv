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

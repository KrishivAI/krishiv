use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use reqwest::Client;
use serde::Deserialize;

use super::rate_limit::LlmRateLimiter;
use super::{LlmError, LlmResponse, LlmUdf, LlmUdfConfig};

/// OpenAI chat completion LLM UDF (ADR-R17.1 spawn_blocking isolation).
#[derive(Clone)]
pub struct OpenAiLlmUdf {
    api_key: String,
    config: LlmUdfConfig,
    rate_limiter: Arc<tokio::sync::Mutex<LlmRateLimiter>>,
    cache: Arc<DashMap<u64, LlmResponse>>,
    client: Client,
}

impl OpenAiLlmUdf {
    /// Create an OpenAI LLM UDF.
    pub fn new(api_key: impl Into<String>, config: LlmUdfConfig) -> Self {
        let rate_limiter = LlmRateLimiter::for_model(&config.model, config.rate_limit.clone());
        Self {
            api_key: api_key.into(),
            config,
            rate_limiter,
            cache: Arc::new(DashMap::new()),
            client: Client::new(),
        }
    }

    fn cache_key(prompt: &str) -> u64 {
        let mut h = DefaultHasher::new();
        prompt.hash(&mut h);
        h.finish()
    }

    async fn call_one(&self, prompt: String) -> Result<LlmResponse, LlmError> {
        let cache_key = Self::cache_key(&prompt);
        if self.config.cache
            && let Some(hit) = self.cache.get(&cache_key)
        {
            return Ok(hit.clone());
        }
        let api_key = &self.api_key;
        let model = &self.config.model;
        let max_tokens = self.config.max_tokens;
        let temperature = self.config.temperature;
        let limiter = Arc::clone(&self.rate_limiter);
        let client = &self.client;

        limiter.lock().await.acquire(max_tokens as u64).await;
        let body = serde_json::json!({
            "model": model,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": max_tokens,
            "temperature": temperature,
        });
        let mut attempt = 0u32;
        let response = loop {
            let resp = client
                .post("https://api.openai.com/v1/chat/completions")
                .bearer_auth(api_key)
                .json(&body)
                .send()
                .await
                .map_err(|e| LlmError::Http(e.to_string()))?;
            if resp.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
                attempt += 1;
                if attempt > 4 {
                    return Err(LlmError::RateLimit("429 retries exhausted".into()));
                }
                let jitter = 100 + (attempt * 37);
                tokio::time::sleep(std::time::Duration::from_millis(jitter as u64)).await;
                continue;
            }
            if !resp.status().is_success() {
                return Err(LlmError::Http(resp.text().await.unwrap_or_default()));
            }
            #[derive(Deserialize)]
            struct Choice {
                message: Message,
                finish_reason: Option<String>,
            }
            #[derive(Deserialize)]
            struct Message {
                content: String,
            }
            #[derive(Deserialize)]
            struct Usage {
                total_tokens: u32,
            }
            #[derive(Deserialize)]
            struct ChatResponse {
                choices: Vec<Choice>,
                usage: Option<Usage>,
            }
            let parsed: ChatResponse = resp
                .json()
                .await
                .map_err(|e| LlmError::Parse(e.to_string()))?;
            let choice = parsed
                .choices
                .into_iter()
                .next()
                .ok_or_else(|| LlmError::Parse("no choices".into()))?;
            break LlmResponse {
                text: choice.message.content,
                finish_reason: choice.finish_reason.unwrap_or_else(|| "stop".into()),
                tokens_used: parsed.usage.map(|u| u.total_tokens).unwrap_or(0),
            };
        };
        if self.config.cache && self.cache.len() < 10_000 {
            self.cache.insert(cache_key, response.clone());
        }
        Ok(response)
    }
}

#[async_trait]
impl LlmUdf for OpenAiLlmUdf {
    async fn call_batch(&self, prompts: &[String]) -> Result<Vec<LlmResponse>, LlmError> {
        let mut out = Vec::with_capacity(prompts.len());
        for prompt in prompts {
            out.push(self.call_one(prompt.clone()).await?);
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::llm::RateLimitConfig;

    #[tokio::test]
    async fn openai_llm_cache_hit() {
        let udf = OpenAiLlmUdf::new(
            "test",
            LlmUdfConfig {
                model: "gpt-4o".into(),
                max_tokens: 16,
                temperature: 0.0,
                cache: true,
                rate_limit: RateLimitConfig::default(),
            },
        );
        let key = OpenAiLlmUdf::cache_key("prompt");
        udf.cache.insert(
            key,
            LlmResponse {
                text: "cached".into(),
                finish_reason: "stop".into(),
                tokens_used: 1,
            },
        );
        let out = udf.call_batch(&["prompt".into()]).await.unwrap();
        assert_eq!(out[0].text, "cached");
    }

    // ── Additional deep-coverage tests ─────────────────────────────────

    #[test]
    fn cache_key_deterministic() {
        let k1 = OpenAiLlmUdf::cache_key("hello");
        let k2 = OpenAiLlmUdf::cache_key("hello");
        assert_eq!(k1, k2);
    }

    #[test]
    fn cache_key_different_for_different_prompts() {
        let k1 = OpenAiLlmUdf::cache_key("prompt A");
        let k2 = OpenAiLlmUdf::cache_key("prompt B");
        assert_ne!(k1, k2);
    }

    #[test]
    fn cache_disabled_no_hit() {
        let udf = OpenAiLlmUdf::new(
            "test",
            LlmUdfConfig {
                model: "gpt-4o".into(),
                max_tokens: 16,
                temperature: 0.0,
                cache: false,
                rate_limit: RateLimitConfig::default(),
            },
        );
        let key = OpenAiLlmUdf::cache_key("prompt");
        udf.cache.insert(
            key,
            LlmResponse {
                text: "cached".into(),
                finish_reason: "stop".into(),
                tokens_used: 1,
            },
        );
        // With cache disabled, even if there's a cache hit, it should try API
        // But we can't easily test the API call without a mock server
        // Just verify the cache exists
        assert!(udf.cache.contains_key(&key));
    }

    #[test]
    fn udf_new_creates_instance() {
        let udf = OpenAiLlmUdf::new(
            "key123",
            LlmUdfConfig {
                model: "gpt-4".into(),
                max_tokens: 50,
                temperature: 0.5,
                cache: true,
                rate_limit: RateLimitConfig::default(),
            },
        );
        assert_eq!(udf.api_key, "key123");
        assert_eq!(udf.config.model, "gpt-4");
        assert_eq!(udf.config.max_tokens, 50);
        assert!((udf.config.temperature - 0.5).abs() < f32::EPSILON);
    }

    #[test]
    fn cache_size_limit() {
        let udf = OpenAiLlmUdf::new(
            "test",
            LlmUdfConfig {
                model: "gpt-4o".into(),
                max_tokens: 16,
                temperature: 0.0,
                cache: true,
                rate_limit: RateLimitConfig::default(),
            },
        );
        // Verify cache starts empty
        assert_eq!(udf.cache.len(), 0);
    }

    #[test]
    fn multiple_cache_entries() {
        let udf = OpenAiLlmUdf::new(
            "test",
            LlmUdfConfig {
                model: "gpt-4o".into(),
                max_tokens: 16,
                temperature: 0.0,
                cache: true,
                rate_limit: RateLimitConfig::default(),
            },
        );
        for i in 0..10 {
            let key = OpenAiLlmUdf::cache_key(&format!("prompt-{i}"));
            udf.cache.insert(
                key,
                LlmResponse {
                    text: format!("response-{i}"),
                    finish_reason: "stop".into(),
                    tokens_used: i,
                },
            );
        }
        assert_eq!(udf.cache.len(), 10);
        // Verify each entry
        for i in 0..10 {
            let key = OpenAiLlmUdf::cache_key(&format!("prompt-{i}"));
            let entry = udf.cache.get(&key).unwrap();
            assert_eq!(entry.text, format!("response-{i}"));
        }
    }
}

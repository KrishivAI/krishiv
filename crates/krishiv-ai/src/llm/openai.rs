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
        let api_key = self.api_key.clone();
        let model = self.config.model.clone();
        let max_tokens = self.config.max_tokens;
        let temperature = self.config.temperature;
        let limiter = Arc::clone(&self.rate_limiter);
        let client = self.client.clone();
        let response = tokio::task::spawn_blocking(move || {
            tokio::runtime::Handle::current().block_on(async move {
                limiter.lock().await.acquire(max_tokens as u64).await;
                let body = serde_json::json!({
                    "model": model,
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": max_tokens,
                    "temperature": temperature,
                });
                let mut attempt = 0u32;
                loop {
                    let resp = client
                        .post("https://api.openai.com/v1/chat/completions")
                        .bearer_auth(&api_key)
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
                    return Ok(LlmResponse {
                        text: choice.message.content,
                        finish_reason: choice.finish_reason.unwrap_or_else(|| "stop".into()),
                        tokens_used: parsed.usage.map(|u| u.total_tokens).unwrap_or(0),
                    });
                }
            })
        })
        .await
        .map_err(|e| LlmError::Http(e.to_string()))??;
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
}

use std::sync::{Arc, OnceLock};

pub use crate::llm::RateLimitConfig;

/// Dual token-bucket rate limiter for LLM requests and tokens.
#[derive(Debug)]
pub struct LlmRateLimiter {
    config: RateLimitConfig,
    request_tokens: f64,
    token_tokens: f64,
    last_refill_ms: Option<u64>,
}

static GLOBAL_LIMITERS: OnceLock<std::sync::Mutex<std::collections::HashMap<String, Arc<tokio::sync::Mutex<LlmRateLimiter>>>>> =
    OnceLock::new();

impl LlmRateLimiter {
    /// Create a new limiter from config.
    pub fn new(config: RateLimitConfig) -> Self {
        Self {
            request_tokens: config.requests_per_minute as f64,
            token_tokens: config.tokens_per_minute as f64,
            last_refill_ms: None,
            config,
        }
    }

    /// Process-wide singleton per model name.
    pub fn for_model(model: &str, config: RateLimitConfig) -> Arc<tokio::sync::Mutex<Self>> {
        let map = GLOBAL_LIMITERS.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()));
        let mut guard = map.lock().expect("limiter map");
        guard
            .entry(model.to_string())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(Self::new(config))))
            .clone()
    }

    fn now_ms() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }

    fn refill(&mut self, now: u64) {
        if let Some(last) = self.last_refill_ms {
            let elapsed = now.saturating_sub(last);
            let req_rate = self.config.requests_per_minute as f64 / 60_000.0;
            let tok_rate = self.config.tokens_per_minute as f64 / 60_000.0;
            self.request_tokens =
                (self.request_tokens + elapsed as f64 * req_rate).min(self.config.requests_per_minute as f64);
            self.token_tokens =
                (self.token_tokens + elapsed as f64 * tok_rate).min(self.config.tokens_per_minute as f64);
        }
        self.last_refill_ms = Some(now);
    }

    /// Acquire capacity for one request using `token_estimate` tokens.
    pub async fn acquire(&mut self, token_estimate: u64) {
        loop {
            let now = Self::now_ms();
            self.refill(now);
            if self.request_tokens >= 1.0 && self.token_tokens >= token_estimate as f64 {
                self.request_tokens -= 1.0;
                self.token_tokens -= token_estimate as f64;
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    }

    /// Apply coordinator throttle command.
    pub fn apply_throttle(&mut self, max_requests_per_minute: u32, max_tokens_per_minute: u64) {
        self.config.requests_per_minute = max_requests_per_minute;
        self.config.tokens_per_minute = max_tokens_per_minute;
        self.request_tokens = self.request_tokens.min(max_requests_per_minute as f64);
        self.token_tokens = self.token_tokens.min(max_tokens_per_minute as f64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn llm_rate_limiter_enforces_requests() {
        let mut rl = LlmRateLimiter::new(RateLimitConfig {
            requests_per_minute: 2,
            tokens_per_minute: 10_000,
        });
        rl.acquire(10).await;
        rl.acquire(10).await;
        let start = std::time::Instant::now();
        rl.acquire(10).await;
        assert!(start.elapsed() >= std::time::Duration::from_millis(20));
    }
}

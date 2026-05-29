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

static GLOBAL_LIMITERS: OnceLock<
    std::sync::Mutex<std::collections::HashMap<String, Arc<tokio::sync::Mutex<LlmRateLimiter>>>>,
> = OnceLock::new();

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
        let map =
            GLOBAL_LIMITERS.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()));
        let mut guard = map.lock().unwrap_or_else(|e| {
            tracing::error!("LLM rate limiter map lock poisoned: {e}");
            e.into_inner()
        });
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
            self.request_tokens = (self.request_tokens + elapsed as f64 * req_rate)
                .min(self.config.requests_per_minute as f64);
            self.token_tokens = (self.token_tokens + elapsed as f64 * tok_rate)
                .min(self.config.tokens_per_minute as f64);
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
    use crate::llm::{LlmError, LlmResponse};

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

    // ── Additional deep-coverage tests ─────────────────────────────────

    #[test]
    fn rate_limiter_new() {
        let rl = LlmRateLimiter::new(RateLimitConfig {
            requests_per_minute: 100,
            tokens_per_minute: 50_000,
        });
        assert_eq!(rl.config.requests_per_minute, 100);
        assert_eq!(rl.config.tokens_per_minute, 50_000);
        assert_eq!(rl.request_tokens, 100.0);
        assert_eq!(rl.token_tokens, 50_000.0);
        assert!(rl.last_refill_ms.is_none());
    }

    #[tokio::test]
    async fn rate_limiter_acquire_depletes_tokens() {
        let mut rl = LlmRateLimiter::new(RateLimitConfig {
            requests_per_minute: 10,
            tokens_per_minute: 1000,
        });
        let before_req = rl.request_tokens;
        let before_tok = rl.token_tokens;
        rl.acquire(100).await;
        assert!(rl.request_tokens < before_req);
        assert!(rl.token_tokens < before_tok);
    }

    #[tokio::test]
    async fn rate_limiter_refill() {
        let mut rl = LlmRateLimiter::new(RateLimitConfig {
            requests_per_minute: 100,
            tokens_per_minute: 100_000,
        });
        rl.acquire(1000).await;
        let after_first = rl.request_tokens;
        // Simulate time passing
        rl.last_refill_ms = Some(
            rl.last_refill_ms
                .unwrap_or_else(|| LlmRateLimiter::now_ms())
                - 60_000,
        );
        rl.refill(LlmRateLimiter::now_ms());
        assert!(rl.request_tokens > after_first);
    }

    #[test]
    fn apply_throttle() {
        let mut rl = LlmRateLimiter::new(RateLimitConfig {
            requests_per_minute: 100,
            tokens_per_minute: 100_000,
        });
        rl.apply_throttle(50, 50_000);
        assert_eq!(rl.config.requests_per_minute, 50);
        assert_eq!(rl.config.tokens_per_minute, 50_000);
        assert!(rl.request_tokens <= 50.0);
        assert!(rl.token_tokens <= 50_000.0);
    }

    #[test]
    fn apply_throttle_clamps_tokens() {
        let mut rl = LlmRateLimiter::new(RateLimitConfig {
            requests_per_minute: 100,
            tokens_per_minute: 100_000,
        });
        rl.request_tokens = 80.0;
        rl.token_tokens = 80_000.0;
        rl.apply_throttle(50, 50_000);
        assert_eq!(rl.request_tokens, 50.0);
        assert_eq!(rl.token_tokens, 50_000.0);
    }

    #[test]
    fn rate_limit_config_default() {
        let config = RateLimitConfig::default();
        assert_eq!(config.requests_per_minute, 60);
        assert_eq!(config.tokens_per_minute, 90_000);
    }

    #[test]
    fn rate_limit_config_clone() {
        let config = RateLimitConfig {
            requests_per_minute: 120,
            tokens_per_minute: 45_000,
        };
        let cloned = config.clone();
        assert_eq!(cloned.requests_per_minute, 120);
        assert_eq!(cloned.tokens_per_minute, 45_000);
    }

    #[test]
    fn rate_limit_config_debug() {
        let config = RateLimitConfig::default();
        let debug = format!("{:?}", config);
        assert!(debug.contains("60"));
        assert!(debug.contains("90000"));
    }

    #[test]
    fn llm_response_equality() {
        let r1 = LlmResponse {
            text: "hello".into(),
            finish_reason: "stop".into(),
            tokens_used: 10,
        };
        let r2 = LlmResponse {
            text: "hello".into(),
            finish_reason: "stop".into(),
            tokens_used: 10,
        };
        assert_eq!(r1, r2);
    }

    #[test]
    fn llm_response_clone() {
        let r = LlmResponse {
            text: "test".into(),
            finish_reason: "length".into(),
            tokens_used: 5,
        };
        let c = r.clone();
        assert_eq!(r, c);
    }

    #[test]
    fn llm_response_debug() {
        let r = LlmResponse {
            text: "hi".into(),
            finish_reason: "stop".into(),
            tokens_used: 1,
        };
        let debug = format!("{:?}", r);
        assert!(debug.contains("hi"));
    }

    #[test]
    fn llm_error_display_variants() {
        let e1 = LlmError::Http("500".into());
        assert!(e1.to_string().contains("http error"));

        let e2 = LlmError::RateLimit("429".into());
        assert!(e2.to_string().contains("rate limit"));

        let e3 = LlmError::Parse("bad json".into());
        assert!(e3.to_string().contains("parse error"));
    }

    #[test]
    fn llm_error_is_std_error() {
        let err: Box<dyn std::error::Error> = Box::new(LlmError::Http("test".into()));
        assert!(!err.to_string().is_empty());
    }
}

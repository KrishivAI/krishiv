//! Apply coordinator LLM throttle commands to the process-level rate limiter.

use krishiv_ai::{LlmRateLimiter, RateLimitConfig};

/// Apply a throttle command from the coordinator heartbeat response.
pub fn apply_llm_throttle(model: &str, max_requests_per_minute: u32, max_tokens_per_minute: u64) {
    let limiter = LlmRateLimiter::for_model(
        model,
        RateLimitConfig {
            requests_per_minute: max_requests_per_minute,
            tokens_per_minute: max_tokens_per_minute,
        },
    );
    let mut attempts = 0u8;
    loop {
        match limiter.try_lock() {
            Ok(mut guard) => {
                guard.apply_throttle(max_requests_per_minute, max_tokens_per_minute);
                return;
            }
            Err(_) if attempts < 5 => {
                attempts += 1;
                std::thread::yield_now();
            }
            Err(_) => {
                tracing::warn!(
                    model,
                    max_requests_per_minute,
                    max_tokens_per_minute,
                    "llm throttle command dropped: rate limiter locked after retries"
                );
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn llm_throttle_updates_limiter() {
        apply_llm_throttle("gpt-4o", 10, 1000);
        // Second call should not panic; limiter singleton exists.
        apply_llm_throttle("gpt-4o", 5, 500);
    }
}

//! Coordinator-side LLM quota aggregation and throttle commands (R17 S4.4).

use std::collections::HashMap;

/// Per-executor LLM usage report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LlmQuotaReport {
    pub model: String,
    pub requests_used: u64,
    pub tokens_used: u64,
    pub period_ms: u64,
}

/// Coordinator throttle directive for executors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LlmThrottleCommand {
    pub model: String,
    pub max_requests_per_minute: u32,
    pub max_tokens_per_minute: u64,
}

/// Aggregates quota reports and issues throttle when job quota exceeded.
#[derive(Debug, Default)]
pub struct LlmQuotaAggregator {
    job_quota_requests_per_minute: u32,
    job_quota_tokens_per_minute: u64,
    aggregated: HashMap<String, (u64, u64)>,
}

impl LlmQuotaAggregator {
    /// Create with job-level LLM quotas.
    pub fn new(job_quota_requests_per_minute: u32, job_quota_tokens_per_minute: u64) -> Self {
        Self {
            job_quota_requests_per_minute,
            job_quota_tokens_per_minute,
            aggregated: HashMap::new(),
        }
    }

    /// Ingest executor reports for the current period.
    pub fn ingest(&mut self, reports: &[LlmQuotaReport]) {
        for r in reports {
            let entry = self.aggregated.entry(r.model.clone()).or_insert((0, 0));
            entry.0 = entry.0.saturating_add(r.requests_used);
            entry.1 = entry.1.saturating_add(r.tokens_used);
        }
    }

    /// Return throttle commands for models over quota, then reset aggregates.
    pub fn evaluate_and_reset(&mut self) -> Vec<LlmThrottleCommand> {
        let mut commands = Vec::new();
        for (model, (req, tok)) in &self.aggregated {
            if *req > self.job_quota_requests_per_minute as u64
                || *tok > self.job_quota_tokens_per_minute
            {
                commands.push(LlmThrottleCommand {
                    model: model.clone(),
                    max_requests_per_minute: self.job_quota_requests_per_minute,
                    max_tokens_per_minute: self.job_quota_tokens_per_minute,
                });
            }
        }
        self.aggregated.clear();
        commands
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn llm_quota_aggregator_issues_throttle_when_exceeded() {
        let mut agg = LlmQuotaAggregator::new(100, 10_000);
        agg.ingest(&[LlmQuotaReport {
            model: "gpt-4o".into(),
            requests_used: 60,
            tokens_used: 0,
            period_ms: 60_000,
        }]);
        agg.ingest(&[LlmQuotaReport {
            model: "gpt-4o".into(),
            requests_used: 50,
            tokens_used: 0,
            period_ms: 60_000,
        }]);
        let cmds = agg.evaluate_and_reset();
        assert_eq!(cmds.len(), 1);
        assert_eq!(cmds[0].model, "gpt-4o");
        assert_eq!(cmds[0].max_requests_per_minute, 100);
    }
}

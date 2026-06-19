#![forbid(unsafe_code)]
//! Minimal authentication and access-control interfaces for Krishiv.

// ─── AuthProvider ─────────────────────────────────────────────────────────────

/// Authenticate an API key and return the subject string, if known.
pub trait AuthProvider: Send + Sync {
    /// Return `Some(subject)` if the key is valid, `None` otherwise.
    fn authenticate(&self, api_key: &str) -> Option<String>;
}

/// API-key → subject mapping loaded from configuration.
pub struct StaticApiKeyAuthProvider {
    keys: std::collections::HashMap<String, String>,
}

impl StaticApiKeyAuthProvider {
    /// Build from a map of `api_key -> subject` entries.
    pub fn new(keys: std::collections::HashMap<String, String>) -> Self {
        Self { keys }
    }
}

impl AuthProvider for StaticApiKeyAuthProvider {
    fn authenticate(&self, api_key: &str) -> Option<String> {
        use constant_time_eq::constant_time_eq;
        let candidate = api_key.as_bytes();
        // Iterate every entry without short-circuiting so elapsed time is
        // independent of which key matched — prevents timing oracle attacks.
        let mut result: Option<String> = None;
        for (stored, subject) in &self.keys {
            if constant_time_eq(stored.as_bytes(), candidate) {
                result = Some(subject.clone());
            }
        }
        result
    }
}

// ─── PolicyHook ───────────────────────────────────────────────────────────────

/// Pluggable table-level access control hook.
pub trait PolicyHook: Send + Sync {
    /// Return `false` to deny access to the named table.
    fn check_table_access(&self, table_name: &str) -> bool;
}

/// Allow-all policy hook (default for embedded and test contexts).
pub struct AllowAllPolicyHook;

impl PolicyHook for AllowAllPolicyHook {
    fn check_table_access(&self, _table_name: &str) -> bool {
        true
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn static_auth_provider_known_key() {
        let mut keys = std::collections::HashMap::new();
        keys.insert("key1".to_string(), "alice".to_string());
        let provider = StaticApiKeyAuthProvider::new(keys);
        let subject = provider.authenticate("key1");
        assert_eq!(subject.as_deref(), Some("alice"));
    }

    #[test]
    fn static_auth_provider_unknown_key() {
        let mut keys = std::collections::HashMap::new();
        keys.insert("key1".to_string(), "alice".to_string());
        let provider = StaticApiKeyAuthProvider::new(keys);
        assert!(provider.authenticate("unknown").is_none());
    }

    #[test]
    fn static_auth_provider_no_prefix_timing_oracle() {
        let mut keys = std::collections::HashMap::new();
        keys.insert("secretXXX".to_string(), "alice".to_string());
        let provider = StaticApiKeyAuthProvider::new(keys);
        assert!(provider.authenticate("secret").is_none());
        assert!(provider.authenticate("secretXXXextra").is_none());
        assert!(provider.authenticate("secretXXX").is_some());
        assert!(provider.authenticate("").is_none());
    }

    #[test]
    fn allow_all_policy_hook_allows_all() {
        let hook = AllowAllPolicyHook;
        assert!(hook.check_table_access("any_table"));
        assert!(hook.check_table_access("internal_accounts"));
    }
}

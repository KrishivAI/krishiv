//! gRPC auth enforcement.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use krishiv_governance::{Role, StaticApiKeyAuthProvider};

// ── gRPC auth enforcement (P3-20) ─────────────────────────────────────────────

pub const COORDINATOR_BEARER_TOKEN_ENV: &str = "KRISHIV_COORDINATOR_BEARER_TOKEN";
const COORDINATOR_AUTH_SUBJECT: &str = "coordinator-control-plane";

static GRPC_AUTH_PROVIDER: std::sync::OnceLock<Arc<dyn krishiv_governance::AuthProvider>> =
    std::sync::OnceLock::new();

/// Deny-by-default: gRPC handlers reject anonymous requests when no auth
/// provider has been installed.  Set this to `true` for dev / test.
static ALLOW_ANONYMOUS: AtomicBool = AtomicBool::new(false);

// PRR Long-term (P-LONG-1): Future home for UDF sandboxing, per-job CPU/memory
// quotas, and stronger resource limit enforcement beyond current auth.

/// Install a process-wide auth provider for coordinator gRPC (optional).
pub fn set_grpc_auth_provider(provider: Arc<dyn krishiv_governance::AuthProvider>) {
    let _ = GRPC_AUTH_PROVIDER.set(provider);
}

/// Build the static coordinator gRPC auth provider from a bearer token.
pub fn static_grpc_auth_provider_from_bearer_token(
    token: impl AsRef<str>,
) -> Option<Arc<dyn krishiv_governance::AuthProvider>> {
    let token = token.as_ref().trim();
    if token.is_empty() {
        return None;
    }
    Some(Arc::new(StaticApiKeyAuthProvider::new([(
        token.to_owned(),
        COORDINATOR_AUTH_SUBJECT.to_owned(),
        Role::Admin,
    )])))
}

/// Read the configured coordinator bearer token from process environment.
pub fn configured_coordinator_bearer_token() -> Option<String> {
    std::env::var(COORDINATOR_BEARER_TOKEN_ENV)
        .ok()
        .map(|token| token.trim().to_owned())
        .filter(|token| !token.is_empty())
}

/// Install coordinator gRPC auth from `KRISHIV_COORDINATOR_BEARER_TOKEN`.
pub fn configure_grpc_auth_provider_from_env() -> bool {
    let Some(provider) = configured_coordinator_bearer_token()
        .as_deref()
        .and_then(static_grpc_auth_provider_from_bearer_token)
    else {
        return false;
    };
    set_grpc_auth_provider(provider);
    true
}

/// Allow anonymous gRPC access when no auth provider is configured.
///
/// Call this once during startup for development / test binaries.
pub fn set_allow_anonymous() {
    ALLOW_ANONYMOUS.store(true, Ordering::Release);
}

/// Validate `auth` against the configured provider.
///
/// # Security
///
/// Auth is **mandatory for mutating RPCs**.  Every mutating gRPC handler
/// must either use the [`auth_interceptor`] middleware or call this function
/// directly (wrapped by the [`require_auth!`] macro) before acting on the
/// request.
///
/// When no auth provider has been installed the default behaviour is to
/// **deny** every request (deny-by-default).  Call [`set_allow_anonymous`]
/// during startup to tolerate anonymous traffic in development.
pub fn validate_grpc_auth(auth: &AuthContext) -> Result<(), tonic::Status> {
    let Some(provider) = GRPC_AUTH_PROVIDER.get() else {
        if ALLOW_ANONYMOUS.load(Ordering::Acquire) {
            return Ok(());
        }
        return Err(tonic::Status::unauthenticated(
            "gRPC auth: no provider configured and anonymous access is denied by default; \
             set KRISHIV_ALLOW_ANONYMOUS or deploy an auth provider",
        ));
    };
    match auth {
        AuthContext::Bearer { subject } => {
            if provider.authenticate(subject).is_some() {
                Ok(())
            } else {
                Err(tonic::Status::unauthenticated("invalid API key"))
            }
        }
        AuthContext::Anonymous => Err(tonic::Status::unauthenticated("missing Bearer token")),
    }
}

// ── R8 auth interceptor skeleton ─────────────────────────────────────────────

/// Authentication context extracted by the auth interceptor.
///
/// In R8.1+ this will carry a validated bearer token or mTLS peer identity.
/// For now it is always `Anonymous` — the interceptor is a no-op that ensures
/// every future call site already accepts an `AuthContext` without structural
/// changes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthContext {
    /// No credential presented; denied by default unless [`set_allow_anonymous`] was called.
    Anonymous,
    /// A validated bearer token (R8.1 wiring placeholder).
    Bearer { subject: String },
}

impl AuthContext {
    /// Return `true` if this context represents a known authenticated subject.
    pub fn is_authenticated(&self) -> bool {
        matches!(self, Self::Bearer { .. })
    }

    /// Subject string, or `"anonymous"` for unauthenticated callers.
    pub fn subject(&self) -> &str {
        match self {
            Self::Anonymous => "anonymous",
            Self::Bearer { subject } => subject.as_str(),
        }
    }
}

/// Tonic interceptor that enforces auth on every incoming request.
///
/// When no auth provider is configured requests are **denied by default**
/// (call [`set_allow_anonymous`] for dev mode).  When a provider is installed
/// the request must carry a valid bearer token in the `authorization`
/// metadata header.
pub fn auth_interceptor(req: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
    let ctx = extract_auth_context(req.metadata());
    validate_grpc_auth(&ctx)?;
    Ok(req)
}

/// Macro that every mutating gRPC handler MUST use at its entry point.
///
/// Expands to `validate_grpc_auth($auth)?`, returning an
/// `unauthenticated` tonic status when auth is required but missing.
#[macro_export]
macro_rules! require_auth {
    ($auth:expr) => {
        $crate::auth::validate_grpc_auth($auth)?
    };
}

/// Extract an `AuthContext` from the gRPC request metadata.
///
/// Reads the `authorization` header. If it starts with `"Bearer "` the token
/// is extracted and returned as `Bearer { subject: <token> }`. In R9 the token
/// is the API key validated by `krishiv_governance::StaticApiKeyAuthProvider`;
/// JWT/OIDC validation is deferred to R10.
///
/// Returns `Anonymous` when no header is present or parsing fails.
pub fn extract_auth_context(metadata: &tonic::metadata::MetadataMap) -> AuthContext {
    let header = metadata
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    if let Some(token) = header.strip_prefix("Bearer ") {
        let token = token.trim();
        if !token.is_empty() {
            return AuthContext::Bearer {
                subject: token.to_owned(),
            };
        }
    }
    AuthContext::Anonymous
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn static_provider_accepts_configured_bearer_token() {
        let provider = static_grpc_auth_provider_from_bearer_token("coord-secret").unwrap();

        let principal = provider.authenticate("coord-secret").unwrap();

        assert_eq!(principal.subject, COORDINATOR_AUTH_SUBJECT);
        assert_eq!(principal.role, Role::Admin);
        assert!(provider.authenticate("wrong-secret").is_none());
    }

    #[test]
    fn static_provider_rejects_empty_token() {
        assert!(static_grpc_auth_provider_from_bearer_token(" ").is_none());
    }
}

//! gRPC auth enforcement.

use std::sync::Arc;

// ── gRPC auth enforcement (P3-20) ─────────────────────────────────────────────

static GRPC_AUTH_PROVIDER: std::sync::OnceLock<Arc<dyn krishiv_governance::AuthProvider>> =
    std::sync::OnceLock::new();

/// Install a process-wide auth provider for coordinator gRPC (optional).
pub fn set_grpc_auth_provider(provider: Arc<dyn krishiv_governance::AuthProvider>) {
    let _ = GRPC_AUTH_PROVIDER.set(provider);
}

/// Validate `auth` when a provider is configured; otherwise allow anonymous access.
///
/// # Security
///
/// Auth is **mandatory for mutating RPCs**.  Every mutating gRPC handler
/// must either use the [`auth_interceptor`] middleware or call this function
/// directly (wrapped by the [`require_auth!`] macro) before acting on the
/// request.  Anonymous traffic is only permitted when no auth provider is
/// configured.
pub fn validate_grpc_auth(auth: &AuthContext) -> Result<(), tonic::Status> {
    let Some(provider) = GRPC_AUTH_PROVIDER.get() else {
        return Ok(());
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
    /// No credential presented; accepted in development / internal-only deployments.
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
/// When no auth provider is configured all requests pass through.  Otherwise
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

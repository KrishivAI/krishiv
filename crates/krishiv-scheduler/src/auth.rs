//! gRPC auth enforcement.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use krishiv_common::PRODUCTION_ENV;
use krishiv_plan::governance::{AuthProvider, Principal, Role, StaticApiKeyAuthProvider};

// ── gRPC auth enforcement (P3-20) ─────────────────────────────────────────────

pub const COORDINATOR_BEARER_TOKEN_ENV: &str = "KRISHIV_COORDINATOR_BEARER_TOKEN";
pub const COORDINATOR_BEARER_TOKENS_ENV: &str = "KRISHIV_COORDINATOR_BEARER_TOKENS";
pub const COORDINATOR_BEARER_TOKEN_FILE_ENV: &str = "KRISHIV_COORDINATOR_BEARER_TOKEN_FILE";
pub const COORDINATOR_BEARER_TOKENS_FILE_ENV: &str = "KRISHIV_COORDINATOR_BEARER_TOKENS_FILE";
pub const COORDINATOR_AUTH_RELOAD_INTERVAL_SECS_ENV: &str =
    "KRISHIV_COORDINATOR_AUTH_RELOAD_INTERVAL_SECS";
const COORDINATOR_AUTH_SUBJECT: &str = "coordinator-control-plane";

#[derive(Default)]
struct ReloadableGrpcAuthProvider {
    provider: RwLock<Option<Arc<dyn AuthProvider>>>,
}

impl ReloadableGrpcAuthProvider {
    fn set(&self, provider: Arc<dyn AuthProvider>) {
        let mut guard = self.provider.write().unwrap_or_else(|p| p.into_inner());
        *guard = Some(provider);
    }

    fn current(&self) -> Option<Arc<dyn AuthProvider>> {
        let guard = self.provider.read().unwrap_or_else(|p| p.into_inner());
        guard.clone()
    }
}

static GRPC_AUTH_PROVIDER: std::sync::OnceLock<ReloadableGrpcAuthProvider> =
    std::sync::OnceLock::new();

/// Deny-by-default: gRPC handlers reject anonymous requests when no auth
/// provider has been installed.  Set this to `true` for dev / test.
static ALLOW_ANONYMOUS: AtomicBool = AtomicBool::new(false);

// PRR Long-term (P-LONG-1): Future home for UDF sandboxing, per-job CPU/memory
// quotas, and stronger resource limit enforcement beyond current auth.

fn grpc_auth_provider() -> &'static ReloadableGrpcAuthProvider {
    GRPC_AUTH_PROVIDER.get_or_init(ReloadableGrpcAuthProvider::default)
}

/// Install or replace the process-wide auth provider for coordinator gRPC.
///
/// Replacing the provider is intentional: long-lived coordinators can reload a
/// rotated token set without restarting the process.
pub fn set_grpc_auth_provider(provider: Arc<dyn AuthProvider>) {
    grpc_auth_provider().set(provider);
}

/// Build the static coordinator gRPC auth provider from a bearer token.
pub fn static_grpc_auth_provider_from_bearer_token(
    token: impl AsRef<str>,
) -> Option<Arc<dyn AuthProvider>> {
    static_grpc_auth_provider_from_bearer_tokens([token])
}

/// Build the static coordinator gRPC auth provider from bearer tokens.
///
/// Multiple tokens are accepted so operators can run a bounded key-rotation
/// window across rolling coordinator restarts. Outbound clients still use the
/// single active [`COORDINATOR_BEARER_TOKEN_ENV`] token.
pub fn static_grpc_auth_provider_from_bearer_tokens<I, S>(
    tokens: I,
) -> Option<Arc<dyn AuthProvider>>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let tokens = normalized_bearer_tokens(tokens);
    if tokens.is_empty() {
        return None;
    }
    let entries = tokens
        .into_iter()
        .map(|token| (token, COORDINATOR_AUTH_SUBJECT.to_owned(), Role::Admin));
    Some(Arc::new(StaticApiKeyAuthProvider::new(entries)))
}

fn normalized_bearer_tokens<I, S>(tokens: I) -> Vec<String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut normalized = Vec::new();
    for token in tokens {
        let token = token.as_ref().trim();
        if token.is_empty() || normalized.iter().any(|seen| seen == token) {
            continue;
        }
        normalized.push(token.to_owned());
    }
    normalized
}

fn parse_extra_coordinator_bearer_tokens(raw: &str) -> impl Iterator<Item = &str> {
    raw.split([',', '\n'])
        .map(str::trim)
        .filter(|token| !token.is_empty())
}

fn coordinator_bearer_tokens_from_all_values(
    primary: Option<&str>,
    extra: Option<&str>,
    primary_file: Option<&str>,
    extra_file: Option<&str>,
) -> Vec<String> {
    let primary = primary.into_iter().chain(primary_file);
    let extra = extra
        .into_iter()
        .chain(extra_file)
        .flat_map(parse_extra_coordinator_bearer_tokens);
    normalized_bearer_tokens(primary.chain(extra))
}

fn env_value(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn read_required_token_file(path: &str) -> std::io::Result<String> {
    std::fs::read_to_string(path)
}

fn read_optional_token_file(path: &str) -> std::io::Result<Option<String>> {
    match std::fs::read_to_string(path) {
        Ok(contents) => Ok(Some(contents)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

/// Read all configured coordinator bearer tokens from process environment and files.
pub fn try_configured_coordinator_bearer_tokens() -> std::io::Result<Vec<String>> {
    let primary = env_value(COORDINATOR_BEARER_TOKEN_ENV);
    let extra = env_value(COORDINATOR_BEARER_TOKENS_ENV);
    let primary_file_path = env_value(COORDINATOR_BEARER_TOKEN_FILE_ENV);
    let extra_file_path = env_value(COORDINATOR_BEARER_TOKENS_FILE_ENV);

    let primary_file = primary_file_path
        .as_deref()
        .map(read_required_token_file)
        .transpose()?;
    let extra_file = extra_file_path
        .as_deref()
        .map(read_optional_token_file)
        .transpose()?
        .flatten();

    Ok(coordinator_bearer_tokens_from_all_values(
        primary.as_deref(),
        extra.as_deref(),
        primary_file.as_deref(),
        extra_file.as_deref(),
    ))
}

/// Read all configured coordinator bearer tokens from process environment.
///
/// `KRISHIV_COORDINATOR_BEARER_TOKEN` is the active client token. The optional
/// comma/newline separated `KRISHIV_COORDINATOR_BEARER_TOKENS` list extends the
/// tokens accepted by the server during planned rotation. File variants are
/// also supported for mounted Secret rotation.
pub fn configured_coordinator_bearer_tokens() -> Vec<String> {
    match try_configured_coordinator_bearer_tokens() {
        Ok(tokens) => tokens,
        Err(error) => {
            tracing::warn!(
                error = %error,
                "failed to read configured coordinator bearer tokens"
            );
            Vec::new()
        }
    }
}

/// Read the configured active coordinator bearer token from process environment.
pub fn configured_coordinator_bearer_token() -> Option<String> {
    std::env::var(COORDINATOR_BEARER_TOKEN_ENV)
        .ok()
        .map(|token| token.trim().to_owned())
        .filter(|token| !token.is_empty())
}

/// Install or reload coordinator gRPC auth from configured bearer tokens.
pub fn configure_grpc_auth_provider_from_env() -> bool {
    let tokens = match try_configured_coordinator_bearer_tokens() {
        Ok(tokens) => tokens,
        Err(error) => {
            tracing::warn!(
                error = %error,
                "failed to reload coordinator gRPC auth provider"
            );
            return false;
        }
    };
    let Some(provider) = static_grpc_auth_provider_from_bearer_tokens(tokens) else {
        return false;
    };
    set_grpc_auth_provider(provider);
    true
}

/// Reload coordinator gRPC auth from configured bearer tokens.
pub fn reload_grpc_auth_provider_from_env() -> bool {
    configure_grpc_auth_provider_from_env()
}

fn grpc_auth_reload_interval_from_value(raw: Option<&str>) -> Option<Duration> {
    let raw = raw?.trim();
    if raw.is_empty() {
        return None;
    }
    let seconds = match raw.parse::<u64>() {
        Ok(seconds) => seconds,
        Err(error) => {
            tracing::warn!(
                value = raw,
                error = %error,
                "invalid coordinator auth reload interval"
            );
            return None;
        }
    };
    if seconds == 0 {
        return None;
    }
    Some(Duration::from_secs(seconds))
}

fn configured_grpc_auth_reload_interval() -> Option<Duration> {
    let raw = std::env::var(COORDINATOR_AUTH_RELOAD_INTERVAL_SECS_ENV).ok();
    grpc_auth_reload_interval_from_value(raw.as_deref())
}

/// Spawn a best-effort periodic coordinator auth reload task when configured.
///
/// Set `KRISHIV_COORDINATOR_AUTH_RELOAD_INTERVAL_SECS` to a positive number.
/// The task re-reads the same env/file token sources used at startup.
pub fn spawn_grpc_auth_reload_task_from_env() -> Option<tokio::task::JoinHandle<()>> {
    let interval = configured_grpc_auth_reload_interval()?;
    Some(tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            if reload_grpc_auth_provider_from_env() {
                tracing::debug!("reloaded coordinator gRPC auth provider");
            } else {
                tracing::warn!("coordinator gRPC auth reload skipped; no valid token set");
            }
        }
    }))
}

/// Return `true` when at least one coordinator server bearer token is configured.
pub fn coordinator_bearer_auth_configured() -> bool {
    match try_configured_coordinator_bearer_tokens() {
        Ok(tokens) => !tokens.is_empty(),
        Err(_) => false,
    }
}

/// Fail closed when bearer token files are configured but unreadable.
pub fn validate_coordinator_bearer_token_sources() -> std::io::Result<()> {
    let _ = try_configured_coordinator_bearer_tokens()?;
    Ok(())
}

/// Error installing permissive gRPC auth for development.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum GrpcAuthSetupError {
    /// Anonymous gRPC access is forbidden in production mode.
    #[error("anonymous gRPC access is forbidden when {PRODUCTION_ENV}=1")]
    AnonymousForbiddenInProduction,
}

/// Allow anonymous gRPC access when no auth provider is configured.
///
/// Call this once during startup for development / test binaries.
/// Returns an error when [`krishiv_common::is_production_mode`] is active.
pub fn set_allow_anonymous() -> Result<(), GrpcAuthSetupError> {
    set_allow_anonymous_when(!krishiv_common::is_production_mode())
}

fn set_allow_anonymous_when(allowed: bool) -> Result<(), GrpcAuthSetupError> {
    if !allowed {
        return Err(GrpcAuthSetupError::AnonymousForbiddenInProduction);
    }
    ALLOW_ANONYMOUS.store(true, Ordering::Release);
    Ok(())
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
    validate_grpc_auth_for_role(auth, Role::Reader)
}

/// Validate `auth` and require at least `required_role`.
///
/// Role order is `Reader < Writer < Admin`. Anonymous development mode keeps
/// bypassing role checks so existing local/test binaries can opt into no-auth
/// explicitly with [`set_allow_anonymous`].
pub fn validate_grpc_auth_for_role(
    auth: &AuthContext,
    required_role: Role,
) -> Result<(), tonic::Status> {
    let Some(provider) = grpc_auth_provider().current() else {
        if ALLOW_ANONYMOUS.load(Ordering::Acquire) {
            return Ok(());
        }
        return Err(tonic::Status::unauthenticated(
            "gRPC auth: no provider configured and anonymous access is denied by default; \
             set KRISHIV_ALLOW_ANONYMOUS or deploy an auth provider",
        ));
    };
    validate_grpc_auth_with_provider(provider.as_ref(), auth, &required_role)
}

fn validate_grpc_auth_with_provider(
    provider: &dyn AuthProvider,
    auth: &AuthContext,
    required_role: &Role,
) -> Result<(), tonic::Status> {
    match auth {
        AuthContext::Bearer { subject } => {
            let Some(principal) = provider.authenticate(subject) else {
                return Err(tonic::Status::unauthenticated("invalid API key"));
            };
            validate_principal_role(&principal, required_role)
        }
        AuthContext::Anonymous => Err(tonic::Status::unauthenticated("missing Bearer token")),
    }
}

fn validate_principal_role(
    principal: &Principal,
    required_role: &Role,
) -> Result<(), tonic::Status> {
    if role_allows(&principal.role, required_role) {
        Ok(())
    } else {
        Err(tonic::Status::permission_denied(format!(
            "principal {} has role {:?}; required role {:?}",
            principal.subject, principal.role, required_role
        )))
    }
}

fn role_allows(actual: &Role, required: &Role) -> bool {
    fn rank(role: &Role) -> u8 {
        match role {
            Role::Reader => 0,
            Role::Writer => 1,
            Role::Admin => 2,
        }
    }

    rank(actual) >= rank(required)
}

/// Validate `auth` against the configured provider and require writer access.
pub fn validate_grpc_writer(auth: &AuthContext) -> Result<(), tonic::Status> {
    validate_grpc_auth_for_role(auth, Role::Writer)
}

/// Validate `auth` against the configured provider and require admin access.
pub fn validate_grpc_admin(auth: &AuthContext) -> Result<(), tonic::Status> {
    validate_grpc_auth_for_role(auth, Role::Admin)
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
/// is the API key validated by `krishiv_plan::governance::StaticApiKeyAuthProvider`;
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

    #[test]
    fn static_provider_accepts_rotation_tokens() {
        let provider =
            static_grpc_auth_provider_from_bearer_tokens([" active-token ", "old-token"]).unwrap();

        let active = provider.authenticate("active-token").unwrap();
        let old = provider.authenticate("old-token").unwrap();

        assert_eq!(active.subject, COORDINATOR_AUTH_SUBJECT);
        assert_eq!(active.role, Role::Admin);
        assert_eq!(old.subject, COORDINATOR_AUTH_SUBJECT);
        assert_eq!(old.role, Role::Admin);
        assert!(provider.authenticate("wrong-token").is_none());
    }

    #[test]
    fn coordinator_bearer_tokens_from_values_dedupes_and_trims_rotation_list() {
        let tokens = coordinator_bearer_tokens_from_all_values(
            Some(" active-token "),
            Some("old-token,\nactive-token,, older-token "),
            None,
            None,
        );

        assert_eq!(
            tokens,
            vec![
                "active-token".to_owned(),
                "old-token".to_owned(),
                "older-token".to_owned(),
            ]
        );
    }

    #[test]
    fn coordinator_bearer_tokens_from_all_values_includes_file_sources() {
        let tokens = coordinator_bearer_tokens_from_all_values(
            Some(" active-token "),
            Some("old-token"),
            Some("file-token\n"),
            Some("old-token\nnew-token"),
        );

        assert_eq!(
            tokens,
            vec![
                "active-token".to_owned(),
                "file-token".to_owned(),
                "old-token".to_owned(),
                "new-token".to_owned(),
            ]
        );
    }

    #[test]
    fn optional_rotation_token_file_may_be_missing() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("tokens");

        assert!(
            read_optional_token_file(missing.to_str().unwrap())
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn auth_reload_interval_parses_positive_seconds_only() {
        assert_eq!(
            grpc_auth_reload_interval_from_value(Some("15")),
            Some(Duration::from_secs(15))
        );
        assert_eq!(grpc_auth_reload_interval_from_value(Some("0")), None);
        assert_eq!(
            grpc_auth_reload_interval_from_value(Some("not-a-number")),
            None
        );
        assert_eq!(grpc_auth_reload_interval_from_value(None), None);
    }

    #[test]
    fn reloadable_provider_replaces_accepted_tokens() {
        let holder = ReloadableGrpcAuthProvider::default();
        holder.set(static_grpc_auth_provider_from_bearer_token("old-token").unwrap());

        let old_auth = AuthContext::Bearer {
            subject: "old-token".to_owned(),
        };
        let new_auth = AuthContext::Bearer {
            subject: "new-token".to_owned(),
        };

        validate_grpc_auth_with_provider(
            holder.current().unwrap().as_ref(),
            &old_auth,
            &Role::Admin,
        )
        .unwrap();

        holder.set(static_grpc_auth_provider_from_bearer_token("new-token").unwrap());

        let old_status = validate_grpc_auth_with_provider(
            holder.current().unwrap().as_ref(),
            &old_auth,
            &Role::Admin,
        )
        .unwrap_err();
        assert_eq!(old_status.code(), tonic::Code::Unauthenticated);
        validate_grpc_auth_with_provider(
            holder.current().unwrap().as_ref(),
            &new_auth,
            &Role::Admin,
        )
        .unwrap();
    }

    #[test]
    fn role_hierarchy_allows_higher_roles_to_satisfy_lower_requirements() {
        assert!(role_allows(&Role::Admin, &Role::Writer));
        assert!(role_allows(&Role::Admin, &Role::Reader));
        assert!(role_allows(&Role::Writer, &Role::Reader));
        assert!(role_allows(&Role::Reader, &Role::Reader));
        assert!(!role_allows(&Role::Reader, &Role::Writer));
        assert!(!role_allows(&Role::Writer, &Role::Admin));
    }

    #[test]
    fn principal_role_validation_denies_insufficient_role() {
        let principal = Principal {
            subject: "read-only-client".to_owned(),
            role: Role::Reader,
        };

        let status = validate_principal_role(&principal, &Role::Writer).unwrap_err();

        assert_eq!(status.code(), tonic::Code::PermissionDenied);
        assert!(status.message().contains("read-only-client"));
    }

    #[test]
    fn set_allow_anonymous_rejected_in_production_mode() {
        let result = set_allow_anonymous_when(false);
        assert!(matches!(
            result,
            Err(GrpcAuthSetupError::AnonymousForbiddenInProduction)
        ));
    }
}

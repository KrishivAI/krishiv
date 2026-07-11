//! gRPC auth enforcement.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use krishiv_common::PRODUCTION_ENV;
use krishiv_plan::governance::AuthProvider;

// ── Role (coordinator-internal, not in governance) ────────────────────────────

/// Roles used internally by the coordinator for gRPC access control.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Role {
    Reader,
    Writer,
    Admin,
}

/// An authenticated identity with an assigned role.
#[derive(Debug, Clone)]
pub struct Principal {
    pub subject: String,
    pub role: Role,
}

/// Static API key → (subject, role) mapping for the coordinator.
struct StaticApiKeyAuthProviderWithRole {
    keys: HashMap<String, Principal>,
}

impl StaticApiKeyAuthProviderWithRole {
    fn new(entries: impl IntoIterator<Item = (String, String, Role)>) -> Self {
        let keys = entries
            .into_iter()
            .map(|(k, s, r)| {
                (
                    k,
                    Principal {
                        subject: s,
                        role: r,
                    },
                )
            })
            .collect();
        Self { keys }
    }
}

impl AuthProvider for StaticApiKeyAuthProviderWithRole {
    fn authenticate(&self, api_key: &str) -> Option<String> {
        use constant_time_eq::constant_time_eq;
        let candidate = api_key.as_bytes();
        let mut result: Option<String> = None;
        for (stored, principal) in &self.keys {
            if constant_time_eq(stored.as_bytes(), candidate) {
                result = Some(principal.subject.clone());
            }
        }
        result
    }
}

/// Maps an authenticated subject string to its access role.
///
/// Convention:
/// - Subjects with the `reader:` prefix → `Role::Reader`
/// - Subjects with the `writer:` prefix → `Role::Writer`
/// - Subjects with the `admin:` prefix → `Role::Admin`
/// - All other subjects (including bare coordinator tokens) → `Role::Reader`
///   (default least privilege; never escalate unprefixed subjects)
///
/// JWT providers should encode the role in a `krishiv_role` claim and return
/// a subject of the form `<role>:<original-sub>` after claim extraction.
fn subject_to_role(subject: &str) -> Role {
    if subject.starts_with("reader:") {
        Role::Reader
    } else if subject.starts_with("writer:") {
        Role::Writer
    } else if subject.starts_with("admin:") {
        Role::Admin
    } else {
        // Default to least privilege — never escalate unknown or unprefixed
        // subjects to Admin.  The `krishiv_role` claim (see `JwtAuthProvider`)
        // is the authoritative source for JWT-based RBAC.
        Role::Reader
    }
}

/// Auth provider that rejects every token.  Installed when token sources are
/// configured but all tokens are empty/revoked (fail-closed revocation).
struct RejectAllAuthProvider;

impl AuthProvider for RejectAllAuthProvider {
    fn authenticate(&self, _api_key: &str) -> Option<String> {
        None
    }
}

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
    let entries = tokens.into_iter().map(|token| {
        (
            token,
            format!("admin:{COORDINATOR_AUTH_SUBJECT}"),
            Role::Admin,
        )
    });
    Some(Arc::new(StaticApiKeyAuthProviderWithRole::new(entries)))
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
    let (tokens, _any_source) = try_configured_coordinator_bearer_tokens_with_flag()?;
    Ok(tokens)
}

/// Like [`try_configured_coordinator_bearer_tokens`] but also reports whether
/// any token source (env var or file path) was configured.  When the flag is
/// `true` but the token list is empty, the caller should install a reject-all
/// provider (fail-closed revocation).
fn try_configured_coordinator_bearer_tokens_with_flag() -> std::io::Result<(Vec<String>, bool)> {
    let primary = env_value(COORDINATOR_BEARER_TOKEN_ENV);
    let extra = env_value(COORDINATOR_BEARER_TOKENS_ENV);
    let primary_file_path = env_value(COORDINATOR_BEARER_TOKEN_FILE_ENV);
    let extra_file_path = env_value(COORDINATOR_BEARER_TOKENS_FILE_ENV);

    let any_source = primary.is_some()
        || extra.is_some()
        || primary_file_path.is_some()
        || extra_file_path.is_some();

    let primary_file = primary_file_path
        .as_deref()
        .map(read_required_token_file)
        .transpose()?;
    let extra_file = extra_file_path
        .as_deref()
        .map(read_optional_token_file)
        .transpose()?
        .flatten();

    Ok((
        coordinator_bearer_tokens_from_all_values(
            primary.as_deref(),
            extra.as_deref(),
            primary_file.as_deref(),
            extra_file.as_deref(),
        ),
        any_source,
    ))
}

/// Read all configured coordinator bearer tokens from process environment.
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
///
/// When any token source is configured (env var or file path is set) but the
/// resolved token set is empty (e.g. all tokens revoked), a reject-all provider
/// is installed so no bearer token is accepted.  Only when NO token source is
/// configured at all is the existing provider left in place (for transient IO
/// errors during reload).
pub fn configure_grpc_auth_provider_from_env() -> bool {
    let (tokens, any_source_configured) = match try_configured_coordinator_bearer_tokens_with_flag()
    {
        Ok(result) => result,
        Err(error) => {
            tracing::warn!(
                error = %error,
                "failed to reload coordinator gRPC auth provider"
            );
            return false;
        }
    };
    if tokens.is_empty() && any_source_configured {
        // Token sources are configured but all tokens are empty/revoked.
        // Install a reject-all provider so the old revoked tokens are no longer
        // accepted. This is the fail-closed path for token revocation.
        set_grpc_auth_provider(Arc::new(RejectAllAuthProvider));
        tracing::info!("installed reject-all auth provider — all bearer tokens are revoked");
        return true;
    }
    let Some(provider) = static_grpc_auth_provider_from_bearer_tokens(tokens) else {
        // No token source configured at all — leave existing provider in place.
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
/// The returned handle is already captured by callers (e.g., `_auth_reload_task`) so the
/// task is properly tracked for the lifetime of the process.
pub fn spawn_grpc_auth_reload_task_from_env() -> Option<tokio::task::JoinHandle<()>> {
    let interval = configured_grpc_auth_reload_interval()?;
    let handle = tokio::spawn(async move {
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
    });
    Some(handle)
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
/// Anonymous access is forbidden when:
/// - `KRISHIV_PRODUCTION=1` is set, OR
/// - The durability profile is anything other than `dev-local` (i.e.
///   `KRISHIV_DURABILITY_PROFILE` is `single-node-durable` or
///   `distributed-durable`).
///
/// This ensures that real clusters never silently accept unauthenticated
/// control-plane RPCs.  Set `KRISHIV_PRODUCTION=0` AND keep the default
/// `dev-local` profile to allow anonymous access for local development.
pub fn set_allow_anonymous() -> Result<(), GrpcAuthSetupError> {
    if krishiv_common::is_production_mode() {
        return Err(GrpcAuthSetupError::AnonymousForbiddenInProduction);
    }
    let profile = krishiv_common::resolve_durability_profile();
    if krishiv_common::requires_http_auth(profile) {
        return Err(GrpcAuthSetupError::AnonymousForbiddenInProduction);
    }
    set_allow_anonymous_when(true)
}

fn set_allow_anonymous_when(allowed: bool) -> Result<(), GrpcAuthSetupError> {
    if !allowed {
        return Err(GrpcAuthSetupError::AnonymousForbiddenInProduction);
    }
    ALLOW_ANONYMOUS.store(true, Ordering::Release);
    Ok(())
}

/// Validate `auth` against the configured provider.
pub fn validate_grpc_auth(auth: &AuthContext) -> Result<(), tonic::Status> {
    validate_grpc_auth_for_role(auth, Role::Reader)
}

/// Validate `auth` and require at least `required_role`.
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
            let Some(authenticated_subject) = provider.authenticate(subject) else {
                return Err(tonic::Status::unauthenticated("invalid API key"));
            };
            let role = subject_to_role(&authenticated_subject);
            let principal = Principal {
                subject: authenticated_subject,
                role,
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthContext {
    /// No credential presented; denied by default unless [`set_allow_anonymous`] was called.
    Anonymous,
    /// A validated bearer token.
    Bearer { subject: String },
}

impl AuthContext {
    /// Return `true` if this context represents a known authenticated subject.
    pub fn is_authenticated(&self) -> bool {
        matches!(self, Self::Bearer { .. })
    }

    /// Log/identity-safe subject string (SEC-5, Phase 63).
    ///
    /// For a bearer context this is a short **stable, non-reversible
    /// fingerprint** of the token — never the raw credential. The raw token is
    /// only ever read internally, by `validate_grpc_auth_with_provider`, which
    /// destructures the `Bearer.subject` field directly. Every logging site
    /// goes through here, so a token can never be written to logs even at
    /// `RUST_LOG=debug` or on a rejected request.
    pub fn subject(&self) -> String {
        match self {
            Self::Anonymous => "anonymous".to_owned(),
            // Shared redaction (Phase 51): a 16-hex-char hash — enough to
            // correlate a caller across log lines, far too little to recover
            // a high-entropy bearer token.
            Self::Bearer { subject } => krishiv_common::redact_token(subject),
        }
    }
}

/// Tonic interceptor that enforces auth on every incoming request.
pub fn auth_interceptor(req: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
    let ctx = extract_auth_context(req.metadata());
    if let Err(status) = validate_grpc_auth(&ctx) {
        tracing::warn!(
            subject = ctx.subject(),
            code = ?status.code(),
            message = status.message(),
            "gRPC request rejected by auth interceptor"
        );
        return Err(status);
    }
    Ok(req)
}

/// Macro that every mutating gRPC handler MUST use at its entry point.
#[macro_export]
macro_rules! require_auth {
    ($auth:expr) => {
        $crate::auth::validate_grpc_auth($auth)?
    };
}

/// Extract an `AuthContext` from the gRPC request metadata.
pub fn extract_auth_context(metadata: &tonic::metadata::MetadataMap) -> AuthContext {
    let header = metadata.get("authorization").and_then(|v| v.to_str().ok());
    if let Some(token) = krishiv_common::bearer_token(header) {
        return AuthContext::Bearer {
            subject: token.to_owned(),
        };
    }
    AuthContext::Anonymous
}

// ── JWT / OIDC auth ───────────────────────────────────────────────────────────

/// Env var naming the JWKS endpoint to fetch verification keys from.
pub const OIDC_JWKS_URI_ENV: &str = "KRISHIV_OIDC_JWKS_URI";

/// Optional env var to restrict accepted JWT audience (`aud` claim).
pub const OIDC_AUDIENCE_ENV: &str = "KRISHIV_OIDC_AUDIENCE";

/// JWT-based [`AuthProvider`] backed by OIDC JWKS key material.
pub struct JwtAuthProvider {
    keys: Vec<jsonwebtoken::DecodingKey>,
    validation: jsonwebtoken::Validation,
}

#[derive(serde::Deserialize)]
struct JwtClaims {
    sub: String,
    /// Optional RBAC role claim. When present the subject is prefixed with
    /// `<role>:` so that `subject_to_role` maps it correctly. When absent the
    /// subject is returned as-is (unprefixed subjects get `Role::Reader`).
    #[serde(default)]
    krishiv_role: Option<String>,
}

impl JwtAuthProvider {
    /// Load JWKS from `KRISHIV_OIDC_JWKS_URI` and build a provider.
    pub async fn from_env() -> Option<Result<Self, Box<dyn std::error::Error + Send + Sync>>> {
        let uri = std::env::var(OIDC_JWKS_URI_ENV).ok()?;
        Some(Self::from_jwks_uri(&uri).await)
    }

    /// Fetch a JWKS endpoint and build a provider.
    ///
    /// Uses the async `reqwest` client so the caller does not block a Tokio
    /// executor thread during the HTTP round-trip.
    pub async fn from_jwks_uri(
        uri: &str,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .map_err(|e| format!("failed to build JWKS HTTP client: {e}"))?;
        let body = client.get(uri).send().await?.text().await?;
        Self::from_jwks_json(&body)
    }

    /// Parse a JWKS JSON document and build a provider.
    ///
    /// SEC-6 (Phase 63): the accepted signature algorithms are pinned from the
    /// key material itself — an RSA key admits only the RSA signing family
    /// (RS*/PS*), an EC key only ES* for its curve, an Ed25519 key only EdDSA;
    /// symmetric (`oct`) keys are refused and `none` can never appear. This
    /// fixes the previous `Validation::default()`, whose `algorithms` was
    /// `[HS256]` — so every real RS256/ES256 OIDC token was rejected and the
    /// path was effectively broken for mainstream IdPs — and it closes the
    /// JWKS algorithm-confusion class (an HS256 token forged with the public
    /// key as the HMAC secret is never accepted).
    pub fn from_jwks_json(json: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let jwks: jsonwebtoken::jwk::JwkSet = serde_json::from_str(json)?;
        let mut keys = Vec::new();
        let mut algorithms: Vec<jsonwebtoken::Algorithm> = Vec::new();
        for jwk in &jwks.keys {
            let algs = match jwk_signing_algorithms(jwk) {
                Ok(algs) => algs,
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "skipping JWK unusable for JWT signature verification"
                    );
                    continue;
                }
            };
            match jsonwebtoken::DecodingKey::from_jwk(jwk) {
                Ok(key) => {
                    keys.push(key);
                    for alg in algs {
                        if !algorithms.contains(&alg) {
                            algorithms.push(alg);
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "skipping undecodable JWK");
                }
            }
        }
        if keys.is_empty() {
            return Err("JWKS contained no usable asymmetric verification keys".into());
        }
        // By construction `algorithms` holds only asymmetric variants — HS* and
        // `none` can never enter it.
        let Some(&first_alg) = algorithms.first() else {
            return Err("JWKS contained no usable asymmetric verification keys".into());
        };
        let mut validation = jsonwebtoken::Validation::new(first_alg);
        validation.algorithms = algorithms;
        if let Ok(aud) = std::env::var(OIDC_AUDIENCE_ENV) {
            validation.set_audience(&[aud]);
        } else if krishiv_common::is_production_mode() {
            return Err(format!(
                "{OIDC_AUDIENCE_ENV} must be set when JWT auth is active in production mode"
            )
            .into());
        } else {
            validation.validate_aud = false;
        }
        Ok(Self { keys, validation })
    }
}

/// SEC-6 (Phase 63): derive the permitted JWT signature algorithms for a JWK
/// from its key material. Never returns an HMAC (HS*) algorithm and refuses
/// symmetric keys, so a JWKS can never widen verification to an algorithm the
/// key type does not support (the algorithm-confusion class).
fn jwk_signing_algorithms(
    jwk: &jsonwebtoken::jwk::Jwk,
) -> Result<Vec<jsonwebtoken::Algorithm>, Box<dyn std::error::Error + Send + Sync>> {
    use jsonwebtoken::Algorithm;
    use jsonwebtoken::jwk::{AlgorithmParameters, EllipticCurve};
    match &jwk.algorithm {
        // An RSA public key verifies the whole RSA signing family; the token's
        // own header `alg` selects which. Every one requires the RSA key — none
        // admit an HMAC secret.
        AlgorithmParameters::RSA(_) => Ok(vec![
            Algorithm::RS256,
            Algorithm::RS384,
            Algorithm::RS512,
            Algorithm::PS256,
            Algorithm::PS384,
            Algorithm::PS512,
        ]),
        AlgorithmParameters::EllipticCurve(ec) => match &ec.curve {
            EllipticCurve::P256 => Ok(vec![Algorithm::ES256]),
            EllipticCurve::P384 => Ok(vec![Algorithm::ES384]),
            other => Err(format!("unsupported EC curve for JWT verification: {other:?}").into()),
        },
        AlgorithmParameters::OctetKeyPair(okp) => match &okp.curve {
            EllipticCurve::Ed25519 => Ok(vec![Algorithm::EdDSA]),
            other => Err(format!("unsupported OKP curve for JWT verification: {other:?}").into()),
        },
        AlgorithmParameters::OctetKey(_) => Err(
            "symmetric (oct) key in OIDC JWKS refused for JWT verification \
             (algorithm-confusion protection)"
                .into(),
        ),
    }
}

impl AuthProvider for JwtAuthProvider {
    fn authenticate(&self, api_key: &str) -> Option<String> {
        let _header = jsonwebtoken::decode_header(api_key).ok()?;
        for key in &self.keys {
            if let Ok(token_data) =
                jsonwebtoken::decode::<JwtClaims>(api_key, key, &self.validation)
            {
                let claims = token_data.claims;
                // Prefix the subject with the role claim so subject_to_role
                // maps it correctly. If no role claim is present the subject
                // is returned bare → default Reader (least privilege).
                return Some(match claims.krishiv_role.as_deref() {
                    Some("admin") => format!("admin:{}", claims.sub),
                    Some("writer") => format!("writer:{}", claims.sub),
                    Some("reader") | Some(_) => format!("reader:{}", claims.sub),
                    None => claims.sub,
                });
            }
        }
        None
    }
}

/// Install OIDC JWKS auth if `KRISHIV_OIDC_JWKS_URI` is set.
pub async fn configure_jwt_auth_provider_from_env() -> bool {
    match JwtAuthProvider::from_env().await {
        None => false,
        Some(Ok(provider)) => {
            tracing::info!("OIDC JWKS JWT auth provider installed");
            set_grpc_auth_provider(Arc::new(provider));
            true
        }
        Some(Err(e)) => {
            tracing::warn!(
                error = %e,
                "failed to load OIDC JWKS; JWT auth provider not installed"
            );
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn static_provider_accepts_configured_bearer_token() {
        let provider = static_grpc_auth_provider_from_bearer_token("coord-secret").unwrap();

        let subject = provider.authenticate("coord-secret").unwrap();

        assert_eq!(subject, format!("admin:{COORDINATOR_AUTH_SUBJECT}"));
        assert!(provider.authenticate("wrong-secret").is_none());
    }

    #[test]
    fn static_provider_rejects_empty_token() {
        assert!(static_grpc_auth_provider_from_bearer_token(" ").is_none());
    }

    /// SEC-6 (Phase 63): a JWKS must pin verification to the key's own
    /// asymmetric algorithm family — RS256 for an RSA key — and must never
    /// admit HS256. The old `Validation::default()` pinned `[HS256]`, which
    /// both rejected every real RS256 OIDC token and left the door to the
    /// algorithm-confusion attack. (RFC 7517 §A.1 example RSA public key.)
    #[test]
    fn sec6_jwks_pins_asymmetric_algorithms_not_hs256() {
        let jwks = r#"{"keys":[{"kty":"RSA","kid":"test","use":"sig","alg":"RS256","n":"0vx7agoebGcQSuuPiLJXZptN9nndrQmbXEps2aiAFbWhM78LhWx4cbbfAAtVT86zwu1RK7aPFFxuhDR1L6tSoc_BJECPebWKRXjBZCiFV4n3oknjhMstn64tZ_2W-5JsGY4Hc5n9yBXArwl93lqt7_RN5w6Cf0h4QyQ5v-65YGjQR0_FDW2QvzqY368QQMicAtaSqzs8KJZgnYb9c7d0zgdAZHzu6qMQvRL5hajrn1n91CbOpbISD08qNLyrdkt-bFTWhAI4vMQFh6WeZu0fM4lFd2NcRwr3XPksINHaQ-G_xBniIqbw0Ls1jF44-csFCur-kEgU8awapJzKnqDKgw","e":"AQAB"}]}"#;
        let provider = JwtAuthProvider::from_jwks_json(jwks).expect("RSA JWKS builds a provider");
        assert!(
            provider
                .validation
                .algorithms
                .contains(&jsonwebtoken::Algorithm::RS256),
            "RS256 must be accepted for an RSA JWKS (the old HS256 default rejected it)"
        );
        assert!(
            !provider
                .validation
                .algorithms
                .contains(&jsonwebtoken::Algorithm::HS256),
            "HS256 must never be accepted from a JWKS (algorithm confusion)"
        );
    }

    /// SEC-5 (Phase 63): the raw bearer token must never be exposed by the
    /// value that every logging site records. `subject()` returns a stable
    /// fingerprint, not the credential, and `extract_auth_context` still holds
    /// the raw token internally (for validation) but never surfaces it.
    #[test]
    fn sec5_subject_never_exposes_raw_bearer_token() {
        let raw = "super-secret-coordinator-token-0xABCDEF";
        let ctx = AuthContext::Bearer {
            subject: raw.to_owned(),
        };
        let logged = ctx.subject();
        assert!(
            !logged.contains(raw),
            "subject() must not contain the raw token, got {logged}"
        );
        assert!(
            logged.starts_with("bearer:"),
            "bearer subject must be a fingerprint, got {logged}"
        );
        // Stable: the same token always fingerprints identically (log
        // correlation), and different tokens differ.
        assert_eq!(logged, ctx.subject());
        let other = AuthContext::Bearer {
            subject: "a-different-token".to_owned(),
        };
        assert_ne!(logged, other.subject());
        assert_eq!(AuthContext::Anonymous.subject(), "anonymous");

        // The raw token is still recoverable internally for validation via the
        // field, but never through the public accessor.
        let extracted = {
            let mut md = tonic::metadata::MetadataMap::new();
            md.insert("authorization", format!("Bearer {raw}").parse().unwrap());
            extract_auth_context(&md)
        };
        assert!(!extracted.subject().contains(raw));
        match extracted {
            AuthContext::Bearer { subject } => assert_eq!(subject, raw),
            AuthContext::Anonymous => panic!("expected a bearer context"),
        }
    }

    #[test]
    fn static_provider_accepts_rotation_tokens() {
        let provider =
            static_grpc_auth_provider_from_bearer_tokens([" active-token ", "old-token"]).unwrap();

        let active = provider.authenticate("active-token").unwrap();
        let old = provider.authenticate("old-token").unwrap();

        assert_eq!(active, format!("admin:{COORDINATOR_AUTH_SUBJECT}"));
        assert_eq!(old, format!("admin:{COORDINATOR_AUTH_SUBJECT}"));
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

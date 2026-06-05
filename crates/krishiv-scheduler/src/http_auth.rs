//! Bearer-token authentication for coordinator HTTP control-plane routes.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

use crate::auth::configured_coordinator_bearer_tokens;

/// Resolve the set of bearer tokens accepted for coordinator HTTP.
///
/// Reuses the same token sources as coordinator gRPC auth.
pub fn resolve_http_bearer_tokens() -> Vec<String> {
    configured_coordinator_bearer_tokens()
}

/// Axum middleware: require `Authorization: Bearer <token>` matching one of `expected`.
pub async fn require_coordinator_bearer(
    request: Request<Body>,
    next: Next,
    expected: &[String],
) -> Response {
    if expected.is_empty() {
        return (
            StatusCode::UNAUTHORIZED,
            "coordinator HTTP auth is required but no bearer tokens are configured",
        )
            .into_response();
    }

    let auth = request
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok());

    match auth {
        Some(value) if value.len() > 7 && value[..7].eq_ignore_ascii_case("bearer ") => {
            let token = value[7..].trim();
            if expected.iter().any(|t| t == token) {
                next.run(request).await
            } else {
                (StatusCode::UNAUTHORIZED, "invalid bearer token").into_response()
            }
        }
        _ => (
            StatusCode::UNAUTHORIZED,
            [("WWW-Authenticate", "Bearer")],
            "missing bearer token",
        )
            .into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_http_tokens_is_non_panicking_without_env() {
        let _ = resolve_http_bearer_tokens();
    }
}

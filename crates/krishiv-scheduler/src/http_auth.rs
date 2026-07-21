//! Bearer-token authentication for coordinator HTTP control-plane routes.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

use crate::auth::configured_coordinator_bearer_tokens;
use constant_time_eq::constant_time_eq;

/// Resolve the set of bearer tokens accepted for coordinator HTTP.
///
/// Reuses the same token sources as coordinator gRPC auth.
pub fn resolve_http_bearer_tokens() -> Vec<String> {
    configured_coordinator_bearer_tokens()
}

/// Axum middleware: require `Authorization: Bearer <token>` matching one of `expected`.
///
/// An empty string in `expected` never matches, even against a blank or
/// missing-content bearer token — defense in depth against a caller that
/// doesn't route through `resolve_http_bearer_tokens`'s own blank-entry
/// filtering (`normalized_bearer_tokens` in `auth.rs`).
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
            if expected
                .iter()
                .any(|t| !t.is_empty() && constant_time_eq(t.as_bytes(), token.as_bytes()))
            {
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
    use axum::Router;
    use axum::middleware;
    use axum::routing::get;
    use tower::ServiceExt;

    use super::*;

    #[test]
    fn resolve_http_tokens_is_non_panicking_without_env() {
        let _ = resolve_http_bearer_tokens();
    }

    /// Builds the same protected-route wiring as `coordinator_daemon.rs`'s
    /// real router: a single probe route behind `require_coordinator_bearer`
    /// with a fixed `expected` token set, so tests exercise the exact
    /// production layering instead of calling the middleware fn in isolation.
    fn probe_router(expected: Vec<String>) -> Router {
        Router::new()
            .route("/probe", get(|| async { "ok" }))
            .layer(middleware::from_fn(move |req, next| {
                let expected = expected.clone();
                async move { require_coordinator_bearer(req, next, &expected).await }
            }))
    }

    fn request_with_auth(auth_header: Option<&str>) -> Request<Body> {
        let mut builder = Request::builder().uri("/probe");
        if let Some(value) = auth_header {
            builder = builder.header(axum::http::header::AUTHORIZATION, value);
        }
        builder.body(Body::empty()).unwrap()
    }

    #[tokio::test]
    async fn no_configured_tokens_is_fail_closed_even_with_a_syntactically_valid_header() {
        // SEC-1/SEC-7 posture: an empty `expected` set must deny every
        // request, not fail open — a misconfiguration (no tokens loaded)
        // must never be equivalent to "auth disabled".
        let router = probe_router(vec![]);
        let response = router
            .oneshot(request_with_auth(Some("Bearer anything")))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn missing_authorization_header_is_rejected_with_www_authenticate() {
        let router = probe_router(vec!["secret-token".to_owned()]);
        let response = router.oneshot(request_with_auth(None)).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            response
                .headers()
                .get("WWW-Authenticate")
                .and_then(|v| v.to_str().ok()),
            Some("Bearer")
        );
    }

    #[tokio::test]
    async fn matching_bearer_token_is_admitted() {
        let router = probe_router(vec!["secret-token".to_owned()]);
        let response = router
            .oneshot(request_with_auth(Some("Bearer secret-token")))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn scheme_matching_is_case_insensitive() {
        let router = probe_router(vec!["secret-token".to_owned()]);
        let response = router
            .oneshot(request_with_auth(Some("BEARER secret-token")))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn token_value_comparison_is_case_sensitive() {
        // Unlike the scheme name, the token itself is an opaque secret and
        // must not be matched case-insensitively.
        let router = probe_router(vec!["Secret-Token".to_owned()]);
        let response = router
            .oneshot(request_with_auth(Some("Bearer secret-token")))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn wrong_token_is_rejected() {
        let router = probe_router(vec!["secret-token".to_owned()]);
        let response = router
            .oneshot(request_with_auth(Some("Bearer wrong-token")))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn any_of_multiple_expected_tokens_is_admitted() {
        let router = probe_router(vec!["token-a".to_owned(), "token-b".to_owned()]);
        for candidate in ["Bearer token-a", "Bearer token-b"] {
            let response = router
                .clone()
                .oneshot(request_with_auth(Some(candidate)))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK, "failed for {candidate}");
        }
    }

    #[tokio::test]
    async fn non_bearer_scheme_is_rejected() {
        let router = probe_router(vec!["secret-token".to_owned()]);
        let response = router
            .oneshot(request_with_auth(Some("Basic dXNlcjpwYXNz")))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn bearer_prefix_with_no_token_is_rejected() {
        let router = probe_router(vec!["secret-token".to_owned()]);
        let response = router
            .oneshot(request_with_auth(Some("Bearer ")))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn empty_string_in_expected_never_matches_a_whitespace_only_header() {
        // `token` is `.trim()`-ed before comparison, so "Bearer   " reduces
        // to an empty candidate. A naive `constant_time_eq` against an empty
        // configured entry would then "match" — this asserts the explicit
        // `!t.is_empty()` guard closes that hole, independent of whatever
        // filtering `resolve_http_bearer_tokens` happens to do upstream.
        let router = probe_router(vec![String::new()]);
        let response = router
            .oneshot(request_with_auth(Some("Bearer    ")))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn leading_and_trailing_whitespace_around_token_is_trimmed() {
        let router = probe_router(vec!["secret-token".to_owned()]);
        let response = router
            .oneshot(request_with_auth(Some("Bearer   secret-token   ")))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
}

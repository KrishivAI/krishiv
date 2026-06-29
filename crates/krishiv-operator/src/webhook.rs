//! Kubernetes validating admission webhook for `KrishivJob` resources.
//!
//! Exposes a `POST /validate` endpoint that accepts an `AdmissionReview`
//! request, calls [`validate_resource`] on the submitted object, and returns an
//! `AdmissionReview` response with `allowed: true/false`.
//!
//! # Wire format
//!
//! Kubernetes sends:
//! ```json
//! {
//!   "apiVersion": "admission.k8s.io/v1",
//!   "kind": "AdmissionReview",
//!   "request": {
//!     "uid": "<uuid>",
//!     "object": { /* KrishivJobResource fields */ }
//!   }
//! }
//! ```
//!
//! The handler returns:
//! ```json
//! {
//!   "apiVersion": "admission.k8s.io/v1",
//!   "kind": "AdmissionReview",
//!   "response": {
//!     "uid": "<same uuid>",
//!     "allowed": true,
//!     "status": { "code": 400, "message": "..." }  // only when allowed=false
//!   }
//! }
//! ```

use axum::Router;
use axum::body::Bytes;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::post;
use serde::{Deserialize, Serialize};

use crate::crd::job::KrishivJobResource;
use crate::reconciler::validate_resource;

// ── Wire types ────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct AdmissionReviewRequest {
    /// Required by the Kubernetes AdmissionReview API spec; not consumed by
    /// the validation handler (which only inspects `request.object`).
    #[expect(
        dead_code,
        reason = "AdmissionReview spec field; handler reads request.object only"
    )]
    #[serde(rename = "apiVersion", default)]
    api_version: String,
    /// Required by the Kubernetes AdmissionReview API spec; not consumed by
    /// the validation handler (which only inspects `request.object`).
    #[expect(
        dead_code,
        reason = "AdmissionReview spec field; handler reads request.object only"
    )]
    #[serde(default)]
    kind: String,
    request: AdmissionRequest,
}

#[derive(Debug, Deserialize)]
struct AdmissionRequest {
    uid: String,
    object: serde_json::Value,
}

#[derive(Debug, Serialize)]
struct AdmissionReviewResponse {
    #[serde(rename = "apiVersion")]
    api_version: String,
    kind: String,
    response: AdmissionResponse,
}

#[derive(Debug, Serialize)]
struct AdmissionResponse {
    uid: String,
    allowed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<AdmissionStatus>,
}

#[derive(Debug, Serialize)]
struct AdmissionStatus {
    code: u16,
    message: String,
}

// ── Core logic ────────────────────────────────────────────────────────────────

/// Process a raw `AdmissionReview` JSON body and return the response JSON.
///
/// Returns an error string if the request body is not parseable.
pub fn handle_admission_review(body: &str) -> Result<String, String> {
    let review: AdmissionReviewRequest =
        serde_json::from_str(body).map_err(|e| format!("invalid AdmissionReview JSON: {e}"))?;

    let uid = review.request.uid.clone();
    let resource: Result<KrishivJobResource, _> =
        serde_json::from_value(review.request.object.clone());

    let (allowed, status) = match resource {
        Err(e) => (
            false,
            Some(AdmissionStatus {
                code: 400,
                message: format!("could not deserialise KrishivJob: {e}"),
            }),
        ),
        Ok(res) => match validate_resource(&res) {
            Ok(()) => (true, None),
            Err(e) => (
                false,
                Some(AdmissionStatus {
                    code: 400,
                    message: e.to_string(),
                }),
            ),
        },
    };

    let response = AdmissionReviewResponse {
        api_version: "admission.k8s.io/v1".to_string(),
        kind: "AdmissionReview".to_string(),
        response: AdmissionResponse {
            uid,
            allowed,
            status,
        },
    };

    serde_json::to_string(&response).map_err(|e| format!("serialisation error: {e}"))
}

// ── Axum handler ──────────────────────────────────────────────────────────────

async fn admission_handler(body: Bytes) -> impl IntoResponse {
    let body_str = match std::str::from_utf8(&body) {
        Ok(s) => s,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                "request body is not valid UTF-8".to_string(),
            );
        }
    };

    match handle_admission_review(body_str) {
        Ok(response_json) => (StatusCode::OK, response_json),
        Err(e) => (StatusCode::BAD_REQUEST, e),
    }
}

/// Build an axum [`Router`] with the `/validate` admission webhook endpoint.
pub fn admission_router() -> Router {
    Router::new().route("/validate", post(admission_handler))
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::constants::{API_GROUP, API_VERSION, KIND};

    fn valid_review(name: &str) -> String {
        format!(
            r#"{{
                "apiVersion": "admission.k8s.io/v1",
                "kind": "AdmissionReview",
                "request": {{
                    "uid": "test-uid-1234",
                    "object": {{
                        "apiVersion": "{API_GROUP}/{API_VERSION}",
                        "kind": "{KIND}",
                        "metadata": {{ "name": "{name}" }},
                        "spec": {{ "mode": "batch", "image": "krishiv:latest", "tasks": 2 }}
                    }}
                }}
            }}"#
        )
    }

    #[test]
    fn valid_krishivjob_is_allowed() {
        let response_json = handle_admission_review(&valid_review("my-job")).unwrap();
        let v: serde_json::Value = serde_json::from_str(&response_json).unwrap();
        assert_eq!(v["response"]["allowed"], serde_json::Value::Bool(true));
        assert_eq!(v["response"]["uid"], "test-uid-1234");
    }

    #[test]
    fn empty_name_is_rejected() {
        let body = format!(
            r#"{{
                "apiVersion": "admission.k8s.io/v1",
                "kind": "AdmissionReview",
                "request": {{
                    "uid": "uid-empty-name",
                    "object": {{
                        "apiVersion": "{API_GROUP}/{API_VERSION}",
                        "kind": "{KIND}",
                        "metadata": {{ "name": "" }},
                        "spec": {{ "mode": "batch", "image": "krishiv:latest", "tasks": 1 }}
                    }}
                }}
            }}"#
        );
        let response_json = handle_admission_review(&body).unwrap();
        let v: serde_json::Value = serde_json::from_str(&response_json).unwrap();
        assert_eq!(v["response"]["allowed"], serde_json::Value::Bool(false));
        assert!(
            v["response"]["status"]["message"]
                .as_str()
                .unwrap()
                .contains("name")
        );
    }

    #[test]
    fn zero_tasks_is_rejected() {
        let body = format!(
            r#"{{
                "apiVersion": "admission.k8s.io/v1",
                "kind": "AdmissionReview",
                "request": {{
                    "uid": "uid-zero-tasks",
                    "object": {{
                        "apiVersion": "{API_GROUP}/{API_VERSION}",
                        "kind": "{KIND}",
                        "metadata": {{ "name": "valid-name" }},
                        "spec": {{ "mode": "batch", "image": "krishiv:latest", "tasks": 0 }}
                    }}
                }}
            }}"#
        );
        let response_json = handle_admission_review(&body).unwrap();
        let v: serde_json::Value = serde_json::from_str(&response_json).unwrap();
        assert_eq!(v["response"]["allowed"], serde_json::Value::Bool(false));
        assert!(
            v["response"]["status"]["message"]
                .as_str()
                .unwrap()
                .contains("tasks")
        );
    }

    #[test]
    fn wrong_api_version_is_rejected() {
        let body = r#"{
            "apiVersion": "admission.k8s.io/v1",
            "kind": "AdmissionReview",
            "request": {
                "uid": "uid-bad-api",
                "object": {
                    "apiVersion": "wrong.group/v99",
                    "kind": "KrishivJob",
                    "metadata": { "name": "j" },
                    "spec": { "image": "img", "tasks": 1 }
                }
            }
        }"#;
        let response_json = handle_admission_review(body).unwrap();
        let v: serde_json::Value = serde_json::from_str(&response_json).unwrap();
        assert_eq!(v["response"]["allowed"], serde_json::Value::Bool(false));
    }

    #[test]
    fn invalid_json_body_returns_error() {
        let result = handle_admission_review("not json at all");
        assert!(result.is_err());
    }

    #[test]
    fn malformed_object_is_rejected() {
        let body = r#"{
            "apiVersion": "admission.k8s.io/v1",
            "kind": "AdmissionReview",
            "request": {
                "uid": "uid-malformed",
                "object": { "not": "a KrishivJob" }
            }
        }"#;
        let response_json = handle_admission_review(body).unwrap();
        let v: serde_json::Value = serde_json::from_str(&response_json).unwrap();
        assert_eq!(v["response"]["allowed"], serde_json::Value::Bool(false));
        assert!(v["response"]["status"]["code"].as_u64().unwrap() == 400);
    }

    #[test]
    fn response_carries_request_uid() {
        let response_json = handle_admission_review(&valid_review("uid-echo-job")).unwrap();
        let v: serde_json::Value = serde_json::from_str(&response_json).unwrap();
        assert_eq!(v["response"]["uid"], "test-uid-1234");
    }

    #[test]
    fn admission_router_exposes_validate_route() {
        let router = admission_router();
        let _ = router; // just verifies the router is constructable
    }
}

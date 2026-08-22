//! The embedded TanStack web console (task #152).
//!
//! `console/dist` (the Vite production build) is compiled into the binary
//! in release builds and read from disk in debug builds (`rust-embed`
//! semantics — an `npm run build` in `console/` is picked up without
//! recompiling in dev). Served under `/console` with SPA fallback: any
//! extensionless path renders `index.html` and the TanStack router
//! (basepath `/console`) restores state from the URL.
//!
//! This is deliberately NOT a second server: the module only serves static
//! assets on the coordinator's existing HTTP listener. The SPA calls the
//! coordinator's canonical `/api/v1/*` endpoints directly (same origin),
//! sending its stored bearer — it does not use the legacy askama UI's
//! read endpoints, which retire once the console reaches parity.

use axum::http::{StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};

#[derive(rust_embed::Embed)]
#[folder = "../../console/dist"]
struct Assets;

/// Serve a console asset or the SPA shell for `/console/...` paths.
pub async fn serve(uri: Uri) -> Response {
    let path = uri
        .path()
        .trim_start_matches("/console")
        .trim_start_matches('/');
    let candidate = if path.is_empty() { "index.html" } else { path };
    if let Some(file) = Assets::get(candidate) {
        return respond(candidate, file);
    }
    // Extensionless paths are SPA routes; anything else is a real 404.
    if !candidate.contains('.')
        && let Some(index) = Assets::get("index.html")
    {
        return respond("index.html", index);
    }
    (
        StatusCode::NOT_FOUND,
        "console not built into this binary — run `npm run build` in console/, then rebuild",
    )
        .into_response()
}

fn respond(path: &str, file: rust_embed::EmbeddedFile) -> Response {
    let mime = mime_guess::from_path(path).first_or_octet_stream();
    let cache = if path.starts_with("assets/") {
        // Hashed filenames: safe to cache forever.
        "public, max-age=31536000, immutable"
    } else {
        "no-cache"
    };
    (
        [
            (header::CONTENT_TYPE, mime.as_ref()),
            (header::CACHE_CONTROL, cache),
        ],
        file.data,
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use axum::http::Uri;

    /// The SPA fallback must serve index.html for extensionless routes and a
    /// real 404 for missing files — with an empty dist/ (CI without node),
    /// everything is the "console not built" 404, which is also asserted.
    #[tokio::test]
    async fn spa_fallback_and_missing_asset_semantics() {
        let index_built = super::Assets::get("index.html").is_some();
        let spa = super::serve(Uri::from_static("/console/jobs")).await;
        let missing = super::serve(Uri::from_static("/console/assets/nope.js")).await;
        if index_built {
            assert_eq!(spa.status(), axum::http::StatusCode::OK);
        } else {
            assert_eq!(spa.status(), axum::http::StatusCode::NOT_FOUND);
        }
        assert_eq!(missing.status(), axum::http::StatusCode::NOT_FOUND);
    }
}

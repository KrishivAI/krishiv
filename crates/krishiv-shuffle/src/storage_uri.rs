//! Open shuffle storage from production URIs (`file://`, `s3://`).

use std::sync::Arc;

use krishiv_common::DurabilityProfile;

use crate::{
    LocalDiskShuffleStore, ObjectStoreShuffleStore, ShuffleBackend, ShuffleError, ShuffleResult,
    tiered_store::TieredShuffleStore,
};

/// Build an S3 object store the same way the streaming sink does
/// (`krishiv_connectors::lakehouse::object_store_io::build_s3_object_store`):
/// `with_bucket_name` + explicit endpoint/creds/region, path-style over plain
/// HTTP. `AmazonS3Builder::from_env` honours only `AWS_ENDPOINT`, not the
/// AWS-SDK `AWS_ENDPOINT_URL` convention prod sets for MinIO; and `with_url`
/// was observed to intermittently fail MinIO writes ("error sending request")
/// where this builder succeeded from the same pod.
fn build_s3_store(bucket: &str) -> Result<object_store::aws::AmazonS3, object_store::Error> {
    let mut builder = object_store::aws::AmazonS3Builder::from_env().with_bucket_name(bucket);
    // Evict pooled HTTP connections after 1s idle so spaced writers don't reuse
    // a socket kube-proxy/MinIO silently closed (which hangs the 30s request
    // timeout). See the note in krishiv-state's checkpoint storage_uri. allow_http
    // must live on this ClientOptions — with_client_options REPLACES the options.
    let mut client_opts = object_store::ClientOptions::new()
        .with_pool_idle_timeout(std::time::Duration::from_secs(1));
    if let Ok(endpoint) = std::env::var("AWS_ENDPOINT_URL")
        && !endpoint.is_empty()
    {
        builder = builder.with_endpoint(endpoint);
        client_opts = client_opts.with_allow_http(true);
    }
    builder = builder.with_client_options(client_opts);
    if let Ok(key) = std::env::var("AWS_ACCESS_KEY_ID") {
        builder = builder.with_access_key_id(key);
    }
    if let Ok(secret) = std::env::var("AWS_SECRET_ACCESS_KEY") {
        builder = builder.with_secret_access_key(secret);
    }
    let region = std::env::var("AWS_REGION")
        .or_else(|_| std::env::var("AWS_DEFAULT_REGION"))
        .unwrap_or_else(|_| "us-east-1".to_string());
    builder = builder.with_region(region);
    builder.build()
}

/// Split `s3://bucket[/prefix]` into a built object-store shuffle store.
///
/// Extracted because the bucket/prefix split, the `"shuffle"` prefix default,
/// and the client construction were duplicated verbatim between
/// [`open_shuffle_backend_from_uri`] and [`open_tiered_shuffle_backend`].
/// Nothing enforced that the two stayed in step, so a fix to either — the
/// endpoint handling, say, which already has a MinIO-specific history — would
/// silently miss the other path.
///
/// `context` names the caller in errors, since the two produce different
/// messages for the same failure.
fn s3_shuffle_store(rest: &str, context: &str) -> ShuffleResult<Arc<ObjectStoreShuffleStore>> {
    let (bucket, prefix) = match rest.split_once('/') {
        Some((b, p)) => (b, p.trim_matches('/')),
        None => (rest, ""),
    };
    // An empty bucket reaches `AmazonS3Builder::build` as a missing-field
    // error that names neither the URI nor the caller. The equivalent check
    // already exists in krishiv-sql's object-store registry; the shuffle path
    // is the one that decides where a query's intermediate data lives, so it
    // should not be the laxer of the two.
    if bucket.is_empty() {
        return Err(ShuffleError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("{context}: s3 URI has no bucket (got 's3://{rest}')"),
        )));
    }
    let url = if prefix.is_empty() {
        format!("s3://{bucket}")
    } else {
        format!("s3://{bucket}/{prefix}")
    };
    let store = build_s3_store(bucket)
        .map_err(|e| ShuffleError::Io(std::io::Error::other(format!("{context} {url}: {e}"))))?;
    let storage_prefix = if prefix.is_empty() {
        "shuffle".to_owned()
    } else {
        prefix.to_owned()
    };
    Ok(Arc::new(ObjectStoreShuffleStore::new(
        Arc::new(store),
        storage_prefix,
    )))
}

/// Refuse an object-store shuffle under a profile that must not use one.
///
/// Shared by the plain and tiered entry points. They had the same rule stated
/// in two places — one as an `if`, one only as a doc comment — and only the
/// `if` was enforced, so the same `s3://` URI was accepted or refused depending
/// on whether a local dir happened to also be configured.
fn check_s3_profile(profile: DurabilityProfile, context: &str) -> ShuffleResult<()> {
    if profile != DurabilityProfile::DistributedDurable && profile != DurabilityProfile::DevLocal {
        return Err(ShuffleError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "{context} requires distributed-durable or dev-local profile \
                 (got '{profile}')"
            ),
        )));
    }
    Ok(())
}

/// Open a shuffle backend for the configured URI and durability profile.
///
/// - `file://path` or bare path — local disk shuffle store
/// - `s3://bucket/prefix` — object-store shuffle (distributed durable)
pub fn open_shuffle_backend_from_uri(
    uri: &str,
    profile: DurabilityProfile,
) -> ShuffleResult<Arc<ShuffleBackend>> {
    let trimmed = uri.trim();
    if trimmed.is_empty() {
        return Err(ShuffleError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "shuffle URI is empty",
        )));
    }

    if trimmed.starts_with("memory://") {
        if !krishiv_common::allows_unbounded_shuffle_store(profile) {
            return Err(ShuffleError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("memory:// shuffle URIs are forbidden for durability profile '{profile}'"),
            )));
        }
        return Ok(Arc::new(ShuffleBackend::InMemory(Arc::new(
            crate::InMemoryShuffleStore::new(),
        ))));
    }

    if let Some(rest) = trimmed.strip_prefix("s3://") {
        check_s3_profile(profile, "s3:// shuffle")?;
        let object = s3_shuffle_store(rest, "s3 shuffle store")?;
        return Ok(Arc::new(ShuffleBackend::Object(object)));
    }

    let path = trimmed.strip_prefix("file://").unwrap_or(trimmed);
    let disk = Arc::new(LocalDiskShuffleStore::new(path)?);
    Ok(Arc::new(ShuffleBackend::Local(disk)))
}

/// Build a `Tiered` shuffle backend: local-disk for fast same-host P2P reads,
/// object-store for cross-host durability.
///
/// `local_dir` is the local cache directory (created if absent).
/// `s3_uri` must be an `s3://bucket[/prefix]` URI.
///
/// Only valid for `DistributedDurable` (or `DevLocal` for testing) — the same
/// rule [`open_shuffle_backend_from_uri`] applies to a bare `s3://` URI, and
/// `profile` is taken here so it is enforced rather than merely documented.
/// It was documented-only, so a deployment that set both a local shuffle dir
/// and an `s3://` URI took this path and skipped a check the other path made:
/// the identical URI was accepted or refused depending on an unrelated setting.
pub fn open_tiered_shuffle_backend(
    local_dir: &std::path::Path,
    s3_uri: &str,
    profile: DurabilityProfile,
) -> ShuffleResult<Arc<ShuffleBackend>> {
    check_s3_profile(profile, "tiered shuffle")?;
    let local = Arc::new(LocalDiskShuffleStore::new(local_dir)?);

    let rest = s3_uri.strip_prefix("s3://").ok_or_else(|| {
        ShuffleError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("tiered shuffle remote URI must be s3://, got: {s3_uri}"),
        ))
    })?;
    let remote = s3_shuffle_store(rest, "tiered shuffle s3 store")?;

    Ok(Arc::new(ShuffleBackend::Tiered(Arc::new(
        TieredShuffleStore::new(local, remote),
    ))))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    /// `ShuffleBackend` has no `Debug`, so `expect_err` will not compile here.
    fn expect_refused(result: ShuffleResult<Arc<ShuffleBackend>>, why: &str) -> String {
        match result {
            Ok(_) => panic!("{why}"),
            Err(e) => e.to_string(),
        }
    }

    // This file had **0.00% coverage** — 181 regions, never executed by any
    // test — while deciding where every query's intermediate data lands. The
    // tests below cover the routing and the refusals, which need no network.

    #[test]
    fn an_empty_uri_is_refused() {
        let err = expect_refused(
            open_shuffle_backend_from_uri("   ", DurabilityProfile::DevLocal),
            "an empty shuffle URI cannot be routed anywhere",
        );
        assert!(err.contains("empty"), "{err}");
    }

    #[test]
    fn a_bare_path_and_a_file_uri_both_open_local_disk() {
        let dir = tempfile::tempdir().unwrap();
        for uri in [
            dir.path().to_string_lossy().to_string(),
            format!("file://{}", dir.path().display()),
        ] {
            let backend = open_shuffle_backend_from_uri(&uri, DurabilityProfile::DevLocal)
                .unwrap_or_else(|e| panic!("{uri} should open local disk: {e}"));
            assert!(
                matches!(backend.as_ref(), ShuffleBackend::Local(_)),
                "{uri} did not route to local disk"
            );
        }
    }

    /// `memory://` loses data on restart, so it is dev-only. A durable profile
    /// asking for it is a misconfiguration that must fail loudly rather than
    /// silently accept an unbounded in-memory store.
    #[test]
    fn memory_shuffle_is_refused_for_durable_profiles() {
        for profile in [
            DurabilityProfile::SingleNodeDurable,
            DurabilityProfile::DistributedDurable,
        ] {
            let result = open_shuffle_backend_from_uri("memory://", profile);
            let err = match result {
                Ok(_) => panic!("{profile:?} must not accept memory:// shuffle"),
                Err(e) => e.to_string(),
            };
            assert!(
                err.contains("forbidden"),
                "{profile:?}: the refusal must say why — {err}"
            );
        }
    }

    /// An `s3://` URI with no bucket used to reach `AmazonS3Builder::build` as
    /// a missing-field error naming neither the URI nor the caller.
    #[test]
    fn an_s3_uri_without_a_bucket_is_refused_by_name() {
        let msg = expect_refused(
            open_shuffle_backend_from_uri("s3://", DurabilityProfile::DevLocal),
            "s3:// with no bucket cannot be opened",
        );
        assert!(msg.contains("no bucket"), "must say what is wrong: {msg}");
        assert!(msg.contains("shuffle"), "must say who is complaining: {msg}");
    }

    /// Same refusal on the tiered path — the point of extracting the helper is
    /// that the two callers cannot drift apart on validation.
    #[test]
    fn the_tiered_path_shares_the_same_bucket_validation() {
        let dir = tempfile::tempdir().unwrap();
        let err = expect_refused(
            open_tiered_shuffle_backend(dir.path(), "s3://", DurabilityProfile::DevLocal),
            "s3:// with no bucket cannot be opened",
        );
        assert!(err.contains("no bucket"), "{err}");
    }

    #[test]
    fn the_tiered_remote_must_be_an_s3_uri() {
        let dir = tempfile::tempdir().unwrap();
        let err = expect_refused(
            open_tiered_shuffle_backend(dir.path(), "file:///tmp/nope", DurabilityProfile::DevLocal),
            "a tiered remote must be object storage",
        );
        assert!(err.contains("must be s3://"), "{err}");
    }

    /// The profile rule must not depend on which entry point you reach.
    ///
    /// It was stated as an `if` on one path and only as a doc comment on the
    /// other, so the same `s3://` URI under the same profile was refused or
    /// accepted according to whether a local shuffle dir happened to be set —
    /// the setting that decides *tiered vs plain*, which has nothing to do with
    /// whether object-store shuffle is permitted at all.
    #[test]
    fn both_entry_points_refuse_s3_under_a_non_distributed_profile() {
        let dir = tempfile::tempdir().unwrap();
        let profile = DurabilityProfile::SingleNodeDurable;
        let plain = expect_refused(
            open_shuffle_backend_from_uri("s3://bucket/pfx", profile),
            "the plain path must refuse s3 under single-node-durable",
        );
        let tiered = expect_refused(
            open_tiered_shuffle_backend(dir.path(), "s3://bucket/pfx", profile),
            "the tiered path must refuse s3 under single-node-durable too",
        );
        for err in [&plain, &tiered] {
            assert!(
                err.contains("distributed-durable"),
                "the refusal must name the requirement: {err}"
            );
        }
    }
}

//! An object-store registry that builds cloud stores on first use.
//!
//! DataFusion resolves an object store by scheme+authority through the
//! runtime's [`ObjectStoreRegistry`]. The default registry only knows what was
//! explicitly registered, which forces every code path that might touch
//! `s3://` to remember to register the bucket first. That is a rule nothing
//! enforces, and the paths that forgot did not fail loudly:
//!
//! - the stage builder planned on a context with no store, so `register_parquet`
//!   errored, the caller read that as "decline to stage", and the query silently
//!   ran on ONE executor instead of the cluster;
//! - the executor decoding a `dfplan:` fragment failed outright with
//!   "No suitable object store found for s3://…", because a serialized physical
//!   plan carries file paths but no way to register their backing store.
//!
//! The executor case in particular cannot be fixed by registering ahead of
//! time: the executor learns which buckets a plan touches only by decoding the
//! plan, and the decode is what needs the store. So resolution has to be lazy.
//!
//! This registry delegates to the default one and, on a miss for an
//! object-store scheme it can construct, builds the store, caches it, and
//! returns it. Explicit registration still wins, so a caller that wants
//! specific credentials or an emulator endpoint can install its own store and
//! this never overrides it.

use std::sync::Arc;

use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::object_store::{DefaultObjectStoreRegistry, ObjectStoreRegistry};
use object_store::ObjectStore;
use url::Url;

/// Registry that lazily constructs S3-compatible stores on first reference.
#[derive(Debug, Default)]
pub struct LazyCloudObjectStoreRegistry {
    inner: DefaultObjectStoreRegistry,
}

impl LazyCloudObjectStoreRegistry {
    pub fn new() -> Self {
        Self::default()
    }
}

impl ObjectStoreRegistry for LazyCloudObjectStoreRegistry {
    fn register_store(
        &self,
        url: &Url,
        store: Arc<dyn ObjectStore>,
    ) -> Option<Arc<dyn ObjectStore>> {
        self.inner.register_store(url, store)
    }

    fn get_store(&self, url: &Url) -> DataFusionResult<Arc<dyn ObjectStore>> {
        // Explicit registration wins: only fall through to construction when
        // the default registry does not already have a store for this bucket.
        if let Ok(store) = self.inner.get_store(url) {
            return Ok(store);
        }

        if matches!(url.scheme(), "s3" | "s3a") {
            let bucket = url.host_str().unwrap_or_default();
            if bucket.is_empty() {
                return Err(DataFusionError::Execution(format!(
                    "object-store url {url} has no bucket"
                )));
            }
            let store = crate::build_s3_object_store(bucket).map_err(|error| {
                DataFusionError::Execution(format!(
                    "cannot build an S3 object store for bucket '{bucket}': {error}"
                ))
            })?;
            let store: Arc<dyn ObjectStore> = store;
            // Cache under the scheme+authority key DataFusion looks up, so the
            // next reference to this bucket is a plain map hit.
            self.inner.register_store(url, Arc::clone(&store));
            return Ok(store);
        }

        // Not a scheme we can construct: surface the default registry's own
        // error, which names the url and points at register_object_store.
        self.inner.get_store(url)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The point of the registry: a bucket nobody registered still resolves.
    /// Under the default registry this is an error, which is exactly what made
    /// staged planning decline and dfplan decode fail.
    #[test]
    fn an_unregistered_s3_bucket_resolves() {
        let registry = LazyCloudObjectStoreRegistry::new();
        let url = Url::parse("s3://krishiv-bench/tpch/sf100/lineitem/").expect("url");
        assert!(
            registry.get_store(&url).is_ok(),
            "an s3 bucket must resolve without prior registration"
        );
    }

    /// Two references to the same bucket must return the same cached store
    /// rather than rebuilding a client per file scan.
    #[test]
    fn repeated_lookups_reuse_one_store() {
        let registry = LazyCloudObjectStoreRegistry::new();
        let url = Url::parse("s3://krishiv-bench/a").expect("url");
        let first = registry.get_store(&url).expect("first lookup");
        let second = registry.get_store(&url).expect("second lookup");
        assert!(
            Arc::ptr_eq(&first, &second),
            "the store must be cached, not rebuilt per lookup"
        );
    }

    /// Explicit registration must not be overridden — callers that install a
    /// store with specific credentials keep it.
    #[test]
    fn explicit_registration_wins() {
        let registry = LazyCloudObjectStoreRegistry::new();
        let url = Url::parse("s3://explicit-bucket/").expect("url");
        let installed: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        registry.register_store(&url, Arc::clone(&installed));
        let resolved = registry.get_store(&url).expect("lookup");
        assert!(
            Arc::ptr_eq(&installed, &resolved),
            "an explicitly registered store must win over lazy construction"
        );
    }

    /// A scheme the registry cannot build must still produce the default
    /// registry's error rather than a confusing S3-flavoured one.
    #[test]
    fn unknown_schemes_keep_the_default_error() {
        let registry = LazyCloudObjectStoreRegistry::new();
        let url = Url::parse("ftp://somewhere/path").expect("url");
        assert!(registry.get_store(&url).is_err());
    }
}

#![forbid(unsafe_code)]

//! P6: Async Operator Execution — non-blocking state access for streaming operators.
//!
//! Inspired by Flink 2.0's asynchronous execution model (FLIP-425), this module
//! provides `StateFuture`-based callbacks that decouple state access from the
//! operator thread, improving throughput for state-heavy workloads.
//!
//! The async operator execution model decouples state access from the operator
//! thread, allowing multiple state lookups to proceed concurrently:
//!
//! - The operator thread processes records and launches state futures
//! - State futures are driven by the Tokio async runtime
//! - Callbacks run when state access completes
//! - Results are collected and forwarded to downstream operators
//!
//! This improves throughput for state-heavy workloads by avoiding blocking
//! on individual state accesses.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::backend::StateBackend;
use crate::error::StateResult;
use crate::namespace::Namespace;

/// A boxed future that resolves to a state access result.
type BoxFuture<T> = Pin<Box<dyn Future<Output = StateResult<T>> + Send>>;

/// A boxed transform callback for `StateFuture`.
type TransformFn<T> = Box<dyn FnOnce(Option<Vec<u8>>) -> T + Send>;

// ── StateFuture ──────────────────────────────────────────────────────────────

/// A future that resolves to the result of a state access.
///
/// `StateFuture<T>` wraps a state access operation and allows chaining
/// a callback via `.then()`. The callback is executed on the operator's
/// async runtime without blocking the task thread.
pub struct StateFuture<T> {
    inner: BoxFuture<Option<Vec<u8>>>,
    transform: Option<TransformFn<T>>,
}

impl<T: Send + 'static> StateFuture<T> {
    /// Create a new `StateFuture` from an async state access.
    pub fn new<F>(future: F) -> Self
    where
        F: Future<Output = StateResult<Option<Vec<u8>>>> + Send + 'static,
    {
        Self {
            inner: Box::pin(future),
            transform: None,
        }
    }

    /// Chain a transformation callback that runs when the state access completes.
    ///
    /// The callback receives the raw value bytes (or `None` if the key is absent)
    /// and returns a transformed result of type `U`.
    pub fn then<F, U>(self, f: F) -> StateFuture<U>
    where
        F: FnOnce(Option<Vec<u8>>) -> U + Send + 'static,
    {
        let mut next = StateFuture::<U> {
            inner: self.inner,
            transform: None,
        };
        next.transform = Some(Box::new(f));
        next
    }

    /// Await the state access and apply the transformation.
    pub async fn await_result(self) -> StateResult<T> {
        let value = self.inner.await?;
        let transform =
            self.transform
                .ok_or_else(|| crate::error::StateError::BackendUnavailable {
                    message: "StateFuture must have a transform callback".to_string(),
                    source: None,
                })?;
        Ok(transform(value))
    }
}

// ── AsyncOperatorContext ──────────────────────────────────────────────────────

/// Context provided to async operators for launching non-blocking state accesses.
///
/// Wraps a reference to the state backend and provides convenience methods
/// for common access patterns.
pub struct AsyncOperatorContext {
    backend: Arc<dyn StateBackend + Send + Sync>,
    namespace: Namespace,
}

impl AsyncOperatorContext {
    /// Create a new context for the given backend and namespace.
    pub fn new(backend: Arc<dyn StateBackend + Send + Sync>, namespace: Namespace) -> Self {
        Self { backend, namespace }
    }

    /// Launch a non-blocking state get operation.
    ///
    /// Returns a `StateFuture<Option<Vec<u8>>>` that resolves when the value is available.
    /// Uses `spawn_blocking` to avoid blocking Tokio worker threads for sync backends
    /// like RocksDB.
    pub fn state_get(&self, key: &[u8]) -> StateFuture<Option<Vec<u8>>> {
        let backend = self.backend.clone();
        let ns = self.namespace.clone();
        let key = key.to_vec();
        StateFuture::new(async move {
            let result = tokio::task::spawn_blocking(move || backend.get(&ns, &key))
                .await
                .map_err(|e| crate::error::StateError::BackendUnavailable {
                    message: format!("spawn_blocking join error: {e}"),
                    source: None,
                })??;
            Ok(result)
        })
    }

    /// Get a reference to the underlying backend.
    pub fn backend(&self) -> &Arc<dyn StateBackend + Send + Sync> {
        &self.backend
    }

    /// Get the namespace for this context.
    pub fn namespace(&self) -> &Namespace {
        &self.namespace
    }
}

// ── AsyncStateOperator trait ─────────────────────────────────────────────────

/// Trait for streaming operators that use non-blocking state access.
///
/// Operators implementing this trait receive input of type `I` and return
/// futures that resolve when state access completes. The async runtime drives
/// these futures concurrently, improving throughput for state-heavy workloads.
pub trait AsyncStateOperator<I>: Send + Sync {
    /// Process input and return state futures for async resolution.
    ///
    /// Each future represents a state access that should be performed
    /// concurrently. The operator's callback is chained onto each future.
    fn process(&self, ctx: &AsyncOperatorContext, input: &I) -> Vec<StateFuture<I>>;
}

// ── AsyncOperatorExecutor ────────────────────────────────────────────────────

/// Executor that drives `AsyncStateOperator` instances, running state accesses
/// concurrently via Tokio's async runtime.
pub struct AsyncOperatorExecutor {
    concurrency_limit: usize,
}

impl AsyncOperatorExecutor {
    /// Create a new executor with the given concurrency limit.
    pub fn new(concurrency_limit: usize) -> Self {
        Self {
            concurrency_limit: concurrency_limit.max(1),
        }
    }

    /// Drive a set of state futures to completion with bounded concurrency.
    ///
    /// Each batch of up to `concurrency_limit` futures is polled concurrently
    /// (`join_all`), so independent state accesses overlap. The previous
    /// implementation awaited every future sequentially — the batching was a
    /// no-op and "async operator execution" was serial in disguise.
    pub async fn drive_futures<T: Send + 'static>(
        &self,
        futures: Vec<StateFuture<T>>,
    ) -> StateResult<Vec<T>> {
        let mut results = Vec::with_capacity(futures.len());
        let mut remaining: Vec<_> = futures.into_iter().collect();

        while !remaining.is_empty() {
            let batch_size = remaining.len().min(self.concurrency_limit);
            let batch: Vec<_> = remaining.drain(..batch_size).collect();
            let batch_results =
                futures::future::join_all(batch.into_iter().map(StateFuture::await_result)).await;
            for result in batch_results {
                results.push(result?);
            }
        }

        Ok(results)
    }

    /// Process input through an operator and collect results.
    pub async fn process_operator<I: Send + 'static>(
        &self,
        operator: &dyn AsyncStateOperator<I>,
        ctx: &AsyncOperatorContext,
        input: &I,
    ) -> StateResult<Vec<I>> {
        let futures = operator.process(ctx, input);
        self.drive_futures(futures).await
    }
}

// ── BatchedStateAccess ───────────────────────────────────────────────────────

/// Utility for batching state accesses to reduce per-key overhead.
///
/// Collects multiple `get` requests and dispatches them as a single batch,
/// then distributes results to waiting futures.
pub struct BatchedStateAccess {
    backend: Arc<dyn StateBackend + Send + Sync>,
    batch_size: usize,
}

impl BatchedStateAccess {
    /// Create a new batched accessor with the given backend and batch size.
    pub fn new(backend: Arc<dyn StateBackend + Send + Sync>, batch_size: usize) -> Self {
        Self {
            backend,
            batch_size: batch_size.max(1),
        }
    }

    /// Get multiple keys at once from the backend.
    ///
    /// Delegates to `StateBackend::get_batch` so backends with a real batched
    /// read path (RocksDB opens one read transaction for all keys) are used —
    /// the previous per-key loop bypassed those overrides.
    pub fn get_batch(
        &self,
        namespace: &Namespace,
        keys: &[Vec<u8>],
    ) -> StateResult<Vec<Option<Vec<u8>>>> {
        let triples: Vec<(&str, &str, &[u8])> = keys
            .iter()
            .map(|key| {
                (
                    namespace.operator_id(),
                    namespace.state_name(),
                    key.as_slice(),
                )
            })
            .collect();
        self.backend.get_batch(&triples)
    }

    /// Get a single key, returning a future for consistency with batched pattern.
    pub async fn get(&self, namespace: &Namespace, key: &[u8]) -> StateResult<Option<Vec<u8>>> {
        self.backend.get(namespace, key)
    }

    /// Return the configured batch size.
    pub fn batch_size(&self) -> usize {
        self.batch_size
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::backend::InMemoryStateBackend;

    fn make_ctx() -> (Arc<InMemoryStateBackend>, AsyncOperatorContext) {
        let mut backend = InMemoryStateBackend::default();
        let ns = Namespace::new("op-1", "state");
        backend
            .put(&ns, b"key1".to_vec(), b"val1".to_vec())
            .unwrap();
        let backend = Arc::new(backend);
        let ctx = AsyncOperatorContext::new(backend.clone(), ns);
        (backend, ctx)
    }

    #[tokio::test]
    async fn state_future_resolves() {
        let (_backend, ctx) = make_ctx();
        let fut = ctx.state_get(b"key1").then(|v| v);
        let result = fut.await_result().await.unwrap();
        assert_eq!(result, Some(b"val1".to_vec()));
    }

    #[tokio::test]
    async fn state_future_with_transform() {
        let (_backend, ctx) = make_ctx();
        let fut = ctx.state_get(b"key1").then(|val| val.map(|v| v.len()));
        let result = fut.await_result().await.unwrap();
        assert_eq!(result, Some(4));
    }

    #[tokio::test]
    async fn state_future_missing_key() {
        let (_backend, ctx) = make_ctx();
        let fut = ctx.state_get(b"no-such-key").then(|v| v);
        let result = fut.await_result().await.unwrap();
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn executor_drives_futures_concurrently() {
        let (_backend, ctx) = make_ctx();
        let executor = AsyncOperatorExecutor::new(4);

        let mut futures = Vec::new();
        for _ in 0..10 {
            futures.push(ctx.state_get(b"key1").then(|v| v));
        }

        let results = executor.drive_futures(futures).await.unwrap();
        assert_eq!(results.len(), 10);
        for r in results {
            assert_eq!(r, Some(b"val1".to_vec()));
        }
    }

    #[tokio::test]
    async fn batched_state_access_works() {
        let (backend, _ctx) = make_ctx();
        let batched = BatchedStateAccess::new(backend, 10);
        let ns = Namespace::new("op-1", "state");

        let result = batched.get(&ns, b"key1").await.unwrap();
        assert_eq!(result, Some(b"val1".to_vec()));

        let keys: Vec<Vec<u8>> = vec![b"key1".to_vec(), b"missing".to_vec()];
        let results = batched.get_batch(&ns, &keys).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0], Some(b"val1".to_vec()));
        assert_eq!(results[1], None);
    }

    /// F7: drive_futures must actually overlap state accesses within a batch.
    /// Each backend get sleeps 50ms and records the peak number of concurrent
    /// gets; a sequential executor (the pre-fix behavior) never exceeds 1.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn drive_futures_overlaps_accesses_within_a_batch() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct SlowBackend {
            current: AtomicUsize,
            peak: AtomicUsize,
        }
        impl StateBackend for SlowBackend {
            fn get(&self, _: &Namespace, _: &[u8]) -> StateResult<Option<Vec<u8>>> {
                let now = self.current.fetch_add(1, Ordering::SeqCst) + 1;
                self.peak.fetch_max(now, Ordering::SeqCst);
                std::thread::sleep(std::time::Duration::from_millis(50));
                self.current.fetch_sub(1, Ordering::SeqCst);
                Ok(Some(b"v".to_vec()))
            }
            fn put(&mut self, _: &Namespace, _: Vec<u8>, _: Vec<u8>) -> StateResult<()> {
                Ok(())
            }
            fn delete(&mut self, _: &Namespace, _: &[u8]) -> StateResult<()> {
                Ok(())
            }
            fn clear_namespace(&mut self, _: &Namespace) -> StateResult<()> {
                Ok(())
            }
            fn list_namespaces(&self) -> StateResult<Vec<Namespace>> {
                Ok(vec![])
            }
            fn list_keys(&self, _: &Namespace) -> StateResult<Vec<Vec<u8>>> {
                Ok(vec![])
            }
            fn snapshot(&self) -> StateResult<Vec<u8>> {
                Ok(vec![])
            }
            fn load_snapshot(&mut self, _: &[u8]) -> StateResult<()> {
                Ok(())
            }
        }

        let backend = Arc::new(SlowBackend {
            current: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
        });
        let ctx = AsyncOperatorContext::new(backend.clone(), Namespace::new("op", "s"));
        let executor = AsyncOperatorExecutor::new(4);
        let futures: Vec<_> = (0..4).map(|_| ctx.state_get(b"k").then(|v| v)).collect();
        let results = executor.drive_futures(futures).await.unwrap();
        assert_eq!(results.len(), 4);
        assert!(
            backend.peak.load(Ordering::SeqCst) >= 2,
            "a batch of 4 with limit 4 must overlap at least 2 accesses; \
             peak={} means execution was sequential",
            backend.peak.load(Ordering::SeqCst)
        );
    }
}

/// Typed error returned by [`FaultInjector::apply`].
#[derive(Debug, thiserror::Error)]
pub enum ChaosError {
    /// The chaos injector dropped the operation entirely (no `operation()`
    /// call was made; the caller can decide whether to retry).
    #[error("operation dropped by chaos injector")]
    Dropped,
    /// The chaos injector synthesised an error with the given message
    /// instead of running the operation.
    #[error("injected fault: {0}")]
    Injected(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FaultMode {
    Delay { duration_ms: u64 },
    Error { message: String },
    Drop,
    None,
}

pub struct FaultInjector {
    faults: Vec<FaultMode>,
    call_count: std::sync::atomic::AtomicUsize,
}

impl FaultInjector {
    pub fn new(faults: Vec<FaultMode>) -> Self {
        Self {
            faults,
            call_count: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    pub fn next_fault(&self) -> &FaultMode {
        if self.faults.is_empty() {
            return &FaultMode::None;
        }
        let idx = self
            .call_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        // is_empty() check above guarantees len > 0 so modulo is safe.
        self.faults
            .get(idx % self.faults.len())
            .unwrap_or(&FaultMode::None)
    }

    /// Apply the next fault, returning a typed [`ChaosError`] on failure.
    pub fn apply<F, Fut, T>(
        &self,
        operation: F,
    ) -> impl std::future::Future<Output = Result<T, ChaosError>>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = T>,
    {
        let fault = self.next_fault().clone();
        async move {
            match fault {
                FaultMode::None => Ok(operation().await),
                FaultMode::Delay { duration_ms } => {
                    tokio::time::sleep(std::time::Duration::from_millis(duration_ms)).await;
                    Ok(operation().await)
                }
                FaultMode::Error { message } => Err(ChaosError::Injected(message)),
                FaultMode::Drop => Err(ChaosError::Dropped),
            }
        }
    }
}

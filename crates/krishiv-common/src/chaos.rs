#![forbid(unsafe_code)]

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
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        &self.faults[idx % self.faults.len()]
    }

    pub fn apply<F, Fut, T>(
        &self,
        operation: F,
    ) -> impl std::future::Future<Output = Result<T, String>>
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
                FaultMode::Error { message } => Err(message),
                FaultMode::Drop => Err(String::from("operation dropped by chaos injector")),
            }
        }
    }
}

#![forbid(unsafe_code)]

pub type IvmResult<T> = Result<T, IvmError>;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum IvmError {
    #[error("ivm error: {0}")]
    Execution(String),
}

impl IvmError {
    pub fn execution(msg: impl Into<String>) -> Self {
        Self::Execution(msg.into())
    }
}

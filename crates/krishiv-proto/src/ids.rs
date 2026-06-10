//! Typed identifiers.

use std::fmt;

/// Result alias for control-plane contract validation.
pub type ProtoResult<T> = Result<T, IdError>;

/// Identifier validation error.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{kind} {reason}")]
pub struct IdError {
    kind: &'static str,
    reason: &'static str,
}

impl IdError {
    fn empty(kind: &'static str) -> Self {
        Self {
            kind,
            reason: "cannot be empty",
        }
    }

    fn zero(kind: &'static str) -> Self {
        Self {
            kind,
            reason: "must be greater than zero",
        }
    }

    pub(crate) fn range(kind: &'static str) -> Self {
        Self {
            kind,
            reason: "start must not exceed end",
        }
    }

    /// Identifier kind that failed validation.
    pub fn kind(&self) -> &'static str {
        self.kind
    }

    /// Human-readable validation reason.
    pub fn reason(&self) -> &'static str {
        self.reason
    }
}

macro_rules! id_type {
    ($name:ident, $kind:literal) => {
        #[doc = concat!("Typed ", $kind, " identifier.")]
        #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
        #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
        pub struct $name(String);

        impl $name {
            #[doc = concat!("Create a ", $kind, " identifier after validation.")]
            pub fn try_new(value: impl Into<String>) -> ProtoResult<Self> {
                let value = value.into();
                if value.trim().is_empty() {
                    return Err(IdError::empty($kind));
                }
                Ok(Self(value))
            }

            /// Borrow the identifier string.
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }
    };
}

id_type!(CoordinatorId, "coordinator id");
id_type!(JobId, "job id");
id_type!(StageId, "stage id");
id_type!(TaskId, "task id");
id_type!(ExecutorId, "executor id");
id_type!(PartitionId, "partition id");
id_type!(OperatorId, "operator id");

/// Monotonic task attempt identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct AttemptId(u32);

impl AttemptId {
    /// Create an attempt id after validation.
    pub fn try_new(value: u32) -> ProtoResult<Self> {
        if value == 0 {
            return Err(IdError::zero("attempt id"));
        }
        Ok(Self(value))
    }

    /// First attempt for a task.
    pub fn initial() -> Self {
        Self(1)
    }

    /// Next monotonic attempt id.
    pub fn next(self) -> Self {
        let next = self.0.saturating_add(1);
        if next == u32::MAX {
            tracing::warn!(current = self.0, "AttemptId saturated at u32::MAX; further retries cannot be distinguished");
        }
        Self(next)
    }

    /// Numeric attempt id.
    pub fn as_u32(self) -> u32 {
        self.0
    }
}

impl fmt::Display for AttemptId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Monotonic executor lease generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct LeaseGeneration(u64);

impl LeaseGeneration {
    /// Create a lease generation after validation.
    pub fn try_new(value: u64) -> ProtoResult<Self> {
        if value == 0 {
            return Err(IdError::zero("lease generation"));
        }
        Ok(Self(value))
    }

    /// First lease generation for an executor registration.
    pub fn initial() -> Self {
        Self(1)
    }

    /// Next monotonic lease generation.
    pub fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }

    /// Numeric lease generation.
    pub fn as_u64(self) -> u64 {
        self.0
    }
}

impl fmt::Display for LeaseGeneration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Monotonic fencing token for checkpoint epoch ownership.
///
/// Checkpoint writers must carry the active coordinator token so stale
/// coordinators cannot commit superseded epochs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct FencingToken(u64);

impl FencingToken {
    pub fn try_new(value: u64) -> ProtoResult<Self> {
        if value == 0 {
            return Err(IdError::zero("fencing token"));
        }
        Ok(Self(value))
    }
    pub fn initial() -> Self {
        Self(1)
    }
    pub fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }
    pub fn as_u64(self) -> u64 {
        self.0
    }
}

impl fmt::Display for FencingToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Version for coordinator/executor transport contracts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TransportVersion {
    major: u16,
    minor: u16,
}

impl TransportVersion {
    /// R3.1 transport contract version.
    pub const R3_1: Self = Self { major: 3, minor: 1 };

    /// Current transport contract version.
    pub const CURRENT: Self = Self::R3_1;

    /// Create a transport version.
    pub const fn new(major: u16, minor: u16) -> Self {
        Self { major, minor }
    }

    /// Major version.
    pub fn major(self) -> u16 {
        self.major
    }

    /// Minor version.
    pub fn minor(self) -> u16 {
        self.minor
    }

    /// Whether this version can satisfy a peer requiring `required`.
    pub fn is_compatible_with(self, required: Self) -> bool {
        self.major == required.major && self.minor >= required.minor
    }
}

impl fmt::Display for TransportVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.major, self.minor)
    }
}

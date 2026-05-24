#!/usr/bin/env python3
"""Split krishiv-scheduler/src/lib.rs into coordinator + grpc modules."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "crates" / "krishiv-scheduler" / "src"
LIB = SRC / "lib.rs"

# 1-based inclusive ranges (from monolithic lib.rs layout)
RANGES = {
    "coordinator_types.rs": (146, 407),
    "coordinator.rs": (412, 465),
    "shared_coordinator.rs": (467, 573),
    "grpc_service.rs": (575, 1352),
    "coordinator_impl.rs": (1354, 2705),
    "scheduler_tests.rs": (2708, None),
}


def main() -> None:
    lines = LIB.read_text().splitlines(keepends=True)
    header_end = 121  # through store re-exports

    preamble = "".join(lines[:header_end])

    for name, (start, end) in RANGES.items():
        chunk = lines[start - 1 : end]
        body = "".join(chunk)
        if name == "scheduler_tests.rs":
            body = body.replace("use super::", "use crate::").replace(
                "mod tests {", "mod scheduler_tests {", 1
            )
        out = SRC / name
        out.write_text(f"//! Extracted from lib.rs ({name}).\n\n" + body)
        print(f"wrote {name}")

    new_lib = preamble + """
mod coordinator_types;
mod coordinator;
mod shared_coordinator;
mod grpc_service;
mod coordinator_impl;

pub use coordinator_types::{
    AdaptiveDecisionKind, AdaptiveDecisionLog, AdaptiveOverrideConfig, CoordinatorConfig,
    ExecutorHeartbeatEffects, JobSubmitter, SchedulerError, TaskUpdateOutcome, ThrottleDecision,
};
pub use coordinator::Coordinator;
pub use shared_coordinator::SharedCoordinator;
pub use coordinator_impl::{LeaderElection, SingleNodeElection, TlsConfig};
pub use grpc_service::{
    CoordinatorExecutorGrpcService, CoordinatorExecutorTonicService, CoordinatorManagementGrpcService,
    extract_auth_context, validate_grpc_auth,
};

pub type SchedulerResult<T> = Result<T, SchedulerError>;

#[cfg(test)]
mod scheduler_tests;
"""
    LIB.write_text(new_lib)
    print("wrote lib.rs")


if __name__ == "__main__":
    main()

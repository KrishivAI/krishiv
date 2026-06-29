# krishiv-chaos

Cross-crate chaos and fault-injection integration tests.

## Overview

`krishiv-chaos` validates system resilience by injecting faults at crate
boundaries:

- Network partition simulation
- Executor crash/recovery
- State backend corruption
- Scheduler failover under load

This crate is for testing only and is excluded from production builds.

## Usage

```bash
cargo test -p krishiv-chaos
```

## License

Apache-2.0

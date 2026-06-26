# krishiv-common

Shared types, error definitions, and utility functions used across the Krishiv
workspace.

## Overview

`krishiv-common` provides the foundational types that all other crates depend on:
typed IDs, error enums, retry logic, and common constants. It has minimal
dependencies to keep the dependency graph light.

## Usage

```rust
use krishiv_common::retry::{retry_fn, RetryConfig};
```

## License

Apache-2.0

# krishiv-ivm

Incremental View Maintenance (IVM) engine for keeping derived tables up to date.

## Overview

`krishiv-ivm` implements incremental computation of views:

- Tracks changes to base tables
- Propagates deltas through view definitions
- Maintains materialized views with minimal recomputation
- Integrates with DataFusion for query planning

## Usage

```rust
use krishiv_ivm::IncrementalView;
```

## License

Apache-2.0

# krishiv-delta

Delta Lake protocol implementation for Krishiv.

## Overview

`krishiv-delta` provides Delta Lake table support:

- Log protocol parsing and replay
- Transaction log ACID semantics
- Schema evolution and enforcement
- Time travel via versioned snapshots
- Integration with Arrow/Parquet I/O

## Usage

```rust
use krishiv_delta::DeltaTable;
```

## License

Apache-2.0

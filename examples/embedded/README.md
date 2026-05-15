# Embedded Example Placeholder

This directory will hold R1 embedded-mode examples.

Expected first example:

```rust
use krishiv_api::{ExecutionMode, Session};

let session = Session::builder()
    .with_execution_mode(ExecutionMode::Embedded)
    .build()?;
```

The current bootstrap slice only validates the API shape.

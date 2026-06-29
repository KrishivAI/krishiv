# krishiv-plan

Query planning and logical/physical plan representation for Krishiv.

## Overview

`krishiv-plan` defines:

- Logical plan nodes (scan, filter, project, aggregate, join, sink)
- Physical plan operators with cost-based optimization
- SQL-to-plan translation via `sqlparser`
- Secret management for connector credentials

## Usage

```rust
use krishiv_plan::{LogicalPlan, PhysicalPlan};
```

## License

Apache-2.0

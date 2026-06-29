# krishiv-runtime

Unified runtime that wires together planning, scheduling, and execution.

## Overview

`krishiv-runtime` is the high-level orchestration layer:

- Connects SQL planning to scheduler and executor
- Manages session state and transaction lifecycle
- Handles Flight SQL protocol for client connections
- Coordinates embedded and distributed execution paths

## Features

| Feature | Description |
|---------|-------------|
| `kafka` | Kafka connector support |
| `vector-sinks` | Vector database sink support |

## License

Apache-2.0

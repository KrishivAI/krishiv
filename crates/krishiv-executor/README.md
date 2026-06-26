# krishiv-executor

Task execution engine for running dataflow stages on worker nodes.

## Overview

`krishiv-executor` executes assigned tasks:

- Reads partitioned input from shuffle or connectors
- Runs dataflow operators (filter, project, aggregate, join)
- Writes output to sinks or shuffle
- Reports metrics and heartbeats to the scheduler

## Binary

`krishiv-executor`

## Features

| Feature | Description |
|---------|-------------|
| `kafka` | Kafka sink support |

## Usage

```bash
krishiv-executor --coordinator http://localhost:50051 --slots 4
```

## License

Apache-2.0

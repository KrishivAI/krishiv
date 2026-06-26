# krishiv-scheduler

Job scheduling and coordination for single-node and distributed Krishiv.

## Overview

`krishiv-scheduler` manages job lifecycle, stage planning, and resource
allocation:

- Single-node scheduler with embedded state
- Distributed scheduler with etcd-backed metadata
- Stage DAG planning and task assignment
- Checkpoint coordination for exactly-once recovery
- JWT-based authentication

## Binaries

| Binary | Description |
|--------|-------------|
| `krishiv-coordinator` | Single-node coordinator |
| `krishiv-clusterd` | Distributed coordinator (requires `etcd` feature) |
| `krishiv-job-coordinator` | Per-job coordinator |

## Features

| Feature | Description |
|---------|-------------|
| `etcd` | etcd metadata backend for distributed mode |

## Usage

```bash
# Single-node
krishiv-coordinator --grpc-addr 0.0.0.0:50051 --insecure

# Distributed
krishiv-clusterd --etcd-endpoints http://localhost:2379 --insecure
```

## License

Apache-2.0

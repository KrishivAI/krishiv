# krishiv-operator

Kubernetes operator for managing Krishiv clusters and jobs via CRDs.

## Overview

`krishiv-operator` provides:

- Custom Resource Definitions for KrishivJob, KrishivCluster
- Controller loop for reconciling desired vs actual state
- Automatic failover and stale coordinator detection
- Optional embedded web UI

## Features

| Feature | Description |
|---------|-------------|
| `k8s` | Kubernetes API integration |
| `ui` | Embedded web dashboard |
| `cluster` | Full cluster mode (k8s + ui) |

## Usage

```bash
# Apply CRDs
kubectl apply -k k8s/operator

# Run the operator
krishiv-operator --in-cluster
```

## License

Apache-2.0

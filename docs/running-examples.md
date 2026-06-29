# Running Krishiv Examples

This guide shows how to run the example programs in all three execution modes:
**embedded**, **single-node cluster**, and **distributed (k3s)**. The same
examples work in all three modes; only the session configuration changes.

---

## Examples at a Glance

### Rust (`examples/rust/src/bin/`)

| Example | What it demonstrates |
|---------|---------------------|
| `batch_iot_sensor` | Parquet ingest → SQL aggregation (avg/max/count) |
| `batch_ecommerce` | Multi-table JOIN + GROUP BY revenue segmentation |
| `batch_log_analytics` | Error-rate calculation per service |
| `batch_sql` | General batch SQL on a local Parquet file |
| `batch_delta_audit` | Delta Lake time-travel (Version 0 vs. latest) |
| `batch_hudi_ingest` | Hudi CoW table write + snapshot read |
| `memory_stream` | In-memory bounded stream collect |
| `stream_transaction_count` | Tumbling event-time window (1-minute) |
| `stream_multi_source` | Sliding window with per-source watermark |
| `stream_session_window` | Inactivity-gap session window |
| `stream_continuous_job` | Continuous unbounded job (submit / push / poll) |
| `stream_state_ttl` | Stateful windowing with TTL eviction |

### Python (`crates/krishiv-python/examples/`)

Same set of 12 examples, named identically with a `.py` extension.

---

## Prerequisites

### Build the binary

```bash
# Standard debug build (fast iteration)
cargo build -p krishiv

# The binary is at target/debug/krishiv
# Add it to PATH for the local-cluster commands:
export PATH="$PWD/target/debug:$PATH"
```

### Install the Python package (for Python examples)

```bash
# Requires maturin
.venv/bin/maturin develop -m crates/krishiv-python/Cargo.toml
```

---

## Mode 1 — Embedded

Everything runs in-process. No daemons, no network, no cluster setup required.
Best for development, testing, and single-machine use.

### How examples select embedded mode

Each example checks for `KRISHIV_COORDINATOR_URL`. If unset it falls back to
embedded:

```rust
// Rust
let mut builder = Session::builder();
if let Ok(url) = std::env::var("KRISHIV_COORDINATOR_URL") {
    builder = builder.with_local_cluster(url);
} else {
    builder = builder.with_execution_mode(ExecutionMode::Embedded);
}
```

```python
# Python — Session.from_env() does the same automatically
session = ks.Session.from_env()   # embedded when KRISHIV_COORDINATOR_URL is unset
```

### Run Rust examples (embedded)

```bash
# Unset the coordinator URL to force embedded mode
unset KRISHIV_COORDINATOR_URL

cargo run -p krishiv-rust-examples --bin batch_iot_sensor
cargo run -p krishiv-rust-examples --bin batch_ecommerce
cargo run -p krishiv-rust-examples --bin batch_log_analytics
cargo run -p krishiv-rust-examples --bin batch_sql
cargo run -p krishiv-rust-examples --bin batch_delta_audit
cargo run -p krishiv-rust-examples --bin batch_hudi_ingest
cargo run -p krishiv-rust-examples --bin memory_stream
cargo run -p krishiv-rust-examples --bin stream_transaction_count
cargo run -p krishiv-rust-examples --bin stream_multi_source
cargo run -p krishiv-rust-examples --bin stream_session_window
cargo run -p krishiv-rust-examples --bin stream_continuous_job
cargo run -p krishiv-rust-examples --bin stream_state_ttl
```

### Run Python examples (embedded)

```bash
unset KRISHIV_COORDINATOR_URL

python3 crates/krishiv-python/examples/batch_iot_sensor.py
python3 crates/krishiv-python/examples/batch_ecommerce.py
python3 crates/krishiv-python/examples/batch_log_analytics.py
python3 crates/krishiv-python/examples/batch_sql.py
python3 crates/krishiv-python/examples/batch_delta_audit.py
python3 crates/krishiv-python/examples/batch_hudi_ingest.py
python3 crates/krishiv-python/examples/memory_stream.py
python3 crates/krishiv-python/examples/stream_transaction_count.py
python3 crates/krishiv-python/examples/stream_multi_source.py
python3 crates/krishiv-python/examples/stream_session_window.py
python3 crates/krishiv-python/examples/stream_continuous_job.py
python3 crates/krishiv-python/examples/stream_state_ttl.py
```

---

## Mode 2 — Single-Node Cluster

A local coordinator + executor + flight-server run as background daemons on
the same machine. Batch SQL is dispatched to the executor via the coordinator
HTTP API. Streaming windows run on the flight server's embedded cluster.

Best for validating the real coordinator/executor pipeline without Kubernetes.

### Start the cluster

```bash
# Start all three daemons (coordinator, executor, flight-server)
cargo run -p krishiv -- local start --data-dir /tmp/krishiv-cluster

# Output (ports are selected automatically):
#   gRPC:    http://127.0.0.1:9090
#   HTTP:    http://127.0.0.1:18080
#   Flight:  http://127.0.0.1:50051
#   UI:      http://127.0.0.1:18080/ui

# Check status
cargo run -p krishiv -- local status --data-dir /tmp/krishiv-cluster

# Verify executors registered and healthy
curl http://127.0.0.1:18080/api/v1/executors
```

### Run Rust examples (single-node)

```bash
export KRISHIV_COORDINATOR_URL=http://127.0.0.1:50051

cargo run -p krishiv-rust-examples --bin batch_iot_sensor
cargo run -p krishiv-rust-examples --bin batch_ecommerce
cargo run -p krishiv-rust-examples --bin batch_log_analytics
cargo run -p krishiv-rust-examples --bin batch_sql
cargo run -p krishiv-rust-examples --bin batch_delta_audit
cargo run -p krishiv-rust-examples --bin batch_hudi_ingest
cargo run -p krishiv-rust-examples --bin memory_stream
cargo run -p krishiv-rust-examples --bin stream_transaction_count
cargo run -p krishiv-rust-examples --bin stream_multi_source
cargo run -p krishiv-rust-examples --bin stream_session_window
cargo run -p krishiv-rust-examples --bin stream_continuous_job
cargo run -p krishiv-rust-examples --bin stream_state_ttl
```

### Run Python examples (single-node)

```bash
export KRISHIV_COORDINATOR_URL=http://127.0.0.1:50051

python3 crates/krishiv-python/examples/batch_iot_sensor.py
python3 crates/krishiv-python/examples/batch_ecommerce.py
python3 crates/krishiv-python/examples/batch_log_analytics.py
python3 crates/krishiv-python/examples/batch_sql.py
python3 crates/krishiv-python/examples/batch_delta_audit.py
python3 crates/krishiv-python/examples/batch_hudi_ingest.py
python3 crates/krishiv-python/examples/memory_stream.py
python3 crates/krishiv-python/examples/stream_transaction_count.py
python3 crates/krishiv-python/examples/stream_multi_source.py
python3 crates/krishiv-python/examples/stream_session_window.py
python3 crates/krishiv-python/examples/stream_continuous_job.py
python3 crates/krishiv-python/examples/stream_state_ttl.py
```

### Monitor jobs

```bash
# Watch job list
curl http://127.0.0.1:18080/api/v1/jobs | python3 -m json.tool

# Web UI (coordinator dashboard)
open http://127.0.0.1:18080/ui
```

### Stop the cluster

```bash
cargo run -p krishiv -- local stop --data-dir /tmp/krishiv-cluster
```

---

## Mode 3 — Distributed (k3s Kubernetes)

A full coordinator + 2 executor pods + flight-server pod run on k3s.  Batch
SQL data travels as inline Arrow IPC (no shared filesystem required).  Windows
are dispatched to executor pods via the coordinator.

Best for validating multi-process execution and Kubernetes deployment.

### Prerequisites

- **k3s** installed and running (`kubectl get nodes` shows `Ready`)
- **buildah** for building the container image
- **musl toolchain** for the static binary (`apt-get install musl-tools` +
  `rustup target add x86_64-unknown-linux-musl`)

### Build the container image

```bash
# 1. Build a statically-linked release binary (no glibc dependency)
cargo build -p krishiv --profile release --target x86_64-unknown-linux-musl

# 2. Symbols are already stripped by the release profile; this is a no-op
#    (kept for parity with older toolchains that didn't strip automatically).

# 3. Stage the binary and write the Dockerfile
mkdir -p /tmp/krishiv-image
cp target/x86_64-unknown-linux-musl/release/krishiv /tmp/krishiv-image/
cat > /tmp/krishiv-image/Dockerfile <<'EOF'
FROM alpine:3.21
RUN apk add --no-cache ca-certificates
COPY krishiv /usr/local/bin/krishiv
RUN chmod +x /usr/local/bin/krishiv
ENTRYPOINT ["/usr/local/bin/krishiv"]
EOF

# 4. Build and import into k3s containerd
buildah bud --isolation chroot -t krishiv:local /tmp/krishiv-image/
buildah push krishiv:local docker-archive:/tmp/krishiv.tar
k3s ctr images import /tmp/krishiv.tar
```

### Deploy to k3s

```bash
# Apply the full manifest (namespace, RBAC, deployments, services, NetworkPolicy)
kubectl apply -f deploy/k8s/direct/krishiv-distributed.yaml

# Wait for all pods to be ready
kubectl rollout status deployment/coordinator deployment/executor deployment/flight-server \
    -n krishiv-system --timeout=120s

# Verify executors registered with real pod IPs
curl http://127.0.0.1:30080/api/v1/executors | python3 -m json.tool
```

The manifest exposes two NodePort services:

| Service | NodePort | Purpose |
|---------|----------|---------|
| `flight-server` | `:30051` | Arrow Flight endpoint for examples |
| `coordinator-ext` | `:30080` | Coordinator HTTP (diagnostics / jobs API) |

### Run Rust examples (distributed)

```bash
export KRISHIV_COORDINATOR_URL=http://127.0.0.1:30051

cargo run -p krishiv-rust-examples --bin batch_iot_sensor
cargo run -p krishiv-rust-examples --bin batch_ecommerce
cargo run -p krishiv-rust-examples --bin batch_log_analytics
cargo run -p krishiv-rust-examples --bin batch_sql
cargo run -p krishiv-rust-examples --bin batch_delta_audit
cargo run -p krishiv-rust-examples --bin batch_hudi_ingest
cargo run -p krishiv-rust-examples --bin memory_stream
cargo run -p krishiv-rust-examples --bin stream_transaction_count
cargo run -p krishiv-rust-examples --bin stream_multi_source
cargo run -p krishiv-rust-examples --bin stream_session_window
cargo run -p krishiv-rust-examples --bin stream_continuous_job
cargo run -p krishiv-rust-examples --bin stream_state_ttl
```

### Run Python examples (distributed)

```bash
export KRISHIV_COORDINATOR_URL=http://127.0.0.1:30051

python3 crates/krishiv-python/examples/batch_iot_sensor.py
python3 crates/krishiv-python/examples/batch_ecommerce.py
python3 crates/krishiv-python/examples/batch_log_analytics.py
python3 crates/krishiv-python/examples/batch_sql.py
python3 crates/krishiv-python/examples/batch_delta_audit.py
python3 crates/krishiv-python/examples/batch_hudi_ingest.py
python3 crates/krishiv-python/examples/memory_stream.py
python3 crates/krishiv-python/examples/stream_transaction_count.py
python3 crates/krishiv-python/examples/stream_multi_source.py
python3 crates/krishiv-python/examples/stream_session_window.py
python3 crates/krishiv-python/examples/stream_continuous_job.py
python3 crates/krishiv-python/examples/stream_state_ttl.py
```

### Monitor jobs

```bash
# Coordinator health
curl http://127.0.0.1:30080/readyz

# Job list (batch-sql-* and bounded-window-* entries)
curl http://127.0.0.1:30080/api/v1/jobs | python3 -m json.tool

# Executor state (host, slots, consecutive_task_failures)
curl http://127.0.0.1:30080/api/v1/executors | python3 -m json.tool

# Pod logs
kubectl logs -n krishiv-system -l app=krishiv-coordinator --tail=50
kubectl logs -n krishiv-system -l app=krishiv-executor --tail=50
kubectl logs -n krishiv-system -l app=krishiv-flight --tail=50
```

### Redeploy after code changes

```bash
# Rebuild binary and image
cargo build -p krishiv --profile release --target x86_64-unknown-linux-musl
cp target/x86_64-unknown-linux-musl/release/krishiv /tmp/krishiv-image/
buildah bud --isolation chroot -t krishiv:local /tmp/krishiv-image/
rm -f /tmp/krishiv.tar && buildah push krishiv:local docker-archive:/tmp/krishiv.tar
k3s ctr images import /tmp/krishiv.tar

# Rolling restart (zero downtime for flight-server/executor, brief for coordinator)
kubectl rollout restart deployment/coordinator deployment/executor deployment/flight-server \
    -n krishiv-system
kubectl rollout status deployment/coordinator deployment/executor deployment/flight-server \
    -n krishiv-system --timeout=120s
```

### Tear down

```bash
kubectl delete namespace krishiv-system
```

---

## Mode Comparison

| | Embedded | Single-node | Distributed (k3s) |
|---|---|---|---|
| Setup | None | `local start` | `kubectl apply` |
| Coordinator | In-process | Daemon on host | Pod in cluster |
| Executor | In-process | Daemon on host | 2 pods, 4 slots |
| Flight server | In-process | Daemon on host | Pod in cluster |
| Parquet data | Local filesystem | Local filesystem | Inline IPC (in-band) |
| Batch SQL routing | DataFusion direct | Coordinator → executor | Coordinator → executor |
| Streaming windows | Embedded cluster | Embedded (flight server) | Executor pods |
| Durability | None | JSON file metadata | JSON file metadata |
| Best for | Dev / unit tests | Integration / CI | Production validation |

---

## Troubleshooting

### Executors not registering

Executors register using their advertised IP (`POD_IP` in k8s, `HOSTNAME` for
local). Check:

```bash
# Single-node: verify executor is running
cargo run -p krishiv -- local status --data-dir /tmp/krishiv-cluster

# k3s: verify executor pods are healthy and advertising real pod IPs
curl http://127.0.0.1:30080/api/v1/executors
```

### Batch SQL returns HTTP 500

The job failed on the executor. Check the executor logs for the DataFusion
error (usually a missing table or type mismatch):

```bash
# Single-node
cat /tmp/krishiv-cluster/*.log 2>/dev/null

# k3s
kubectl logs -n krishiv-system -l app=krishiv-executor --tail=30
```

Also check the coordinator job list for the failure reason:

```bash
curl http://127.0.0.1:30080/api/v1/jobs | python3 -m json.tool | grep -A3 '"state": "Failed"'
```

### Circuit breaker tripped (executor shows failures)

If `consecutive_task_failures > 0` on an executor, the coordinator may skip it.
Restart the cluster to reset:

```bash
# Single-node
cargo run -p krishiv -- local stop  --data-dir /tmp/krishiv-cluster
cargo run -p krishiv -- local start --data-dir /tmp/krishiv-cluster

# k3s
kubectl rollout restart deployment/coordinator -n krishiv-system
```

### Container image not found

k3s uses its own containerd registry. The image must be imported with
`k3s ctr images import`, not `docker pull`:

```bash
k3s ctr images ls | grep krishiv
# If missing, re-run the import step in "Build the container image" above.
```

### Python examples fail with import error

Rebuild the Python module after any Rust changes:

```bash
.venv/bin/maturin develop -m crates/krishiv-python/Cargo.toml
```

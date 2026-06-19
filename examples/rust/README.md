# Krishiv Rust Batch Examples

Three real-life batch-mode examples:

- `embedded_retail_sales`: embedded process analytics over retail orders.
- `single_node_inventory_replenishment`: single-node batch planning for stock replenishment.
- `distributed_kubernetes_claims`: distributed Kubernetes batch analytics over healthcare claims data.

Run from the repository root:

```bash
cargo run --manifest-path examples/rust/Cargo.toml --bin embedded_retail_sales
cargo run --manifest-path examples/rust/Cargo.toml --bin single_node_inventory_replenishment
```

For the Kubernetes example, deploy/port-forward a Krishiv Flight endpoint and provide a Parquet
path visible to the coordinator/executor pods, for example a PVC mount or shared object-store mount:

```bash
kubectl -n krishiv-system port-forward svc/krishiv-flight-sql 50051:50051
export KRISHIV_K8S_FLIGHT_URL=http://127.0.0.1:50051
export CLAIMS_PARQUET=/data/claims/claims.parquet
cargo run --manifest-path examples/rust/Cargo.toml --bin distributed_kubernetes_claims
```


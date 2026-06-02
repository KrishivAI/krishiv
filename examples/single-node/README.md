# Single-Node Real-Life Examples

This directory contains real-life use-case examples designed to run against a single-node Krishiv cluster. 
A single-node cluster runs the coordinator, executor, and flight server locally on your machine, enabling you to test full end-to-end multi-process execution without needing a full Kubernetes setup.

## Prerequisites

Before running these examples, ensure you have:
1. Compiled the Krishiv binary (`cargo build -p krishiv`)
2. Compiled the Krishiv Python package (`.venv/bin/maturin develop -m crates/krishiv-python/Cargo.toml`)

## Deploying the Single-Node Cluster

To deploy the cluster on your local machine, run:

```bash
cargo run -p krishiv -- local start --data-dir /tmp/krishiv-single-node
```

You can verify the cluster is healthy by checking the jobs API or executors API:
```bash
curl http://127.0.0.1:18080/api/v1/executors
```

## Running the Examples

Set the environment variable `KRISHIV_COORDINATOR_URL` to point to the local cluster's Flight endpoint (`http://127.0.0.1:50051`). Then run the Python scripts.

```bash
export KRISHIV_COORDINATOR_URL=http://127.0.0.1:50051

# 1. Run the Batch Example (Log Analytics)
python3 examples/single-node/batch_example.py

# 2. Run the Streaming Example (Sensor Data)
python3 examples/single-node/streaming_example.py
```

## Stopping the Cluster

Once you're done, gracefully shut down the local daemon:
```bash
cargo run -p krishiv -- local stop --data-dir /tmp/krishiv-single-node
```

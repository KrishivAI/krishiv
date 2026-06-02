# Bare Metal Deployment

## Setup
Start the daemon processes manually across your servers.

**On Node 1 (Coordinator & Flight Server):**
```bash
krishiv coordinator --grpc-addr 0.0.0.0:9090 --metadata-backend json --metadata-path /tmp/meta.json --insecure
KRISHIV_FLIGHT_ADDR=0.0.0.0:50051 KRISHIV_COORDINATOR_HTTP=http://127.0.0.1:18080 krishiv flight-server
```

**On Node 2 (Executor):**
```bash
krishiv executor --executor-id node2-exec --coordinator http://<NODE_1_IP>:9090 --durability-profile distributed-durable --connect
```

## Running Examples
Point your scripts to Node 1's Flight server.
```bash
export KRISHIV_COORDINATOR_URL=http://<NODE_1_IP>:50051
python3 python_batch.py
```

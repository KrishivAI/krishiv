#!/usr/bin/env bash
# Stand up the Phase 58 HA chaos rig on a local kind cluster.
#
# `deploy/k8s/phase58/ha-cert.yaml` is the CI-rig topology (nodes s1–s3,
# pre-seeded hostPath data, long-lived secrets). This script adapts it to a
# kind cluster so `scripts/phase58_chaos.sh` can run anywhere:
#
#   - stages the host-built binary into the `localhost/krishiv:phase58-ha`
#     image (Dockerfile.fast) and loads it into every kind node
#   - repoints the executor/driver `nodeName` pins at real kind node names
#   - mints fresh phase58-tokens + minio-s3-creds secrets (never reused)
#   - deploys a single-node MinIO and creates the DUR-2 checkpoint bucket
#   - seeds events.parquet / events/part-*.parquet / changes.csv onto every
#     node's /var/lib/krishiv-phase58 (generated deterministically when no
#     PHASE58_DATA_DIR is supplied)
#
# Requirements: kind, kubectl, docker, python3 + pyarrow, and a binary at
# dist/docker/krishiv built with `--features prod` (etcd + cloud + kafka).
set -euo pipefail

CLUSTER="${PHASE58_KIND_CLUSTER:-krishiv}"
NS=krishiv-phase58
INFRA_NS=krishiv-infra
REPO="$(cd "$(dirname "$0")/.." && pwd)"

log() { printf '[phase58-up] %s\n' "$*"; }
fail() { log "FAIL: $*"; exit 1; }
rand_token() { head -c 24 /dev/urandom | base64 | tr -d '/+=' ; }

mapfile -t NODES < <(kind get nodes --name "$CLUSTER" 2>/dev/null)
[ "${#NODES[@]}" -ge 3 ] || fail "kind cluster '$CLUSTER' needs >=3 nodes (etcd/coordinator anti-affinity); found ${#NODES[@]}"
WORKERS=()
for node in "${NODES[@]}"; do
  case "$node" in *control-plane*) ;; *) WORKERS+=("$node") ;; esac
done
[ "${#WORKERS[@]}" -ge 2 ] || fail "need two non-control-plane nodes for the executor pins"
NODE_EXEC_A="${PHASE58_NODE_A:-${WORKERS[0]}}"
NODE_EXEC_B="${PHASE58_NODE_B:-${WORKERS[1]}}"
DRIVER_NODE="${PHASE58_DRIVER_NODE:-$NODE_EXEC_A}"

# ── Image ───────────────────────────────────────────────────────────────────
BIN="$REPO/dist/docker/krishiv"
[ -x "$BIN" ] || fail "stage a prod-feature binary at dist/docker/krishiv first (cp target/debug/krishiv dist/docker/)"
"$BIN" capabilities | grep -q 'etcd *ON' || fail "dist/docker/krishiv lacks etcd; rebuild with --features prod"
[ -e "$REPO/dist/docker/krishiv-operator" ] || cp "$BIN" "$REPO/dist/docker/krishiv-operator"
log "building localhost/krishiv:phase58-ha (RUNTIME_BASE=ubuntu:26.04 to match the host glibc)"
docker buildx build --load -f "$REPO/deploy/docker/Dockerfile.fast" \
  --build-arg RUNTIME_BASE=ubuntu:26.04 -t localhost/krishiv:phase58-ha "$REPO" >/dev/null
docker run --rm --entrypoint /usr/local/bin/krishiv localhost/krishiv:phase58-ha capabilities >/dev/null \
  || fail "image smoke-run failed (glibc mismatch?)"
kind load docker-image localhost/krishiv:phase58-ha --name "$CLUSTER" >/dev/null
log "image loaded into ${#NODES[@]} nodes"

# ── Data ────────────────────────────────────────────────────────────────────
DATA_DIR="${PHASE58_DATA_DIR:-}"
if [ -z "$DATA_DIR" ]; then
  DATA_DIR="$(mktemp -d /tmp/phase58-data.XXXXXX)"
  log "generating deterministic datasets in $DATA_DIR"
  python3 - "$DATA_DIR" <<'PY'
import random
import sys

import pyarrow as pa
import pyarrow.parquet as pq

out = sys.argv[1]
rng = random.Random(58)
n, users, base_ms = 100_000, 1_000, 1_722_470_400_000
events = {
    "user_id": [f"user-{rng.randrange(users):04d}" for _ in range(n)],
    "amount": [round(rng.uniform(1, 500), 2) for _ in range(n)],
    "event_time": [base_ms + i * 37 for i in range(n)],
}
table = pa.table(events)
pq.write_table(table, f"{out}/events.parquet")
import os
os.makedirs(f"{out}/events", exist_ok=True)
part = n // 8
for i in range(8):
    pq.write_table(table.slice(i * part, part), f"{out}/events/part-{i}.parquet")
with open(f"{out}/changes.csv", "w") as f:
    f.write("k,v\n")
    for i in range(5_000):
        f.write(f"k{i % 500},{i}\n")
# Streaming workload subset: 10k events (~37 tumbling windows). Small enough
# that the windowed drain fits one bounded do_action response — the chaos
# gate pins recovery correctness, not payload size (the oversized-drain
# semantics are unit/service-tested engine-side).
pq.write_table(table.slice(0, 10_000), f"{out}/events_stream.parquet")
# One far-future event: the chaos gate pushes this after events_stream to
# advance the watermark past the final window so every window closes and
# the poll digest covers the complete aggregation.
advance_ts = max(events["event_time"]) + 100_000
pq.write_table(
    pa.table({"user_id": ["user-9999"], "amount": [1.0], "event_time": [advance_ts]}),
    f"{out}/advance.parquet",
)
PY
fi
[ -s "$DATA_DIR/events.parquet" ] || fail "$DATA_DIR/events.parquet missing"
[ -s "$DATA_DIR/changes.csv" ] || fail "$DATA_DIR/changes.csv missing"
[ -s "$DATA_DIR/advance.parquet" ] || fail "$DATA_DIR/advance.parquet missing"
[ -s "$DATA_DIR/events_stream.parquet" ] || fail "$DATA_DIR/events_stream.parquet missing"
ls "$DATA_DIR"/events/part-*.parquet >/dev/null 2>&1 || fail "$DATA_DIR/events/part-*.parquet missing"
for node in "${NODES[@]}"; do
  docker exec "$node" mkdir -p /var/lib/krishiv-phase58/events
  docker cp "$DATA_DIR/events.parquet" "$node":/var/lib/krishiv-phase58/events.parquet
  docker cp "$DATA_DIR/changes.csv" "$node":/var/lib/krishiv-phase58/changes.csv
  docker cp "$DATA_DIR/advance.parquet" "$node":/var/lib/krishiv-phase58/advance.parquet
  docker cp "$DATA_DIR/events_stream.parquet" "$node":/var/lib/krishiv-phase58/events_stream.parquet
  for part in "$DATA_DIR"/events/part-*.parquet; do
    docker cp "$part" "$node":/var/lib/krishiv-phase58/events/
  done
done
log "datasets seeded on every node"

# ── Namespaces + secrets ────────────────────────────────────────────────────
kubectl create namespace "$NS" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl create namespace "$INFRA_NS" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
MINIO_USER=phase58-minio
MINIO_PASS="$(rand_token)"
kubectl -n "$NS" create secret generic phase58-tokens \
  --from-literal=coordinator="$(rand_token)" \
  --from-literal=task="$(rand_token)" \
  --from-literal=shuffle="$(rand_token)" \
  --from-literal=apikeys="$(rand_token)=chaos-gate" \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl -n "$NS" create secret generic minio-s3-creds \
  --from-literal=AWS_ACCESS_KEY_ID="$MINIO_USER" \
  --from-literal=AWS_SECRET_ACCESS_KEY="$MINIO_PASS" \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null

# ── MinIO (DUR-2 checkpoint bucket) ─────────────────────────────────────────
cat <<EOF | kubectl apply -f - >/dev/null
apiVersion: apps/v1
kind: Deployment
metadata: { name: minio, namespace: $INFRA_NS }
spec:
  replicas: 1
  selector: { matchLabels: { app: minio } }
  template:
    metadata: { labels: { app: minio } }
    spec:
      containers:
        - name: minio
          image: quay.io/minio/minio:latest
          command: ["minio", "server", "/data"]
          env:
            - { name: MINIO_ROOT_USER, value: "$MINIO_USER" }
            - { name: MINIO_ROOT_PASSWORD, value: "$MINIO_PASS" }
          ports: [{ containerPort: 9000 }]
          volumeMounts: [{ name: data, mountPath: /data }]
      volumes: [{ name: data, emptyDir: {} }]
---
apiVersion: v1
kind: Service
metadata: { name: minio, namespace: $INFRA_NS }
spec:
  selector: { app: minio }
  ports: [{ port: 9000, targetPort: 9000 }]
EOF
kubectl -n "$INFRA_NS" rollout status deploy/minio --timeout=180s >/dev/null
kubectl -n "$INFRA_NS" delete pod minio-mb --ignore-not-found >/dev/null 2>&1
kubectl -n "$INFRA_NS" run minio-mb --rm -i --restart=Never --image=quay.io/minio/mc --command -- \
  sh -ec "mc alias set local http://minio.$INFRA_NS:9000 '$MINIO_USER' '$MINIO_PASS' && mc mb -p local/krishiv-cert-dur2" >/dev/null
log "MinIO up, bucket krishiv-cert-dur2 ready"

# ── Topology (nodeName pins repointed for kind) ─────────────────────────────
sed -e "s/nodeName: s2/nodeName: $NODE_EXEC_A/" \
    -e "s/nodeName: s3/nodeName: $NODE_EXEC_B/" \
    -e "s/nodeName: s1/nodeName: $DRIVER_NODE/" \
    "$REPO/deploy/k8s/phase58/ha-cert.yaml" | kubectl apply -f - >/dev/null
log "topology applied (executors: $NODE_EXEC_A, $NODE_EXEC_B; driver: $DRIVER_NODE)"

kubectl -n "$NS" rollout status statefulset/phase58-etcd --timeout=300s >/dev/null
# No coordinator rollout wait: /leaderz readiness intentionally keeps the two
# standbys Unready, so `rollout status` never converges. One ready endpoint
# (the elected leader) is the real readiness signal.
deadline=$((SECONDS + 300))
while :; do
  ready="$(kubectl -n "$NS" get endpointslice -l kubernetes.io/service-name=phase58-coordinator \
    -o jsonpath='{range .items[*].endpoints[?(@.conditions.ready==true)]}{.targetRef.name}{"\n"}{end}' 2>/dev/null | sed '/^$/d' | wc -l)"
  [ "$ready" -eq 1 ] && break
  [ "$SECONDS" -lt "$deadline" ] || fail "no single ready coordinator leader within 300s"
  sleep 3
done
kubectl -n "$NS" wait --for=condition=Ready pod phase58-driver --timeout=300s >/dev/null
log "rig is up — run: PHASE58_NAMESPACE=$NS bash scripts/phase58_chaos.sh"

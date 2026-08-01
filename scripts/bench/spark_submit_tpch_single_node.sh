#!/usr/bin/env bash
# Submit the TPC-H SF100 corpus to Spark confined to ONE node.
#
# The counterpart to `spark_submit_tpch.sh`. Everything is copied from that
# script unchanged except the two settings that define "single node":
#
#   spark.executor.instances                     3 -> 1
#   spark.kubernetes.node.selector.<hostname>    (new) -> $NODE
#
# The node selector applies to the driver AND the executor, so the whole job
# lands on one machine and reads from that machine's own MinIO — the same
# `internalTrafficPolicy: Local` endpoint the 3-node run uses, which is what
# keeps the storage path comparable instead of accidentally measuring the
# cross-node network (a cross-node MinIO read was measured at 7.6 MB/s).
#
# STILL NOT TUNED. Per-executor cores and memory are byte-identical to the
# 3-node submission, so the only variable between the two Spark runs is how
# many executors exist. Krishiv's single-node deployment is matched the same
# way: one executor, same 3800m/5000Mi limits, coordinator and executor pinned
# to the same node.
#
# Usage: scripts/bench/spark_submit_tpch_single_node.sh [<only-query-ids>]
set -euo pipefail

NS=krishiv-apitest
IMAGE=localhost/krishiv-spark:3.5.3
DATA=s3a://krishiv-bench/tpch/sf100
S3_ENDPOINT=http://minio-bench-local.krishiv-infra.svc.cluster.local:9000
NODE="${KRISHIV_BENCH_NODE:-s1}"
APISERVER="${KRISHIV_APISERVER:-}"
if [ -z "$APISERVER" ]; then
  command -v kubectl >/dev/null 2>&1 || {
    echo "error: set KRISHIV_APISERVER (no kubectl in this image)" >&2
    exit 2
  }
  APISERVER="$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}')"
fi
ONLY="${1:-}"

echo "# api server : $APISERVER"
echo "# namespace  : $NS"
echo "# node       : $NODE  (driver AND executor pinned here)"
echo "# data       : $DATA via $S3_ENDPOINT"

exec /opt/spark/bin/spark-submit \
  --master "k8s://${APISERVER}" \
  --deploy-mode cluster \
  --name krishiv-spark-tpch-1node \
  --conf spark.kubernetes.namespace="$NS" \
  --conf spark.kubernetes.container.image="$IMAGE" \
  --conf spark.kubernetes.container.image.pullPolicy=Never \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-bench \
  --conf spark.kubernetes.authenticate.submission.caCertFile="${KRISHIV_KUBE_PEMS:-/kube}/ca.pem" \
  --conf spark.kubernetes.authenticate.submission.oauthTokenFile="${KRISHIV_KUBE_PEMS:-/kube}/token" \
  \
  `# ── The single-node confinement, and the only deliberate difference ──` \
  `# The key is passed unescaped. A quoted "kubernetes\.io/hostname" reaches` \
  `# the API server with a literal backslash and the driver pod is rejected:` \
  `#   spec.nodeSelector: Invalid value: "kubernetes\\.io/hostname"` \
  `# Spark splits --conf on the FIRST '=', so dots in the key need no escaping.` \
  --conf spark.executor.instances=1 \
  --conf spark.kubernetes.node.selector.kubernetes.io/hostname=$NODE \
  \
  `# ── Memory: sized to the NODE, not copied from the 3-node run ────────` \
  `#` \
  `# The 3-node submission gives each executor 5000Mi and the driver 2048Mi.` \
  `# That is fine when the driver lives on a different machine from most of` \
  `# the executors. Co-locating them does not fit:` \
  `#` \
  `#   driver 2048Mi + executor 5000Mi + system/k3s/MinIO ~1400Mi = 8448Mi` \
  `#   node total                                                  7936Mi` \
  `#` \
  `# Measured, not predicted: the first attempt drove s1 to 108Mi free and` \
  `# load 14, kubelet went to "activating", and the API server was starved` \
  `# for two hours. Spark reached 12 of 22 queries while swapping, which is a` \
  `# measurement of an overcommitted node rather than of Spark.` \
  `#` \
  `# 4500 + 1500 = 6000Mi leaves ~1.9Gi for the system. This is NOT tuning:` \
  `# it is the budget the hardware has, and the Krishiv single-node run is` \
  `# given the SAME 6000Mi split the same way so the two remain comparable.` \
  --conf spark.executor.cores=3 \
  --conf spark.executor.memory=3500m \
  --conf spark.executor.memoryOverhead=1000m \
  --conf spark.kubernetes.executor.request.cores=250m \
  --conf spark.kubernetes.executor.limit.cores=3800m \
  \
  --conf spark.driver.cores=1 \
  --conf spark.driver.memory=1100m \
  --conf spark.driver.memoryOverhead=400m \
  --conf spark.kubernetes.driver.request.cores=250m \
  --conf spark.kubernetes.driver.limit.cores=2 \
  \
  --conf spark.hadoop.fs.s3a.endpoint="$S3_ENDPOINT" \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.EnvironmentVariableCredentialsProvider \
  --conf spark.kubernetes.driver.secretKeyRef.AWS_ACCESS_KEY_ID=minio-s3-creds:AWS_ACCESS_KEY_ID \
  --conf spark.kubernetes.driver.secretKeyRef.AWS_SECRET_ACCESS_KEY=minio-s3-creds:AWS_SECRET_ACCESS_KEY \
  --conf spark.kubernetes.executor.secretKeyRef.AWS_ACCESS_KEY_ID=minio-s3-creds:AWS_ACCESS_KEY_ID \
  --conf spark.kubernetes.executor.secretKeyRef.AWS_SECRET_ACCESS_KEY=minio-s3-creds:AWS_SECRET_ACCESS_KEY \
  \
  --conf spark.kubernetes.driver.podTemplateFile=/opt/job/spark-driver-pod-template.yaml \
  \
  local:///opt/job/tpch_spark_run.py \
    --data "$DATA" \
    --corpus-json /opt/job/corpus.json \
    --out /tmp/spark-tpch-1node.json \
    --label spark-3.5.3-sf100-1node \
    ${ONLY:+--only "$ONLY"}

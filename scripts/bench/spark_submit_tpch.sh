#!/usr/bin/env bash
# Submit the TPC-H SF100 corpus to Spark on the same three nodes Krishiv runs on.
#
# The point of this script is that every resource number below is copied from
# the live Krishiv bench deployment rather than chosen. Krishiv's executors are
# *limit*-bound (requests 250m/512Mi, limits 3800m/5000Mi), so Spark is given
# the same requests and the same limits — see the conf block for the one place
# the two cannot be made identical (integral task slots) and why.
#
# NOT TUNED. No broadcast-threshold changes, no AQE knobs beyond the 3.5
# defaults, no statistics collection, no query rewrites. Krishiv's numbers came
# from submitting the corpus to a default engine; this submits the same SQL to a
# default Spark. Tuning one side is how a baseline becomes marketing.
#
# Usage: scripts/bench/spark_submit_tpch.sh [<only-query-ids>]
set -euo pipefail

NS=krishiv-apitest
IMAGE=localhost/krishiv-spark:3.5.3
DATA=s3a://krishiv-bench/tpch/sf100
S3_ENDPOINT=http://minio-bench-local.krishiv-infra.svc.cluster.local:9000
# The API server URL, from the caller if supplied.
#
# This script runs *inside* the Spark image, which has no kubectl — reading it
# with `kubectl config view` here failed the whole submission with
# "kubectl: command not found" after the harness had already scaled Krishiv's
# executors to zero. spark-submit talks to the API server directly and never
# needed kubectl; only this one lookup did. The caller (which runs on the host,
# where kubectl exists) passes it in, and the kubectl path stays as a fallback
# for interactive use from a machine that has one.
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
echo "# data       : $DATA via $S3_ENDPOINT"

exec /opt/spark/bin/spark-submit \
  --master "k8s://${APISERVER}" \
  --deploy-mode cluster \
  --name krishiv-spark-tpch \
  --conf spark.kubernetes.namespace="$NS" \
  --conf spark.kubernetes.container.image="$IMAGE" \
  --conf spark.kubernetes.container.image.pullPolicy=Never \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-bench \
  \
  `# ── Submission credentials: CA + service-account token ─────────────` \
  `# Not kubeconfig auto-discovery, and not client certs.` \
  `#` \
  `# Auto-discovery fails because the Spark image runs as uid 185 and cannot` \
  `# read a root-owned ~/.kube mount; fabric8 then silently falls back to the` \
  `# JVM trust store and rejects the k3s CA with "PKIX path building failed" —` \
  `# an error naming TLS, not the permission problem behind it.` \
  `#` \
  `# Client certs fail because k3s issues an EC client key and fabric8 hands` \
  `# every key to its PKCS#1 (RSA) decoder, dying on "Invalid DER: object is` \
  `# not integer". Converting the key to PKCS#8 and setting clientKeyAlgo=EC` \
  `# both failed to change that.` \
  `#` \
  `# A bearer token has no key to parse. The runner mints a short-lived one for` \
  `# the spark-bench service account, which is the identity the driver already` \
  `# runs as — so submission and driver authenticate as the same principal.` \
  --conf spark.kubernetes.authenticate.submission.caCertFile="${KRISHIV_KUBE_PEMS:-/kube}/ca.pem" \
  --conf spark.kubernetes.authenticate.submission.oauthTokenFile="${KRISHIV_KUBE_PEMS:-/kube}/token" \
  \
  `# ── Executors: matched to bench-exec-s{1,2,3} ────────────────────────` \
  `# Krishiv: limits cpu 3800m / mem 5000Mi, requests cpu 250m / mem 512Mi,` \
  `# and it derives 3 task slots from the 3800m cgroup quota. Spark's task` \
  `# slots are integral, so cores=3 reproduces that exactly; 4 would let one` \
  `# executor oversubscribe its own 3800m limit and be throttled, which would` \
  `# understate Spark. memory+memoryOverhead sums to 5000Mi, the same cap.` \
  --conf spark.executor.instances=3 \
  --conf spark.executor.cores=3 \
  --conf spark.executor.memory=3800m \
  --conf spark.executor.memoryOverhead=1200m \
  --conf spark.kubernetes.executor.request.cores=250m \
  --conf spark.kubernetes.executor.limit.cores=3800m \
  \
  `# ── Driver: matched to apitest-coordinator (limits cpu 2 / mem 2Gi) ──` \
  --conf spark.driver.cores=1 \
  --conf spark.driver.memory=1600m \
  --conf spark.driver.memoryOverhead=448m \
  --conf spark.kubernetes.driver.request.cores=250m \
  --conf spark.kubernetes.driver.limit.cores=2 \
  \
  `# ── S3A against the same node-local MinIO the engine reads ──────────` \
  `# The endpoint is the internalTrafficPolicy:Local service, so each Spark` \
  `# executor reads from the MinIO on its own node — the same locality the` \
  `# Krishiv executors get, from the identical endpoint string.` \
  --conf spark.hadoop.fs.s3a.endpoint="$S3_ENDPOINT" \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  `# The env-var provider lives in the AWS SDK, not the hadoop-aws package;` \
  `# org.apache.hadoop.fs.s3a.EnvironmentVariableCredentialsProvider does not` \
  `# exist and fails at first read with ClassNotFoundException.` \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.EnvironmentVariableCredentialsProvider \
  --conf spark.kubernetes.driver.secretKeyRef.AWS_ACCESS_KEY_ID=minio-s3-creds:AWS_ACCESS_KEY_ID \
  --conf spark.kubernetes.driver.secretKeyRef.AWS_SECRET_ACCESS_KEY=minio-s3-creds:AWS_SECRET_ACCESS_KEY \
  --conf spark.kubernetes.executor.secretKeyRef.AWS_ACCESS_KEY_ID=minio-s3-creds:AWS_ACCESS_KEY_ID \
  --conf spark.kubernetes.executor.secretKeyRef.AWS_SECRET_ACCESS_KEY=minio-s3-creds:AWS_SECRET_ACCESS_KEY \
  \
  `# ── The job + corpus arrive via a ConfigMap mounted at /opt/job ─────` \
  --conf spark.kubernetes.driver.podTemplateFile=/opt/job/spark-driver-pod-template.yaml \
  \
  local:///opt/job/tpch_spark_run.py \
    --data "$DATA" \
    --corpus-json /opt/job/corpus.json \
    --out /tmp/spark-tpch.json \
    --label spark-3.5.3-sf100 \
    ${ONLY:+--only "$ONLY"}

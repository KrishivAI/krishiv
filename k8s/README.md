# Krishiv Kubernetes Manifests

Two distinct deployment paths — pick one, they do not need to coexist:

| Path | Entry point | When to use |
|------|-------------|-------------|
| **Operator** | `kubectl apply -k k8s/operator` | Production; CRDs + operator manages jobs |
| **Direct** | `kubectl apply -f k8s/direct/krishiv-dev.yaml` | Local dev, k3s, bare-metal — no operator |
| **Helm** | `helm install krishiv k8s/helm/krishiv` | Production; Helm-managed releases |

---

## Directory layout

```
k8s/
  crds/               Custom Resource Definitions (KrishivJob, KrishivQueue, KrishivExecutorPool)
  operator/           Operator-managed production deployment
    kustomization.yaml
    namespace.yaml, serviceaccount.yaml, rbac.yaml
    operator-deployment.yaml
    coordinator-service.yaml, executor-deployment.yaml
    network-policy.yaml, jcp-pod-template.yaml, keda-scaledobject.yaml
    samples/           example KrishivJob CRs
  direct/             Raw Deployments — no operator, no CRDs required
    krishiv-dev.yaml         single-node local cluster (uses localhost/krishiv:local)
    krishiv-distributed.yaml full multi-node direct deployment
  infra/              Shared infrastructure dependencies
    redpanda.yaml      Redpanda StatefulSet + headless Service
  jobs/               One-shot Kubernetes Jobs for examples and benchmarks
    python-examples.yaml
    kafka-streaming-sql.yaml
    benchmark.yaml
  helm/               Helm chart (operator-mode, production releases)
```

---

## Operator path (production)

Install CRDs + operator in one command:

```bash
just deploy-k8s
```

Or manually:

```bash
kubectl apply -k k8s/operator
```

Submit a batch job via the `KrishivJob` CR:

```bash
kubectl apply -f k8s/operator/samples/krishivjob-batch.yaml
kubectl get krishivjobs -n krishiv-system
```

Build and load the image before applying:

```bash
just docker-local    # multi-stage build + load into k3s in one step
```

Or stage binaries manually (faster if already built):

```bash
just build-k8s && just stage
docker build -t localhost/krishiv:local .
```

---

## Direct path (dev / local k3s)

No operator or CRDs required. Runs coordinator + executors as plain Deployments.

```bash
# Quick local cluster (uses localhost/krishiv:local image)
kubectl apply -f k8s/direct/krishiv-dev.yaml

# Full multi-node deployment
kubectl apply -f k8s/direct/krishiv-distributed.yaml
```

---

## Infrastructure

Redpanda (Kafka-compatible) StatefulSet for streaming scenarios:

```bash
kubectl apply -f k8s/infra/redpanda.yaml

# Verify
kubectl exec redpanda-0 -- rpk topic list
```

---

## Jobs (one-shot examples)

```bash
# Native Rust Kafka streaming SQL (10 scenarios)
kubectl apply -f k8s/jobs/kafka-streaming-sql.yaml

# Python example suite
kubectl apply -f k8s/jobs/python-examples.yaml

# Throughput benchmark
kubectl apply -f k8s/jobs/benchmark.yaml
```

Local run against in-cluster Redpanda:

```bash
kubectl port-forward pod/redpanda-0 9092:9092 &
BOOTSTRAP=localhost:9092 cargo run --bin kafka_streaming_sql
```

---

## Helm

```bash
helm install krishiv k8s/helm/krishiv \
  --set image.repository=ghcr.io/yourorg/krishiv \
  --set image.tag=0.1.0
```

---

## kind smoke test

```bash
docker build -t krishiv:dev .
KRISHIV_KIND_E2E=1 KRISHIV_KIND_IMAGE=krishiv:dev \
  cargo test -p krishiv-operator --test r2_kind_smoke
```

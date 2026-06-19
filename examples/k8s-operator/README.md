# Kubernetes Operator Deployment

## Setup
Deploy the Krishiv Operator. The operator manages Custom Resources (CRDs) and natively spawns an ephemeral **JCP (Job Coordinator Pod)** per job for total isolation.

```bash
kubectl apply -k ../../k8s/operator
```

## Running Examples
Submit the job using the `KrishivJob` CRD. The Operator will detect this and automatically spin up the JCP and Executor Pool specifically for this workload.
```bash
kubectl apply -f batch-job.yaml
```

# Kubernetes Direct Deployment

## Setup
Deploy the static Coordinator, Flight Server, and Executors directly to Kubernetes without the Operator.

```bash
kubectl apply -f ../../k8s/direct/krishiv-distributed.yaml
```
Port-forward the Flight Server so you can run scripts locally against the remote cluster:
```bash
kubectl port-forward svc/krishiv-flight-server 50051:50051
```

## Running Examples
```bash
export KRISHIV_COORDINATOR_URL=http://127.0.0.1:50051
python3 python_streaming.py
```

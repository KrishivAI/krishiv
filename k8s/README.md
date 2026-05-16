# Krishiv Kubernetes Manifests

These manifests are the R2 Kubernetes Distributed Alpha skeleton. They define
the first `KrishivJob` custom resource shape and the minimal objects needed for
a single active coordinator plus replaceable executors.

Apply from the repository root after building/publishing a compatible image:

```bash
kubectl apply -k k8s/manifests
```

R2 limitations:

- The coordinator deployment is intentionally `replicas: 1`.
- `crates/krishiv-operator` includes the typed reconciliation foundation and
  first live Kubernetes watch/status-patch path.
- No HA leader election, leases, or fencing tokens are included in R2.
- The example `KrishivJob` can now be observed by the operator path, but R2
  still needs `kind` smoke tests before the Kubernetes path is accepted.

# Krishiv Kubernetes Manifests

These manifests are the R2 Kubernetes Distributed Alpha skeleton. They define
the first `KrishivJob` custom resource shape and the minimal objects needed for
a single active coordinator plus replaceable executors.

Apply from the repository root after building/publishing a compatible image:

```bash
kubectl apply -k k8s/manifests
```

For local `kind` smoke testing:

```bash
docker build -t krishiv:dev .
KRISHIV_KIND_E2E=1 KRISHIV_KIND_IMAGE=krishiv:dev cargo test -p krishiv-operator --test r2_kind_smoke
```

Useful test flags:

- `KRISHIV_KIND_CLUSTER` sets the cluster name, default `krishiv-r2`.
- `KRISHIV_KIND_SKIP_CREATE=1` reuses the current `kind` cluster context.
- `KRISHIV_KIND_SKIP_LOAD_IMAGE=1` skips `kind load docker-image`.
- `KRISHIV_KIND_TIMEOUT_SECS` changes status polling timeout.

R2 limitations:

- The coordinator deployment is intentionally `replicas: 1`.
- `crates/krishiv-operator` includes the typed reconciliation foundation and
  first live Kubernetes watch/status-patch path.
- No HA leader election, leases, or fencing tokens are included in R2.
- The `kind` smoke tests are opt-in because they require Docker, `kind`,
  `kubectl`, and a locally built or published image.

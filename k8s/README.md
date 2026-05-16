# Krishiv Kubernetes Manifests

These manifests are the R2 Kubernetes Distributed Alpha skeleton. They define
the first `KrishivJob` custom resource shape and the minimal objects needed for
one operator-owned active coordinator runtime plus replaceable executors.

Apply from the repository root after building/publishing a compatible image:

```bash
kubectl apply -k k8s/manifests
```

After the operator is ready, the scheduler-backed status UI can be inspected
through the coordinator service:

```bash
kubectl -n krishiv-system port-forward svc/krishiv-coordinator 8080:8080
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

- The operator deployment is intentionally `replicas: 1` and owns the active
  R2 coordinator scheduler in this release.
- The `krishiv-coordinator` service exposes the operator's scheduler-backed
  status API and Web UI on port 8080.
- `crates/krishiv-operator` includes the typed reconciliation foundation,
  first live Kubernetes watch/status-patch path, and shared in-process
  coordinator runtime used by the status API.
- Executor pods are still a R2 manifest/runtime placeholder; the operator uses
  a bootstrap executor until real executor registration is added.
- No HA leader election, leases, or fencing tokens are included in R2.
- The `kind` smoke tests are opt-in because they require Docker, `kind`,
  `kubectl`, and a locally built or published image.

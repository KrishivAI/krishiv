# krishiv-operator

Kubernetes operator: the `KrishivJob`, `KrishivQueue`, and
`KrishivExecutorPool` CRD types and the reconciler that turns them into
coordinator submissions, tracks status (conditions, phases, task counters),
adds a finalizer so deletion cancels the job, and reports executor pod launch
failures. Manifests and CRDs live in `deploy/k8s/`.

Documentation: `docs/architecture/14-deployment-and-durability.md`,
`docs/architecture/11-public-interfaces.md`, `deploy/k8s/README.md`.

License: Apache-2.0.

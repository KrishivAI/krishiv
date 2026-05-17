# Krishiv Pre-R9 Security Posture

**Status:** Active — applies from R3.1 until R9 TLS/RBAC lands.
**Owner:** Architecture team.
**Linked releases:** R3.1 (controls implemented), R9 (replaced by full TLS + RBAC).

---

## Context

From R3.1 to R9 (six releases), the Krishiv gRPC transport between coordinator and executors operates **without mutual TLS or application-level authentication**. This is a known, intentional trade-off to reduce implementation scope in early releases.

This document defines the security controls that must be in place before R3.1 is deployed in any shared Kubernetes environment, and explicitly names what is **not** protected until R9.

---

## What Is Not Protected Until R9

| Attack Surface | Risk | Deferred to |
|---|---|---|
| Any pod in the cluster can register as a fake executor | Steal task assignments, inject false status | R9 (mTLS + RBAC) |
| Any pod can send fake heartbeats | Prevent coordinator from detecting real failures | R9 |
| gRPC traffic is unencrypted on the wire | Credentials in task specs visible to network observers | R9 |
| No audit log for who submitted a job | Non-repudiation gap | R9 (audit logs) |

These risks are acceptable in a **dedicated single-tenant Kubernetes namespace** with proper NetworkPolicy. They are **not acceptable** in a shared multi-tenant cluster without R9 controls.

---

## Required Controls (R3.1 Onwards)

### 1. Dedicated Namespace With NetworkPolicy

All Krishiv components (coordinator, executors, operator, shuffle store) must run in a dedicated Kubernetes namespace. A `NetworkPolicy` restricts gRPC access to within that namespace:

```yaml
# docs/deploy/network-policy.yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: krishiv-internal-only
  namespace: krishiv
spec:
  podSelector: {}          # applies to all pods in namespace
  policyTypes:
    - Ingress
    - Egress
  ingress:
    - from:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: krishiv
  egress:
    - to:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: krishiv
    - to: {}               # allow egress to S3 and Kafka (external)
      ports:
        - port: 443
        - port: 9092
```

This ensures that pods outside the `krishiv` namespace cannot reach the coordinator gRPC port.

### 2. Kubernetes RBAC For Job Submission

The `KrishivJob` CRD must have RBAC rules that restrict who can create/delete jobs:

```yaml
# Only users/service accounts with this ClusterRole can submit jobs
rules:
- apiGroups: ["krishiv.io"]
  resources: ["krishivjobs"]
  verbs: ["create", "delete", "get", "list", "watch"]
```

Job submission authorization is enforced by the Kubernetes API server, not by Krishiv itself.

### 3. Service Account Per Component

- Coordinator pod: dedicated `ServiceAccount` with `KrishivJob` read/write access.
- Executor pods: dedicated `ServiceAccount` with S3 access via IAM role binding (IRSA or Workload Identity).
- No pod should run as `default` service account.

### 4. No Sensitive Data In Task Specs (R3.1)

Task specs transmitted over gRPC must not contain S3 credentials, Kafka credentials, or other secrets. Credentials must be injected via:
- Kubernetes `Secret` volumes mounted into executor pods.
- IAM role bindings (IRSA / Workload Identity) for S3.
- Kubernetes `Secret`-backed environment variables for Kafka.

The task spec itself contains only object paths and configuration references, not credential values.

---

## Upgrade Path to R9

R9 replaces all of the above with:
- Mutual TLS (mTLS) on all gRPC connections (coordinator ↔ executor).
- RBAC integration at the Krishiv API level (not just Kubernetes RBAC for CRDs).
- Audit logs for all sensitive operations.

The NetworkPolicy and service account controls above are additive — they remain in place after R9 as defense-in-depth.

---

## Known Limitations (Must Be Documented In R3.1 Release Notes)

- gRPC between coordinator and executor is unencrypted. Do not deploy in shared clusters before R9.
- Executor identity is not verified beyond Kubernetes network isolation. Any pod in the `krishiv` namespace that knows the coordinator gRPC address can register as an executor.
- Job submission is authorized at the Kubernetes API level only. Krishiv itself does not perform authentication.

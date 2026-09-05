# Security

Krishiv's security model is small and explicit: authenticate every network
surface with a bearer credential, authorise by role and table policy, fence
every durable write, validate every untrusted identifier, and fail closed in
production. This document lists the mechanisms and where each is enforced.

## Production mode and fail-closed defaults

`KRISHIV_PRODUCTION=1` (`krishiv_common::production`) switches the process to
fail-closed behaviour, read once and cached:

| Behaviour | Development (`dev-local`, not production) | Production or any durable profile |
|---|---|---|
| anonymous coordinator HTTP / Flight | allowed | refused unless `KRISHIV_ALLOW_ANONYMOUS_HTTP` / `KRISHIV_ALLOW_ANONYMOUS` (dev escape hatches, logged) |
| untyped legacy task fragments | allowed | refused unless `KRISHIV_ALLOW_LEGACY_FRAGMENTS` |
| metadata/event write failure | logged | fails the operation (`profile_requires_fail_closed_metadata`) |
| simulation connectors | allowed | refused |
| window state durability | in-memory | required (`profile_requires_durable_window_state`) |

The `security-durability-gate.yml` workflow asserts these behaviours against
a built binary on every push.

## Authentication

| Surface | Credential | Source variables |
|---|---|---|
| coordinator gRPC (`CoordinatorService`) | bearer token | `KRISHIV_COORDINATOR_BEARER_TOKEN[S]`, `…_FILE`; reloaded every `KRISHIV_COORDINATOR_AUTH_RELOAD_INTERVAL_SECS`; Kubernetes secret via `KRISHIV_COORDINATOR_AUTH_SECRET_NAME`/`_KEY` |
| coordinator HTTP `/api/v1` | `Authorization: Bearer` (`http_auth.rs`) | same token set as gRPC |
| executor task RPCs | bearer | `KRISHIV_EXECUTOR_TASK_BEARER_TOKEN`, `KRISHIV_REQUIRE_EXECUTOR_TASK_AUTH`, secret name/key |
| shuffle service | bearer (`token_auth.rs`) | `KRISHIV_SHUFFLE_TOKEN[_FILE]`, `KRISHIV_SHUFFLE_TOKEN_RELOAD_SECS` |
| Flight SQL | API key / bearer through an `AuthProvider` | `KRISHIV_FLIGHT_API_KEY`, `KRISHIV_API_KEY[S]`, `KRISHIV_FLIGHT_ALLOW_ALL_AUTHENTICATED` |
| web console | bearer injected into the page | `KRISHIV_UI_TOKEN[_FILE]` |
| OIDC / JWT | `krishiv_role` claim, audience check | `KRISHIV_OIDC_JWKS_URI`, `KRISHIV_OIDC_AUDIENCE` |
| catalogs | per-catalog tokens | `KRISHIV_ICEBERG_REST_TOKEN`, `KRISHIV_UNITY_TOKEN` |

Implementation rules that hold everywhere:

- Comparison is constant-time (`constant_time_eq`) and iterates **every**
  configured key without short-circuiting, so elapsed time does not reveal
  which key matched (`StaticApiKeyAuthProvider`,
  `StaticApiKeyAuthProviderWithRole`).
- An empty configured token never matches anything, even a blank header.
- Token sources configured but all empty install `RejectAllAuthProvider` —
  revocation fails closed, it does not fall back to anonymous.
- Tokens are reloaded from files/secrets on an interval so rotation needs no
  restart.
- No token value is ever logged or rendered; the console receives it only in
  the authenticated page body.

## Authorisation

Coordinator RBAC (`auth.rs`): `Role::{Reader, Writer, Admin}`. The role comes
from the authenticated subject's prefix — `reader:`, `writer:`, `admin:` —
and **anything unprefixed is `Reader`** (least privilege; never escalated).
JWT providers put the role in the `krishiv_role` claim and return
`<role>:<sub>`. Readers can list and inspect; writers can submit, cancel,
feed; admins can drain executors, change queues, restore.

Table policy (`krishiv_plan::governance::PolicyHook::check_table_access`) is
consulted by the SQL engine and Flight SQL before a plan touches a table;
`AllowAllPolicyHook` is the embedded default. `Session::with_auth` /
`with_policy` install providers for embedded use; the MCP frontend refuses
write SQL unless `KRISHIV_MCP_ALLOW_WRITE_SQL` is set.

## Transport security

TLS for gRPC/Flight is configured with `KRISHIV_TLS_CERT`, `KRISHIV_TLS_KEY`,
and `KRISHIV_CA_CERT` (client verification); the Flight SQL JDBC string
selects `useEncryption=true`. In Kubernetes the manifests add
`NetworkPolicy` objects restricting executor and shuffle ports to the
namespace (`14`).

## Fencing and integrity

- Every metadata write, checkpoint commit, and shuffle write carries a
  fencing token or lease generation (`04`, `06`, `07`); a deposed leader or a
  zombie task cannot commit.
- Checkpoint epochs are sealed by a SHA-256 manifest; a flipped byte is
  detected and the epoch skipped (`07`).
- Object-store shuffle partitions carry BLAKE3 content hashes (`06`).
- IVM ticks carry a fence; replay or gap is an error, never a double-apply
  (`05`).

## Input validation

- `validate_safe_id` rejects empty ids, path separators, NUL, and `..` before
  any job/stage/task/partition id becomes a path (`krishiv-shuffle`,
  state directories, spool files).
- Fragment payload parts are base64 so user SQL cannot break wire framing.
- Sizes are capped: result spool (8 GiB), inline results, shuffle store,
  input buffers, MCP row limits.
- `unsafe_code` is forbidden in every crate but one audited `pre_exec` site
  in the CLI's process utilities; `unwrap`/`expect`/`panic`/`print` are
  denied in non-test code by workspace lints (`17`).

## Supply chain

`security.yml` runs `cargo-deny` (advisories, licenses, bans); `codeql.yml`
runs CodeQL; `just lint-deps` is the local form. Release images are built
from pinned Dockerfiles (`deploy/docker`) and published by `release.yml`.

## Operational rules

Secrets never enter the repository; deployment manifests reference
Kubernetes `Secret`s by name. The GA soak and chaos environments run with
production mode on, so the fail-closed paths are exercised continuously
(`../engineering-log/ga-soak-report-2026-08-10.md`).

## Related

- `04`, `06`, `07` (fencing), `11` (surfaces), `14` (deployment), `15`
  (every variable above in the registry).

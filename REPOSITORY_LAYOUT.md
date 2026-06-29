# Repository Layout

Where things live in the Krishiv repo. Read this first if you are new here;
`AGENTS.md`, `docs/README.md`, and `docs/implementation/status.md` are the
authoritative source for rules, crate ownership, and session handoff.

## Top-level directories

| Path | Role |
|---|---|
| `crates/` | Rust workspace — 24 crates, one per engine/feature boundary. See `docs/README.md` for the full map. |
| `docs/` | Architecture, contracts, decisions, implementation notes, governance, roadmap. |
| `examples/` | Runnable example programs in Rust, Python, and Enterprise layouts. |
| `python/` | First-party Python packages (PyO3 bindings + vendor integrations). |
| `deploy/k8s/` | Kubernetes manifests, Helm chart, CRDs, deployment yamls. |
| `deploy/docker/` | Container image definitions (multi-stage build, distributed, single-node, fast, prod). |
| `web/` | Next.js marketing + documentation site. |
| `tests/` | Workspace-wide integration test artifacts (golden files, placeholders). |
| `scripts/` | Python + shell helpers — mix of CI/release gates and dev/operator scripts. |
| `skills/` | Canonical location for AI-agent skills (`krishiv-engine`, `release`). |
| `deploy/systemd/` | Bare-metal / systemd service units. |
| `api/` | Versioned snapshots of the public Rust, Python, and SQL surfaces + the stable-API phase table. |
| `dist/` | Build staging area (`dist/docker/` for pre-built binaries; `dist/helm/` for packaged charts). Gitignored. |
| `target/` | Cargo build output. Gitignored. |
| `codex/`, `.claude/` | AI agent tool state (worktrees, permissions, settings). |
| `.cargo/` | Cargo workspace config (linker, env, aliases) — checked in intentionally. |

## Top-level files (non-config)

| Path | Role |
|---|---|
| `AGENTS.md` | AI-agent + contributor rules. **Read first.** |
| `README.md` | Project overview and quick start. |
| `CHANGELOG.md` | Release history. |
| `LICENSE` | Apache-2.0. |
| `CODE_OF_CONDUCT.md`, `CONTRIBUTING.md`, `SECURITY.md` | GitHub-mandated community files. |
| `Cargo.toml` | Rust workspace manifest. |
| `Cargo.lock` | Resolved dependency versions. |
| `justfile` | `just` runner recipes — the canonical entrypoint for build / test / lint / release. |
| `rust-toolchain.toml` | Pinned Rust toolchain version. |
| `deny.toml` | `cargo-deny` policy (licenses, bans, advisories). |
| `REPOSITORY_LAYOUT.md` | This file. |
| `RELEASE.md`, `RUNNING.md` (in docs/) | Per-task guides. |

## "I want to X" cheat sheet

- **Add a new feature to the engine** → touch one crate under `crates/`; see
  the crate map in `docs/README.md` to find the right owner.
- **Add a new connector** → `crates/krishiv-connectors/src/`, implement
  `SourceProvider` / `SinkProvider`, add a maturity label and a test.
- **Add a new Python binding** → `crates/krishiv-python/src/`, regenerate
  `python-public.json` via `just api-inventory`.
- **Run benchmarks** → `just bench-*` recipes; see `docs/BENCHMARKING.md`.
- **Update the public API surface** → `just api-inventory`, then review
  `target/api-change-report.json` and add to `api/approved-breaking.toml`
  if needed.
- **Cut a release** → follow `skills/release/SKILL.md` step by step.
- **Investigate a production issue** → start at `docs/implementation/status.md`
  for the latest session note + validation command.

## Why some things look the way they do

- **One crate per behavior, not one crate per binary.** The facade `krishiv`
  re-exports user-facing types; the actual work happens in `krishiv-runtime`,
  `krishiv-dataflow`, `krishiv-scheduler`, etc. New code belongs in the
  crate that owns the behavior, not in the facade.
- **Shuffle, state, checkpoint, metadata, connector behavior live behind
  trait APIs.** Don't call into another crate's internals — add a method to
  the trait. See `AGENTS.md` architecture invariants.
- **`api/` is the public-surface baseline**, not an HTTP API. CI runs
  `scripts/check_api_surface.py` against it; breaking changes must be
  recorded in `api/approved-breaking.toml`.
- **`dist/docker/` is a build staging area** (binaries copied here before
  `docker build -f Dockerfile.fast`). It is gitignored and intentionally
  kept outside `target/` so `cargo clean` does not delete it.
- **5 Dockerfiles is intentional**: `Dockerfile.build` is the multi-stage
  chef-cached source build, `Dockerfile.distributed`/`Dockerfile.single-node`
  are the final runtime images, `Dockerfile.fast` is the constrained-VM
  fallback that uses pre-staged binaries, and `Dockerfile.prod` is the
  CI/release image. They share builder logic but split runtime by
  deployment mode; consolidating them into one Dockerfile is intentionally
  not done (see `docs/implementation/status.md`). All five live under
  `deploy/docker/`.
- **Three ignore files is intentional**: `.gitignore` (Rust + Python),
  `.dockerignore` (container build context), `web/.gitignore` (Next.js
  build artifacts). Each co-locates with the tool that produces the
  artifacts.
- **`.cargo/config.toml` is checked in intentionally**: it pins the
  linker (mold), the GCC 15 `cstdint` workaround, and the `cargo check-*`
  / `cargo build-*` mode aliases. Delete it only if you have a different
  workspace-level config strategy.

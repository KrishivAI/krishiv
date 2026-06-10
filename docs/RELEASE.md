# Release Process

## Version scheme

Krishiv uses [Semantic Versioning](https://semver.org/) (`MAJOR.MINOR.PATCH`).

| Change type | Version bump |
|-------------|-------------|
| Breaking public API change | `MAJOR` |
| Backward-compatible new feature or behaviour change | `MINOR` |
| Bug fix, dependency update, docs, refactoring | `PATCH` |

Pre-1.0 (current): `0.MINOR.PATCH` — minor bumps may include breaking changes.
Starting at `1.0.0`, the standard semver stability guarantees apply.

## Checklist before cutting a release

1. All CI checks pass on `main` (`cargo check --workspace`, `cargo test --workspace --lib`, `cargo clippy --workspace`).
2. `cargo fmt --check` passes.
3. `docs/implementation/status.md` reflects the current implementation state.
4. `CHANGELOG.md` (if maintained) has an `## Unreleased` section converted to the new version heading.

## Tagging a release

Use the `just release` recipe:

```bash
VERSION=0.2.0 just release
git push && git push --tags
```

This:
1. Updates `version = "…"` in `Cargo.toml` (workspace root).
2. Runs `cargo check --workspace` to verify no breakage.
3. Commits `Cargo.toml` + `Cargo.lock` with message `chore: bump version to X.Y.Z`.
4. Creates an annotated tag `vX.Y.Z`.

## CI gate matrix

CI enforces the following on every PR and push to `main`:

| Job | What it checks |
|-----|---------------|
| `fmt-lint` | `cargo fmt --check`, `cargo clippy -D warnings` |
| `check (embedded)` | `cargo check -p krishiv --features embedded` |
| `check (single-node)` | `cargo check -p krishiv --features single-node` |
| `check (bare-metal)` | `cargo check -p krishiv --features bare-metal` |
| `check (k8s)` | `cargo check -p krishiv -p krishiv-operator --features k8s` |
| `test` | `cargo test --workspace --lib` (excludes python, chaos) |
| `bench` | `cargo bench -p krishiv-bench` (stores baseline on main, compares on PRs) |

## Benchmark baselines

Criterion baselines are stored in `target/criterion/` and cached by CI under the key
`bench-baseline-main-<SHA>`. PRs automatically compare against the most recent `main`
baseline. A regression is a warning, not a hard failure — the comparison output is
uploaded as an artifact for manual review.

To update a local baseline after intentional performance improvements:

```bash
just bench-save main
```

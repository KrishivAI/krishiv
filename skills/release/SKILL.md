---
name: release
description: Cut a Krishiv release — bump versions, validate, tag, and push.
---

# Release Skill

Orchestrates a Krishiv release from version bump through git tag.

## Usage

```
/release 0.2.0
/release 0.2.0-rc.1
```

## Workflow

### 1. Pre-flight checks

```bash
git status --short          # clean worktree required
git branch                  # must be on main or release/**
```

### 2. Bump version

The version must be updated in these files:

| File | Field |
|------|-------|
| `Cargo.toml` | `[workspace.package] version` |
| `k8s/helm/krishiv/Chart.yaml` | `version` (chart) + `appVersion` (app) |
| `examples/rust/Cargo.toml` | `version` |
| `examples/enterprise/rust/Cargo.toml` | `version` |
| `python/krishiv-airflow/pyproject.toml` | `version` |
| `python/krishiv-dbt-adapter/pyproject.toml` | `version` |
| `python/krishiv-ge/pyproject.toml` | `version` |
| `crates/krishiv-python/pyproject.toml` | `version` |

Use `sed` for each file:

```bash
VERSION="0.2.0"

# Workspace version (Cargo.toml [workspace.package])
sed -i 's/^version = ".*"/version = "'"${VERSION}"'"/' Cargo.toml

# Helm chart
sed -i 's/^version:.*/version: '"${VERSION}"'/' k8s/helm/krishiv/Chart.yaml
sed -i 's/^appVersion:.*/appVersion: "'"${VERSION}"'"/' k8s/helm/krishiv/Chart.yaml

# Examples
sed -i 's/^version = ".*"/version = "'"${VERSION}"'"/' examples/rust/Cargo.toml
sed -i 's/^version = ".*"/version = "'"${VERSION}"'"/' examples/enterprise/rust/Cargo.toml

# Python packages
sed -i 's/^version = ".*"/version = "'"${VERSION}"'"/' python/krishiv-airflow/pyproject.toml
sed -i 's/^version = ".*"/version = "'"${VERSION}"'"/' python/krishiv-dbt-adapter/pyproject.toml
sed -i 's/^version = ".*"/version = "'"${VERSION}"'"/' python/krishiv-ge/pyproject.toml
sed -i 's/^version = ".*"/version = "'"${VERSION}"'"/' crates/krishiv-python/pyproject.toml
```

### 3. Update CHANGELOG.md

Move `[Unreleased]` to the new version:

```bash
DATE=$(date +%Y-%m-%d)
sed -i 's/## \[Unreleased\]/## ['"${VERSION}"'] - '"${DATE}"'/' CHANGELOG.md
```

Add an `[Unreleased]` section at the top:

```bash
sed -i '/^## \['"${VERSION}"'\]/i ## [Unreleased]\n\n### Added\n\n### Changed\n\n### Fixed\n\n' CHANGELOG.md
```

### 4. Validate

Run all three validation scripts:

```bash
python3 scripts/check_release.py --tag "v${VERSION}"
python3 scripts/check_parity_manifest.py
python3 scripts/check_migration_notes.py
```

### 5. Compile check

```bash
cargo check --workspace
cargo fmt --check
```

### 6. Commit, tag, push

```bash
git add Cargo.toml Cargo.lock k8s/helm/krishiv/Chart.yaml \
  examples/rust/Cargo.toml examples/enterprise/rust/Cargo.toml \
  python/krishiv-airflow/pyproject.toml python/krishiv-dbt-adapter/pyproject.toml \
  python/krishiv-ge/pyproject.toml \
  crates/krishiv-python/pyproject.toml CHANGELOG.md

git commit -m "chore: release v${VERSION}"
git tag -a "v${VERSION}" -m "Release v${VERSION}"
git push
git push origin "v${VERSION}"
```

### 7. Create GitHub Release

```bash
gh release create "v${VERSION}" --generate-notes
```

This triggers `.github/workflows/release.yml` which builds binaries, wheels,
Docker images, Helm chart, SBOM, and publishes to PyPI/crates.io (stable only).

## RC Releases

For release candidates:

```bash
VERSION="0.2.0"
RC="1"
git tag -a "v${VERSION}-rc.${RC}" -m "Release candidate v${VERSION}-rc.${RC}"
git push origin "v${VERSION}-rc.${RC}"
gh release create "v${VERSION}-rc.${RC}" --prerelease --generate-notes
```

RC releases skip PyPI/crates.io publishing.

## Post-release

Update `docs/implementation/status.md` with a short handoff note.

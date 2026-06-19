set shell := ["bash", "-cu"]
set dotenv-load := true   # picks up .env for IMAGE, BOOTSTRAP, etc.

# Rust toolchain (override: RUST_TOOLCHAIN=nightly just check-k8s)
rust_toolchain := env_var_or_default("RUST_TOOLCHAIN", "stable")
cargo := "cargo +" + rust_toolchain

# Optional build accelerators — used automatically if installed.
#   sccache:  cargo binstall sccache   (caches across branches and CI runs)
#   nextest:  cargo binstall nextest   (parallel test runner, ~2x faster)
#   mold:     apt install mold         (5-10x faster linker than lld)
sccache_env  := if `which sccache 2>/dev/null` != "" { "RUSTC_WRAPPER=sccache" } else { "" }
cargo_test   := if `which cargo-nextest 2>/dev/null` != "" { cargo + " nextest run" } else { cargo + " test" }

# Target triple for portable release binaries (k8s / bare-metal)
target := env_var_or_default("TARGET", "x86_64-unknown-linux-musl")

# Docker image coordinates
image          := env_var_or_default("IMAGE", "localhost/krishiv:local")
registry_image := env_var_or_default("REGISTRY_IMAGE", "ghcr.io/yourorg/krishiv:dev")

# Executor slots for the bare-metal cluster
slots := env_var_or_default("SLOTS", "4")

# ── Default ──────────────────────────────────────────────────────────────────
# List all available recipes
default:
    @just --list

# ── Check (fast, no codegen) ─────────────────────────────────────────────────

# Fastest possible check — dev-fast profile (opt-level 0 for deps, no debuginfo).
# Use during active development when you only care about compile errors, not runtime speed.
check-fast:
    {{ sccache_env }} {{ cargo }} check --workspace --profile dev-fast \
        --exclude krishiv-python \
        --exclude krishiv-chaos

# Verify every feature set compiles independently
check: check-embedded check-single-node check-distributed check-k8s check-full
    @echo "✓ all feature sets check clean"

# Check embedded mode (in-process library only)
check-embedded:
    {{ cargo }} check -p krishiv --no-default-features --features embedded

# Check single-node mode (Flight SQL + local shuffle + SQLite metadata)
check-single-node:
    {{ cargo }} check -p krishiv --no-default-features --features single-node

# Check bare-metal distributed mode (etcd metadata, no operator)
check-bare-metal:
    {{ cargo }} check -p krishiv --no-default-features --features bare-metal

# Alias kept for backwards compat with any local scripts
check-distributed: check-bare-metal

# Check k8s mode (distributed + operator CRD support)
check-k8s:
    {{ cargo }} check -p krishiv -p krishiv-operator \
        --no-default-features --features k8s

# Check full feature set (kafka + iceberg + delta connectors)
check-full:
    {{ cargo }} check -p krishiv -p krishiv-operator \
        --no-default-features --features full

# ── Build ─────────────────────────────────────────────────────────────────────

# Fast debug build using dev-fast profile (deps at opt-level 0, no debuginfo).
# Link time is the bottleneck — install mold for an additional 5-10x speedup.
build-fast:
    {{ sccache_env }} {{ cargo }} build -p krishiv \
        --no-default-features --features single-node \
        --profile dev-fast

# Build embedded mode (library — no binary produced)
build-embedded:
    {{ cargo }} build -p krishiv --no-default-features --features embedded

# Build debug binary for single-node local development
build-single-node:
    {{ cargo }} build -p krishiv --no-default-features --features single-node

# Build release binary for bare-metal / VM distributed clusters
build-bare-metal:
    {{ cargo }} build -p krishiv \
        --no-default-features --features bare-metal \
        --profile release-k8s

# Maximum-performance release build (fat LTO + panic=abort + strip).
# Matches what nightly and production release CI produce.
# Expect 3-5x longer compile than build-single-node; run only when benchmarking.
build-max:
    {{ sccache_env }} {{ cargo }} build -p krishiv \
        --no-default-features --features full \
        --profile release-max

# Build release binaries for Kubernetes (krishiv + krishiv-operator)
# Uses release-k8s profile (thin LTO) to stay within CI runner RAM limits.
# Outputs: target/{{ target }}/release-k8s/{krishiv,krishiv-operator}
build-k8s:
    {{ cargo }} build -p krishiv -p krishiv-operator \
        --no-default-features --features k8s \
        --profile release-k8s \
        --target {{ target }}

# ── Docker ────────────────────────────────────────────────────────────────────

# Multi-stage build → tag as IMAGE → load into local k3s (default: localhost/krishiv:local)
# Single image works for single-node, bare-metal, and k8s — mode selected at runtime via KRISHIV_MODE.
docker-local:
    docker build \
        --build-arg FEATURES=full \
        --build-arg PROFILE=release-k8s \
        -f Dockerfile.build \
        -t {{ image }} .
    docker save {{ image }} | k3s ctr images import -
    @echo "✓ loaded {{ image }} into k3s"

# Multi-stage build → push to registry (set REGISTRY_IMAGE env var)
docker-push:
    docker build \
        --build-arg FEATURES=full \
        --build-arg PROFILE=release-k8s \
        -f Dockerfile.build \
        -t {{ registry_image }} .
    docker push {{ registry_image }}

# Copy release-k8s binaries to dist/docker/ for the staged (non-multi-stage) Dockerfile
stage: build-k8s
    @mkdir -p dist/docker
    cp target/{{ target }}/release-k8s/krishiv          dist/docker/krishiv
    cp target/{{ target }}/release-k8s/krishiv-operator  dist/docker/krishiv-operator
    @echo "✓ staged binaries to dist/docker/"

# ── Run ───────────────────────────────────────────────────────────────────────

# Start a local single-node coordinator (builds debug binary first)
run-single-node: build-single-node
    ./target/debug/krishiv coordinator \
        --grpc-addr 0.0.0.0:50051 \
        --durability-profile dev-local \
        --insecure

# Start a local bare-metal cluster (coordinator + flight + executor)
run-bare-metal:
    SLOTS={{ slots }} bash scripts/run_bare_metal.sh

# ── Kubernetes ────────────────────────────────────────────────────────────────

# Apply operator + CRDs to the current kubectl context
deploy-k8s:
    kubectl apply -k k8s/operator

# Apply raw Deployments without operator (dev / local k3s)
deploy-direct:
    kubectl apply -f k8s/direct/krishiv-dev.yaml

# Apply shared infrastructure (Redpanda StatefulSet)
deploy-infra:
    kubectl apply -f k8s/infra/redpanda.yaml

# Remove operator deployment from the current kubectl context
undeploy-k8s:
    kubectl delete -k k8s/operator --ignore-not-found

# ── Test ──────────────────────────────────────────────────────────────────────

# Run all workspace lib tests.
# Uses cargo-nextest automatically if installed (parallel, faster output).
# Install nextest: cargo binstall nextest
test:
    {{ sccache_env }} {{ cargo_test }} --workspace --lib \
        --exclude krishiv-python \
        --exclude krishiv-chaos

# Fast test iteration — dev-fast profile (opt-level 0 for deps).
# Runtime is slower but compile-link cycle is as short as possible.
test-fast:
    {{ sccache_env }} {{ cargo }} test --workspace --lib --profile dev-fast \
        --exclude krishiv-python \
        --exclude krishiv-chaos

# Tests that must pass with only embedded features enabled
test-embedded:
    {{ sccache_env }} {{ cargo }} test -p krishiv --no-default-features --features embedded --lib

# Single-node scheduler and runtime tests
test-single-node:
    {{ sccache_env }} {{ cargo }} test -p krishiv-scheduler --lib --no-default-features --features sqlite
    {{ sccache_env }} {{ cargo }} test -p krishiv-runtime --lib

# Kubernetes operator unit tests
test-k8s:
    {{ sccache_env }} {{ cargo }} test -p krishiv-operator --lib

# Connector certification suite (no live broker required)
test-connectors:
    {{ sccache_env }} {{ cargo }} test -p krishiv-connectors --lib

# SQL engine tests
test-sql:
    {{ sccache_env }} {{ cargo }} test -p krishiv-sql --lib

# ── Quality ───────────────────────────────────────────────────────────────────

# Check code formatting (run `cargo fmt` to fix)
fmt:
    {{ cargo }} fmt --check

# Run clippy across the workspace
lint:
    {{ cargo }} clippy \
        --workspace \
        --exclude krishiv-python \
        --exclude krishiv-chaos \
        -- -D warnings

# Format then lint in one shot
tidy: fmt lint

# ── Benchmarks ────────────────────────────────────────────────────────────────

# Run all criterion benchmarks and emit Bencher-compatible JSON to stdout.
# Pipe to tee to keep a local copy: just bench | tee bench-output.txt
bench:
    {{ cargo }} bench -p krishiv-bench \
        --features "" \
        -- --output-format bencher

# Save a baseline under `.bench-baselines/<name>` using criterion's --save-baseline.
# Usage: just bench-save main   (saves to .bench-baselines/main)
bench-save name:
    @mkdir -p .bench-baselines
    {{ cargo }} bench -p krishiv-bench \
        -- --save-baseline {{ name }}
    @echo "✓ baseline '{{ name }}' saved"

# Compare current performance against a saved baseline.
# Usage: just bench-compare main
bench-compare name:
    {{ cargo }} bench -p krishiv-bench \
        -- --baseline {{ name }}

# ── Project hygiene ────────────────────────────────────────────────────────────

# Validate repository scripts, local documentation links, and release metadata.
project-check:
    python3 -m unittest discover -s scripts/tests -v
    python3 scripts/check_api_surface.py
    python3 scripts/check_markdown_links.py
    python3 scripts/check_release.py

# Regenerate checked-in Rust, Python, and SQL public API inventories.
api-inventory:
    python3 scripts/check_api_surface.py --write

# Classify public API changes against a Git ref (default: origin/main).
api-diff ref="origin/main":
    python3 scripts/compare_api_surface.py --against-ref "{{ ref }}" --report target/api-change-report.json


# Record the machine and revision used for a benchmark run.
# Usage: just bench-manifest criterion "cargo bench -p krishiv-bench"
bench-manifest suite command:
    python3 scripts/benchmark_manifest.py --suite "{{ suite }}" --command "{{ command }}" --output target/benchmark-manifest.json

# ── Release ───────────────────────────────────────────────────────────────────

# Bump workspace + Helm chart to VERSION, commit, tag, and push.
# After pushing, go to GitHub → Releases → Draft a new release → select
# tag v{{ version }} → fill in release notes → Publish.
# Publishing the release triggers the CI pipeline (builds artifacts, Docker, Helm, PyPI, crates.io).
# Usage: just release 0.2.0
release version=env_var_or_default("VERSION", ""):
    @if [ -z "{{ version }}" ]; then echo "ERROR: pass version: just release 0.2.0"; exit 1; fi
    sed -i 's/^version = ".*"/version = "{{ version }}"/' Cargo.toml
    sed -i 's/^version:.*/version: {{ version }}/' k8s/helm/krishiv/Chart.yaml
    sed -i 's/^appVersion:.*/appVersion: "{{ version }}"/' k8s/helm/krishiv/Chart.yaml
    {{ cargo }} check --workspace --quiet
    git add Cargo.toml Cargo.lock k8s/helm/krishiv/Chart.yaml
    git commit -m "chore: bump version to {{ version }}"
    git tag -a "v{{ version }}" -m "Release v{{ version }}"
    git push
    git push origin "v{{ version }}"
    @echo ""
    @echo "✓ pushed v{{ version }}"
    @echo "→ Next: GitHub → Releases → Draft a new release → tag v{{ version }} → Publish"
    @echo "  Or:    gh release create v{{ version }} --generate-notes [--prerelease]"

# Tag and push a release candidate (does not bump version).
# Usage: just release-rc 0.2.0        → v0.2.0-rc.1
#        RC=2 just release-rc 0.2.0   → v0.2.0-rc.2
release-rc version=env_var_or_default("VERSION", "") rc=env_var_or_default("RC", "1"):
    @if [ -z "{{ version }}" ]; then echo "ERROR: pass version: just release-rc 0.2.0"; exit 1; fi
    git tag -a "v{{ version }}-rc.{{ rc }}" -m "Release candidate v{{ version }}-rc.{{ rc }}"
    git push origin "v{{ version }}-rc.{{ rc }}"
    @echo "✓ pushed v{{ version }}-rc.{{ rc }}"
    @echo "→ Next: gh release create v{{ version }}-rc.{{ rc }} --prerelease --generate-notes"

# Dry-run: verify all 17 publishable crates pass packaging checks without uploading.
publish-dry-run:
    @echo "=== Dry-run publish for all crates ==="
    @for crate in \
        krishiv-common krishiv-proto krishiv-metrics krishiv-plan \
        krishiv-dataflow krishiv-state krishiv-shuffle krishiv-connectors \
        krishiv-sql krishiv-scheduler krishiv-executor krishiv-runtime \
        krishiv-api krishiv-flight-sql krishiv-operator krishiv-ui krishiv; do \
        echo "--> $crate"; \
        cargo publish -p "$crate" --dry-run --allow-dirty 2>&1 | tail -1; \
    done
    @echo "✓ dry-run complete"

# Publish all 17 Rust crates to crates.io in topological order.
# Requires CARGO_REGISTRY_TOKEN env var.
# Usage: CARGO_REGISTRY_TOKEN=<token> VERSION=0.2.0 just publish-crates
publish-crates version=env_var_or_default("VERSION", ""):
    @if [ -z "{{ version }}" ]; then echo "ERROR: set VERSION"; exit 1; fi
    @if [ -z "${CARGO_REGISTRY_TOKEN:-}" ]; then echo "ERROR: set CARGO_REGISTRY_TOKEN"; exit 1; fi
    @echo "=== Publishing crates at v{{ version }} ==="
    @for crate in \
        krishiv-common krishiv-proto krishiv-metrics krishiv-plan \
        krishiv-dataflow krishiv-state krishiv-shuffle krishiv-connectors \
        krishiv-sql krishiv-scheduler krishiv-executor krishiv-runtime \
        krishiv-api krishiv-flight-sql krishiv-operator krishiv-ui krishiv; do \
        echo "--> Publishing $crate"; \
        cargo publish -p "$crate" --no-verify || echo "WARNING: $crate may already be published"; \
        sleep 30; \
    done
    @echo "✓ crates published"

# Build and publish the Python wheel to PyPI.
# Uses maturin for the krishiv-python crate (PyO3 bindings).
# Requires MATURIN_PYPI_TOKEN env var (or keyring credentials).
# Usage: MATURIN_PYPI_TOKEN=<token> just publish-wheel
publish-wheel:
    cd crates/krishiv-python && maturin publish --no-sdist

# Build and push a Docker image to the registry.
# Defaults to the k8s feature set. Override FEATURES/PROFILE/REGISTRY_IMAGE as needed.
# Usage: VERSION=0.2.0 REGISTRY_IMAGE=ghcr.io/myorg/krishiv just publish-docker
publish-docker version=env_var_or_default("VERSION", "dev"):
    docker buildx build \
        --platform linux/amd64,linux/arm64 \
        --build-arg FEATURES=full \
        --build-arg PROFILE=release-max \
        -f Dockerfile.build \
        -t {{ registry_image }}:{{ version }} \
        -t {{ registry_image }}:latest \
        --push .
    @echo "✓ pushed {{ registry_image }}:{{ version }}"

# Package the Helm chart and push it to the OCI registry.
# Usage: VERSION=0.2.0 REGISTRY_IMAGE=ghcr.io/myorg/krishiv just publish-helm
publish-helm version=env_var_or_default("VERSION", ""):
    @if [ -z "{{ version }}" ]; then echo "ERROR: set VERSION"; exit 1; fi
    sed -i 's/^version:.*/version: {{ version }}/' k8s/helm/krishiv/Chart.yaml
    sed -i 's/^appVersion:.*/appVersion: "{{ version }}"/' k8s/helm/krishiv/Chart.yaml
    @mkdir -p dist/helm
    helm package k8s/helm/krishiv --destination dist/helm
    helm push dist/helm/krishiv-{{ version }}.tgz oci://$(echo {{ registry_image }} | cut -d/ -f1-2)/charts
    @echo "✓ pushed helm chart krishiv-{{ version }}.tgz"

# ── Web docs site ─────────────────────────────────────────────────────────────

# Install dependencies for the Fumadocs/Next.js public website.
web-install:
    cd web && npm install

# Start the public website locally.
web-dev:
    cd web && npm run dev

# Build the public website.
web-build:
    cd web && npm run build

# Type-check the public website.
web-typecheck:
    cd web && npm run typecheck

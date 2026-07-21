set shell := ["bash", "-cu"]
set dotenv-load := true   # picks up .env for IMAGE, BOOTSTRAP, etc.

# Rust toolchain (override: RUST_TOOLCHAIN=nightly just check-k8s)
rust_toolchain := env_var_or_default("RUST_TOOLCHAIN", "stable")
cargo := "cargo +" + rust_toolchain

# Optional build accelerators — used automatically if installed.
#   sccache:  cargo binstall sccache   (caches across branches and CI runs)
#   nextest:  cargo binstall nextest   (parallel test runner, ~2x faster)
#   mold:     apt install mold         (5-10x faster linker than lld)
sccache_env  := if `which sccache 2>/dev/null || true` != "" { "RUSTC_WRAPPER=sccache" } else { "" }
cargo_test   := if `which cargo-nextest 2>/dev/null || true` != "" { cargo + " nextest run" } else { cargo + " test" }

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

# Build embedded mode (library — no binary produced)
build-embedded:
    {{ cargo }} build -p krishiv --no-default-features --features embedded

# Build debug binary for single-node local development
build-single-node:
    {{ cargo }} build -p krishiv --no-default-features --features single-node

# Build release binary for bare-metal / VM distributed clusters
# (rest-catalog: same rationale as build-k8s — deployed daemons must be able
# to attach a platform Iceberg REST catalog from KRISHIV_ICEBERG_REST_*.)
build-bare-metal:
    {{ sccache_env }} {{ cargo }} build -p krishiv \
        --no-default-features --features bare-metal,rest-catalog \
        --profile release

# Build release binaries for Kubernetes (krishiv + krishiv-operator)
# Outputs: target/{{ target }}/release/{krishiv,krishiv-operator}
# rest-catalog is required in deployed images: without it the daemon silently
# ignores KRISHIV_ICEBERG_REST_* and governed Iceberg tables never resolve.
build-k8s:
    {{ sccache_env }} {{ cargo }} build -p krishiv -p krishiv-operator \
        --no-default-features --features k8s,rest-catalog \
        --profile release \
        --target {{ target }}

# ── Docker ────────────────────────────────────────────────────────────────────

# Fast local Docker build using host-compiled dev binaries.
# Use when the multi-stage Dockerfile.build times out on constrained VMs.
docker-fast:
    @mkdir -p dist/docker
    cp target/debug/krishiv dist/docker/krishiv
    cp target/debug/krishiv-operator dist/docker/krishiv-operator 2>/dev/null || true
    docker buildx build --load \
        -f deploy/docker/Dockerfile.fast \
        -t {{ image }} .
    @echo "✓ loaded {{ image }} into local docker (use k3s ctr images import for k3s)"

# Prod fast image: release binary in an isolated target dir, verified before
# imaging (guards against parallel-build artifact clobbering). Optional arg
# overrides the tag (default localhost/krishiv:fast-<sha>).
docker-fast-prod tag="":
    scripts/build-fast-engine.sh {{ tag }}

# Build the k8s image locally and load into the local docker daemon.
# For single-node dev use: just docker-single-node (faster, smaller)
docker-local:
    docker buildx build --load \
        --build-arg FEATURES="k8s,jemalloc" \
        -f deploy/docker/Dockerfile.distributed \
        -t {{ image }} .
    @echo "✓ loaded {{ image }} into local docker (use k3s ctr images import for k3s)"

# Build the k8s image and push to registry (single-arch, fast path for dev)
docker-push:
    docker buildx build --push \
        --build-arg FEATURES="k8s,jemalloc" \
        -f deploy/docker/Dockerfile.distributed \
        -t {{ registry_image }} .

# Copy release binaries to dist/docker/ for the staged Dockerfile.fast path
stage: build-k8s
    @mkdir -p dist/docker
    cp target/{{ target }}/release/krishiv          dist/docker/krishiv
    cp target/{{ target }}/release/krishiv-operator  dist/docker/krishiv-operator
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
    kubectl apply -k deploy/k8s/operator

# Apply raw Deployments without operator (dev / local k3s)
deploy-direct:
    kubectl apply -f deploy/k8s/direct/krishiv-dev.yaml

# Apply shared infrastructure (Redpanda StatefulSet)
deploy-infra:
    kubectl apply -f deploy/k8s/infra/redpanda.yaml

# Remove operator deployment from the current kubectl context
undeploy-k8s:
    kubectl delete -k deploy/k8s/operator --ignore-not-found

# ── Test ──────────────────────────────────────────────────────────────────────

# Run all workspace lib tests.
# Uses cargo-nextest automatically if installed (parallel, faster output).
# Install nextest: cargo binstall nextest
#
# Required-tier companion recipes (Phase 51 CI honesty — the PR gate is
# `test` + `test-integration` + `test-doc`; the tier map with a named
# rationale per exclusion lives in docs/implementation/ci-tiers.md):
test:
    {{ sccache_env }} {{ cargo_test }} --workspace --lib \
        --exclude krishiv-python \
        --exclude krishiv-chaos

# All crates' tests/*.rs integration suites. External-service tests inside
# them are `#[ignore = "requires …"]`-gated and stay opt-in (see ci-tiers.md).
test-integration:
    {{ sccache_env }} {{ cargo_test }} --workspace --tests \
        --exclude krishiv-python \
        --exclude krishiv-chaos \
        --exclude krishiv-bench

# Documentation examples compile and run.
test-doc:
    {{ sccache_env }} {{ cargo }} test --workspace --doc \
        --exclude krishiv-python \
        --exclude krishiv-chaos

# External-service tests (audit §14 TEST-6): runs the `#[ignore = "requires …"]`
# tests against provisioned backends instead of leaving them false-green.
# Start/stop the services with `scripts/external-test-services.sh {up,down}`
# (docker compose: postgres :5439, MinIO :9102, OTLP collector :4319); every
# endpoint below can be overridden with the same env var the test reads.
# The two live-cluster tests (mode_conformance :9090, api :50051) stay out
# until Phase 58's real multi-executor harness exists (docs/implementation/ci-tiers.md).
test-external:
    KRISHIV_TEST_DATABASE_URL="${KRISHIV_TEST_DATABASE_URL:-postgres://krishiv:krishiv@127.0.0.1:5439/krishiv_test}" \
        {{ sccache_env }} {{ cargo }} test -p krishiv-sql --features postgres-catalog --lib -- --ignored postgres_catalog
    AWS_ENDPOINT_URL="${AWS_ENDPOINT_URL:-http://127.0.0.1:9102}" \
    AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-minio}" \
    AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-minio12345}" \
    AWS_REGION="${AWS_REGION:-us-east-1}" \
    AWS_ALLOW_HTTP=true \
    KRISHIV_TEST_S3_BUCKET="${KRISHIV_TEST_S3_BUCKET:-krishiv-test}" \
        {{ sccache_env }} {{ cargo }} test -p krishiv-sql --features postgres-catalog --lib -- --ignored s3_round_trip
    OTEL_EXPORTER_OTLP_ENDPOINT="${OTEL_EXPORTER_OTLP_ENDPOINT:-http://127.0.0.1:4319}" \
        {{ sccache_env }} {{ cargo }} test -p krishiv-metrics --lib -- --ignored otlp_integration
    {{ sccache_env }} {{ cargo }} test -p krishiv-runtime --lib -- --ignored do_action_

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

# Line-coverage measurement (audit §14 TEST-4) over the same scope as the
# required test gate: lib + integration tests, python/chaos/bench excluded
# (see docs/implementation/ci-tiers.md). Prints the per-crate summary table;
# coverage.yml runs this nightly and publishes the number to the job summary.
# Install: cargo binstall cargo-llvm-cov
coverage:
    {{ sccache_env }} {{ cargo }} llvm-cov --workspace \
        --exclude krishiv-python \
        --exclude krishiv-chaos \
        --exclude krishiv-bench \
        --lib --tests \
        --summary-only

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
    # krishiv-sql's `default = []` means a plain --workspace pass above never
    # enables iceberg-datafusion/local-catalog, so execute_iceberg_ctas and
    # its callees (the code path the `prod` preset actually ships) are
    # invisible to the check above — a lint blindspot, not a clean bill of
    # health. Lint that combination explicitly too.
    {{ cargo }} clippy \
        -p krishiv-sql \
        --features iceberg-datafusion,local-catalog \
        -- -D warnings

# Verify each optional feature compiles on its own — catches forwarding-flag
# rot and "doesn't build with --no-default-features" breakage in the crates
# that own the feature graph. Installs cargo-hack on demand.
#
# Quarantined features (pre-existing dependency-API rot in optional, non-preset
# integrations; tracked in docs/feature-graph.md → "Quarantined features"):
#   connectors: pulsar-source, cassandra, elasticsearch, vortex, cloud
#   sql:        rest-catalog, unity-catalog, glue-catalog
#   (postgres-catalog un-quarantined 2026-07-11: fixed against iceberg 0.9.1,
#    live-verified by `just test-external`)
lint-features:
    @command -v cargo-hack >/dev/null 2>&1 || {{ cargo }} install cargo-hack --locked
    {{ cargo }} hack check --each-feature --no-dev-deps -p krishiv-connectors \
        --exclude-features pulsar-source,cassandra,elasticsearch,vortex,cloud
    {{ cargo }} hack check --each-feature --no-dev-deps -p krishiv-sql \
        --exclude-features rest-catalog,unity-catalog,glue-catalog
    @echo "✓ per-feature builds clean (quarantined features: see docs/feature-graph.md)"

# Format then lint in one shot
tidy: fmt lint

# Audit dead code: count `#[allow(dead_code)]` (legacy) and `#[expect(dead_code, ...)]`
# annotations, then run `cargo-machete` to find unused dependencies and
# unreachable symbols. Install cargo-machete on first run.
#
# The legacy `#[allow(dead_code)]` annotations are tolerated for cases where
# lint propagation is needed (e.g. test-only struct with helpers); see
# AGENTS.md → "Dead code (12 scenarios — pick the right annotation)" for
# the full taxonomy.
audit-dead-code:
    @command -v cargo-machete >/dev/null 2>&1 || {{ cargo }} install cargo-machete --locked
    @echo "── #[allow(dead_code)] (legacy, prefer #[expect]) ──"
    @grep -rn "#\[allow(dead_code)\]" crates/ --include="*.rs" | wc -l
    @echo
    @echo "── #[expect(dead_code, ...)] (preferred) ──"
    @grep -rn "#\[expect(dead_code" crates/ --include="*.rs" | wc -l
    @echo
    @echo "── cargo-machete scan ──"
    {{ cargo }} machete --with-metadata

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

# TPC-H batch SQL ladder (embedded engine + in-process cluster overhead).
# Set KRISHIV_TPCH_DATA_DIR_SF1/_SF10/_SF100 to Parquet dirs (tpchgen-cli);
# unset scale factors are skipped with a notice.
bench-tpch:
    {{ cargo }} bench -p krishiv-bench --bench tpch_sf10
    {{ cargo }} bench -p krishiv-bench --bench tpch_distributed
    {{ cargo }} bench -p krishiv-bench --bench tpch_overhead

# Nexmark streaming queries (Q1/Q2/Q5/Q8) through the embedded SqlEngine.
bench-nexmark:
    {{ cargo }} bench -p krishiv-bench --bench nexmark

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
    sed -i 's/^version:.*/version: {{ version }}/' deploy/k8s/helm/krishiv/Chart.yaml
    sed -i 's/^appVersion:.*/appVersion: "{{ version }}"/' deploy/k8s/helm/krishiv/Chart.yaml
    {{ cargo }} check --workspace --quiet
    git add Cargo.toml Cargo.lock deploy/k8s/helm/krishiv/Chart.yaml
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

# Build the single-node embedded image locally.
docker-single-node:
    docker buildx build --load \
        --build-arg FEATURES="embedded,jemalloc" \
        -f deploy/docker/Dockerfile.single-node \
        -t {{ image }}-single-node .
    @echo "✓ built {{ image }}-single-node"

# Build the distributed cluster image locally (all daemons, no K8s operator).
docker-distributed:
    docker buildx build --load \
        --build-arg FEATURES="distributed,jemalloc" \
        -f deploy/docker/Dockerfile.distributed \
        -t {{ image }}-distributed .
    @echo "✓ built {{ image }}-distributed"

# Build and push all three image variants to the registry.
# Publishes: :{version}, :{version}-single-node, :{version}-distributed, :latest (stable only).
# Usage: VERSION=0.2.0 REGISTRY_IMAGE=ghcr.io/myorg/krishiv just publish-docker
publish-docker version=env_var_or_default("VERSION", "dev"):
    @if [ "{{ version }}" = "dev" ]; then echo "WARNING: publishing dev tag — set VERSION for a release"; fi
    # k8s / full image  →  :version  +  :latest
    docker buildx build \
        --platform linux/amd64,linux/arm64 \
        --build-arg FEATURES="k8s,jemalloc" \
        -f deploy/docker/Dockerfile.distributed \
        -t {{ registry_image }}:{{ version }} \
        -t {{ registry_image }}:latest \
        --push .
    # Single-node embedded image  →  :version-single-node  +  :single-node
    docker buildx build \
        --platform linux/amd64,linux/arm64 \
        --build-arg FEATURES="embedded,jemalloc" \
        -f deploy/docker/Dockerfile.single-node \
        -t {{ registry_image }}:{{ version }}-single-node \
        -t {{ registry_image }}:single-node \
        --push .
    # Distributed cluster image  →  :version-distributed  +  :distributed
    docker buildx build \
        --platform linux/amd64,linux/arm64 \
        --build-arg FEATURES="distributed,jemalloc" \
        -f deploy/docker/Dockerfile.distributed \
        -t {{ registry_image }}:{{ version }}-distributed \
        -t {{ registry_image }}:distributed \
        --push .
    @echo "✓ pushed {{ registry_image }}:{{ version }} / :{{ version }}-single-node / :{{ version }}-distributed"

# Package the Helm chart and push it to the OCI registry.
# Usage: VERSION=0.2.0 REGISTRY_IMAGE=ghcr.io/myorg/krishiv just publish-helm
publish-helm version=env_var_or_default("VERSION", ""):
    @if [ -z "{{ version }}" ]; then echo "ERROR: set VERSION"; exit 1; fi
    sed -i 's/^version:.*/version: {{ version }}/' deploy/k8s/helm/krishiv/Chart.yaml
    sed -i 's/^appVersion:.*/appVersion: "{{ version }}"/' deploy/k8s/helm/krishiv/Chart.yaml
    @mkdir -p dist/helm
    helm package deploy/k8s/helm/krishiv --destination dist/helm
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

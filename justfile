set shell := ["bash", "-cu"]
set dotenv-load := true   # picks up .env for IMAGE, BOOTSTRAP, etc.

# Rust toolchain (override: RUST_TOOLCHAIN=nightly just check-k8s)
rust_toolchain := env_var_or_default("RUST_TOOLCHAIN", "stable")
cargo := "cargo +" + rust_toolchain

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

# Verify every execution-mode feature set compiles independently
check: check-embedded check-single-node check-distributed check-k8s
    @echo "✓ all execution modes check clean"

# Check embedded mode (in-process library only)
check-embedded:
    {{ cargo }} check -p krishiv --no-default-features --features embedded

# Check single-node mode (Flight SQL + local shuffle + SQLite metadata)
check-single-node:
    {{ cargo }} check -p krishiv --no-default-features --features single-node

# Check bare-metal distributed mode (etcd metadata, no operator)
check-distributed:
    {{ cargo }} check -p krishiv --no-default-features --features bare-metal

# Check k8s mode (distributed + operator CRD support)
check-k8s:
    {{ cargo }} check -p krishiv -p krishiv-operator \
        --no-default-features --features k8s

# ── Build ─────────────────────────────────────────────────────────────────────

# Build embedded mode (library — no binary produced)
build-embedded:
    {{ cargo }} build -p krishiv --no-default-features --features embedded

# Build debug binary for single-node local development
build-single-node:
    {{ cargo }} build -p krishiv --no-default-features --features single-node

# Build release binary for bare-metal / VM distributed clusters
build-bare-metal:
    @mkdir -p .tmp
    {{ cargo }} build -p krishiv \
        --no-default-features --features bare-metal \
        --profile release-bare-metal

# Build release binaries for Kubernetes (krishiv + krishiv-operator)
# Uses release-k8s profile (thin LTO) to stay within CI runner RAM limits.
# Outputs: target/{{ target }}/release-k8s/{krishiv,krishiv-operator}
build-k8s:
    @mkdir -p .tmp
    {{ cargo }} build -p krishiv -p krishiv-operator \
        --no-default-features --features k8s \
        --profile release-k8s \
        --target {{ target }}

# ── Docker ────────────────────────────────────────────────────────────────────

# Multi-stage build → tag as IMAGE → load into local k3s (default: localhost/krishiv:local)
docker-local:
    docker build \
        --build-arg FEATURES=k8s \
        --build-arg PROFILE=release-k8s \
        -f Dockerfile.build \
        -t {{ image }} .
    docker save {{ image }} | k3s ctr images import -
    @echo "✓ loaded {{ image }} into k3s"

# Multi-stage build → push to registry (set REGISTRY_IMAGE env var)
docker-push:
    docker build \
        --build-arg FEATURES=k8s \
        --build-arg PROFILE=release-k8s \
        -f Dockerfile.build \
        -t {{ registry_image }} .
    docker push {{ registry_image }}

# Build a bare-metal image (no operator, smaller binary)
docker-bare-metal:
    docker build \
        --build-arg FEATURES=bare-metal \
        --build-arg PROFILE=release-bare-metal \
        -f Dockerfile.build \
        -t {{ image }}-bare-metal .

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

# Run all workspace lib tests
test:
    {{ cargo }} test --workspace --lib \
        --exclude krishiv-python \
        --exclude krishiv-chaos

# Tests that must pass with only embedded features enabled
test-embedded:
    {{ cargo }} test -p krishiv --no-default-features --features embedded --lib

# Single-node scheduler and runtime tests
test-single-node:
    {{ cargo }} test -p krishiv-scheduler --lib --no-default-features --features sqlite
    {{ cargo }} test -p krishiv-runtime --lib

# Kubernetes operator unit tests
test-k8s:
    {{ cargo }} test -p krishiv-operator --lib

# Connector certification suite (no live broker required)
test-connectors:
    {{ cargo }} test -p krishiv-connectors --lib

# SQL engine tests
test-sql:
    {{ cargo }} test -p krishiv-sql --lib

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

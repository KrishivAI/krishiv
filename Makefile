# Krishiv build automation.
#
# Each execution mode maps to a distinct set of Cargo features and build
# profile.  The mode is the only thing the caller needs to know.
#
# Quick reference:
#   make check          — fast feature-matrix check (no codegen)
#   make build-single-node
#   make build-bare-metal
#   make build-k8s
#   make docker-local   — build image + load into local k3s
#   make docker-push IMAGE=ghcr.io/yourorg/krishiv:tag
#   make run-bare-metal — start a local coordinator + executor cluster
#   make deploy-k8s     — kubectl apply -k k8s/operator
#   make test           — workspace tests
#   make lint           — clippy + fmt check

# ── Toolchain ─────────────────────────────────────────────────────────────────

RUST_TOOLCHAIN   ?= stable
CARGO            := cargo +$(RUST_TOOLCHAIN)

# Target triple for k8s/bare-metal release builds.
# Override to cross-compile: make build-k8s TARGET=aarch64-unknown-linux-musl
TARGET           ?= x86_64-unknown-linux-musl

# Docker image coordinates.
IMAGE            ?= localhost/krishiv:local
REGISTRY_IMAGE   ?= ghcr.io/yourorg/krishiv:$(shell git rev-parse --short HEAD)

# ── Feature sets (match docs/README.md Build Feature Matrix) ─────────────────

FEATURES_EMBEDDED    := embedded
FEATURES_SINGLE_NODE := single-node
FEATURES_BARE_METAL  := bare-metal
FEATURES_K8S         := k8s
FEATURES_FULL        := full

# ── Directories ───────────────────────────────────────────────────────────────

DIST_DIR := dist/docker
TMP_DIR  := .tmp

$(DIST_DIR) $(TMP_DIR):
	mkdir -p $@

# ── Check (no codegen — fast feedback) ───────────────────────────────────────

.PHONY: check check-embedded check-single-node check-distributed check-k8s check-full

## check: verify all execution-mode feature sets compile (no binary output)
check: check-embedded check-single-node check-distributed check-k8s
	@echo "✓ All execution modes check clean"

check-embedded:
	$(CARGO) check -p krishiv --no-default-features --features $(FEATURES_EMBEDDED)

check-single-node:
	$(CARGO) check -p krishiv --no-default-features --features $(FEATURES_SINGLE_NODE)

check-distributed:
	$(CARGO) check -p krishiv --no-default-features --features $(FEATURES_BARE_METAL)

check-k8s:
	$(CARGO) check -p krishiv -p krishiv-operator \
	    --no-default-features --features $(FEATURES_K8S)

check-full:
	$(CARGO) check -p krishiv --no-default-features --features $(FEATURES_FULL)

# ── Builds ────────────────────────────────────────────────────────────────────

.PHONY: build-embedded build-single-node build-bare-metal build-k8s

## build-embedded: library check (no binary — embedded mode has no standalone process)
build-embedded:
	$(CARGO) build -p krishiv --no-default-features --features $(FEATURES_EMBEDDED)

## build-single-node: CLI binary with Flight SQL + local shuffle + SQLite metadata
build-single-node:
	$(CARGO) build -p krishiv --no-default-features --features $(FEATURES_SINGLE_NODE)

## build-bare-metal: release binary for bare-metal/VM distributed deployment
build-bare-metal: | $(TMP_DIR)
	$(CARGO) build -p krishiv \
	    --no-default-features --features $(FEATURES_BARE_METAL) \
	    --release

## build-k8s: release binary + operator for Kubernetes deployment
##   Uses release-k8s profile (thin LTO, 4 codegen units) to avoid OOM on VPS builders.
##   Produces binaries at target/$(TARGET)/release-k8s/{krishiv,krishiv-operator}.
build-k8s: | $(TMP_DIR)
	$(CARGO) build -p krishiv -p krishiv-operator \
	    --no-default-features --features $(FEATURES_K8S) \
	    --profile release-k8s \
	    --target $(TARGET)

# ── Docker ────────────────────────────────────────────────────────────────────

.PHONY: stage docker-local docker-push docker-bare-metal

## stage: copy release-k8s binaries to dist/docker/ for the staged Dockerfile path
stage: build-k8s | $(DIST_DIR)
	cp target/$(TARGET)/release-k8s/krishiv         $(DIST_DIR)/krishiv
	cp target/$(TARGET)/release-k8s/krishiv-operator $(DIST_DIR)/krishiv-operator
	@echo "Staged binaries to $(DIST_DIR)/"

## docker-local: multi-stage build → tag as localhost/krishiv:local → load into k3s
docker-local:
	docker build \
	    --build-arg FEATURES=$(FEATURES_K8S) \
	    --build-arg PROFILE=release-k8s \
	    -f Dockerfile.build \
	    -t $(IMAGE) .
	k3s ctr images import <(docker save $(IMAGE)) 2>/dev/null || \
	    docker save $(IMAGE) | k3s ctr images import -
	@echo "Loaded $(IMAGE) into k3s"

## docker-push: multi-stage build → push to registry (set REGISTRY_IMAGE)
docker-push:
	docker build \
	    --build-arg FEATURES=$(FEATURES_K8S) \
	    --build-arg PROFILE=release-k8s \
	    -f Dockerfile.build \
	    -t $(REGISTRY_IMAGE) .
	docker push $(REGISTRY_IMAGE)

## docker-bare-metal: image without operator (for bare-metal distributed clusters)
docker-bare-metal:
	docker build \
	    --build-arg FEATURES=$(FEATURES_BARE_METAL) \
	    --build-arg PROFILE=release \
	    -f Dockerfile.build \
	    -t $(IMAGE)-bare-metal .

# ── Run ───────────────────────────────────────────────────────────────────────

.PHONY: run-single-node run-bare-metal

## run-single-node: start a local single-node coordinator (Flight SQL on :50051)
run-single-node: build-single-node
	./target/debug/krishiv coordinator \
	    --grpc-addr 0.0.0.0:50051 \
	    --durability-profile dev-local \
	    --insecure

## run-bare-metal: start a local bare-metal cluster (coordinator + executor)
run-bare-metal:
	bash scripts/run_bare_metal.sh

# ── Kubernetes ────────────────────────────────────────────────────────────────

.PHONY: deploy-k8s deploy-direct undeploy-k8s deploy-infra

## deploy-k8s: apply operator + CRDs to current kubectl context
deploy-k8s:
	kubectl apply -k k8s/operator

## deploy-direct: apply raw Deployments (no operator, for local k3s / dev)
deploy-direct:
	kubectl apply -f k8s/direct/krishiv-dev.yaml

## deploy-infra: apply shared infrastructure (Redpanda)
deploy-infra:
	kubectl apply -f k8s/infra/redpanda.yaml

## undeploy-k8s: remove operator deployment from current kubectl context
undeploy-k8s:
	kubectl delete -k k8s/operator --ignore-not-found

# ── Test ──────────────────────────────────────────────────────────────────────

.PHONY: test test-embedded test-single-node test-k8s test-connectors

## test: run all workspace lib tests
test:
	$(CARGO) test --workspace --lib \
	    --exclude krishiv-python \
	    --exclude krishiv-chaos

## test-embedded: tests that must compile and pass with only embedded features
test-embedded:
	$(CARGO) test -p krishiv --no-default-features --features $(FEATURES_EMBEDDED) --lib

## test-single-node: tests for single-node mode
test-single-node:
	$(CARGO) test -p krishiv-scheduler --lib \
	    --no-default-features --features sqlite
	$(CARGO) test -p krishiv-runtime --lib

## test-k8s: operator unit tests
test-k8s:
	$(CARGO) test -p krishiv-operator --lib

## test-connectors: connector certification tests (no live broker needed)
test-connectors:
	$(CARGO) test -p krishiv-connectors --lib

## test-sql: sql engine tests
test-sql:
	$(CARGO) test -p krishiv-sql --lib

# ── Code quality ──────────────────────────────────────────────────────────────

.PHONY: fmt lint

## fmt: check formatting (use `cargo fmt` to fix)
fmt:
	$(CARGO) fmt --check

## lint: clippy across the workspace
lint:
	$(CARGO) clippy --workspace \
	    --exclude krishiv-python \
	    --exclude krishiv-chaos \
	    -- -D warnings

# ── Help ──────────────────────────────────────────────────────────────────────

.PHONY: help

help:
	@grep -E '^## ' Makefile | sed 's/## //'

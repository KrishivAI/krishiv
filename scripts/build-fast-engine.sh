#!/usr/bin/env bash
# Build the fast (host-compiled) engine prod image from the `prod` preset, in an
# isolated target dir so parallel builds in the same checkout cannot overwrite
# the binary between compile and image (the platformd variant of this clobber
# shipped a broken image on 2026-07-10).
#
# Feature notes (see crates/krishiv/Cargo.toml):
#   prod = distributed + rest-catalog + kafka + iceberg + cloud + jemalloc.
#   This is the deployed-cluster capability set: the distributed control plane
#   (flight-sql + shuffle + etcd), Kafka source/sink (rdkafka), the full Iceberg
#   stack incl. durable CTAS/DML interception, AND `cloud` object-store I/O so
#   Iceberg tables/checkpoints can live on S3/MinIO. The old `local` default
#   silently omitted kafka AND cloud — a shipped image could not do Kafka
#   sources or object storage, with no runtime signal (see the flag-minimization
#   plan + `krishiv capabilities`). Building `prod` closes that gap.
#
# The runtime base is ubuntu:26.04 to match the build host's glibc (2.43);
# debian:trixie-slim is too old for host-compiled binaries.
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
SHA="$(git -C "$REPO" rev-parse --short HEAD)"
TAG="${1:-localhost/krishiv:fast-$SHA}"
TARGET_DIR="$REPO/target/prod-image"

# `x86-64-v3` (AVX2 + BMI2 + FMA), not the default baseline and not `native`.
#
# Why this is a correction and not a tuning knob: Spark runs on the JVM, whose
# JIT inspects the CPU at runtime and emits AVX2 by itself. A Rust binary is
# AOT-compiled, so without this flag it targets the `x86-64` baseline — roughly
# SSE2 — and Arrow's vectorised kernels never see the machine's actual SIMD.
# Benchmarking a baseline-compiled AOT binary against a JIT that self-tunes
# measures the build flags, not the engines.
#
# `v3` rather than `native`: `.cargo/config.toml` explains why `native` is not
# set globally — it silently produces a binary that runs only on the builder.
# `v3` names a floor instead of a machine. Verified 2026-07-29 that the build
# box and all three cluster nodes report avx2/bmi2/fma (AMD EPYC, no AVX-512),
# so the floor holds everywhere this image runs. If a node without AVX2 ever
# joins, it will fault loudly on first instruction rather than mis-execute —
# check `/proc/cpuinfo` before widening the fleet.
RUSTFLAGS_BENCH="${RUSTFLAGS:-} -C target-cpu=x86-64-v3"

echo "== cargo build (prod preset; isolated target dir; target-cpu=x86-64-v3)"
(cd "$REPO" && CARGO_TARGET_DIR="$TARGET_DIR" RUSTFLAGS="$RUSTFLAGS_BENCH" \
    cargo build --locked --release -p krishiv --no-default-features --features prod)

BIN="$TARGET_DIR/release/krishiv"
echo "== verify binary capabilities (prod preset must carry the durable-CTAS, kafka, and cloud markers)"
# grep -c, not -q: -q exits at first match and SIGPIPEs `strings`, which
# pipefail turns into a false FATAL even when the marker is present.
missing=""
if [ "$(strings "$BIN" | grep -c "removed replaced snapshot's data files")" -eq 0 ]; then
    missing="$missing durable-CTAS(iceberg)"
fi
if [ "$(strings "$BIN" | grep -ic "rdkafka")" -eq 0 ]; then
    missing="$missing kafka(rdkafka)"
fi
if [ "$(strings "$BIN" | grep -ic "object_store.*aws\|dispatch.*s3\|AmazonS3\|s3.amazonaws")" -eq 0 ]; then
    missing="$missing cloud(s3)"
fi
if [ -n "$missing" ]; then
    echo "FATAL: prod binary is missing expected capabilities:$missing — wrong features or stale build" >&2
    exit 1
fi
echo "   capabilities OK: durable-CTAS + kafka + cloud present"

echo "== docker image $TAG"
CTX="$(mktemp -d)"
trap 'rm -rf "$CTX"' EXIT
cp "$BIN" "$CTX/krishiv"
cat > "$CTX/Dockerfile" <<'EOF'
FROM ubuntu:26.04
# python3 + pyarrow/numpy/cloudpickle power the distributed Python-UDF worker
# (krishiv_sql::python_udf spawns `python3` to run cloudpickled UDFs on executors).
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates python3 python3-pip \
    && pip3 install --no-cache-dir --break-system-packages pyarrow numpy cloudpickle \
    && apt-get purge -y python3-pip && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/* /root/.cache \
    && groupadd -r krishiv \
    && useradd -r -g krishiv -d /var/lib/krishiv -s /sbin/nologin krishiv
COPY krishiv /usr/local/bin/krishiv
USER krishiv
WORKDIR /var/lib/krishiv
ENTRYPOINT ["/usr/local/bin/krishiv"]
EOF
docker build -q -t "$TAG" "$CTX"
echo "== done: $TAG (import to ALL nodes before kubectl set image)"

#!/usr/bin/env bash
# Build the fast (host-compiled) engine prod image from the default `local`
# preset, in an isolated target dir so parallel builds in the same checkout
# cannot overwrite the binary between compile and image (the platformd
# variant of this clobber shipped a broken image on 2026-07-10).
#
# Feature notes (see crates/krishiv/Cargo.toml):
#   default = local = embedded + single-node + jemalloc + rest-catalog.
#   rest-catalog transitively enables krishiv-sql/iceberg-datafusion +
#   local-catalog, i.e. the FULL Iceberg stack incl. durable CTAS/DML
#   interception — no extra flags needed for the prod single-pod deploy.
#   Kafka SQL tables are always compiled (krishiv-sql hard-requires
#   krishiv-connectors/kafka); prod streaming ingest is platformd's bridge,
#   not engine rdkafka.
#
# The runtime base is ubuntu:26.04 to match the build host's glibc (2.43);
# debian:trixie-slim is too old for host-compiled binaries.
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
SHA="$(git -C "$REPO" rev-parse --short HEAD)"
TAG="${1:-localhost/krishiv:fast-$SHA}"
TARGET_DIR="$REPO/target/prod-image"

echo "== cargo build (default features; isolated target dir)"
(cd "$REPO" && CARGO_TARGET_DIR="$TARGET_DIR" \
    cargo build --locked --release -p krishiv)

BIN="$TARGET_DIR/release/krishiv"
echo "== verify binary (durable-CTAS marker from the lakehouse DML path)"
if ! strings "$BIN" | grep -q "removed replaced snapshot's data files"; then
    echo "FATAL: binary lacks the durable-CTAS lakehouse path — wrong features or stale build" >&2
    exit 1
fi

echo "== docker image $TAG"
CTX="$(mktemp -d)"
trap 'rm -rf "$CTX"' EXIT
cp "$BIN" "$CTX/krishiv"
cat > "$CTX/Dockerfile" <<'EOF'
FROM ubuntu:26.04
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -r krishiv \
    && useradd -r -g krishiv -d /var/lib/krishiv -s /sbin/nologin krishiv
COPY krishiv /usr/local/bin/krishiv
USER krishiv
WORKDIR /var/lib/krishiv
ENTRYPOINT ["/usr/local/bin/krishiv"]
EOF
docker build -q -t "$TAG" "$CTX"
echo "== done: $TAG (import to ALL nodes before kubectl set image)"

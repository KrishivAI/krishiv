#!/usr/bin/env bash
# Manage the external-service containers for `just test-external`
# (audit §14 TEST-6). Usage: scripts/external-test-services.sh {up|down|status}
set -euo pipefail

COMPOSE_FILE="$(cd "$(dirname "$0")/.." && pwd)/tests/external/docker-compose.yml"
PROJECT=krishiv-external-tests

case "${1:-}" in
  up)
    docker compose -p "$PROJECT" -f "$COMPOSE_FILE" up -d --wait
    # The S3 round-trip test needs its bucket to exist; the minio image ships mc.
    docker compose -p "$PROJECT" -f "$COMPOSE_FILE" exec -T minio \
      sh -c 'mc alias set local http://127.0.0.1:9000 minio minio12345 >/dev/null && mc mb --ignore-existing local/krishiv-test'
    echo "external test services ready (postgres :5439, minio :9102, otlp :4319)"
    ;;
  down)
    docker compose -p "$PROJECT" -f "$COMPOSE_FILE" down -v
    ;;
  status)
    docker compose -p "$PROJECT" -f "$COMPOSE_FILE" ps
    ;;
  *)
    echo "usage: $0 {up|down|status}" >&2
    exit 2
    ;;
esac

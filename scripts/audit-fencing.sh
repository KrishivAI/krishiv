#!/usr/bin/env bash
# Verify checkpoint fencing helpers are used on commit/restore paths (GAP-CP-03).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

fail=0

if ! rg -q 'validate_fencing_token' crates/krishiv-scheduler/src/checkpoint.rs; then
  echo "missing validate_fencing_token in scheduler checkpoint.rs"
  fail=1
fi

if ! rg -q 'validate_fencing_token' crates/krishiv-scheduler/src/lib.rs; then
  echo "missing validate_fencing_token in scheduler restore path"
  fail=1
fi

if rg 'write_epoch_metadata' crates/krishiv-scheduler/src/checkpoint.rs | rg -v 'validate_fencing_token' >/dev/null 2>&1; then
  :
fi

if [[ "$fail" -ne 0 ]]; then
  exit 1
fi

echo "fencing audit passed"

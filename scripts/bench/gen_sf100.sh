#!/usr/bin/env bash
# Generate TPC-H SF100 as parquet on this node.
#
# Big tables are split into parts so the distributed scan has real parallelism
# to work with: one file is one scan task, and a single 23 GB lineitem file
# would serialise the whole benchmark behind one reader.
set -euo pipefail
OUT=/data/tpch-sf100
mkdir -p "$OUT"
rm -rf "$OUT/probe"

gen() {
  local table=$1 parts=$2
  echo "[$(date -u +%H:%M:%S)] generating $table (parts=$parts)"
  if [ "$parts" -le 1 ]; then
    tpchgen-cli parquet -s 100 --tables="$table" --output-dir="$OUT"
  else
    tpchgen-cli parquet -s 100 --tables="$table" --parts="$parts" --output-dir="$OUT"
  fi
}

gen nation   1
gen region   1
gen supplier 2
gen customer 4
gen part     4
gen partsupp 8
gen orders   16
gen lineitem 32

echo "[$(date -u +%H:%M:%S)] DONE"
du -sh "$OUT"
du -sh "$OUT"/* | sort -h

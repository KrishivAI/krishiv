#!/usr/bin/env bash
# Generate a TPC-H parquet dataset at any scale factor on this node.
#
# `gen_sf100.sh` hardcodes SF100 and `/data/tpch-sf100`. Generating SF1000
# meant either editing it in place (losing the SF100 recipe) or copying it
# (two part-count tables that drift, and a drifted part count changes scan
# parallelism — which is a performance variable, not a detail). So the part
# counts live here once, as a ratio, and the scale factor is an argument.
#
# Big tables are split into parts because one file is one scan task: a single
# 200 GB lineitem file would serialise the whole benchmark behind one reader.
# The part counts below are the SF100 recipe scaled linearly, which keeps each
# part in the same ~700 MB neighbourhood at every scale.
#
# Usage: gen_tpch.sh <scale-factor> <output-dir>
set -euo pipefail

SF="${1:?usage: gen_tpch.sh <scale-factor> <output-dir>}"
OUT="${2:?usage: gen_tpch.sh <scale-factor> <output-dir>}"

# Parts per table at SF100, the shape `gen_sf100.sh` established.
# nation and region are tiny dimension tables and stay single-file; every
# runner's `table_path` treats exactly those two as single files, so changing
# it here would break path resolution rather than just parallelism.
declare -A PARTS_AT_SF100=(
  [nation]=1 [region]=1 [supplier]=2 [customer]=4
  [part]=4 [partsupp]=8 [orders]=16 [lineitem]=32
)

mkdir -p "$OUT"

# Scale the part count with the data, so part size stays constant. Guard the
# floor at 1: a scale below 100 would otherwise compute 0 parts and generate
# an empty table, which reads downstream as a query returning no rows rather
# than as a generation failure.
parts_for() {
  local table=$1 base=${PARTS_AT_SF100[$1]}
  if [ "$base" -le 1 ]; then echo 1; return; fi
  python3 -c "print(max(1, round($base * $SF / 100)))"
}

gen() {
  local table=$1
  local parts
  parts=$(parts_for "$table")
  echo "[$(date -u +%H:%M:%S)] generating $table (sf=$SF parts=$parts)"
  if [ "$parts" -le 1 ]; then
    tpchgen-cli parquet -s "$SF" --tables="$table" --output-dir="$OUT"
  else
    tpchgen-cli parquet -s "$SF" --tables="$table" --parts="$parts" \
      --output-dir="$OUT"
  fi
}

# Smallest first: a failure on lineitem (the one that takes hours) then leaves
# every dimension table already on disk to resume against.
for table in nation region supplier customer part partsupp orders lineitem; do
  gen "$table"
done

echo "[$(date -u +%H:%M:%S)] DONE"
du -sh "$OUT"
du -sh "$OUT"/* | sort -h

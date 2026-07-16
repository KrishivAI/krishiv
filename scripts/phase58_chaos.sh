#!/usr/bin/env bash
# Live Phase 58 fault-tolerance gate. The 3 workload classes x 4 fault classes
# are selected round-robin; 25 iterations cover every cell at least twice.
set -euo pipefail

NS="${PHASE58_NAMESPACE:-krishiv-phase58}"
RUNS="${PHASE58_RUNS:-2}"
ITERATIONS="${PHASE58_ITERATIONS:-25}"
TIMEOUT_S="${PHASE58_TIMEOUT_S:-180}"
DRIVER="phase58-driver"
DATA_DIR="/phase58-data"
MATRIX_ID="${PHASE58_MATRIX_ID:-$(date +%s)}"

log() { printf '[phase58] %s\n' "$*"; }
fail() { log "FAIL: $*"; exit 1; }

engine() {
  kubectl -n "$NS" exec "$DRIVER" -c engine -- "$@"
}

# A client RPC can be in flight while the active coordinator endpoint moves.
# Keep the workload process alive through that bounded control-plane gap; the
# commands used below are create-or-get/poll/replay operations with stable job
# IDs, so retrying exercises the same durable job rather than inventing a new
# one. Individual attempts are capped so one dead HTTP/Flight connection does
# not consume the workload's whole recovery budget.
retry_engine() {
  local deadline=$((SECONDS + TIMEOUT_S))
  while ! engine timeout 30 "$@"; do
    [ "$SECONDS" -lt "$deadline" ] || return 1
    sleep 2
  done
}

http() {
  local path="$1"
  kubectl -n "$NS" exec "$DRIVER" -c curl -- sh -ec \
    'curl -fsS -H "Authorization: Bearer ${COORD_TOKEN}" "http://phase58-coordinator:2002'"$path"'"'
}

http_post() {
  local path="$1" payload="$2"
  kubectl -n "$NS" exec "$DRIVER" -c curl -- sh -ec \
    'curl -fsS -H "Authorization: Bearer ${COORD_TOKEN}" -H "Content-Type: application/json" -d "$1" "http://phase58-coordinator:2002'"$path"'"' \
    phase58 "$payload"
}

http_delete() {
  local path="$1"
  kubectl -n "$NS" exec "$DRIVER" -c curl -- sh -ec \
    'curl -fsS -X DELETE -H "Authorization: Bearer ${COORD_TOKEN}" "http://phase58-coordinator:2002'"$path"'"'
}

# HTTP counterparts of retry_engine: the failover SLO allows up to 30s with no
# routable coordinator, so any workload call that can land inside that window
# must retry across it instead of treating one refused connection as a
# recovery failure. A retried submit may duplicate a job whose accept response
# was lost; the workload tracks the job id from the response it actually got,
# so a duplicate only spends slots until it terminates on its own.
retry_http() {
  local path="$1" deadline=$((SECONDS + TIMEOUT_S)) out
  while :; do
    if out="$(http "$path" 2>/dev/null)"; then printf '%s' "$out"; return 0; fi
    [ "$SECONDS" -lt "$deadline" ] || return 1
    sleep 2
  done
}

retry_http_post() {
  local path="$1" payload="$2" deadline=$((SECONDS + TIMEOUT_S)) out
  while :; do
    if out="$(http_post "$path" "$payload" 2>/dev/null)"; then printf '%s' "$out"; return 0; fi
    [ "$SECONDS" -lt "$deadline" ] || return 1
    sleep 2
  done
}

cleanup_nonterminal_jobs() {
  local job
  while IFS= read -r job; do
    [ -n "$job" ] || continue
    http_post "/api/v1/jobs/$job/cancel" '{}' >/dev/null 2>&1 || true
  done < <(http /api/v1/jobs | python3 -c '
import json, sys
for job in json.load(sys.stdin).get("jobs", []):
    if job.get("state") in {"Queued", "Running"}:
        print(job["job_id"])
')
}

leader() {
  kubectl -n "$NS" get endpointslice \
    -l kubernetes.io/service-name=phase58-coordinator \
    -o jsonpath='{range .items[*].endpoints[?(@.conditions.ready==true)]}{.targetRef.name}{"\n"}{end}'
}

assert_one_leader() {
  local leaders count
  leaders="$(leader)"
  count="$(printf '%s\n' "$leaders" | sed '/^$/d' | wc -l)"
  [ "$count" -eq 1 ] || fail "expected one routable coordinator, found $count: $leaders"
}

wait_cluster() {
  kubectl -n "$NS" wait --for=condition=Ready pod "$DRIVER" --timeout=180s >/dev/null
  local deadline=$((SECONDS + 180)) ready leaders count
  while :; do
    ready="$(kubectl -n "$NS" get pods -l component=executor \
      --field-selector=status.phase=Running \
      -o jsonpath='{range .items[*]}{.status.containerStatuses[0].ready}{"\n"}{end}' | grep -c true || true)"
    [ "$ready" -ge 2 ] && break
    [ "$SECONDS" -lt "$deadline" ] || fail "two executors were not ready within 180s"
    sleep 2
  done
  while :; do
    leaders="$(leader 2>/dev/null || true)"
    count="$(printf '%s\n' "$leaders" | sed '/^$/d' | wc -l)"
    [ "$count" -eq 1 ] && break
    [ "$SECONDS" -lt "$deadline" ] || fail "leader was not elected within 180s"
    sleep 2
  done
  deadline=$((SECONDS + 180))
  while :; do
    ready="$(http /api/v1/executors 2>/dev/null | grep -o '"state":"Healthy"' | wc -l || true)"
    [ "$ready" -ge 2 ] && break
    [ "$SECONDS" -lt "$deadline" ] || fail "two executors did not re-register within 180s"
    sleep 2
  done
}

executor_pod() {
  local index="$1"
  kubectl -n "$NS" get pods -l component=executor --field-selector=status.phase=Running \
    -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' | sed -n "$((index % 2 + 1))p"
}

inject_fault() {
  local fault="$1" index="$2" before after start pod
  case "$fault" in
    executor-kill|shuffle-kill)
      pod="$(executor_pod "$index")"
      [ -n "$pod" ] || fail "no executor pod available for $fault"
      kubectl -n "$NS" delete pod "$pod" --wait=false >/dev/null
      ;;
    coordinator-kill)
      before="$(leader)"
      [ -n "$before" ] || fail "no active coordinator before kill"
      start=$SECONDS
      kubectl -n "$NS" delete pod "$before" --wait=false >/dev/null
      while :; do
        after="$(leader 2>/dev/null || true)"
        if [ -n "$after" ] && [ "$after" != "$before" ]; then
          [ $((SECONDS - start)) -le 30 ] || fail "coordinator failover exceeded 30s"
          break
        fi
        [ $((SECONDS - start)) -le 30 ] || fail "no coordinator failover within 30s"
        sleep 1
      done
      ;;
    network-partition)
      pod="$(executor_pod "$index")"
      [ -n "$pod" ] || fail "no executor pod available for network partition"
      kubectl -n "$NS" label pod "$pod" phase58-partition=true --overwrite >/dev/null
      sleep 3
      kubectl -n "$NS" label pod "$pod" phase58-partition- >/dev/null
      ;;
    *) fail "unknown fault $fault" ;;
  esac
}

run_batch() {
  local submitted job status deadline history
  submitted="$(retry_http_post /api/v1/batch-sql/submit \
    '{"query":"SELECT user_id, COUNT(*) AS n FROM events GROUP BY user_id","table_paths":[{"table_name":"events","path":"/phase58-data/events/*.parquet"}]}')" || return 1
  job="$(printf '%s' "$submitted" | sed -n 's/.*"job_id":"\([^"]*\)".*/\1/p')"
  [ -n "$job" ] || return 1
  deadline=$((SECONDS + TIMEOUT_S))
  while [ "$SECONDS" -lt "$deadline" ]; do
    status="$(http "/api/v1/jobs/$job" 2>/dev/null || true)"
    if printf '%s' "$status" | grep -q '"state":"Succeeded"'; then
      break
    fi
    printf '%s' "$status" | grep -Eq '"state":"(Failed|Cancelled)"' && return 1
    history="$(http '/api/v1/history?limit=100' 2>/dev/null || true)"
    printf '%s' "$history" | grep -q "\"job_id\":\"$job\"" && break
    sleep 1
  done
  # The history record lands after the terminal transition; give it a bounded
  # window of its own. A job that never reaches history in that window fails
  # loudly WITH its live state, so a stuck-Running job (product bug) is
  # distinguishable from history-append lag (benign) in the harness log.
  deadline=$((SECONDS + 60))
  while :; do
    history="$(http '/api/v1/history?limit=100' 2>/dev/null || true)"
    printf '%s' "$history" | grep -q "\"job_id\":\"$job\"" && break
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "job $job absent from history; live state: $(http "/api/v1/jobs/$job" 2>/dev/null || echo unreachable)"
      return 1
    fi
    sleep 2
  done
  PHASE58_JOB_ID="$job" python3 -c '
import json, os, sys
records = json.load(sys.stdin)["records"]
record = next((item for item in records if item["job_id"] == os.environ["PHASE58_JOB_ID"]), None)
assert record is not None, "terminal job absent from history"
assert record["final_state"] == "succeeded", record
assert record["stage_count"] >= 2, record
assert record["task_count"] >= 5, record
' <<<"$history"
}

run_streaming() {
  local job="$1"
  retry_engine krishiv -c http://phase58-coordinator:2003 stream submit \
    --job-id "$job" --key-column user_id --event-time-column ts \
    --window tumbling --window-size-ms 10000 >/dev/null
  retry_engine krishiv -c http://phase58-coordinator:2003 stream push \
    --job-id "$job" --parquet "${DATA_DIR}/events.parquet" >/dev/null
  retry_engine krishiv -c http://phase58-coordinator:2003 stream poll \
    --job-id "$job"
  # A continuous job intentionally remains Running after a successful poll.
  # Tear it down so repeated matrix cells do not reserve all executor slots
  # and starve later batch/IVM work. The teardown itself may land in the
  # failover gap, so it retries like every other workload call.
  local deadline=$((SECONDS + TIMEOUT_S))
  while ! http_delete "/api/v1/continuous/$job" >/dev/null 2>&1; do
    [ "$SECONDS" -lt "$deadline" ] || return 1
    sleep 2
  done
}

run_ivm() {
  local job="$1"
  retry_engine krishiv -c http://phase58-coordinator:2002 ivm run \
    --job-id "$job" \
    --sql 'SELECT k, SUM(v) AS total FROM changes GROUP BY k' \
    --source "changes=${DATA_DIR}/changes.csv" --source-format csv \
    --sink "/tmp/${job}.ndjson" --sink-format json
  # Keep the workload alive across the injected fault, then prove the job is
  # discoverable from the newly promoted coordinator's durable registry
  # (retried: the poll may land inside the ≤30s failover gap).
  sleep 5
  retry_http /api/v1/ivm/jobs | grep -q "\"$job\""
  retry_engine krishiv -c http://phase58-coordinator:2002 ivm run \
    --job-id "$job" \
    --sql 'SELECT k, SUM(v) AS total FROM changes GROUP BY k' \
    --source "changes=${DATA_DIR}/changes.csv" --source-format csv \
    --sink "/tmp/${job}-post-fault.ndjson" --sink-format json
}

cleanup_partition() {
  kubectl -n "$NS" label pod -l component=executor phase58-partition- >/dev/null 2>&1 || true
}
trap cleanup_partition EXIT

wait_cluster
cleanup_nonterminal_jobs
wait_cluster
[ -s "${DATA_DIR}/events.parquet" ] 2>/dev/null || \
  engine test -s "${DATA_DIR}/events.parquet" || fail "shared events.parquet is missing"

workloads=(batch streaming ivm)
faults=(executor-kill coordinator-kill shuffle-kill network-partition)

for run in $(seq 1 "$RUNS"); do
  log "run=$run iterations=$ITERATIONS start"
  for iteration in $(seq 0 $((ITERATIONS - 1))); do
    workload="${workloads[$((iteration % 3))]}"
    fault="${faults[$(((iteration / 3) % 4))]}"
    job="phase58-${MATRIX_ID}-r${run}-i${iteration}-${workload}"
    log "run=$run iteration=$iteration workload=$workload fault=$fault"

    case "$workload" in
      batch) run_batch >/tmp/phase58-workload.log 2>&1 & ;;
      streaming) run_streaming "$job" >/tmp/phase58-workload.log 2>&1 & ;;
      ivm) run_ivm "$job" >/tmp/phase58-workload.log 2>&1 & ;;
    esac
    workload_pid=$!
    sleep 1
    inject_fault "$fault" "$iteration"
    if ! wait "$workload_pid"; then
      sed -n '1,160p' /tmp/phase58-workload.log >&2
      fail "workload=$workload did not recover from fault=$fault"
    fi

    wait_cluster
    assert_one_leader
    log "PASS run=$run iteration=$iteration workload=$workload fault=$fault"
  done
  log "run=$run complete"
done

# History is shared etcd state: record it, replace the active coordinator, and
# require the same terminal-job id after failover.
history="$(http /api/v1/history)"
history_job="$(printf '%s' "$history" | sed -n 's/.*\(batch-sql-[0-9][^" ]*\).*/\1/p' | head -1)"
[ -n "$history_job" ] || fail "no Phase 58 terminal job found in durable history"
inject_fault coordinator-kill 0
wait_cluster
http /api/v1/history | grep -q "$history_job" || fail "history lost after coordinator restart"

log "PASS: matrix ${RUNS}x${ITERATIONS}, failover <=30s, one leader, durable history"

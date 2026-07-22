#!/usr/bin/env bash
# Phase 59 cancel-latency cert: mid-drain cancel must free the query's
# executor-side resources within 2s (the #217/#223 exit bar).
#
# Drives a batch-SQL job large enough to have a genuine multi-second
# in-flight window (compute + disk-spool phase), cancels it mid-flight via
# the coordinator's generic job-control API, and asserts the executor's
# running_task_count reaches 0 within the bound. Also asserts the terminal
# state is Cancelled (never a resurrected Succeeded) and that the client
# is released with a clean error rather than hanging.
#
# Requires a `cancel-cert-driver` pod (engine + curl containers, see
# deploy/k8s/cancel-cert/driver.yaml) already running in the target
# namespace, pointed at a coordinator service reachable as
# "$COORDINATOR_SVC:2002" (HTTP) / "$COORDINATOR_SVC:2003" (Flight SQL).
set -euo pipefail

NS="${PHASE59_NAMESPACE:-krishiv-cert}"
DRIVER="${PHASE59_DRIVER:-cancel-cert-driver}"
COORDINATOR_SVC="${PHASE59_COORDINATOR_SVC:-cert-v2-coordinator}"
BOUND_S="${PHASE59_CANCEL_BOUND_S:-2.0}"
# ~1.5 GiB: large enough for a multi-second drain window, safely under the
# 2 GiB fallback cap (#211) and far below the ~9 GiB size that separately
# triggers #222's throughput ceiling -- this query succeeds cleanly if left
# alone, so cancellation latency is the only thing under test.
ROWS="${PHASE59_ROWS:-1500000}"
PAYLOAD_BYTES="${PHASE59_PAYLOAD_BYTES:-1000}"
# How long to let the query run before cancelling -- late enough to land
# inside the spool-write phase (the case #223 fixed), not the initial
# scheduling tick.
CANCEL_DELAY_S="${PHASE59_CANCEL_DELAY_S:-1}"

log() { printf '[phase59-cancel] %s\n' "$*"; }
fail() { log "FAIL: $*"; exit 1; }

CURL_CONNECT_TIMEOUT_S=5
CURL_MAX_TIME_S=20

http_get() {
  local path="$1"
  kubectl -n "$NS" exec "$DRIVER" -c curl -- sh -ec \
    'curl -fsS --connect-timeout '"$CURL_CONNECT_TIMEOUT_S"' -m '"$CURL_MAX_TIME_S"' -H "Authorization: Bearer ${COORD_TOKEN}" "http://'"$COORDINATOR_SVC"':2002'"$path"'"'
}

http_post_empty() {
  local path="$1"
  kubectl -n "$NS" exec "$DRIVER" -c curl -- sh -ec \
    'curl -fsS --connect-timeout '"$CURL_CONNECT_TIMEOUT_S"' -m '"$CURL_MAX_TIME_S"' -X POST -H "Authorization: Bearer ${COORD_TOKEN}" -H "Content-Length: 0" "http://'"$COORDINATOR_SVC"':2002'"$path"'"'
}

json_field() {
  # $1 = field name, reads JSON on stdin, first match only.
  grep -m1 -oE "\"$1\":[^,}]+" | sed -E 's/.*:"?([^,}"]+)"?/\1/'
}

main() {
  log "submit"
  local q="SELECT value AS id, repeat('x', $PAYLOAD_BYTES) AS payload FROM generate_series(1, $ROWS)"
  kubectl -n "$NS" exec "$DRIVER" -c engine -- \
    krishiv sql --remote -c "http://$COORDINATOR_SVC:2003" --timeout 120 --query "$q" \
    >/tmp/phase59-cancel.out 2>&1 &
  local qpid=$!

  local job_id=""
  local i
  for i in $(seq 1 60); do
    job_id="$(http_get /api/v1/jobs 2>/dev/null | tr '{' '\n' \
      | grep '"state":"Running"' | grep -m1 -oE '"job_id":"[^"]+"' | cut -d'"' -f4 || true)"
    [ -n "$job_id" ] && break
    sleep 0.2
  done
  [ -n "$job_id" ] || fail "no job reached Running within 12s; client output: $(cat /tmp/phase59-cancel.out)"
  log "job running: $job_id"

  sleep "$CANCEL_DELAY_S"
  local t0 t_now stopped rtc state
  t0=$(date +%s.%N)
  log "cancel job=$job_id"
  http_post_empty "/api/v1/jobs/$job_id/cancel" >/dev/null

  stopped=""
  while :; do
    t_now=$(date +%s.%N)
    rtc="$(http_get "/api/v1/jobs/$job_id" 2>/dev/null | json_field running_task_count || true)"
    if [ "$rtc" = "0" ]; then
      stopped=$(awk "BEGIN{printf \"%.2f\", $t_now-$t0}")
      break
    fi
    awk "BEGIN{exit !($t_now-$t0 > 10)}" && break
    sleep 0.1
  done
  [ -n "$stopped" ] || fail "running_task_count never reached 0 within 10s"
  log "executor stopped ${stopped}s after cancel"

  state="$(http_get "/api/v1/jobs/$job_id" 2>/dev/null | json_field state || true)"
  log "terminal state=$state"
  case "$state" in
    *ancel*) ;;
    *) fail "terminal state '$state' is not Cancelled" ;;
  esac

  local deadline=$(( $(date +%s) + 30 ))
  while kill -0 "$qpid" 2>/dev/null; do
    [ "$(date +%s)" -ge "$deadline" ] && fail "client still waiting 30s after cancel"
    sleep 0.2
  done
  log "client released; final output: $(tail -c 300 /tmp/phase59-cancel.out)"

  awk "BEGIN{exit !($stopped <= $BOUND_S)}" \
    && log "PASS: executor stopped in ${stopped}s (<= ${BOUND_S}s bound)" \
    || fail "${stopped}s exceeds the ${BOUND_S}s cancel bound"
}

main "$@"

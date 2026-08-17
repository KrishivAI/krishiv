#!/usr/bin/env bash
# Live verification of the distributed run-loop streaming engine.
#
# WHY THIS EXISTS: the in-repo "distributed" conformance harness
# (crates/krishiv-api/src/mode_conformance.rs) builds
# FlightExecutionHost::from_env(), which is hardcoded to embedded(). It
# therefore compares in-process DataFusion against in-process DataFusion over a
# socket, while asserting -- on the CLIENT's placement, not the server's
# backend -- that it is not doing exactly that. No unit test in this repo can
# tell you whether run-loop works on a real cluster. This can.
#
# Runs against the isolated krishiv-stream namespace (1 coordinator, 3
# executors). Never touches krishiv-cert (active soak) or krishiv (tunnel).
#
# Usage: scripts/stream_runloop_verify.sh
set -uo pipefail

NS=krishiv-stream
JOB=rloop-verify-$$
PF_PORT=${PF_PORT:-28002}
PASS=0
FAIL=0

log()  { printf '\n\033[1m== %s\033[0m\n' "$*"; }
ok()   { printf '  \033[32mPASS\033[0m %s\n' "$*"; PASS=$((PASS+1)); }
bad()  { printf '  \033[31mFAIL\033[0m %s\n' "$*"; FAIL=$((FAIL+1)); }

cleanup() { [[ -n "${PF_PID:-}" ]] && kill "$PF_PID" 2>/dev/null; }
trap cleanup EXIT

log "port-forward $NS/stream-coordinator :$PF_PORT"
kubectl -n "$NS" port-forward svc/stream-coordinator "$PF_PORT":2002 >/dev/null 2>&1 &
PF_PID=$!
for _ in $(seq 1 40); do
  curl -sf "http://127.0.0.1:$PF_PORT/healthz" >/dev/null 2>&1 && break
  sleep 0.5
done
BASE="http://127.0.0.1:$PF_PORT"

curl -sf "$BASE/healthz" >/dev/null 2>&1 \
  && ok "coordinator HTTP reachable" \
  || { bad "coordinator HTTP unreachable — aborting"; exit 1; }

log "executors registered"
EXECS=$(curl -sf "$BASE/api/v1/executors" 2>/dev/null | grep -o '"executor_id"' | wc -l)
[[ "$EXECS" -ge 3 ]] \
  && ok "$EXECS executors registered (need >= 3 for key-group parallelism)" \
  || bad "only $EXECS executors registered; run-loop at parallelism 3 cannot spread"

# ---------------------------------------------------------------------------
# 1. Register a run-loop job at parallelism 3.
#
# This is the option set that was unreachable from every Rust caller until
# 054a064: the HTTP handler has accepted it since Phase 55, but the client body
# declared only {job_id, spec}.
# ---------------------------------------------------------------------------
log "register run-loop job at parallelism 3"
REG=$(curl -sf -X POST "$BASE/api/v1/continuous-register" \
  -H 'content-type: application/json' \
  -d "{\"job_id\":\"$JOB\",
       \"mode\":\"run-loop\",
       \"parallelism\":3,
       \"spec\":{\"key_column\":\"key\",
                 \"key_column_type\":\"utf8\",
                 \"event_time_column\":\"ts\",
                 \"watermark_lag_ms\":0,
                 \"window_kind\":\"Tumbling\",
                 \"window_size_ms\":10000,
                 \"slide_ms\":null,
                 \"session_gap_ms\":null,
                 \"state_ttl_ms\":null,
                 \"agg_exprs\":[{\"kind\":\"Count\",\"input_column\":\"\",\"output_column\":\"n\"}]}}" \
  2>&1)
RC=$?
if [[ $RC -eq 0 ]]; then
  ok "registration accepted: $REG"
else
  bad "registration rejected (curl rc=$RC): $REG"
fi

# ---------------------------------------------------------------------------
# 2. THE POINT OF F0. Registration must not report success while launching
#    nothing. Before aa4c5e1 the acceptance guard compared `accepted <
#    responses.len()` -- two numbers that shrink together -- so a launch that
#    dispatched zero subtasks returned Ok. Ask the coordinator what it actually
#    built.
# ---------------------------------------------------------------------------
log "job shape as the coordinator sees it"
VIEW=$(curl -sf "$BASE/api/v1/continuous/$JOB" 2>/dev/null)
echo "  $VIEW"

echo "$VIEW" | grep -q '"model":"run-loop"' \
  && ok "coordinator recorded model=run-loop (not the cycle default)" \
  || bad "model is not run-loop — the options did not take effect"

echo "$VIEW" | grep -q '"parallelism":3' \
  && ok "coordinator recorded parallelism=3" \
  || bad "parallelism is not 3"

TASKS=$(echo "$VIEW" | grep -o '"task_count":[0-9]*' | head -1 | cut -d: -f2)
[[ "${TASKS:-0}" -eq 3 ]] \
  && ok "3 subtasks registered" \
  || bad "task_count=${TASKS:-unset}, expected 3"

# ---------------------------------------------------------------------------
# 3. Subtasks must be RUNNING on executors, not merely declared. This is the
#    live equivalent of the F0 unit test.
# ---------------------------------------------------------------------------
log "subtasks actually RUNNING, not merely declared"
RUNNING=$(echo "$VIEW" | grep -o '"running_task_count":[0-9]*' | cut -d: -f2)
[[ "${RUNNING:-0}" -eq 3 ]] \
  && ok "running_task_count=3 — every subtask launched" \
  || bad "running_task_count=${RUNNING:-unset}, expected 3 (launched nothing?)"

log "each subtask landed on a DISTINCT executor pod"
SUBTASKS=""
for POD in $(kubectl -n "$NS" get pods -l app=stream-executor -o name 2>/dev/null); do
  LINE=$(kubectl -n "$NS" logs "$POD" --tail=400 2>/dev/null \
    | grep "stream:rloop promoted run-loop started" | grep "\"$JOB\"" | tail -1)
  [[ -z "$LINE" ]] && continue
  ST=$(echo "$LINE" | grep -o '"subtask":[0-9]*' | cut -d: -f2)
  [[ -n "$ST" ]] && SUBTASKS="$SUBTASKS$ST\n"
done
DISTINCT=$(printf "$SUBTASKS" | sort -u | grep -c . || true)
[[ "${DISTINCT:-0}" -eq 3 ]] \
  && ok "3 distinct subtask indices across pods: $(printf "$SUBTASKS" | sort -u | tr '\n' ' ')" \
  || bad "only ${DISTINCT:-0} distinct subtasks landed; key-group parallelism is not real"

# Every subtask must agree on the job's parallelism, or key-group ranges and
# the exchange routing disagree — the F8 class of bug.
BADP=0
for POD in $(kubectl -n "$NS" get pods -l app=stream-executor -o name 2>/dev/null); do
  LINE=$(kubectl -n "$NS" logs "$POD" --tail=400 2>/dev/null \
    | grep "stream:rloop promoted run-loop started" | grep "\"$JOB\"" | tail -1)
  [[ -z "$LINE" ]] && continue
  echo "$LINE" | grep -q '"parallelism":3' || BADP=$((BADP+1))
done
[[ "$BADP" -eq 0 ]] \
  && ok "every subtask agrees parallelism=3" \
  || bad "$BADP subtask(s) disagree on parallelism — ranges and routing will diverge"

# ---------------------------------------------------------------------------
# 4. Teardown, so a re-run starts clean.
# ---------------------------------------------------------------------------
log "deregister"
curl -sf -X DELETE "$BASE/api/v1/continuous/$JOB" >/dev/null 2>&1 \
  && ok "deregistered" || bad "deregister failed"

printf '\n\033[1m%d passed, %d failed\033[0m\n' "$PASS" "$FAIL"
[[ "$FAIL" -eq 0 ]]

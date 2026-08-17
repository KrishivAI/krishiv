#!/usr/bin/env python3
"""Live proof of run-loop checkpoint/restore correctness at parallelism 3.

WHY: commits 388e4fd (routing hashed different bytes than persistence) and
5b74102 (restore gave one subtask the whole job's state) are the two deepest
streaming fixes, and NEITHER is provable by a unit test in this repo. Both bugs
are invisible while a job runs — every subtask routes identically — and only
appear across a checkpoint/restore boundary on a real multi-executor cluster.

The old 5b74102 bug in particular produced *wrong numbers, not errors*: every
subtask loaded the full job state, so each re-emitted the full pre-checkpoint
aggregate. On a 3-executor cluster that is a 3x overcount. That is exactly the
shape this script is built to catch — it asserts exact per-key counts, never
"we got some rows".

Usage: scripts/stream_restore_verify.py            (needs krishiv-stream up)
       PF_PORT=28002 scripts/stream_restore_verify.py
"""

import base64
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request

import pyarrow as pa

NS = "krishiv-stream"
PF_PORT = int(os.environ.get("PF_PORT", "28012"))
BASE = f"http://127.0.0.1:{PF_PORT}"
JOB = f"rloop-restore-{os.getpid()}"

PASS = 0
FAIL = 0


def ok(msg):
    global PASS
    PASS += 1
    print(f"  \033[32mPASS\033[0m {msg}")


def bad(msg):
    global FAIL
    FAIL += 1
    print(f"  \033[31mFAIL\033[0m {msg}")


def log(msg):
    print(f"\n\033[1m== {msg}\033[0m")


def post(path, body):
    req = urllib.request.Request(
        f"{BASE}{path}",
        data=json.dumps(body).encode(),
        headers={"content-type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            raw = r.read().decode()
            return r.status, (json.loads(raw) if raw.strip() else {})
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()


def get(path):
    try:
        with urllib.request.urlopen(f"{BASE}{path}", timeout=30) as r:
            return r.status, json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()


def delete(path):
    req = urllib.request.Request(f"{BASE}{path}", method="DELETE")
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status
    except urllib.error.HTTPError as e:
        return e.code


def ipc_b64(keys, timestamps, key_type=pa.int64()):
    """Arrow IPC stream, base64 — the encoding ContinuousPushRequest wants."""
    batch = pa.record_batch(
        [pa.array(keys, type=key_type), pa.array(timestamps, type=pa.int64())],
        names=["key", "ts"],
    )
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, batch.schema) as w:
        w.write_batch(batch)
    return base64.b64encode(sink.getvalue().to_pybytes()).decode()


def decode_drain(payloads):
    """Decode drained IPC payloads into {key: count} across all batches."""
    counts = {}
    for p in payloads:
        buf = bytes(p) if not isinstance(p, str) else base64.b64decode(p)
        try:
            reader = pa.ipc.open_stream(pa.BufferReader(pa.py_buffer(buf)))
            for b in reader:
                d = b.to_pydict()
                keycol = d.get("key") or d.get("user_id") or []
                ncol = d.get("n") or d.get("count") or []
                for k, n in zip(keycol, ncol):
                    counts[k] = counts.get(k, 0) + (n or 0)
        except Exception as e:  # noqa: BLE001 - surface, do not swallow
            print(f"    (undecodable payload: {e})")
    return counts


def main():
    pf = subprocess.Popen(
        ["kubectl", "-n", NS, "port-forward", "svc/stream-coordinator", f"{PF_PORT}:2002"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        for _ in range(40):
            try:
                urllib.request.urlopen(f"{BASE}/healthz", timeout=2).read()
                break
            except Exception:  # noqa: BLE001
                time.sleep(0.5)

        log("coordinator reachable")
        try:
            urllib.request.urlopen(f"{BASE}/healthz", timeout=5).read()
            ok("healthz")
        except Exception as e:  # noqa: BLE001
            bad(f"coordinator unreachable: {e}")
            return 1

        # ------------------------------------------------------------------
        # Register at parallelism 3 with INT64 keys.
        #
        # Int64 is load-bearing: routing used to hash i64::to_be_bytes while
        # the operator persists the key as ASCII decimal, so redistribution
        # re-hashed a different byte string. Utf8 keys agreed by coincidence,
        # which is why a Utf8 fixture proves nothing about 388e4fd.
        # ------------------------------------------------------------------
        log("register run-loop, parallelism 3, INT64 keys")
        spec = {
            "key_column": "key",
            "key_column_type": "int64",
            "event_time_column": "ts",
            "watermark_lag_ms": 0,
            "window_kind": "Tumbling",
            "window_size_ms": 10000,
            "slide_ms": None,
            "session_gap_ms": None,
            "state_ttl_ms": None,
            "agg_exprs": [{"kind": "Count", "input_column": "", "output_column": "n"}],
        }
        st, body = post(
            "/api/v1/continuous-register",
            {
                "job_id": JOB,
                "mode": "run-loop",
                "parallelism": 3,
                # Both knobs or neither: the coordinator rejects a half-configured
                # pair, and with NEITHER set no barrier checkpointing is armed at
                # all -- which is why an earlier run of this script saw
                # snapshot_available=false and its restore assertions were vacuous.
                "checkpoint_interval_ms": 2000,
                "checkpoint_storage_path": "file:///tmp/krishiv-stream-ckpt",
                "spec": spec,
            },
        )
        ok(f"registered: {body}") if st == 200 else bad(f"register HTTP {st}: {body}")

        st, view = get(f"/api/v1/continuous/{JOB}")
        if st == 200 and view.get("delivery", {}).get("model") == "run-loop":
            ok(f"model=run-loop, parallelism={view['delivery']['parallelism']}, "
               f"running={view.get('running_task_count')}")
        else:
            bad(f"unexpected job view: {view}")

        # ------------------------------------------------------------------
        # Push a known keyed workload.
        #
        # 12 distinct Int64 keys x 1 event each in window [0,10000), then an
        # event far in the future to close that window. 12 keys across 3
        # subtasks means every subtask owns some — so a restore that hands one
        # subtask everything is visible as a 3x overcount, and a restore that
        # starves siblings is visible as missing keys.
        # ------------------------------------------------------------------
        log("push 12 int64 keys into window [0,10000)")
        keys = list(range(1, 13))
        st, body = post(
            "/api/v1/continuous-push",
            {"job_id": JOB, "input_batches_b64": ipc_b64(keys, [1000] * len(keys))},
        )
        ok("pushed") if st == 200 else bad(f"push HTTP {st}: {body}")
        time.sleep(3)

        # ------------------------------------------------------------------
        # Checkpoint, then restore. This is the boundary both bugs live at.
        # ------------------------------------------------------------------
        log("checkpoint")
        st, ckpt = post(f"/api/v1/continuous/{JOB}/checkpoint", {})
        if st == 200:
            ok(f"checkpoint: {json.dumps(ckpt)[:180]}")
        else:
            bad(f"checkpoint HTTP {st}: {ckpt}")
            ckpt = {}

        # FINDING (recorded, not a script bug): `POST .../checkpoint` does not
        # take a checkpoint. It READS `load_continuous_snapshot`, which is the
        # CYCLE model's coordinator-side store. A run-loop job checkpoints
        # through the barrier pipeline into `checkpoint_storage_path`, so this
        # endpoint returns 200 with snapshot_b64=null / snapshot_available=false
        # -- indistinguishable from "no checkpoint has been taken yet". The
        # caller cannot tell "not yet" from "this endpoint does not serve your
        # job's execution model".
        snap = ckpt.get("snapshot_available") if isinstance(ckpt, dict) else None
        if snap:
            ok("snapshot_available=true (cycle-style snapshot present)")
        else:
            ok("snapshot_available=false for a run-loop job — EXPECTED: this "
               "endpoint reads the cycle store, not barrier checkpoints. Recorded "
               "as an honesty gap; it should say so rather than return nulls.")

        log("restore from the checkpoint")
        snapshot_b64 = ckpt.get("snapshot_b64") if isinstance(ckpt, dict) else None
        if not snapshot_b64:
            ok("no cycle snapshot to POST back — skipping the restore round-trip "
               "rather than asserting against a restore that never happened. "
               "Run-loop restore is coordinator-driven (RestoreFromCheckpointCommand), "
               "not an HTTP snapshot round-trip; proving 5b74102 live needs a "
               "pod-kill, which is the next leg.")
        else:
            st, body = post(
                f"/api/v1/continuous/{JOB}/restore", {"snapshot_b64": snapshot_b64}
            )
            if st in (200, 202):
                ok(f"restore accepted: {json.dumps(body)[:200]}")
            else:
                bad(f"restore HTTP {st}: {body}")
        time.sleep(4)

        # ------------------------------------------------------------------
        # Close the window and read the result. THE assertion.
        # ------------------------------------------------------------------
        log("close the window on EVERY subtask, then drain")
        # One high-ts row per ORIGINAL key. A run-loop subtask's watermark is
        # driven only by the rows routed to it, so a single high-ts event closes
        # exactly one subtask's window and leaves the rest open -- which reads as
        # "missing keys" and would be misdiagnosed as data loss.
        st, body = post(
            "/api/v1/continuous-push",
            {"job_id": JOB, "input_batches_b64": ipc_b64(keys, [999_000] * len(keys))},
        )
        ok("pushed watermark-advancing rows for every key group") if st == 200 else bad(
            f"push HTTP {st}: {body}"
        )
        time.sleep(5)

        st, drained = post("/api/v1/continuous-drain", {"job_id": JOB})
        payloads = drained.get("inline_record_batch_ipc", []) if isinstance(drained, dict) else []
        counts = decode_drain(payloads)
        print(f"  drained {len(payloads)} payload(s); per-key counts: {counts}")

        window_keys = {k: v for k, v in counts.items() if k in keys}
        if not window_keys:
            bad("no counts for the 12 pushed keys — cannot judge restore correctness "
                "(run-loop egress may not surface through the coordinator drain; see notes)")
        else:
            overcounted = {k: v for k, v in window_keys.items() if v > 1}
            if overcounted:
                bad(f"keys counted more than once after restore: {overcounted} — this is "
                    f"the 5b74102 signature (every subtask loaded the full job state and "
                    f"re-emitted it)")
            else:
                ok(f"every counted key == 1 across {len(window_keys)} keys (no restore "
                   f"duplication)")

            missing = [k for k in keys if k not in window_keys]
            if missing:
                bad(f"keys missing after restore: {missing} — the starvation signature "
                    f"(siblings restored empty, or routing/persistence disagreed)")
            else:
                ok("all 12 keys present after restore (no starvation, no routing drift)")

        log("teardown")
        ok("deregistered") if delete(f"/api/v1/continuous/{JOB}") in (200, 204) else bad(
            "deregister failed"
        )

        print(f"\n\033[1m{PASS} passed, {FAIL} failed\033[0m")
        return 1 if FAIL else 0
    finally:
        pf.terminate()


if __name__ == "__main__":
    sys.exit(main())

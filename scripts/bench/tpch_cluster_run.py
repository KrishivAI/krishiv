#!/usr/bin/env python3
"""Run the TPC-H corpus against a real Krishiv coordinator and record timings.

Why this exists rather than a Criterion bench: `benches/tpch_distributed.rs`
uses `InProcessCluster`, which is one coordinator and one in-process executor
in a single process. Its own docstring says a true multi-executor topology
cannot be wired from that crate. So it measures the distributed *submission
path*, not distribution. This script talks to a real coordinator over HTTP with
real executors on separate machines, which is the only way to get a number that
means "N nodes".

The queries come from the Rust corpus via `tpch_corpus` (one source of truth —
see that binary's header), so this runner and the single-node runner execute
byte-identical SQL.

Tables are passed as `table_paths`, NOT `tables`: the latter inlines each
parquet file as base64 Arrow IPC inside the request body, which is fine for a
smoke test and impossible at SF100 (37 GB in one HTTP request). `table_paths`
requires every executor to resolve the path — a shared filesystem, or an
object-store URI now that the stage builder registers object stores.

Usage:
  scripts/bench/tpch_cluster_run.py \
      --coordinator http://213.199.60.184:31902 \
      --data s3://krishiv-bench/tpch/sf100 \
      --scale 100 --label distributed-3x \
      --out benchmarks/tpch-sf100-distributed.json
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request

# Tables that live in a per-table subdirectory (generated with --parts). A
# directory path registers as a multi-file dataset, which is what gives the
# scan more than one partition to spread across executors.
# Measured 2026-07-27: the pod network these executors shuffle over runs at
# ~11 MiB/s (flannel VXLAN, three separate VPS hosts). q8/q9 shuffle ~36 GiB,
# so ~55 minutes is their *honest* floor, not a hang. At 3600 s they reported
# `timeout` — which hides whether the engine was healthy behind a number that
# only describes the link. Give an honest-but-slow query room to finish and
# report its real elapsed time; a genuine hang still shows up, just later.
DEFAULT_TIMEOUT_S = 7200

# Compiling the corpus binary is setup, not measurement. Bound it on its own so
# a stalled build reports as a build failure instead of eating the run.
CORPUS_BUILD_TIMEOUT_S = 1800


def _abridge(message: str, head: int = 120, tail: int = 320) -> str:
    """Shorten a failure message while keeping the part that says why.

    Truncating from the front is worse than useless here. An engine failure
    reads `executor failed fragment '<serialised plan>': <cause>`, and the
    plan is kilobytes of base64 — so `message[:200]` kept only the plan and
    discarded the cause, leaving a report that said a query failed but not
    why. That is the same defect the engine had in its own failure formatting;
    fixing it there and repeating it in the harness wasted a benchmark run.

    Keep both ends: the head identifies the fragment, the tail carries the
    error.
    """
    message = message.strip()
    if len(message) <= head + tail:
        return message
    elided = len(message) - head - tail
    return f"{message[:head]} …[{elided} chars elided]… {message[-tail:]}"


def load_corpus(corpus_json: str | None) -> list[dict]:
    """Return the 22-query corpus, from a file or by running the Rust binary.

    The cargo path is silent and can take many minutes on a cold target dir —
    it compiles the workspace in release. That cost a benchmark run: a
    `--only q6` invocation under a 600 s wrapper spent the whole budget in
    rustc and was read as a q6 hang, when q6 on the cluster takes 35 s. So
    announce the compile before it starts and bound it separately from the
    benchmark's own budget, and point at `--corpus-json` for repeat runs.
    """
    if corpus_json:
        with open(corpus_json, encoding="utf-8") as handle:
            return json.load(handle)["queries"]
    print(
        "# building tpch_corpus (cargo, release) — a cold target dir takes "
        "minutes; pass --corpus-json to skip this on repeat runs",
        file=sys.stderr,
        flush=True,
    )
    try:
        proc = subprocess.run(
            [
                "cargo", "run", "-q", "-p", "krishiv-bench",
                "--bin", "tpch_corpus", "--release",
            ],
            capture_output=True,
            text=True,
            check=False,
            timeout=CORPUS_BUILD_TIMEOUT_S,
        )
    except subprocess.TimeoutExpired:
        raise SystemExit(
            f"the corpus build did not finish in {CORPUS_BUILD_TIMEOUT_S}s. This is a "
            "build problem, not a benchmark result — build it once with "
            "`cargo run -q -p krishiv-bench --bin tpch_corpus --release > corpus.json` "
            "and rerun with --corpus-json corpus.json."
        ) from None
    if proc.returncode != 0:
        raise SystemExit(
            f"cannot load the query corpus (cargo exit {proc.returncode}):\n{proc.stderr}"
        )
    return json.loads(proc.stdout)["queries"]


# A coordinator restart, a rolling update, or a momentary connection refusal
# is not a query result. Without a retry the benchmark records the blip as the
# query's outcome: q21 and q22 were both reported failed on 2026-07-25 for
# "Connection refused" while the engine was fine, which silently turns an
# infrastructure hiccup into a fabricated engine failure.
_TRANSIENT_RETRIES = 5
_TRANSIENT_BACKOFF_S = 3.0


def _is_timeout(err: Exception) -> bool:
    """Whether `err` is a read/connect timeout rather than a refused socket.

    urllib wraps a socket timeout in `URLError`, so the type alone does not
    distinguish "the server never answered" from "nothing was listening".
    """
    if isinstance(err, TimeoutError):
        return True
    reason = getattr(err, "reason", None)
    if isinstance(reason, TimeoutError):
        return True
    return "timed out" in str(err).lower()


def _with_retry(operation, what: str):
    """Run `operation`, retrying transient transport errors with backoff.

    Only connection-level failures retry. An HTTP error carrying a real
    response (a 4xx from the coordinator rejecting the query) is the answer,
    not a blip, and is raised immediately.
    """
    last: Exception | None = None
    for attempt in range(_TRANSIENT_RETRIES):
        try:
            return operation()
        except urllib.error.HTTPError:
            raise
        except (urllib.error.URLError, TimeoutError, ConnectionError, OSError) as err:
            last = err
            # A *timeout* on a submit is not a transport blip. `submit` plans
            # the query synchronously, so a timeout means the coordinator is
            # still planning — and retrying starts a second planning attempt
            # for the same query alongside the first. q15 did exactly that:
            # five overlapping plans of one SF100 query, each logged as
            # `planning distributed stages`, on a coordinator that was already
            # the reason the first one was slow. Fail fast instead; the caller
            # records `submit_failed`, which is the honest result.
            if _is_timeout(err) and what == "POST":
                raise RuntimeError(
                    f"{what} timed out; not retried — a submit timeout means the "
                    f"coordinator is still planning, and retrying would run a "
                    f"second plan of the same query alongside the first: {err}"
                ) from err
            if attempt + 1 < _TRANSIENT_RETRIES:
                delay = _TRANSIENT_BACKOFF_S * (2**attempt)
                print(
                    f"#   transient {what} error ({err}); "
                    f"retry {attempt + 1}/{_TRANSIENT_RETRIES - 1} in {delay:.0f}s",
                    flush=True,
                )
                time.sleep(delay)
    raise RuntimeError(f"{what} failed after {_TRANSIENT_RETRIES} attempts: {last}")


def post(url: str, body: dict, token: str | None, timeout: float) -> dict:
    def once() -> dict:
        data = json.dumps(body).encode("utf-8")
        req = urllib.request.Request(url, data=data, method="POST")
        req.add_header("Content-Type", "application/json")
        if token:
            req.add_header("Authorization", f"Bearer {token}")
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))

    return _with_retry(once, "POST")


def get(url: str, token: str | None, timeout: float) -> dict:
    def once() -> dict:
        req = urllib.request.Request(url, method="GET")
        if token:
            req.add_header("Authorization", f"Bearer {token}")
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))

    return _with_retry(once, "GET")


def table_path(data_root: str, table: str, single_file: set[str]) -> str:
    """Resolve a table to its path under `data_root`.

    The layout is not uniform and guessing wrong is not a slow query, it is a
    "table not found" on nine of the twenty-two. `tpchgen-cli --parts N` writes
    a directory `<root>/<table>/<table>.<n>.parquet`, but a table generated
    without `--parts` (nation and region, which are 25 and 5 rows) lands as a
    single `<root>/<table>.parquet`. Directories keep the trailing slash so an
    object store treats the prefix as a dataset rather than a key.
    """
    root = data_root.rstrip("/")
    if table in single_file:
        return f"{root}/{table}.parquet"
    return f"{root}/{table}/"


def run_query(
    coordinator: str,
    query: dict,
    data_root: str,
    token: str | None,
    timeout_s: float,
    poll_interval_s: float,
    single_file: set[str],
) -> dict:
    body = {
        "query": query["sql"],
        "table_paths": [
            {"table_name": table, "path": table_path(data_root, table, single_file)}
            for table in query["tables"]
        ],
    }
    started = time.monotonic()
    try:
        submitted = post(
            f"{coordinator}/api/v1/batch-sql/submit", body, token, timeout=60
        )
    except urllib.error.HTTPError as error:
        detail = error.read().decode("utf-8", "replace")[:400]
        return {
            "id": query["id"],
            "name": query["name"],
            "status": "submit_failed",
            "error": f"HTTP {error.code}: {detail}",
            "elapsed_s": time.monotonic() - started,
        }
    except Exception as error:  # noqa: BLE001 - the message is the finding
        return {
            "id": query["id"],
            "name": query["name"],
            "status": "submit_failed",
            "error": str(error),
            "elapsed_s": time.monotonic() - started,
        }

    job_id = submitted["job_id"]
    deadline = started + timeout_s
    while True:
        try:
            poll = get(
                f"{coordinator}/api/v1/batch-sql/{job_id}", token, timeout=60
            )
        except Exception as error:  # noqa: BLE001
            return {
                "id": query["id"],
                "name": query["name"],
                "job_id": job_id,
                "status": "poll_failed",
                "error": str(error),
                "elapsed_s": time.monotonic() - started,
            }
        state = poll.get("state", "")
        if state == "Succeeded":
            elapsed = time.monotonic() - started
            # Recorded, not assumed: stage_count == 1 and task_count == 1
            # means the query ran on ONE executor. A timing without this is
            # a number whose topology you cannot check afterwards.
            stage_count = poll.get("stage_count")
            task_count = poll.get("task_count")
            # A query that declines to stage still returns the right answer, so
            # it reports as a clean pass — which is how q22 hid the fact that
            # `ScalarSubqueryExpr` cannot round-trip through the fragment
            # encoding and the whole query silently fell back to a single task.
            # "Correct but not distributed" is a finding, not a success; the
            # sweep exists to catch exactly this.
            distributed = bool(task_count and task_count > 1)
            return {
                "id": query["id"],
                "name": query["name"],
                "job_id": job_id,
                "status": "ok",
                "elapsed_s": elapsed,
                "result_batches": len(poll.get("inline_record_batch_ipc", [])),
                "stage_count": stage_count,
                "task_count": task_count,
                "distributed": distributed,
            }
        if state in ("Failed", "Cancelled"):
            return {
                "id": query["id"],
                "name": query["name"],
                "job_id": job_id,
                "status": state.lower(),
                # The coordinator's message is the whole point of a failure
                # row: a bare "Failed" cannot be acted on.
                "error": poll.get("error") or "(no error message returned)",
                "elapsed_s": time.monotonic() - started,
            }
        if time.monotonic() > deadline:
            return {
                "id": query["id"],
                "name": query["name"],
                "job_id": job_id,
                "status": "timeout",
                "error": f"still {state} after {timeout_s:.0f}s",
                "elapsed_s": time.monotonic() - started,
            }
        time.sleep(poll_interval_s)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--coordinator", required=True, help="coordinator HTTP base URL")
    parser.add_argument("--data", required=True, help="dataset root (path or s3:// URI)")
    parser.add_argument("--scale", type=int, required=True, help="TPC-H scale factor")
    parser.add_argument("--label", required=True, help="topology label for the record")
    parser.add_argument("--out", help="write JSON results here (default: stdout only)")
    parser.add_argument("--corpus-json", help="pre-dumped corpus instead of running cargo")
    parser.add_argument("--only", help="comma-separated query ids (default: all 22)")
    parser.add_argument(
        "--timeout", type=float, default=DEFAULT_TIMEOUT_S, help="per-query seconds"
    )
    parser.add_argument("--poll-interval", type=float, default=1.0)
    parser.add_argument(
        "--single-file-tables",
        default="nation,region",
        help="tables stored as <root>/<table>.parquet rather than a directory",
    )
    args = parser.parse_args()
    single_file = {t.strip() for t in args.single_file_tables.split(",") if t.strip()}

    token = os.environ.get("KRISHIV_COORDINATOR_BEARER_TOKEN")
    corpus = load_corpus(args.corpus_json)
    if args.only:
        # Order matters, so `wanted` is a LIST and the corpus is rebuilt in its
        # order. This used to be a set intersection, which silently kept the
        # corpus's own q1..q22 ordering: asking for `--only q10,q11,q16,...` to
        # front-load the queries under investigation ran q1 first and the
        # request looked honoured because every id asked for did eventually run.
        # When a sweep is being used to chase specific failures, running them
        # last is the whole cost.
        wanted = [q.strip() for q in args.only.split(",") if q.strip()]
        by_id = {q["id"]: q for q in corpus}
        missing = sorted({q for q in wanted if q not in by_id})
        if missing:
            raise SystemExit(f"unknown query ids: {missing}")
        seen: set[str] = set()
        corpus = [
            by_id[q] for q in wanted if not (q in seen or seen.add(q))
        ]

    print(
        f"# {args.label}: {len(corpus)} queries at SF{args.scale} "
        f"against {args.coordinator}\n"
        f"# data root {args.data}",
        flush=True,
    )

    results = []
    for query in corpus:
        outcome = run_query(
            args.coordinator,
            query,
            args.data,
            token,
            args.timeout,
            args.poll_interval,
            single_file,
        )
        results.append(outcome)
        if outcome["status"] == "ok":
            # A single-task pass gets its own marker. It is still a correct
            # answer, so it stays in the ok column, but it must never look
            # identical to a query that actually used the cluster.
            marker = "ok  " if outcome.get("distributed") else "ok/1"
            print(
                f"{marker} {outcome['id']:>3} {outcome['name']:<34} "
                f"{outcome['elapsed_s']:>9.2f} s  "
                f"stages={outcome.get('stage_count')} tasks={outcome.get('task_count')}"
                f"{'' if outcome.get('distributed') else '   << NOT DISTRIBUTED'}",
                flush=True,
            )
        else:
            print(
                f"FAIL {outcome['id']:>3} {outcome['name']:<34} "
                f"{outcome['status']}: {_abridge(outcome.get('error', ''))}",
                flush=True,
            )

    ok = [r for r in results if r["status"] == "ok"]
    single_task = [r for r in ok if not r.get("distributed")]
    total = sum(r["elapsed_s"] for r in ok)
    print(
        f"\n{len(ok)} / {len(results)} queries succeeded; "
        f"total {total:.1f} s across the successful ones"
    )
    if single_task:
        # Surfaced at the bottom as well as inline, because the summary line is
        # what gets pasted into a status update and "17/22" alone would carry
        # none of this.
        ids = ", ".join(r["id"] for r in single_task)
        print(
            f"WARNING: {len(single_task)} of those ran as a SINGLE TASK and did "
            f"not use the cluster: {ids}\n"
            f"         Correct answers, but the distributed path declined to "
            f"stage them — investigate before treating this run as a pass."
        )

    record = {
        "label": args.label,
        "scale_factor": args.scale,
        "coordinator": args.coordinator,
        "data_root": args.data,
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "succeeded": len(ok),
        "attempted": len(results),
        "total_elapsed_s": total,
        "queries": results,
    }
    if args.out:
        os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
        with open(args.out, "w", encoding="utf-8") as handle:
            json.dump(record, handle, indent=2)
        print(f"wrote {args.out}")

    # Exit non-zero if any query failed: a partial run must not read as a pass.
    return 0 if len(ok) == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())

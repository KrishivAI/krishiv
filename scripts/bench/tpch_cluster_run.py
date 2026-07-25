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
DEFAULT_TIMEOUT_S = 3600


def load_corpus(corpus_json: str | None) -> list[dict]:
    """Return the 22-query corpus, from a file or by running the Rust binary."""
    if corpus_json:
        with open(corpus_json, encoding="utf-8") as handle:
            return json.load(handle)["queries"]
    proc = subprocess.run(
        [
            "cargo", "run", "-q", "-p", "krishiv-bench",
            "--bin", "tpch_corpus", "--release",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        raise SystemExit(
            f"cannot load the query corpus (cargo exit {proc.returncode}):\n{proc.stderr}"
        )
    return json.loads(proc.stdout)["queries"]


def post(url: str, body: dict, token: str | None, timeout: float) -> dict:
    data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("Content-Type", "application/json")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def get(url: str, token: str | None, timeout: float) -> dict:
    req = urllib.request.Request(url, method="GET")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


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
            return {
                "id": query["id"],
                "name": query["name"],
                "job_id": job_id,
                "status": "ok",
                "elapsed_s": elapsed,
                "result_batches": len(poll.get("inline_record_batch_ipc", [])),
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
        wanted = {q.strip() for q in args.only.split(",") if q.strip()}
        corpus = [q for q in corpus if q["id"] in wanted]
        missing = wanted - {q["id"] for q in corpus}
        if missing:
            raise SystemExit(f"unknown query ids: {sorted(missing)}")

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
            print(
                f"ok   {outcome['id']:>3} {outcome['name']:<34} "
                f"{outcome['elapsed_s']:>9.2f} s",
                flush=True,
            )
        else:
            print(
                f"FAIL {outcome['id']:>3} {outcome['name']:<34} "
                f"{outcome['status']}: {outcome.get('error', '')[:200]}",
                flush=True,
            )

    ok = [r for r in results if r["status"] == "ok"]
    total = sum(r["elapsed_s"] for r in ok)
    print(
        f"\n{len(ok)} / {len(results)} queries succeeded; "
        f"total {total:.1f} s across the successful ones"
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

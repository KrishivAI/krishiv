"""Live cluster proof of the #197 registry-sink batch export.

Committed rather than left in a scratch directory because the claim it proves —
"any registered connector sink is reachable from a distributed batch job" — is
published in docs/reference/connector-reachability-matrix.md, and the first
version of that claim was wrong (csv had no sink driver at all). This is how it
gets re-checked against a real cluster.

Verified 2026-07-25 on the 3-node k3s cluster (namespace krishiv-apitest, image
fast-b907ae23): coordinator on s2, the CSV landed on an executor on s1 — a real
cross-node dispatch, not a loopback. Run it right after a rollout and the first
attempt may fail while executors are still re-registering; retry once the
executor logs stop showing "registration failed; retrying".


Drives the coordinator's Flight `batch_sql_sink` action with a
`registry-sink:<kind>|<base64-json>` contract, which only the executor's
registry dispatch can satisfy. A CSV sink is used deliberately: it has no
dedicated OutputContractDescriptor variant, so success means the write really
went through `default_registry().open_sink()` on a remote executor.
"""

import base64
import json
import os
import sys

import pyarrow.flight as flight

FLIGHT = os.environ.get("KRISHIV_FLIGHT_URL", "grpc://213.199.60.184:31903")
OUT = "/tmp/registry_sink_proof.csv"


def contract(kind: str, props: dict[str, str]) -> str:
    payload = json.dumps({"name": "k8s-proof", "properties": props}).encode()
    return f"registry-sink:{kind}|{base64.standard_b64encode(payload).decode()}"


def main() -> int:
    client = flight.FlightClient(FLIGHT)
    # The action enum is internally tagged (`#[serde(tag = "kind")]`), so the
    # variant name is a `kind` field alongside the body's own fields.
    body = {
        "kind": "BatchSqlSink",
        "query": "SELECT n, n * 10 AS scaled FROM (VALUES (1), (2), (3)) AS t(n)",
        "tables": [],
        "sink_contract": contract("csv", {"path": OUT, "has_header": "true"}),
    }
    action = flight.Action(
        "krishiv.v1.batch_sql_sink", json.dumps(body).encode("utf-8")
    )
    try:
        results = [r.body.to_pybytes().decode("utf-8", "replace") for r in client.do_action(action)]
    except Exception as error:  # noqa: BLE001 - surfacing the server message is the point
        print(f"FAIL do_action: {error}")
        return 1
    print(f"OK do_action -> {results}")
    print(f"expect the CSV at {OUT} on whichever executor ran the task")
    return 0


if __name__ == "__main__":
    sys.exit(main())

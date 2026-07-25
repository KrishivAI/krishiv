# Phase 62 honesty re-audit — 2026-07-25

The GA gate's honesty deliverable: re-check the engine's published claims
against what the code actually does, fix every divergence found, and where
possible replace the human check with a CI gate so the same claim cannot drift
again.

Scope of this pass: connector reachability, compatibility promises, optional
feature availability, delivery guarantees. Not in scope: the 7-day chaos soak
and the fresh-env cluster standup (both still open, see #182).

## Findings

Five divergences, all fixed in this pass. Each one was a *published* claim, not
an internal note.

### 1. `docs/COMPATIBILITY.md` promised checkpoint metadata v2; the engine writes v3

`CheckpointMetadata::VERSION` is 3 and `MIN_SUPPORTED_VERSION` is 1, so readers
accept v1–v3. The document said "Writers emit v2; readers accept supported
v1-v2 metadata." An operator planning an upgrade would have read the wrong
compatibility window.

**Fixed** and made unfalsifiable: `scripts/compatibility_gate.py` (CI job
`compat-gate`) now parses the constants out of the source and fails if the
document disagrees, and additionally requires each versioned promise to name a
test function that exists. Negative-tested: reverting the document to "v2" fails
the gate. The same pass added the two promises that were missing entirely —
wire protocol and metadata store.

### 2. The reachability matrix's `distributed_job` column was stale

`e1b68ab9` (2026-07-22) added the batch `registry-sink:` export contract, which
reaches every registered sink driver. The matrix, generated 2026-07-21, still
said csv/avro/delta/hudi/elasticsearch/cassandra/hbase/jdbc-sink were `no`
there.

**Fixed.** Reachability is now uniform across all four surfaces; the notes carry
the distinction that actually matters — at-least-once batch export versus the
checkpoint-aligned two-phase-commit Iceberg/Kafka sinks. The certification
matrix gained the corresponding data-movement row.

### 3. `csv` sink was claimed reachable everywhere and had no driver at all

`ConnectorKind::Csv` had a registered *source* driver and no sink driver, so
every registry-generic surface failed with `no sink driver registered for kind
'csv'`. The claim held only for the ad-hoc SQL job path, which has its own
hand-written CSV writer. Confirmed live on the 3-node k3s cluster: a
`registry-sink:csv` batch export failed on a real executor with exactly that
message.

**Fixed** by adding `CsvSinkDriver`, and guarded by
`sql_ddl_yes_cells_have_a_registered_driver`, which cross-checks every matrix
row against `default_registry()`. `sql_ddl` is purely registry-generic, so a
`yes` there is a falsifiable statement about the registry — the test now
falsifies it. Rows whose kind is not compiled into the running build are
skipped, and the skip set is asserted to be a strict subset so an empty check
cannot pass silently.

### 4. Three connector features did not compile, while being listed as reachable

`elasticsearch`, `cassandra` and `pulsar-source` had each rotted against a
dependency-API change. All three were on the `--exclude-features` list of `just
lint-features` — the only job that compiles them — so nothing caught it, and
`full` does not include them either. A connector could therefore appear as
`yes` in the reachability matrix and be unshippable.

**Fixed** all three, emptied the quarantine list, and removed the
`--exclude-features` escape hatch so the per-feature guard covers every optional
feature. `docs/feature-graph.md` now records what rotted and why, and says
plainly that a feature which cannot be kept building should be deleted, not
hidden from the guard.

### 5. `WriteTarget::Database` claimed a driver was missing that was registered

The database write path errored "requires a registered database driver" long
after `JdbcSinkDriver` was registered. **Fixed** — it routes through the
registry now.

## Claims checked and found accurate

- `README.md` connector list ("Kafka, S3, Parquet, Iceberg (Delta and Hudi
  experimental)") matches the maturity labels in `ConnectorKind::default_maturity`.
- `docs/architecture.md`'s delivery-guarantee language is properly conditional
  ("a transactional sink does not make a non-rewindable source exactly-once").
- The certification matrix's `certified` cells all carry linked evidence — the
  generator's own test enforces it.
- No connector kind claims `certified` maturity; every row is preview or
  experimental, which is consistent with certification being
  combination-specific.

## What this pass did not resolve

- The 7-day chaos soak (Phase 62 deliverable) has not run. The long-lived
  `cert-soak-driver` pod is a liveness loop on a stale image and does not count.
- The "outsider stands up a 3-node cluster from docs alone" readiness check has
  not been executed.
- TPC-H SF10/SF100 are still one-off `BENCHMARKING.md` entries rather than
  entries in the nightly-gated `benchmarks/results.jsonl` history.

## Standing rule this pass establishes

Every published claim that can be expressed as a comparison against the source
should be a CI gate, not a document review. Three now are: the connector
reachability matrix (golden file + registry cross-check), the certification
matrix (evidence-required test), and the compatibility promises
(`compatibility_gate.py`). The recurring failure mode found here is not
dishonesty — it is a true statement that stopped being true and nothing was
watching.

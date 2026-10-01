---
name: debugging-krishiv
description: Use when a Krishiv test fails, hangs, OOMs or returns a different answer after a change — especially after a DataFusion/arrow bump, in staged/distributed execution, or in a memory-pool/spill path — before proposing any fix.
---

# Debugging Krishiv

**REQUIRED BACKGROUND:** superpowers:systematic-debugging — this is the
repo-specific layer on top of it.

## Isolate

1. One test, alone, under a wall clock:
   `timeout 600 cargo test -p <crate> --lib <full::path> -- --exact --nocapture`
   (prefix filters need no `--exact`; `--exact` with a short name runs 0 tests
   and reports ok — check "N passed").
2. Hang vs slow: load near zero with a test "running for over 60 seconds" is
   a hang. The killed process leaves no panic text; the single run does.
3. Split the path. Staged vs direct (`run_staged` vs `direct` in
   distributed_plan tests); with vs without broadcast; reorder on vs off;
   pool bounded vs unbounded. The passing/failing matrix names the subsystem.

## Find the mechanism

- After a dependency bump: diff the two versions' source in
  `~/.cargo/registry/src/*/<crate>-<old>/` vs `-<new>/` at the function the
  test reaches (`grep -n` the symbol in both, `diff <(sed -n …) <(sed -n …)`).
- Print the plan once: `EXPLAIN` through the same session the test builds; a
  temporary `eprintln!` test in the module is fine and is deleted with the fix.
- A barrier or `OnceAsync` that counts partitions is suspect in any fragment:
  a fragment is one partition of a plan whose others run elsewhere.
- Memory pools: each operator's reservation × concurrent partitions vs the
  pool; the error names the consumer (`ExternalSorterMerge[6]`) and the
  remaining bytes.

## Prove it before fixing

State the hypothesis in one sentence with the mechanism. Make the smallest
change that the mechanism predicts will pass; run the isolated test. If it
passes, that is evidence for the mechanism, not yet the fix — the fix is the
change you would defend in the module doc, with the mechanism written next to
it, and a test that fails without it (a timeout guard counts for a hang).

## Write it down

The module doc or the fix's comment carries the mechanism and the measurement
(`docs/engineering-log/status.md` gets the summary; see logging-work). "DF 55
changed X" is not a mechanism; "DF 55 finds the consumer by expression id,
which a proto round trip preserves, so the fragment arms a barrier it is one
partition of" is.

## Red flags

| Thought | Reality |
|---|---|
| "Disable the option and the test passes, ship it" | A switch that passes is a hypothesis. Name the mechanism or keep looking. |
| "It's flaky, rerun" | Nothing here has turned out flaky; it turned out contended, hung, or OOM-killed. Read `dmesg`/load.log. |
| "The dependency changed, nothing to understand" | The diff of the two versions is 40 lines away. Read it. |

---
name: gating
description: Use before claiming any Krishiv change is done, tested, green or ready to commit — and whenever a cargo/just command will run longer than a minute, a test run must survive the session, or a measurement is in progress on the same machine.
---

# Gating

A claim about code is a claim about a log you have read. The gate is the only
source of "green"; a tool's exit code, a truncated stream, or a run you
started but did not read is not.

## The run

Detach it. A Bash tool call dies at its timeout, a `run_in_background` job
dies with the session, and both have killed hour-long gates here. Use
`bash skills/gating/gate.sh <label> <command…>`; it writes `<label>.log` and
`<label>.done` (`EXIT=<code>`) in the scratchpad and survives everything.
Then watch `<label>.done` with a Monitor whose filter names every terminal
state: `EXIT=|^error|FAILED|panicked|timeout`.

## What to run

| Stage | Command | Must be |
|---|---|---|
| format | `cargo fmt --all --check` | 0 |
| lint | `just lint` (clippy `--workspace --all-targets -D warnings`, then the `iceberg-datafusion,local-catalog` and `etcd` feature arms — read the whole recipe) | 0 |
| lib tests | `just test` (`--no-fail-fast`, excludes krishiv-python/chaos) | 0 |
| integration | `just test-integration` | 0 |
| rocksdb crates | prefix `CXXFLAGS="-include cstdint"` | — |

Only `just test`/`just lint` know the exclusions and `--no-fail-fast`; a
hand-written `cargo test --workspace --lib` stops at the first failing crate
and reports nothing about the rest.

## Reading the result

- Exit code from the `.done` file, never from memory of the command.
- `grep -c "^test result: ok"` and `grep "^test result: FAILED"` — both.
- For a named test: `grep "<test name> ... ok"`; a green suite that filtered
  the test out is not evidence.
- A test "running for over 60 seconds" with load near zero is a hang. Kill the
  gate, run that test alone under `timeout`, then debug (see debugging-krishiv).

## One machine

- Never start a compile while a benchmark or timing run is on the box.
- Never start a second cargo command while one holds the lock; it queues
  silently, and a foreground tool call then times out. Chain it in the script.
- Stop a detached gate with `kill -- -$(cat <scratch>/<label>.pid)` — the minus
  targets the process group, so cargo/rustc children die with it (`gate.sh`
  writes it; the launcher itself has already exited, so a `pkill` on its name
  finds nothing) then `killall -q cargo rustc cargo-clippy`, and confirm with
  `pgrep -c "cargo|rustc"`. A `pkill -f` pattern must use the `[b]racket`
  form or it kills the calling shell too.

## Red flags

| Thought | Reality |
|---|---|
| "The build passed, tests will too" | A build proves compile. Run the tier. |
| "I'll run it in the foreground, it's quick" | Quick runs become 900 s behind the cargo lock. Detach. |
| "Clippy failed on one crate; the rest is probably fine" | Rerun with `--keep-going`; Rust minor versions add lints across crates. |
| "I saw the tests pass earlier" | Earlier was a different tree. Gate the tree you commit. |

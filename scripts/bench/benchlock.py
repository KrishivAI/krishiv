#!/usr/bin/env python3
"""One machine, one benchmark at a time.

Every runner in this directory documents that its measurements are serial, and
each one enforced that with its own `pgrep` guard over a hand-written list of
process names. Those guards do not compose: `tpch_compare_engines.py` was not
in the pattern that `compare.sh` waited on, and `chain_local.sh` waited only
for `rustc`/`cargo`, so a full three-engine comparison ran concurrently with a
second full three-engine comparison on the same eight cores. Both wrote the
same output file. Neither run was wrong in a way that showed up as an error —
they were simply both measuring contention, and the slower numbers looked like
ordinary variance.

A name-matching guard is the wrong mechanism: it has to be updated whenever a
caller is added, and forgetting is silent. The lock therefore lives with the
runners rather than the callers, so any invocation path — shell wrapper, cron,
someone typing the command by hand — is covered by construction.

`flock` is used rather than a pidfile because the kernel releases it when the
holder dies, however it dies. A stale pidfile after an OOM kill would block
every later run until someone noticed and deleted it.
"""

from __future__ import annotations

import contextlib
import fcntl
import os
import sys
import time

LOCK_PATH = os.environ.get("KRISHIV_BENCH_LOCK", "/tmp/krishiv-bench.lock")


@contextlib.contextmanager
def machine_lock(purpose: str, wait: bool = True, stream=sys.stderr):
    """Hold the machine-wide benchmark lock for the duration of the block.

    With `wait=True` (the default) a second runner queues instead of failing:
    chained scripts then serialise themselves correctly without the caller
    having to know what else might be running. With `wait=False` it exits
    non-zero rather than produce a contaminated measurement.
    """
    handle = open(LOCK_PATH, "a+")  # noqa: SIM115 - lifetime is the with-block
    try:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            handle.seek(0)
            holder = handle.read().strip() or "another benchmark"
            if not wait:
                print(f"benchmark lock held by {holder}; refusing to run "
                      f"concurrently ({purpose})", file=stream, flush=True)
                raise SystemExit(2)
            print(f"waiting for benchmark lock held by {holder} ...",
                  file=stream, flush=True)
            started = time.monotonic()
            fcntl.flock(handle, fcntl.LOCK_EX)
            print(f"acquired benchmark lock after "
                  f"{time.monotonic() - started:.0f}s", file=stream, flush=True)
        handle.seek(0)
        handle.truncate()
        handle.write(f"pid={os.getpid()} {purpose} "
                     f"started={time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}\n")
        handle.flush()
        yield
    finally:
        with contextlib.suppress(OSError):
            fcntl.flock(handle, fcntl.LOCK_UN)
        handle.close()

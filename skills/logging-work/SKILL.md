---
name: logging-work
description: Use when finishing a unit of Krishiv work — before the commit — to record it: status log entry, register addendum, CHANGELOG, benchmark files, and the commit message shape.
---

# Logging work

The repo's memory is `docs/engineering-log/`. A change that is not recorded
there is re-derived by the next session.

## Where

| Change | Record |
|---|---|
| Any substantial session | `docs/engineering-log/status.md`: new `## YYYY-MM-DD — title` at the end |
| Revisiting an audited decision (deps, a held pin, a rule) | `docs/engineering-log/crate-audit-register.md`: `### YYYY-MM-DD addendum` under the original § |
| User-facing (MSRV, dependency major, SQL semantics, flags) | `CHANGELOG.md` `[Unreleased]` → Added / Changed / Fixed |
| A measurement | `benchmarks/<name>.md` + `.json` (see benchmarking) |
| A rule that changed | the module doc that states the rule, with the numbers that changed it |

## Status entry shape

```
## 2026-10-01 — one-line title

- **Completed**: what, with commit hashes and file paths.
- **Found / Fixed**: each defect as mechanism → fix, not symptom → patch.
- **Validation**: the exact gate commands and their counts (N passed, 0 failed),
  release build, digest comparison result.
- **Blocker** (if any): what, and what would unblock it.
- **Next**: the one command or task a fresh session should start with.
```

Numbers are measured ones from this session's logs, quoted precisely; "all
green" without counts is not a validation line.

## Commit message

- Title: `<area>: <what changed>` (`sql:`, `bench:`, `deps:`, `ivm:`).
- Body: why, the mechanism of any bug fixed, the measurements, the gate line.
- Trailer, last: the attribution lines the session provides (the
  `Co-Authored-By: Claude <model> <noreply@anthropic.com>` and
  `Claude-Session: <url>` pair), copied verbatim from the session — the model
  name differs per session, so never type it from memory.
- One commit per concern (harness/results, fix, dependency move are three).
- Commit only after the gate is green (gating); never push unless asked.

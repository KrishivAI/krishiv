# 0001: Record architecture decisions

- Status: Accepted
- Date: 2026-06-12
- Owners: project maintainers

## Context

Krishiv spans API, planner, runtime, scheduler, state, shuffle, connector, and
deployment boundaries. Important choices otherwise become implicit in code and
are difficult for contributors to review or safely revise.

## Decision

Use repository-local ADRs for changes to architectural invariants, public
contracts, durable formats, and foundational dependencies. ADRs are reviewed in
the same pull request as the implementation when possible.

## Consequences

Contributors have a durable decision trail and explicit alternatives. Small,
local implementation choices do not require ADRs. Superseded decisions remain
available as historical context.

## Validation

Pull-request review verifies whether architecture-affecting changes include an
ADR and corresponding documentation updates.

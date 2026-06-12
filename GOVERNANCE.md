# Project Governance

Krishiv uses a maintainer-led, contribution-driven model during the pre-1.0
period.

## Roles

- **Contributors** submit issues, documentation, tests, designs, and code.
- **Reviewers** have demonstrated ownership of an area and may approve changes.
- **Maintainers** merge changes, manage releases/security response, and resolve
  architecture or conduct disputes.

Roles are earned through sustained, constructive contribution. There is no
requirement to work for a particular company.

## Decision process

- Local implementation choices are decided in pull-request review.
- Cross-crate, compatibility-breaking, security-sensitive, or irreversible
  decisions require an ADR under `docs/decisions/`.
- Maintainers seek consensus. If consensus cannot be reached, the maintainer
  responsible for the affected subsystem records the decision and rationale.
- One active coordinator owner per job, Arrow/DataFusion foundations, and the
  engine/platform boundary are project invariants; changing them requires an ADR
  and explicit maintainer approval.

## Releases

Maintainers cut releases using `docs/RELEASE.md`. Release artifacts must come
from a tagged commit that passes the release workflow. Security releases may use
an expedited private process before coordinated disclosure.

## Conduct

All project spaces follow `CODE_OF_CONDUCT.md`.

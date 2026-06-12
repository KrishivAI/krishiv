# Phase 5: Open-source Readiness

## Goal

Make the repository understandable, governable, testable, and releasable by
contributors who do not have private project context.

## Implemented resolution

- Replaced the aspirational architecture document with the current engine
  boundary, crate ownership, runtime modes, lifecycle, durability, security, and
  extension rules.
- Added roadmap, governance, conduct, security, compatibility, connector SDK,
  benchmarking, changelog, release, and ADR documentation.
- Added structured issue forms and a stronger pull-request checklist.
- Added offline Markdown-link validation, release metadata validation, benchmark
  environment manifests, and unit tests for repository scripts.
- Added project-hygiene and tagged-release workflows.

## Acceptance checks

- All local documentation links resolve.
- Release tags must match the workspace package version.
- Benchmark artifacts include a machine-readable environment manifest.
- Public feature requests identify engine scope and architecture impact.
- Connector proposals identify capabilities, maturity, and certification work.

## Follow-up work

- Configure the real security advisory contact and repository URL after project
  ownership is finalized.
- Establish named maintainers and reviewers in `CODEOWNERS`.
- Add signed artifact provenance and SBOM generation.
- Add a durable benchmark history service and agreed regression thresholds.
- Add crates.io publication only after every public package has complete metadata
  and a verified publication order.
- Create labelled starter issues from roadmap items.

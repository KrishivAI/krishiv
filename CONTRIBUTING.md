# Contributing to Krishiv

Thank you for helping build Krishiv as an open-source compute engine. By
participating, you agree to the [Code of Conduct](CODE_OF_CONDUCT.md).

## Before starting

1. Read [`docs/README.md`](docs/README.md),
   [`docs/architecture.md`](docs/architecture.md), and the relevant engine
   contract.
2. Search existing issues and pull requests.
3. Open a design issue or ADR before changing a public contract, durable format,
   runtime invariant, or foundational dependency.
4. Keep data-platform concerns outside the engine boundary described in the
   architecture document.

## Development setup

Krishiv uses Rust 2024 and requires Rust 1.92 or newer. Some workspace crates
need a C/C++ toolchain, Python headers, OpenSSL, protobuf, and CMake.

On Debian/Ubuntu:

```bash
sudo apt-get update
sudo apt-get install -y build-essential python3-dev libssl-dev pkg-config protobuf-compiler cmake
rustup toolchain install 1.92
cargo install just
```

Start with narrow checks for the crate being changed, then expand validation:

```bash
cargo fmt --all --check
cargo test -p krishiv-runtime --lib
just check
just project-check
```

A complete workspace link can require substantial memory and native libraries.
Do not weaken code or tests to work around a local dependency limitation; record
the limitation in the pull request.

## Change guidelines

- Keep batch and streaming on the shared runtime model.
- Use Arrow `RecordBatch` internally and DataFusion for SQL/planning integration.
- Prefer typed IDs, errors, fragments, capabilities, and versions over strings.
- Add focused tests for behavior changes and failure paths.
- Preserve checkpoint, savepoint, state serializer, and connector compatibility;
  document intentional breaks in `CHANGELOG.md` and
  `docs/COMPATIBILITY.md`.
- Connector changes must follow `docs/connector-sdk.md` and must not overstate
  delivery guarantees or maturity.
- Include benchmark evidence for performance claims using
  `docs/BENCHMARKING.md`.
- Update `docs/implementation/status.md` only with a concise handoff for a
  substantial implementation session.

## Pull requests

A pull request should explain the problem, architecture impact, compatibility
impact, validation commands, and any remaining work. Keep commits reviewable and
avoid unrelated refactors. The pull-request checklist is a gate, not boilerplate.

Good first contributions include focused documentation fixes, additional
conformance cases, typed error improvements, and isolated connector tests.
Changes to scheduling, durability, state restore, or exactly-once behavior
usually need maintainer design review before implementation.

## Security

Do not open public issues for vulnerabilities. Follow
[`SECURITY.md`](SECURITY.md) for private reporting.

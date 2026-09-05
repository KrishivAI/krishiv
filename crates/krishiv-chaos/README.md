# krishiv-chaos

Cross-crate fault-injection tests: executor loss and re-registration,
coordinator failover, shuffle regeneration, checkpoint kill/restore, state
corruption. Long-running; excluded from the per-PR tier and from the
workspace clippy run, executed by `nightly.yml` and `just test-chaos`. Lint
it directly (`cargo clippy -p krishiv-chaos --all-targets`) so breakage is
not invisible.

Documentation: `docs/architecture/17-testing-and-quality.md`,
`docs/architecture/14-deployment-and-durability.md` (the HA chaos gate).

License: Apache-2.0.

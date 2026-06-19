# Release Process

Krishiv uses semantic versions. Before 1.0, minor versions may include documented
breaking API changes; durable metadata compatibility follows
[`COMPATIBILITY.md`](COMPATIBILITY.md), not an assumption based only on package
version.

## Release checklist

1. Select a clean commit from `main` and confirm all required CI jobs pass.
2. Update `CHANGELOG.md`: move relevant Unreleased entries under a dated version
   heading and leave an empty Unreleased section.
3. Update compatibility, migration, and connector maturity documentation.
4. Run the project checks and mode matrix:

   ```bash
   just project-check
   cargo fmt --all --check
   just check
   cargo test --workspace --lib
   ```

5. Update `[workspace.package].version` and regenerate `Cargo.lock` with Cargo.
6. Validate metadata with `python3 scripts/check_release.py`.
7. Commit the version bump and create an annotated `vMAJOR.MINOR.PATCH` tag.
8. Push the commit and tag. The release workflow validates tag/version agreement,
   builds the standard binary, and uploads a checksummed archive to the GitHub
   release.
9. Smoke-test the archive and publish release notes with compatibility and known
   limitations.

Crate publication is not automated yet. Do not claim crates.io availability
until package metadata, publication order, and a dry-run publish gate are added.

## Hotfixes

Branch from the affected release tag, make the smallest safe change, run the
same validation, and release a patch version. Document whether checkpoint,
savepoint, or connector behavior changes.

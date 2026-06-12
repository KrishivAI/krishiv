## Summary

<!-- What problem does this solve, and why is this design appropriate? -->

## Architecture and compatibility

<!-- Crate ownership, ADR, API/wire/state/checkpoint/savepoint/connector impact. -->

## Validation

```bash
# Exact commands and results
```

## Performance evidence

<!-- Required for performance claims; link the benchmark manifest and raw output. -->

## Checklist

- [ ] The change stays inside the compute-engine boundary in `docs/architecture.md`
- [ ] Focused tests cover behavior and important failure paths
- [ ] Public or durable compatibility changes are documented in `CHANGELOG.md` and `docs/COMPATIBILITY.md`
- [ ] Connector guarantees and maturity remain conservative and combination-specific
- [ ] Documentation and an ADR were updated when architecture or contracts changed
- [ ] `docs/implementation/status.md` was updated for a substantial implementation session
- [ ] Exact validation commands and any environment limitations are recorded above
- [ ] No secrets, private endpoints, generated build output, or unrelated changes are included

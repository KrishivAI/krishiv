# Public API Baselines

Phase A publishes deterministic snapshots for the supported language surfaces:

- `rust-public.json` and `rust-public-api.txt` — Rust items, signatures,
  stability, documentation source, and deprecation metadata.
- `python-public.json` and the generated
  `crates/krishiv-python/python/krishiv/krishiv.pyi` — Python classes, methods,
  functions, and type-checker-visible native module surface.
- `sql-public.json` — public SQL crate items and modules. A grammar-level SQL
  feature inventory remains Phase H work.
- `stable-api.toml` — phase status and cross-language capability parity.

Run:

```bash
python3 scripts/check_api_surface.py --write
python3 scripts/check_api_surface.py
python3 scripts/compare_api_surface.py --against-ref origin/main
```

The comparison classifies:

- **additive** — a new public item;
- **breaking** — a removed public item; and
- **semantic** — a retained item whose signature, stability, deprecation, or
  replacement metadata changed.

Documentation line movement is not semantic. Breaking and semantic changes fail
unless their exact emitted IDs are recorded in `approved-breaking.toml` with a
reason and replacement. Approval is a review record, not permission to omit the
changelog or migration documentation.

All generated surfaces are preview until individual capabilities are promoted
to stable in `stable-api.toml`.

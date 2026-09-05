# krishiv

The user-facing facade and the `krishiv` command-line binary.

- Re-exports the public Rust API (`krishiv::Session`, `DataFrame`, …) from
  `krishiv-api`.
- The CLI: `sql`, `explain [--analyze]`, `stream`, `ivm`, `table`, `submit`,
  `jobs`, `state`, `savepoint`, `restore`, `checkpoints`, `pipeline`,
  `doctor`, `capabilities`, and the daemons `local`, `clusterd`
  (`coordinator`), `job-coordinator`, `executor`, `flight-server`,
  `shuffle-svc`, `mcp`. Invoked as `krishiv-<name>` the binary dispatches to
  the matching subcommand.
- Deployment feature presets live only on this crate: `local` (default),
  `embedded`, `single-node`, `distributed` (= `bare-metal`), `k8s`, `prod`,
  `full`. Features select compiled dependency families; execution mode is a
  runtime choice.

Documentation: `docs/architecture/01-execution-modes.md` (routing and flags),
`docs/architecture/11-public-interfaces.md` (the CLI),
`docs/architecture/15-configuration.md` (presets).

```rust
use krishiv::Session;

let session = Session::new();
let df = session.sql("SELECT 42 AS answer")?;
let batches = df.collect()?;          // or df.collect_async().await?
```

License: Apache-2.0.

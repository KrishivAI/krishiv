# krishiv-runtime

Where work runs: `RuntimeMode` (`Embedded`, `SingleNode`, `Distributed`) and
the deliberately separate `ExecutionPlacement` (`LocalInProcess`,
`SingleNodeDaemon`, `RemoteClusterRequired`); `ExecutionRuntime` with its
async-canonical surface and one sync seam; the in-process cluster
(coordinator + executor over channels); `local_streaming`; the distributed
backend (Flight SQL client, gRPC management client, coordinator HTTP
client). A distributed session with no routable coordinator is rejected —
never silently local.

Documentation: `docs/architecture/01-execution-modes.md`.

License: Apache-2.0.

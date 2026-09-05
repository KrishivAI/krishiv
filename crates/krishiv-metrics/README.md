# krishiv-metrics

OpenTelemetry metrics, traces, and structured logs for every Krishiv process:
`init(MetricsConfig)`, the `krishiv_*` metric families (`KrishivMetrics`,
Prometheus text renderer), gRPC trace-context propagation, the in-process log
ring, system metrics (CPU, memory, threads), and the observability report
used by `krishiv doctor`.

Documentation: `docs/architecture/13-observability.md`,
`docs/grafana/README.md`.

License: Apache-2.0.

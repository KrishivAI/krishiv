# krishiv-metrics

OpenTelemetry-based metrics collection and export for Krishiv components.

## Overview

`krishiv-metrics` provides:

- Prometheus-compatible metrics via OpenTelemetry
- OTLP and stdout export
- Per-component metric registration (scheduler, executor, connectors)
- System-level metrics (CPU, memory) via `sysinfo`

## Usage

```rust
use krishiv_metrics::MetricsCollector;
```

## License

Apache-2.0

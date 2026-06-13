# Krishiv Grafana Dashboard

`krishiv-dashboard.json` is a reference Grafana dashboard for the Krishiv
compute engine. It visualizes the Prometheus metrics exposed by the
coordinator's `/metrics` endpoint (see `krishiv-metrics`): task throughput and
retries, operator memory and spill, streaming watermarks / source-offset lag /
state size, checkpoint progress and commit latency, shuffle, and gRPC latency.

## Requirements

- A Prometheus datasource scraping the Krishiv coordinator `/metrics` endpoint.
- Grafana 10+ (dashboard `schemaVersion` 39).

## Import

1. In Grafana, go to **Dashboards → New → Import**.
2. Upload `krishiv-dashboard.json` (or paste its contents).
3. When prompted, select your Prometheus datasource for the `datasource`
   variable. The `job` variable is populated from `krishiv_checkpoint_epoch`
   labels and defaults to *All*.
4. Click **Import**.

The dashboard refreshes every 15s over the last 1h by default; adjust the time
range and refresh interval from the top-right controls.

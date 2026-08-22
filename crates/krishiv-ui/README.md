# krishiv-ui

Serves the embedded web console and the UI-specific JSON endpoints on the
coordinator's HTTP listener. This is not a separate server: `embedded_router`
merges into the coordinator daemon's router (`krishiv daemon`), and `serve`
binds a standalone listener for the operator — both read coordinator state
in-process.

## The console

`/console` serves a React 19 + TanStack SPA (source in this crate's `console/`, same structure and design system as the krishiv-platform console). The
Vite build output (`console/dist`, committed) is embedded via `rust-embed` —
compiled in for release builds, read from disk in debug builds, so
`npm run build` in `console/` is picked up without recompiling.

The SPA talks to the coordinator's canonical `/api/v1/*` endpoints directly
(jobs, executors, batch-sql, continuous, IVM, queryable state) plus the
aggregation endpoints this crate serves (`diagnose`, `queues`, `history`,
`sql`). Auth is a stored bearer sent per request; the static assets stay
public.

Dev loop: `cd crates/krishiv-ui/console && npm run dev` (proxies to a local coordinator on
7072; override with `KRISHIV_COORDINATOR_URL`).

The previous askama/HTMX server-rendered UI was removed when the console
reached parity (task #152).

## License

Apache-2.0

# Krishiv Web

Production-oriented Next.js App Router site for the Krishiv landing page, documentation, architecture pages, blog, and release notes.

## Local development

```bash
cd web
pnpm install
pnpm dev
```

## Validation

```bash
pnpm typecheck
pnpm build
```

## Cloudflare Workers deployment

The app is configured for OpenNext on Cloudflare Workers and intentionally does not use static `output: "export"`.

```bash
pnpm preview
pnpm deploy
```

Configuration lives in `open-next.config.ts` and `wrangler.jsonc`.

## Environment variables

No runtime environment variables are required for the current marketing/docs site. Add deployment secrets through Cloudflare Wrangler when future integrations need them.

## Content creation

Docs are versioned under `content/docs/<version>/` for maintainers and mirrored by the central docs navigation config in `lib/versions.ts`. Keep website claims aligned with `PRODUCT_FACTS.md`, repository docs, examples, tests, and public crate APIs.

## Adding a docs version

1. Add a new entry to `lib/versions.ts`.
2. Add a matching `content/docs/<version>/` folder.
3. Populate pages with source-backed copy.
4. Run `pnpm typecheck` and `pnpm build`.

## Writing docs safely

Do not claim support solely because a dependency appears in `Cargo.toml`. Prefer these labels: `Available`, `Experimental`, `In Progress`, `Planned`, and `Preview`. Avoid global exactly-once claims unless a source/sink/checkpoint combination is certified in the engine contracts.

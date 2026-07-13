# Krishiv Web

Next.js App Router site for the Krishiv product family, Fumadocs documentation,
architecture pages, blog, and release notes.

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

`pnpm build` produces a static site in `out/`. Fumadocs source files are generated
automatically during the build.

## Cloudflare Pages deployment

The site uses Next.js static export and deploys the `out/` directory to Cloudflare Pages.
Response headers are configured in `public/_headers` because Next.js runtime
`headers()` are not available in a static export.

```bash
pnpm preview
pnpm deploy
```

## Environment variables

No runtime environment variables are required for the current marketing/docs site. Add deployment secrets through Cloudflare Wrangler when future integrations need them.

## Content creation

Docs live under product roots in `content/docs/`:

- `engine/` contains the public Engine manual.
- `platform/` contains only an availability notice until Platform has a public preview.

Fumadocs builds navigation from each folder's `meta.json`. Keep claims aligned
with public APIs, CLI help, normative contracts, examples, and tests.

## Adding a docs page

1. Add an MDX file below the correct product root.
2. Add it to the nearest `meta.json` navigation list.
3. Set a validated `status` in frontmatter.
4. Prefer Fumadocs Cards, Callouts, Steps, and Tabs to custom HTML.
5. Run `pnpm typecheck` and `pnpm build`.

## Writing docs safely

Do not claim support solely because a dependency appears in `Cargo.toml`. The
validated labels are `available`, `preview`, `experimental`, `in-progress`, and
`coming-soon`. Avoid global exactly-once or production-readiness claims.

Canonical Engine docs live at `/docs/engine/...`; `/docs` resolves to that root.
Add versioned docs only when a release has a frozen, maintained documentation set,
instead of publishing aliases that imply historical content exists.

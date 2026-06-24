import { docs } from '../.source/server';

export type DocSlug = { version: string; slug: string[] };

export async function buildDocParams(): Promise<DocSlug[]> {
  const resolved = await (docs as unknown as Promise<{ docs: Array<{ info: { path: string } }> }>);
  const list = resolved.docs;
  const out: DocSlug[] = [];
  for (const entry of list) {
    if (!entry || !entry.info || !entry.info.path) continue;
    // Path is like "latest/getting-started/why-krishiv.mdx"
    const parts = entry.info.path.replace(/\.mdx$/, '').split('/');
    if (parts.length < 2) continue;
    const version = parts[0];
    const slug = parts.slice(1);
    if (slug[slug.length - 1] === 'index') {
      slug.pop();
    }
    out.push({ version, slug });
  }
  if (typeof process !== 'undefined') {
    process.stderr.write(`buildDocParams: returning ${out.length} params from ${list.length} entries\n`);
    for (const entry of list.slice(0, 3)) {
      process.stderr.write(`  entry path: ${JSON.stringify(entry.info?.path)}\n`);
    }
  }
  return out;
}

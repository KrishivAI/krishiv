#!/usr/bin/env node
import { readFileSync, writeFileSync, readdirSync, statSync } from 'fs';
import { join, dirname, basename } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, '..', 'content', 'docs', 'latest');
const OUT = join(__dirname, '..', 'lib', 'search-index.json');

function walk(dir, base = '', groupName = '') {
  const out = [];
  for (const entry of readdirSync(dir)) {
    const full = join(dir, entry);
    const st = statSync(full);
    if (st.isDirectory()) {
      out.push(...walk(full, join(base, entry), entry));
    } else if (entry.endsWith('.mdx')) {
      const content = readFileSync(full, 'utf-8');
      const fmMatch = content.match(/^---\n([\s\S]*?)\n---/);
      let title = basename(entry, '.mdx');
      let description = '';
      if (fmMatch) {
        const titleMatch = fmMatch[1].match(/^title:\s*(.+)$/m);
        const descMatch = fmMatch[1].match(/^description:\s*(.+)$/m);
        if (titleMatch) title = titleMatch[1].trim().replace(/^['"]|['"]$/g, '');
        if (descMatch) description = descMatch[1].trim().replace(/^['"]|['"]$/g, '');
      }
      const slug = entry === 'index.mdx' ? base : join(base, basename(entry, '.mdx'));
      const href = `/docs/latest${slug ? `/${slug}` : ''}`;
      out.push({ title, description, href, group: groupName });
    }
  }
  return out;
}

const items = walk(ROOT);
writeFileSync(OUT, JSON.stringify(items, null, 2));
console.log(`Wrote ${items.length} search items to ${OUT}`);

#!/usr/bin/env node
/* eslint-disable */
import { readFileSync, writeFileSync, mkdirSync, existsSync } from 'fs';
import { dirname, join } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const SRC = join(__dirname, '..', 'lib', 'docs-content');
const OUT = join(__dirname, '..', 'content', 'docs', 'latest');

const GROUP_MAP = {
  'Getting Started': 'getting-started',
  'Concepts': 'concepts',
  'Recipes': 'recipes',
  'SQL Reference': 'sql',
  'Rust API': 'rust',
  'Python API': 'python',
  'Connectors': 'connectors',
  'Operations': 'operations',
};

const FILES = [
  'getting-started.ts',
  'recipes.ts',
  'sql.ts',
  'rust.ts',
  'python.ts',
  'connectors.ts',
];

const STATUS_MAP = {
  'Available': 'Available',
  'Experimental': 'Experimental',
  'In Progress': 'In Progress',
  'Preview': 'Preview',
  'Planned': 'Planned',
};

function convertBody(html) {
  let body = html;
  // note-box → Callout
  body = body.replace(
    /<div class="note-box"><strong>(.*?):<\/strong>([\s\S]*?)<\/div>/g,
    '<Callout type="info" title="$1">$2</Callout>'
  );
  body = body.replace(
    /<div class="note-box">([\s\S]*?)<\/div>/g,
    '<Callout type="info">$1</Callout>'
  );
  body = body.replace(
    /<div class="warn-box"><strong>(.*?):<\/strong>([\s\S]*?)<\/div>/g,
    '<Callout type="warn" title="$1">$2</Callout>'
  );
  body = body.replace(
    /<div class="warn-box">([\s\S]*?)<\/div>/g,
    '<Callout type="warn">$1</Callout>'
  );
  // code blocks: <pre><code class="language-X">\n...\n</code></pre>
  body = body.replace(
    /<pre><code class="language-([\w-]+)">\n?([\s\S]*?)\n?<\/code><\/pre>/g,
    (_, lang, code) => {
      const decoded = code
        .replace(/&lt;/g, '<')
        .replace(/&gt;/g, '>')
        .replace(/&amp;/g, '&')
        .replace(/&quot;/g, '"')
        .replace(/&#39;/g, "'");
      return '\n```' + lang + '\n' + decoded + '\n```\n';
    }
  );
  // inline code in tables: <code>name</code> → `name`
  body = body.replace(/<code>([^<]+)<\/code>/g, '`$1`');
  return body;
}

function escapeYaml(s) {
  if (!s) return '';
  if (s.includes('\n') || s.includes(':') || s.includes('"') || s.includes("'")) {
    return '"' + s.replace(/"/g, '\\"').replace(/\n/g, ' ') + '"';
  }
  return s;
}

function processFile(filename) {
  const path = join(SRC, filename);
  const content = readFileSync(path, 'utf-8');
  const pageRe = /\{\s*slug:\s*'([^']*)',\s*group:\s*'([^']*)',\s*title:\s*'((?:[^'\\]|\\.)*)',\s*description:\s*'((?:[^'\\]|\\.)*)',\s*status:\s*'([^']*)',\s*body:\s*`([\s\S]*?)`\s*,?\s*\}/g;
  let m;
  let count = 0;
  while ((m = pageRe.exec(content)) !== null) {
    const [, slug, group, title, description, status, body] = m;
    const groupFolder = GROUP_MAP[group] || group.toLowerCase().replace(/\s+/g, '-');
    // Strip leading group folder from slug if present
    let cleanSlug = slug;
    if (cleanSlug === groupFolder || cleanSlug.startsWith(groupFolder + '/')) {
      cleanSlug = cleanSlug.slice(groupFolder.length);
      if (cleanSlug.startsWith('/')) cleanSlug = cleanSlug.slice(1);
    }
    let fileSlug;
    if (cleanSlug === '') {
      fileSlug = 'index';
    } else {
      // Use the full cleanSlug (with slashes) as the relative path
      fileSlug = cleanSlug;
    }
    const dir = join(OUT, groupFolder);
    mkdirSync(dir, { recursive: true });
    const filePath = join(dir, `${fileSlug}.mdx`);
    const fm = [
      '---',
      `title: ${escapeYaml(title.replace(/\\'/g, "'"))}`,
      `description: ${escapeYaml(description.replace(/\\'/g, "'"))}`,
      `status: ${STATUS_MAP[status] || 'Available'}`,
      '---',
      '',
    ].join('\n');
    const converted = convertBody(body.trim());
    writeFileSync(filePath, fm + converted + '\n');
    count++;
    console.log(`  ${filePath.replace(OUT + '/', '')}`);
  }
  console.log(`Wrote ${count} pages from ${filename}`);
}

console.log('Migrating content to MDX...');
for (const f of FILES) {
  console.log(`\n${f}:`);
  processFile(f);
}

// Write meta.json for each group
const groupFolders = ['getting-started', 'concepts', 'recipes', 'sql', 'rust', 'python', 'connectors', 'operations'];

const slugOrder = {};
for (const f of FILES) {
  const path = join(SRC, f);
  const content = readFileSync(path, 'utf-8');
  const pageRe = /\{\s*slug:\s*'([^']*)',\s*group:\s*'([^']*)'/g;
  let m;
  while ((m = pageRe.exec(content)) !== null) {
    const [, slug, group] = m;
    const groupFolder = GROUP_MAP[group] || group.toLowerCase().replace(/\s+/g, '-');
    if (!slugOrder[groupFolder]) slugOrder[groupFolder] = [];
    slugOrder[groupFolder].push(slug);
  }
}

for (const g of groupFolders) {
  const dir = join(OUT, g);
  if (!existsSync(dir)) continue;
  const metaPath = join(dir, 'meta.json');
  if (existsSync(metaPath)) continue;
  const pages = (slugOrder[g] || []).map((s) => s === '' ? 'index' : s);
  const meta = { title: g.replace(/-/g, ' ').replace(/\b\w/g, (c) => c.toUpperCase()), pages };
  writeFileSync(metaPath, JSON.stringify(meta, null, 2) + '\n');
  console.log(`\nWrote meta.json for ${g}:`, pages.length, 'pages');
}

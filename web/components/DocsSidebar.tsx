import Link from 'next/link';
import type { ReactNode } from 'react';
import type { Node, Root } from 'fumadocs-core/page-tree';

function slugFromUrl(url: string) {
  const parts = url.split('/').filter(Boolean);
  return parts.slice(2);
}

function renderPages(items: Node[], version: string, activeSlug: string): ReactNode {
  return items.map((item) => {
    if (item.type === 'page') {
      const url = item.url ?? '#';
      const slug = slugFromUrl(url).join('/');
      const isActive = slug === activeSlug;
      return (
        <Link key={url} href={url} className={isActive ? 'sidebar-active' : ''}>
          {typeof item.name === 'string' ? item.name : String(item.name ?? url)}
        </Link>
      );
    }
    if (item.type === 'folder') {
      const folderName = typeof item.name === 'string' ? item.name : '';
      const isGroupHeader = folderName.startsWith('---') && folderName.endsWith('---');
      const label = isGroupHeader ? folderName.replace(/^---\s*|\s*---$/g, '').trim() : folderName;
      return (
        <div key={folderName}>
          <div className="sidebar-group-label">{label}</div>
          {item.children && renderPages(item.children as Node[], version, activeSlug)}
        </div>
      );
    }
    return null;
  });
}

export function DocsSidebar({ tree, version, activeSlug }: { tree: Root; version: string; activeSlug: string }) {
  return (
    <aside className="sidebar">
      <select className="version" defaultValue={version} aria-label="Documentation version" disabled>
        <option value="latest">Latest</option>
        <option value="v0.1">v0.1</option>
      </select>
      {tree.children && renderPages(tree.children as Node[], version, activeSlug)}
    </aside>
  );
}

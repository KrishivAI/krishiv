'use client';

import Link from 'next/link';
import { useEffect, useMemo, useRef, useState } from 'react';
import type { docsVersions } from '@/lib/versions';

type Version = (typeof docsVersions)[number];
type Heading = { id: string; text: string };
type GroupedPages = { group: string; pages: Array<{ slug: string; title: string; description: string }> };

export function DocsMobileControls({
  title,
  version,
  versions,
  groups,
  activeSlug,
  headings,
  versionPathTemplate,
}: {
  title: string;
  version: string;
  versions: readonly Version[];
  groups: GroupedPages[];
  activeSlug: string;
  headings: Heading[];
  versionPathTemplate?: string;
}) {
  const [drawerOpen, setDrawerOpen] = useState(false);
  const [searchOpen, setSearchOpen] = useState(false);
  const [tocOpen, setTocOpen] = useState(false);
  const [expanded, setExpanded] = useState(() => new Set(groups.filter((g) => g.pages.some((p) => p.slug === activeSlug)).map((g) => g.group)));
  const searchInput = useRef<HTMLInputElement>(null);
  const allPages = useMemo(() => groups.flatMap((g) => g.pages.map((p) => ({ ...p, group: g.group }))), [groups]);
  const [query, setQuery] = useState('');
  const results = allPages.filter((p: { title: string; description: string; group: string }) => `${p.title} ${p.description} ${p.group}`.toLowerCase().includes(query.toLowerCase())).slice(0, 8);

  useEffect(() => {
    document.body.classList.toggle('scroll-locked', drawerOpen || searchOpen);
    const onKey = (event: KeyboardEvent) => {
      if (event.key === 'Escape') { setDrawerOpen(false); setSearchOpen(false); setTocOpen(false); }
    };
    window.addEventListener('keydown', onKey);
    return () => { document.body.classList.remove('scroll-locked'); window.removeEventListener('keydown', onKey); };
  }, [drawerOpen, searchOpen]);

  useEffect(() => { if (searchOpen) setTimeout(() => searchInput.current?.focus(), 30); }, [searchOpen]);

  const vPath = versionPathTemplate ? (v: string) => versionPathTemplate.replace(`/docs/${version}`, `/docs/${v}`) : (v: string) => `/docs/${v}${activeSlug ? `/${activeSlug}` : ''}`;
  return <>
    <div className="docs-mobile-toolbar" role="navigation" aria-label="Documentation controls">
      <button className="touch-button" aria-label="Open docs menu" onClick={() => setDrawerOpen(true)}>☰</button>
      <span className="docs-toolbar-title">{title}</span>
      <button className="touch-button" aria-label="Search docs" onClick={() => setSearchOpen(true)}>⌕</button>
      <select className="docs-version-compact" aria-label="Documentation version" value={version} onChange={(e) => { window.location.href = vPath(e.target.value); }}>
        {versions.map((v) => <option key={v.slug} value={v.slug}>{v.label}</option>)}
      </select>
    </div>

    <div className={`docs-drawer-layer ${drawerOpen ? 'open' : ''}`} aria-hidden={!drawerOpen}>
      <button className="docs-backdrop" aria-label="Close docs menu" onClick={() => setDrawerOpen(false)} />
      <aside className="docs-drawer" aria-label="Documentation menu">
        <div className="drawer-top"><strong>Documentation</strong><button className="touch-button" aria-label="Close docs menu" onClick={() => setDrawerOpen(false)}>×</button></div>
        <label className="drawer-label">Version<select className="version" value={version} onChange={(e) => { window.location.href = vPath(e.target.value); }}>{versions.map((v) => <option key={v.slug} value={v.slug}>{v.label}</option>)}</select></label>
        <button className="drawer-search" onClick={() => setSearchOpen(true)}>Search documentation</button>
        <nav className="drawer-nav">
          {groups.map(({ group, pages }) => {
            const isOpen = expanded.has(group);
            return <section key={group}><button className="drawer-group" aria-expanded={isOpen} onClick={() => setExpanded((old) => { const next = new Set(old); if (next.has(group)) next.delete(group); else next.add(group); return next; })}>{group}<span>{isOpen ? '−' : '+'}</span></button>
              {isOpen && <div>{pages.map((p) => <Link onClick={() => setDrawerOpen(false)} key={p.slug} href={`/docs/${version}${p.slug ? `/${p.slug}` : ''}`} className={p.slug === activeSlug ? 'sidebar-active' : ''}>{p.title}</Link>)}</div>}
            </section>;
          })}
        </nav>
      </aside>
    </div>

    {headings.length > 0 && <div className="mobile-toc"><button onClick={() => setTocOpen((v) => !v)} aria-expanded={tocOpen}>On this page <span>{tocOpen ? '−' : '+'}</span></button>{tocOpen && <nav>{headings.map((h) => <a key={h.id} href={`#${h.id}`} onClick={() => setTocOpen(false)}>{h.text}</a>)}</nav>}</div>}

    {searchOpen && <div className="search-overlay" role="dialog" aria-modal="true" aria-label="Search documentation"><div className="search-panel"><div className="search-row"><input ref={searchInput} value={query} onChange={(e) => setQuery(e.target.value)} placeholder="Search Krishiv docs…"/><button className="touch-button" onClick={() => setSearchOpen(false)} aria-label="Close search">×</button></div><div className="search-results">{results.map((p) => <Link key={p.slug} onClick={() => setSearchOpen(false)} href={`/docs/${version}${p.slug ? `/${p.slug}` : ''}`}><strong>{p.title}</strong><span>{p.group} · {p.description}</span></Link>)}</div></div></div>}
  </>;
}

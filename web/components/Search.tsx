'use client';

import Link from 'next/link';
import { useEffect, useMemo, useRef, useState } from 'react';
import searchIndex from '@/lib/search-index.json';

type SearchItem = {
  title: string;
  description: string;
  href: string;
  group: string;
};

const INDEX: SearchItem[] = searchIndex as SearchItem[];

function score(item: SearchItem, terms: string[]): number {
  const hay = `${item.title} ${item.group} ${item.description}`.toLowerCase();
  let s = 0;
  for (const t of terms) {
    if (!t) continue;
    const idx = hay.indexOf(t);
    if (idx < 0) return -1;
    s += idx < 200 ? 100 - Math.min(idx, 99) : 10;
    if (item.title.toLowerCase().includes(t)) s += 50;
    if (item.group.toLowerCase().includes(t)) s += 20;
  }
  return s;
}

export function SearchButton() {
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState('');
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if ((e.key === 'k' || e.key === 'K') && (e.metaKey || e.ctrlKey)) {
        e.preventDefault();
        setOpen(true);
      } else if (e.key === 'Escape' && open) {
        setOpen(false);
      }
    };
    const onSlash = (e: KeyboardEvent) => {
      if (e.key === '/' && !open && document.activeElement?.tagName !== 'INPUT' && document.activeElement?.tagName !== 'TEXTAREA') {
        e.preventDefault();
        setOpen(true);
      }
    };
    window.addEventListener('keydown', onKey);
    window.addEventListener('keydown', onSlash);
    return () => {
      window.removeEventListener('keydown', onKey);
      window.removeEventListener('keydown', onSlash);
    };
  }, [open]);

  useEffect(() => {
    if (open) setTimeout(() => inputRef.current?.focus(), 30);
    document.body.classList.toggle('scroll-locked', open);
    return () => { document.body.classList.remove('scroll-locked'); };
  }, [open]);

  const terms = useMemo(() => query.toLowerCase().split(/\s+/).filter(Boolean), [query]);
  const results = terms.length === 0
    ? INDEX.slice(0, 8)
    : INDEX
        .map((i) => ({ i, s: score(i, terms) }))
        .filter((r) => r.s >= 0)
        .sort((a, b) => b.s - a.s)
        .slice(0, 12)
        .map((r) => r.i);

  return (
    <>
      <button
        type="button"
        className="icon-link search-trigger"
        aria-label="Search documentation"
        onClick={() => setOpen(true)}
        title="Search (Ctrl/⌘+K)"
      >
        <svg viewBox="0 0 20 20" width="18" height="18" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
          <circle cx="9" cy="9" r="6"/>
          <path d="m17 17-3.5-3.5"/>
        </svg>
      </button>
      {open && (
        <div className="search-overlay" role="dialog" aria-modal="true" aria-label="Search documentation">
          <div className="search-panel">
            <div className="search-row">
              <input
                ref={inputRef}
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                placeholder="Search Krishiv docs…"
                aria-label="Search query"
              />
              <button className="touch-button" onClick={() => setOpen(false)} aria-label="Close search" type="button">×</button>
            </div>
            <div className="search-results">
              {results.length === 0 && <p className="muted" style={{ padding: 14, color: 'var(--muted)' }}>No matches. Try a different term.</p>}
              {results.map((r) => (
                <Link key={r.href} href={r.href} onClick={() => setOpen(false)}>
                  <strong>{r.title}</strong>
                  <span>{r.group} · {r.description}</span>
                </Link>
              ))}
            </div>
            <p className="muted" style={{ padding: '8px 14px 4px', fontSize: 12, color: 'var(--muted)' }}>
              Press <kbd>Esc</kbd> to close · <kbd>Ctrl/⌘+K</kbd> to open
            </p>
          </div>
        </div>
      )}
    </>
  );
}

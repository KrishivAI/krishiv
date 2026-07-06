'use client';

import type { DocStatus } from '../../lib/docs-data';

/* Desaturated status colors from the shared true-black palette — status is a
   signal, not paint, and never uses the brand accent. */
const statusColors: Record<DocStatus, { bg: string; border: string; text: string; label: string }> = {
  Available: { bg: 'rgba(79,184,126,.1)', border: 'rgba(79,184,126,.35)', text: '#4fb87e', label: 'Available' },
  Preview: { bg: 'rgba(88,166,218,.1)', border: 'rgba(88,166,218,.35)', text: '#58a6da', label: 'Preview' },
  Experimental: { bg: 'rgba(167,139,217,.1)', border: 'rgba(167,139,217,.35)', text: '#a78bd9', label: 'Experimental' },
  'In Progress': { bg: 'rgba(217,161,59,.1)', border: 'rgba(217,161,59,.35)', text: '#d9a13b', label: 'In Progress' },
  Planned: { bg: 'rgba(113,113,122,.1)', border: 'rgba(113,113,122,.35)', text: '#a1a1aa', label: 'Planned' },
};

export function MaturityBadge({ status }: { status: DocStatus }) {
  const c = statusColors[status] ?? statusColors.Available;
  return (
    <span
      className="maturity-badge"
      style={{ background: c.bg, border: `1px solid ${c.border}`, color: c.text }}
    >
      {c.label}
    </span>
  );
}

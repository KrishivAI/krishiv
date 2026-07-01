'use client';

import type { DocStatus } from '../../lib/docs-data';

const statusColors: Record<DocStatus, { bg: string; border: string; text: string; label: string }> = {
  Available: { bg: 'rgba(34,197,94,.1)', border: 'rgba(34,197,94,.35)', text: '#b8f7c8', label: 'Available' },
  Preview: { bg: 'rgba(59,130,246,.1)', border: 'rgba(59,130,246,.35)', text: '#93c5fd', label: 'Preview' },
  Experimental: { bg: 'rgba(168,85,247,.1)', border: 'rgba(168,85,247,.35)', text: '#c4b5fd', label: 'Experimental' },
  'In Progress': { bg: 'rgba(245,158,11,.1)', border: 'rgba(245,158,11,.35)', text: '#ffd28a', label: 'In Progress' },
  Planned: { bg: 'rgba(107,114,128,.1)', border: 'rgba(107,114,128,.35)', text: '#9ca3af', label: 'Planned' },
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

// Auto-refresh honesty: a polling UI that silently stops updating shows
// stale green during the exact minutes it matters. This chip shows when
// the last successful sample landed and turns hard red on fetch errors.

import { useEffect, useState } from "react";

export function Freshness({ dataUpdatedAt, error }: { dataUpdatedAt: number; error: unknown }) {
  const [, tick] = useState(0);
  useEffect(() => {
    const t = setInterval(() => tick((n) => n + 1), 1000);
    return () => clearInterval(t);
  }, []);
  if (error) {
    return (
      <span className="rounded border border-failed/40 px-1.5 py-0.5 text-xs font-medium text-failed">
        connection lost
      </span>
    );
  }
  if (!dataUpdatedAt) return <span className="text-xs text-faint">loading…</span>;
  const age = Math.max(0, Math.round((Date.now() - dataUpdatedAt) / 1000));
  const stale = age > 15;
  return (
    <span className={`text-xs tnum ${stale ? "text-queued" : "text-faint"}`}>
      updated {age}s ago
    </span>
  );
}

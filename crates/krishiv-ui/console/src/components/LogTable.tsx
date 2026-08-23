// Shared log viewer over the daemons' /logs ring endpoints. Recent history
// only (the ring holds ~2000 INFO+ events since process start) — stated in
// the caption so nobody mistakes this for log search.

import { useQuery } from "@tanstack/react-query";
import { useState } from "react";

import { api } from "../api/client";
import { ErrorText } from "../components/ui";

export interface LogEntry {
  at_ms: number;
  level: string;
  target: string;
  message: string;
}

const LEVEL_TONE: Record<string, string> = {
  ERROR: "text-failed",
  WARN: "text-queued",
  INFO: "text-muted",
};

function ts(ms: number): string {
  return new Date(ms).toISOString().replace("T", " ").slice(5, 19);
}

export function LogTable({ path, title }: { path: string; title: string }) {
  const [level, setLevel] = useState<"info" | "warn" | "error">("info");
  const logs = useQuery({
    queryKey: ["logs", path, level],
    queryFn: () =>
      api.get<{ entries: LogEntry[] }>(`${path}?limit=300&level=${level}`),
    refetchInterval: 4000,
    retry: 0,
  });
  return (
    <div>
      <div className="mb-2 flex items-center gap-3">
        <h2 className="text-sm font-semibold text-muted">{title}</h2>
        <div className="flex gap-1">
          {(["info", "warn", "error"] as const).map((l) => (
            <button
              key={l}
              onClick={() => setLevel(l)}
              className={`rounded border px-1.5 py-0.5 text-xs uppercase ${level === l ? "border-accent text-text" : "border-border text-faint hover:text-text"}`}
            >
              {l}
            </button>
          ))}
        </div>
        <span className="text-xs text-faint">
          recent history since process start — not an archive
        </span>
      </div>
      {logs.error && <ErrorText>{String(logs.error)}</ErrorText>}
      <div className="max-h-96 overflow-auto rounded border border-border bg-surface font-mono text-xs">
        {(logs.data?.entries ?? []).map((e, i) => (
          <div key={i} className="flex gap-2 border-b border-border px-2 py-1 last:border-0">
            <span className="shrink-0 text-faint tnum">{ts(e.at_ms)}</span>
            <span className={`w-11 shrink-0 font-semibold ${LEVEL_TONE[e.level] ?? "text-muted"}`}>
              {e.level}
            </span>
            <span className="shrink-0 text-faint">{e.target.split("::").slice(-1)[0]}</span>
            <span className="min-w-0 break-all text-text">{e.message}</span>
          </div>
        ))}
        {logs.data && logs.data.entries.length === 0 && (
          <div className="px-3 py-6 text-center text-faint">
            No {level.toUpperCase()}+ entries in the ring
          </div>
        )}
      </div>
    </div>
  );
}

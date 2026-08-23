import { useState } from "react";

import { useHistory } from "../api/queries";
import type { JobHistoryView } from "../api/types";
import { StateBadge } from "../components/StateBadge";
import { Card, ErrorText } from "../components/ui";
import { fmtBytes } from "../lib/format";

function delta(a: number, b: number): string {
  if (a === 0 && b === 0) return "—";
  if (a === 0) return "new";
  const pct = ((b - a) / a) * 100;
  return `${pct >= 0 ? "+" : ""}${pct.toFixed(1)}%`;
}

function ComparePanel({ a, b }: { a: JobHistoryView; b: JobHistoryView }) {
  const rows: { label: string; av: string; bv: string; d: string }[] = [
    { label: "CPU", av: `${(a.cpu_nanos / 1e9).toFixed(3)}s`, bv: `${(b.cpu_nanos / 1e9).toFixed(3)}s`, d: delta(a.cpu_nanos, b.cpu_nanos) },
    { label: "Peak task memory", av: fmtBytes(a.memory_peak_task_bytes), bv: fmtBytes(b.memory_peak_task_bytes), d: delta(a.memory_peak_task_bytes, b.memory_peak_task_bytes) },
    { label: "Stages", av: String(a.stage_count), bv: String(b.stage_count), d: delta(a.stage_count, b.stage_count) },
    { label: "Tasks", av: String(a.task_count), bv: String(b.task_count), d: delta(a.task_count, b.task_count) },
    { label: "Failed tasks", av: String(a.failed_task_count), bv: String(b.failed_task_count), d: delta(a.failed_task_count, b.failed_task_count) },
  ];
  return (
    <Card className="mb-4">
      <div className="mb-2 text-sm font-semibold">
        Compare: <span className="tnum">{a.job_id}</span> → <span className="tnum">{b.job_id}</span>
      </div>
      <table className="w-full max-w-2xl text-sm">
        <thead>
          <tr className="text-left text-xs uppercase tracking-wide text-faint">
            <th className="py-1 pr-4"></th>
            <th className="py-1 pr-4">A (earlier pick)</th>
            <th className="py-1 pr-4">B</th>
            <th className="py-1">Δ</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r) => (
            <tr key={r.label} className="border-t border-border">
              <td className="py-1 pr-4 text-muted">{r.label}</td>
              <td className="py-1 pr-4 tnum">{r.av}</td>
              <td className="py-1 pr-4 tnum">{r.bv}</td>
              <td className="py-1 tnum">{r.d}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </Card>
  );
}

export function HistoryPage() {
  const { data, error } = useHistory();
  const [picked, setPicked] = useState<string[]>([]);
  const records = data?.records ?? [];
  const pick = (id: string) =>
    setPicked((p) => (p.includes(id) ? p.filter((x) => x !== id) : [...p.slice(-1), id]));
  const a = records.find((r) => r.job_id === picked[0]);
  const b = records.find((r) => r.job_id === picked[1]);
  return (
    <div>
      <h1 className="mb-4 text-lg font-semibold">
        History{data && <span className="ml-2 text-sm font-normal text-faint tnum">{data.total} completed</span>}
      </h1>
      {error && <ErrorText>{String(error)}</ErrorText>}
      {a && b && <ComparePanel a={a} b={b} />}
      {picked.length === 1 && (
        <p className="mb-2 text-xs text-faint">Pick a second run to compare.</p>
      )}
      <div className="overflow-x-auto rounded border border-border bg-surface">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-border text-left text-xs uppercase tracking-wide text-faint">
              <th className="px-3 py-2"></th>
              <th className="px-3 py-2">Job</th>
              <th className="px-3 py-2">Kind</th>
              <th className="px-3 py-2">Final state</th>
              <th className="px-3 py-2">Completed</th>
              <th className="px-3 py-2 text-right">Tasks (ok / fail)</th>
              <th className="px-3 py-2 text-right">CPU</th>
              <th className="px-3 py-2 text-right">Peak task mem</th>
            </tr>
          </thead>
          <tbody>
            {(data?.records ?? []).map((r) => (
              <tr key={r.job_id} className="border-b border-border last:border-0 hover:bg-surface-2">
                <td className="px-3 py-2">
                  <input
                    type="checkbox"
                    aria-label={`Compare ${r.job_id}`}
                    checked={picked.includes(r.job_id)}
                    onChange={() => pick(r.job_id)}
                  />
                </td>
                <td className="px-3 py-2 tnum">{r.job_id}</td>
                <td className="px-3 py-2 text-muted">{r.job_kind}</td>
                <td className="px-3 py-2"><StateBadge state={r.final_state} /></td>
                <td className="px-3 py-2 tnum text-muted">
                  {new Date(r.completed_at_ms).toISOString().replace("T", " ").slice(0, 19)}
                </td>
                <td className="px-3 py-2 text-right tnum">
                  {r.succeeded_task_count} / {r.failed_task_count}
                </td>
                <td className="px-3 py-2 text-right tnum">{(r.cpu_nanos / 1e9).toFixed(2)}s</td>
                <td className="px-3 py-2 text-right tnum">{fmtBytes(r.memory_peak_task_bytes)}</td>
              </tr>
            ))}
            {data && data.records.length === 0 && (
              <tr><td colSpan={8} className="px-3 py-6 text-center text-faint">No completed jobs yet</td></tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}

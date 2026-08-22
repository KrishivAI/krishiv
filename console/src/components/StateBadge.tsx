// Job/executor state chip — status colors come from the platform's token
// set (status colors are reserved for status; never the accent).

const TONE: Record<string, string> = {
  Running: "text-running border-running/40",
  Succeeded: "text-success border-success/40",
  Failed: "text-failed border-failed/40",
  Cancelled: "text-faint border-border-strong",
  Pending: "text-queued border-queued/40",
  Queued: "text-queued border-queued/40",
};

export function StateBadge({ state }: { state: string }) {
  const tone = TONE[state] ?? "text-muted border-border-strong";
  return (
    <span className={`inline-block rounded border px-1.5 py-0.5 text-xs ${tone}`}>{state}</span>
  );
}

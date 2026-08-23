// Cluster health strip shown on every authenticated page: leader status,
// executor liveness (heartbeat lag in ticks against the coordinator's own
// current_tick — a real reference point, not a guessed threshold), and a
// red banner when any executor is failing.

import { useExecutors, useLeader } from "../api/queries";

function Dot({ tone }: { tone: "success" | "failed" | "queued" }) {
  const color =
    tone === "success" ? "bg-success" : tone === "failed" ? "bg-failed" : "bg-queued";
  return <span className={`inline-block h-2 w-2 rounded-full ${color}`} />;
}

export function HealthHeader() {
  const leader = useLeader();
  const executors = useExecutors();
  const execs = executors.data?.executors ?? [];
  const tick = executors.data?.current_tick;
  // Heartbeat lag is reported raw (ticks behind the coordinator) — the
  // console does not invent a staleness threshold the engine doesn't have.
  const maxLag =
    tick === undefined || execs.length === 0
      ? null
      : Math.max(...execs.map((e) => tick - e.last_heartbeat_tick));
  const failing = execs.filter((e) => e.consecutive_task_failures > 0);
  const lost = execs.filter((e) => e.state === "Lost");
  const healthy = execs.filter((e) => e.state !== "Lost");
  // Zero healthy executors means nothing can be placed: every task queues
  // as Pending. This is the loudest cluster condition, not just a number.
  const noCapacity = !executors.isLoading && !executors.error && healthy.length === 0;

  return (
    <div className="mb-5">
      <div className="flex flex-wrap items-center gap-x-5 gap-y-1 border-b border-border pb-3 text-xs text-muted">
        <span className="flex items-center gap-1.5">
          <Dot tone={leader.data?.leader ? "success" : "failed"} />
          {leader.isLoading ? "leader: …" : leader.data?.leader ? "leader" : "not leader"}
        </span>
        <span className="flex items-center gap-1.5">
          <Dot tone={execs.length > 0 ? "success" : "queued"} />
          {executors.isLoading ? "executors: …" : `${execs.length} executor${execs.length === 1 ? "" : "s"}`}
        </span>
        {tick !== undefined && (
          <span className="tnum">
            tick {tick}
            {maxLag !== null && ` · max heartbeat lag ${maxLag}`}
          </span>
        )}
      </div>
      {noCapacity && (
        <div className="mt-3 rounded border border-failed/40 bg-surface px-3 py-2 text-sm text-failed">
          No healthy executors — tasks cannot be placed and will sit Pending.
          {lost.map((e) => (
            <div key={e.executor_id} className="text-xs">
              {e.executor_id} is Lost (last heartbeat tick {e.last_heartbeat_tick})
            </div>
          ))}
        </div>
      )}
      {failing.length > 0 && (
        <div className="mt-3 rounded border border-failed/40 bg-surface px-3 py-2 text-sm text-failed">
          {failing.map((e) => (
            <div key={e.executor_id}>
              {e.executor_id}: {e.consecutive_task_failures} consecutive task failure
              {e.consecutive_task_failures === 1 ? "" : "s"} (circuit breaker input)
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

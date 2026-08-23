// Client-side time series over the /api/v1/metrics-snapshot poll.
//
// The ring buffer is a module singleton so navigating away from the
// dashboard does not erase history; it holds ~10 minutes at the 3s poll.
// Every point is a server-produced sample (at_ms is the coordinator's
// clock) — the client never interpolates or backfills.

import { useQuery } from "@tanstack/react-query";

import { api } from "./client";
import type { MetricsSnapshot } from "./types";

const MAX_SAMPLES = 200;
const history: MetricsSnapshot[] = [];

function record(sample: MetricsSnapshot) {
  if (history.length > 0 && history[history.length - 1].at_ms === sample.at_ms) return;
  history.push(sample);
  if (history.length > MAX_SAMPLES) history.splice(0, history.length - MAX_SAMPLES);
}

export function useMetricsHistory(): {
  history: MetricsSnapshot[];
  latest: MetricsSnapshot | undefined;
  error: unknown;
  dataUpdatedAt: number;
} {
  const q = useQuery({
    queryKey: ["metrics-snapshot"],
    queryFn: async () => {
      const sample = await api.get<MetricsSnapshot>("/api/v1/metrics-snapshot");
      record(sample);
      return sample;
    },
    refetchInterval: 3000,
  });
  return {
    history: [...history],
    latest: q.data,
    error: q.error,
    dataUpdatedAt: q.dataUpdatedAt,
  };
}

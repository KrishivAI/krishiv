// TanStack Query hooks over the coordinator API. Poll intervals are chosen
// per surface: live pages refresh on a short cadence, detail pages faster.

import { useQuery } from "@tanstack/react-query";

import { api } from "./client";
import type {
  ContinuousListResponse,
  LiveExecutorsResponse,
  LiveJobView,
  LiveJobsResponse,
} from "./types";

export function useJobs() {
  return useQuery({
    queryKey: ["jobs"],
    queryFn: () => api.get<LiveJobsResponse>("/api/v1/jobs"),
    refetchInterval: 3000,
  });
}

export function useJob(jobId: string) {
  return useQuery({
    queryKey: ["jobs", jobId],
    queryFn: () => api.get<LiveJobView>(`/api/v1/jobs/${encodeURIComponent(jobId)}`),
    refetchInterval: 2000,
  });
}

export function useExecutors() {
  return useQuery({
    queryKey: ["executors"],
    queryFn: () => api.get<LiveExecutorsResponse>("/api/v1/executors"),
    refetchInterval: 3000,
  });
}

export function useContinuousJobs() {
  return useQuery({
    queryKey: ["continuous"],
    queryFn: () => api.get<ContinuousListResponse>("/api/v1/continuous"),
    refetchInterval: 3000,
  });
}

export function useHistory() {
  return useQuery({
    queryKey: ["history"],
    queryFn: () =>
      api.get<import("./types").JobHistoryListResponse>("/api/v1/history?limit=100"),
    refetchInterval: 10_000,
  });
}

export function useJobDiagnose(jobId: string) {
  return useQuery({
    queryKey: ["diagnose", jobId],
    queryFn: () =>
      api.get<Record<string, unknown>>(
        `/api/v1/jobs/${encodeURIComponent(jobId)}/diagnose`,
      ),
    retry: 0,
  });
}

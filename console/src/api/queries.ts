// TanStack Query hooks + mutations over the coordinator API. Poll intervals
// are chosen per surface: live pages refresh on a short cadence.

import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";

import { api } from "./client";
import type {
  ContinuousCheckpointResponse,
  ContinuousFlushResponse,
  ContinuousJobView,
  ContinuousListResponse,
  ContinuousStopWithSavepointResponse,
  ContinuousTargetsResponse,
  JobHistoryListResponse,
  LiveExecutorsResponse,
  LiveJobView,
  LiveJobsResponse,
  StageTimingResponse,
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

export function useJobStages(jobId: string) {
  return useQuery({
    queryKey: ["jobs", jobId, "stages"],
    queryFn: () =>
      api.get<StageTimingResponse>(`/api/v1/jobs/${encodeURIComponent(jobId)}/stages`),
    refetchInterval: 2000,
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

export function useCancelJob() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (jobId: string) =>
      api.post<{ cancelled: boolean }>(`/api/v1/jobs/${encodeURIComponent(jobId)}/cancel`),
    onSuccess: () => void qc.invalidateQueries({ queryKey: ["jobs"] }),
  });
}

export function useExecutors() {
  return useQuery({
    queryKey: ["executors"],
    queryFn: () => api.get<LiveExecutorsResponse>("/api/v1/executors"),
    refetchInterval: 3000,
  });
}

export function useResetExecutorBreaker() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (executorId: string) =>
      api.post<{ reset: boolean }>(
        `/api/v1/executors/${encodeURIComponent(executorId)}/reset`,
      ),
    onSuccess: () => void qc.invalidateQueries({ queryKey: ["executors"] }),
  });
}

/** Leader status: /leaderz answers 200 "leader" on the active replica. */
export function useLeader() {
  return useQuery({
    queryKey: ["leaderz"],
    queryFn: async () => {
      const res = await fetch("/leaderz");
      return { leader: res.ok };
    },
    refetchInterval: 5000,
    retry: 0,
  });
}

export function useContinuousJobs() {
  return useQuery({
    queryKey: ["continuous"],
    queryFn: () => api.get<ContinuousListResponse>("/api/v1/continuous"),
    refetchInterval: 3000,
  });
}

export function useContinuousJob(jobId: string) {
  return useQuery({
    queryKey: ["continuous", jobId],
    queryFn: () =>
      api.get<ContinuousJobView>(`/api/v1/continuous/${encodeURIComponent(jobId)}`),
    refetchInterval: 2000,
  });
}

export function useContinuousTargets(jobId: string) {
  return useQuery({
    queryKey: ["continuous", jobId, "targets"],
    queryFn: () =>
      api.get<ContinuousTargetsResponse>(
        `/api/v1/continuous/${encodeURIComponent(jobId)}/targets`,
      ),
    refetchInterval: 5000,
    retry: 0,
  });
}

export function useTriggerCheckpoint() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (jobId: string) =>
      api.post<ContinuousCheckpointResponse>(
        `/api/v1/continuous/${encodeURIComponent(jobId)}/checkpoint`,
      ),
    onSuccess: () => void qc.invalidateQueries({ queryKey: ["continuous"] }),
  });
}

export function useStopWithSavepoint() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (jobId: string) =>
      api.post<ContinuousStopWithSavepointResponse>(
        `/api/v1/continuous/${encodeURIComponent(jobId)}/stop-with-savepoint`,
      ),
    onSuccess: () => void qc.invalidateQueries({ queryKey: ["continuous"] }),
  });
}

export function useFlushStream() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (jobId: string) =>
      api.post<ContinuousFlushResponse>("/api/v1/continuous-flush", { job_id: jobId }),
    onSuccess: () => void qc.invalidateQueries({ queryKey: ["continuous"] }),
  });
}

export function useDeregisterStream() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (jobId: string) =>
      api.post<{ cancelled: boolean }>(
        `/api/v1/continuous/${encodeURIComponent(jobId)}`,
        undefined,
        "DELETE",
      ),
    onSuccess: () => void qc.invalidateQueries({ queryKey: ["continuous"] }),
  });
}

export function useHistory() {
  return useQuery({
    queryKey: ["history"],
    queryFn: () => api.get<JobHistoryListResponse>("/api/v1/history?limit=100"),
    refetchInterval: 10_000,
  });
}

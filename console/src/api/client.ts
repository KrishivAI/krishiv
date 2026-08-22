// The data layer: one fetch wrapper (quality gate mirrored from the platform
// console: no hand-written fetch calls outside this file). Adds the bearer
// header; a 401 clears the stored token and the router redirects to /login.

import { session } from "../auth/session";

export class ApiError extends Error {
  constructor(
    public status: number,
    body: string,
  ) {
    super(`HTTP ${status}: ${body.slice(0, 300)}`);
  }
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const headers = new Headers(init?.headers);
  const token = session.token();
  if (token && token !== "-") headers.set("authorization", `Bearer ${token}`);
  if (init?.body) headers.set("content-type", "application/json");
  const res = await fetch(path, { ...init, headers });
  if (res.status === 401) {
    session.clear();
    window.location.assign("/console/login");
  }
  if (!res.ok) throw new ApiError(res.status, await res.text());
  return (await res.json()) as T;
}

export const api = {
  get: <T>(path: string) => request<T>(path),
  post: <T>(path: string, body?: unknown) =>
    request<T>(path, { method: "POST", body: body === undefined ? undefined : JSON.stringify(body) }),
};

// Engine auth is a static bearer token (KRISHIV_COORDINATOR_BEARER_TOKEN on
// the daemon side) — no refresh flow, no session server. The token lives in
// localStorage; clearing it sends the router back to /login.

const KEY = "krishiv.bearer";

export const session = {
  token(): string | null {
    return localStorage.getItem(KEY);
  },
  set(token: string) {
    localStorage.setItem(KEY, token);
  },
  clear() {
    localStorage.removeItem(KEY);
  },
  /** Anonymous coordinators (DevLocal) need no token; "-" marks that choice. */
  isConfigured(): boolean {
    return localStorage.getItem(KEY) !== null;
  },
};

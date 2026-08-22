// Shared honest formatters. i64::MIN (the engine's "no watermark yet"
// sentinel) parses to -(2**63) as a JS double — render it as absent, never
// as a garbage prehistoric date.
const I64_MIN = -(2 ** 63);

export function watermark(ms: number | null | undefined): string {
  if (ms === null || ms === undefined || ms <= I64_MIN) return "—";
  return new Date(ms).toISOString().replace("T", " ").replace("Z", "");
}

export function fmtBytes(n: number): string {
  if (n >= 1 << 30) return `${(n / (1 << 30)).toFixed(1)} GiB`;
  if (n >= 1 << 20) return `${(n / (1 << 20)).toFixed(1)} MiB`;
  if (n >= 1 << 10) return `${(n / (1 << 10)).toFixed(1)} KiB`;
  return `${n} B`;
}

export function fmtMs(ms: number | null | undefined): string {
  if (ms === null || ms === undefined) return "—";
  if (ms >= 60_000) return `${(ms / 60_000).toFixed(1)}m`;
  if (ms >= 1000) return `${(ms / 1000).toFixed(2)}s`;
  return `${ms}ms`;
}

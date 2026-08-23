// Queryable-state browser over the E4.4 endpoints: list an operator's state
// names, then point-lookup a key. Keys are typed as text and sent hex-encoded
// (the wire takes raw key bytes as hex); values come back base64 and are
// shown as UTF-8 when they decode cleanly, else as hex.

import { useState } from "react";

import { api } from "../api/client";
import { useJobs, useStateNames } from "../api/queries";
import type { QueryStateResponse } from "../api/types";
import { Button, Card, ErrorText, Input, Label, StatusText } from "../components/ui";

function toHex(text: string): string {
  return Array.from(new TextEncoder().encode(text))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

function decodeValue(b64: string): { text: string; utf8: boolean } {
  const bytes = Uint8Array.from(atob(b64), (c) => c.charCodeAt(0));
  try {
    return { text: new TextDecoder("utf-8", { fatal: true }).decode(bytes), utf8: true };
  } catch {
    return {
      text: Array.from(bytes).map((b) => b.toString(16).padStart(2, "0")).join(""),
      utf8: false,
    };
  }
}

export function StateBrowserPage() {
  const jobs = useJobs();
  const [jobId, setJobId] = useState("");
  const [opId, setOpId] = useState("");
  const [key, setKey] = useState("");
  const [result, setResult] = useState<QueryStateResponse | null>(null);
  const [lookupError, setLookupError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const names = useStateNames(jobId, opId);
  const [stateName, setStateName] = useState("");

  async function lookup() {
    setBusy(true);
    setLookupError(null);
    setResult(null);
    try {
      const r = await api.get<QueryStateResponse>(
        `/api/v1/jobs/${encodeURIComponent(jobId)}/state/${encodeURIComponent(opId)}/${encodeURIComponent(stateName)}/${toHex(key)}`,
      );
      setResult(r);
    } catch (e) {
      setLookupError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div>
      <h1 className="mb-1 text-lg font-semibold">State</h1>
      <p className="mb-4 text-xs text-faint">
        Live operator state lookups (queryable state). Pick a job, name the operator, then
        look a key up — reads hit the coordinator&apos;s live store.
      </p>
      <div className="grid max-w-3xl gap-3 lg:grid-cols-3">
        <div>
          <Label htmlFor="qs-job">Job</Label>
          <select
            id="qs-job"
            value={jobId}
            onChange={(e) => setJobId(e.target.value)}
            className="w-full rounded border border-border bg-surface px-2 py-1.5 text-sm focus:border-accent focus:outline-none"
          >
            <option value="">— select —</option>
            {(jobs.data?.jobs ?? []).map((j) => (
              <option key={j.job_id} value={j.job_id}>{j.job_id}</option>
            ))}
          </select>
        </div>
        <div>
          <Label htmlFor="qs-op">Operator id</Label>
          <Input id="qs-op" value={opId} onChange={(e) => setOpId(e.target.value)} placeholder="e.g. window-0" />
        </div>
        <div>
          <Label htmlFor="qs-name">State name</Label>
          {names.data && names.data.state_names.length > 0 ? (
            <select
              id="qs-name"
              value={stateName}
              onChange={(e) => setStateName(e.target.value)}
              className="w-full rounded border border-border bg-surface px-2 py-1.5 text-sm focus:border-accent focus:outline-none"
            >
              <option value="">— select —</option>
              {names.data.state_names.map((n) => (
                <option key={n} value={n}>{n}</option>
              ))}
            </select>
          ) : (
            <Input id="qs-name" value={stateName} onChange={(e) => setStateName(e.target.value)} placeholder="state name" />
          )}
        </div>
      </div>
      {names.data && (
        <StatusText>
          {names.data.state_names.length} state name(s) registered for this operator
        </StatusText>
      )}
      {names.error && jobId && opId ? <ErrorText>{String(names.error)}</ErrorText> : null}
      <div className="mt-3 flex max-w-3xl items-end gap-2">
        <div className="flex-1">
          <Label htmlFor="qs-key">Key (text, sent as raw bytes)</Label>
          <Input id="qs-key" value={key} onChange={(e) => setKey(e.target.value)} placeholder="key" />
        </div>
        <Button disabled={busy || !jobId || !opId || !stateName} onClick={() => void lookup()}>
          Look up
        </Button>
      </div>
      {lookupError && <ErrorText>{lookupError}</ErrorText>}
      {result && (
        <Card className="mt-4 max-w-3xl">
          {result.found === "true" ? (
            (() => {
              const v = decodeValue(result.value_base64);
              return (
                <div>
                  <div className="text-xs text-faint">
                    value ({v.utf8 ? "utf-8" : "hex — not valid utf-8"})
                  </div>
                  <pre className="mt-1 overflow-x-auto text-sm">{v.text}</pre>
                </div>
              );
            })()
          ) : (
            <div className="text-sm text-muted">key not found in {result.state_name}</div>
          )}
        </Card>
      )}
    </div>
  );
}

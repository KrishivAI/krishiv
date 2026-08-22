// Token entry. The engine's auth is a static bearer (or anonymous under
// DevLocal) — no session server, so "login" just stores the token locally
// and verifies it with one authenticated read.

import { useNavigate } from "@tanstack/react-router";
import { useState } from "react";

import { api } from "../api/client";
import { session } from "../auth/session";
import { Button, Card, ErrorText, Input, Label } from "../components/ui";

export function LoginPage() {
  const navigate = useNavigate();
  const [token, setToken] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  async function submit(anonymous: boolean) {
    setBusy(true);
    setError(null);
    session.set(anonymous ? "-" : token.trim());
    try {
      await api.get("/api/v1/executors");
      void navigate({ to: "/" });
    } catch (e) {
      session.clear();
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="flex min-h-screen items-center justify-center bg-bg">
      <Card className="w-96">
        <div className="mb-4 flex items-center gap-2">
          <span className="h-2 w-2 rounded-full bg-accent" />
          <span className="text-sm font-semibold tracking-wide">krishiv engine console</span>
        </div>
        <Label htmlFor="token">Coordinator bearer token</Label>
        <Input
          id="token"
          type="password"
          value={token}
          onChange={(e) => setToken(e.target.value)}
          placeholder="KRISHIV_COORDINATOR_BEARER_TOKEN"
          onKeyDown={(e) => e.key === "Enter" && void submit(false)}
        />
        {error && <ErrorText>{error}</ErrorText>}
        <div className="mt-4 flex gap-2">
          <Button disabled={busy || !token.trim()} onClick={() => void submit(false)}>
            Connect
          </Button>
          <Button variant="ghost" disabled={busy} onClick={() => void submit(true)}>
            Anonymous (DevLocal)
          </Button>
        </div>
      </Card>
    </div>
  );
}

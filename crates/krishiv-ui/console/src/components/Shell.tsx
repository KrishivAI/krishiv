// The app shell, faithful to the platform console's Shell: BrandRow logo,
// true-black token system, icon nav with 2px accent bar on the active item,
// header with dark/light toggle (`.dark` on <html> + localStorage, same
// "krishiv.theme" key so the theme follows you between the two consoles).

import { Link, Outlet, useNavigate } from "@tanstack/react-router";
import { type ReactNode, useEffect, useState } from "react";

import { session } from "../auth/session";
import { HealthHeader } from "./HealthHeader";
import { Button } from "./ui";

const ICONS: Record<string, ReactNode> = {
  dashboard: (
    <svg viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5" className="h-[15px] w-[15px] shrink-0 opacity-75">
      <rect x="1.5" y="1.5" width="5.5" height="5.5" rx="1" />
      <rect x="9" y="1.5" width="5.5" height="5.5" rx="1" />
      <rect x="1.5" y="9" width="5.5" height="5.5" rx="1" />
      <rect x="9" y="9" width="5.5" height="5.5" rx="1" />
    </svg>
  ),
  jobs: (
    <svg viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5" className="h-[15px] w-[15px] shrink-0 opacity-75">
      <rect x="2" y="3" width="12" height="10" rx="1.8" />
      <path d="M6.6 6.2l3.6 1.8-3.6 1.8z" />
    </svg>
  ),
  streaming: (
    <svg viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5" className="h-[15px] w-[15px] shrink-0 opacity-75">
      <path d="M1.5 5c2-2 4-2 6 0s4 2 6 0M1.5 8c2-2 4-2 6 0s4 2 6 0M1.5 11c2-2 4-2 6 0s4 2 6 0" />
    </svg>
  ),
  executors: (
    <svg viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5" className="h-[15px] w-[15px] shrink-0 opacity-75">
      <rect x="2" y="2" width="12" height="5" rx="1.2" />
      <rect x="2" y="9" width="12" height="5" rx="1.2" />
      <path d="M4.5 4.5h.01M4.5 11.5h.01" />
    </svg>
  ),
  events: (
    <svg viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5" className="h-[15px] w-[15px] shrink-0 opacity-75">
      <path d="M2.5 3.5h11M2.5 8h11M2.5 12.5h7" />
    </svg>
  ),
  state: (
    <svg viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5" className="h-[15px] w-[15px] shrink-0 opacity-75">
      <circle cx="7" cy="7" r="4.5" />
      <path d="M10.5 10.5L14 14" />
    </svg>
  ),
  history: (
    <svg viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5" className="h-[15px] w-[15px] shrink-0 opacity-75">
      <circle cx="8" cy="8" r="6" />
      <path d="M8 4.5V8l2.4 1.6" />
    </svg>
  ),
  sql: (
    <svg viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5" className="h-[15px] w-[15px] shrink-0 opacity-75">
      <path d="M2 4l4 4-4 4M8 12h6" />
    </svg>
  ),
};

const NAV: { to: string; label: string; icon: string; exact?: boolean }[] = [
  { to: "/", label: "Dashboard", icon: "dashboard", exact: true },
  { to: "/jobs", label: "Jobs", icon: "jobs" },
  { to: "/streaming", label: "Streaming", icon: "streaming" },
  { to: "/executors", label: "Executors", icon: "executors" },
  { to: "/events", label: "Events", icon: "events" },
  { to: "/state", label: "State", icon: "state" },
  { to: "/history", label: "History", icon: "history" },
  { to: "/sql", label: "SQL", icon: "sql" },
];

function NavItem({ to, label, icon, exact }: (typeof NAV)[number]) {
  return (
    <Link
      to={to}
      activeOptions={exact ? { exact: true } : undefined}
      className="flex items-center gap-2.5 rounded-md border-l-2 border-transparent px-3 py-1.5 text-sm font-medium text-muted hover:bg-surface-2 hover:text-text"
      activeProps={{
        className:
          "flex items-center gap-2.5 rounded-r-md border-l-2 border-accent bg-surface-2 px-3 py-1.5 text-sm font-medium text-text",
      }}
    >
      {ICONS[icon]}
      {label}
    </Link>
  );
}

function BrandRow() {
  return (
    <div className="flex h-14 items-center gap-2.5 px-4">
      <span
        aria-hidden
        className="flex h-[26px] w-[26px] items-center justify-center rounded-md bg-accent text-[15px] font-extrabold text-black"
      >
        K
      </span>
      <span className="text-base font-bold tracking-tight">Krishiv</span>
      <span className="rounded-full border border-border-strong px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wide text-muted">
        engine
      </span>
    </div>
  );
}

function useTheme(): [string, () => void] {
  const [theme, setTheme] = useState(
    document.documentElement.classList.contains("dark") ? "dark" : "light",
  );
  useEffect(() => {
    document.documentElement.classList.toggle("dark", theme === "dark");
    localStorage.setItem("krishiv.theme", theme);
  }, [theme]);
  return [theme, () => setTheme((t) => (t === "dark" ? "light" : "dark"))];
}

export function Shell({ children }: { children?: ReactNode }) {
  const navigate = useNavigate();
  const [theme, toggleTheme] = useTheme();
  return (
    <div className="flex h-screen">
      <aside className="hidden w-56 shrink-0 flex-col border-r border-border bg-bg md:flex">
        <BrandRow />
        <nav className="flex-1 space-y-0.5 overflow-y-auto p-2" aria-label="Primary">
          {NAV.map((item) => (
            <NavItem key={item.to} {...item} />
          ))}
        </nav>
      </aside>
      <div className="flex min-w-0 flex-1 flex-col">
        <header className="flex h-14 shrink-0 items-center gap-3 border-b border-border px-3 sm:px-4">
          <span className="md:hidden">
            <BrandRow />
          </span>
          <div className="flex-1" />
          <button
            type="button"
            onClick={toggleTheme}
            aria-label={`Switch to ${theme === "dark" ? "light" : "dark"} mode`}
            className="flex items-center rounded-md p-1.5 text-muted hover:bg-surface-2 hover:text-text focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-accent"
          >
            {theme === "dark" ? (
              <svg viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5" className="h-4 w-4">
                <circle cx="8" cy="8" r="3" />
                <path d="M8 1.5V3M8 13v1.5M1.5 8H3M13 8h1.5M3.4 3.4l1 1M11.6 11.6l1 1M12.6 3.4l-1 1M4.4 11.6l-1 1" />
              </svg>
            ) : (
              <svg viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5" className="h-4 w-4">
                <path d="M13.5 9.5A6 6 0 116.5 2.5a5 5 0 007 7z" />
              </svg>
            )}
          </button>
          <Button
            variant="ghost"
            onClick={() => {
              session.clear();
              void navigate({ to: "/login" });
            }}
          >
            Reset token
          </Button>
        </header>
        <main className="min-h-0 flex-1 overflow-auto p-4 md:p-6">
          <HealthHeader />
          {children ?? <Outlet />}
        </main>
      </div>
    </div>
  );
}

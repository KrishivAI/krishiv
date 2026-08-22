// The app shell: sidebar navigation + header, reusing the platform
// console's true-black system verbatim (index.css is a copy of the
// platform's tokens): sidebar on the page ground, active nav item gets a
// 2px accent bar + surface-2, accent never becomes a text color here.

import { Link, Outlet, useNavigate } from "@tanstack/react-router";
import type { ReactNode } from "react";

import { session } from "../auth/session";
import { HealthHeader } from "./HealthHeader";
import { cx } from "./ui";

const NAV: { to: string; label: string }[] = [
  { to: "/", label: "Dashboard" },
  { to: "/jobs", label: "Jobs" },
  { to: "/streaming", label: "Streaming" },
  { to: "/executors", label: "Executors" },
  { to: "/history", label: "History" },
  { to: "/sql", label: "SQL" },
];

function NavItem({ to, label }: { to: string; label: string }) {
  return (
    <Link
      to={to}
      activeOptions={{ exact: to === "/" }}
      className="block border-l-2 border-transparent px-4 py-2 text-sm text-muted hover:bg-surface-2 hover:text-text"
      activeProps={{
        className: cx(
          "block border-l-2 px-4 py-2 text-sm",
          "border-accent bg-surface-2 text-text",
        ),
      }}
    >
      {label}
    </Link>
  );
}

export function Shell({ children }: { children?: ReactNode }) {
  const navigate = useNavigate();
  return (
    <div className="flex min-h-screen">
      <aside className="flex w-52 shrink-0 flex-col border-r border-border bg-bg">
        <div className="flex items-center gap-2 px-4 py-4">
          <span className="h-2 w-2 rounded-full bg-accent" />
          <span className="text-sm font-semibold tracking-wide">krishiv engine</span>
        </div>
        <nav className="flex-1">
          {NAV.map((item) => (
            <NavItem key={item.to} {...item} />
          ))}
        </nav>
        <button
          className="border-t border-border px-4 py-3 text-left text-xs text-faint hover:text-text"
          onClick={() => {
            session.clear();
            void navigate({ to: "/login" });
          }}
        >
          Reset token
        </button>
      </aside>
      <main className="min-w-0 flex-1 bg-bg p-6">
        <HealthHeader />
        {children ?? <Outlet />}
      </main>
    </div>
  );
}

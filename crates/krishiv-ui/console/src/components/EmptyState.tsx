// Empty states are onboarding (docs/ui-design.md): say what the screen
// will do, when it arrives, and what to do meanwhile.

import type { ReactNode } from "react";

export function EmptyState({
  title,
  children,
  badge,
}: {
  title: string;
  children: ReactNode;
  badge?: string;
}) {
  return (
    <div className="flex h-full min-h-[50vh] items-center justify-center">
      <div className="max-w-md text-center">
        {badge ? (
          <span className="mb-3 inline-block rounded-full bg-accent-soft px-3 py-0.5 text-xs font-medium text-accent">
            {badge}
          </span>
        ) : null}
        <h2 className="text-lg font-semibold">{title}</h2>
        <div className="mt-2 text-sm text-muted">{children}</div>
      </div>
    </div>
  );
}

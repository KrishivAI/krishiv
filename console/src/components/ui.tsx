// Small design-system primitives. Timeboxed to what the shell + auth
// screens need (phase-08 risk note); product screens extend from here.

import type { ButtonHTMLAttributes, InputHTMLAttributes, ReactNode } from "react";

export function cx(...parts: (string | false | undefined)[]): string {
  return parts.filter(Boolean).join(" ");
}

export function Button({
  variant = "primary",
  className,
  ...props
}: ButtonHTMLAttributes<HTMLButtonElement> & { variant?: "primary" | "ghost" | "danger" }) {
  return (
    <button
      className={cx(
        "inline-flex items-center gap-2 rounded-md px-3 py-1.5 text-sm font-medium transition-colors",
        "focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-accent",
        "disabled:cursor-not-allowed disabled:opacity-50",
        variant === "primary" && "bg-accent font-semibold text-black hover:opacity-90",
        variant === "ghost" &&
          "border border-border-strong text-muted hover:bg-surface-2 hover:text-text",
        variant === "danger" && "text-failed hover:bg-failed/10",
        className,
      )}
      {...props}
    />
  );
}

export function Input({
  className,
  ...props
}: InputHTMLAttributes<HTMLInputElement>) {
  return (
    <input
      className={cx(
        "w-full rounded-md border border-border bg-surface-2 px-3 py-1.5 text-sm",
        "placeholder:text-muted",
        "focus-visible:outline-2 focus-visible:outline-offset-1 focus-visible:outline-accent",
        className,
      )}
      {...props}
    />
  );
}

export function Card({ className, children }: { className?: string; children: ReactNode }) {
  return (
    <div className={cx("rounded-lg border border-border bg-surface p-6", className)}>
      {children}
    </div>
  );
}

export function Label({ htmlFor, children }: { htmlFor: string; children: ReactNode }) {
  return (
    <label htmlFor={htmlFor} className="mb-1 block text-sm font-medium">
      {children}
    </label>
  );
}

export function ErrorText({ children }: { children: ReactNode }) {
  return (
    <p role="alert" className="text-sm text-failed">
      {children}
    </p>
  );
}

/** Success/progress confirmation for an async action; announced politely. */
export function StatusText({ children }: { children: ReactNode }) {
  return (
    <p role="status" className="text-sm text-success">
      {children}
    </p>
  );
}

'use client';

import type { ReactNode } from 'react';

export function WarningBox({ children, title = 'Warning' }: { children: ReactNode; title?: string }) {
  return (
    <div className="docs-box docs-box-warn">
      <strong>{title}</strong>
      <div>{children}</div>
    </div>
  );
}

export function NoteBox({ children, title = 'Note' }: { children: ReactNode; title?: string }) {
  return (
    <div className="docs-box docs-box-note">
      <strong>{title}</strong>
      <div>{children}</div>
    </div>
  );
}

export function InfoBox({ children, title = 'Info' }: { children: ReactNode; title?: string }) {
  return (
    <div className="docs-box docs-box-info">
      <strong>{title}</strong>
      <div>{children}</div>
    </div>
  );
}

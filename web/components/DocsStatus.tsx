const labels = {
  available: 'Available',
  preview: 'Preview',
  experimental: 'Experimental',
  'in-progress': 'In progress',
  'coming-soon': 'Coming soon',
} as const;

export type DocsStatusName = keyof typeof labels;

export function DocsStatus({ status }: { status?: DocsStatusName }) {
  if (!status) return null;

  return (
    <span className={`docs-status docs-status-${status}`}>
      <span aria-hidden="true" />
      {labels[status]}
    </span>
  );
}

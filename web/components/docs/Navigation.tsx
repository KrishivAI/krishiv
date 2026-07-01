import Link from 'next/link';

export function NextSteps({ items }: { items: Array<{ label: string; href: string; desc?: string }> }) {
  return (
    <div className="docs-next-steps">
      <h3>Next steps</h3>
      <div className="docs-next-grid">
        {items.map((item) => (
          <Link className="docs-next-card" key={item.href} href={item.href}>
            <strong>{item.label}</strong>
            {item.desc && <span>{item.desc}</span>}
          </Link>
        ))}
      </div>
    </div>
  );
}

export function RelatedPages({ items }: { items: Array<{ label: string; href: string }> }) {
  return (
    <div className="docs-related">
      <h3>See also</h3>
      <ul>
        {items.map((item) => (
          <li key={item.href}><Link href={item.href}>{item.label}</Link></li>
        ))}
      </ul>
    </div>
  );
}

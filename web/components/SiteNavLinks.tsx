'use client';

import Link from 'next/link';
import { usePathname } from 'next/navigation';

export const navItems = [
  { label: 'Engine', href: '/engine' },
  { label: 'Platform', href: '/platform' },
  { label: 'Docs', href: '/docs' },
  { label: 'Blog', href: '/blog' },
];

export function SiteNavLinks() {
  const pathname = usePathname();

  return navItems.map((item) => {
    const active = pathname === item.href || pathname.startsWith(`${item.href}/`);

    return (
      <Link aria-current={active ? 'page' : undefined} href={item.href} key={item.href}>
        {item.label}
      </Link>
    );
  });
}

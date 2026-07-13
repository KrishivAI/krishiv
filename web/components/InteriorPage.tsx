import type { ReactNode } from 'react';

type InteriorHeroProps = {
  eyebrow: string;
  title: ReactNode;
  description: ReactNode;
  beforeTitle?: ReactNode;
  children?: ReactNode;
  aside?: ReactNode;
  compact?: boolean;
};

export function ArrowIcon() {
  return (
    <svg viewBox="0 0 20 20" aria-hidden="true">
      <path d="M4 10h12m-5-5 5 5-5 5" />
    </svg>
  );
}

export function InteriorHero({
  eyebrow,
  title,
  description,
  beforeTitle,
  children,
  aside,
  compact = false,
}: InteriorHeroProps) {
  return (
    <section className={`ip-hero${compact ? ' ip-hero-compact' : ''}`}>
      <div className={`mk-wrap ip-hero-grid${aside ? '' : ' ip-hero-grid-single'}`}>
        <div className="ip-hero-copy">
          <p className="ip-eyebrow"><i />{eyebrow}</p>
          {beforeTitle}
          <h1>{title}</h1>
          <div className="ip-lead">{description}</div>
          {children}
        </div>
        {aside && <div className="ip-hero-aside">{aside}</div>}
      </div>
    </section>
  );
}

export function DiagramFrame({ children, label }: { children: ReactNode; label: string }) {
  return (
    <div className="ip-diagram" role="region" aria-label={label} tabIndex={0}>
      {children}
      <span className="ip-diagram-hint">Scroll horizontally to inspect the full diagram.</span>
    </div>
  );
}

type SectionIntroProps = {
  eyebrow: string;
  title: ReactNode;
  description?: ReactNode;
  compact?: boolean;
};

export function SectionIntro({ eyebrow, title, description, compact = false }: SectionIntroProps) {
  return (
    <div className={`ip-section-intro${compact ? ' ip-section-intro-compact' : ''}`}>
      <div>
        <p className="ip-kicker">{eyebrow}</p>
        <h2>{title}</h2>
      </div>
      {description && <div className="ip-section-description">{description}</div>}
    </div>
  );
}

export function Eyebrow({ children }: { children: ReactNode }) {
  return <p className="ip-kicker">{children}</p>;
}

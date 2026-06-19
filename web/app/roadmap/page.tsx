import { SiteHeader } from '@/components/SiteHeader';

const priorities = [
  'Distributed batch reliability',
  'Authoritative streaming recovery',
  'User API completeness',
  'Iceberg-first lakehouse quality',
  'Open-source readiness',
];

export const metadata = {
  title: 'Roadmap',
  description: 'Krishiv public roadmap priorities.',
};

export default function RoadmapPage() {
  return (
    <main className="home-shell">
      <div className="home-container list-page">
        <SiteHeader />
        <span className="eyebrow">Roadmap</span>
        <h1>Priorities, not promises.</h1>
        <p className="section-lead">The public roadmap summarizes current focus areas while detailed development notes stay in the repository docs.</p>
        <div className="grid two">
          {priorities.map((priority) => (
            <article className="card" key={priority}>
              <h3>{priority}</h3>
              <p>Public planning page placeholder for this focus area.</p>
            </article>
          ))}
        </div>
      </div>
    </main>
  );
}

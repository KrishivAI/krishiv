import { SiteHeader } from '@/components/SiteHeader';
import { Hero } from '@/components/landing/Hero';
import { Features } from '@/components/landing/Features';
import { CodeExamples } from '@/components/landing/CodeExamples';
import { Ecosystem } from '@/components/landing/Ecosystem';
import { Architecture } from '@/components/landing/Architecture';
import { WhyKrishiv } from '@/components/landing/WhyKrishiv';
import { Cloud } from '@/components/landing/Cloud';
import { Footer } from '@/components/landing/Footer';

export default function Home() {
  return (
    <div className="landing-shell">
      <div className="landing-grid-bg" />
      <div className="landing-glow-1" />
      <div className="landing-glow-2" />
      <div className="landing-container">
        <SiteHeader />
        <Hero />
        <Features />
        <CodeExamples />
        <Ecosystem />
        <Architecture />
        <WhyKrishiv />
        <Cloud />
        <Footer />
      </div>
    </div>
  );
}

import dynamic from 'next/dynamic';
import { Bot, Zap, Shield } from "lucide-react";
import { Suspense } from 'react';

// Dynamic imports for heavy sections
const HeroSection = dynamic(() => import('@/components/landing/HeroSection'), {
  loading: () => <div className="h-[600px] w-full animate-pulse bg-white/5" />
});

const FeaturesSection = dynamic(() => import('@/components/landing/FeaturesSection'), {
  loading: () => <div className="h-[400px] w-full animate-pulse bg-white/5" />
});


export default function Home() {
  return (
    <div className="flex min-h-screen flex-col">
      {/* Navigation */}
      <header className="sticky top-0 z-50 w-full border-b border-white/10 bg-background/80 backdrop-blur-xl">
        <div className="container mx-auto flex h-16 items-center justify-between px-4 sm:px-8">
          <div className="flex items-center gap-2 font-bold text-xl tracking-tight">
            <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-primary/20 text-primary">
              <Bot className="h-5 w-5" />
            </div>
            PCA Agent
          </div>
          <nav className="hidden md:flex items-center gap-6 text-sm font-medium text-muted-foreground">
            <a href="#" className="hover:text-foreground transition-colors">Features</a>
            <a href="#" className="hover:text-foreground transition-colors">Pricing</a>
            <a href="#" className="hover:text-foreground transition-colors">Docs</a>
          </nav>
          <div className="flex items-center gap-4">
            <button className="text-sm font-medium hover:text-primary transition-colors">
              Sign In
            </button>
            <button className="rounded-full bg-primary px-4 py-2 text-sm font-bold text-white shadow-lg hover:bg-primary/90 transition-all hover:scale-105 active:scale-95">
              Get Started
            </button>
          </div>
        </div>
      </header>

      <main className="flex-1 flex flex-col">
        <Suspense fallback={<div className="h-screen w-full bg-background" />}>
          <HeroSection />
        </Suspense>

        <Suspense fallback={<div className="h-96 w-full bg-background" />}>
          <FeaturesSection />
        </Suspense>
      </main>

      <footer className="border-t border-white/10 bg-black/20 py-12">
        <div className="container mx-auto px-4 text-center text-sm text-muted-foreground">
          <p>© 2025 PCA Agent. All rights reserved.</p>
        </div>
      </footer>
    </div>
  );
}


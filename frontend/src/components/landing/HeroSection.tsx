import { ArrowRight } from "lucide-react";

export default function HeroSection() {
    return (
        <section className="relative flex flex-col items-center justify-center pt-24 pb-32 px-4 text-center overflow-hidden">
            <div className="absolute inset-0 -z-10 bg-[radial-gradient(ellipse_at_center,_var(--tw-gradient-stops))] from-primary/20 via-background to-background opacity-50" />

            <div className="animate-fade-in-up space-y-6 max-w-4xl">
                <div className="inline-flex items-center rounded-full border border-primary/20 bg-primary/10 px-3 py-1 text-sm font-medium text-primary mb-4 backdrop-blur-sm">
                    <span className="flex h-2 w-2 rounded-full bg-primary mr-2 animate-pulse"></span>
                    v2.0 is now live
                </div>

                <h1 className="text-5xl md:text-7xl font-bold tracking-tight bg-gradient-to-b from-white to-white/60 bg-clip-text text-transparent pb-2">
                    The Next Generation <br /> of Agentic AI
                </h1>

                <p className="mx-auto max-w-2xl text-lg text-muted-foreground md:text-xl leading-relaxed">
                    Empower your workflow with intelligent agents that understand, reason, and execute complex tasks with precision.
                </p>

                <div className="flex flex-col sm:flex-row items-center justify-center gap-4 pt-4">
                    <button className="group relative inline-flex h-12 items-center justify-center overflow-hidden rounded-full bg-primary px-8 font-medium text-white shadow-lg transition-all hover:bg-primary/90 hover:scale-105">
                        <span className="mr-2">Start Building</span>
                        <ArrowRight className="h-4 w-4 transition-transform group-hover:translate-x-1" />
                    </button>
                    <button className="inline-flex h-12 items-center justify-center rounded-full border border-white/10 bg-white/5 px-8 font-medium text-white shadow-sm hover:bg-white/10 transition-all backdrop-blur-sm">
                        View Documentation
                    </button>
                </div>
            </div>
        </section>
    );
}

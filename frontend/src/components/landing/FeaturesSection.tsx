import { Bot, Zap, Shield } from "lucide-react";

export default function FeaturesSection() {
    return (
        <section className="container mx-auto px-4 py-24 sm:px-8 border-t border-white/5">
            <h2 className="text-3xl font-bold text-center mb-16">Why Choose PCA Agent?</h2>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-8">
                <FeatureCard
                    icon={<Zap className="h-6 w-6 text-yellow-400" />}
                    title="Lightning Fast"
                    description="Optimized for speed and efficiency, ensuring your tasks are completed in record time."
                />
                <FeatureCard
                    icon={<Shield className="h-6 w-6 text-green-400" />}
                    title="Secure by Design"
                    description="Enterprise-grade security with end-to-end encryption for all your sensitive data."
                />
                <FeatureCard
                    icon={<Bot className="h-6 w-6 text-primary" />}
                    title="Advanced Intelligence"
                    description="Powered by state-of-the-art LLMs to handle complex reasoning and decision making."
                />
            </div>
        </section>
    );
}

function FeatureCard({ icon, title, description }: { icon: React.ReactNode, title: string, description: string }) {
    return (
        <div className="group rounded-2xl border border-white/5 bg-white/5 p-8 transition-all hover:bg-white/10 hover:border-white/10 hover:-translate-y-1">
            <div className="mb-4 inline-flex h-12 w-12 items-center justify-center rounded-xl bg-white/5 group-hover:bg-white/10 transition-colors">
                {icon}
            </div>
            <h3 className="mb-2 text-xl font-bold">{title}</h3>
            <p className="text-muted-foreground leading-relaxed">
                {description}
            </p>
        </div>
    );
}

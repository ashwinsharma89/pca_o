"use client";

import React from 'react';
import Image from 'next/image';
import Link from 'next/link';
import { motion } from 'framer-motion';
import {
    ArrowRight, CheckCircle2, BarChart3, MessageSquare,
    TrendingUp, Zap, Globe, Shield, Sparkles
} from 'lucide-react';

const FeatureCard = ({ icon: Icon, title, description }: any) => (
    <motion.div
        initial={{ opacity: 0, y: 20 }}
        whileInView={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.5 }}
        viewport={{ once: true }}
        className="group bg-white p-8 rounded-3xl border border-slate-100 hover:border-purple-200 hover:shadow-xl transition-all duration-300"
    >
        <div className="mb-6 inline-flex h-14 w-14 items-center justify-center rounded-2xl bg-gradient-to-br from-purple-500 to-purple-600 text-white shadow-lg">
            <Icon className="h-7 w-7" />
        </div>
        <h3 className="text-xl font-bold text-slate-900 mb-3">{title}</h3>
        <p className="text-slate-600 leading-relaxed">
            {description}
        </p>
    </motion.div>
);

export default function ShowcasePage() {
    return (
        <div className="min-h-screen bg-white text-slate-900 font-sans overflow-x-hidden">
            {/* Simple Navigation */}
            <nav className="fixed top-0 z-50 w-full bg-white/80 backdrop-blur-lg border-b border-slate-100">
                <div className="container mx-auto px-8 h-16 flex items-center justify-between max-w-7xl">
                    <div className="flex items-center gap-3">
                        <div className="grid grid-cols-2 gap-0.5 w-6">
                            <div className="h-2.5 w-2.5 bg-purple-600 rounded-sm" />
                            <div className="h-2.5 w-2.5 bg-purple-400 rounded-sm" />
                            <div className="h-2.5 w-2.5 bg-purple-500 rounded-sm" />
                            <div className="h-2.5 w-2.5 bg-purple-300 rounded-sm" />
                        </div>
                        <span className="text-xl font-bold text-slate-900">PCA</span>
                    </div>
                    <Link href="/visualizations-2" className="bg-gradient-to-r from-purple-600 to-purple-500 text-white px-6 py-2.5 rounded-full text-sm font-semibold hover:shadow-lg hover:scale-105 transition-all">
                        Launch Dashboard
                    </Link>
                </div>
            </nav>

            {/* Hero Section */}
            <section className="relative pt-32 pb-20 px-8 overflow-hidden">
                <div className="absolute inset-0 bg-gradient-to-b from-purple-50/50 to-white pointer-events-none" />

                <div className="container mx-auto max-w-6xl relative z-10 text-center">
                    <motion.div
                        initial={{ opacity: 0, y: 20 }}
                        animate={{ opacity: 1, y: 0 }}
                        transition={{ duration: 0.8 }}
                    >
                        <h1 className="text-7xl lg:text-8xl font-bold text-slate-900 leading-[1.1] mb-8 tracking-tight">
                            Media Intelligence.<br />
                            <span className="bg-gradient-to-r from-purple-600 to-purple-400 bg-clip-text text-transparent">Simplified.</span>
                        </h1>
                        <p className="text-2xl text-slate-600 mb-12 max-w-3xl mx-auto font-medium">
                            AI-powered analytics for modern marketing teams. Get actionable insights in seconds, not hours.
                        </p>
                        <div className="flex flex-col sm:flex-row gap-4 justify-center items-center">
                            <Link href="/visualizations-2" className="bg-gradient-to-r from-purple-600 to-purple-500 text-white px-10 py-5 rounded-full text-lg font-semibold hover:shadow-2xl hover:scale-105 transition-all flex items-center gap-3">
                                Start Free Trial
                                <ArrowRight className="h-5 w-5" />
                            </Link>
                            <button className="text-slate-600 px-10 py-5 rounded-full text-lg font-semibold hover:bg-slate-50 transition-all">
                                Watch Demo
                            </button>
                        </div>
                    </motion.div>
                </div>
            </section>

            {/* SQL Engine Agent */}
            <section className="py-32 px-8 bg-white">
                <div className="container mx-auto max-w-7xl">
                    <div className="grid lg:grid-cols-2 gap-16 items-center">
                        <motion.div
                            initial={{ opacity: 0, x: -30 }}
                            whileInView={{ opacity: 1, x: 0 }}
                            transition={{ duration: 0.6 }}
                            viewport={{ once: true }}
                        >
                            <div className="inline-flex items-center gap-2 px-4 py-2 bg-purple-50 rounded-full mb-6">
                                <MessageSquare className="h-4 w-4 text-purple-600" />
                                <span className="text-sm font-semibold text-purple-600">SQL Engine Agent</span>
                            </div>
                            <h2 className="text-5xl lg:text-6xl font-bold text-slate-900 mb-6 leading-tight">
                                Natural language<br />to SQL. Instantly.
                            </h2>
                            <p className="text-xl text-slate-600 leading-relaxed mb-8">
                                Our NL2SQL agent converts your questions into optimized database queries using advanced LLM fine-tuning and semantic parsing.
                            </p>
                            <div className="space-y-4 mb-8">
                                <div className="flex items-start gap-3">
                                    <div className="mt-1 h-6 w-6 rounded-full bg-purple-100 flex items-center justify-center shrink-0">
                                        <div className="h-2 w-2 rounded-full bg-purple-600" />
                                    </div>
                                    <div>
                                        <h4 className="font-semibold text-slate-900 mb-1">Context-Aware Parsing</h4>
                                        <p className="text-slate-600">Understands marketing terminology and platform-specific metrics</p>
                                    </div>
                                </div>
                                <div className="flex items-start gap-3">
                                    <div className="mt-1 h-6 w-6 rounded-full bg-purple-100 flex items-center justify-center shrink-0">
                                        <div className="h-2 w-2 rounded-full bg-purple-600" />
                                    </div>
                                    <div>
                                        <h4 className="font-semibold text-slate-900 mb-1">Query Optimization</h4>
                                        <p className="text-slate-600">Automatically generates indexed, performant SQL queries</p>
                                    </div>
                                </div>
                                <div className="flex items-start gap-3">
                                    <div className="mt-1 h-6 w-6 rounded-full bg-purple-100 flex items-center justify-center shrink-0">
                                        <div className="h-2 w-2 rounded-full bg-purple-600" />
                                    </div>
                                    <div>
                                        <h4 className="font-semibold text-slate-900 mb-1">Multi-Platform Joins</h4>
                                        <p className="text-slate-600">Seamlessly queries across Meta, Google, TikTok in one request</p>
                                    </div>
                                </div>
                            </div>
                            <div className="inline-flex items-center gap-2 px-4 py-2 bg-slate-50 rounded-lg border border-slate-200">
                                <code className="text-sm font-mono text-slate-700">Tech: GPT-4 + PostgreSQL + FastAPI</code>
                            </div>
                        </motion.div>

                        <motion.div
                            initial={{ opacity: 0, x: 30 }}
                            whileInView={{ opacity: 1, x: 0 }}
                            transition={{ duration: 0.6 }}
                            viewport={{ once: true }}
                            className="relative"
                        >
                            <div className="absolute -inset-4 bg-gradient-to-r from-purple-200 to-purple-100 rounded-3xl blur-2xl opacity-30" />
                            <div className="relative bg-white p-2 rounded-2xl shadow-xl border border-slate-200">
                                <Image
                                    src="/assets/showcase/google_nlq.png"
                                    alt="SQL Engine Agent"
                                    width={700}
                                    height={500}
                                    className="rounded-xl w-full h-auto"
                                />
                            </div>
                        </motion.div>
                    </div>
                </div>
            </section>

            {/* Visualization Agent */}
            <section className="py-32 px-8 bg-slate-50">
                <div className="container mx-auto max-w-7xl">
                    <div className="grid lg:grid-cols-2 gap-16 items-center">
                        <motion.div
                            initial={{ opacity: 0, x: -30 }}
                            whileInView={{ opacity: 1, x: 0 }}
                            transition={{ duration: 0.6 }}
                            viewport={{ once: true }}
                            className="order-2 lg:order-1 relative"
                        >
                            <div className="absolute -inset-4 bg-gradient-to-r from-purple-200 to-purple-100 rounded-3xl blur-2xl opacity-30" />
                            <div className="relative bg-white p-2 rounded-2xl shadow-xl border border-slate-200">
                                <Image
                                    src="/assets/showcase/google_logic.png"
                                    alt="Visualization Agent"
                                    width={700}
                                    height={500}
                                    className="rounded-xl w-full h-auto"
                                />
                            </div>
                        </motion.div>

                        <motion.div
                            initial={{ opacity: 0, x: 30 }}
                            whileInView={{ opacity: 1, x: 0 }}
                            transition={{ duration: 0.6 }}
                            viewport={{ once: true }}
                            className="order-1 lg:order-2"
                        >
                            <div className="inline-flex items-center gap-2 px-4 py-2 bg-purple-50 rounded-full mb-6">
                                <BarChart3 className="h-4 w-4 text-purple-600" />
                                <span className="text-sm font-semibold text-purple-600">Visualization Agent</span>
                            </div>
                            <h2 className="text-5xl lg:text-6xl font-bold text-slate-900 mb-6 leading-tight">
                                Charts that tell<br />the story.
                            </h2>
                            <p className="text-xl text-slate-600 leading-relaxed mb-8">
                                Our AI automatically selects the perfect chart type based on your data structure and query intent. Powered by Recharts and intelligent data analysis.
                            </p>
                            <div className="space-y-4 mb-8">
                                <div className="flex items-start gap-3">
                                    <div className="mt-1 h-6 w-6 rounded-full bg-purple-100 flex items-center justify-center shrink-0">
                                        <div className="h-2 w-2 rounded-full bg-purple-600" />
                                    </div>
                                    <div>
                                        <h4 className="font-semibold text-slate-900 mb-1">Smart Chart Selection</h4>
                                        <p className="text-slate-600">Analyzes data density to choose line, bar, pie, or scatter plots</p>
                                    </div>
                                </div>
                                <div className="flex items-start gap-3">
                                    <div className="mt-1 h-6 w-6 rounded-full bg-purple-100 flex items-center justify-center shrink-0">
                                        <div className="h-2 w-2 rounded-full bg-purple-600" />
                                    </div>
                                    <div>
                                        <h4 className="font-semibold text-slate-900 mb-1">Interactive Dashboards</h4>
                                        <p className="text-slate-600">Drill-down, filtering, and real-time updates built-in</p>
                                    </div>
                                </div>
                                <div className="flex items-start gap-3">
                                    <div className="mt-1 h-6 w-6 rounded-full bg-purple-100 flex items-center justify-center shrink-0">
                                        <div className="h-2 w-2 rounded-full bg-purple-600" />
                                    </div>
                                    <div>
                                        <h4 className="font-semibold text-slate-900 mb-1">Export Ready</h4>
                                        <p className="text-slate-600">Download as PNG, PDF, or embed in presentations</p>
                                    </div>
                                </div>
                            </div>
                            <div className="inline-flex items-center gap-2 px-4 py-2 bg-slate-50 rounded-lg border border-slate-200">
                                <code className="text-sm font-mono text-slate-700">Tech: Recharts + D3.js + Next.js</code>
                            </div>
                        </motion.div>
                    </div>
                </div>
            </section>

            {/* Reporting Agent */}
            <section className="py-32 px-8 bg-white">
                <div className="container mx-auto max-w-7xl">
                    <div className="grid lg:grid-cols-2 gap-16 items-center">
                        <motion.div
                            initial={{ opacity: 0, x: -30 }}
                            whileInView={{ opacity: 1, x: 0 }}
                            transition={{ duration: 0.6 }}
                            viewport={{ once: true }}
                        >
                            <div className="inline-flex items-center gap-2 px-4 py-2 bg-purple-50 rounded-full mb-6">
                                <TrendingUp className="h-4 w-4 text-purple-600" />
                                <span className="text-sm font-semibold text-purple-600">Reporting Agent</span>
                            </div>
                            <h2 className="text-5xl lg:text-6xl font-bold text-slate-900 mb-6 leading-tight">
                                Automated pacing<br />reports. Daily.
                            </h2>
                            <p className="text-xl text-slate-600 leading-relaxed mb-8">
                                Wake up to comprehensive pacing reports in your inbox. Our agent analyzes spend vs. targets across all campaigns and generates Excel reports with pivot tables.
                            </p>
                            <div className="space-y-4 mb-8">
                                <div className="flex items-start gap-3">
                                    <div className="mt-1 h-6 w-6 rounded-full bg-purple-100 flex items-center justify-center shrink-0">
                                        <div className="h-2 w-2 rounded-full bg-purple-600" />
                                    </div>
                                    <div>
                                        <h4 className="font-semibold text-slate-900 mb-1">Dynamic Pivot Analysis</h4>
                                        <p className="text-slate-600">Auto-generated pivot tables with SUMIF formulas for deep-dive analysis</p>
                                    </div>
                                </div>
                                <div className="flex items-start gap-3">
                                    <div className="mt-1 h-6 w-6 rounded-full bg-purple-100 flex items-center justify-center shrink-0">
                                        <div className="h-2 w-2 rounded-full bg-purple-600" />
                                    </div>
                                    <div>
                                        <h4 className="font-semibold text-slate-900 mb-1">Universal Dimension Discovery</h4>
                                        <p className="text-slate-600">Automatically detects and normalizes campaign structures across platforms</p>
                                    </div>
                                </div>
                                <div className="flex items-start gap-3">
                                    <div className="mt-1 h-6 w-6 rounded-full bg-purple-100 flex items-center justify-center shrink-0">
                                        <div className="h-2 w-2 rounded-full bg-purple-600" />
                                    </div>
                                    <div>
                                        <h4 className="font-semibold text-slate-900 mb-1">Scheduled Delivery</h4>
                                        <p className="text-slate-600">Daily, weekly, or custom schedules via email or Slack</p>
                                    </div>
                                </div>
                            </div>
                            <div className="inline-flex items-center gap-2 px-4 py-2 bg-slate-50 rounded-lg border border-slate-200">
                                <code className="text-sm font-mono text-slate-700">Tech: OpenPyXL + Celery + Python</code>
                            </div>
                        </motion.div>

                        <motion.div
                            initial={{ opacity: 0, x: 30 }}
                            whileInView={{ opacity: 1, x: 0 }}
                            transition={{ duration: 0.6 }}
                            viewport={{ once: true }}
                            className="relative"
                        >
                            <div className="absolute -inset-4 bg-gradient-to-r from-purple-200 to-purple-100 rounded-3xl blur-2xl opacity-30" />
                            <div className="relative bg-white p-2 rounded-2xl shadow-xl border border-slate-200">
                                <Image
                                    src="/assets/showcase/google_hub.png"
                                    alt="Reporting Agent"
                                    width={700}
                                    height={500}
                                    className="rounded-xl w-full h-auto"
                                />
                            </div>
                        </motion.div>
                    </div>
                </div>
            </section>

            {/* Why PCA Section */}
            <section className="py-32 px-8 bg-gradient-to-b from-purple-50 to-white">
                <div className="container mx-auto max-w-7xl">
                    <div className="text-center mb-20">
                        <h2 className="text-5xl lg:text-6xl font-bold text-slate-900 mb-6">
                            Why teams choose PCA
                        </h2>
                        <p className="text-xl text-slate-600 max-w-3xl mx-auto">
                            Join hundreds of marketing teams who've transformed their analytics workflow
                        </p>
                    </div>

                    <div className="grid md:grid-cols-2 lg:grid-cols-4 gap-8">
                        <FeatureCard
                            icon={Zap}
                            title="Save 10+ hours/week"
                            description="Automate repetitive reporting and focus on strategy instead of spreadsheets."
                        />
                        <FeatureCard
                            icon={BarChart3}
                            title="Make data-driven decisions"
                            description="Get clear recommendations backed by AI-powered attribution modeling."
                        />
                        <FeatureCard
                            icon={Globe}
                            title="Unify all platforms"
                            description="One dashboard for Meta, Google, TikTok, and all your marketing channels."
                        />
                        <FeatureCard
                            icon={Sparkles}
                            title="Instant insights"
                            description="Ask questions in plain English and get answers in seconds, not hours."
                        />
                    </div>
                </div>
            </section>

            {/* CTA Section */}
            <section className="py-32 px-8 bg-white">
                <div className="container mx-auto max-w-4xl text-center">
                    <motion.div
                        initial={{ opacity: 0, y: 20 }}
                        whileInView={{ opacity: 1, y: 0 }}
                        transition={{ duration: 0.6 }}
                        viewport={{ once: true }}
                    >
                        <h2 className="text-5xl lg:text-6xl font-bold text-slate-900 mb-6">
                            Ready to transform your analytics?
                        </h2>
                        <p className="text-xl text-slate-600 mb-12">
                            Start your free trial today. No credit card required.
                        </p>
                        <div className="flex flex-col sm:flex-row gap-4 justify-center">
                            <Link href="/visualizations-2" className="bg-gradient-to-r from-purple-600 to-purple-500 text-white px-12 py-5 rounded-full text-lg font-semibold hover:shadow-2xl hover:scale-105 transition-all">
                                Start Free Trial
                            </Link>
                            <button className="text-slate-600 px-12 py-5 rounded-full text-lg font-semibold border-2 border-slate-200 hover:bg-slate-50 transition-all">
                                Schedule Demo
                            </button>
                        </div>
                    </motion.div>
                </div>
            </section>

            {/* Footer */}
            <footer className="bg-slate-50 border-t border-slate-100 py-16 px-8">
                <div className="container mx-auto max-w-7xl">
                    <div className="grid md:grid-cols-4 gap-12">
                        <div>
                            <div className="flex items-center gap-3 mb-4">
                                <div className="grid grid-cols-2 gap-0.5 w-6">
                                    <div className="h-2.5 w-2.5 bg-purple-600 rounded-sm" />
                                    <div className="h-2.5 w-2.5 bg-purple-400 rounded-sm" />
                                    <div className="h-2.5 w-2.5 bg-purple-500 rounded-sm" />
                                    <div className="h-2.5 w-2.5 bg-purple-300 rounded-sm" />
                                </div>
                                <span className="text-xl font-bold text-slate-900">PCA</span>
                            </div>
                            <p className="text-slate-600 text-sm">
                                AI-powered media analytics for modern marketing teams.
                            </p>
                        </div>
                        <div>
                            <h5 className="font-semibold text-slate-900 mb-4">Product</h5>
                            <ul className="space-y-2 text-sm text-slate-600">
                                <li><a href="#" className="hover:text-purple-600 transition-colors">Features</a></li>
                                <li><a href="#" className="hover:text-purple-600 transition-colors">Pricing</a></li>
                                <li><a href="#" className="hover:text-purple-600 transition-colors">Integrations</a></li>
                            </ul>
                        </div>
                        <div>
                            <h5 className="font-semibold text-slate-900 mb-4">Company</h5>
                            <ul className="space-y-2 text-sm text-slate-600">
                                <li><a href="#" className="hover:text-purple-600 transition-colors">About</a></li>
                                <li><a href="#" className="hover:text-purple-600 transition-colors">Blog</a></li>
                                <li><a href="#" className="hover:text-purple-600 transition-colors">Careers</a></li>
                            </ul>
                        </div>
                        <div>
                            <h5 className="font-semibold text-slate-900 mb-4">Legal</h5>
                            <ul className="space-y-2 text-sm text-slate-600">
                                <li><a href="#" className="hover:text-purple-600 transition-colors">Privacy</a></li>
                                <li><a href="#" className="hover:text-purple-600 transition-colors">Terms</a></li>
                                <li><a href="#" className="hover:text-purple-600 transition-colors">Security</a></li>
                            </ul>
                        </div>
                    </div>
                    <div className="mt-12 pt-8 border-t border-slate-200 text-center text-sm text-slate-500">
                        © 2025 PCA Media Systems. All rights reserved.
                    </div>
                </div>
            </footer>
        </div>
    );
}

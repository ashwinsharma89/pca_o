"use client";

import { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import {
    Loader2, TrendingUp, TrendingDown, Target, BarChart3,
    Users, Smartphone, Calendar, CheckCircle, AlertTriangle,
    ArrowUpRight, ArrowDownRight, Minus, Sparkles, RefreshCw
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { api } from "@/lib/api";

interface BreakdownItem {
    name: string;
    spend: number;
    revenue: number;
    roas: number;
    cpa: number;
    conversions: number;
}

interface Insight {
    segment: string;
    dimension: string;
    metric: string;
    value: number;
    vs_avg: string;
    spend: number;
    reason: string;
}

interface Optimization {
    action: "SCALE" | "CUT" | "HOLD";
    segment: string;
    dimension: string;
    reason: string;
}

interface FunnelInsight {
    stage: string;
    goal: string;
    kpi: string;
    value: string;
    spend: number;
    insight: string;
    status: "good" | "needs_attention";
    secondary_kpi?: string;
    secondary_value?: string;
}

interface SummaryData {
    generated_at: string;
    platform_breakdown: BreakdownItem[];
    channel_breakdown: BreakdownItem[];
    funnel_breakdown: BreakdownItem[];
    device_breakdown: BreakdownItem[];
    age_breakdown: BreakdownItem[];
    what_worked: Insight[];
    what_didnt_work: Insight[];
    optimizations: Optimization[];
    funnel_insights?: FunnelInsight[];
    averages: { roas: number; cpa: number };
}

const formatCurrency = (val: number) => {
    if (val >= 1000000) return `$${(val / 1000000).toFixed(1)}M`;
    if (val >= 1000) return `$${(val / 1000).toFixed(0)}K`;
    return `$${val.toFixed(0)}`;
};

const BreakdownCard = ({ title, icon: Icon, data }: { title: string; icon: any; data: BreakdownItem[] }) => (
    <Card className="bg-slate-800/50 border-slate-700">
        <CardHeader className="pb-2">
            <CardTitle className="flex items-center gap-2 text-lg">
                <Icon className="h-5 w-5 text-blue-400" />
                {title}
            </CardTitle>
        </CardHeader>
        <CardContent>
            <div className="space-y-3">
                {data.slice(0, 5).map((item, i) => (
                    <div key={i} className="flex items-center justify-between text-sm">
                        <span className="text-slate-300 truncate max-w-[140px]" title={item.name}>
                            {item.name?.replace(/_/g, ' ').substring(0, 20)}
                        </span>
                        <div className="flex items-center gap-3">
                            <span className="text-slate-400">{formatCurrency(item.spend)}</span>
                            <Badge variant={item.roas > 4 ? "default" : item.roas > 2 ? "secondary" : "destructive"}
                                className="text-xs px-1.5">
                                {item.roas.toFixed(1)}x
                            </Badge>
                        </div>
                    </div>
                ))}
            </div>
        </CardContent>
    </Card>
);

const InsightCard = ({ insight, type }: { insight: Insight; type: "worked" | "didnt" }) => (
    <div className={`p-3 rounded-lg border ${type === "worked" ? "bg-emerald-900/20 border-emerald-700/50" : "bg-red-900/20 border-red-700/50"}`}>
        <div className="flex items-start justify-between">
            <div className="flex items-center gap-2">
                {type === "worked" ? <CheckCircle className="h-4 w-4 text-emerald-400" /> : <AlertTriangle className="h-4 w-4 text-red-400" />}
                <span className="font-medium text-slate-200">{insight.segment?.replace(/_/g, ' ')}</span>
            </div>
            <Badge variant="outline" className="text-xs">{insight.dimension}</Badge>
        </div>
        <p className="text-sm text-slate-400 mt-1.5">{insight.reason}</p>
        <div className="flex items-center gap-2 mt-2 text-xs text-slate-500">
            <span>{insight.metric}: {insight.value}</span>
            <span className={type === "worked" ? "text-emerald-400" : "text-red-400"}>{insight.vs_avg}</span>
        </div>
    </div>
);

const OptimizationCard = ({ opt }: { opt: Optimization }) => {
    const colors = {
        SCALE: { bg: "bg-emerald-900/30", border: "border-emerald-600", icon: ArrowUpRight, iconColor: "text-emerald-400" },
        CUT: { bg: "bg-red-900/30", border: "border-red-600", icon: ArrowDownRight, iconColor: "text-red-400" },
        HOLD: { bg: "bg-amber-900/30", border: "border-amber-600", icon: Minus, iconColor: "text-amber-400" },
    };
    const style = colors[opt.action];
    const Icon = style.icon;

    return (
        <div className={`p-3 rounded-lg border ${style.bg} ${style.border}`}>
            <div className="flex items-center gap-2">
                <Icon className={`h-5 w-5 ${style.iconColor}`} />
                <span className="font-semibold text-slate-200">{opt.action}</span>
                <Badge variant="outline" className="ml-auto text-xs">{opt.dimension}</Badge>
            </div>
            <p className="text-sm font-medium text-slate-300 mt-1">{opt.segment?.replace(/_/g, ' ')}</p>
            <p className="text-sm text-slate-400 mt-1">{opt.reason}</p>
        </div>
    );
};

export default function RAGSummaryPage() {
    const [loading, setLoading] = useState(true);
    const [data, setData] = useState<SummaryData | null>(null);
    const [error, setError] = useState<string | null>(null);

    const loadSummary = async () => {
        setLoading(true);
        setError(null);
        try {
            const result = await api.get('/kg/summary') as SummaryData;
            setData(result);
        } catch (e: any) {
            setError(e.message || 'Failed to load summary');
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => {
        loadSummary();
    }, []);

    if (loading) {
        return (
            <div className="flex items-center justify-center min-h-[60vh]">
                <div className="text-center space-y-4">
                    <Loader2 className="h-12 w-12 animate-spin text-blue-400 mx-auto" />
                    <p className="text-slate-400">Generating Performance Summary...</p>
                </div>
            </div>
        );
    }

    if (error || !data) {
        return (
            <div className="flex items-center justify-center min-h-[60vh]">
                <Card className="bg-red-900/20 border-red-700 p-6">
                    <p className="text-red-400">{error || "No data available"}</p>
                    <Button onClick={loadSummary} className="mt-4" variant="outline">Retry</Button>
                </Card>
            </div>
        );
    }

    return (
        <div className="p-6 space-y-6 max-w-7xl mx-auto">
            {/* Header */}
            <div className="flex items-center justify-between">
                <div>
                    <h1 className="text-2xl font-bold text-slate-100 flex items-center gap-2">
                        <Sparkles className="h-6 w-6 text-amber-400" />
                        Performance Summary
                    </h1>
                    <p className="text-slate-400 text-sm mt-1">
                        Auto-generated insights from your campaign data
                    </p>
                </div>
                <div className="flex items-center gap-3">
                    <span className="text-xs text-slate-500">
                        Generated: {new Date(data.generated_at).toLocaleString()}
                    </span>
                    <Button variant="outline" size="sm" onClick={loadSummary}>
                        <RefreshCw className="h-4 w-4 mr-1" /> Refresh
                    </Button>
                </div>
            </div>

            {/* Averages Banner */}
            <div className="grid grid-cols-2 gap-4">
                <Card className="bg-gradient-to-r from-blue-900/40 to-blue-800/20 border-blue-700/50">
                    <CardContent className="p-4 flex items-center justify-between">
                        <span className="text-slate-300">Avg ROAS</span>
                        <span className="text-2xl font-bold text-blue-300">{data.averages.roas}x</span>
                    </CardContent>
                </Card>
                <Card className="bg-gradient-to-r from-purple-900/40 to-purple-800/20 border-purple-700/50">
                    <CardContent className="p-4 flex items-center justify-between">
                        <span className="text-slate-300">Avg CPA</span>
                        <span className="text-2xl font-bold text-purple-300">${data.averages.cpa}</span>
                    </CardContent>
                </Card>
            </div>

            {/* Breakdowns Grid */}
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                <BreakdownCard title="By Platform" icon={BarChart3} data={data.platform_breakdown} />
                <BreakdownCard title="By Channel" icon={Target} data={data.channel_breakdown} />
                <BreakdownCard title="By Funnel" icon={TrendingUp} data={data.funnel_breakdown} />
                <BreakdownCard title="By Device" icon={Smartphone} data={data.device_breakdown} />
                <BreakdownCard title="By Age Group" icon={Users} data={data.age_breakdown} />
            </div>

            {/* Funnel Strategy Insights */}
            {data.funnel_insights && data.funnel_insights.length > 0 && (
                <Card className="bg-gradient-to-r from-indigo-900/30 to-purple-900/20 border-indigo-700/50">
                    <CardHeader>
                        <CardTitle className="flex items-center gap-2 text-indigo-300">
                            <TrendingUp className="h-5 w-5" />
                            Funnel Strategy Analysis
                        </CardTitle>
                        <CardDescription>KPIs matched to funnel objectives</CardDescription>
                    </CardHeader>
                    <CardContent>
                        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                            {data.funnel_insights.map((fi, i) => (
                                <div key={i} className={`p-4 rounded-lg border ${fi.status === 'good' ? 'bg-emerald-900/20 border-emerald-700/50' : 'bg-amber-900/20 border-amber-700/50'}`}>
                                    <div className="flex items-center justify-between mb-2">
                                        <span className="font-semibold text-slate-200">{fi.stage}</span>
                                        <Badge variant={fi.status === 'good' ? 'default' : 'secondary'} className="text-xs">
                                            {fi.goal}
                                        </Badge>
                                    </div>
                                    <div className="text-2xl font-bold text-slate-100 mb-1">{fi.value}</div>
                                    <div className="text-xs text-slate-400 mb-2">{fi.kpi}</div>
                                    {fi.secondary_kpi && (
                                        <div className="text-sm text-slate-300">{fi.secondary_kpi}: {fi.secondary_value}</div>
                                    )}
                                    <p className="text-sm text-slate-400 mt-2">{fi.insight}</p>
                                </div>
                            ))}
                        </div>
                    </CardContent>
                </Card>
            )}

            {/* Insights Section */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                {/* What Worked */}
                <Card className="bg-slate-800/50 border-slate-700">
                    <CardHeader>
                        <CardTitle className="flex items-center gap-2 text-emerald-400">
                            <CheckCircle className="h-5 w-5" />
                            What Worked
                        </CardTitle>
                        <CardDescription>Top performing segments with data-backed reasons</CardDescription>
                    </CardHeader>
                    <CardContent className="space-y-3">
                        {data.what_worked.length > 0 ? (
                            data.what_worked.map((insight, i) => (
                                <InsightCard key={i} insight={insight} type="worked" />
                            ))
                        ) : (
                            <p className="text-slate-500 text-sm">No standout performers identified.</p>
                        )}
                    </CardContent>
                </Card>

                {/* What Didn't Work */}
                <Card className="bg-slate-800/50 border-slate-700">
                    <CardHeader>
                        <CardTitle className="flex items-center gap-2 text-red-400">
                            <AlertTriangle className="h-5 w-5" />
                            What Didn't Work
                        </CardTitle>
                        <CardDescription>Underperforming segments requiring attention</CardDescription>
                    </CardHeader>
                    <CardContent className="space-y-3">
                        {data.what_didnt_work.length > 0 ? (
                            data.what_didnt_work.map((insight, i) => (
                                <InsightCard key={i} insight={insight} type="didnt" />
                            ))
                        ) : (
                            <p className="text-slate-500 text-sm">No major underperformers identified.</p>
                        )}
                    </CardContent>
                </Card>
            </div>

            {/* Optimizations */}
            <Card className="bg-slate-800/50 border-slate-700">
                <CardHeader>
                    <CardTitle className="flex items-center gap-2 text-amber-400">
                        <Target className="h-5 w-5" />
                        Optimization Recommendations
                    </CardTitle>
                    <CardDescription>Actionable next steps based on your data</CardDescription>
                </CardHeader>
                <CardContent>
                    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                        {data.optimizations.length > 0 ? (
                            data.optimizations.map((opt, i) => (
                                <OptimizationCard key={i} opt={opt} />
                            ))
                        ) : (
                            <p className="text-slate-500 text-sm col-span-3">No optimizations generated. Data may be uniform.</p>
                        )}
                    </div>
                </CardContent>
            </Card>
        </div>
    );
}

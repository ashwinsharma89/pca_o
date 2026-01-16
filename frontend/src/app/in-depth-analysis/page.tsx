"use client";

import { useState, useEffect, useRef } from "react";
import { useRouter } from "next/navigation";
import {
    BarChart3, Activity, Loader2, TrendingUp, TrendingDown,
    LineChart, PieChart, Download, RefreshCw, Sparkles,
    ArrowUpRight, ArrowDownRight, Minus, Plus, Settings2, X, Filter
} from "lucide-react";
import { format } from "date-fns";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Label } from "@/components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { MultiSelect } from "@/components/ui/multi-select";
import { DateRangePicker } from "@/components/ui/date-range-picker";
import { api } from "@/lib/api";
import { useAuth } from "@/context/AuthContext";
import dynamic from 'next/dynamic';
import { DateRange } from "react-day-picker";

// Dynamically import Recharts
const ResponsiveContainer = dynamic(() => import('recharts').then(mod => mod.ResponsiveContainer), { ssr: false });
const RechartsBarChart = dynamic(() => import('recharts').then(mod => mod.BarChart), { ssr: false });
const Bar = dynamic(() => import('recharts').then(mod => mod.Bar), { ssr: false });
const RechartsLineChart = dynamic(() => import('recharts').then(mod => mod.LineChart), { ssr: false });
const Line = dynamic(() => import('recharts').then(mod => mod.Line), { ssr: false });
const RechartsAreaChart = dynamic(() => import('recharts').then(mod => mod.AreaChart), { ssr: false });
const Area = dynamic(() => import('recharts').then(mod => mod.Area), { ssr: false });
const RechartsPieChart = dynamic(() => import('recharts').then(mod => mod.PieChart), { ssr: false });
const Pie = dynamic(() => import('recharts').then(mod => mod.Pie), { ssr: false });
const Cell = dynamic(() => import('recharts').then(mod => mod.Cell), { ssr: false });
const RechartsScatterChart = dynamic(() => import('recharts').then(mod => mod.ScatterChart), { ssr: false });
const Scatter = dynamic(() => import('recharts').then(mod => mod.Scatter), { ssr: false });
const RechartsComposedChart = dynamic(() => import('recharts').then(mod => mod.ComposedChart), { ssr: false });
const XAxis = dynamic(() => import('recharts').then(mod => mod.XAxis), { ssr: false });
const YAxis = dynamic(() => import('recharts').then(mod => mod.YAxis), { ssr: false });
const ZAxis = dynamic(() => import('recharts').then(mod => mod.ZAxis), { ssr: false });
const CartesianGrid = dynamic(() => import('recharts').then(mod => mod.CartesianGrid), { ssr: false });
const Tooltip = dynamic(() => import('recharts').then(mod => mod.Tooltip), { ssr: false });
const Legend = dynamic(() => import('recharts').then(mod => mod.Legend), { ssr: false });

const CHART_COLORS = ['#6366f1', '#8b5cf6', '#a855f7', '#ec4899', '#f43f5e', '#f97316', '#22c55e', '#14b8a6', '#06b6d4', '#3b82f6'];

// Chart type configurations
const CHART_TYPES = [
    { id: 'bar', name: 'Bar', icon: '📊' },
    { id: 'line', name: 'Line', icon: '📈' },
    { id: 'area', name: 'Area', icon: '🌊' },
    { id: 'pie', name: 'Pie', icon: '🥧' },
    { id: 'scatter', name: 'Scatter', icon: '🎯' },
    { id: 'stacked-bar', name: 'Stacked', icon: '📚' },
    { id: 'dual-axis', name: 'Dual', icon: '⚖️' },
];

// All available dimensions - expanded with user's suggestions
// All available dimensions - expanded with user's suggestions
const DIMENSION_OPTIONS = [
    { value: 'platform', label: 'Platform', description: 'Ad platform' },
    { value: 'channel', label: 'Channel', description: 'Marketing channel' },
    { value: 'funnel_stage', label: 'Funnel Stage', description: 'Funnel stages' },
    { value: 'objective', label: 'Objective', description: 'Campaign objective' },
    { value: 'name', label: 'Campaign Name', description: 'Campaigns' },
    { value: 'placement', label: 'Placement', description: 'Ad placement' },
    { value: 'ad_type', label: 'Ad Type', description: 'Creative type' },
    { value: 'audience_segment', label: 'Audience', description: 'Target audience' },
    { value: 'device_type', label: 'Device', description: 'Device type' },
    { value: 'region', label: 'Region', description: 'Geographic region' },
    { value: 'bid_strategy', label: 'Bid Strategy', description: 'Bidding strategy' },
];

// All available metrics for Y-axis
const METRIC_OPTIONS = [
    { value: 'spend', label: 'Spend ($)', description: 'Total ad spend' },
    { value: 'impressions', label: 'Impressions', description: 'Total impressions' },
    { value: 'clicks', label: 'Clicks', description: 'Total clicks' },
    { value: 'conversions', label: 'Conversions', description: 'Total conversions' },
    { value: 'ctr', label: 'CTR (%)', description: 'Click-through rate' },
    { value: 'cpc', label: 'CPC ($)', description: 'Cost per click' },
    { value: 'cpa', label: 'CPA ($)', description: 'Cost per acquisition' },
    { value: 'roas', label: 'ROAS', description: 'Return on ad spend' },
];

interface MainChartConfig {
    chartType: string;
    xAxis: string;
    yAxis: string;
    yAxis2?: string;
    groupBy?: string;
    aggregation: string;
}

interface QuickChart {
    id: string;
    title: string;
    chartType: string;
    xAxis: string;
    yAxis: string;
    data: any[];
    loading: boolean;
}

interface SmartFilters {
    platforms: string[];
    dateRange?: DateRange;
}

interface MetricCardData {
    title: string;
    value: number;
    previousValue?: number;
    format: 'currency' | 'number' | 'percent';
    icon: React.ReactNode;
    color: string;
}

// Animated number component (Optimized with requestAnimationFrame)
const AnimatedNumber = ({ value, format: fmt }: { value: number; format: 'currency' | 'number' | 'percent' }) => {
    const [displayValue, setDisplayValue] = useState(0);

    useEffect(() => {
        if (value === 0) {
            setDisplayValue(0);
            return;
        }

        const duration = 500; // Faster, snappier animation
        const startTime = performance.now();
        let animationFrameId: number;

        const animate = (currentTime: number) => {
            const elapsed = currentTime - startTime;
            const progress = Math.min(elapsed / duration, 1);
            // Ease-out for smooth finish
            const eased = 1 - Math.pow(1 - progress, 3);
            setDisplayValue(value * eased);

            if (progress < 1) {
                animationFrameId = requestAnimationFrame(animate);
            }
        };

        animationFrameId = requestAnimationFrame(animate);
        return () => cancelAnimationFrame(animationFrameId);
    }, [value]);

    if (fmt === 'currency') {
        return <>${displayValue >= 1000000 ? `${(displayValue / 1000000).toFixed(2)}M` : displayValue >= 1000 ? `${(displayValue / 1000).toFixed(1)}K` : displayValue.toFixed(0)}</>;
    }
    if (fmt === 'percent') {
        return <>{displayValue.toFixed(2)}%</>;
    }
    return <>{displayValue >= 1000000 ? `${(displayValue / 1000000).toFixed(2)}M` : displayValue >= 1000 ? `${(displayValue / 1000).toFixed(1)}K` : Math.round(displayValue).toLocaleString()}</>;
};

// Metric Card with Sparkline
const MetricCard = ({ data, sparklineData }: { data: MetricCardData; sparklineData?: number[] }) => {
    const change = data.previousValue ? ((data.value - data.previousValue) / data.previousValue) * 100 : 0;
    const isPositive = change > 0;

    return (
        <Card className="overflow-hidden group hover:shadow-lg transition-all duration-300 border-l-4" style={{ borderLeftColor: data.color }}>
            <CardContent className="p-4">
                <div className="flex items-start justify-between">
                    <div className="space-y-1">
                        <p className="text-xs font-medium text-muted-foreground">{data.title}</p>
                        <div className="text-xl font-bold">
                            <AnimatedNumber value={data.value} format={data.format} />
                        </div>
                        {data.previousValue && (
                            <div className={`flex items-center gap-1 text-xs ${isPositive ? 'text-green-500' : 'text-red-500'}`}>
                                {isPositive ? <ArrowUpRight className="h-3 w-3" /> : <ArrowDownRight className="h-3 w-3" />}
                                <span>{Math.abs(change).toFixed(1)}%</span>
                            </div>
                        )}
                    </div>
                    <div className="p-2 rounded-full" style={{ backgroundColor: `${data.color}20` }}>
                        {data.icon}
                    </div>
                </div>
                {sparklineData && sparklineData.length > 0 && (
                    <div className="mt-2 h-8 opacity-60 group-hover:opacity-100 transition-opacity">
                        <ResponsiveContainer width="100%" height="100%">
                            <RechartsAreaChart data={sparklineData.map((v, i) => ({ value: v, i }))}>
                                <Area type="monotone" dataKey="value" stroke={data.color} strokeWidth={1.5} fill={data.color} fillOpacity={0.2} />
                            </RechartsAreaChart>
                        </ResponsiveContainer>
                    </div>
                )}
            </CardContent>
        </Card>
    );
};

export default function InDepthAnalysisPage() {
    const { token, isLoading } = useAuth();
    const router = useRouter();
    const mainChartRef = useRef<HTMLDivElement>(null);

    const [loading, setLoading] = useState(false);
    const [mainChartData, setMainChartData] = useState<any[]>([]);
    const [metrics, setMetrics] = useState<MetricCardData[]>([]);
    const [sparklines, setSparklines] = useState<Record<string, number[]>>({});
    const [availablePlatforms, setAvailablePlatforms] = useState<{ label: string, value: string }[]>([]);

    // Main Chart Configuration
    const [mainConfig, setMainConfig] = useState<MainChartConfig>({
        chartType: 'bar',
        xAxis: 'platform',
        yAxis: 'spend',
        yAxis2: 'impressions',
        aggregation: 'sum'
    });

    // Secondary Chart Configuration
    const secondaryChartRef = useRef<HTMLDivElement>(null);
    const [secondaryLoading, setSecondaryLoading] = useState(false);
    const [secondaryChartData, setSecondaryChartData] = useState<any[]>([]);
    const [secondaryConfig, setSecondaryConfig] = useState<MainChartConfig>({
        chartType: 'bar',
        xAxis: 'platform',
        yAxis: 'spend',
        yAxis2: 'impressions',
        aggregation: 'sum'
    });

    // Quick Charts (4 small preset charts)
    const [quickCharts, setQuickCharts] = useState<QuickChart[]>([
        { id: '1', title: 'Spend by Channel', chartType: 'bar', xAxis: 'channel', yAxis: 'spend', data: [], loading: false },
        { id: '2', title: 'Conversions by Funnel', chartType: 'pie', xAxis: 'funnel_stage', yAxis: 'conversions', data: [], loading: false },
        { id: '3', title: 'CTR by Platform', chartType: 'bar', xAxis: 'platform', yAxis: 'ctr', data: [], loading: false },
        { id: '4', title: 'Clicks by Objective', chartType: 'bar', xAxis: 'objective', yAxis: 'clicks', data: [], loading: false },
    ]);

    // Global Filters
    const [filters, setFilters] = useState<SmartFilters>({ platforms: [] });

    // Auth Guard
    useEffect(() => {
        if (!isLoading && !token) router.push("/login");
    }, [isLoading, token, router]);

    useEffect(() => {
        if (token) loadInitialData();
    }, [token]);

    const loadInitialData = async () => {
        setLoading(true);
        try {
            // Use the optimized snapshot endpoint
            const response: any = await api.get('/campaigns/analytics-snapshot');

            if (response.success) {
                // 1. Set Platforms
                setAvailablePlatforms(response.platforms.map((p: string) => ({ label: p, value: p })));

                // 2. Set KPIs
                const k = response.kpis;
                setMetrics([
                    { title: 'Total Spend', value: k.total_spend, previousValue: k.total_spend * 0.92, format: 'currency', icon: <TrendingUp className="h-4 w-4" style={{ color: '#6366f1' }} />, color: '#6366f1' },
                    { title: 'Impressions', value: k.total_impressions, previousValue: k.total_impressions * 1.05, format: 'number', icon: <Activity className="h-4 w-4" style={{ color: '#22c55e' }} />, color: '#22c55e' },
                    { title: 'Total Clicks', value: k.total_clicks, previousValue: k.total_clicks * 0.88, format: 'number', icon: <BarChart3 className="h-4 w-4" style={{ color: '#f59e0b' }} />, color: '#f59e0b' },
                    { title: 'Conversions', value: k.total_conversions, previousValue: k.total_conversions * 0.95, format: 'number', icon: <Sparkles className="h-4 w-4" style={{ color: '#ec4899' }} />, color: '#ec4899' },
                ]);

                setSparklines({
                    'Total Spend': [65, 72, 68, 80, 75, 82, 90, 85, 95, 100],
                    'Impressions': [100, 95, 98, 92, 88, 90, 85, 82, 80, 78],
                    'Total Clicks': [40, 45, 42, 55, 60, 58, 65, 70, 72, 80],
                    'Conversions': [20, 22, 25, 28, 30, 32, 35, 38, 40, 42],
                });

                // 3. Set Main Chart Data
                setMainChartData(response.main_chart || []);

                // 4. Set Quick Charts Data
                setQuickCharts(prev => [
                    { ...prev[0], data: response.quick_charts.spend_by_channel, loading: false },
                    { ...prev[1], data: response.quick_charts.conversions_by_funnel, loading: false },
                    { ...prev[2], data: response.quick_charts.ctr_by_platform, loading: false },
                    { ...prev[3], data: response.quick_charts.clicks_by_objective, loading: false },
                ]);
            }
        } catch (error) {
            console.error("Failed to load analytics snapshot", error);
        } finally {
            setLoading(false);
        }
    };

    const fetchChartData = async (xAxis: string, yAxis: string, aggregation: string = 'sum', groupBy?: string, yAxis2?: string): Promise<any[]> => {
        try {
            const startDate = filters.dateRange?.from ? format(filters.dateRange.from, 'yyyy-MM-dd') : undefined;
            const endDate = filters.dateRange?.to ? format(filters.dateRange.to, 'yyyy-MM-dd') : undefined;

            const params = new URLSearchParams({
                aggregation,
                ...(filters.platforms.length > 0 && { platforms: filters.platforms.join(',') }),
                ...(startDate && { start_date: startDate }),
                ...(endDate && { end_date: endDate })
            });

            if (groupBy) {
                // Scatter plot case: X=Metric, Y=Metric, GroupBy=Dimension
                params.append('x_axis', xAxis); // xAxis is a metric here
                params.append('y_axis', yAxis);
                params.append('group_by', groupBy);
            } else if (yAxis2) {
                // Dual Axis case
                params.append('x_axis', xAxis);
                params.append('y_axis', `${yAxis},${yAxis2}`);
            } else {
                // Standard case
                params.append('x_axis', xAxis);
                params.append('y_axis', yAxis);
            }

            const response: any = await api.get(`/campaigns/chart-data?${params.toString()}`);
            return response.data || [];
        } catch (error) {
            console.error("Failed to fetch chart data", error);
            return [];
        }
    };

    const loadMainChart = async () => {
        setLoading(true);
        // Ensure secondary metric is requested for charts that support dual-axis
        const supportsDualAxis = ['dual-axis', 'stacked-bar', 'bar', 'line'].includes(mainConfig.chartType);
        const effectiveYAxis2 = supportsDualAxis && mainConfig.yAxis2 ? mainConfig.yAxis2 : undefined;

        await fetchChartData(
            mainConfig.xAxis,
            mainConfig.yAxis,
            mainConfig.aggregation,
            mainConfig.groupBy,
            effectiveYAxis2
        ).then(data => {
            setMainChartData(data);
            setLoading(false);
        });
    };

    const loadSecondaryChart = async () => {
        setSecondaryLoading(true);
        // Ensure secondary metric is requested for charts that support dual-axis
        const supportsDualAxis = ['dual-axis', 'stacked-bar', 'bar', 'line'].includes(secondaryConfig.chartType);
        const effectiveYAxis2 = supportsDualAxis && secondaryConfig.yAxis2 ? secondaryConfig.yAxis2 : undefined;

        await fetchChartData(
            secondaryConfig.xAxis,
            secondaryConfig.yAxis,
            secondaryConfig.aggregation,
            secondaryConfig.groupBy,
            effectiveYAxis2
        ).then(data => {
            setSecondaryChartData(data);
            setSecondaryLoading(false);
        });
    };

    const loadQuickCharts = async () => {
        setQuickCharts(prev => prev.map(c => ({ ...c, loading: true })));
        const loaded = await Promise.all(
            quickCharts.map(async (chart) => {
                const data = await fetchChartData(chart.xAxis, chart.yAxis);
                return { ...chart, data, loading: false };
            })
        );
        setQuickCharts(loaded);
    };

    const updateQuickChart = async (id: string, updates: Partial<QuickChart>) => {
        setQuickCharts(prev => prev.map(c => c.id === id ? { ...c, ...updates, loading: true } : c));
        const chart = quickCharts.find(c => c.id === id);
        if (chart) {
            const updated = { ...chart, ...updates };
            const data = await fetchChartData(updated.xAxis, updated.yAxis);
            setQuickCharts(prev => prev.map(c => c.id === id ? { ...c, ...updates, data, loading: false } : c));
        }
    };

    const exportChart = async (ref: React.RefObject<HTMLDivElement | null>, config: MainChartConfig) => {
        if (!ref.current) return;
        try {
            const html2canvas = (await import('html2canvas')).default;
            const canvas = await html2canvas(ref.current, { backgroundColor: null, scale: 2 });
            const link = document.createElement('a');
            link.download = `chart-${config.chartType}-${Date.now()}.png`;
            link.href = canvas.toDataURL('image/png');
            link.click();
        } catch (error) {
            console.error("Failed to export chart", error);
        }
    };

    const applyFilters = async () => {
        await loadMainChart();
        await loadQuickCharts();
    };

    const renderChart = (config: MainChartConfig, data: any[], isLoading: boolean) => {
        if (isLoading) return <div className="flex items-center justify-center h-[400px]"><Loader2 className="h-8 w-8 animate-spin text-primary" /></div>;
        if (!data.length) return <div className="flex flex-col items-center justify-center h-[400px] text-muted-foreground"><BarChart3 className="h-16 w-16 mb-4 opacity-20" /><p>Click "Generate Chart" to load data</p></div>;

        switch (config.chartType) {
            case 'bar':
                // If yAxis2 is provided, render as dual-axis ComposedChart
                if (config.yAxis2 && data.some(d => d[config.yAxis2!] !== undefined)) {
                    return (
                        <ResponsiveContainer width="100%" height={400}>
                            <RechartsComposedChart data={data}>
                                <defs><linearGradient id="barGrad" x1="0" y1="0" x2="0" y2="1"><stop offset="5%" stopColor="#6366f1" stopOpacity={0.9} /><stop offset="95%" stopColor="#6366f1" stopOpacity={0.6} /></linearGradient></defs>
                                <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
                                <XAxis dataKey={config.xAxis} className="text-xs" angle={-30} textAnchor="end" height={70} interval={0} />
                                <YAxis yAxisId="left" className="text-xs" />
                                <YAxis yAxisId="right" orientation="right" className="text-xs" />
                                <Tooltip contentStyle={{ backgroundColor: 'var(--background)', borderColor: 'var(--border)', borderRadius: '8px' }} />
                                <Legend />
                                <Bar yAxisId="left" dataKey={config.yAxis} fill="url(#barGrad)" radius={[6, 6, 0, 0]} />
                                <Line yAxisId="right" type="monotone" dataKey={config.yAxis2} stroke="#22c55e" strokeWidth={3} dot={{ fill: '#22c55e', r: 3 }} />
                            </RechartsComposedChart>
                        </ResponsiveContainer>
                    );
                }
                return (
                    <ResponsiveContainer width="100%" height={400}>
                        <RechartsBarChart data={data}>
                            <defs><linearGradient id="barGrad" x1="0" y1="0" x2="0" y2="1"><stop offset="5%" stopColor="#6366f1" stopOpacity={0.9} /><stop offset="95%" stopColor="#6366f1" stopOpacity={0.6} /></linearGradient></defs>
                            <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
                            <XAxis dataKey={config.xAxis} className="text-xs" angle={-30} textAnchor="end" height={70} interval={0} />
                            <YAxis className="text-xs" />
                            <Tooltip contentStyle={{ backgroundColor: 'var(--background)', borderColor: 'var(--border)', borderRadius: '8px' }} />
                            <Legend />
                            <Bar dataKey={config.yAxis} fill="url(#barGrad)" radius={[6, 6, 0, 0]} />
                        </RechartsBarChart>
                    </ResponsiveContainer>
                );
            case 'line':
                // If yAxis2 is provided, render as dual-axis ComposedChart
                if (config.yAxis2 && data.some(d => d[config.yAxis2!] !== undefined)) {
                    return (
                        <ResponsiveContainer width="100%" height={400}>
                            <RechartsComposedChart data={data}>
                                <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
                                <XAxis dataKey={config.xAxis} className="text-xs" angle={-30} textAnchor="end" height={70} />
                                <YAxis yAxisId="left" className="text-xs" />
                                <YAxis yAxisId="right" orientation="right" className="text-xs" />
                                <Tooltip contentStyle={{ backgroundColor: 'var(--background)', borderColor: 'var(--border)', borderRadius: '8px' }} />
                                <Legend />
                                <Line yAxisId="left" type="monotone" dataKey={config.yAxis} stroke="#6366f1" strokeWidth={3} dot={{ fill: '#6366f1', r: 4 }} />
                                <Line yAxisId="right" type="monotone" dataKey={config.yAxis2} stroke="#22c55e" strokeWidth={3} dot={{ fill: '#22c55e', r: 3 }} />
                            </RechartsComposedChart>
                        </ResponsiveContainer>
                    );
                }
                return (
                    <ResponsiveContainer width="100%" height={400}>
                        <RechartsLineChart data={data}>
                            <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
                            <XAxis dataKey={config.xAxis} className="text-xs" />
                            <YAxis className="text-xs" />
                            <Tooltip contentStyle={{ backgroundColor: 'var(--background)', borderColor: 'var(--border)', borderRadius: '8px' }} />
                            <Legend />
                            <Line type="monotone" dataKey={config.yAxis} stroke="#6366f1" strokeWidth={3} dot={{ fill: '#6366f1', r: 4 }} />
                        </RechartsLineChart>
                    </ResponsiveContainer>
                );
            case 'area':
                return (
                    <ResponsiveContainer width="100%" height={400}>
                        <RechartsAreaChart data={data}>
                            <defs><linearGradient id="areaGrad" x1="0" y1="0" x2="0" y2="1"><stop offset="5%" stopColor="#6366f1" stopOpacity={0.8} /><stop offset="95%" stopColor="#6366f1" stopOpacity={0} /></linearGradient></defs>
                            <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
                            <XAxis dataKey={config.xAxis} className="text-xs" />
                            <YAxis className="text-xs" />
                            <Tooltip contentStyle={{ backgroundColor: 'var(--background)', borderColor: 'var(--border)', borderRadius: '8px' }} />
                            <Legend />
                            <Area type="monotone" dataKey={config.yAxis} stroke="#6366f1" fill="url(#areaGrad)" />
                        </RechartsAreaChart>
                    </ResponsiveContainer>
                );
            case 'pie':
                return (
                    <ResponsiveContainer width="100%" height={400}>
                        <RechartsPieChart>
                            <Pie data={data} dataKey={config.yAxis} nameKey={config.xAxis} cx="50%" cy="50%" outerRadius={140} innerRadius={80} paddingAngle={2} label={({ percent }) => percent != null ? `${(percent * 100).toFixed(0)}%` : ''}>
                                {data.map((_, i) => <Cell key={i} fill={CHART_COLORS[i % CHART_COLORS.length]} />)}
                            </Pie>
                            <Tooltip contentStyle={{ backgroundColor: 'var(--background)', borderColor: 'var(--border)', borderRadius: '8px' }} />
                            <Legend />
                        </RechartsPieChart>
                    </ResponsiveContainer>
                );
            case 'scatter':
                return (
                    <ResponsiveContainer width="100%" height={400}>
                        <RechartsScatterChart>
                            <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
                            <XAxis dataKey={config.xAxis} name={METRIC_OPTIONS.find(m => m.value === config.xAxis)?.label || config.xAxis} className="text-xs" type="number" unit={config.xAxis === 'ctr' ? '%' : ''} />
                            <YAxis dataKey={config.yAxis} name={METRIC_OPTIONS.find(m => m.value === config.yAxis)?.label || config.yAxis} className="text-xs" type="number" unit={config.yAxis === 'ctr' ? '%' : ''} />
                            <ZAxis range={[60, 400]} />
                            <Tooltip cursor={{ strokeDasharray: '3 3' }} content={({ active, payload }) => {
                                if (active && payload && payload.length) {
                                    const data = payload[0].payload;
                                    return (
                                        <div className="bg-background border rounded-lg p-2 shadow-sm text-xs">
                                            <p className="font-semibold mb-1">{data[config.groupBy || 'name']}</p>
                                            <div className="flex flex-col gap-1">
                                                <p>{METRIC_OPTIONS.find(m => m.value === config.xAxis)?.label}: {typeof data[config.xAxis] === 'number' ? data[config.xAxis].toFixed(2) : data[config.xAxis]}</p>
                                                <p>{METRIC_OPTIONS.find(m => m.value === config.yAxis)?.label}: {typeof data[config.yAxis] === 'number' ? data[config.yAxis].toFixed(2) : data[config.yAxis]}</p>
                                            </div>
                                        </div>
                                    );
                                }
                                return null;
                            }} />
                            <Legend />
                            <Scatter name={DIMENSION_OPTIONS.find(d => d.value === (config.groupBy || 'name'))?.label || 'Items'} data={data} fill="#6366f1" />
                        </RechartsScatterChart>
                    </ResponsiveContainer>
                );
            case 'dual-axis':
                return (
                    <ResponsiveContainer width="100%" height={400}>
                        <RechartsComposedChart data={data}>
                            <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
                            <XAxis dataKey={config.xAxis} className="text-xs" angle={-30} textAnchor="end" height={70} />
                            <YAxis yAxisId="left" className="text-xs" />
                            <YAxis yAxisId="right" orientation="right" className="text-xs" />
                            <Tooltip contentStyle={{ backgroundColor: 'var(--background)', borderColor: 'var(--border)', borderRadius: '8px' }} />
                            <Legend />
                            <Bar yAxisId="left" dataKey={config.yAxis} fill="#6366f1" radius={[4, 4, 0, 0]} />
                            <Line yAxisId="right" type="monotone" dataKey={config.yAxis2 || 'ctr'} stroke="#22c55e" strokeWidth={3} />
                        </RechartsComposedChart>
                    </ResponsiveContainer>
                );
            default:
                return null;
        }
    };

    const renderQuickChart = (chart: QuickChart) => {
        if (chart.loading) return <div className="flex items-center justify-center h-full"><Loader2 className="h-6 w-6 animate-spin text-primary" /></div>;
        if (!chart.data.length) return <div className="flex items-center justify-center h-full text-xs text-muted-foreground">No data</div>;

        if (chart.chartType === 'pie') {
            return (
                <ResponsiveContainer width="100%" height="100%">
                    <RechartsPieChart>
                        <Pie data={chart.data} dataKey={chart.yAxis} nameKey={chart.xAxis} cx="50%" cy="50%" outerRadius="70%" innerRadius="45%" paddingAngle={2}>
                            {chart.data.map((_, i) => <Cell key={i} fill={CHART_COLORS[i % CHART_COLORS.length]} />)}
                        </Pie>
                        <Tooltip contentStyle={{ backgroundColor: 'var(--background)', borderColor: 'var(--border)', fontSize: '12px' }} />
                    </RechartsPieChart>
                </ResponsiveContainer>
            );
        }
        return (
            <ResponsiveContainer width="100%" height="100%">
                <RechartsBarChart data={chart.data} margin={{ top: 5, right: 5, left: 0, bottom: 25 }}>
                    <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
                    <XAxis dataKey={chart.xAxis} className="text-xs" angle={-30} textAnchor="end" interval={0} tick={{ fontSize: 10 }} />
                    <YAxis className="text-xs" tick={{ fontSize: 10 }} width={40} />
                    <Tooltip contentStyle={{ backgroundColor: 'var(--background)', borderColor: 'var(--border)', fontSize: '12px' }} />
                    <Bar dataKey={chart.yAxis} fill="#6366f1" radius={[3, 3, 0, 0]} />
                </RechartsBarChart>
            </ResponsiveContainer>
        );
    };

    if (isLoading) return <div className="flex h-screen items-center justify-center"><Loader2 className="h-8 w-8 animate-spin" /></div>;
    if (!token) return null;

    return (
        <div className="container mx-auto py-6 space-y-6">
            {/* Header */}
            <div className="flex items-center justify-between">
                <div>
                    <h1 className="text-3xl font-bold tracking-tight bg-gradient-to-r from-indigo-500 to-purple-600 bg-clip-text text-transparent">📊 Analytics Studio</h1>
                    <p className="text-muted-foreground text-sm">Build custom visualizations across your campaigns</p>
                </div>
                <Button type="button" variant="outline" size="sm" onClick={loadInitialData}><RefreshCw className="mr-2 h-4 w-4" />Refresh</Button>
            </div>

            {/* KPI Cards */}
            <div className="grid gap-3 md:grid-cols-2 lg:grid-cols-4">
                {metrics.map(m => <MetricCard key={m.title} data={m} sparklineData={sparklines[m.title]} />)}
            </div>

            {/* Global Filters */}
            <Card>
                <CardContent className="py-4">
                    <div className="flex flex-wrap gap-4 items-end">
                        <div className="space-y-1 min-w-[200px]">
                            <Label className="text-xs">Platforms</Label>
                            <MultiSelect options={availablePlatforms} selected={filters.platforms} onChange={(s) => setFilters({ ...filters, platforms: s })} placeholder="All platforms" />
                        </div>
                        <div className="space-y-1 min-w-[250px]">
                            <Label className="text-xs">Date Range</Label>
                            <DateRangePicker date={filters.dateRange} onDateChange={(r) => setFilters({ ...filters, dateRange: r })} />
                        </div>
                        <Button type="button" onClick={applyFilters} className="bg-gradient-to-r from-indigo-500 to-purple-600"><Filter className="mr-2 h-4 w-4" />Apply Filters</Button>
                    </div>
                </CardContent>
            </Card>

            {/* Main Chart Builder */}
            <div className="grid gap-4 lg:grid-cols-4">
                {/* Controls */}
                <Card className="lg:col-span-1">
                    <CardHeader className="pb-3">
                        <CardTitle className="text-lg flex items-center gap-2"><Settings2 className="h-5 w-5" />Primary Chart</CardTitle>
                    </CardHeader>
                    <CardContent className="space-y-4">
                        <div className="space-y-2">
                            <Label className="text-xs">Chart Type</Label>
                            <div className="grid grid-cols-4 gap-1">
                                {CHART_TYPES.map(t => (
                                    <Button key={t.id} type="button" size="sm" variant={mainConfig.chartType === t.id ? "default" : "outline"} className="h-10 text-lg" onClick={() => {
                                        const isNewScatter = t.id === 'scatter';
                                        const wasScatter = mainConfig.chartType === 'scatter';

                                        // Reset axes when switching between scatter and non-scatter
                                        if (isNewScatter && !wasScatter) {
                                            // Switching TO scatter: xAxis needs to be a metric
                                            setMainConfig({ ...mainConfig, chartType: t.id, xAxis: 'spend', groupBy: 'name' });
                                            setMainChartData([]); // Clear data to force re-generate
                                        } else if (!isNewScatter && wasScatter) {
                                            // Switching FROM scatter: xAxis needs to be a dimension
                                            setMainConfig({ ...mainConfig, chartType: t.id, xAxis: 'platform', groupBy: undefined });
                                            setMainChartData([]); // Clear data to force re-generate
                                        } else {
                                            // Same category (both scatter or both non-scatter)
                                            setMainConfig({ ...mainConfig, chartType: t.id });
                                        }
                                    }}>{t.icon}</Button>
                                ))}
                            </div>
                        </div>

                        {/* X-Axis: Dimension OR Metric (for Scatter) */}
                        <div className="space-y-2">
                            <Label className="text-xs">{mainConfig.chartType === 'scatter' ? 'X-Axis (Metric)' : 'X-Axis (Dimension)'}</Label>
                            <Select value={mainConfig.xAxis} onValueChange={(v) => setMainConfig({ ...mainConfig, xAxis: v })}>
                                <SelectTrigger><SelectValue /></SelectTrigger>
                                <SelectContent>
                                    {(mainConfig.chartType === 'scatter' ? METRIC_OPTIONS : DIMENSION_OPTIONS).map(o => (
                                        <SelectItem key={o.value} value={o.value}>{o.label}</SelectItem>
                                    ))}
                                </SelectContent>
                            </Select>
                        </div>

                        {/* Y-Axis: Metric in all cases */}
                        <div className="space-y-2">
                            <Label className="text-xs">Y-Axis (Metric)</Label>
                            <Select value={mainConfig.yAxis} onValueChange={(v) => setMainConfig({ ...mainConfig, yAxis: v })}>
                                <SelectTrigger><SelectValue /></SelectTrigger>
                                <SelectContent>
                                    {METRIC_OPTIONS.map(m => <SelectItem key={m.value} value={m.value}>{m.label}</SelectItem>)}
                                </SelectContent>
                            </Select>
                        </div>

                        {/* Group By (Only for Scatter) */}
                        {mainConfig.chartType === 'scatter' && (
                            <div className="space-y-2">
                                <Label className="text-xs">Group By (Entity)</Label>
                                <Select value={mainConfig.groupBy || 'name'} onValueChange={(v) => setMainConfig({ ...mainConfig, groupBy: v })}>
                                    <SelectTrigger><SelectValue /></SelectTrigger>
                                    <SelectContent>
                                        {DIMENSION_OPTIONS.map(d => <SelectItem key={d.value} value={d.value}>{d.label}</SelectItem>)}
                                    </SelectContent>
                                </Select>
                            </div>
                        )}

                        {/* Secondary Metric (For Dual Axis, Stacked, Bar, or Line) */}
                        {['dual-axis', 'stacked-bar', 'bar', 'line'].includes(mainConfig.chartType) && (
                            <div className="space-y-2">
                                <Label className="text-xs">Secondary Metric</Label>
                                <Select value={mainConfig.yAxis2 || 'ctr'} onValueChange={(v) => setMainConfig({ ...mainConfig, yAxis2: v })}>
                                    <SelectTrigger><SelectValue /></SelectTrigger>
                                    <SelectContent>
                                        {METRIC_OPTIONS.map(m => <SelectItem key={m.value} value={m.value}>{m.label}</SelectItem>)}
                                    </SelectContent>
                                </Select>
                            </div>
                        )}

                        <div className="space-y-2">
                            <Label className="text-xs">Aggregation</Label>
                            <Select value={mainConfig.aggregation} onValueChange={(v) => setMainConfig({ ...mainConfig, aggregation: v })}>
                                <SelectTrigger><SelectValue /></SelectTrigger>
                                <SelectContent>
                                    <SelectItem value="sum">Sum</SelectItem>
                                    <SelectItem value="avg">Average</SelectItem>
                                    <SelectItem value="count">Count</SelectItem>
                                </SelectContent>
                            </Select>
                        </div>
                        <Button type="button" onClick={loadMainChart} className="w-full bg-gradient-to-r from-indigo-500 to-purple-600"><Sparkles className="mr-2 h-4 w-4" />Generate</Button>
                    </CardContent>
                </Card>

                {/* Main Chart Display */}
                <Card className="lg:col-span-3">
                    <CardHeader className="pb-2 flex flex-row items-center justify-between">
                        <div>
                            <CardTitle>Visualization</CardTitle>
                            <CardDescription>{mainChartData.length > 0 ? `${mainChartData.length} data points` : 'Configure and generate'}</CardDescription>
                        </div>
                        {mainChartData.length > 0 && <Button type="button" variant="outline" size="sm" onClick={() => exportChart(mainChartRef, mainConfig)}><Download className="mr-2 h-4 w-4" />Export PNG</Button>}
                    </CardHeader>
                    <CardContent ref={mainChartRef}>{renderChart(mainConfig, mainChartData, loading)}</CardContent>
                </Card>
            </div>

            {/* Secondary Chart Builder */}
            <div className="grid gap-4 lg:grid-cols-4">
                {/* Controls */}
                <Card className="lg:col-span-1">
                    <CardHeader className="pb-3">
                        <CardTitle className="text-lg flex items-center gap-2"><Activity className="h-5 w-5" />Secondary Chart</CardTitle>
                    </CardHeader>
                    <CardContent className="space-y-4">
                        <div className="space-y-2">
                            <Label className="text-xs">Chart Type</Label>
                            <div className="grid grid-cols-4 gap-1">
                                {CHART_TYPES.map(t => (
                                    <Button key={t.id} type="button" size="sm" variant={secondaryConfig.chartType === t.id ? "default" : "outline"} className="h-10 text-lg" onClick={() => {
                                        const isNewScatter = t.id === 'scatter';
                                        const wasScatter = secondaryConfig.chartType === 'scatter';

                                        // Reset axes when switching between scatter and non-scatter
                                        if (isNewScatter && !wasScatter) {
                                            // Switching TO scatter: xAxis needs to be a metric
                                            setSecondaryConfig({ ...secondaryConfig, chartType: t.id, xAxis: 'spend', groupBy: 'name' });
                                            setSecondaryChartData([]); // Clear data to force re-generate
                                        } else if (!isNewScatter && wasScatter) {
                                            // Switching FROM scatter: xAxis needs to be a dimension
                                            setSecondaryConfig({ ...secondaryConfig, chartType: t.id, xAxis: 'platform', groupBy: undefined });
                                            setSecondaryChartData([]); // Clear data to force re-generate
                                        } else {
                                            // Same category (both scatter or both non-scatter)
                                            setSecondaryConfig({ ...secondaryConfig, chartType: t.id });
                                        }
                                    }}>{t.icon}</Button>
                                ))}
                            </div>
                        </div>

                        {/* X-Axis: Dimension OR Metric (for Scatter) */}
                        <div className="space-y-2">
                            <Label className="text-xs">{secondaryConfig.chartType === 'scatter' ? 'X-Axis (Metric)' : 'X-Axis (Dimension)'}</Label>
                            <Select value={secondaryConfig.xAxis} onValueChange={(v) => setSecondaryConfig({ ...secondaryConfig, xAxis: v })}>
                                <SelectTrigger><SelectValue /></SelectTrigger>
                                <SelectContent>
                                    {(secondaryConfig.chartType === 'scatter' ? METRIC_OPTIONS : DIMENSION_OPTIONS).map(o => (
                                        <SelectItem key={o.value} value={o.value}>{o.label}</SelectItem>
                                    ))}
                                </SelectContent>
                            </Select>
                        </div>

                        {/* Y-Axis: Metric in all cases */}
                        <div className="space-y-2">
                            <Label className="text-xs">Y-Axis (Metric)</Label>
                            <Select value={secondaryConfig.yAxis} onValueChange={(v) => setSecondaryConfig({ ...secondaryConfig, yAxis: v })}>
                                <SelectTrigger><SelectValue /></SelectTrigger>
                                <SelectContent>
                                    {METRIC_OPTIONS.map(m => <SelectItem key={m.value} value={m.value}>{m.label}</SelectItem>)}
                                </SelectContent>
                            </Select>
                        </div>

                        {/* Group By (Only for Scatter) */}
                        {secondaryConfig.chartType === 'scatter' && (
                            <div className="space-y-2">
                                <Label className="text-xs">Group By (Entity)</Label>
                                <Select value={secondaryConfig.groupBy || 'name'} onValueChange={(v) => setSecondaryConfig({ ...secondaryConfig, groupBy: v })}>
                                    <SelectTrigger><SelectValue /></SelectTrigger>
                                    <SelectContent>
                                        {DIMENSION_OPTIONS.map(d => <SelectItem key={d.value} value={d.value}>{d.label}</SelectItem>)}
                                    </SelectContent>
                                </Select>
                            </div>
                        )}

                        {/* Secondary Metric (For Dual Axis, Stacked, Bar, or Line) */}
                        {['dual-axis', 'stacked-bar', 'bar', 'line'].includes(secondaryConfig.chartType) && (
                            <div className="space-y-2">
                                <Label className="text-xs">Secondary Metric</Label>
                                <Select value={secondaryConfig.yAxis2 || 'ctr'} onValueChange={(v) => setSecondaryConfig({ ...secondaryConfig, yAxis2: v })}>
                                    <SelectTrigger><SelectValue /></SelectTrigger>
                                    <SelectContent>
                                        {METRIC_OPTIONS.map(m => <SelectItem key={m.value} value={m.value}>{m.label}</SelectItem>)}
                                    </SelectContent>
                                </Select>
                            </div>
                        )}

                        <div className="space-y-2">
                            <Label className="text-xs">Aggregation</Label>
                            <Select value={secondaryConfig.aggregation} onValueChange={(v) => setSecondaryConfig({ ...secondaryConfig, aggregation: v })}>
                                <SelectTrigger><SelectValue /></SelectTrigger>
                                <SelectContent>
                                    <SelectItem value="sum">Sum</SelectItem>
                                    <SelectItem value="avg">Average</SelectItem>
                                    <SelectItem value="count">Count</SelectItem>
                                </SelectContent>
                            </Select>
                        </div>
                        <Button type="button" onClick={loadSecondaryChart} className="w-full bg-gradient-to-r from-purple-500 to-indigo-600"><Sparkles className="mr-2 h-4 w-4" />Generate</Button>
                    </CardContent>
                </Card>

                {/* Secondary Chart Display */}
                <Card className="lg:col-span-3">
                    <CardHeader className="pb-2 flex flex-row items-center justify-between">
                        <div>
                            <CardTitle>Visualization</CardTitle>
                            <CardDescription>{secondaryChartData.length > 0 ? `${secondaryChartData.length} data points` : 'Configure and generate'}</CardDescription>
                        </div>
                        {secondaryChartData.length > 0 && <Button type="button" variant="outline" size="sm" onClick={() => exportChart(secondaryChartRef, secondaryConfig)}><Download className="mr-2 h-4 w-4" />Export PNG</Button>}
                    </CardHeader>
                    <CardContent ref={secondaryChartRef}>{renderChart(secondaryConfig, secondaryChartData, secondaryLoading)}</CardContent>
                </Card>
            </div>

            {/* Quick Charts Grid */}
            <div>
                <h2 className="text-lg font-semibold mb-3">Quick Insights</h2>
                <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
                    {quickCharts.map(chart => (
                        <Card key={chart.id} className="group">
                            <CardHeader className="py-2 px-3">
                                <div className="flex items-center justify-between">
                                    <CardTitle className="text-sm">{chart.title}</CardTitle>
                                    <Select value={chart.yAxis} onValueChange={(v) => updateQuickChart(chart.id, { yAxis: v })}>
                                        <SelectTrigger className="h-6 w-20 text-xs border-0 bg-transparent"><SelectValue /></SelectTrigger>
                                        <SelectContent>
                                            {METRIC_OPTIONS.map(m => <SelectItem key={m.value} value={m.value} className="text-xs">{m.label}</SelectItem>)}
                                        </SelectContent>
                                    </Select>
                                </div>
                            </CardHeader>
                            <CardContent className="h-[180px] p-2">{renderQuickChart(chart)}</CardContent>
                        </Card>
                    ))}
                </div>
            </div>
        </div>
    );
}

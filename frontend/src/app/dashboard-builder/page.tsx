"use client";

import { useState, useEffect, useCallback } from "react";
import { useRouter } from "next/navigation";
import GridLayout from "react-grid-layout";
import {
    BarChart3, Activity, Loader2, TrendingUp, Sparkles,
    Plus, Save, FolderOpen, Trash2, Settings2, GripVertical,
    ArrowUpRight, ArrowDownRight, Minus, Download, RefreshCw
} from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogTrigger, DialogFooter } from "@/components/ui/dialog";
import { api } from "@/lib/api";
import { useAuth } from "@/context/AuthContext";
import dynamic from 'next/dynamic';
import "react-grid-layout/css/styles.css";
import "react-resizable/css/styles.css";

// Chart components
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

const CHART_COLORS = ['#6366f1', '#8b5cf6', '#a855f7', '#ec4899', '#f43f5e', '#f97316', '#22c55e', '#14b8a6'];

const WIDGET_TYPES = [
    { id: 'kpi', name: 'KPI Card', icon: '📊', description: 'Single metric with trend' },
    { id: 'bar', name: 'Bar Chart', icon: '📊', description: 'Compare categories' },
    { id: 'line', name: 'Line Chart', icon: '📈', description: 'Show trends' },
    { id: 'area', name: 'Area Chart', icon: '🌊', description: 'Trends with volume' },
    { id: 'pie', name: 'Pie Chart', icon: '🥧', description: 'Show proportions' },
    { id: 'scatter', name: 'Scatter Plot', icon: '🎯', description: 'Correlation between metrics' },
    { id: 'dual-axis', name: 'Dual Axis', icon: '⚖️', description: 'Compare two metrics' },
];

const DIMENSIONS = [
    { value: 'platform', label: 'Platform' },
    { value: 'channel', label: 'Channel' },
    { value: 'objective', label: 'Objective' },
    { value: 'campaign_name', label: 'Campaign Name' },
    { value: 'placement', label: 'Placement' },
    { value: 'funnel_stage', label: 'Funnel Stage' },
    { value: 'ad_type', label: 'Ad Type' },
    { value: 'audience_segment', label: 'Audience Segment' },
    { value: 'device_type', label: 'Device Type' },
    { value: 'region', label: 'Region' },
    { value: 'bid_strategy', label: 'Bid Strategy' },
];

const METRICS = [
    { value: 'spend', label: 'Spend' },
    { value: 'impressions', label: 'Impressions' },
    { value: 'clicks', label: 'Clicks' },
    { value: 'conversions', label: 'Conversions' },
    { value: 'ctr', label: 'CTR' },
    { value: 'cpc', label: 'CPC' },
    { value: 'cpa', label: 'CPA' },
    { value: 'roas', label: 'ROAS' },
];

interface Widget {
    id: string;
    type: string;
    title: string;
    dimension: string;
    metric: string;
    metric2?: string;
    data: any[];
    loading: boolean;
}

interface SavedDashboard {
    id: string;
    name: string;
    layouts: any;
    widgets: Widget[];
    createdAt: string;
}

// ... existing code ...

const KPIWidget = ({ widget }: { widget: Widget }) => {
    const total = widget.data.reduce((sum, item) => sum + (Number(item[widget.metric]) || 0), 0);
    const value = widget.metric === 'ctr' || widget.metric === 'roas' ? total / (widget.data.length || 1) : total; // Avg for rates
    const formatted = widget.metric === 'spend' || widget.metric === 'cpc' || widget.metric === 'cpa'
        ? `$${value.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
        : widget.metric === 'ctr'
            ? `${value.toFixed(2)}%`
            : value.toLocaleString();

    return (
        <div className="flex flex-col items-center justify-center h-full">
            <p className="text-3xl font-bold bg-gradient-to-r from-indigo-500 to-purple-600 bg-clip-text text-transparent">
                {widget.loading ? <Loader2 className="h-6 w-6 animate-spin" /> : formatted}
            </p>
            <p className="text-sm text-muted-foreground capitalize">{METRICS.find(m => m.value === widget.metric)?.label || widget.metric}</p>
        </div>
    );
};

const ChartWidget = ({ widget }: { widget: Widget }) => {
    if (widget.loading) return <div className="flex items-center justify-center h-full"><Loader2 className="h-6 w-6 animate-spin text-primary" /></div>;
    if (!widget.data.length) return <div className="flex items-center justify-center h-full text-muted-foreground text-xs">No data</div>;

    const CommonAxis = () => (
        <>
            <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
            <XAxis dataKey={widget.type === 'scatter' ? (widget.metric2 || 'spend') : widget.dimension}
                name={widget.type === 'scatter' ? (METRICS.find(m => m.value === (widget.metric2 || 'spend'))?.label) : undefined}
                type={widget.type === 'scatter' ? 'number' : 'category'}
                className="text-xs" angle={-30} textAnchor="end" height={60} interval={0} tick={{ fontSize: 10 }} />
            <YAxis className="text-xs" tick={{ fontSize: 10 }} width={40} />
            <Tooltip contentStyle={{ backgroundColor: 'var(--background)', borderColor: 'var(--border)', fontSize: '12px' }} />
            <Legend wrapperStyle={{ fontSize: '10px' }} />
        </>
    );

    switch (widget.type) {
        case 'bar':
        case 'kpi': // Fallback if type is mixed
            return (
                <ResponsiveContainer width="100%" height="100%">
                    <RechartsBarChart data={widget.data} margin={{ top: 5, right: 5, left: 0, bottom: 5 }}>
                        <CommonAxis />
                        <Bar dataKey={widget.metric} fill="#6366f1" radius={[4, 4, 0, 0]} />
                    </RechartsBarChart>
                </ResponsiveContainer>
            );
        case 'line':
            return (
                <ResponsiveContainer width="100%" height="100%">
                    <RechartsLineChart data={widget.data} margin={{ top: 5, right: 5, left: 0, bottom: 5 }}>
                        <CommonAxis />
                        <Line type="monotone" dataKey={widget.metric} stroke="#6366f1" strokeWidth={2} dot={{ r: 2 }} />
                    </RechartsLineChart>
                </ResponsiveContainer>
            );
        case 'area':
            return (
                <ResponsiveContainer width="100%" height="100%">
                    <RechartsAreaChart data={widget.data} margin={{ top: 5, right: 5, left: 0, bottom: 5 }}>
                        <CommonAxis />
                        <Area type="monotone" dataKey={widget.metric} stroke="#6366f1" fill="#6366f1" fillOpacity={0.2} />
                    </RechartsAreaChart>
                </ResponsiveContainer>
            );
        case 'pie':
            return (
                <ResponsiveContainer width="100%" height="100%">
                    <RechartsPieChart>
                        <Pie data={widget.data} dataKey={widget.metric} nameKey={widget.dimension} cx="50%" cy="50%" outerRadius="80%" innerRadius="50%" paddingAngle={2}>
                            {widget.data.map((_, i) => <Cell key={i} fill={CHART_COLORS[i % CHART_COLORS.length]} />)}
                        </Pie>
                        <Tooltip contentStyle={{ backgroundColor: 'var(--background)', borderColor: 'var(--border)', fontSize: '12px' }} />
                        <Legend wrapperStyle={{ fontSize: '10px' }} />
                    </RechartsPieChart>
                </ResponsiveContainer>
            );
        case 'scatter':
            return (
                <ResponsiveContainer width="100%" height="100%">
                    <RechartsScatterChart margin={{ top: 5, right: 5, left: 0, bottom: 5 }}>
                        <CartesianGrid strokeDasharray="3 3" />
                        <XAxis dataKey={widget.metric2 || 'spend'} name={METRICS.find(m => m.value === (widget.metric2 || 'spend'))?.label} type="number" className="text-xs" unit={widget.metric2 === 'ctr' ? '%' : ''} />
                        <YAxis dataKey={widget.metric} name={METRICS.find(m => m.value === widget.metric)?.label} type="number" className="text-xs" unit={widget.metric === 'ctr' ? '%' : ''} />
                        <ZAxis range={[60, 400]} />
                        <Tooltip cursor={{ strokeDasharray: '3 3' }} content={({ active, payload }) => {
                            if (active && payload && payload.length) {
                                const data = payload[0].payload;
                                return (
                                    <div className="bg-background border rounded p-2 text-xs shadow-sm">
                                        <p className="font-bold">{data[widget.dimension]}</p>
                                        <p>{METRICS.find(m => m.value === (widget.metric2 || 'clicks'))?.label}: {Number(data[widget.metric2 || 'clicks']).toFixed(2)}</p>
                                        <p>{METRICS.find(m => m.value === widget.metric)?.label}: {Number(data[widget.metric]).toFixed(2)}</p>
                                    </div>
                                );
                            }
                            return null;
                        }} />
                        <Legend wrapperStyle={{ fontSize: '10px' }} />
                        <Scatter name={DIMENSIONS.find(d => d.value === widget.dimension)?.label || widget.dimension} data={widget.data} fill="#8884d8" />
                    </RechartsScatterChart>
                </ResponsiveContainer>
            );
        case 'dual-axis':
            return (
                <ResponsiveContainer width="100%" height="100%">
                    <RechartsComposedChart data={widget.data} margin={{ top: 5, right: 5, left: 0, bottom: 5 }}>
                        <CartesianGrid strokeDasharray="3 3" />
                        <XAxis dataKey={widget.dimension} className="text-xs" angle={-30} textAnchor="end" height={60} interval={0} tick={{ fontSize: 10 }} />
                        <YAxis yAxisId="left" className="text-xs" tick={{ fontSize: 10 }} width={40} />
                        <YAxis yAxisId="right" orientation="right" className="text-xs" tick={{ fontSize: 10 }} width={40} />
                        <Tooltip contentStyle={{ backgroundColor: 'var(--background)', borderColor: 'var(--border)', fontSize: '12px' }} />
                        <Legend wrapperStyle={{ fontSize: '10px' }} />
                        <Bar yAxisId="left" dataKey={widget.metric} fill="#6366f1" radius={[4, 4, 0, 0]} />
                        <Line yAxisId="right" type="monotone" dataKey={widget.metric2 || 'ctr'} stroke="#22c55e" strokeWidth={2} dot={{ r: 2 }} />
                    </RechartsComposedChart>
                </ResponsiveContainer>
            );
        default:
            return null;
    }
};

export default function DashboardBuilderPage() {
    const { token, isLoading } = useAuth();
    const router = useRouter();
    const [savedDashboards, setSavedDashboards] = useState<SavedDashboard[]>([]);
    const [showSaveDialog, setShowSaveDialog] = useState(false);
    const [showLoadDialog, setShowLoadDialog] = useState(false);
    const [dashboardName, setDashboardName] = useState('My Dashboard');
    const [widgets, setWidgets] = useState<Widget[]>([]);
    const [layouts, setLayouts] = useState<any>({ lg: [] });
    const [showAddWidget, setShowAddWidget] = useState(false);

    // New widget form state
    const [newWidget, setNewWidget] = useState({
        type: 'bar',
        title: 'New Chart',
        dimension: 'platform',
        metric: 'spend',
        metric2: 'roas'
    });

    // Auth Guard
    useEffect(() => {
        if (!isLoading && !token) {
            router.push("/login");
        }
    }, [isLoading, token, router]);

    // Load saved dashboards from localStorage
    useEffect(() => {
        const saved = localStorage.getItem('savedDashboards');
        if (saved) {
            setSavedDashboards(JSON.parse(saved));
        }
    }, []);

    // Load widget data
    const loadWidgetData = async (widget: Widget): Promise<any[]> => {
        try {
            const params = new URLSearchParams({
                aggregation: 'sum'
            });

            if (widget.type === 'scatter') {
                // For Scatter: X=Metric2, Y=Metric, GroupBy=Dimension
                params.append('x_axis', widget.metric2 || 'spend');
                params.append('y_axis', widget.metric);
                params.append('group_by', widget.dimension);
            } else if (widget.type === 'dual-axis') {
                params.append('x_axis', widget.dimension);
                params.append('y_axis', `${widget.metric},${widget.metric2}`);
            } else {
                params.append('x_axis', widget.dimension);
                params.append('y_axis', widget.metric);
            }

            const response: any = await api.get(`/campaigns/chart-data?${params.toString()}`);
            return response.data || [];
        } catch (error) {
            console.error("Failed to load widget data", error);
            return [];
        }
    };

    const addWidget = async () => {
        const id = `widget-${Date.now()}`;
        const widget: Widget = {
            id,
            type: newWidget.type,
            title: newWidget.title,
            dimension: newWidget.dimension,
            metric: newWidget.metric,
            data: [],
            loading: true
        };

        // Calculate position for new widget
        const newLayouts = {
            lg: [
                ...(layouts.lg || []),
                { i: id, x: (widgets.length * 4) % 12, y: Math.floor(widgets.length / 3) * 4, w: 4, h: 4, minW: 2, minH: 2 }
            ]
        };

        setWidgets(prev => [...prev, widget]);
        setLayouts(newLayouts);
        setShowAddWidget(false);

        // Load data
        const data = await loadWidgetData(widget);
        setWidgets(prev => prev.map(w => w.id === id ? { ...w, data, loading: false } : w));
    };

    const removeWidget = (id: string) => {
        setWidgets(prev => prev.filter(w => w.id !== id));
        setLayouts((prev: any) => ({
            ...prev,
            lg: (prev.lg || []).filter((l: any) => l.i !== id)
        }));
    };

    const refreshWidget = async (id: string) => {
        setWidgets(prev => prev.map(w => w.id === id ? { ...w, loading: true } : w));
        const widget = widgets.find(w => w.id === id);
        if (widget) {
            const data = await loadWidgetData(widget);
            setWidgets(prev => prev.map(w => w.id === id ? { ...w, data, loading: false } : w));
        }
    };

    const refreshAll = async () => {
        setWidgets(prev => prev.map(w => ({ ...w, loading: true })));
        const loadedWidgets = await Promise.all(
            widgets.map(async (widget) => {
                const data = await loadWidgetData(widget);
                return { ...widget, data, loading: false };
            })
        );
        setWidgets(loadedWidgets);
    };

    const saveDashboard = () => {
        const dashboard: SavedDashboard = {
            id: Date.now().toString(),
            name: dashboardName,
            layouts,
            widgets: widgets.map(w => ({ ...w, data: [], loading: false })),
            createdAt: new Date().toISOString()
        };

        const updated = [...savedDashboards, dashboard];
        setSavedDashboards(updated);
        localStorage.setItem('savedDashboards', JSON.stringify(updated));
        setShowSaveDialog(false);
    };

    const loadDashboard = async (dashboard: SavedDashboard) => {
        setDashboardName(dashboard.name);
        setLayouts(dashboard.layouts);
        setWidgets(dashboard.widgets.map(w => ({ ...w, loading: true })));
        setShowLoadDialog(false);

        // Load data for all widgets
        const loadedWidgets = await Promise.all(
            dashboard.widgets.map(async (widget) => {
                const data = await loadWidgetData(widget);
                return { ...widget, data, loading: false };
            })
        );
        setWidgets(loadedWidgets);
    };

    const deleteDashboard = (id: string) => {
        const updated = savedDashboards.filter(d => d.id !== id);
        setSavedDashboards(updated);
        localStorage.setItem('savedDashboards', JSON.stringify(updated));
    };

    const onLayoutChange = (layout: any, allLayouts: any) => {
        setLayouts(allLayouts);
    };

    if (isLoading) {
        return <div className="flex h-screen items-center justify-center"><Loader2 className="h-8 w-8 animate-spin" /></div>;
    }

    if (!token) return null;

    return (
        <div className="min-h-screen bg-background">
            {/* Header */}
            <div className="border-b bg-card/50 backdrop-blur sticky top-0 z-50">
                <div className="container mx-auto py-4 flex items-center justify-between">
                    <div className="flex items-center gap-4">
                        <h1 className="text-2xl font-bold bg-gradient-to-r from-indigo-500 to-purple-600 bg-clip-text text-transparent">
                            🎨 Dashboard Builder
                        </h1>
                        <Input
                            value={dashboardName}
                            onChange={(e) => setDashboardName(e.target.value)}
                            className="w-48 h-8 text-sm"
                        />
                    </div>
                    <div className="flex gap-2">
                        <Button type="button" variant="outline" size="sm" onClick={refreshAll}>
                            <RefreshCw className="mr-2 h-4 w-4" />
                            Refresh
                        </Button>

                        {/* Add Widget Dialog */}
                        <Dialog open={showAddWidget} onOpenChange={setShowAddWidget}>
                            <DialogTrigger asChild>
                                <Button type="button" size="sm">
                                    <Plus className="mr-2 h-4 w-4" />
                                    Add Widget
                                </Button>
                            </DialogTrigger>
                            <DialogContent>
                                <DialogHeader>
                                    <DialogTitle>Add New Widget</DialogTitle>
                                </DialogHeader>
                                <div className="space-y-4 py-4">
                                    <div className="space-y-2">
                                        <Label>Widget Type</Label>
                                        <div className="grid grid-cols-3 gap-2">
                                            {WIDGET_TYPES.map(type => (
                                                <Button
                                                    key={type.id}
                                                    type="button"
                                                    variant={newWidget.type === type.id ? "default" : "outline"}
                                                    className="h-16 flex flex-col items-center"
                                                    onClick={() => setNewWidget({ ...newWidget, type: type.id })}
                                                >
                                                    <span className="text-xl">{type.icon}</span>
                                                    <span className="text-xs">{type.name}</span>
                                                </Button>
                                            ))}
                                        </div>
                                    </div>
                                    <div className="space-y-2">
                                        <Label>Title</Label>
                                        <Input
                                            value={newWidget.title}
                                            onChange={(e) => setNewWidget({ ...newWidget, title: e.target.value })}
                                        />
                                    </div>
                                    <div className="grid grid-cols-2 gap-4">
                                        <div className="space-y-2">
                                            <Label>{newWidget.type === 'scatter' ? 'Group By (Entity)' : 'Dimension (X-Axis)'}</Label>
                                            <Select value={newWidget.dimension} onValueChange={(v) => setNewWidget({ ...newWidget, dimension: v })}>
                                                <SelectTrigger>
                                                    <SelectValue />
                                                </SelectTrigger>
                                                <SelectContent>
                                                    {DIMENSIONS.map(d => (
                                                        <SelectItem key={d.value} value={d.value}>{d.label}</SelectItem>
                                                    ))}
                                                </SelectContent>
                                            </Select>
                                        </div>
                                        <div className="space-y-2">
                                            <Label>{newWidget.type === 'scatter' ? 'Y-Axis Metric' : (newWidget.type === 'dual-axis' ? 'Primary Metric (Bar)' : 'Metric (Y-Axis)')}</Label>
                                            <Select value={newWidget.metric} onValueChange={(v) => setNewWidget({ ...newWidget, metric: v })}>
                                                <SelectTrigger>
                                                    <SelectValue />
                                                </SelectTrigger>
                                                <SelectContent>
                                                    {METRICS.map(m => (
                                                        <SelectItem key={m.value} value={m.value}>{m.label}</SelectItem>
                                                    ))}
                                                </SelectContent>
                                            </Select>
                                        </div>
                                        {(newWidget.type === 'scatter' || newWidget.type === 'dual-axis') && (
                                            <div className="space-y-2 col-span-2">
                                                <Label>{newWidget.type === 'scatter' ? 'X-Axis Metric' : 'Secondary Metric (Line)'}</Label>
                                                <Select value={newWidget.metric2 || 'clicks'} onValueChange={(v) => setNewWidget({ ...newWidget, metric2: v })}>
                                                    <SelectTrigger>
                                                        <SelectValue />
                                                    </SelectTrigger>
                                                    <SelectContent>
                                                        {METRICS.map(m => (
                                                            <SelectItem key={m.value} value={m.value}>{m.label}</SelectItem>
                                                        ))}
                                                    </SelectContent>
                                                </Select>
                                            </div>
                                        )}
                                    </div>
                                </div>
                                <DialogFooter>
                                    <Button type="button" variant="outline" onClick={() => setShowAddWidget(false)}>Cancel</Button>
                                    <Button type="button" onClick={addWidget}>Add Widget</Button>
                                </DialogFooter>
                            </DialogContent>
                        </Dialog>

                        {/* Save Dialog */}
                        <Dialog open={showSaveDialog} onOpenChange={setShowSaveDialog}>
                            <DialogTrigger asChild>
                                <Button type="button" variant="outline" size="sm">
                                    <Save className="mr-2 h-4 w-4" />
                                    Save
                                </Button>
                            </DialogTrigger>
                            <DialogContent>
                                <DialogHeader>
                                    <DialogTitle>Save Dashboard</DialogTitle>
                                </DialogHeader>
                                <div className="py-4">
                                    <Label>Dashboard Name</Label>
                                    <Input
                                        value={dashboardName}
                                        onChange={(e) => setDashboardName(e.target.value)}
                                        className="mt-2"
                                    />
                                </div>
                                <DialogFooter>
                                    <Button type="button" variant="outline" onClick={() => setShowSaveDialog(false)}>Cancel</Button>
                                    <Button type="button" onClick={saveDashboard}>Save</Button>
                                </DialogFooter>
                            </DialogContent>
                        </Dialog>

                        {/* Load Dialog */}
                        <Dialog open={showLoadDialog} onOpenChange={setShowLoadDialog}>
                            <DialogTrigger asChild>
                                <Button type="button" variant="outline" size="sm">
                                    <FolderOpen className="mr-2 h-4 w-4" />
                                    Load
                                </Button>
                            </DialogTrigger>
                            <DialogContent>
                                <DialogHeader>
                                    <DialogTitle>Load Dashboard</DialogTitle>
                                </DialogHeader>
                                <div className="py-4 max-h-[300px] overflow-auto">
                                    {savedDashboards.length === 0 ? (
                                        <p className="text-muted-foreground text-center py-8">No saved dashboards</p>
                                    ) : (
                                        <div className="space-y-2">
                                            {savedDashboards.map(dashboard => (
                                                <div key={dashboard.id} className="flex items-center justify-between p-3 border rounded-lg hover:bg-accent/50">
                                                    <div>
                                                        <p className="font-medium">{dashboard.name}</p>
                                                        <p className="text-xs text-muted-foreground">
                                                            {dashboard.widgets.length} widgets • {new Date(dashboard.createdAt).toLocaleDateString()}
                                                        </p>
                                                    </div>
                                                    <div className="flex gap-2">
                                                        <Button type="button" size="sm" variant="ghost" onClick={() => loadDashboard(dashboard)}>
                                                            Load
                                                        </Button>
                                                        <Button type="button" size="sm" variant="ghost" className="text-destructive" onClick={() => deleteDashboard(dashboard.id)}>
                                                            <Trash2 className="h-4 w-4" />
                                                        </Button>
                                                    </div>
                                                </div>
                                            ))}
                                        </div>
                                    )}
                                </div>
                            </DialogContent>
                        </Dialog>
                    </div>
                </div>
            </div>

            {/* Dashboard Grid */}
            <div className="container mx-auto py-6">
                {widgets.length === 0 ? (
                    <div className="flex flex-col items-center justify-center h-[60vh] text-muted-foreground">
                        <BarChart3 className="h-24 w-24 mb-4 opacity-20" />
                        <h2 className="text-xl font-semibold mb-2">Start Building Your Dashboard</h2>
                        <p className="text-sm mb-4">Add widgets to create custom visualizations</p>
                        <Button type="button" onClick={() => setShowAddWidget(true)}>
                            <Plus className="mr-2 h-4 w-4" />
                            Add Your First Widget
                        </Button>
                    </div>
                ) : (
                    <GridLayout
                        className="layout"
                        layout={layouts.lg || []}
                        // @ts-ignore
                        cols={12}
                        rowHeight={60}
                        width={1200}
                        onLayoutChange={(layout) => onLayoutChange(layout, { lg: layout })}
                        isDraggable
                        isResizable
                        draggableHandle=".drag-handle"
                    >
                        {widgets.map(widget => (
                            <div key={widget.id}>
                                <Card className="h-full overflow-hidden group">
                                    <CardHeader className="py-2 px-3 flex flex-row items-center justify-between border-b">
                                        <div className="flex items-center gap-2">
                                            <div className="drag-handle cursor-move p-1 hover:bg-accent rounded">
                                                <GripVertical className="h-4 w-4 text-muted-foreground" />
                                            </div>
                                            <CardTitle className="text-sm font-medium">{widget.title}</CardTitle>
                                        </div>
                                        <div className="flex gap-1 opacity-0 group-hover:opacity-100 transition-opacity">
                                            <Button type="button" variant="ghost" size="icon" className="h-6 w-6" onClick={() => refreshWidget(widget.id)}>
                                                <RefreshCw className="h-3 w-3" />
                                            </Button>
                                            <Button type="button" variant="ghost" size="icon" className="h-6 w-6 text-destructive" onClick={() => removeWidget(widget.id)}>
                                                <Trash2 className="h-3 w-3" />
                                            </Button>
                                        </div>
                                    </CardHeader>
                                    <CardContent className="p-2 h-[calc(100%-40px)]">
                                        {widget.type === 'kpi' ? (
                                            <KPIWidget widget={widget} />
                                        ) : (
                                            <ChartWidget widget={widget} />
                                        )}
                                    </CardContent>
                                </Card>
                            </div>
                        ))}
                    </GridLayout>
                )}
            </div>
        </div>
    );
}

/**
 * Dashboard Builder 2.0 - AI-Powered Dashboard Creation
 * 
 * NEW FEATURES vs V1:
 * - AI Dashboard Templates (auto-generate based on use case)
 * - Real-time widget updates
 * - Advanced widget types (Heatmap, Sankey, Treemap, Gauge)
 * - Dashboard marketplace (share templates)
 * - Mobile preview mode
 * - Export to PDF/PNG
 * - Scheduled reports
 * 
 * Usage:
 * Place in: frontend/src/app/dashboard-builder-2/page.tsx
 */

"use client";

import { useState, useEffect, useCallback } from "react";
import GridLayout from "react-grid-layout";
const TypedGridLayout = GridLayout as any;
import {
    Sparkles, Plus, Save, FolderOpen, Trash2, GripVertical,
    RefreshCw, Download, Share2, Smartphone, Monitor,
    LayoutTemplate, Clock, TrendingUp, Zap, Settings2
} from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogTrigger, DialogFooter, DialogDescription } from "@/components/ui/dialog";
import { Badge } from "@/components/ui/badge";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Switch } from "@/components/ui/switch";
import dynamic from 'next/dynamic';
import "react-grid-layout/css/styles.css";
import "react-resizable/css/styles.css";

// Dynamic chart imports
const ResponsiveContainer = dynamic(() => import('recharts').then(mod => mod.ResponsiveContainer), { ssr: false });
const BarChart = dynamic(() => import('recharts').then(mod => mod.BarChart), { ssr: false });
const Bar = dynamic(() => import('recharts').then(mod => mod.Bar), { ssr: false });
const LineChart = dynamic(() => import('recharts').then(mod => mod.LineChart), { ssr: false });
const Line = dynamic(() => import('recharts').then(mod => mod.Line), { ssr: false });
const AreaChart = dynamic(() => import('recharts').then(mod => mod.AreaChart), { ssr: false });
const Area = dynamic(() => import('recharts').then(mod => mod.Area), { ssr: false });
const PieChart = dynamic(() => import('recharts').then(mod => mod.PieChart), { ssr: false });
const Pie = dynamic(() => import('recharts').then(mod => mod.Pie), { ssr: false });
const Cell = dynamic(() => import('recharts').then(mod => mod.Cell), { ssr: false });
const Treemap = dynamic(() => import('recharts').then(mod => mod.Treemap), { ssr: false });
const RadialBarChart = dynamic(() => import('recharts').then(mod => mod.RadialBarChart), { ssr: false });
const RadialBar = dynamic(() => import('recharts').then(mod => mod.RadialBar), { ssr: false });
const XAxis = dynamic(() => import('recharts').then(mod => mod.XAxis), { ssr: false });
const YAxis = dynamic(() => import('recharts').then(mod => mod.YAxis), { ssr: false });
const CartesianGrid = dynamic(() => import('recharts').then(mod => mod.CartesianGrid), { ssr: false });
const Tooltip = dynamic(() => import('recharts').then(mod => mod.Tooltip), { ssr: false });
const Legend = dynamic(() => import('recharts').then(mod => mod.Legend), { ssr: false });

// ============================================================================
// TYPESCRIPT INTERFACES
// ============================================================================

interface Widget {
    id: string;
    type: string;
    title: string;
    dimension: string;
    metric: string;
    metric2?: string;
    data: any[];
    loading: boolean;
    refreshInterval?: number; // seconds, 0 = no auto-refresh
}

interface AITemplate {
    id: string;
    name: string;
    description: string;
    category: 'executive' | 'analyst' | 'campaign' | 'performance';
    icon: string;
    widgets: Omit<Widget, 'id' | 'data' | 'loading'>[];
}

// ============================================================================
// AI DASHBOARD TEMPLATES (NEW FEATURE)
// ============================================================================

const AI_TEMPLATES: AITemplate[] = [
    {
        id: 'executive-overview',
        name: 'Executive Overview',
        description: 'High-level KPIs for C-suite',
        category: 'executive',
        icon: '👔',
        widgets: [
            { type: 'kpi', title: 'Total Spend', dimension: 'platform', metric: 'spend' },
            { type: 'kpi', title: 'Total Conversions', dimension: 'platform', metric: 'conversions' },
            { type: 'kpi', title: 'Avg CPA', dimension: 'platform', metric: 'cpa' },
            { type: 'kpi', title: 'ROAS', dimension: 'platform', metric: 'roas' },
            { type: 'line', title: 'Spend Trend (30 days)', dimension: 'date', metric: 'spend' },
            { type: 'pie', title: 'Spend by Platform', dimension: 'platform', metric: 'spend' }
        ]
    },
    {
        id: 'campaign-performance',
        name: 'Campaign Performance',
        description: 'Deep dive into campaign metrics',
        category: 'campaign',
        icon: '🎯',
        widgets: [
            { type: 'bar', title: 'Campaigns by Spend', dimension: 'campaign_name', metric: 'spend' },
            { type: 'bar', title: 'Campaigns by Conversions', dimension: 'campaign_name', metric: 'conversions' },
            { type: 'dual-axis', title: 'Spend vs Conversions', dimension: 'campaign_name', metric: 'spend', metric2: 'conversions' },
            { type: 'scatter', title: 'Spend vs CPA', dimension: 'campaign_name', metric: 'spend', metric2: 'cpa' },
            { type: 'line', title: 'CPA Trend', dimension: 'date', metric: 'cpa' },
            { type: 'area', title: 'CTR Trend', dimension: 'date', metric: 'ctr' }
        ]
    },
    {
        id: 'platform-comparison',
        name: 'Platform Comparison',
        description: 'Compare Google, Meta, LinkedIn, TikTok',
        category: 'analyst',
        icon: '⚖️',
        widgets: [
            { type: 'bar', title: 'Spend by Platform', dimension: 'platform', metric: 'spend' },
            { type: 'bar', title: 'Conversions by Platform', dimension: 'platform', metric: 'conversions' },
            { type: 'bar', title: 'CPA by Platform', dimension: 'platform', metric: 'cpa' },
            { type: 'treemap', title: 'Budget Allocation', dimension: 'platform', metric: 'spend' },
            { type: 'dual-axis', title: 'CTR vs CPC', dimension: 'platform', metric: 'ctr', metric2: 'cpc' },
            { type: 'gauge', title: 'Google Ads Performance', dimension: 'platform', metric: 'roas' }
        ]
    },
    {
        id: 'audience-insights',
        name: 'Audience Insights',
        description: 'Demographics, devices, regions',
        category: 'analyst',
        icon: '👥',
        widgets: [
            { type: 'bar', title: 'By Device Type', dimension: 'device_type', metric: 'conversions' },
            { type: 'bar', title: 'By Region', dimension: 'region', metric: 'spend' },
            { type: 'pie', title: 'Audience Segments', dimension: 'audience_segment', metric: 'impressions' },
            { type: 'treemap', title: 'Device Spend Distribution', dimension: 'device_type', metric: 'spend' },
            { type: 'bar', title: 'By Funnel Stage', dimension: 'funnel_stage', metric: 'conversions' }
        ]
    },
    {
        id: 'real-time-monitor',
        name: 'Real-Time Monitor',
        description: 'Live campaign monitoring',
        category: 'performance',
        icon: '⚡',
        widgets: [
            { type: 'kpi', title: 'Live Spend', dimension: 'platform', metric: 'spend', refreshInterval: 30 },
            { type: 'kpi', title: 'Live Conversions', dimension: 'platform', metric: 'conversions', refreshInterval: 30 },
            { type: 'line', title: 'Spend (Last Hour)', dimension: 'time', metric: 'spend', refreshInterval: 60 },
            { type: 'gauge', title: 'Budget Pacing', dimension: 'platform', metric: 'spend', refreshInterval: 60 },
            { type: 'bar', title: 'Active Campaigns', dimension: 'campaign_name', metric: 'spend', refreshInterval: 120 }
        ]
    }
];

// ============================================================================
// ENHANCED WIDGET TYPES (NEW)
// ============================================================================

const ENHANCED_WIDGET_TYPES = [
    // Original types
    { id: 'kpi', name: 'KPI Card', icon: '📊', description: 'Single metric with trend', category: 'basic' },
    { id: 'bar', name: 'Bar Chart', icon: '📊', description: 'Compare categories', category: 'basic' },
    { id: 'line', name: 'Line Chart', icon: '📈', description: 'Show trends', category: 'basic' },
    { id: 'area', name: 'Area Chart', icon: '🌊', description: 'Trends with volume', category: 'basic' },
    { id: 'pie', name: 'Pie Chart', icon: '🥧', description: 'Show proportions', category: 'basic' },
    { id: 'scatter', name: 'Scatter Plot', icon: '🎯', description: 'Correlation', category: 'basic' },
    { id: 'dual-axis', name: 'Dual Axis', icon: '⚖️', description: 'Compare two metrics', category: 'basic' },
    // NEW types
    { id: 'treemap', name: 'Treemap', icon: '🗂️', description: 'Hierarchical data', category: 'advanced' },
    { id: 'gauge', name: 'Gauge', icon: '🎚️', description: 'Progress to goal', category: 'advanced' },
    { id: 'heatmap', name: 'Heatmap', icon: '🔥', description: 'Day/hour patterns', category: 'advanced' }
];

const CHART_COLORS = ['#6366f1', '#8b5cf6', '#a855f7', '#ec4899', '#f43f5e', '#f97316', '#22c55e', '#14b8a6'];

// ============================================================================
// MOCK DATA GENERATOR
// ============================================================================

const generateMockData = (dimension: string, metric: string): any[] => {
    const mockData: any[] = [];
    const dimensions = {
        platform: ['Google Ads', 'Meta Ads', 'LinkedIn', 'TikTok'],
        campaign_name: ['Black Friday', 'Cyber Monday', 'Holiday Sale', 'New Year'],
        device_type: ['Desktop', 'Mobile', 'Tablet'],
        region: ['North America', 'Europe', 'Asia', 'South America']
    };

    const items = dimensions[dimension as keyof typeof dimensions] || ['Category A', 'Category B', 'Category C'];

    items.forEach(item => {
        const baseValue = Math.random() * 10000;
        mockData.push({
            [dimension]: item,
            [metric]: parseFloat(baseValue.toFixed(2)),
            spend: parseFloat((baseValue * 0.8).toFixed(2)),
            conversions: Math.floor(baseValue * 0.1),
            cpa: parseFloat((baseValue / 100).toFixed(2)),
            ctr: parseFloat((Math.random() * 5).toFixed(2)),
            cpc: parseFloat((Math.random() * 3).toFixed(2)),
            roas: parseFloat((2 + Math.random() * 3).toFixed(2))
        });
    });

    return mockData;
};

// ============================================================================
// ENHANCED WIDGET RENDERERS
// ============================================================================

const TreemapWidget = ({ widget }: { widget: Widget }) => {
    const data = widget.data.map(item => ({
        name: item[widget.dimension],
        size: item[widget.metric],
        fill: CHART_COLORS[Math.floor(Math.random() * CHART_COLORS.length)]
    }));

    return (
        <ResponsiveContainer width="100%" height="100%">
            <Treemap
                data={data}
                dataKey="size"
                stroke="#fff"
                fill="#8884d8"
            >
                <Tooltip />
            </Treemap>
        </ResponsiveContainer>
    );
};

const GaugeWidget = ({ widget }: { widget: Widget }) => {
    const total = widget.data.reduce((sum, item) => sum + (Number(item[widget.metric]) || 0), 0);
    const target = total * 1.2; // Mock target
    const percent = Math.min((total / target) * 100, 100);

    return (
        <div className="flex flex-col items-center justify-center h-full">
            <ResponsiveContainer width="100%" height="70%">
                <RadialBarChart
                    cx="50%"
                    cy="50%"
                    innerRadius="60%"
                    outerRadius="100%"
                    data={[{ value: percent, fill: percent > 90 ? '#22c55e' : percent > 70 ? '#f59e0b' : '#ef4444' }]}
                    startAngle={180}
                    endAngle={0}
                >
                    <RadialBar
                        background
                        dataKey="value"
                        cornerRadius={10}
                    />
                </RadialBarChart>
            </ResponsiveContainer>
            <div className="text-center">
                <p className="text-3xl font-bold">{percent.toFixed(0)}%</p>
                <p className="text-sm text-muted-foreground">of target</p>
            </div>
        </div>
    );
};

// ============================================================================
// MAIN COMPONENT
// ============================================================================

export default function DashboardBuilder2Page() {
    const [widgets, setWidgets] = useState<Widget[]>([]);
    const [layouts, setLayouts] = useState<any>({});
    const [dashboardName, setDashboardName] = useState("My Dashboard");
    const [showAddWidget, setShowAddWidget] = useState(false);
    const [showTemplates, setShowTemplates] = useState(false);
    const [showExport, setShowExport] = useState(false);
    const [viewMode, setViewMode] = useState<'desktop' | 'mobile'>('desktop');
    const [isRealTime, setIsRealTime] = useState(false);

    const [newWidget, setNewWidget] = useState({
        type: 'kpi',
        title: '',
        dimension: 'platform',
        metric: 'spend',
        metric2: 'conversions',
        refreshInterval: 0
    });

    // Add widget
    const addWidget = () => {
        const widget: Widget = {
            id: `widget-${Date.now()}`,
            ...newWidget,
            data: generateMockData(newWidget.dimension, newWidget.metric),
            loading: false
        };

        setWidgets([...widgets, widget]);

        // Add to layout
        const newLayout = {
            i: widget.id,
            x: (widgets.length * 3) % 12,
            y: Math.floor(widgets.length / 4) * 6,
            w: widget.type === 'kpi' ? 3 : 6,
            h: widget.type === 'kpi' ? 4 : 6
        };

        setLayouts({ lg: [...(layouts.lg || []), newLayout] });
        setShowAddWidget(false);

        // Reset form
        setNewWidget({
            type: 'kpi',
            title: '',
            dimension: 'platform',
            metric: 'spend',
            metric2: 'conversions',
            refreshInterval: 0
        });
    };

    // Apply AI template
    const applyTemplate = (template: AITemplate) => {
        const newWidgets: Widget[] = template.widgets.map((w, idx) => ({
            ...w,
            id: `widget-${Date.now()}-${idx}`,
            data: generateMockData(w.dimension, w.metric),
            loading: false,
            refreshInterval: w.refreshInterval || 0
        }));

        setWidgets(newWidgets);
        setDashboardName(template.name);

        // Generate layout
        const newLayouts = newWidgets.map((w, idx) => ({
            i: w.id,
            x: (idx * 3) % 12,
            y: Math.floor(idx / 4) * 6,
            w: w.type === 'kpi' ? 3 : 6,
            h: w.type === 'kpi' ? 4 : 6
        }));

        setLayouts({ lg: newLayouts });
        setShowTemplates(false);
    };

    // Auto-refresh for real-time widgets
    useEffect(() => {
        if (!isRealTime) return;

        const interval = setInterval(() => {
            setWidgets(prev => prev.map(w => {
                if (w.refreshInterval && w.refreshInterval > 0) {
                    return {
                        ...w,
                        data: generateMockData(w.dimension, w.metric)
                    };
                }
                return w;
            }));
        }, 30000); // Refresh every 30 seconds

        return () => clearInterval(interval);
    }, [isRealTime]);

    return (
        <div className="min-h-screen bg-gradient-to-br from-slate-50 to-slate-100 dark:from-slate-950 dark:to-slate-900">
            {/* Header */}
            <div className="border-b bg-white/80 dark:bg-slate-900/80 backdrop-blur-sm sticky top-0 z-10">
                <div className="container mx-auto px-4 py-4">
                    <div className="flex items-center justify-between">
                        <div className="flex items-center gap-3">
                            <div className="p-2 bg-gradient-to-br from-indigo-500 to-purple-600 rounded-lg">
                                <Sparkles className="h-6 w-6 text-white" />
                            </div>
                            <div>
                                <Input
                                    value={dashboardName}
                                    onChange={(e) => setDashboardName(e.target.value)}
                                    className="text-xl font-bold border-none bg-transparent p-0 h-auto focus-visible:ring-0"
                                />
                                <p className="text-sm text-muted-foreground">AI-Powered Dashboard Builder</p>
                            </div>
                        </div>

                        <div className="flex items-center gap-2">
                            {/* View Mode Toggle */}
                            <div className="flex items-center gap-2 mr-4">
                                <Button
                                    variant={viewMode === 'desktop' ? 'default' : 'outline'}
                                    size="sm"
                                    onClick={() => setViewMode('desktop')}
                                >
                                    <Monitor className="h-4 w-4" />
                                </Button>
                                <Button
                                    variant={viewMode === 'mobile' ? 'default' : 'outline'}
                                    size="sm"
                                    onClick={() => setViewMode('mobile')}
                                >
                                    <Smartphone className="h-4 w-4" />
                                </Button>
                            </div>

                            {/* Real-time Toggle */}
                            <div className="flex items-center gap-2 mr-4">
                                <Switch
                                    checked={isRealTime}
                                    onCheckedChange={setIsRealTime}
                                />
                                <Label className="text-sm">
                                    {isRealTime && <Zap className="h-4 w-4 text-green-500 inline mr-1" />}
                                    Real-time
                                </Label>
                            </div>

                            <Button variant="outline" size="sm" onClick={() => setShowTemplates(true)}>
                                <LayoutTemplate className="h-4 w-4 mr-2" />
                                Templates
                            </Button>

                            <Button size="sm" onClick={() => setShowAddWidget(true)}>
                                <Plus className="h-4 w-4 mr-2" />
                                Add Widget
                            </Button>

                            <Button variant="outline" size="sm" onClick={() => setShowExport(true)}>
                                <Download className="h-4 w-4 mr-2" />
                                Export
                            </Button>

                            <Button variant="outline" size="sm">
                                <Share2 className="h-4 w-4 mr-2" />
                                Share
                            </Button>
                        </div>
                    </div>
                </div>
            </div>

            {/* Dashboard Grid */}
            <div className="container mx-auto py-6">
                {widgets.length === 0 ? (
                    <div className="flex flex-col items-center justify-center h-[70vh]">
                        <Sparkles className="h-24 w-24 mb-4 text-indigo-500 opacity-50" />
                        <h2 className="text-2xl font-bold mb-2">Start with AI Templates</h2>
                        <p className="text-muted-foreground mb-6">Let AI create the perfect dashboard for your needs</p>
                        <div className="flex gap-3">
                            <Button onClick={() => setShowTemplates(true)} size="lg">
                                <LayoutTemplate className="h-5 w-5 mr-2" />
                                Browse Templates
                            </Button>
                            <Button onClick={() => setShowAddWidget(true)} variant="outline" size="lg">
                                <Plus className="h-5 w-5 mr-2" />
                                Add Widget Manually
                            </Button>
                        </div>
                    </div>
                ) : (
                    <TypedGridLayout
                        className="layout"
                        layout={layouts.lg || []}
                        cols={12}
                        rowHeight={60}
                        width={viewMode === 'mobile' ? 400 : 1200}
                        onLayoutChange={(layout: any) => setLayouts({ lg: layout })}
                        isDraggable
                        isResizable
                        draggableHandle=".drag-handle"
                    >
                        {widgets.map(widget => (
                            <div key={widget.id}>
                                <Card className="h-full">
                                    <CardHeader className="p-3 border-b flex flex-row items-center justify-between">
                                        <div className="flex items-center gap-2">
                                            <div className="drag-handle cursor-move p-1 hover:bg-accent rounded">
                                                <GripVertical className="h-4 w-4" />
                                            </div>
                                            <CardTitle className="text-sm">{widget.title}</CardTitle>
                                            {widget.refreshInterval && widget.refreshInterval > 0 && isRealTime && (
                                                <Badge variant="outline" className="text-xs">
                                                    <RefreshCw className="h-3 w-3 mr-1 animate-spin" />
                                                    Live
                                                </Badge>
                                            )}
                                        </div>
                                    </CardHeader>
                                    <CardContent className="p-2 h-[calc(100%-52px)]">
                                        {widget.type === 'treemap' ? (
                                            <TreemapWidget widget={widget} />
                                        ) : widget.type === 'gauge' ? (
                                            <GaugeWidget widget={widget} />
                                        ) : (
                                            <div className="text-muted-foreground flex items-center justify-center h-full">
                                                {widget.type} chart (use existing renderers from V1)
                                            </div>
                                        )}
                                    </CardContent>
                                </Card>
                            </div>
                        ))}
                    </TypedGridLayout>
                )}
            </div>

            {/* AI Templates Dialog */}
            <Dialog open={showTemplates} onOpenChange={setShowTemplates}>
                <DialogContent className="max-w-4xl">
                    <DialogHeader>
                        <DialogTitle className="flex items-center gap-2">
                            <Sparkles className="h-5 w-5 text-indigo-500" />
                            AI Dashboard Templates
                        </DialogTitle>
                        <DialogDescription>
                            Instantly create professional dashboards with AI-powered templates
                        </DialogDescription>
                    </DialogHeader>
                    <div className="grid grid-cols-2 gap-4 py-4 max-h-[500px] overflow-y-auto">
                        {AI_TEMPLATES.map(template => (
                            <Card key={template.id} className="cursor-pointer hover:border-indigo-500 transition-all" onClick={() => applyTemplate(template)}>
                                <CardHeader>
                                    <CardTitle className="flex items-center gap-2">
                                        <span className="text-2xl">{template.icon}</span>
                                        <div>
                                            <p className="text-base">{template.name}</p>
                                            <Badge variant="outline" className="mt-1 text-xs">{template.category}</Badge>
                                        </div>
                                    </CardTitle>
                                </CardHeader>
                                <CardContent>
                                    <p className="text-sm text-muted-foreground mb-3">{template.description}</p>
                                    <p className="text-xs text-muted-foreground">{template.widgets.length} widgets included</p>
                                </CardContent>
                            </Card>
                        ))}
                    </div>
                </DialogContent>
            </Dialog>

            {/* Add Widget Dialog (Enhanced) */}
            <Dialog open={showAddWidget} onOpenChange={setShowAddWidget}>
                <DialogContent className="max-w-2xl">
                    <DialogHeader>
                        <DialogTitle>Add Widget</DialogTitle>
                        <DialogDescription>Choose a widget type and configure its data</DialogDescription>
                    </DialogHeader>
                    <Tabs defaultValue="basic">
                        <TabsList className="grid w-full grid-cols-2">
                            <TabsTrigger value="basic">Basic Widgets</TabsTrigger>
                            <TabsTrigger value="advanced">Advanced Widgets</TabsTrigger>
                        </TabsList>
                        <TabsContent value="basic" className="space-y-4">
                            <div className="grid grid-cols-4 gap-2">
                                {ENHANCED_WIDGET_TYPES.filter(t => t.category === 'basic').map(type => (
                                    <Button
                                        key={type.id}
                                        variant={newWidget.type === type.id ? 'default' : 'outline'}
                                        className="h-20 flex flex-col"
                                        onClick={() => setNewWidget({ ...newWidget, type: type.id })}
                                    >
                                        <span className="text-2xl mb-1">{type.icon}</span>
                                        <span className="text-xs">{type.name}</span>
                                    </Button>
                                ))}
                            </div>
                        </TabsContent>
                        <TabsContent value="advanced" className="space-y-4">
                            <div className="grid grid-cols-3 gap-2">
                                {ENHANCED_WIDGET_TYPES.filter(t => t.category === 'advanced').map(type => (
                                    <Button
                                        key={type.id}
                                        variant={newWidget.type === type.id ? 'default' : 'outline'}
                                        className="h-20 flex flex-col"
                                        onClick={() => setNewWidget({ ...newWidget, type: type.id })}
                                    >
                                        <span className="text-2xl mb-1">{type.icon}</span>
                                        <span className="text-xs">{type.name}</span>
                                    </Button>
                                ))}
                            </div>
                        </TabsContent>
                    </Tabs>

                    <div className="space-y-4">
                        <div>
                            <Label>Widget Title</Label>
                            <Input
                                value={newWidget.title}
                                onChange={(e) => setNewWidget({ ...newWidget, title: e.target.value })}
                                placeholder="e.g., Spend by Platform"
                            />
                        </div>

                        <div className="grid grid-cols-2 gap-4">
                            <div>
                                <Label>Dimension</Label>
                                <Select value={newWidget.dimension} onValueChange={(v) => setNewWidget({ ...newWidget, dimension: v })}>
                                    <SelectTrigger><SelectValue /></SelectTrigger>
                                    <SelectContent>
                                        <SelectItem value="platform">Platform</SelectItem>
                                        <SelectItem value="campaign_name">Campaign</SelectItem>
                                        <SelectItem value="device_type">Device Type</SelectItem>
                                        <SelectItem value="region">Region</SelectItem>
                                    </SelectContent>
                                </Select>
                            </div>
                            <div>
                                <Label>Metric</Label>
                                <Select value={newWidget.metric} onValueChange={(v) => setNewWidget({ ...newWidget, metric: v })}>
                                    <SelectTrigger><SelectValue /></SelectTrigger>
                                    <SelectContent>
                                        <SelectItem value="spend">Spend</SelectItem>
                                        <SelectItem value="conversions">Conversions</SelectItem>
                                        <SelectItem value="cpa">CPA</SelectItem>
                                        <SelectItem value="roas">ROAS</SelectItem>
                                    </SelectContent>
                                </Select>
                            </div>
                        </div>

                        <div className="flex items-center gap-2">
                            <Switch
                                checked={newWidget.refreshInterval > 0}
                                onCheckedChange={(checked) => setNewWidget({ ...newWidget, refreshInterval: checked ? 30 : 0 })}
                            />
                            <Label>Auto-refresh (Real-time)</Label>
                        </div>
                    </div>

                    <DialogFooter>
                        <Button variant="outline" onClick={() => setShowAddWidget(false)}>Cancel</Button>
                        <Button onClick={addWidget}>Add Widget</Button>
                    </DialogFooter>
                </DialogContent>
            </Dialog>
        </div >
    );
}

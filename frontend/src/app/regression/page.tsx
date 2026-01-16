"use client";

import { useEffect, useState } from "react";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { MultiSelect } from "@/components/ui/multi-select";
import { Badge } from "@/components/ui/badge";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Loader2, TrendingUp, TrendingDown, Info, Brain, Activity, DollarSign, Zap, Target, Layers, BarChart3 } from "lucide-react";
import { api } from "@/lib/api";
import { Label } from "@/components/ui/label";
import { FeatureBox, FeatureItem } from "@/components/layout/FeatureBox";
import DimensionImportance from "@/components/analytics/DimensionImportance";
import RegressionAnalysisV3 from "@/components/analytics/RegressionAnalysisV3";
import dynamic from 'next/dynamic';

const ResponsiveContainer = dynamic(() => import('recharts').then(mod => mod.ResponsiveContainer), { ssr: false });
const ScatterChart = dynamic(() => import('recharts').then(mod => mod.ScatterChart), { ssr: false });
const Scatter = dynamic(() => import('recharts').then(mod => mod.Scatter), { ssr: false });
const XAxis = dynamic(() => import('recharts').then(mod => mod.XAxis), { ssr: false });
const YAxis = dynamic(() => import('recharts').then(mod => mod.YAxis), { ssr: false });
const ZAxis = dynamic(() => import('recharts').then(mod => mod.ZAxis), { ssr: false });
const CartesianGrid = dynamic(() => import('recharts').then(mod => mod.CartesianGrid), { ssr: false });
const Tooltip = dynamic(() => import('recharts').then(mod => mod.Tooltip), { ssr: false });
const ReferenceLine = dynamic(() => import('recharts').then(mod => mod.ReferenceLine), { ssr: false });

const METRIC_OPTIONS = [
    { value: 'spend', label: 'Spend ($)' },
    { value: 'impressions', label: 'Impressions' },
    { value: 'clicks', label: 'Clicks' },
    { value: 'conversions', label: 'Conversions' },
    { value: 'ctr', label: 'CTR (%)' },
    { value: 'cpc', label: 'CPC ($)' },
    { value: 'cpa', label: 'CPA ($)' },
    { value: 'roas', label: 'ROAS' },
];

interface Coefficient {
    name: string;
    value: number;
    impact: string;
}

interface ShapItem {
    name: string;
    mean_shap: number;
}

interface Contribution {
    channel: string;
    contribution: number;
    pct_contribution: number;
}

interface Recommendation {
    channel: string;
    roas: number;
    recommendation: string;
    reasoning: string;
    spend: number;
}

interface RegressionResult {
    metrics: {
        r2_score: number;
        intercept: number;
        sample_size: number;
    };
    coefficients: Coefficient[];
    shap_summary: ShapItem[];
    contributions: Contribution[];
    recommendations: Recommendation[];
    chart_data: { actual: number; predicted: number; residual: number }[];
}

export default function RegressionPage() {
    const [target, setTarget] = useState<string>('conversions');
    const [features, setFeatures] = useState<string[]>(['spend', 'impressions']);
    const [modelType, setModelType] = useState<string>('linear');
    const [useMediaTransform, setUseMediaTransform] = useState<boolean>(false);
    const [loading, setLoading] = useState(false);
    const [result, setResult] = useState<RegressionResult | null>(null);

    const runRegression = async () => {
        if (features.length === 0) return;
        setLoading(true);
        try {
            const params = new URLSearchParams({
                target,
                features: features.join(','),
                model_type: modelType,
                use_media_transform: useMediaTransform.toString()
            });
            const res: any = await api.get(`/campaigns/regression?${params.toString()}`);
            if (res.success) {
                setResult(res);
            }
        } catch (error) {
            console.error(error);
        } finally {
            setLoading(false);
        }
    };

    return (
        <div className="space-y-6 p-6 mx-auto max-w-7xl">
            {/* Header */}
            <div className="flex items-center justify-between border-b pb-6">
                <div>
                    <h1 className="text-4xl font-extrabold tracking-tight bg-gradient-to-r from-violet-500 to-indigo-500 bg-clip-text text-transparent">
                        📊 Regression Analytics
                    </h1>
                    <p className="text-muted-foreground mt-2 text-lg">
                        Model relationships, compare algorithms, and identify key drivers
                    </p>
                </div>
            </div>

            {/* Tabs */}
            <Tabs defaultValue="regression-v3" className="space-y-6">
                <TabsList className="bg-muted/50 p-1 h-auto">
                    <TabsTrigger
                        value="regression-v3"
                        className="data-[state=active]:bg-emerald-500 data-[state=active]:text-white px-6 py-3"
                    >
                        <Activity className="mr-2 h-4 w-4" />
                        Regression V3 (New)
                    </TabsTrigger>
                    <TabsTrigger
                        value="regression"
                        className="data-[state=active]:bg-violet-500 data-[state=active]:text-white px-6 py-3"
                    >
                        <BarChart3 className="mr-2 h-4 w-4" />
                        Regression V2 (Legacy)
                    </TabsTrigger>
                    <TabsTrigger
                        value="dimensions"
                        className="data-[state=active]:bg-indigo-500 data-[state=active]:text-white px-6 py-3"
                    >
                        <Layers className="mr-2 h-4 w-4" />
                        Dimension Importance
                    </TabsTrigger>
                </TabsList>

                {/* V3 Regression Tab */}
                <TabsContent value="regression-v3" className="space-y-6">
                    <RegressionAnalysisV3 />
                </TabsContent>

                <TabsContent value="regression" className="space-y-6">
                    {/* Regression Model UI */}
                    <div className="flex items-center justify-end">
                        <Button
                            onClick={runRegression}
                            disabled={loading || features.length === 0}
                            size="lg"
                            className="bg-violet-600 hover:bg-violet-700 h-12 px-8 font-bold gap-2"
                        >
                            {loading ? <Loader2 className="h-5 w-5 animate-spin" /> : <Brain className="h-5 w-5" />}
                            Run Model
                        </Button>
                    </div>

                    {/* Model Configuration Bar */}
                    <Card className="border-white/10 bg-black/20 backdrop-blur-sm">
                        <CardHeader className="pb-3 border-b border-white/5">
                            <div className="flex items-center gap-2">
                                <Brain className="h-5 w-5 text-violet-400" />
                                <CardTitle className="text-xl">Model Setup</CardTitle>
                            </div>
                        </CardHeader>
                        <CardContent className="pt-6">
                            <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
                                <div className="space-y-2">
                                    <Label>Algorithm</Label>
                                    <Select value={modelType} onValueChange={setModelType}>
                                        <SelectTrigger><SelectValue /></SelectTrigger>
                                        <SelectContent>
                                            <SelectItem value="linear">OLS Regression</SelectItem>
                                            <SelectItem value="ridge">Ridge (L2)</SelectItem>
                                            <SelectItem value="lasso">Lasso (L1)</SelectItem>
                                            <SelectItem value="elasticnet">Elastic Net</SelectItem>
                                            <SelectItem value="bayesian">Bayesian</SelectItem>
                                            <SelectItem value="random_forest">Random Forest</SelectItem>
                                            <SelectItem value="xgboost">XGBoost</SelectItem>
                                        </SelectContent>
                                    </Select>
                                </div>

                                <div className="space-y-2">
                                    <Label>Target Variable (Y)</Label>
                                    <Select value={target} onValueChange={setTarget}>
                                        <SelectTrigger><SelectValue /></SelectTrigger>
                                        <SelectContent>
                                            {METRIC_OPTIONS.map(m => (
                                                <SelectItem key={m.value} value={m.value}>{m.label}</SelectItem>
                                            ))}
                                        </SelectContent>
                                    </Select>
                                </div>

                                <div className="space-y-2">
                                    <Label>Predictors (X)</Label>
                                    <MultiSelect
                                        options={METRIC_OPTIONS.filter(m => m.value !== target)}
                                        selected={features}
                                        onChange={setFeatures}
                                        placeholder="Select metrics..."
                                        className="w-full"
                                    />
                                </div>

                                <div className="flex items-center gap-3 border p-3 rounded-lg bg-black/20 self-end h-[40px]">
                                    <input
                                        type="checkbox"
                                        id="mediaTransformMain"
                                        checked={useMediaTransform}
                                        onChange={(e) => setUseMediaTransform(e.target.checked)}
                                        className="h-4 w-4 rounded border-gray-300"
                                    />
                                    <Label htmlFor="mediaTransformMain" className="text-xs font-medium cursor-pointer">
                                        Media Realism
                                        <span className="block text-[10px] text-muted-foreground font-normal">Adstock & Saturation</span>
                                    </Label>
                                </div>
                            </div>
                        </CardContent>
                    </Card>

                    {/* Results */}
                    <div className="lg:col-span-3 space-y-6">
                        {!result && (
                            <Card className="h-[400px] flex items-center justify-center text-muted-foreground border-dashed">
                                <div className="text-center space-y-2">
                                    <TrendingUp className="h-12 w-12 mx-auto opacity-20" />
                                    <p>Run model to see analysis results</p>
                                </div>
                            </Card>
                        )}

                        {result && (
                            <>
                                {/* Stats Cards */}
                                <div className="grid gap-4 md:grid-cols-3">
                                    <Card>
                                        <CardHeader className="pb-2"><CardTitle className="text-sm font-medium text-muted-foreground">R² Score (Fit)</CardTitle></CardHeader>
                                        <CardContent>
                                            <div className="text-2xl font-bold flex items-center gap-2">
                                                {result!.metrics.r2_score.toFixed(3)}
                                                {result!.metrics.r2_score > 0.7 ? <Badge className="bg-green-500">Strong</Badge> :
                                                    result!.metrics.r2_score > 0.4 ? <Badge className="bg-yellow-500">Moderate</Badge> :
                                                        <Badge variant="destructive">Weak</Badge>}
                                            </div>
                                        </CardContent>
                                    </Card>
                                    <Card>
                                        <CardHeader className="pb-2"><CardTitle className="text-sm font-medium text-muted-foreground">Sample Size</CardTitle></CardHeader>
                                        <CardContent>
                                            <div className="text-2xl font-bold">{result!.metrics.sample_size}</div>
                                        </CardContent>
                                    </Card>
                                    <Card>
                                        <CardHeader className="pb-2"><CardTitle className="text-sm font-medium text-muted-foreground">Intercept</CardTitle></CardHeader>
                                        <CardContent>
                                            <div className="text-2xl font-bold">{result!.metrics.intercept.toFixed(2)}</div>
                                        </CardContent>
                                    </Card>
                                </div>

                                {/* Coefficients Table */}
                                <Card>
                                    <CardHeader>
                                        <CardTitle className="text-lg">Feature Impact</CardTitle>
                                        <CardDescription>Quantifying how much each feature influences {target}.</CardDescription>
                                    </CardHeader>
                                    <CardContent>
                                        <div className="space-y-4">
                                            {result!.coefficients.map(c => (
                                                <div key={c.name} className="flex items-center justify-between p-3 bg-muted/30 rounded-lg border">
                                                    <div className="flex items-center gap-3">
                                                        <Badge variant="outline">{c.name}</Badge>
                                                        <span className="text-sm font-medium">
                                                            {c.value.toFixed(4)}
                                                        </span>
                                                    </div>
                                                    <Badge variant={c.value > 0 ? 'default' : 'secondary'} className={c.value > 0 ? 'bg-green-500/10 text-green-600 hover:bg-green-500/20' : 'bg-red-500/10 text-red-600 hover:bg-red-500/20'}>
                                                        {c.impact === 'Importance' ? '📊 Importance' : (c.value > 0 ? '+ Positive Impact' : '- Negative Impact')}
                                                    </Badge>
                                                </div>
                                            ))}
                                        </div>
                                    </CardContent>
                                </Card>

                                {/* SHAP Summary (for tree models) */}
                                {result.shap_summary && result.shap_summary.length > 0 && (
                                    <Card className="border-violet-500/30 bg-gradient-to-br from-violet-500/5 to-transparent">
                                        <CardHeader>
                                            <CardTitle className="text-lg flex items-center gap-2">🔍 SHAP Analysis</CardTitle>
                                            <CardDescription>Explainability scores showing global feature importance from tree models.</CardDescription>
                                        </CardHeader>
                                        <CardContent>
                                            <div className="space-y-3">
                                                {result.shap_summary.map(s => (
                                                    <div key={s.name} className="flex items-center justify-between">
                                                        <span className="text-sm font-medium">{s.name}</span>
                                                        <div className="flex items-center gap-2">
                                                            <div className="w-32 h-2 bg-muted rounded-full overflow-hidden">
                                                                <div
                                                                    className="h-full bg-violet-500"
                                                                    style={{ width: `${(s.mean_shap / Math.max(...result.shap_summary.map(x => x.mean_shap))) * 100}%` }}
                                                                />
                                                            </div>
                                                            <span className="text-xs text-muted-foreground w-16 text-right">{s.mean_shap.toFixed(3)}</span>
                                                        </div>
                                                    </div>
                                                ))}
                                            </div>
                                        </CardContent>
                                    </Card>
                                )}

                                {/* Channel Contributions */}
                                {result.contributions && result.contributions.length > 0 && (
                                    <Card className="border-blue-500/30">
                                        <CardHeader>
                                            <CardTitle className="text-lg">📊 Channel Contribution</CardTitle>
                                            <CardDescription>Estimated contribution of each feature to total predicted {target}.</CardDescription>
                                        </CardHeader>
                                        <CardContent>
                                            <div className="space-y-3">
                                                {result.contributions.map(c => (
                                                    <div key={c.channel} className="flex items-center justify-between p-2 rounded border bg-muted/20">
                                                        <span className="font-medium">{c.channel}</span>
                                                        <div className="text-right">
                                                            <span className="text-sm font-bold">{c.pct_contribution}%</span>
                                                            <p className="text-xs text-muted-foreground">({c.contribution.toFixed(1)} units)</p>
                                                        </div>
                                                    </div>
                                                ))}
                                            </div>
                                        </CardContent>
                                    </Card>
                                )}

                                {/* Budget Recommendations */}
                                {result.recommendations && result.recommendations.length > 0 && (
                                    <Card className="border-amber-500/30 bg-gradient-to-br from-amber-500/5 to-transparent">
                                        <CardHeader>
                                            <CardTitle className="text-lg">💡 Budget Recommendations</CardTitle>
                                            <CardDescription>AI-generated suggestions based on channel efficiency (ROAS).</CardDescription>
                                        </CardHeader>
                                        <CardContent>
                                            <div className="space-y-3">
                                                {result.recommendations.map(r => (
                                                    <div key={r.channel} className="flex items-center justify-between p-3 rounded-lg border bg-muted/20">
                                                        <div>
                                                            <span className="font-bold">{r.channel}</span>
                                                            <p className="text-xs text-muted-foreground">{r.reasoning}</p>
                                                        </div>
                                                        <Badge
                                                            className={
                                                                r.recommendation === 'SCALE' ? 'bg-green-500 text-white' :
                                                                    r.recommendation === 'HOLD' ? 'bg-yellow-500 text-black' :
                                                                        'bg-red-500 text-white'
                                                            }
                                                        >
                                                            {r.recommendation === 'SCALE' ? '🚀' : r.recommendation === 'HOLD' ? '⏸️' : '✂️'} {r.recommendation}
                                                        </Badge>
                                                    </div>
                                                ))}
                                            </div>
                                        </CardContent>
                                    </Card>
                                )}

                                {/* Actual vs Predicted Chart */}
                                <Card>
                                    <CardHeader>
                                        <CardTitle>Predicted vs Actual</CardTitle>
                                        <CardDescription>Visualizing model accuracy. Points closer to the diagonal line indicate better predictions.</CardDescription>
                                    </CardHeader>
                                    <CardContent className="h-[400px]">
                                        <ResponsiveContainer width="100%" height="100%">
                                            <ScatterChart margin={{ top: 20, right: 20, bottom: 20, left: 20 }}>
                                                <CartesianGrid strokeDasharray="3 3" />
                                                <XAxis type="number" dataKey="actual" name="Actual" label={{ value: 'Actual', position: 'bottom' }} />
                                                <YAxis type="number" dataKey="predicted" name="Predicted" label={{ value: 'Predicted', angle: -90, position: 'left' }} />
                                                <Tooltip cursor={{ strokeDasharray: '3 3' }} />
                                                <Scatter name="Data Points" data={result.chart_data} fill="#8884d8" />
                                                {/* Ideal Line */}
                                                <ReferenceLine segment={[{ x: 0, y: 0 }, { x: Math.max(...result.chart_data.map(d => d.actual)), y: Math.max(...result.chart_data.map(d => d.actual)) }]} stroke="red" strokeDasharray="3 3" />
                                            </ScatterChart>
                                        </ResponsiveContainer>
                                    </CardContent>
                                </Card>
                            </>
                        )}
                    </div>
                </TabsContent>

                <TabsContent value="dimensions" className="mt-6">
                    <DimensionImportance />
                </TabsContent>
            </Tabs>
        </div>
    );
}

"use client";

import dynamic from 'next/dynamic';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { TrendingUp, ArrowRight, Brain } from "lucide-react";
import { RegressionV3Response } from "@/types/regression";

const ResponsiveContainer = dynamic(() => import('recharts').then(mod => mod.ResponsiveContainer), { ssr: false });
const BarChart = dynamic(() => import('recharts').then(mod => mod.BarChart), { ssr: false });
const Bar = dynamic(() => import('recharts').then(mod => mod.Bar), { ssr: false });
const XAxis = dynamic(() => import('recharts').then(mod => mod.XAxis), { ssr: false });
const YAxis = dynamic(() => import('recharts').then(mod => mod.YAxis), { ssr: false });
const CartesianGrid = dynamic(() => import('recharts').then(mod => mod.CartesianGrid), { ssr: false });
const Tooltip = dynamic(() => import('recharts').then(mod => mod.Tooltip), { ssr: false });

interface Props {
    results: RegressionV3Response;
    target: string;
}

export function RegressionFeatureImportance({ results, target }: Props) {
    return (
        <div className="space-y-6">
            {/* SHAP Feature Importance */}
            {results.explanations?.summary && (
                <Card>
                    <CardHeader>
                        <CardTitle>Global Feature Importance (SHAP)</CardTitle>
                        <CardDescription>
                            Shows the average impact of each feature on the model output magnitude.
                            Higher values mean the feature is more important.
                        </CardDescription>
                    </CardHeader>
                    <CardContent>
                        <ResponsiveContainer width="100%" height={300}>
                            <BarChart
                                layout="vertical"
                                data={results.explanations.summary.slice(0, 10)}
                                margin={{ top: 5, right: 30, left: 20, bottom: 5 }}
                            >
                                <CartesianGrid strokeDasharray="3 3" opacity={0.1} />
                                <XAxis type="number" />
                                <YAxis dataKey="feature" type="category" width={100} tick={{ fontSize: 12 }} />
                                <Tooltip
                                    content={({ active, payload }) => {
                                        if (active && payload && payload.length) {
                                            return (
                                                <div className="bg-background border rounded-lg p-2 shadow-lg">
                                                    <p className="font-medium">{payload[0].payload.feature}</p>
                                                    <p className="text-sm">Mean, Absolute SHAP: {payload[0].value?.toFixed(4)}</p>
                                                </div>
                                            );
                                        }
                                        return null;
                                    }}
                                />
                                <Bar dataKey="mean_abs_shap" fill="#0ea5e9" radius={[0, 4, 4, 0]} />
                            </BarChart>
                        </ResponsiveContainer>
                    </CardContent>
                </Card>
            )}

            {/* Top Drivers */}
            <Card className="border-emerald-500/20 bg-emerald-500/5">
                <CardHeader>
                    <div className="flex items-center gap-3">
                        <div className="bg-emerald-500 rounded-lg p-2">
                            <TrendingUp className="h-5 w-5 text-white" />
                        </div>
                        <div>
                            <CardTitle>Top Drivers</CardTitle>
                            <CardDescription>Features with highest impact on {target}</CardDescription>
                        </div>
                    </div>
                </CardHeader>
                <CardContent className="space-y-4">
                    {results.feature_insights.slice(0, 5).map((insight) => (
                        <div key={insight.rank} className="flex items-start gap-4 p-4 bg-muted/50 rounded-lg">
                            <div className="flex-shrink-0 w-8 h-8 bg-emerald-500 rounded-full flex items-center justify-center text-white font-bold">
                                {insight.rank}
                            </div>
                            <div className="flex-1 space-y-2">
                                <div className="flex items-center gap-2">
                                    <span className="font-semibold">{insight.feature}</span>
                                    <Badge variant="outline">{insight.impact} Impact</Badge>
                                </div>
                                <p className="text-sm text-muted-foreground">{insight.interpretation}</p>
                                <div className="flex items-center gap-2 text-sm">
                                    <ArrowRight className="h-4 w-4" />
                                    <span>{insight.action}</span>
                                </div>
                            </div>
                        </div>
                    ))}
                </CardContent>
            </Card>

            {/* Executive Summary */}
            <Card className="border-purple-500/20 bg-purple-500/5">
                <CardHeader>
                    <div className="flex items-center gap-3">
                        <div className="bg-purple-500 rounded-lg p-2">
                            <Brain className="h-5 w-5 text-white" />
                        </div>
                        <div>
                            <CardTitle>Executive Summary</CardTitle>
                            <CardDescription>AI-generated insights and recommendations</CardDescription>
                        </div>
                    </div>
                </CardHeader>
                <CardContent>
                    <div className="prose prose-sm max-w-none">
                        <div className="whitespace-pre-wrap">{results.executive_summary}</div>
                    </div>
                </CardContent>
            </Card>
        </div>
    );
}

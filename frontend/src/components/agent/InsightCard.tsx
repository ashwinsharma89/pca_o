import React from 'react';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';
import { TrendingUp, TrendingDown, AlertTriangle, Lightbulb, Activity } from 'lucide-react';

interface InsightCardProps {
    insights: any;
    isLoading: boolean;
}

export function InsightCard({ insights, isLoading }: InsightCardProps) {
    if (isLoading) {
        return (
            <div className="space-y-4">
                {[1, 2, 3].map((i) => (
                    <div key={i} className="h-24 w-full animate-pulse rounded-lg bg-slate-100 dark:bg-slate-800" />
                ))}
            </div>
        );
    }

    if (!insights) {
        return (
            <Card>
                <CardContent className="p-8 text-center text-muted-foreground">
                    No insights available yet.
                </CardContent>
            </Card>
        );
    }

    const { pattern_insights, recommendations } = insights;

    return (
        <div className="space-y-6">
            {/* High Level Summary */}
            {insights.performance_summary && (
                <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                    <Card className="bg-slate-50 dark:bg-slate-900 border-none shadow-sm">
                        <CardContent className="p-4">
                            <p className="text-xs text-muted-foreground uppercase">CTR</p>
                            <p className="text-xl font-bold">{(insights.performance_summary.overall_ctr * 100).toFixed(2)}%</p>
                        </CardContent>
                    </Card>
                    {/* Add more summary metrics if available, using safe checks */}
                </div>
            )}

            {/* Pattern Alerts */}
            <div className="grid gap-4">
                {pattern_insights && pattern_insights.length > 0 && (
                    <div className="space-y-3">
                        <h3 className="text-lg font-semibold flex items-center gap-2">
                            <Activity className="w-5 h-5 text-blue-500" />
                            Detected Patterns
                        </h3>
                        {pattern_insights.map((insight: string, index: number) => {
                            const isNegative = insight.includes('declining') || insight.includes('fatigue') || insight.includes('saturation');
                            return (
                                <Card key={index} className={`border-l-4 ${isNegative ? 'border-l-red-500' : 'border-l-green-500'}`}>
                                    <CardContent className="p-4 flex items-start gap-3">
                                        {isNegative ? <TrendingDown className="w-5 h-5 text-red-500 shrink-0 mt-0.5" /> : <TrendingUp className="w-5 h-5 text-green-500 shrink-0 mt-0.5" />}
                                        <p className="text-sm">{insight}</p>
                                    </CardContent>
                                </Card>
                            );
                        })}
                    </div>
                )}

                {/* Recommendations */}
                {recommendations && recommendations.length > 0 && (
                    <div className="space-y-3 mt-4">
                        <h3 className="text-lg font-semibold flex items-center gap-2">
                            <Lightbulb className="w-5 h-5 text-yellow-500" />
                            Strategic Recommendations
                        </h3>
                        {recommendations.map((rec: any, index: number) => (
                            <Card key={index} className="border-l-4 border-l-yellow-500 bg-yellow-50/50 dark:bg-yellow-900/10">
                                <CardHeader className="pb-2">
                                    <CardTitle className="text-base font-medium flex justify-between">
                                        {rec.category}
                                        <span className="text-xs font-normal px-2 py-0.5 rounded-full bg-white/50 dark:bg-black/20 border">
                                            Impact: {rec.expected_impact}
                                        </span>
                                    </CardTitle>
                                    <CardDescription className="text-xs uppercase tracking-wider font-semibold text-yellow-600 dark:text-yellow-400">
                                        {rec.issue}
                                    </CardDescription>
                                </CardHeader>
                                <CardContent>
                                    <p className="text-sm font-medium">{rec.recommendation}</p>
                                </CardContent>
                            </Card>
                        ))}
                    </div>
                )}
            </div>
        </div>
    );
}

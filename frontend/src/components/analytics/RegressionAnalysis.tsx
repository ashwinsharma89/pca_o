"use client";

import { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Badge } from "@/components/ui/badge";
import { Loader2, TrendingUp, BarChart3, Zap, Target, AlertTriangle, CheckCircle, ArrowRight } from "lucide-react";
import { api } from "@/lib/api";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";

interface ModelResult {
    Model: string;
    "R² (Test)": number;
    RMSE: number;
    MAE: number;
    MAPE: string;
    "Training Time": string;
    Interpretability: string;
}

interface FeatureImportance {
    feature: string;
    rank: number;
    avg_importance: number;
}

interface RegressionResponse {
    success: boolean;
    model: {
        type: string;
        r2: number;
        rmse: number;
        target: string;
        target_type: string;
    };
    insights: {
        executive_summary: string;
        recommendations: string[];
        platform_insights?: string;
    };
    model_comparison?: ModelResult[];
    feature_importance?: FeatureImportance[];
    warnings?: string[];
}

export default function RegressionAnalysis() {
    const [loading, setLoading] = useState(false);
    const [results, setResults] = useState<RegressionResponse | null>(null);
    const [error, setError] = useState<string | null>(null);

    // Config
    const [target, setTarget] = useState("conversions");
    const [features, setFeatures] = useState("spend,impressions,clicks");
    const [models, setModels] = useState("OLS,Ridge,Lasso,Bayesian");
    const [preferInterpretability, setPreferInterpretability] = useState(false);

    const runRegression = async () => {
        setLoading(true);
        setError(null);
        try {
            const response = await api.get(`/campaigns/regression/v2?target=${target}&features=${features}&models=${models}&prefer_interpretability=${preferInterpretability}&quick_mode=true`) as RegressionResponse;
            setResults(response);
        } catch (err) {
            setError(String(err));
        } finally {
            setLoading(false);
        }
    };

    const getEffectColor = (r2: number) => {
        if (r2 >= 0.7) return "text-emerald-500";
        if (r2 >= 0.5) return "text-yellow-500";
        return "text-red-500";
    };

    return (
        <div className="space-y-6">
            {/* Configuration */}
            <Card className="border-violet-500/20 bg-violet-500/5">
                <CardHeader>
                    <div className="flex items-center gap-3">
                        <div className="bg-violet-500 rounded-lg p-2">
                            <BarChart3 className="h-5 w-5 text-white" />
                        </div>
                        <div>
                            <CardTitle>Regression Analysis</CardTitle>
                            <CardDescription>Compare models and identify key drivers</CardDescription>
                        </div>
                    </div>
                </CardHeader>
                <CardContent className="space-y-4">
                    <div className="grid gap-4 md:grid-cols-3">
                        <div className="space-y-2">
                            <label className="text-sm font-medium">Target Metric</label>
                            <Select value={target} onValueChange={setTarget}>
                                <SelectTrigger>
                                    <SelectValue />
                                </SelectTrigger>
                                <SelectContent>
                                    <SelectItem value="conversions">Conversions</SelectItem>
                                    <SelectItem value="clicks">Clicks</SelectItem>
                                    <SelectItem value="impressions">Impressions</SelectItem>
                                    <SelectItem value="roas">ROAS</SelectItem>
                                </SelectContent>
                            </Select>
                        </div>
                        <div className="space-y-2">
                            <label className="text-sm font-medium">Features</label>
                            <Select value={features} onValueChange={setFeatures}>
                                <SelectTrigger>
                                    <SelectValue />
                                </SelectTrigger>
                                <SelectContent>
                                    <SelectItem value="spend,impressions,clicks">Spend, Impressions, Clicks</SelectItem>
                                    <SelectItem value="spend,impressions,clicks,ctr">+ CTR</SelectItem>
                                    <SelectItem value="spend,impressions,clicks,ctr,cpm">+ CTR, CPM</SelectItem>
                                </SelectContent>
                            </Select>
                        </div>
                        <div className="space-y-2">
                            <label className="text-sm font-medium">Models</label>
                            <Select value={models} onValueChange={setModels}>
                                <SelectTrigger>
                                    <SelectValue />
                                </SelectTrigger>
                                <SelectContent>
                                    <SelectItem value="OLS,Ridge,Lasso,Bayesian">Linear Models</SelectItem>
                                    <SelectItem value="OLS,Ridge,Lasso,Bayesian,Random Forest">+ Random Forest</SelectItem>
                                    <SelectItem value="OLS,Ridge,Lasso,Bayesian,Random Forest,XGBoost">All Models</SelectItem>
                                </SelectContent>
                            </Select>
                        </div>
                    </div>
                    <Button
                        onClick={runRegression}
                        disabled={loading}
                        className="w-full bg-violet-600 hover:bg-violet-700"
                    >
                        {loading ? (
                            <>
                                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                                Running Models...
                            </>
                        ) : (
                            <>
                                <Zap className="mr-2 h-4 w-4" />
                                Run Regression Analysis
                            </>
                        )}
                    </Button>
                </CardContent>
            </Card>

            {/* Error */}
            {error && (
                <Card className="border-red-500/30 bg-red-500/5">
                    <CardContent className="pt-6 flex items-center gap-3 text-red-500">
                        <AlertTriangle className="h-5 w-5" />
                        <span>{error}</span>
                    </CardContent>
                </Card>
            )}

            {/* Results */}
            {results?.success && (
                <div className="space-y-6 animate-in slide-in-from-bottom-4">
                    {/* Best Model Summary */}
                    <Card className="border-emerald-500/30 bg-emerald-500/5">
                        <CardHeader>
                            <div className="flex items-center justify-between">
                                <div className="flex items-center gap-3">
                                    <div className="bg-emerald-500 rounded-lg p-2">
                                        <CheckCircle className="h-5 w-5 text-white" />
                                    </div>
                                    <div>
                                        <CardTitle>Best Model: {results.model.type}</CardTitle>
                                        <CardDescription>Target: {results.model.target}</CardDescription>
                                    </div>
                                </div>
                                <div className="text-right">
                                    <div className={`text-3xl font-bold ${getEffectColor(results.model.r2)}`}>
                                        {(results.model.r2 * 100).toFixed(1)}%
                                    </div>
                                    <div className="text-sm text-muted-foreground">R² Score</div>
                                </div>
                            </div>
                        </CardHeader>
                        {results.insights?.executive_summary && (
                            <CardContent>
                                <p className="text-sm text-muted-foreground">{results.insights.executive_summary}</p>
                            </CardContent>
                        )}
                    </Card>

                    {/* Model Comparison */}
                    {results.model_comparison && results.model_comparison.length > 0 && (
                        <Card>
                            <CardHeader>
                                <CardTitle className="flex items-center gap-2">
                                    <BarChart3 className="h-5 w-5 text-violet-500" />
                                    Model Comparison
                                </CardTitle>
                            </CardHeader>
                            <CardContent>
                                <Table>
                                    <TableHeader>
                                        <TableRow>
                                            <TableHead>Model</TableHead>
                                            <TableHead>R²</TableHead>
                                            <TableHead>RMSE</TableHead>
                                            <TableHead>Time</TableHead>
                                            <TableHead>Interpretability</TableHead>
                                        </TableRow>
                                    </TableHeader>
                                    <TableBody>
                                        {results.model_comparison.map((model, i) => (
                                            <TableRow key={i} className={model.Model === results.model.type ? "bg-emerald-500/10" : ""}>
                                                <TableCell className="font-medium">
                                                    {model.Model}
                                                    {model.Model === results.model.type && (
                                                        <Badge className="ml-2 bg-emerald-500">Best</Badge>
                                                    )}
                                                </TableCell>
                                                <TableCell className={getEffectColor(model["R² (Test)"])}>
                                                    {(model["R² (Test)"] * 100).toFixed(1)}%
                                                </TableCell>
                                                <TableCell>{model.RMSE?.toFixed(2)}</TableCell>
                                                <TableCell>{model["Training Time"]}</TableCell>
                                                <TableCell>
                                                    <Badge variant="outline">{model.Interpretability}</Badge>
                                                </TableCell>
                                            </TableRow>
                                        ))}
                                    </TableBody>
                                </Table>
                            </CardContent>
                        </Card>
                    )}

                    {/* Feature Importance */}
                    {results.feature_importance && results.feature_importance.length > 0 && (
                        <Card>
                            <CardHeader>
                                <CardTitle className="flex items-center gap-2">
                                    <TrendingUp className="h-5 w-5 text-violet-500" />
                                    Feature Importance
                                </CardTitle>
                            </CardHeader>
                            <CardContent>
                                <div className="space-y-3">
                                    {results.feature_importance.map((feat, i) => (
                                        <div key={i} className="flex items-center gap-4">
                                            <span className="text-sm font-bold text-violet-500 w-8">#{feat.rank}</span>
                                            <span className="flex-1 font-medium">{feat.feature}</span>
                                            <div className="w-48 bg-muted rounded-full h-2">
                                                <div
                                                    className="bg-violet-500 h-2 rounded-full transition-all"
                                                    style={{ width: `${Math.min(feat.avg_importance * 100, 100)}%` }}
                                                />
                                            </div>
                                            <span className="text-sm text-muted-foreground w-16 text-right">
                                                {(feat.avg_importance * 100).toFixed(1)}%
                                            </span>
                                        </div>
                                    ))}
                                </div>
                            </CardContent>
                        </Card>
                    )}

                    {/* Recommendations */}
                    {results.insights?.recommendations && results.insights.recommendations.length > 0 && (
                        <Card>
                            <CardHeader>
                                <CardTitle className="flex items-center gap-2">
                                    <Target className="h-5 w-5 text-violet-500" />
                                    Recommendations
                                </CardTitle>
                            </CardHeader>
                            <CardContent>
                                <div className="space-y-3">
                                    {results.insights.recommendations.map((rec, i) => (
                                        <div key={i} className="flex items-start gap-3 p-3 rounded-lg bg-muted/50">
                                            <ArrowRight className="h-4 w-4 text-violet-500 mt-0.5" />
                                            <span className="text-sm">{rec}</span>
                                        </div>
                                    ))}
                                </div>
                            </CardContent>
                        </Card>
                    )}

                    {/* Warnings */}
                    {results.warnings && results.warnings.length > 0 && (
                        <Card className="border-yellow-500/30 bg-yellow-500/5">
                            <CardHeader>
                                <CardTitle className="flex items-center gap-2 text-yellow-500">
                                    <AlertTriangle className="h-5 w-5" />
                                    Validation Warnings
                                </CardTitle>
                            </CardHeader>
                            <CardContent>
                                <ul className="space-y-2">
                                    {results.warnings.map((warning, i) => (
                                        <li key={i} className="text-sm text-yellow-600">{warning}</li>
                                    ))}
                                </ul>
                            </CardContent>
                        </Card>
                    )}
                </div>
            )}
        </div>
    );
}

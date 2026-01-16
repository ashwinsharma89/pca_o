"use client";

import { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Badge } from "@/components/ui/badge";
import { Loader2, Layers, Target, AlertTriangle, ArrowRight, TrendingUp, TrendingDown } from "lucide-react";
import { api } from "@/lib/api";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";

interface DimensionDetail {
    dimension: string;
    importance_score: number;
    effect_size: number;
    effect: string;
    p_value: number | null;
    n_unique: number;
    top_values: Array<{ value: string; mean: number; count: number }>;
    recommendation: string;
}

interface Interaction {
    dimension_1: string;
    dimension_2: string;
    f_statistic: number;
    best_combination: string;
    best_value: number;
}

interface DimensionResponse {
    success: boolean;
    target_metric: string;
    sample_size: number;
    dimensions_analyzed: string[];
    rankings: Array<{
        Rank: number;
        Dimension: string;
        "Importance Score": number;
        "Effect Size": string;
        Effect: string;
    }>;
    dimension_details: DimensionDetail[];
    interactions?: Interaction[];
    recommendations: string[];
}

export default function DimensionImportance() {
    const [loading, setLoading] = useState(false);
    const [results, setResults] = useState<DimensionResponse | null>(null);
    const [error, setError] = useState<string | null>(null);
    const [selectedDimension, setSelectedDimension] = useState<DimensionDetail | null>(null);

    // Config
    const [target, setTarget] = useState("conversions");

    const runAnalysis = async () => {
        setLoading(true);
        setError(null);
        try {
            const response = await api.get(`/campaigns/dimension-importance?target=${target}&include_interactions=true`) as DimensionResponse;
            setResults(response);
            if (response.dimension_details?.length > 0) {
                setSelectedDimension(response.dimension_details[0]);
            }
        } catch (err) {
            setError(String(err));
        } finally {
            setLoading(false);
        }
    };

    const getEffectBadge = (effect: string) => {
        switch (effect) {
            case "Large":
                return <Badge className="bg-emerald-500">Large</Badge>;
            case "Medium":
                return <Badge className="bg-yellow-500">Medium</Badge>;
            case "Small":
                return <Badge className="bg-blue-500">Small</Badge>;
            default:
                return <Badge variant="secondary">Negligible</Badge>;
        }
    };

    const getImportanceColor = (score: number) => {
        if (score >= 50) return "text-emerald-500";
        if (score >= 30) return "text-yellow-500";
        return "text-muted-foreground";
    };

    return (
        <div className="space-y-6">
            {/* Configuration */}
            <Card className="border-indigo-500/20 bg-indigo-500/5">
                <CardHeader>
                    <div className="flex items-center gap-3">
                        <div className="bg-indigo-500 rounded-lg p-2">
                            <Layers className="h-5 w-5 text-white" />
                        </div>
                        <div>
                            <CardTitle>Dimension Importance</CardTitle>
                            <CardDescription>Identify which dimensions drive performance</CardDescription>
                        </div>
                    </div>
                </CardHeader>
                <CardContent className="space-y-4">
                    <div className="grid gap-4 md:grid-cols-2">
                        <div className="space-y-2">
                            <label className="text-sm font-medium">Target Metric</label>
                            <Select value={target} onValueChange={setTarget}>
                                <SelectTrigger>
                                    <SelectValue />
                                </SelectTrigger>
                                <SelectContent>
                                    <SelectItem value="conversions">Conversions</SelectItem>
                                    <SelectItem value="clicks">Clicks</SelectItem>
                                    <SelectItem value="spend">Spend</SelectItem>
                                    <SelectItem value="roas">ROAS</SelectItem>
                                </SelectContent>
                            </Select>
                        </div>
                        <div className="flex items-end">
                            <Button
                                onClick={runAnalysis}
                                disabled={loading}
                                className="w-full bg-indigo-600 hover:bg-indigo-700"
                            >
                                {loading ? (
                                    <>
                                        <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                                        Analyzing...
                                    </>
                                ) : (
                                    <>
                                        <Layers className="mr-2 h-4 w-4" />
                                        Analyze Dimensions
                                    </>
                                )}
                            </Button>
                        </div>
                    </div>
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
                    {/* Summary */}
                    <div className="grid gap-4 md:grid-cols-3">
                        <Card className="bg-gradient-to-br from-indigo-500/10 to-violet-500/10">
                            <CardContent className="pt-6">
                                <div className="text-3xl font-bold text-indigo-500">{results.dimensions_analyzed.length}</div>
                                <div className="text-sm text-muted-foreground">Dimensions Analyzed</div>
                            </CardContent>
                        </Card>
                        <Card className="bg-gradient-to-br from-emerald-500/10 to-teal-500/10">
                            <CardContent className="pt-6">
                                <div className="text-3xl font-bold text-emerald-500">
                                    {results.dimension_details.filter(d => d.effect === "Large").length}
                                </div>
                                <div className="text-sm text-muted-foreground">High-Impact Dimensions</div>
                            </CardContent>
                        </Card>
                        <Card className="bg-gradient-to-br from-violet-500/10 to-purple-500/10">
                            <CardContent className="pt-6">
                                <div className="text-3xl font-bold text-violet-500">
                                    {results.sample_size.toLocaleString()}
                                </div>
                                <div className="text-sm text-muted-foreground">Rows Analyzed</div>
                            </CardContent>
                        </Card>
                    </div>

                    {/* Rankings Table */}
                    <Card>
                        <CardHeader>
                            <CardTitle className="flex items-center gap-2">
                                <TrendingUp className="h-5 w-5 text-indigo-500" />
                                Dimension Rankings
                            </CardTitle>
                        </CardHeader>
                        <CardContent>
                            <Table>
                                <TableHeader>
                                    <TableRow>
                                        <TableHead className="w-16">Rank</TableHead>
                                        <TableHead>Dimension</TableHead>
                                        <TableHead>Importance</TableHead>
                                        <TableHead>Effect</TableHead>
                                        <TableHead>Values</TableHead>
                                        <TableHead></TableHead>
                                    </TableRow>
                                </TableHeader>
                                <TableBody>
                                    {results.dimension_details.map((dim, i) => (
                                        <TableRow
                                            key={i}
                                            className={`cursor-pointer hover:bg-indigo-500/5 ${selectedDimension?.dimension === dim.dimension ? "bg-indigo-500/10" : ""}`}
                                            onClick={() => setSelectedDimension(dim)}
                                        >
                                            <TableCell>
                                                <span className="text-lg font-bold text-indigo-500">#{i + 1}</span>
                                            </TableCell>
                                            <TableCell className="font-medium">{dim.dimension}</TableCell>
                                            <TableCell>
                                                <div className="flex items-center gap-2">
                                                    <div className="w-24 bg-muted rounded-full h-2">
                                                        <div
                                                            className="bg-indigo-500 h-2 rounded-full"
                                                            style={{ width: `${Math.min(dim.importance_score, 100)}%` }}
                                                        />
                                                    </div>
                                                    <span className={`text-sm font-medium ${getImportanceColor(dim.importance_score)}`}>
                                                        {dim.importance_score.toFixed(0)}%
                                                    </span>
                                                </div>
                                            </TableCell>
                                            <TableCell>{getEffectBadge(dim.effect)}</TableCell>
                                            <TableCell>{dim.n_unique}</TableCell>
                                            <TableCell>
                                                <ArrowRight className="h-4 w-4 text-muted-foreground" />
                                            </TableCell>
                                        </TableRow>
                                    ))}
                                </TableBody>
                            </Table>
                        </CardContent>
                    </Card>

                    {/* Selected Dimension Detail */}
                    {selectedDimension && (
                        <Card className="border-indigo-500/30">
                            <CardHeader>
                                <div className="flex items-center justify-between">
                                    <div>
                                        <CardTitle className="flex items-center gap-2">
                                            <Target className="h-5 w-5 text-indigo-500" />
                                            {selectedDimension.dimension}
                                        </CardTitle>
                                        <CardDescription>{selectedDimension.recommendation}</CardDescription>
                                    </div>
                                    {getEffectBadge(selectedDimension.effect)}
                                </div>
                            </CardHeader>
                            <CardContent className="space-y-4">
                                {/* Stats */}
                                <div className="grid grid-cols-3 gap-4">
                                    <div className="text-center p-3 bg-muted/50 rounded-lg">
                                        <div className="text-2xl font-bold text-indigo-500">
                                            {selectedDimension.importance_score.toFixed(1)}%
                                        </div>
                                        <div className="text-xs text-muted-foreground">Importance</div>
                                    </div>
                                    <div className="text-center p-3 bg-muted/50 rounded-lg">
                                        <div className="text-2xl font-bold">
                                            {(selectedDimension.effect_size * 100).toFixed(1)}%
                                        </div>
                                        <div className="text-xs text-muted-foreground">Effect Size</div>
                                    </div>
                                    <div className="text-center p-3 bg-muted/50 rounded-lg">
                                        <div className="text-2xl font-bold">
                                            {selectedDimension.p_value !== null ? (
                                                selectedDimension.p_value < 0.001 ? "<0.001" : selectedDimension.p_value.toFixed(3)
                                            ) : "N/A"}
                                        </div>
                                        <div className="text-xs text-muted-foreground">p-value</div>
                                    </div>
                                </div>

                                {/* Top Values */}
                                <div>
                                    <h4 className="text-sm font-medium mb-3">Top Performing Values</h4>
                                    <div className="space-y-2">
                                        {selectedDimension.top_values.map((val, i) => (
                                            <div key={i} className="flex items-center justify-between p-2 bg-muted/30 rounded-lg">
                                                <div className="flex items-center gap-3">
                                                    <span className="text-xs font-bold text-indigo-500 w-6">#{i + 1}</span>
                                                    <span className="font-medium">{val.value}</span>
                                                </div>
                                                <div className="flex items-center gap-4">
                                                    <span className="text-sm">
                                                        Avg: <span className="font-bold">{val.mean.toFixed(1)}</span>
                                                    </span>
                                                    <span className="text-xs text-muted-foreground">
                                                        ({val.count.toLocaleString()} rows)
                                                    </span>
                                                </div>
                                            </div>
                                        ))}
                                    </div>
                                </div>
                            </CardContent>
                        </Card>
                    )}

                    {/* Interactions */}
                    {results.interactions && results.interactions.length > 0 && (
                        <Card>
                            <CardHeader>
                                <CardTitle className="flex items-center gap-2">
                                    <Layers className="h-5 w-5 text-violet-500" />
                                    Dimension Interactions
                                </CardTitle>
                                <CardDescription>Significant two-way interactions</CardDescription>
                            </CardHeader>
                            <CardContent>
                                <div className="grid gap-3 md:grid-cols-2">
                                    {results.interactions.slice(0, 6).map((inter, i) => (
                                        <div key={i} className="p-4 bg-muted/30 rounded-lg">
                                            <div className="flex items-center gap-2 mb-2">
                                                <Badge variant="outline">{inter.dimension_1}</Badge>
                                                <span className="text-muted-foreground">×</span>
                                                <Badge variant="outline">{inter.dimension_2}</Badge>
                                            </div>
                                            <div className="text-sm">
                                                <span className="text-muted-foreground">Best: </span>
                                                <span className="font-medium">{inter.best_combination}</span>
                                            </div>
                                            <div className="text-xs text-muted-foreground mt-1">
                                                F={inter.f_statistic.toFixed(1)} | Value={inter.best_value.toFixed(1)}
                                            </div>
                                        </div>
                                    ))}
                                </div>
                            </CardContent>
                        </Card>
                    )}

                    {/* Recommendations */}
                    {results.recommendations.length > 0 && (
                        <Card>
                            <CardHeader>
                                <CardTitle className="flex items-center gap-2">
                                    <Target className="h-5 w-5 text-indigo-500" />
                                    Recommendations
                                </CardTitle>
                            </CardHeader>
                            <CardContent>
                                <div className="space-y-3">
                                    {results.recommendations.map((rec, i) => (
                                        <div key={i} className="flex items-start gap-3 p-3 rounded-lg bg-indigo-500/5 border border-indigo-500/20">
                                            <ArrowRight className="h-4 w-4 text-indigo-500 mt-0.5" />
                                            <span className="text-sm">{rec}</span>
                                        </div>
                                    ))}
                                </div>
                            </CardContent>
                        </Card>
                    )}
                </div>
            )}
        </div>
    );
}

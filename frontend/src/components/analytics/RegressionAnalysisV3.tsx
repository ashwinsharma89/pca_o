"use client";

import { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { MultiSelect } from "@/components/ui/multi-select";
import { Badge } from "@/components/ui/badge";
import { Loader2, TrendingUp, BarChart3, Zap, Target, AlertTriangle, CheckCircle, ArrowRight, Activity, Brain, TrendingDown } from "lucide-react";
import { api } from "@/lib/api";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Progress } from "@/components/ui/progress";
import dynamic from 'next/dynamic';

import { RegressionModelHealth } from "./RegressionModelHealth";
import { RegressionDiagnostics } from "./RegressionDiagnostics";
import { RegressionFeatureImportance } from "./RegressionFeatureImportance";
import { RegressionSimulator } from "./RegressionSimulator";
import { RegressionV3Response } from "@/types/regression";

// V3 API Response Types

export default function RegressionAnalysisV3() {
    const [loading, setLoading] = useState(false);
    const [results, setResults] = useState<RegressionV3Response | null>(null);
    const [error, setError] = useState<string | null>(null);
    const [availableColumns, setAvailableColumns] = useState<string[]>([]);

    // Config
    const [target, setTarget] = useState("Conversions");
    const [selectedFeatures, setSelectedFeatures] = useState<string[]>(["Total Spent", "Impressions"]);
    const [selectedModels, setSelectedModels] = useState<string[]>(["Ridge"]);

    // Define model options
    const modelOptions = [
        { label: "Ridge (Recommended)", value: "Ridge" },
        { label: "OLS (Linear Regression)", value: "OLS" },
        { label: "Elastic Net", value: "Elastic Net" },
        { label: "Bayesian Ridge (Probabilistic)", value: "Bayesian Ridge" },
        { label: "Gradient Descent (SGD)", value: "Gradient Descent" },
        { label: "Random Forest", value: "Random Forest" },
        { label: "XGBoost", value: "XGBoost" }
    ];



    // Fetch available columns on mount
    useEffect(() => {
        const fetchColumns = async () => {
            try {
                const response: any = await api.get('/campaigns/columns');
                if (response.columns) {
                    setAvailableColumns(response.columns);
                }
            } catch (err) {
                console.error('Failed to fetch columns:', err);
                // Fallback to common columns
                setAvailableColumns([
                    'Total Spent', 'Spend', 'Impressions', 'Clicks', 'Conversions',
                    'Revenue', 'CTR', 'CPC', 'CPA', 'ROAS', 'Reach', 'Frequency'
                ]);
            }
        };
        fetchColumns();
    }, []);

    const runRegression = async () => {
        if (selectedFeatures.length === 0) {
            setError("Please select at least one feature");
            return;
        }

        setLoading(true);
        setError(null);
        try {
            const featuresParam = encodeURIComponent(selectedFeatures.join(','));
            const modelsParam = encodeURIComponent(selectedModels.join(','));
            const targetParam = encodeURIComponent(target);

            const response = await api.get(
                `/campaigns/regression/v3?target=${targetParam}&features=${featuresParam}&models=${modelsParam}&quick_mode=true`
            ) as RegressionV3Response;

            if (response.success) {
                setResults(response);
            } else {
                setError(response.error || "Analysis failed. Please check your data.");
            }
        } catch (err: any) {
            console.error('Regression error:', err);
            let errorMsg = err.message || String(err);

            if (err.response?.data?.detail) {
                const detail = err.response.data.detail;
                if (typeof detail === 'object') {
                    errorMsg = detail.error || JSON.stringify(detail);
                } else {
                    errorMsg = detail;
                }
            }

            setError(errorMsg);
        } finally {
            setLoading(false);
        }
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
                            <CardTitle>Regression Analysis V3</CardTitle>
                            <CardDescription>Production ML with comprehensive diagnostics</CardDescription>
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
                                    <SelectItem value="Conversions">Conversions</SelectItem>
                                    <SelectItem value="Revenue">Revenue</SelectItem>
                                    <SelectItem value="Clicks">Clicks</SelectItem>
                                </SelectContent>
                            </Select>
                        </div>

                        <div className="space-y-2">
                            <label className="text-sm font-medium">Features</label>
                            <MultiSelect
                                options={availableColumns.map(col => ({ label: col, value: col }))}
                                selected={selectedFeatures}
                                onChange={setSelectedFeatures}
                                placeholder="Select features..."
                            />
                        </div>

                        <div className="space-y-2">
                            <label className="text-sm font-medium">Models</label>
                            <MultiSelect
                                options={modelOptions}
                                selected={selectedModels}
                                onChange={setSelectedModels}
                                placeholder="Select models..."
                            />
                        </div>
                    </div>

                    <Button onClick={runRegression} disabled={loading || selectedFeatures.length === 0} className="w-full">
                        {loading ? (
                            <>
                                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                                Running Analysis...
                            </>
                        ) : (
                            <>
                                <Zap className="mr-2 h-4 w-4" />
                                Run Regression
                            </>
                        )}
                    </Button>
                </CardContent>
            </Card>

            {/* Error */}
            {error && (
                <Card className="border-red-500/20 bg-red-500/5">
                    <CardContent className="pt-6">
                        <div className="flex items-center gap-2 text-red-500">
                            <AlertTriangle className="h-5 w-5" />
                            <span>{error}</span>
                        </div>
                    </CardContent>
                </Card>
            )}

            {/* Results */}
            {results && (
                <>
                    {/* Modular Components */}
                    <RegressionModelHealth results={results} />
                    <RegressionSimulator results={results} target={target} />
                    <RegressionDiagnostics results={results} />
                    <RegressionFeatureImportance results={results} target={target} />
                </>
            )}
        </div>
    );
}

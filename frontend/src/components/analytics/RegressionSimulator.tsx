"use client";

import { useState, useEffect, useMemo } from "react";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Slider } from "@/components/ui/slider";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { RotateCcw, Calculator, Info } from "lucide-react";
import { RegressionV3Response } from "@/types/regression";

interface Props {
    results: RegressionV3Response;
    target: string;
}

export function RegressionSimulator({ results, target }: Props) {
    const [values, setValues] = useState<Record<string, number>>({});
    const [prediction, setPrediction] = useState<number>(0);

    // Initialize values to mean (average case)
    useEffect(() => {
        if (results.feature_stats) {
            const initial: Record<string, number> = {};
            Object.entries(results.feature_stats).forEach(([feat, stats]) => {
                initial[feat] = stats.mean;
            });
            setValues(initial);
        }
    }, [results]);

    // Calculate Prediction
    // Linear Model Formula: y = base_value + sum(coef * (x - mean) / std)
    // Note: coefficients are from StandardScaled data
    useEffect(() => {
        if (!values || Object.keys(values).length === 0) return;

        // Base value from SHAP (expected value) or fallback to mean prediction
        let pred = results.explanations?.base_value || results.predictions.sample[0]?.predicted || 0;

        // If we don't have coefficients (e.g. Random Forest), we can't simulate easily client-side
        // In that case, we might need to disable or show warning
        const coeffs = results.coefficients || {};
        const stats = results.feature_stats || {};

        if (Object.keys(coeffs).length > 0) {
            // Linear Simulation
            // We use the difference from the mean, as base_value represents the average prediction
            Object.entries(values).forEach(([feat, val]) => {
                const coef = coeffs[feat] || 0;
                const featStats = stats[feat];

                if (featStats && featStats.std !== 0) {
                    // Calculate contribution relative to mean
                    const z_score = (val - featStats.mean) / featStats.std;
                    // Contribution = Coef * Z-Score
                    pred += coef * z_score;
                }
            });
        }

        setPrediction(pred);
    }, [values, results]);

    const handleReset = () => {
        if (results.feature_stats) {
            const initial: Record<string, number> = {};
            Object.entries(results.feature_stats).forEach(([feat, stats]) => {
                initial[feat] = stats.mean;
            });
            setValues(initial);
        }
    };

    const handleSliderChange = (feat: string, val: number) => {
        setValues(prev => ({
            ...prev,
            [feat]: val
        }));
    };

    if (!results.coefficients || !results.feature_stats) {
        return null; // Don't show if data missing or non-linear model without coeffs
    }

    const isLinear = results.model.type === "Ridge" || results.model.type === "Linear";

    return (
        <Card className="border-indigo-500/20 bg-indigo-500/5">
            <CardHeader>
                <div className="flex items-center justify-between">
                    <div className="flex items-center gap-3">
                        <div className="bg-indigo-500 rounded-lg p-2">
                            <Calculator className="h-5 w-5 text-white" />
                        </div>
                        <div>
                            <CardTitle>What-If Simulator</CardTitle>
                            <CardDescription>
                                Adjust drivers to simulate impact on {target}
                            </CardDescription>
                        </div>
                    </div>
                    <Button variant="outline" size="sm" onClick={handleReset} className="gap-2">
                        <RotateCcw className="h-4 w-4" />
                        Reset
                    </Button>
                </div>
            </CardHeader>
            <CardContent>
                {/* Prediction Display */}
                <div className="mb-8 p-6 bg-background rounded-xl border shadow-sm text-center">
                    <div className="text-sm text-muted-foreground uppercase tracking-wider font-semibold mb-1">
                        Simulated {target}
                    </div>
                    <div className="text-4xl font-bold text-indigo-600 dark:text-indigo-400">
                        {prediction.toLocaleString(undefined, { maximumFractionDigits: 0 })}
                    </div>
                    {!isLinear && (
                        <div className="flex items-center justify-center gap-2 mt-2 text-xs text-yellow-600 bg-yellow-100 dark:bg-yellow-900/30 dark:text-yellow-400 py-1 px-3 rounded-full w-fit mx-auto">
                            <Info className="h-3 w-3" />
                            Approximation (Model is non-linear)
                        </div>
                    )}
                </div>

                {/* Sliders Grid */}
                <div className="grid gap-6 md:grid-cols-2">
                    {Object.entries(results.feature_stats)
                        .filter(([feat]) => results.coefficients?.[feat] !== 0) // Only active features
                        .slice(0, 8) // Limit to top 8
                        .map(([feat, stats]) => (
                            <div key={feat} className="space-y-3">
                                <div className="flex items-center justify-between">
                                    <label className="text-sm font-medium">{feat}</label>
                                    <Badge variant="secondary">
                                        {values[feat]?.toLocaleString(undefined, { maximumFractionDigits: 1 }) ?? stats.mean.toFixed(1)}
                                    </Badge>
                                </div>
                                <Slider
                                    value={[values[feat] || stats.mean]}
                                    min={stats.min}
                                    max={stats.max}
                                    step={(stats.max - stats.min) / 100}
                                    onValueChange={(vals: number[]) => handleSliderChange(feat, vals[0])}
                                    className="[&>.relative>.absolute]:bg-indigo-500"
                                />
                                <div className="flex justify-between text-xs text-muted-foreground">
                                    <span>{stats.min.toLocaleString(undefined, { maximumFractionDigits: 0 })}</span>
                                    <span>Avg: {stats.mean.toLocaleString(undefined, { maximumFractionDigits: 0 })}</span>
                                    <span>{stats.max.toLocaleString(undefined, { maximumFractionDigits: 0 })}</span>
                                </div>
                            </div>
                        ))}
                </div>
            </CardContent>
        </Card>
    );
}

"use client";

import dynamic from 'next/dynamic';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { RegressionV3Response } from "@/types/regression";

// Dynamic imports for charts
const ResponsiveContainer = dynamic(() => import('recharts').then(mod => mod.ResponsiveContainer), { ssr: false });
const BarChart = dynamic(() => import('recharts').then(mod => mod.BarChart), { ssr: false });
const Bar = dynamic(() => import('recharts').then(mod => mod.Bar), { ssr: false });
const ScatterChart = dynamic(() => import('recharts').then(mod => mod.ScatterChart), { ssr: false });
const Scatter = dynamic(() => import('recharts').then(mod => mod.Scatter), { ssr: false });
const XAxis = dynamic(() => import('recharts').then(mod => mod.XAxis), { ssr: false });
const YAxis = dynamic(() => import('recharts').then(mod => mod.YAxis), { ssr: false });
const CartesianGrid = dynamic(() => import('recharts').then(mod => mod.CartesianGrid), { ssr: false });
const Tooltip = dynamic(() => import('recharts').then(mod => mod.Tooltip), { ssr: false });
const ReferenceLine = dynamic(() => import('recharts').then(mod => mod.ReferenceLine), { ssr: false });
const Cell = dynamic(() => import('recharts').then(mod => mod.Cell), { ssr: false });

interface Props {
    results: RegressionV3Response;
}

export function RegressionDiagnostics({ results }: Props) {
    const getStatusColor = (status: string) => {
        if (status === "Good" || status === "Low") return "text-emerald-500 bg-emerald-500/10";
        if (status === "Moderate") return "text-yellow-500 bg-yellow-500/10";
        return "text-red-500 bg-red-500/10";
    };

    const getCorrelationColor = (corr: number) => {
        const abs = Math.abs(corr);
        if (abs > 0.8) return "#ef4444";
        if (abs > 0.6) return "#f97316";
        if (abs > 0.4) return "#eab308";
        if (abs > 0.2) return "#84cc16";
        return "#22c55e";
    };

    // Prepare residual histogram data
    const prepareResidualHistogram = () => {
        if (!results?.predictions.sample) return [];

        const residuals = results.predictions.sample.map(p => p.residual);
        if (residuals.length === 0) return [];

        const min = Math.min(...residuals);
        const max = Math.max(...residuals);
        const binCount = 15;
        const binSize = (max - min) / binCount;

        const bins = Array(binCount).fill(0).map((_, i) => ({
            bin: (min + i * binSize).toFixed(1),
            count: 0,
            range: `${(min + i * binSize).toFixed(1)} to ${(min + (i + 1) * binSize).toFixed(1)}`
        }));

        residuals.forEach(r => {
            const binIndex = Math.min(Math.floor((r - min) / binSize), binCount - 1);
            if (bins[binIndex]) {
                bins[binIndex].count++;
            }
        });

        return bins;
    };

    return (
        <div className="space-y-6">
            <div className="grid gap-6 md:grid-cols-2">
                {/* Residual Distribution */}
                <Card>
                    <CardHeader>
                        <CardTitle>Residual Distribution</CardTitle>
                        <CardDescription>
                            {results.diagnostics.residuals.normality_test.is_normal
                                ? "✅ Normally distributed"
                                : "⚠️ Not normally distributed"}
                        </CardDescription>
                    </CardHeader>
                    <CardContent>
                        <ResponsiveContainer width="100%" height={250}>
                            <BarChart data={prepareResidualHistogram()}>
                                <CartesianGrid strokeDasharray="3 3" opacity={0.1} />
                                <XAxis
                                    dataKey="bin"
                                    label={{ value: 'Residual', position: 'insideBottom', offset: -5 }}
                                    tick={{ fontSize: 11 }}
                                />
                                <YAxis label={{ value: 'Frequency', angle: -90, position: 'insideLeft' }} />
                                <Tooltip
                                    content={({ active, payload }) => {
                                        if (active && payload && payload.length) {
                                            return (
                                                <div className="bg-background border rounded-lg p-2 shadow-lg">
                                                    <p className="text-sm font-medium">Range: {payload[0].payload.range}</p>
                                                    <p className="text-sm">Count: {payload[0].value}</p>
                                                </div>
                                            );
                                        }
                                        return null;
                                    }}
                                />
                                <Bar dataKey="count" fill="#8b5cf6" />
                            </BarChart>
                        </ResponsiveContainer>
                        <div className="mt-4 grid grid-cols-3 gap-2 text-xs">
                            <div>
                                <div className="text-muted-foreground">Mean</div>
                                <div className="font-medium">{results.diagnostics.residuals.distribution.mean.toFixed(2)}</div>
                            </div>
                            <div>
                                <div className="text-muted-foreground">Std Dev</div>
                                <div className="font-medium">{results.diagnostics.residuals.distribution.std.toFixed(2)}</div>
                            </div>
                            <div>
                                <div className="text-muted-foreground">Skewness</div>
                                <div className="font-medium">{results.diagnostics.residuals.distribution.skewness.toFixed(2)}</div>
                            </div>
                        </div>
                    </CardContent>
                </Card>

                {/* Prediction Intervals */}
                <Card>
                    <CardHeader>
                        <CardTitle>Prediction Intervals</CardTitle>
                        <CardDescription>95% confidence bounds (first 20 predictions)</CardDescription>
                    </CardHeader>
                    <CardContent>
                        <ResponsiveContainer width="100%" height={250}>
                            <ScatterChart margin={{ top: 20, right: 20, bottom: 20, left: 20 }}>
                                <CartesianGrid strokeDasharray="3 3" opacity={0.1} />
                                <XAxis
                                    type="number"
                                    dataKey="actual"
                                    name="Actual"
                                    label={{ value: 'Actual', position: 'insideBottom', offset: -5 }}
                                    domain={['dataMin - 10', 'dataMax + 10']}
                                />
                                <YAxis
                                    type="number"
                                    dataKey="predicted"
                                    name="Predicted"
                                    label={{ value: 'Predicted', angle: -90, position: 'insideLeft' }}
                                    domain={['dataMin - 10', 'dataMax + 10']}
                                />
                                <Tooltip
                                    cursor={{ strokeDasharray: '3 3' }}
                                    content={({ active, payload }) => {
                                        if (active && payload && payload.length) {
                                            const data = payload[0].payload;
                                            return (
                                                <div className="bg-background border rounded-lg p-2 shadow-lg text-xs">
                                                    <p>Actual: {data.actual?.toFixed(1)}</p>
                                                    <p>Predicted: {data.predicted?.toFixed(1)}</p>
                                                    <p>95% CI: [{data.lower_bound?.toFixed(1)}, {data.upper_bound?.toFixed(1)}]</p>
                                                </div>
                                            );
                                        }
                                        return null;
                                    }}
                                />
                                <ReferenceLine
                                    stroke="#666"
                                    strokeDasharray="3 3"
                                    segment={[
                                        { x: 0, y: 0 },
                                        { x: 400, y: 400 }
                                    ]}
                                />
                                {/* Prediction points */}
                                <Scatter
                                    name="Predictions"
                                    data={results.predictions.sample}
                                    fill="#8b5cf6"
                                >
                                    {results.predictions.sample.map((entry, index) => (
                                        <Cell key={`cell-${index}`} fill="#8b5cf6" />
                                    ))}
                                </Scatter>
                            </ScatterChart>
                        </ResponsiveContainer>

                        <div className="mt-4 text-xs text-muted-foreground">
                            Points near diagonal line = accurate predictions. Vertical spread shows uncertainty.
                        </div>
                    </CardContent>
                </Card>
            </div>

            {/* Correlation Heatmap */}
            {results.diagnostics.correlation.high_correlations.length > 0 && (
                <Card>
                    <CardHeader>
                        <CardTitle>Feature Correlations</CardTitle>
                        <CardDescription>
                            {results.diagnostics.correlation.summary.message}
                        </CardDescription>
                    </CardHeader>
                    <CardContent>
                        <div className="space-y-2">
                            {results.diagnostics.correlation.high_correlations.map((corr, idx) => (
                                <div key={idx} className="flex items-center gap-4 p-3 bg-muted/50 rounded-lg">
                                    <div className="flex-1">
                                        <div className="font-medium text-sm">
                                            {corr.feature_1} ↔ {corr.feature_2}
                                        </div>
                                        <div className="text-xs text-muted-foreground">
                                            {corr.interpretation}
                                        </div>
                                    </div>
                                    <div className="flex items-center gap-2">
                                        <div
                                            className="w-16 h-8 rounded flex items-center justify-center text-white font-bold text-sm"
                                            style={{ backgroundColor: getCorrelationColor(corr.correlation) }}
                                        >
                                            {corr.correlation.toFixed(2)}
                                        </div>
                                    </div>
                                </div>
                            ))}
                        </div>
                    </CardContent>
                </Card>
            )}

            {/* Feature Diagnostics */}
            <Card>
                <CardHeader>
                    <CardTitle>Feature Diagnostics</CardTitle>
                    <CardDescription>VIF analysis for multicollinearity detection</CardDescription>
                </CardHeader>
                <CardContent>
                    <Table>
                        <TableHeader>
                            <TableRow>
                                <TableHead>Feature</TableHead>
                                <TableHead>VIF</TableHead>
                                <TableHead>Status</TableHead>
                                <TableHead>Recommendation</TableHead>
                            </TableRow>
                        </TableHeader>
                        <TableBody>
                            {results.diagnostics.multicollinearity.features.map((feat) => (
                                <TableRow key={feat.feature}>
                                    <TableCell className="font-medium">{feat.feature}</TableCell>
                                    <TableCell>{feat.vif.toFixed(2)}</TableCell>
                                    <TableCell>
                                        <Badge className={getStatusColor(feat.status)}>
                                            {feat.status}
                                        </Badge>
                                    </TableCell>
                                    <TableCell className="text-sm text-muted-foreground">
                                        {feat.recommendation}
                                    </TableCell>
                                </TableRow>
                            ))}
                        </TableBody>
                    </Table>
                </CardContent>
            </Card>
        </div>
    );
}

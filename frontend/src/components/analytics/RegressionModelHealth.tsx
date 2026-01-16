import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Progress } from "@/components/ui/progress";
import { Activity } from "lucide-react";
import { RegressionV3Response } from "@/types/regression";

interface Props {
    results: RegressionV3Response;
}

export function RegressionModelHealth({ results }: Props) {
    const getStatusColor = (status: string) => {
        if (status === "Good" || status === "Low") return "text-emerald-500 bg-emerald-500/10";
        if (status === "Moderate") return "text-yellow-500 bg-yellow-500/10";
        return "text-red-500 bg-red-500/10";
    };

    return (
        <Card className="border-blue-500/20 bg-blue-500/5">
            <CardHeader>
                <div className="flex items-center gap-3">
                    <div className="bg-blue-500 rounded-lg p-2">
                        <Activity className="h-5 w-5 text-white" />
                    </div>
                    <div>
                        <CardTitle>Model Health</CardTitle>
                        <CardDescription>
                            {results.model.type} - {results.model.confidence} Confidence
                        </CardDescription>
                    </div>
                </div>
            </CardHeader>
            <CardContent>
                <div className="grid gap-4 md:grid-cols-4">
                    <div className="space-y-2">
                        <div className="text-sm text-muted-foreground">Accuracy (R²)</div>
                        <div className="text-2xl font-bold">
                            {(results.performance.r2_test * 100).toFixed(1)}%
                        </div>
                        <Progress value={results.performance.r2_test * 100} className="h-2" />
                        <div className="text-xs text-muted-foreground">
                            Explains variance in test data
                        </div>
                    </div>

                    <div className="space-y-2">
                        <div className="text-sm text-muted-foreground">Avg Error (MAE)</div>
                        <div className="text-2xl font-bold">±{results.performance.mae.toFixed(1)}</div>
                        <div className="text-xs text-muted-foreground">
                            {results.performance.mape.toFixed(1)}% MAPE
                        </div>
                    </div>

                    <div className="space-y-2">
                        <div className="text-sm text-muted-foreground">Overfitting Risk</div>
                        <Badge className={getStatusColor(
                            results.performance.train_test_gap < 0.05 ? "Low" :
                                results.performance.train_test_gap < 0.10 ? "Moderate" : "High"
                        )}>
                            {results.performance.train_test_gap < 0.05 ? "Low" :
                                results.performance.train_test_gap < 0.10 ? "Moderate" : "High"}
                        </Badge>
                        <div className="text-xs text-muted-foreground">
                            {(results.performance.train_test_gap * 100).toFixed(1)}% gap
                        </div>
                    </div>

                    <div className="space-y-2">
                        <div className="text-sm text-muted-foreground">Multicollinearity</div>
                        <Badge className={getStatusColor(results.diagnostics.multicollinearity.summary.status)}>
                            {results.diagnostics.multicollinearity.summary.status}
                        </Badge>
                        <div className="text-xs text-muted-foreground">
                            Max VIF: {results.diagnostics.multicollinearity.summary.max_vif.toFixed(1)}
                        </div>
                    </div>
                </div>

                <div className="mt-4 p-3 bg-muted/50 rounded-lg">
                    <p className="text-sm">{results.performance.interpretation}</p>
                </div>
            </CardContent>
        </Card>
    );
}

"use client";

import { useState, useEffect } from "react";
import { useRouter } from "next/navigation";
import { Upload, FileUp, Database, Loader2, CheckCircle2 } from "lucide-react";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { api } from "@/lib/api";
import { useAuth } from "@/context/AuthContext";

interface UploadMetrics {
    total_spend: number;
    total_clicks: number;
    total_impressions: number;
    total_conversions: number;
    avg_ctr: number;
}

interface SchemaInfo {
    column: string;
    dtype: string;
    null_count: number;
}

interface UploadResult {
    success: boolean;
    imported_count: number;
    message: string;
    summary?: UploadMetrics;
    schema?: SchemaInfo[];
    preview?: any[];
}

export default function UploadPage() {
    const { token, isLoading } = useAuth();
    const router = useRouter();

    // Auth Guard
    useEffect(() => {
        if (!isLoading && !token) {
            router.push("/login");
        }
    }, [isLoading, token, router]);

    const [file, setFile] = useState<File | null>(null);
    const [uploading, setUploading] = useState(false);
    const [status, setStatus] = useState<{ type: 'success' | 'error', message: string } | null>(null);
    const [result, setResult] = useState<UploadResult | null>(null);

    const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
        if (e.target.files && e.target.files[0]) {
            setFile(e.target.files[0]);
            setStatus(null);
            setResult(null); // Reset result on new file selection
        }
    };

    const handleUpload = async () => {
        if (!file) return;

        setUploading(true);
        setStatus(null);
        setResult(null);

        try {
            const uploadResult = await api.uploadCampaigns(file);

            if (uploadResult.success) {
                setStatus({
                    type: 'success',
                    message: `Successfully imported ${uploadResult.imported_count} campaigns.`
                });
                setResult(uploadResult);
                // setFile(null); // Keep file selected to show 'Done', or clear it? Better clear for next upload but we are showing summary now.
            } else {
                throw new Error(uploadResult.message || "Upload failed");
            }

        } catch (error: any) {
            setStatus({
                type: 'error',
                message: error.message || "Failed to upload file."
            });
        } finally {
            setUploading(false);
        }
    };

    if (isLoading) {
        return <div className="flex h-screen items-center justify-center">Loading...</div>;
    }

    if (!token) {
        return null;
    }

    return (
        <div className="container mx-auto py-10 space-y-8">
            <h1 className="text-3xl font-bold tracking-tight">Data Upload</h1>

            {/* Success Summary View */}
            {result && result.summary ? (
                <div className="space-y-6">
                    <Alert className="border-green-500 text-green-700 bg-green-50 dark:bg-green-900/10">
                        <CheckCircle2 className="h-4 w-4" />
                        <AlertTitle>Upload Complete</AlertTitle>
                        <AlertDescription>
                            {result.message}
                        </AlertDescription>
                    </Alert>

                    {/* Metrics Cards */}
                    <div className="grid gap-4 md:grid-cols-4">
                        <Card>
                            <CardHeader className="pb-2">
                                <CardTitle className="text-sm font-medium text-muted-foreground">Total Spend</CardTitle>
                            </CardHeader>
                            <CardContent>
                                <div className="text-2xl font-bold">${result.summary.total_spend.toLocaleString(undefined, { minimumFractionDigits: 2 })}</div>
                            </CardContent>
                        </Card>
                        <Card>
                            <CardHeader className="pb-2">
                                <CardTitle className="text-sm font-medium text-muted-foreground">Total Clicks</CardTitle>
                            </CardHeader>
                            <CardContent>
                                <div className="text-2xl font-bold">{result.summary.total_clicks.toLocaleString()}</div>
                            </CardContent>
                        </Card>
                        <Card>
                            <CardHeader className="pb-2">
                                <CardTitle className="text-sm font-medium text-muted-foreground">Conversions</CardTitle>
                            </CardHeader>
                            <CardContent>
                                <div className="text-2xl font-bold">{result.summary.total_conversions.toLocaleString()}</div>
                            </CardContent>
                        </Card>
                        <Card>
                            <CardHeader className="pb-2">
                                <CardTitle className="text-sm font-medium text-muted-foreground">Avg CTR</CardTitle>
                            </CardHeader>
                            <CardContent>
                                <div className="text-2xl font-bold">{result.summary.avg_ctr.toFixed(2)}%</div>
                            </CardContent>
                        </Card>
                    </div>

                    <div className="grid gap-6 md:grid-cols-2">
                        {/* Schema Info */}
                        <Card>
                            <CardHeader>
                                <CardTitle>Data Schema</CardTitle>
                                <CardDescription>Detected columns and data types.</CardDescription>
                            </CardHeader>
                            <CardContent className="h-[300px] overflow-auto">
                                <Table>
                                    <TableHeader>
                                        <TableRow>
                                            <TableHead>Column</TableHead>
                                            <TableHead>Type</TableHead>
                                            <TableHead>Nulls</TableHead>
                                        </TableRow>
                                    </TableHeader>
                                    <TableBody>
                                        {result.schema?.map((col) => (
                                            <TableRow key={col.column}>
                                                <TableCell className="font-medium">{col.column}</TableCell>
                                                <TableCell>{col.dtype}</TableCell>
                                                <TableCell className={col.null_count > 0 ? "text-yellow-600 font-bold" : "text-muted-foreground"}>
                                                    {col.null_count}
                                                </TableCell>
                                            </TableRow>
                                        ))}
                                    </TableBody>
                                </Table>
                            </CardContent>
                        </Card>

                        {/* Data Preview */}
                        <Card>
                            <CardHeader>
                                <CardTitle>Data Preview</CardTitle>
                                <CardDescription>First 5 rows of imported data.</CardDescription>
                            </CardHeader>
                            <CardContent className="h-[300px] overflow-auto">
                                <Table>
                                    <TableHeader>
                                        <TableRow>
                                            {result.schema?.map(col => (
                                                <TableHead key={col.column} className="whitespace-nowrap">{col.column}</TableHead>
                                            ))}
                                        </TableRow>
                                    </TableHeader>
                                    <TableBody>
                                        {result.preview?.map((row, i) => (
                                            <TableRow key={i}>
                                                {result.schema?.map(col => (
                                                    <TableCell key={col.column} className="whitespace-nowrap">
                                                        {row[col.column] !== null ? String(row[col.column]) : <span className="text-muted-foreground italic">null</span>}
                                                    </TableCell>
                                                ))}
                                            </TableRow>
                                        ))}
                                    </TableBody>
                                </Table>
                            </CardContent>
                        </Card>
                    </div>

                    <div className="flex justify-end">
                        <Button onClick={() => { setFile(null); setResult(null); setStatus(null); }}>
                            Upload Another File
                        </Button>
                    </div>
                </div>
            ) : (
                <div className="grid gap-6 md:grid-cols-2">
                    {/* File Upload Section */}
                    <Card>
                        <CardHeader>
                            <CardTitle className="flex items-center gap-2">
                                <FileUp className="h-5 w-5" />
                                File Upload
                            </CardTitle>
                            <CardDescription>
                                Upload your campaign data as CSV or Excel files.
                                Required columns: Campaign_Name, Platform, Spend, Impressions, Clicks.
                            </CardDescription>
                        </CardHeader>
                        <CardContent className="space-y-4">
                            <div className="grid w-full max-w-sm items-center gap-1.5">
                                <Label htmlFor="campaign-file">Campaign Data File</Label>
                                <Input
                                    id="campaign-file"
                                    type="file"
                                    accept=".csv,.xlsx,.xls"
                                    onChange={handleFileChange}
                                />
                            </div>

                            {status && (
                                <Alert variant={status.type === 'error' ? "destructive" : "default"} className={status.type === 'success' ? "border-green-500 text-green-700 bg-green-50 dark:bg-green-900/10" : ""}>
                                    <AlertTitle>{status.type === 'success' ? "Success" : "Error"}</AlertTitle>
                                    <AlertDescription>
                                        {status.message}
                                    </AlertDescription>
                                </Alert>
                            )}

                            <Button
                                onClick={handleUpload}
                                disabled={!file || uploading}
                                className="w-full"
                            >
                                {uploading ? (
                                    <>
                                        <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                                        Uploading...
                                    </>
                                ) : (
                                    <>
                                        <Upload className="mr-2 h-4 w-4" />
                                        Upload Data
                                    </>
                                )}
                            </Button>
                        </CardContent>
                    </Card>

                    {/* Database Connection Placeholder */}
                    <Card className="opacity-75">
                        <CardHeader>
                            <CardTitle className="flex items-center gap-2">
                                <Database className="h-5 w-5" />
                                Database Connections
                            </CardTitle>
                            <CardDescription>
                                Connect directly to your ad platforms or data warehouse.
                            </CardDescription>
                        </CardHeader>
                        <CardContent className="space-y-4">
                            <div className="rounded-md bg-muted p-4 text-sm text-muted-foreground flex flex-col items-center justify-center h-[180px] border border-dashed text-center">
                                <p className="mb-2 font-medium">Coming Soon</p>
                                <p>Direct integration with Google Ads, Meta, Snowflake, and BigQuery.</p>
                            </div>
                            <Button disabled variant="outline" className="w-full">
                                Configure Connection
                            </Button>
                        </CardContent>
                    </Card>
                </div>
            )}
        </div>
    );
}

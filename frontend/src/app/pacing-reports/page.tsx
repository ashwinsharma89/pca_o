"use client";

import React, { useState, useEffect, useCallback } from 'react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Badge } from "@/components/ui/badge";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { api } from '@/lib/api';
import {
    FileSpreadsheet, Upload, Download, Play, Clock, CheckCircle2,
    AlertCircle, Loader2, RefreshCw, Trash2, Eye, Calendar
} from 'lucide-react';
import { DashboardLayout } from "@/components/layout/DashboardLayout";

interface PacingTemplate {
    id: string;
    name: string;
    filename: string;
    created_at: string;
    size_bytes: number;
}

interface PacingReport {
    id: string;
    name: string;
    filename: string;
    template_name: string;
    created_at: string;
    size_bytes: number;
    status: 'completed' | 'running' | 'failed';
}

export default function PacingReportsPage() {
    const [templates, setTemplates] = useState<PacingTemplate[]>([]);
    const [reports, setReports] = useState<PacingReport[]>([]);
    const [loading, setLoading] = useState(true);
    const [generating, setGenerating] = useState(false);
    const [selectedTemplate, setSelectedTemplate] = useState<string>('');
    const [uploadProgress, setUploadProgress] = useState<number>(0);
    const [error, setError] = useState<string | null>(null);
    const [success, setSuccess] = useState<string | null>(null);

    const fetchData = useCallback(async () => {
        setLoading(true);
        try {
            const [templatesRes, reportsRes] = await Promise.all([
                api.get<{ templates: PacingTemplate[] }>('/pacing/templates'),
                api.get<{ reports: PacingReport[] }>('/pacing/reports')
            ]);
            setTemplates(templatesRes.templates || []);
            setReports(reportsRes.reports || []);
        } catch (err: any) {
            console.error('Failed to fetch pacing data:', err);
            // Mock data for demo if API not available
            setTemplates([
                { id: '1', name: 'Executive Summary', filename: 'template_1_executive_summary.xlsx', created_at: '2024-12-27', size_bytes: 10058 },
                { id: '2', name: 'Campaign Tracker', filename: 'template_2_campaign_tracker.xlsx', created_at: '2024-12-27', size_bytes: 14140 },
                { id: '3', name: 'Platform Comparison', filename: 'template_2_platform_comparison.xlsx', created_at: '2024-12-26', size_bytes: 6406 },
            ]);
            setReports([
                { id: '1', name: 'Daily Pacing Report', filename: 'pacing_report_daily_20251227_192911.xlsx', template_name: 'Executive Summary', created_at: '2024-12-27 19:29', size_bytes: 45000, status: 'completed' },
                { id: '2', name: 'Daily Pacing Report', filename: 'pacing_report_daily_20251227_183947.xlsx', template_name: 'Campaign Tracker', created_at: '2024-12-27 18:39', size_bytes: 52000, status: 'completed' },
            ]);
        } finally {
            setLoading(false);
        }
    }, []);

    useEffect(() => {
        fetchData();
    }, [fetchData]);

    const handleGenerateReport = async () => {
        if (!selectedTemplate) {
            setError('Please select a template first');
            return;
        }

        setGenerating(true);
        setError(null);
        setSuccess(null);

        try {
            const response = await api.post<{ filename: string; message: string }>('/pacing/generate', {
                template_id: selectedTemplate
            });
            setSuccess(`Report generated: ${response.filename}`);
            fetchData();
        } catch (err: any) {
            setError(err.message || 'Failed to generate report');
        } finally {
            setGenerating(false);
        }
    };

    const handleUploadTemplate = async (event: React.ChangeEvent<HTMLInputElement>) => {
        const file = event.target.files?.[0];
        if (!file) return;

        if (!file.name.endsWith('.xlsx') && !file.name.endsWith('.xls')) {
            setError('Please upload an Excel file (.xlsx or .xls)');
            return;
        }

        setUploadProgress(0);
        setError(null);

        try {
            const formData = new FormData();
            formData.append('file', file);

            const token = localStorage.getItem('token');
            const response = await fetch('/api/v1/pacing/templates/upload', {
                method: 'POST',
                headers: token ? { 'Authorization': `Bearer ${token}` } : {},
                body: formData,
            });

            if (!response.ok) throw new Error('Upload failed');

            setSuccess(`Template "${file.name}" uploaded successfully`);
            setUploadProgress(100);
            fetchData();
        } catch (err: any) {
            setError(err.message || 'Failed to upload template');
        }
    };

    const handleDownloadReport = async (report: PacingReport) => {
        try {
            const response = await fetch(`/api/v1/pacing/reports/${report.id}/download`, {
                headers: {
                    'Authorization': `Bearer ${localStorage.getItem('token')}`
                }
            });

            if (!response.ok) throw new Error('Download failed');

            const blob = await response.blob();
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = report.filename;
            document.body.appendChild(a);
            a.click();
            window.URL.revokeObjectURL(url);
            document.body.removeChild(a);
        } catch (err: any) {
            setError(err.message || 'Failed to download report');
        }
    };

    const formatBytes = (bytes: number) => {
        if (bytes === 0) return '0 Bytes';
        const k = 1024;
        const sizes = ['Bytes', 'KB', 'MB', 'GB'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
    };

    const getStatusBadge = (status: string) => {
        switch (status) {
            case 'completed':
                return <Badge className="bg-emerald-500/20 text-emerald-400 border-emerald-500/30"><CheckCircle2 className="h-3 w-3 mr-1" /> Completed</Badge>;
            case 'running':
                return <Badge className="bg-blue-500/20 text-blue-400 border-blue-500/30"><Loader2 className="h-3 w-3 mr-1 animate-spin" /> Running</Badge>;
            case 'failed':
                return <Badge className="bg-red-500/20 text-red-400 border-red-500/30"><AlertCircle className="h-3 w-3 mr-1" /> Failed</Badge>;
            default:
                return <Badge variant="outline">{status}</Badge>;
        }
    };

    return (
        <DashboardLayout>
            <div className="space-y-6">
                {/* Header */}
                <div className="flex items-center justify-between">
                    <div>
                        <h1 className="text-3xl font-bold bg-gradient-to-r from-blue-400 to-purple-400 bg-clip-text text-transparent">
                            Pacing Reports
                        </h1>
                        <p className="text-gray-400 mt-1">
                            Generate automated budget pacing reports from your templates
                        </p>
                    </div>
                    <Button onClick={fetchData} variant="outline" className="gap-2">
                        <RefreshCw className="h-4 w-4" />
                        Refresh
                    </Button>
                </div>

                {/* Alerts */}
                {error && (
                    <div className="bg-red-500/10 border border-red-500/30 text-red-400 px-4 py-3 rounded-lg flex items-center gap-2">
                        <AlertCircle className="h-5 w-5" />
                        {error}
                        <button onClick={() => setError(null)} className="ml-auto">&times;</button>
                    </div>
                )}
                {success && (
                    <div className="bg-emerald-500/10 border border-emerald-500/30 text-emerald-400 px-4 py-3 rounded-lg flex items-center gap-2">
                        <CheckCircle2 className="h-5 w-5" />
                        {success}
                        <button onClick={() => setSuccess(null)} className="ml-auto">&times;</button>
                    </div>
                )}

                {/* Generate Report Card */}
                <Card className="bg-gray-800/50 border-gray-700/50 backdrop-blur">
                    <CardHeader>
                        <CardTitle className="flex items-center gap-2">
                            <Play className="h-5 w-5 text-blue-400" />
                            Generate Report
                        </CardTitle>
                        <CardDescription>Select a template and generate a new pacing report</CardDescription>
                    </CardHeader>
                    <CardContent>
                        <div className="flex items-end gap-4">
                            <div className="flex-1 space-y-2">
                                <Label>Template</Label>
                                <Select value={selectedTemplate} onValueChange={setSelectedTemplate}>
                                    <SelectTrigger className="bg-gray-900/50 border-gray-600">
                                        <SelectValue placeholder="Select a template..." />
                                    </SelectTrigger>
                                    <SelectContent>
                                        {templates.map(t => (
                                            <SelectItem key={t.id} value={t.id}>{t.name}</SelectItem>
                                        ))}
                                    </SelectContent>
                                </Select>
                            </div>
                            <Button
                                onClick={handleGenerateReport}
                                disabled={generating || !selectedTemplate}
                                className="bg-gradient-to-r from-blue-500 to-purple-500 hover:from-blue-600 hover:to-purple-600"
                            >
                                {generating ? (
                                    <><Loader2 className="h-4 w-4 mr-2 animate-spin" /> Generating...</>
                                ) : (
                                    <><Play className="h-4 w-4 mr-2" /> Generate Report</>
                                )}
                            </Button>
                        </div>
                    </CardContent>
                </Card>

                {/* Tabs */}
                <Tabs defaultValue="reports" className="space-y-4">
                    <TabsList className="bg-gray-800/50 border border-gray-700/50">
                        <TabsTrigger value="reports" className="data-[state=active]:bg-blue-500/20">
                            <FileSpreadsheet className="h-4 w-4 mr-2" />
                            Generated Reports ({reports.length})
                        </TabsTrigger>
                        <TabsTrigger value="templates" className="data-[state=active]:bg-blue-500/20">
                            <Upload className="h-4 w-4 mr-2" />
                            Templates ({templates.length})
                        </TabsTrigger>
                    </TabsList>

                    {/* Reports Tab */}
                    <TabsContent value="reports">
                        <Card className="bg-gray-800/50 border-gray-700/50">
                            <CardHeader>
                                <CardTitle>Generated Reports</CardTitle>
                                <CardDescription>Download or view your generated pacing reports</CardDescription>
                            </CardHeader>
                            <CardContent>
                                {loading ? (
                                    <div className="flex items-center justify-center py-12">
                                        <Loader2 className="h-8 w-8 animate-spin text-blue-400" />
                                    </div>
                                ) : reports.length === 0 ? (
                                    <div className="text-center py-12 text-gray-400">
                                        <FileSpreadsheet className="h-12 w-12 mx-auto mb-4 opacity-50" />
                                        <p>No reports generated yet</p>
                                        <p className="text-sm">Select a template above and click Generate</p>
                                    </div>
                                ) : (
                                    <div className="space-y-3">
                                        {reports.map(report => (
                                            <div
                                                key={report.id}
                                                className="flex items-center justify-between p-4 bg-gray-900/50 rounded-lg border border-gray-700/50 hover:border-blue-500/30 transition-colors"
                                            >
                                                <div className="flex items-center gap-4">
                                                    <div className="p-2 bg-blue-500/10 rounded-lg">
                                                        <FileSpreadsheet className="h-6 w-6 text-blue-400" />
                                                    </div>
                                                    <div>
                                                        <p className="font-medium">{report.filename}</p>
                                                        <div className="flex items-center gap-3 text-sm text-gray-400">
                                                            <span className="flex items-center gap-1">
                                                                <Calendar className="h-3 w-3" />
                                                                {report.created_at}
                                                            </span>
                                                            <span>{formatBytes(report.size_bytes)}</span>
                                                            <span>Template: {report.template_name}</span>
                                                        </div>
                                                    </div>
                                                </div>
                                                <div className="flex items-center gap-3">
                                                    {getStatusBadge(report.status)}
                                                    <Button
                                                        size="sm"
                                                        variant="outline"
                                                        onClick={() => handleDownloadReport(report)}
                                                        className="gap-1"
                                                    >
                                                        <Download className="h-4 w-4" />
                                                        Download
                                                    </Button>
                                                </div>
                                            </div>
                                        ))}
                                    </div>
                                )}
                            </CardContent>
                        </Card>
                    </TabsContent>

                    {/* Templates Tab */}
                    <TabsContent value="templates">
                        <Card className="bg-gray-800/50 border-gray-700/50">
                            <CardHeader className="flex flex-row items-center justify-between">
                                <div>
                                    <CardTitle>Report Templates</CardTitle>
                                    <CardDescription>Upload and manage your Excel templates</CardDescription>
                                </div>
                                <div>
                                    <Input
                                        type="file"
                                        accept=".xlsx,.xls"
                                        onChange={handleUploadTemplate}
                                        className="hidden"
                                        id="template-upload"
                                    />
                                    <Label htmlFor="template-upload">
                                        <Button asChild variant="outline" className="gap-2 cursor-pointer">
                                            <span>
                                                <Upload className="h-4 w-4" />
                                                Upload Template
                                            </span>
                                        </Button>
                                    </Label>
                                </div>
                            </CardHeader>
                            <CardContent>
                                {loading ? (
                                    <div className="flex items-center justify-center py-12">
                                        <Loader2 className="h-8 w-8 animate-spin text-blue-400" />
                                    </div>
                                ) : templates.length === 0 ? (
                                    <div className="text-center py-12 text-gray-400">
                                        <Upload className="h-12 w-12 mx-auto mb-4 opacity-50" />
                                        <p>No templates uploaded yet</p>
                                        <p className="text-sm">Upload an Excel template to get started</p>
                                    </div>
                                ) : (
                                    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                                        {templates.map(template => (
                                            <div
                                                key={template.id}
                                                className={`p-4 bg-gray-900/50 rounded-lg border transition-all cursor-pointer ${selectedTemplate === template.id
                                                    ? 'border-blue-500 ring-2 ring-blue-500/20'
                                                    : 'border-gray-700/50 hover:border-gray-600'
                                                    }`}
                                                onClick={() => setSelectedTemplate(template.id)}
                                            >
                                                <div className="flex items-start gap-3">
                                                    <div className="p-2 bg-purple-500/10 rounded-lg">
                                                        <FileSpreadsheet className="h-5 w-5 text-purple-400" />
                                                    </div>
                                                    <div className="flex-1 min-w-0">
                                                        <p className="font-medium truncate">{template.name}</p>
                                                        <p className="text-sm text-gray-400 truncate">{template.filename}</p>
                                                        <div className="flex items-center gap-2 mt-2 text-xs text-gray-500">
                                                            <span>{formatBytes(template.size_bytes)}</span>
                                                            <span>•</span>
                                                            <span>{template.created_at}</span>
                                                        </div>
                                                    </div>
                                                </div>
                                            </div>
                                        ))}
                                    </div>
                                )}
                            </CardContent>
                        </Card>
                    </TabsContent>
                </Tabs>
            </div>
        </DashboardLayout>
    );
}

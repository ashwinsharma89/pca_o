'use client';

import * as React from 'react';
import { useParams, useRouter } from 'next/navigation';
import { api } from '@/lib/api';
import { Campaign } from '@/types';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';
import { ArrowLeft, Loader2, RefreshCw } from 'lucide-react';

import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { ChatInterface } from '@/components/agent/ChatInterface';

import { InsightCard } from '@/components/agent/InsightCard';
import { CampaignVisualizations } from '@/components/agent/CampaignVisualizations';

export default function CampaignDetailsPage() {
    const params = useParams();
    const router = useRouter();
    const id = params.id as string;

    const [campaign, setCampaign] = React.useState<Campaign | null>(null);
    const [insights, setInsights] = React.useState<any>(null);
    const [isLoading, setIsLoading] = React.useState(true);
    const [isLoadingInsights, setIsLoadingInsights] = React.useState(false);
    const [isRegenerating, setIsRegenerating] = React.useState(false);
    const [error, setError] = React.useState<string | null>(null);

    const fetchCampaign = async () => {
        try {
            const token = localStorage.getItem('token');
            if (!token) return;
            const response = await api.get<Campaign>(`/campaigns/${id}`, { token });
            setCampaign(response);

            // Fetch insights in parallel or after
            fetchInsights();
        } catch (err: any) {
            setError(err.message || 'Failed to fetch campaign details');
        } finally {
            setIsLoading(false);
        }
    };

    const fetchInsights = async () => {
        setIsLoadingInsights(true);
        try {
            const response = await api.getCampaignInsights(id);
            setInsights(response);
        } catch (err) {
            console.error("Failed to fetch insights", err);
        } finally {
            setIsLoadingInsights(false);
        }
    };

    React.useEffect(() => {
        fetchCampaign();
    }, [id]);

    const handleRegenerate = async () => {
        setIsRegenerating(true);
        try {
            const token = localStorage.getItem('token');
            await api.post(`/campaigns/${id}/report/regenerate`, {}, {
                token: token || '',
            });
            alert('Report regeneration queued!');
            fetchCampaign();
        } catch (err: any) {
            alert(err.message || 'Failed to regenerate report');
        } finally {
            setIsRegenerating(false);
        }
    };

    if (isLoading) {
        return (
            <div className="flex h-[50vh] items-center justify-center">
                <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
            </div>
        );
    }

    if (error || !campaign) {
        return (
            <div className="flex max-w-md flex-col items-center justify-center space-y-4">
                <p className="text-red-500">{error || 'Campaign not found'}</p>
                <Button variant="outline" onClick={() => router.back()}>
                    <ArrowLeft className="mr-2 h-4 w-4" />
                    Go Back
                </Button>
            </div>
        );
    }

    return (
        <div className="space-y-8">
            <div className="flex items-center justify-between">
                <div className="flex items-center gap-4">
                    <Button variant="ghost" size="icon" onClick={() => router.back()}>
                        <ArrowLeft className="h-5 w-5" />
                    </Button>
                    <div>
                        <div className="flex items-center gap-3">
                            <h1 className="text-3xl font-bold tracking-tight">{campaign.name}</h1>
                            <span className={`inline-flex items-center rounded-full px-2.5 py-0.5 text-xs font-semibold transition-colors focus:outline-none focus:ring-2 focus:ring-ring focus:ring-offset-2 ${campaign.status === 'completed' ? 'bg-green-500/10 text-green-500' :
                                campaign.status === 'running' ? 'bg-blue-500/10 text-blue-500' :
                                    'bg-zinc-500/10 text-zinc-500'
                                }`}>
                                {campaign.status}
                            </span>
                        </div>
                        <p className="text-muted-foreground">{campaign.created_at ? `Created on ${new Date(campaign.created_at).toLocaleDateString()}` : ''}</p>
                    </div>
                </div>
                <Button onClick={handleRegenerate} disabled={isRegenerating || campaign.status !== 'completed'}>
                    <RefreshCw className={`mr-2 h-4 w-4 ${isRegenerating ? 'animate-spin' : ''}`} />
                    Regenerate Report
                </Button>
            </div>

            <Tabs defaultValue="overview" className="space-y-4">
                <TabsList>
                    <TabsTrigger value="overview">Overview</TabsTrigger>
                    <TabsTrigger value="charts">Visualizations</TabsTrigger>
                    <TabsTrigger value="insights">AI Insights</TabsTrigger>
                    <TabsTrigger value="chat">Chat with Data</TabsTrigger>
                    <TabsTrigger value="report">Report</TabsTrigger>
                </TabsList>

                <TabsContent value="overview" className="space-y-4">
                    <div className="grid gap-6 md:grid-cols-2">
                        <Card>
                            <CardHeader>
                                <CardTitle>Campaign Details</CardTitle>
                            </CardHeader>
                            <CardContent className="space-y-4">
                                <div className="grid grid-cols-2 gap-4">
                                    <div>
                                        <p className="text-sm font-medium text-muted-foreground">Objective</p>
                                        <p>{campaign.objective}</p>
                                    </div>
                                    <div>
                                        <p className="text-sm font-medium text-muted-foreground">Date Range</p>
                                        <p>{campaign.start_date && campaign.end_date ? `${new Date(campaign.start_date).toLocaleDateString()} - ${new Date(campaign.end_date).toLocaleDateString()}` : 'No dates set'}</p>
                                    </div>
                                </div>
                            </CardContent>
                        </Card>
                        <Card>
                            <CardHeader>
                                <CardTitle>Analysis Status</CardTitle>
                                <CardDescription>Current state of the report generation.</CardDescription>
                            </CardHeader>
                            <CardContent>
                                <div className="flex items-center gap-2">
                                    <div className={`h-2 w-2 rounded-full ${campaign.status === 'completed' ? 'bg-green-500' : 'bg-yellow-500'}`} />
                                    <p className="text-sm font-medium capitalize">{campaign.status}</p>
                                </div>
                            </CardContent>
                        </Card>
                    </div>
                </TabsContent>

                <TabsContent value="charts" className="space-y-4">
                    <CampaignVisualizations campaignId={id} />
                </TabsContent>

                <TabsContent value="insights" className="space-y-4">
                    <Card>
                        <CardHeader>
                            <CardTitle>AI Generated Insights</CardTitle>
                            <CardDescription>Automated analysis of your campaign performance.</CardDescription>
                        </CardHeader>
                        <CardContent>
                            <InsightCard insights={insights} isLoading={isLoadingInsights} />
                        </CardContent>
                    </Card>
                </TabsContent>

                <TabsContent value="chat" className="space-y-4">
                    <ChatInterface campaignId={id} />
                </TabsContent>

                <TabsContent value="report" className="space-y-4">
                    <Card>
                        <CardHeader>
                            <CardTitle>Generated Report</CardTitle>
                            <CardDescription>Download or view the final report.</CardDescription>
                        </CardHeader>
                        <CardContent>
                            {campaign.status === 'completed' ? (
                                <div className="rounded-lg border border-dashed border-slate-200 p-8 text-center text-muted-foreground dark:border-slate-800">
                                    <p>Report visualization would go here.</p>
                                </div>
                            ) : (
                                <div className="rounded-lg border border-dashed border-slate-200 p-8 text-center text-muted-foreground dark:border-slate-800">
                                    <p>Analysis in progress. Check back later.</p>
                                </div>
                            )}
                        </CardContent>
                    </Card>
                </TabsContent>
            </Tabs>
        </div>
    );
}

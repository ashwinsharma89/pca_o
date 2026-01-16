'use client';

import * as React from 'react';
import Link from 'next/link';
import { api } from '@/lib/api';
import { CampaignListResponse, Campaign } from '@/types';
import { Button } from '@/components/ui/button';
import { Card, CardHeader, CardTitle, CardDescription, CardContent } from '@/components/ui/card';
import { Plus, Trash2, FileText, Loader2 } from 'lucide-react';
import { Modal } from '@/components/ui/modal';
import { CreateCampaignForm } from '@/components/campaigns/CreateCampaignForm';

export default function CampaignsPage() {
    const [campaigns, setCampaigns] = React.useState<Campaign[]>([]);
    const [isLoading, setIsLoading] = React.useState(true);
    const [error, setError] = React.useState<string | null>(null);
    const [page, setPage] = React.useState(1);
    const [total, setTotal] = React.useState(0);
    const limit = 10;

    const fetchCampaigns = async () => {
        try {
            setIsLoading(true);
            const token = localStorage.getItem('token');
            if (!token) return;
            const response = await api.get<CampaignListResponse>(`/campaigns?page=${page}&limit=${limit}`, { token });
            setCampaigns(response.items);
            setTotal(response.total);
        } catch (err: any) {
            setError(err.message || 'Failed to fetch campaigns');
        } finally {
            setIsLoading(false);
        }
    };

    React.useEffect(() => {
        fetchCampaigns();
    }, [page]);

    const handleDelete = async (id: string) => {
        if (!confirm('Are you sure you want to delete this campaign?')) return;
        try {
            const token = localStorage.getItem('token');
            await api.delete(`/campaigns/${id}`, { token: token || '' });
            setCampaigns(campaigns.filter(c => c.campaign_id !== id));
        } catch (err: any) {
            alert(err.message || 'Failed to delete campaign');
        }
    };

    const [isCreateModalOpen, setIsCreateModalOpen] = React.useState(false);

    return (
        <div className="space-y-8">
            <div className="flex items-center justify-between">
                <div>
                    <h1 className="text-3xl font-bold tracking-tight">Campaigns</h1>
                    <p className="text-muted-foreground">Manage and analyze your marketing campaigns.</p>
                </div>
                <Button onClick={() => setIsCreateModalOpen(true)}>
                    <Plus className="mr-2 h-4 w-4" />
                    New Campaign
                </Button>
            </div>

            {isLoading ? (
                <div className="flex justify-center p-8">
                    <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
                </div>
            ) : error ? (
                <div className="rounded-lg border border-red-500/50 p-4 text-red-500">
                    {error}
                </div>
            ) : campaigns.length === 0 ? (
                <Card className="flex flex-col items-center justify-center py-12 text-center">
                    <div className="rounded-full bg-primary/10 p-4 mb-4">
                        <FileText className="h-8 w-8 text-primary" />
                    </div>
                    <h3 className="text-lg font-semibold">No campaigns yet</h3>
                    <p className="text-muted-foreground mb-4 max-w-sm">
                        Get started by creating your first campaign to track performance and generate insights.
                    </p>
                    <Button onClick={() => setIsCreateModalOpen(true)}>
                        <Plus className="mr-2 h-4 w-4" />
                        Create Campaign
                    </Button>
                </Card>
            ) : (
                <>
                    <div className="grid gap-4">
                        {campaigns.map((campaign) => (
                            <Card key={campaign.campaign_id} className="flex flex-row items-center justify-between p-6">
                                <div className="space-y-1">
                                    <div className="flex items-center gap-3">
                                        <CardTitle className="text-base">{campaign.name}</CardTitle>
                                        <span className={`inline-flex items-center rounded-full px-2.5 py-0.5 text-xs font-semibold transition-colors focus:outline-none focus:ring-2 focus:ring-ring focus:ring-offset-2 ${campaign.status === 'completed' ? 'bg-green-500/10 text-green-500' :
                                            campaign.status === 'running' ? 'bg-blue-500/10 text-blue-500' :
                                                'bg-zinc-500/10 text-zinc-500'
                                            }`}>
                                            {campaign.status}
                                        </span>
                                    </div>
                                    <CardDescription>
                                        {campaign.objective || campaign.name}
                                    </CardDescription>
                                </div>
                                <div className="flex items-center gap-2">
                                    <Button variant="outline" size="sm" asChild>
                                        <Link href={`/campaigns/${campaign.campaign_id}`}>View Report</Link>
                                    </Button>
                                    <Button variant="ghost" size="sm" className="text-red-500 hover:text-red-600 hover:bg-red-500/10" onClick={() => handleDelete(campaign.campaign_id)}>
                                        <Trash2 className="h-4 w-4" />
                                    </Button>
                                </div>
                            </Card>
                        ))}
                    </div>

                    {/* Pagination Controls */}
                    <div className="flex items-center justify-between py-4">
                        <div className="text-sm text-muted-foreground">
                            Showing {((page - 1) * limit) + 1} to {Math.min(page * limit, total)} of {total} campaigns
                        </div>
                        <div className="flex items-center gap-2">
                            <Button
                                variant="outline"
                                size="sm"
                                onClick={() => setPage(p => Math.max(1, p - 1))}
                                disabled={page === 1 || isLoading}
                            >
                                Previous
                            </Button>
                            <div className="text-sm font-medium">Page {page} of {Math.max(1, Math.ceil(total / limit))}</div>
                            <Button
                                variant="outline"
                                size="sm"
                                onClick={() => setPage(p => p + 1)}
                                disabled={page >= Math.ceil(total / limit) || isLoading}
                            >
                                Next
                            </Button>
                        </div>
                    </div>
                </>
            )
            }

            <Modal
                isOpen={isCreateModalOpen}
                onClose={() => setIsCreateModalOpen(false)}
                title="Create New Campaign"
            >
                <CreateCampaignForm
                    onSuccess={() => {
                        setIsCreateModalOpen(false);
                        fetchCampaigns();
                    }}
                    onCancel={() => setIsCreateModalOpen(false)}
                />
            </Modal>
        </div >
    );
}

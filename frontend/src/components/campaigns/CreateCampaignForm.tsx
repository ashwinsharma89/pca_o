'use client';

import * as React from 'react';
import { api } from '@/lib/api';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';

interface CreateCampaignFormProps {
    onSuccess: () => void;
    onCancel: () => void;
}

export function CreateCampaignForm({ onSuccess, onCancel }: CreateCampaignFormProps) {
    const [isLoading, setIsLoading] = React.useState(false);
    const [error, setError] = React.useState<string | null>(null);

    const handleSubmit = async (e: React.FormEvent<HTMLFormElement>) => {
        e.preventDefault();
        setIsLoading(true);
        setError(null);

        const formData = new FormData(e.currentTarget);
        const campaign_name = formData.get('name') as string;
        const objective = formData.get('objective') as string;
        const start_date = formData.get('startDate') as string;
        const end_date = formData.get('endDate') as string;

        try {
            const token = localStorage.getItem('token');
            // Construct query params as the backend endpoint expects query params for now based on the signature
            // "async def create_campaign(request: Request, campaign_name: str, objective: str...)"
            // Wait, FastAPI usually takes JSON body if not specified as Form or Query. 
            // Let's check `src/api/v1/campaigns.py` again.
            // It uses simple arguments without Pydantic model in the signature: `campaign_name: str, objective: str...`. 
            // In FastAPI, these are interpreted as **Query Parameters** by default unless `Body()` is used.
            // This is a common "gotcha". 
            // Confirmed: `campaign_name: str` in function signature = Query Param.
            // I should assume they are Query Params for now.

            const params = new URLSearchParams({
                campaign_name,
                objective,
                start_date,
                end_date
            });

            await api.post(`/campaigns?${params.toString()}`, {}, { token: token || '' });

            onSuccess();
        } catch (err: any) {
            setError(err.message || 'Failed to create campaign');
        } finally {
            setIsLoading(false);
        }
    };

    return (
        <form onSubmit={handleSubmit} className="space-y-4">
            <div className="grid gap-2">
                <Label htmlFor="name">Campaign Name</Label>
                <Input id="name" name="name" required placeholder="Summer Sale 2025" />
            </div>
            <div className="grid gap-2">
                <Label htmlFor="objective">Objective</Label>
                <Input id="objective" name="objective" required placeholder="Increase brand awareness" />
            </div>
            <div className="grid grid-cols-2 gap-4">
                <div className="grid gap-2">
                    <Label htmlFor="startDate">Start Date</Label>
                    <Input id="startDate" name="startDate" type="date" required />
                </div>
                <div className="grid gap-2">
                    <Label htmlFor="endDate">End Date</Label>
                    <Input id="endDate" name="endDate" type="date" required />
                </div>
            </div>

            {error && <p className="text-sm text-red-500">{error}</p>}

            <div className="flex justify-end gap-2 pt-4">
                <Button type="button" variant="ghost" onClick={onCancel} disabled={isLoading}>Cancel</Button>
                <Button type="submit" isLoading={isLoading}>Create Campaign</Button>
            </div>
        </form>
    );
}

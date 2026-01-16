export interface User {
    username: string;
    email?: string;
    role: 'user' | 'admin';
    tier?: 'free' | 'pro' | 'enterprise';
    is_active?: boolean;
}

export interface LoginResponse {
    access_token: string;
    token_type: string;
    user: User;
}

export interface Campaign {
    campaign_id: string;
    name: string;
    objective: string;
    status: 'draft' | 'running' | 'completed' | 'failed';
    start_date?: string;
    end_date?: string;
    created_at: string;
}

export interface CampaignListResponse {
    items: Campaign[];
    total: number;
    page: number;
    limit: number;
    pages: number;
}

export interface CreateCampaignRequest {
    campaign_name: string; // Backend expects snake_case in query params mostly, but let's check integration
    objective: string;
    start_date: string;
    end_date: string;
}

export interface RegenerateReportResponse {
    job_id: string;
    campaign_id: string;
    template: string;
    status: string;
    message: string;
}

export interface ApiErrorResponse {
    detail: string;
}

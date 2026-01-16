/**
 * useDataAvailability Hook
 * 
 * Centralized logic for determining what filters and charts should be visible
 * based on the data schema and available filter options.
 */

export interface FilterAvailability {
    platform: boolean;
    channel: boolean;
    funnel: boolean;
    device: boolean;
    placement: boolean;
    region: boolean;
    adType: boolean;
}

export interface MetricAvailability {
    spend: boolean;
    impressions: boolean;
    clicks: boolean;
    conversions: boolean;
    reach: boolean;
    cpm: boolean;
    ctr: boolean;
    cpc: boolean;
    cpa: boolean;
    roas: boolean;
    revenue: boolean;
}

export interface ChartAvailability {
    trendSpend: boolean;
    trendClicks: boolean;
    trendConversions: boolean;
    trendROAS: boolean;
    trendCTR: boolean;
    platformBreakdown: boolean;
    channelBreakdown: boolean;
    funnelChart: boolean;
    monthlyPerformance: boolean;
}

export interface ColumnAvailability {
    spend: boolean;
    impressions: boolean;
    clicks: boolean;
    ctr: boolean;
    cpc: boolean;
    cpm: boolean;
    conversions: boolean;
    cpa: boolean;
    roas: boolean;
    reach: boolean;
    revenue: boolean;
}

export interface DataAvailability {
    filters: FilterAvailability;
    metrics: MetricAvailability;
    charts: ChartAvailability;
    columns: ColumnAvailability;
    hasAnyData: boolean;
    hasAnyFilters: boolean;
    hasAnyCharts: boolean;
}

interface Schema {
    has_data: boolean;
    metrics: Record<string, boolean>;
    dimensions: Record<string, boolean>;
    extra_metrics?: string[];
    extra_dimensions?: string[];
    all_columns?: string[];
}

interface FilterOptions {
    platforms: string[];
    channels: string[];
    funnelStages: string[];
    devices: string[];
    placements: string[];
    regions: string[];
    adTypes: string[];
}

/**
 * Compute data availability from schema and filter options
 */
export function computeDataAvailability(
    schema: Schema | null,
    filterOptions: FilterOptions
): DataAvailability {
    // Filter availability - based on whether there are options to filter by
    const filters: FilterAvailability = {
        platform: filterOptions.platforms.length > 0,
        channel: filterOptions.channels.length > 0,
        funnel: filterOptions.funnelStages.length > 0,
        device: filterOptions.devices.length > 0,
        placement: filterOptions.placements.length > 0,
        region: filterOptions.regions.length > 0,
        adType: filterOptions.adTypes.length > 0,
    };

    // Metric availability - based on schema
    const metrics: MetricAvailability = {
        spend: schema?.metrics?.spend ?? false,
        impressions: schema?.metrics?.impressions ?? false,
        clicks: schema?.metrics?.clicks ?? false,
        conversions: schema?.metrics?.conversions ?? false,
        reach: schema?.metrics?.reach ?? false,
        cpm: schema?.metrics?.cpm ?? (schema?.metrics?.spend && schema?.metrics?.impressions) ?? false,
        ctr: schema?.metrics?.ctr ?? (schema?.metrics?.clicks && schema?.metrics?.impressions) ?? false,
        cpc: schema?.metrics?.cpc ?? (schema?.metrics?.spend && schema?.metrics?.clicks) ?? false,
        cpa: schema?.metrics?.cpa ?? (schema?.metrics?.spend && schema?.metrics?.conversions) ?? false,
        roas: schema?.metrics?.roas ?? (schema?.metrics?.spend && schema?.metrics?.revenue) ?? false,
        revenue: schema?.metrics?.revenue ?? false,
    };

    // Chart availability - based on required metrics
    const charts: ChartAvailability = {
        trendSpend: metrics.spend,
        trendClicks: metrics.clicks,
        trendConversions: metrics.conversions,
        trendROAS: metrics.roas,
        trendCTR: metrics.ctr,
        platformBreakdown: filters.platform && (metrics.spend || metrics.clicks || metrics.impressions),
        channelBreakdown: filters.channel && (metrics.spend || metrics.clicks || metrics.impressions),
        funnelChart: filters.funnel || (metrics.impressions && metrics.clicks && metrics.conversions),
        monthlyPerformance: schema?.has_data ?? false,
    };

    // Column availability - for table headers (same as metrics but explicit for table rendering)
    const columns: ColumnAvailability = {
        spend: metrics.spend,
        impressions: metrics.impressions,
        clicks: metrics.clicks,
        ctr: metrics.ctr,
        cpc: metrics.cpc,
        cpm: metrics.cpm,
        conversions: metrics.conversions,
        cpa: metrics.cpa,
        roas: metrics.roas,
        reach: metrics.reach,
        revenue: metrics.revenue,
    };

    const hasAnyFilters = Object.values(filters).some(Boolean);
    const hasAnyCharts = Object.values(charts).some(Boolean);

    return {
        filters,
        metrics,
        charts,
        columns,
        hasAnyData: schema?.has_data ?? false,
        hasAnyFilters,
        hasAnyCharts,
    };
}

/**
 * Get a user-friendly message for why something is unavailable
 */
export function getUnavailableReason(
    type: 'filter' | 'chart',
    name: string,
    availability: DataAvailability
): string {
    if (type === 'filter') {
        return `No ${name} data available in the uploaded file`;
    }

    // Chart-specific messages
    switch (name) {
        case 'trendSpend':
            return 'Spend data not available';
        case 'trendClicks':
            return 'Clicks data not available';
        case 'trendConversions':
            return 'Conversions data not available';
        case 'trendROAS':
            return 'ROAS cannot be calculated (requires spend and revenue)';
        case 'trendCTR':
            return 'CTR cannot be calculated (requires clicks and impressions)';
        case 'platformBreakdown':
            return 'Platform breakdown not available';
        case 'channelBreakdown':
            return 'Channel breakdown not available';
        case 'funnelChart':
            return 'Funnel data not available';
        default:
            return 'Data not available';
    }
}

"use client";

import React, { useState, useMemo } from 'react';
import { ArrowUpDown, ArrowUp, ArrowDown } from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";

interface PerformanceTableProps {
    title: string;
    description: string;
    data: any[];
    type: 'month' | 'platform' | 'channel' | 'funnel' | 'region' | 'audience' | 'age' | 'ad_type' | 'objective' | 'targeting' | 'device';
    onMonthClick?: (month: string) => void;
    selectedMonth?: string | null;
    onPlatformClick?: (platform: string) => void;
    selectedPlatform?: string | null;
    onChannelClick?: (channel: string) => void;
    selectedChannel?: string | null;
    onFunnelStageClick?: (funnelStage: string) => void;
    selectedFunnelStage?: string | null;
    onDimensionClick?: (value: string) => void;
    selectedDimension?: string | null;
    schema?: {
        metrics?: Record<string, boolean>;
    };
    maxRows?: number;
}

export function PerformanceTable({
    title, description, data, type,
    onMonthClick, selectedMonth,
    onPlatformClick, selectedPlatform,
    onChannelClick, selectedChannel,
    onFunnelStageClick, selectedFunnelStage,
    onDimensionClick, selectedDimension,
    schema,
    maxRows = 10
}: PerformanceTableProps) {
    const [sortKey, setSortKey] = useState<string | null>(null);
    const [sortDirection, setSortDirection] = useState<'asc' | 'desc'>('desc');

    // Determine primary dimension key and label
    const getPrimaryDimension = (t: string): { key: string; label: string } => {
        switch (t) {
            case 'month': return { key: 'month', label: 'Month' };
            case 'platform': return { key: 'platform', label: 'Platform' };
            case 'channel': return { key: 'channel', label: 'Channel' };
            case 'funnel': return { key: 'funnel', label: 'Funnel Stage' };
            case 'region': return { key: 'region', label: 'Region' };
            case 'audience': return { key: 'audience', label: 'Audience' };
            case 'age': return { key: 'age', label: 'Age Group' };
            case 'ad_type': return { key: 'ad_type', label: 'Ad Type' };
            case 'objective': return { key: 'objective', label: 'Objective' };
            case 'targeting': return { key: 'targeting', label: 'Targeting' };
            case 'device': return { key: 'device', label: 'Device' };
            default: return { key: 'name', label: 'Name' };
        }
    };

    const primaryDim = getPrimaryDimension(type);

    // Define columns - fewer metrics for cleaner display
    const allColumns: { key: string; label: string }[] = [
        primaryDim,
        { key: 'spend', label: 'Spend' },
        { key: 'impressions', label: 'Impr.' },
        { key: 'clicks', label: 'Clicks' },
        { key: 'ctr', label: 'CTR' },
        { key: 'conversions', label: 'Conv.' },
        { key: 'cpa', label: 'CPA' },
        { key: 'roas', label: 'ROAS' },
        { key: 'cpc', label: 'CPC' }
    ];

    // Filter columns based on schema availability
    const columns = allColumns.filter(col => {
        if (col.key === primaryDim.key) return true;
        if (!schema?.metrics) return true;
        return schema.metrics[col.key] !== false;
    });

    // Sort data
    const sortedData = useMemo(() => {
        if (!data || data.length === 0) return [];
        let result = [...data];

        if (sortKey) {
            result.sort((a, b) => {
                const aVal = a[sortKey];
                const bVal = b[sortKey];

                if (typeof aVal === 'string' && typeof bVal === 'string') {
                    const comparison = aVal.localeCompare(bVal);
                    return sortDirection === 'asc' ? comparison : -comparison;
                }

                const aNum = Number(aVal) || 0;
                const bNum = Number(bVal) || 0;
                return sortDirection === 'asc' ? aNum - bNum : bNum - aNum;
            });
        }

        // Limit rows to prevent overflow
        return result.slice(0, maxRows);
    }, [data, sortKey, sortDirection, maxRows]);

    if (!data || data.length === 0) return null;

    const formatMonth = (monthStr: string) => {
        const [year, month] = monthStr.split('-');
        const date = new Date(parseInt(year), parseInt(month) - 1);
        const monthName = date.toLocaleDateString('en-US', { month: 'short' });
        const yearShort = year.slice(-2);
        return `${monthName} ${yearShort}`;
    };

    const formatCompactNumber = (num: number) => {
        if (num >= 1000000) {
            return (num / 1000000).toLocaleString(undefined, { minimumFractionDigits: 1, maximumFractionDigits: 1 }) + 'M';
        }
        if (num >= 1000) {
            return (num / 1000).toLocaleString(undefined, { minimumFractionDigits: 1, maximumFractionDigits: 1 }) + 'K';
        }
        return num.toLocaleString();
    };

    const formatValue = (key: string, val: unknown) => {
        if (key === 'month') return formatMonth(String(val));
        if (key === 'spend') return `$${formatCompactNumber(Number(val))}`;
        if (key === 'cpm' || key === 'cpc' || key === 'cpa') return `$${Number(val).toFixed(2)}`;
        if (key === 'ctr') return `${Number(val).toFixed(2)}%`;
        if (key === 'roas') return `${Number(val).toFixed(2)}x`;
        if (key === 'impressions' || key === 'clicks' || key === 'conversions' || key === 'reach') return formatCompactNumber(Number(val));
        return String(val);
    };

    const getHeatmapColor = (key: string, val: number, allData: Record<string, any>[]) => {
        const values = allData.map(d => Number(d[key]));
        const min = Math.min(...values);
        const max = Math.max(...values);
        const range = max - min;
        if (range === 0) return 'rgba(16, 185, 129, 0.1)';

        const percentage = (val - min) / range;
        const opacity = 0.1 + (percentage * 0.4);
        return `rgba(16, 185, 129, ${opacity})`;
    };

    const handleSort = (key: string) => {
        if (sortKey === key) {
            setSortDirection(sortDirection === 'asc' ? 'desc' : 'asc');
        } else {
            setSortKey(key);
            setSortDirection(key === 'month' || key === 'platform' ? 'asc' : 'desc');
        }
    };

    const getSortIcon = (key: string) => {
        if (sortKey !== key) {
            return <ArrowUpDown className="h-3 w-3 opacity-50" />;
        }
        return sortDirection === 'asc'
            ? <ArrowUp className="h-3 w-3" />
            : <ArrowDown className="h-3 w-3" />;
    };

    const handleRowClick = (row: any) => {
        if (type === 'month' && onMonthClick) onMonthClick(row.month);
        else if (type === 'platform' && onPlatformClick) onPlatformClick(row.platform);
        else if (type === 'channel' && onChannelClick) onChannelClick(row.channel);
        else if (type === 'funnel' && onFunnelStageClick) onFunnelStageClick(row.funnel);
        else if (onDimensionClick) onDimensionClick(row[primaryDim.key]);
    };

    const isRowSelected = (row: any) => {
        return (type === 'month' && selectedMonth === row.month)
            || (type === 'platform' && selectedPlatform === row.platform)
            || (type === 'channel' && selectedChannel === row.channel)
            || (type === 'funnel' && selectedFunnelStage === row.funnel)
            || (selectedDimension === row[primaryDim.key]);
    };

    const isClickable = onMonthClick || onPlatformClick || onChannelClick || onFunnelStageClick || onDimensionClick;

    return (
        <Card className="border-none bg-background/50 backdrop-blur-sm shadow-lg ring-1 ring-white/10">
            <CardHeader className="pb-2">
                <CardTitle className="text-sm font-bold uppercase tracking-wider">{title}</CardTitle>
                <CardDescription>{description}</CardDescription>
            </CardHeader>
            <CardContent className="p-0">
                {/* Header Row */}
                <div className="grid gap-0 px-3 py-2 border-y border-white/10 bg-white/5 font-bold text-[10px] uppercase tracking-tight text-muted-foreground"
                    style={{ gridTemplateColumns: `minmax(80px, 1fr) repeat(${columns.length - 1}, minmax(50px, 1fr))` }}
                >
                    {columns.map((col, colIndex) => (
                        <div
                            key={col.key}
                            className={`flex items-center gap-1 cursor-pointer hover:text-white transition-colors ${colIndex > 0 ? 'justify-end' : ''}`}
                            onClick={() => handleSort(col.key)}
                        >
                            <span className="truncate">{col.label}</span>
                            {getSortIcon(col.key)}
                        </div>
                    ))}
                </div>

                {/* Data Rows - No scrolling, just render all visible rows */}
                {sortedData.map((row, rowIndex) => {
                    const isSelected = isRowSelected(row);
                    return (
                        <div
                            key={rowIndex}
                            className={`grid gap-0 px-3 py-2 border-b border-white/5 transition-all text-xs
                                ${isClickable ? 'cursor-pointer hover:bg-blue-500/20' : 'hover:bg-white/5'}
                                ${isSelected ? 'bg-blue-500/30' : ''}
                            `}
                            style={{ gridTemplateColumns: `minmax(80px, 1fr) repeat(${columns.length - 1}, minmax(50px, 1fr))` }}
                            onClick={() => handleRowClick(row)}
                        >
                            {columns.map((col, colIndex) => {
                                const isNumeric = typeof row[col.key] === 'number';
                                const bgColor = isNumeric && colIndex > 0
                                    ? getHeatmapColor(col.key, row[col.key], data)
                                    : 'transparent';

                                return (
                                    <div
                                        key={col.key}
                                        className={`font-medium truncate ${colIndex > 0 ? 'text-right' : ''} ${isSelected && colIndex === 0 ? 'font-bold text-blue-400' : ''}`}
                                        style={{ backgroundColor: bgColor }}
                                    >
                                        {formatValue(col.key, row[col.key])}
                                    </div>
                                );
                            })}
                        </div>
                    );
                })}
            </CardContent>
        </Card>
    );
}


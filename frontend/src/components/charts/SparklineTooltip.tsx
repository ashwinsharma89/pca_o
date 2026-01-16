"use client";

import dynamic from 'next/dynamic';

const ResponsiveContainer = dynamic(() => import('recharts').then(mod => mod.ResponsiveContainer), { ssr: false });
const LineChart = dynamic(() => import('recharts').then(mod => mod.LineChart), { ssr: false });
const Line = dynamic(() => import('recharts').then(mod => mod.Line), { ssr: false });

interface SparklineTooltipProps {
    active?: boolean;
    payload?: any[];
    label?: string;
    trendData?: any[];
    dataKey?: string;
}

// Mini sparkline component for tooltips
function MiniSparkline({ data, dataKey, color = '#3b82f6' }: { data: any[]; dataKey: string; color?: string }) {
    if (!data || data.length === 0) return null;

    return (
        <div className="w-32 h-8">
            <ResponsiveContainer width="100%" height="100%">
                <LineChart data={data}>
                    <Line
                        type="monotone"
                        dataKey={dataKey}
                        stroke={color}
                        strokeWidth={1.5}
                        dot={false}
                    />
                </LineChart>
            </ResponsiveContainer>
        </div>
    );
}

export function SparklineTooltip({ active, payload, label, trendData = [], dataKey = 'value' }: SparklineTooltipProps) {
    if (!active || !payload || payload.length === 0) return null;

    const currentIndex = trendData.findIndex(d => d.date === label);
    const last7Days = trendData.slice(Math.max(0, currentIndex - 6), currentIndex + 1);

    return (
        <div className="bg-card border border-border rounded-lg p-3 shadow-lg min-w-[180px]">
            <p className="text-xs text-muted-foreground mb-1">{label}</p>
            {payload.map((entry: any, index: number) => (
                <div key={index} className="flex justify-between items-center mb-1">
                    <span className="text-sm font-medium" style={{ color: entry.color }}>
                        {entry.name}:
                    </span>
                    <span className="text-sm font-bold">
                        {typeof entry.value === 'number'
                            ? entry.value >= 1000
                                ? `${(entry.value / 1000).toFixed(1)}k`
                                : entry.value.toLocaleString()
                            : entry.value}
                    </span>
                </div>
            ))}
            {last7Days.length > 1 && (
                <div className="mt-2 pt-2 border-t border-border">
                    <p className="text-xs text-muted-foreground mb-1">7-Day Trend</p>
                    <MiniSparkline
                        data={last7Days}
                        dataKey={payload[0]?.dataKey || dataKey}
                        color={payload[0]?.color || '#3b82f6'}
                    />
                </div>
            )}
        </div>
    );
}

// Factory function to create tooltip with trend data
export function createSparklineTooltip(trendData: any[], dataKey: string) {
    return function CustomTooltip(props: any) {
        return <SparklineTooltip {...props} trendData={trendData} dataKey={dataKey} />;
    };
}

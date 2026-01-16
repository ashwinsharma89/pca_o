"use client";

import dynamic from 'next/dynamic';
import { motion } from 'framer-motion';

const ResponsiveContainer = dynamic(() => import('recharts').then(mod => mod.ResponsiveContainer), { ssr: false });
const AreaChart = dynamic(() => import('recharts').then(mod => mod.AreaChart), { ssr: false });
const Area = dynamic(() => import('recharts').then(mod => mod.Area), { ssr: false });
const XAxis = dynamic(() => import('recharts').then(mod => mod.XAxis), { ssr: false });
const YAxis = dynamic(() => import('recharts').then(mod => mod.YAxis), { ssr: false });
const CartesianGrid = dynamic(() => import('recharts').then(mod => mod.CartesianGrid), { ssr: false });
const Tooltip = dynamic(() => import('recharts').then(mod => mod.Tooltip), { ssr: false });
const Brush = dynamic(() => import('recharts').then(mod => mod.Brush), { ssr: false });

interface GradientAreaChartProps {
    data: any[];
    dataKey: string;
    xKey?: string;
    color?: string;
    showBrush?: boolean;
    onBrushChange?: (startIndex: number, endIndex: number) => void;
    comparisonData?: any[];
    comparisonKey?: string;
}

export function GradientAreaChart({
    data,
    dataKey,
    xKey = 'date',
    color = '#3b82f6',
    showBrush = false,
    onBrushChange,
    comparisonData,
    comparisonKey
}: GradientAreaChartProps) {
    if (!data || data.length === 0) {
        return <div className="flex items-center justify-center h-full text-muted-foreground">No data available</div>;
    }

    // Merge comparison data if provided
    const chartData = comparisonData
        ? data.map((item, idx) => ({
            ...item,
            comparison: comparisonData[idx]?.[comparisonKey || dataKey] || 0
        }))
        : data;

    const formatValue = (value: number) => {
        if (value >= 1000000) return `${(value / 1000000).toFixed(1)}M`;
        if (value >= 1000) return `${(value / 1000).toFixed(0)}k`;
        return value.toFixed(0);
    };

    return (
        <motion.div
            className="w-full h-full"
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.5 }}
        >
            <ResponsiveContainer width="100%" height="100%">
                <AreaChart data={chartData} margin={{ top: 10, right: 30, left: 0, bottom: showBrush ? 30 : 0 }}>
                    <defs>
                        <linearGradient id={`gradient-${dataKey}`} x1="0" y1="0" x2="0" y2="1">
                            <stop offset="5%" stopColor={color} stopOpacity={0.4} />
                            <stop offset="95%" stopColor={color} stopOpacity={0} />
                        </linearGradient>
                        {comparisonData && (
                            <linearGradient id="gradient-comparison" x1="0" y1="0" x2="0" y2="1">
                                <stop offset="5%" stopColor="#94a3b8" stopOpacity={0.4} />
                                <stop offset="95%" stopColor="#94a3b8" stopOpacity={0} />
                            </linearGradient>
                        )}
                    </defs>
                    <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border))" />
                    <XAxis
                        dataKey={xKey}
                        tick={{ fill: 'hsl(var(--foreground))', fontSize: 11 }}
                        tickFormatter={(value) => {
                            if (typeof value === 'string' && value.includes('-')) {
                                const date = new Date(value);
                                return `${date.getMonth() + 1}/${date.getDate()}`;
                            }
                            return value;
                        }}
                    />
                    <YAxis
                        tick={{ fill: 'hsl(var(--foreground))', fontSize: 11 }}
                        tickFormatter={formatValue}
                    />
                    <Tooltip
                        contentStyle={{
                            backgroundColor: 'hsl(var(--card))',
                            border: '1px solid hsl(var(--border))',
                            borderRadius: '8px'
                        }}
                        formatter={(value: any, name: any) => [formatValue(value), name === 'comparison' ? 'Previous' : dataKey]}
                    />
                    {comparisonData && (
                        <Area
                            type="monotone"
                            dataKey="comparison"
                            stroke="#94a3b8"
                            strokeWidth={2}
                            strokeDasharray="5 5"
                            fillOpacity={1}
                            fill="url(#gradient-comparison)"
                            animationDuration={1000}
                        />
                    )}
                    <Area
                        type="monotone"
                        dataKey={dataKey}
                        stroke={color}
                        strokeWidth={2}
                        fillOpacity={1}
                        fill={`url(#gradient-${dataKey})`}
                        animationDuration={1000}
                    />
                    {showBrush && (
                        <Brush
                            dataKey={xKey}
                            height={30}
                            stroke={color}
                            onChange={(e: any) => onBrushChange?.(e.startIndex, e.endIndex)}
                        />
                    )}
                </AreaChart>
            </ResponsiveContainer>
        </motion.div>
    );
}

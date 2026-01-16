"use client";

import { motion } from 'framer-motion';
import dynamic from 'next/dynamic';

const ResponsiveContainer = dynamic(() => import('recharts').then(mod => mod.ResponsiveContainer), { ssr: false });
const AreaChart = dynamic(() => import('recharts').then(mod => mod.AreaChart), { ssr: false });
const Area = dynamic(() => import('recharts').then(mod => mod.Area), { ssr: false });
const XAxis = dynamic(() => import('recharts').then(mod => mod.XAxis), { ssr: false });
const YAxis = dynamic(() => import('recharts').then(mod => mod.YAxis), { ssr: false });
const CartesianGrid = dynamic(() => import('recharts').then(mod => mod.CartesianGrid), { ssr: false });
const Tooltip = dynamic(() => import('recharts').then(mod => mod.Tooltip), { ssr: false });
const Legend = dynamic(() => import('recharts').then(mod => mod.Legend), { ssr: false });

interface StackedPercentageAreaProps {
    data: Record<string, any>[];
    dataKeys: string[];
    xKey?: string;
    colors?: string[];
}

const DEFAULT_COLORS = ['#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6', '#06b6d4'];

export function StackedPercentageArea({
    data,
    dataKeys,
    xKey = 'date',
    colors = DEFAULT_COLORS
}: StackedPercentageAreaProps) {
    if (!data || data.length === 0) {
        return <div className="flex items-center justify-center h-full text-muted-foreground">No data</div>;
    }

    // Normalize data to percentages
    const normalizedData = data.map(item => {
        const total = dataKeys.reduce((sum, key) => sum + (Number(item[key]) || 0), 0);
        const normalized: Record<string, any> = { [xKey]: item[xKey] };
        dataKeys.forEach(key => {
            normalized[key] = total > 0 ? ((Number(item[key]) || 0) / total) * 100 : 0;
        });
        return normalized;
    });

    return (
        <motion.div
            className="w-full h-full"
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
        >
            <ResponsiveContainer width="100%" height="100%">
                <AreaChart
                    data={normalizedData}
                    stackOffset="expand"
                    margin={{ top: 10, right: 30, left: 0, bottom: 0 }}
                >
                    <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border))" />
                    <XAxis
                        dataKey={xKey}
                        tick={{ fill: 'hsl(var(--foreground))', fontSize: 11 }}
                        axisLine={{ stroke: 'hsl(var(--border))' }}
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
                        axisLine={{ stroke: 'hsl(var(--border))' }}
                        tickFormatter={(value) => `${(value * 100).toFixed(0)}%`}
                    />
                    <Tooltip
                        contentStyle={{
                            backgroundColor: 'hsl(var(--card))',
                            border: '1px solid hsl(var(--border))',
                            borderRadius: '8px'
                        }}
                        formatter={((value: number, name: string) => [`${value.toFixed(1)}%`, name]) as any}
                    />
                    <Legend />
                    {dataKeys.map((key, idx) => (
                        <Area
                            key={key}
                            type="monotone"
                            dataKey={key}
                            stackId="1"
                            stroke={colors[idx % colors.length]}
                            fill={colors[idx % colors.length]}
                            fillOpacity={0.8}
                            animationDuration={1000}
                            animationBegin={idx * 100}
                        />
                    ))}
                </AreaChart>
            </ResponsiveContainer>
        </motion.div>
    );
}

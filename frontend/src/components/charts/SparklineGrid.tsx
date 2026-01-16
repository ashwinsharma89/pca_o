"use client";

import { motion } from 'framer-motion';
import dynamic from 'next/dynamic';

const ResponsiveContainer = dynamic(() => import('recharts').then(mod => mod.ResponsiveContainer), { ssr: false });
const LineChart = dynamic(() => import('recharts').then(mod => mod.LineChart), { ssr: false });
const Line = dynamic(() => import('recharts').then(mod => mod.Line), { ssr: false });

interface SparklineItem {
    label: string;
    value: number;
    trend: any[];
    trendKey?: string;
    prefix?: string;
    suffix?: string;
    change?: number;
}

interface SparklineGridProps {
    items: SparklineItem[];
    columns?: 2 | 3 | 4;
}

export function SparklineGrid({ items, columns = 3 }: SparklineGridProps) {
    if (!items || items.length === 0) {
        return <div className="flex items-center justify-center h-full text-muted-foreground">No data</div>;
    }

    const colClass = {
        2: 'grid-cols-2',
        3: 'grid-cols-3',
        4: 'grid-cols-4'
    }[columns];

    return (
        <div className={`grid ${colClass} gap-4 h-full`}>
            {items.map((item, idx) => {
                const isPositive = (item.change || 0) >= 0;
                const color = isPositive ? '#22c55e' : '#ef4444';

                return (
                    <motion.div
                        key={item.label}
                        initial={{ opacity: 0, y: 10 }}
                        animate={{ opacity: 1, y: 0 }}
                        transition={{ delay: idx * 0.08 }}
                        className="flex flex-col p-3 rounded-lg bg-muted/20 hover:bg-muted/40 transition-colors"
                    >
                        <span className="text-xs text-muted-foreground truncate">{item.label}</span>

                        <div className="flex items-end justify-between mt-1">
                            <span className="text-lg font-bold">
                                {item.prefix}{item.value.toLocaleString()}{item.suffix}
                            </span>
                            {item.change !== undefined && (
                                <span className={`text-xs font-medium ${isPositive ? 'text-green-500' : 'text-red-500'}`}>
                                    {isPositive ? '+' : ''}{item.change.toFixed(1)}%
                                </span>
                            )}
                        </div>

                        {/* Mini sparkline */}
                        <div className="h-8 mt-2">
                            <ResponsiveContainer width="100%" height="100%">
                                <LineChart data={item.trend}>
                                    <Line
                                        type="monotone"
                                        dataKey={item.trendKey || 'value'}
                                        stroke={color}
                                        strokeWidth={1.5}
                                        dot={false}
                                        animationDuration={800}
                                    />
                                </LineChart>
                            </ResponsiveContainer>
                        </div>
                    </motion.div>
                );
            })}
        </div>
    );
}

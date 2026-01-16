"use client";

import { motion } from 'framer-motion';

interface MiniBarChartProps {
    data: { label: string; value: number; color?: string }[];
    maxHeight?: number;
}

const COLORS = ['#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6'];

export function MiniBarChart({ data, maxHeight = 60 }: MiniBarChartProps) {
    if (!data || data.length === 0) return null;

    const maxValue = Math.max(...data.map(d => d.value));

    return (
        <div className="flex items-end gap-1 h-full">
            {data.map((item, idx) => {
                const height = (item.value / maxValue) * maxHeight;
                return (
                    <motion.div
                        key={item.label}
                        initial={{ height: 0 }}
                        animate={{ height: height }}
                        transition={{ delay: idx * 0.05, duration: 0.4 }}
                        className="flex-1 rounded-t"
                        style={{
                            backgroundColor: item.color || COLORS[idx % COLORS.length],
                            minWidth: '8px'
                        }}
                        title={`${item.label}: ${item.value.toLocaleString()}`}
                    />
                );
            })}
        </div>
    );
}

// Comparison bar showing two values side by side
interface ComparisonBarProps {
    label: string;
    current: number;
    previous: number;
    format?: (v: number) => string;
}

export function ComparisonBar({ label, current, previous, format = (v) => v.toLocaleString() }: ComparisonBarProps) {
    const max = Math.max(current, previous);
    const change = previous > 0 ? ((current - previous) / previous) * 100 : 0;
    const isPositive = change >= 0;

    return (
        <motion.div
            className="space-y-1"
            initial={{ opacity: 0, y: 10 }}
            animate={{ opacity: 1, y: 0 }}
        >
            <div className="flex justify-between text-xs">
                <span className="font-medium">{label}</span>
                <span className={isPositive ? 'text-green-500' : 'text-red-500'}>
                    {isPositive ? '+' : ''}{change.toFixed(1)}%
                </span>
            </div>
            <div className="flex gap-1 h-4">
                <motion.div
                    className="bg-primary rounded"
                    initial={{ width: 0 }}
                    animate={{ width: `${(current / max) * 100}%` }}
                    transition={{ duration: 0.5 }}
                />
            </div>
            <div className="flex gap-1 h-4">
                <motion.div
                    className="bg-muted-foreground/30 rounded"
                    initial={{ width: 0 }}
                    animate={{ width: `${(previous / max) * 100}%` }}
                    transition={{ duration: 0.5, delay: 0.1 }}
                />
            </div>
            <div className="flex justify-between text-xs text-muted-foreground">
                <span>Current: {format(current)}</span>
                <span>Previous: {format(previous)}</span>
            </div>
        </motion.div>
    );
}

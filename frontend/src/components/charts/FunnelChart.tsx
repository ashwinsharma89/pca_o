"use client";

import { motion } from 'framer-motion';

interface FunnelChartProps {
    data: {
        stage: string;
        value: number;
        color?: string;
    }[];
    showPercentage?: boolean;
}

const DEFAULT_COLORS = ['#3b82f6', '#6366f1', '#8b5cf6', '#a855f7', '#d946ef'];

export function FunnelChart({ data, showPercentage = true }: FunnelChartProps) {
    if (!data || data.length === 0) {
        return <div className="flex items-center justify-center h-full text-muted-foreground">No data</div>;
    }

    const maxValue = Math.max(...data.map(d => d.value));
    const total = data[0]?.value || 1;

    return (
        <div className="w-full h-full flex flex-col justify-center gap-2 px-4">
            {data.map((item, idx) => {
                const widthPercent = (item.value / maxValue) * 100;
                const conversionRate = ((item.value / total) * 100).toFixed(1);
                const dropOff = idx > 0 ? ((data[idx - 1].value - item.value) / data[idx - 1].value * 100).toFixed(0) : null;

                return (
                    <motion.div
                        key={item.stage}
                        initial={{ scaleX: 0, opacity: 0 }}
                        animate={{ scaleX: 1, opacity: 1 }}
                        transition={{ delay: idx * 0.15, duration: 0.5 }}
                        className="flex items-center gap-3"
                    >
                        {/* Label */}
                        <div className="w-24 text-right text-sm font-medium truncate">
                            {item.stage}
                        </div>

                        {/* Bar container */}
                        <div className="flex-1 relative">
                            <div
                                className="h-10 rounded-lg flex items-center justify-center relative overflow-hidden"
                                style={{
                                    width: `${widthPercent}%`,
                                    background: item.color || DEFAULT_COLORS[idx % DEFAULT_COLORS.length],
                                    minWidth: '80px'
                                }}
                            >
                                {/* Shimmer effect */}
                                <motion.div
                                    className="absolute inset-0 bg-gradient-to-r from-transparent via-white/20 to-transparent"
                                    initial={{ x: '-100%' }}
                                    animate={{ x: '100%' }}
                                    transition={{ duration: 2, repeat: Infinity, repeatDelay: 3 }}
                                />
                                <span className="text-white text-sm font-bold relative z-10">
                                    {item.value.toLocaleString()}
                                </span>
                            </div>

                            {/* Drop-off indicator */}
                            {dropOff && Number(dropOff) > 0 && (
                                <motion.div
                                    className="absolute -top-1 right-0 text-xs text-red-500 font-medium"
                                    initial={{ opacity: 0, y: 5 }}
                                    animate={{ opacity: 1, y: 0 }}
                                    transition={{ delay: idx * 0.15 + 0.3 }}
                                >
                                    ↓{dropOff}%
                                </motion.div>
                            )}
                        </div>

                        {/* Percentage */}
                        {showPercentage && (
                            <div className="w-16 text-sm text-muted-foreground">
                                {conversionRate}%
                            </div>
                        )}
                    </motion.div>
                );
            })}

            {/* Conversion summary */}
            <motion.div
                className="text-center mt-4 pt-4 border-t"
                initial={{ opacity: 0 }}
                animate={{ opacity: 1 }}
                transition={{ delay: 0.8 }}
            >
                <span className="text-2xl font-bold text-primary">
                    {((data[data.length - 1]?.value / total) * 100).toFixed(2)}%
                </span>
                <p className="text-xs text-muted-foreground">Overall Conversion Rate</p>
            </motion.div>
        </div>
    );
}

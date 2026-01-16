"use client";

import { motion } from 'framer-motion';
import dynamic from 'next/dynamic';

const ResponsiveContainer = dynamic(() => import('recharts').then(mod => mod.ResponsiveContainer), { ssr: false });
const PieChart = dynamic(() => import('recharts').then(mod => mod.PieChart), { ssr: false });
const Pie = dynamic(() => import('recharts').then(mod => mod.Pie), { ssr: false });
const Cell = dynamic(() => import('recharts').then(mod => mod.Cell), { ssr: false });
const Tooltip = dynamic(() => import('recharts').then(mod => mod.Tooltip), { ssr: false });

interface DonutWithStatsProps {
    data: { name: string; value: number; color?: string }[];
    centerValue?: string | number;
    centerLabel?: string;
    animated?: boolean;
}

const COLORS = ['#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6', '#06b6d4', '#ec4899'];

export function DonutWithStats({ data, centerValue, centerLabel, animated = true }: DonutWithStatsProps) {
    if (!data || data.length === 0) {
        return <div className="flex items-center justify-center h-full text-muted-foreground">No data</div>;
    }

    const total = data.reduce((sum, item) => sum + item.value, 0);
    const displayValue = centerValue ?? total.toLocaleString();

    return (
        <div className="w-full h-full relative">
            <ResponsiveContainer width="100%" height="100%">
                <PieChart>
                    <Pie
                        data={data}
                        cx="50%"
                        cy="50%"
                        innerRadius="65%"
                        outerRadius="90%"
                        paddingAngle={2}
                        dataKey="value"
                        animationBegin={0}
                        animationDuration={animated ? 1200 : 0}
                    >
                        {data.map((entry, index) => (
                            <Cell
                                key={`cell-${index}`}
                                fill={entry.color || COLORS[index % COLORS.length]}
                                stroke="none"
                            />
                        ))}
                    </Pie>
                    <Tooltip
                        contentStyle={{
                            backgroundColor: 'hsl(var(--card))',
                            border: '1px solid hsl(var(--border))',
                            borderRadius: '8px'
                        }}
                        formatter={(value: any, name: any) => [
                            `${value.toLocaleString()} (${((value / total) * 100).toFixed(1)}%)`,
                            name
                        ]}
                    />
                </PieChart>
            </ResponsiveContainer>

            {/* Center content */}
            <motion.div
                className="absolute inset-0 flex flex-col items-center justify-center pointer-events-none"
                initial={animated ? { scale: 0.5, opacity: 0 } : {}}
                animate={{ scale: 1, opacity: 1 }}
                transition={{ delay: 0.5, duration: 0.5 }}
            >
                <motion.span
                    className="text-3xl font-bold text-foreground"
                    initial={animated ? { opacity: 0, y: 10 } : {}}
                    animate={{ opacity: 1, y: 0 }}
                    transition={{ delay: 0.7 }}
                >
                    {displayValue}
                </motion.span>
                {centerLabel && (
                    <motion.span
                        className="text-xs text-muted-foreground mt-1"
                        initial={animated ? { opacity: 0 } : {}}
                        animate={{ opacity: 1 }}
                        transition={{ delay: 0.9 }}
                    >
                        {centerLabel}
                    </motion.span>
                )}
            </motion.div>

            {/* Legend */}
            <motion.div
                className="absolute bottom-0 left-0 right-0 flex flex-wrap justify-center gap-3 px-2"
                initial={animated ? { opacity: 0, y: 10 } : {}}
                animate={{ opacity: 1, y: 0 }}
                transition={{ delay: 1 }}
            >
                {data.slice(0, 5).map((entry, index) => (
                    <div key={entry.name} className="flex items-center gap-1">
                        <div
                            className="w-2 h-2 rounded-full"
                            style={{ backgroundColor: entry.color || COLORS[index % COLORS.length] }}
                        />
                        <span className="text-xs text-muted-foreground truncate max-w-[60px]">
                            {entry.name}
                        </span>
                    </div>
                ))}
            </motion.div>
        </div>
    );
}

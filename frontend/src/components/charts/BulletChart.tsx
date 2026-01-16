"use client";

import { motion } from 'framer-motion';

interface BulletChartProps {
    data: {
        label: string;
        value: number;
        target: number;
        ranges: [number, number, number]; // bad, ok, good thresholds
    }[];
    maxValue?: number;
}

export function BulletChart({ data, maxValue }: BulletChartProps) {
    if (!data || data.length === 0) {
        return <div className="flex items-center justify-center h-full text-muted-foreground">No data</div>;
    }

    const max = maxValue || Math.max(...data.flatMap(d => [d.value, d.target, ...d.ranges])) * 1.1;

    return (
        <div className="w-full h-full flex flex-col justify-center gap-6 px-2">
            {data.map((item, idx) => {
                const valuePercent = (item.value / max) * 100;
                const targetPercent = (item.target / max) * 100;
                const [bad, ok, good] = item.ranges.map(r => (r / max) * 100);

                return (
                    <motion.div
                        key={item.label}
                        initial={{ opacity: 0, x: -20 }}
                        animate={{ opacity: 1, x: 0 }}
                        transition={{ delay: idx * 0.15 }}
                        className="space-y-1"
                    >
                        <div className="flex justify-between items-center">
                            <span className="text-sm font-medium">{item.label}</span>
                            <span className="text-xs text-muted-foreground">
                                {item.value.toLocaleString()} / {item.target.toLocaleString()}
                            </span>
                        </div>

                        <div className="relative h-6 rounded overflow-hidden">
                            {/* Background ranges */}
                            <div className="absolute inset-0 flex">
                                <div
                                    className="bg-red-200 dark:bg-red-900/50"
                                    style={{ width: `${bad}%` }}
                                />
                                <div
                                    className="bg-amber-200 dark:bg-amber-900/50"
                                    style={{ width: `${ok - bad}%` }}
                                />
                                <div
                                    className="bg-emerald-200 dark:bg-emerald-900/50"
                                    style={{ width: `${good - ok}%` }}
                                />
                                <div
                                    className="bg-muted"
                                    style={{ width: `${100 - good}%` }}
                                />
                            </div>

                            {/* Value bar */}
                            <motion.div
                                className="absolute top-1 bottom-1 left-0 bg-foreground rounded"
                                initial={{ width: 0 }}
                                animate={{ width: `${valuePercent}%` }}
                                transition={{ delay: idx * 0.15 + 0.2, duration: 0.6 }}
                                style={{ maxWidth: '100%' }}
                            />

                            {/* Target marker */}
                            <motion.div
                                className="absolute top-0 bottom-0 w-0.5 bg-red-500"
                                initial={{ opacity: 0 }}
                                animate={{ opacity: 1, left: `${targetPercent}%` }}
                                transition={{ delay: idx * 0.15 + 0.5 }}
                            >
                                <div className="absolute -top-1 left-1/2 -translate-x-1/2 w-0 h-0 border-l-4 border-r-4 border-t-4 border-l-transparent border-r-transparent border-t-red-500" />
                            </motion.div>
                        </div>
                    </motion.div>
                );
            })}

            {/* Legend */}
            <div className="flex justify-center gap-4 mt-2">
                <div className="flex items-center gap-1">
                    <div className="w-3 h-3 bg-red-200 dark:bg-red-900/50 rounded" />
                    <span className="text-xs text-muted-foreground">Below</span>
                </div>
                <div className="flex items-center gap-1">
                    <div className="w-3 h-3 bg-amber-200 dark:bg-amber-900/50 rounded" />
                    <span className="text-xs text-muted-foreground">Near</span>
                </div>
                <div className="flex items-center gap-1">
                    <div className="w-3 h-3 bg-emerald-200 dark:bg-emerald-900/50 rounded" />
                    <span className="text-xs text-muted-foreground">Above</span>
                </div>
                <div className="flex items-center gap-1">
                    <div className="w-3 h-0.5 bg-red-500" />
                    <span className="text-xs text-muted-foreground">Target</span>
                </div>
            </div>
        </div>
    );
}

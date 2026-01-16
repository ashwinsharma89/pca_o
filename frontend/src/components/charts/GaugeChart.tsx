"use client";

import { motion } from 'framer-motion';
import dynamic from 'next/dynamic';

const ResponsiveContainer = dynamic(() => import('recharts').then(mod => mod.ResponsiveContainer), { ssr: false });
const RadialBarChart = dynamic(() => import('recharts').then(mod => mod.RadialBarChart), { ssr: false });
const RadialBar = dynamic(() => import('recharts').then(mod => mod.RadialBar), { ssr: false });
const PolarAngleAxis = dynamic(() => import('recharts').then(mod => mod.PolarAngleAxis), { ssr: false });

interface GaugeChartProps {
    value: number;
    max?: number;
    target?: number;
    label: string;
    unit?: string;
    size?: 'sm' | 'md' | 'lg';
}

const getGaugeColor = (percent: number): string => {
    if (percent >= 90) return '#22c55e'; // Green
    if (percent >= 70) return '#eab308'; // Yellow
    if (percent >= 50) return '#f97316'; // Orange
    return '#ef4444'; // Red
};

export function GaugeChart({ value, max = 100, target, label, unit = '', size = 'md' }: GaugeChartProps) {
    const percent = Math.min((value / max) * 100, 100);
    const color = getGaugeColor(target ? (value / target) * 100 : percent);

    const sizeMap = {
        sm: { height: 120, fontSize: 'text-lg' },
        md: { height: 180, fontSize: 'text-3xl' },
        lg: { height: 240, fontSize: 'text-4xl' }
    };

    const data = [{ value: percent, fill: color }];

    return (
        <motion.div
            className="flex flex-col items-center justify-center h-full"
            initial={{ scale: 0.8, opacity: 0 }}
            animate={{ scale: 1, opacity: 1 }}
            transition={{ duration: 0.5 }}
        >
            <div style={{ width: '100%', height: sizeMap[size].height }} className="relative">
                <ResponsiveContainer>
                    <RadialBarChart
                        cx="50%"
                        cy="100%"
                        innerRadius="60%"
                        outerRadius="100%"
                        startAngle={180}
                        endAngle={0}
                        data={data}
                    >
                        <PolarAngleAxis
                            type="number"
                            domain={[0, 100]}
                            angleAxisId={0}
                            tick={false}
                        />
                        <RadialBar
                            background={{ fill: 'hsl(var(--muted))' }}
                            dataKey="value"
                            cornerRadius={10}
                            animationDuration={1500}
                        />
                    </RadialBarChart>
                </ResponsiveContainer>

                {/* Center value */}
                <motion.div
                    className="absolute inset-x-0 bottom-2 text-center"
                    initial={{ opacity: 0, y: 10 }}
                    animate={{ opacity: 1, y: 0 }}
                    transition={{ delay: 0.5 }}
                >
                    <span className={`${sizeMap[size].fontSize} font-bold`} style={{ color }}>
                        {typeof value === 'number' ? value.toLocaleString() : value}
                    </span>
                    {unit && <span className="text-muted-foreground ml-1">{unit}</span>}
                </motion.div>
            </div>

            <div className="text-center mt-2">
                <p className="text-sm font-medium text-foreground">{label}</p>
                {target && (
                    <p className="text-xs text-muted-foreground">
                        Target: {target.toLocaleString()}{unit}
                    </p>
                )}
            </div>

            {/* Scale markers */}
            <div className="flex justify-between w-full px-4 mt-1">
                <span className="text-xs text-muted-foreground">0</span>
                <span className="text-xs text-muted-foreground">{max.toLocaleString()}</span>
            </div>
        </motion.div>
    );
}

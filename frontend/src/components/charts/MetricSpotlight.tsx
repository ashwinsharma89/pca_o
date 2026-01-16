"use client";

import { useState } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { TrendingUp, TrendingDown, Maximize2, X } from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import dynamic from 'next/dynamic';

const ResponsiveContainer = dynamic(() => import('recharts').then(mod => mod.ResponsiveContainer), { ssr: false });
const AreaChart = dynamic(() => import('recharts').then(mod => mod.AreaChart), { ssr: false });
const Area = dynamic(() => import('recharts').then(mod => mod.Area), { ssr: false });

interface MetricSpotlightProps {
    label: string;
    value: number;
    prefix?: string;
    suffix?: string;
    change?: number;
    trend?: any[];
    trendKey?: string;
    details?: { label: string; value: string }[];
    color?: string;
}

export function MetricSpotlight({
    label,
    value,
    prefix = '',
    suffix = '',
    change,
    trend = [],
    trendKey = 'value',
    details = [],
    color = '#3b82f6'
}: MetricSpotlightProps) {
    const [expanded, setExpanded] = useState(false);
    const isPositive = (change || 0) >= 0;

    const formatValue = (val: number): string => {
        if (val >= 1000000) return (val / 1000000).toFixed(1) + 'M';
        if (val >= 1000) return (val / 1000).toFixed(1) + 'k';
        return val.toLocaleString();
    };

    return (
        <>
            <motion.div
                className="cursor-pointer"
                whileHover={{ scale: 1.02 }}
                whileTap={{ scale: 0.98 }}
                onClick={() => setExpanded(true)}
            >
                <Card className="overflow-hidden group hover:shadow-lg transition-shadow">
                    <CardContent className="p-4">
                        <div className="flex justify-between items-start">
                            <div>
                                <p className="text-xs text-muted-foreground mb-1">{label}</p>
                                <p className="text-2xl font-bold">
                                    {prefix}{formatValue(value)}{suffix}
                                </p>
                                {change !== undefined && (
                                    <div className={`flex items-center gap-1 mt-1 text-xs ${isPositive ? 'text-green-500' : 'text-red-500'}`}>
                                        {isPositive ? <TrendingUp className="h-3 w-3" /> : <TrendingDown className="h-3 w-3" />}
                                        {isPositive ? '+' : ''}{change.toFixed(1)}%
                                    </div>
                                )}
                            </div>
                            <Button
                                variant="ghost"
                                size="icon"
                                className="h-6 w-6 opacity-0 group-hover:opacity-100 transition-opacity"
                            >
                                <Maximize2 className="h-3 w-3" />
                            </Button>
                        </div>

                        {/* Mini trend */}
                        {trend.length > 0 && (
                            <div className="h-12 mt-3">
                                <ResponsiveContainer width="100%" height="100%">
                                    <AreaChart data={trend}>
                                        <defs>
                                            <linearGradient id={`mini-gradient-${label}`} x1="0" y1="0" x2="0" y2="1">
                                                <stop offset="5%" stopColor={color} stopOpacity={0.3} />
                                                <stop offset="95%" stopColor={color} stopOpacity={0} />
                                            </linearGradient>
                                        </defs>
                                        <Area
                                            type="monotone"
                                            dataKey={trendKey}
                                            stroke={color}
                                            strokeWidth={1.5}
                                            fill={`url(#mini-gradient-${label})`}
                                            dot={false}
                                        />
                                    </AreaChart>
                                </ResponsiveContainer>
                            </div>
                        )}
                    </CardContent>
                </Card>
            </motion.div>

            {/* Expanded modal */}
            <AnimatePresence>
                {expanded && (
                    <motion.div
                        initial={{ opacity: 0 }}
                        animate={{ opacity: 1 }}
                        exit={{ opacity: 0 }}
                        className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm"
                        onClick={() => setExpanded(false)}
                    >
                        <motion.div
                            initial={{ scale: 0.9, opacity: 0 }}
                            animate={{ scale: 1, opacity: 1 }}
                            exit={{ scale: 0.9, opacity: 0 }}
                            className="w-full max-w-lg m-4"
                            onClick={(e) => e.stopPropagation()}
                        >
                            <Card>
                                <CardHeader className="flex flex-row items-center justify-between">
                                    <CardTitle>{label}</CardTitle>
                                    <Button variant="ghost" size="icon" onClick={() => setExpanded(false)}>
                                        <X className="h-4 w-4" />
                                    </Button>
                                </CardHeader>
                                <CardContent className="space-y-4">
                                    {/* Main value */}
                                    <div className="text-center py-4">
                                        <p className="text-4xl font-bold">
                                            {prefix}{formatValue(value)}{suffix}
                                        </p>
                                        {change !== undefined && (
                                            <div className={`flex items-center justify-center gap-2 mt-2 ${isPositive ? 'text-green-500' : 'text-red-500'}`}>
                                                {isPositive ? <TrendingUp className="h-5 w-5" /> : <TrendingDown className="h-5 w-5" />}
                                                <span className="text-lg">{isPositive ? '+' : ''}{change.toFixed(1)}% vs last period</span>
                                            </div>
                                        )}
                                    </div>

                                    {/* Trend chart */}
                                    {trend.length > 0 && (
                                        <div className="h-40">
                                            <ResponsiveContainer width="100%" height="100%">
                                                <AreaChart data={trend}>
                                                    <defs>
                                                        <linearGradient id={`expanded-gradient-${label}`} x1="0" y1="0" x2="0" y2="1">
                                                            <stop offset="5%" stopColor={color} stopOpacity={0.4} />
                                                            <stop offset="95%" stopColor={color} stopOpacity={0} />
                                                        </linearGradient>
                                                    </defs>
                                                    <Area
                                                        type="monotone"
                                                        dataKey={trendKey}
                                                        stroke={color}
                                                        strokeWidth={2}
                                                        fill={`url(#expanded-gradient-${label})`}
                                                    />
                                                </AreaChart>
                                            </ResponsiveContainer>
                                        </div>
                                    )}

                                    {/* Details */}
                                    {details.length > 0 && (
                                        <div className="grid grid-cols-2 gap-3 pt-4 border-t">
                                            {details.map((d, i) => (
                                                <div key={i} className="text-center p-2 rounded-lg bg-muted/30">
                                                    <p className="text-xs text-muted-foreground">{d.label}</p>
                                                    <p className="font-medium">{d.value}</p>
                                                </div>
                                            ))}
                                        </div>
                                    )}
                                </CardContent>
                            </Card>
                        </motion.div>
                    </motion.div>
                )}
            </AnimatePresence>
        </>
    );
}

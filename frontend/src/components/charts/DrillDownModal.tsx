"use client";

import { useState } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { X, ChevronDown } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import dynamic from 'next/dynamic';

const ResponsiveContainer = dynamic(() => import('recharts').then(mod => mod.ResponsiveContainer), { ssr: false });
const BarChart = dynamic(() => import('recharts').then(mod => mod.BarChart), { ssr: false });
const Bar = dynamic(() => import('recharts').then(mod => mod.Bar), { ssr: false });
const XAxis = dynamic(() => import('recharts').then(mod => mod.XAxis), { ssr: false });
const YAxis = dynamic(() => import('recharts').then(mod => mod.YAxis), { ssr: false });
const CartesianGrid = dynamic(() => import('recharts').then(mod => mod.CartesianGrid), { ssr: false });
const Tooltip = dynamic(() => import('recharts').then(mod => mod.Tooltip), { ssr: false });

interface DrillDownModalProps {
    isOpen: boolean;
    onClose: () => void;
    dimension: string;
    value: string;
    data: any[];
    breakdownBy?: 'channel' | 'device' | 'campaign';
}

export function DrillDownModal({ isOpen, onClose, dimension, value, data, breakdownBy = 'channel' }: DrillDownModalProps) {
    const [activeTab, setActiveTab] = useState<'spend' | 'clicks' | 'conversions'>('spend');

    // Mock breakdown data - in real implementation, this would be fetched
    const breakdownData = data.length > 0
        ? data
        : [
            { name: 'Channel A', spend: 12000, clicks: 450, conversions: 23 },
            { name: 'Channel B', spend: 8500, clicks: 320, conversions: 18 },
            { name: 'Channel C', spend: 6200, clicks: 280, conversions: 12 },
            { name: 'Channel D', spend: 4100, clicks: 190, conversions: 8 },
        ];

    const metrics = [
        { key: 'spend', label: 'Spend', format: (v: number) => `$${v.toLocaleString()}` },
        { key: 'clicks', label: 'Clicks', format: (v: number) => v.toLocaleString() },
        { key: 'conversions', label: 'Conversions', format: (v: number) => v.toLocaleString() },
    ];

    return (
        <AnimatePresence>
            {isOpen && (
                <motion.div
                    initial={{ opacity: 0 }}
                    animate={{ opacity: 1 }}
                    exit={{ opacity: 0 }}
                    className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm"
                    onClick={onClose}
                >
                    <motion.div
                        initial={{ scale: 0.9, opacity: 0, y: 20 }}
                        animate={{ scale: 1, opacity: 1, y: 0 }}
                        exit={{ scale: 0.9, opacity: 0, y: 20 }}
                        transition={{ type: 'spring', damping: 25, stiffness: 300 }}
                        className="w-full max-w-3xl max-h-[80vh] overflow-auto"
                        onClick={(e) => e.stopPropagation()}
                    >
                        <Card>
                            <CardHeader className="flex flex-row items-center justify-between">
                                <div>
                                    <CardTitle className="flex items-center gap-2">
                                        <span className="text-muted-foreground">{dimension}:</span>
                                        <span>{value}</span>
                                        <ChevronDown className="h-4 w-4 text-muted-foreground" />
                                        <span className="text-muted-foreground">by {breakdownBy}</span>
                                    </CardTitle>
                                </div>
                                <Button variant="ghost" size="icon" onClick={onClose}>
                                    <X className="h-4 w-4" />
                                </Button>
                            </CardHeader>
                            <CardContent className="space-y-4">
                                {/* Metric tabs */}
                                <div className="flex gap-2">
                                    {metrics.map(metric => (
                                        <Button
                                            key={metric.key}
                                            variant={activeTab === metric.key ? 'default' : 'outline'}
                                            size="sm"
                                            onClick={() => setActiveTab(metric.key as any)}
                                        >
                                            {metric.label}
                                        </Button>
                                    ))}
                                </div>

                                {/* Chart */}
                                <div className="h-[300px]">
                                    <ResponsiveContainer width="100%" height="100%">
                                        <BarChart data={breakdownData} layout="vertical" margin={{ left: 80 }}>
                                            <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border))" />
                                            <XAxis
                                                type="number"
                                                tick={{ fill: 'hsl(var(--foreground))', fontSize: 11 }}
                                            />
                                            <YAxis
                                                type="category"
                                                dataKey="name"
                                                tick={{ fill: 'hsl(var(--foreground))', fontSize: 11 }}
                                                width={80}
                                            />
                                            <Tooltip
                                                contentStyle={{
                                                    backgroundColor: 'hsl(var(--card))',
                                                    border: '1px solid hsl(var(--border))',
                                                    borderRadius: '8px'
                                                }}
                                                formatter={(value: any) => [
                                                    metrics.find(m => m.key === activeTab)?.format(value),
                                                    metrics.find(m => m.key === activeTab)?.label
                                                ]}
                                            />
                                            <Bar
                                                dataKey={activeTab}
                                                fill="#3b82f6"
                                                radius={[0, 4, 4, 0]}
                                                animationDuration={500}
                                            />
                                        </BarChart>
                                    </ResponsiveContainer>
                                </div>

                                {/* Summary table */}
                                <div className="rounded-lg border">
                                    <table className="w-full text-sm">
                                        <thead>
                                            <tr className="border-b bg-muted/50">
                                                <th className="text-left p-3">Name</th>
                                                <th className="text-right p-3">Spend</th>
                                                <th className="text-right p-3">Clicks</th>
                                                <th className="text-right p-3">Conversions</th>
                                            </tr>
                                        </thead>
                                        <tbody>
                                            {breakdownData.map((row, idx) => (
                                                <tr key={idx} className="border-b last:border-0">
                                                    <td className="p-3 font-medium">{row.name}</td>
                                                    <td className="p-3 text-right">${row.spend?.toLocaleString()}</td>
                                                    <td className="p-3 text-right">{row.clicks?.toLocaleString()}</td>
                                                    <td className="p-3 text-right">{row.conversions?.toLocaleString()}</td>
                                                </tr>
                                            ))}
                                        </tbody>
                                    </table>
                                </div>
                            </CardContent>
                        </Card>
                    </motion.div>
                </motion.div>
            )}
        </AnimatePresence>
    );
}

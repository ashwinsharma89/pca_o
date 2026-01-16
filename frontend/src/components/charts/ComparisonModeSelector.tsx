"use client";

import { useState } from 'react';
import { motion } from 'framer-motion';
import { Calendar, ChevronDown } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover';
import { useDashboard } from '@/context/DashboardContext';

interface ComparisonModeSelectorProps {
    onComparisonChange?: (enabled: boolean, type: string, customDays?: number) => void;
}

export function ComparisonModeSelector({ onComparisonChange }: ComparisonModeSelectorProps) {
    const { comparison, setComparison } = useDashboard();
    const [isOpen, setIsOpen] = useState(false);

    const presets = [
        { value: 'week', label: 'Week over Week', days: 7 },
        { value: 'month', label: 'Month over Month', days: 30 },
        { value: 'quarter', label: 'Quarter over Quarter', days: 90 },
        { value: 'year', label: 'Year over Year', days: 365 },
    ];

    const handleToggle = () => {
        const newEnabled = !comparison.enabled;
        setComparison({ ...comparison, enabled: newEnabled });
        onComparisonChange?.(newEnabled, comparison.type);
    };

    const handleTypeChange = (type: string) => {
        setComparison({ ...comparison, type: type as any, enabled: true });
        onComparisonChange?.(true, type);
        setIsOpen(false);
    };

    return (
        <div className="flex items-center gap-2">
            <motion.div
                initial={{ opacity: 0, x: -10 }}
                animate={{ opacity: 1, x: 0 }}
                className="flex items-center gap-2"
            >
                <Button
                    variant={comparison.enabled ? 'default' : 'outline'}
                    size="sm"
                    onClick={handleToggle}
                    className="gap-2"
                >
                    <Calendar className="h-4 w-4" />
                    {comparison.enabled ? 'Comparison On' : 'Compare'}
                </Button>

                {comparison.enabled && (
                    <motion.div
                        initial={{ opacity: 0, scale: 0.9 }}
                        animate={{ opacity: 1, scale: 1 }}
                    >
                        <Popover open={isOpen} onOpenChange={setIsOpen}>
                            <PopoverTrigger asChild>
                                <Button variant="outline" size="sm" className="gap-1">
                                    {presets.find(p => p.value === comparison.type)?.label || 'Select Period'}
                                    <ChevronDown className="h-3 w-3" />
                                </Button>
                            </PopoverTrigger>
                            <PopoverContent className="w-48 p-1" align="end">
                                {presets.map(preset => (
                                    <Button
                                        key={preset.value}
                                        variant={comparison.type === preset.value ? 'secondary' : 'ghost'}
                                        size="sm"
                                        className="w-full justify-start"
                                        onClick={() => handleTypeChange(preset.value)}
                                    >
                                        {preset.label}
                                    </Button>
                                ))}
                            </PopoverContent>
                        </Popover>
                    </motion.div>
                )}
            </motion.div>
        </div>
    );
}

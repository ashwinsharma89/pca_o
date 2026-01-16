"use client";

import { motion } from 'framer-motion';

interface PulseIndicatorProps {
    active?: boolean;
    color?: 'green' | 'red' | 'yellow' | 'blue';
    label?: string;
    size?: 'sm' | 'md' | 'lg';
}

const colorMap = {
    green: { bg: 'bg-green-500', ring: 'ring-green-400' },
    red: { bg: 'bg-red-500', ring: 'ring-red-400' },
    yellow: { bg: 'bg-yellow-500', ring: 'ring-yellow-400' },
    blue: { bg: 'bg-blue-500', ring: 'ring-blue-400' }
};

const sizeMap = {
    sm: { dot: 'w-2 h-2', outer: 'w-4 h-4' },
    md: { dot: 'w-3 h-3', outer: 'w-6 h-6' },
    lg: { dot: 'w-4 h-4', outer: 'w-8 h-8' }
};

export function PulseIndicator({
    active = true,
    color = 'green',
    label,
    size = 'md'
}: PulseIndicatorProps) {
    const colors = colorMap[color];
    const sizes = sizeMap[size];

    return (
        <div className="flex items-center gap-2">
            <div className="relative flex items-center justify-center">
                {/* Pulsing rings */}
                {active && (
                    <>
                        <motion.div
                            className={`absolute ${sizes.outer} rounded-full ${colors.bg} opacity-30`}
                            animate={{ scale: [1, 1.8, 1.8], opacity: [0.3, 0, 0] }}
                            transition={{ duration: 2, repeat: Infinity }}
                        />
                        <motion.div
                            className={`absolute ${sizes.outer} rounded-full ${colors.bg} opacity-30`}
                            animate={{ scale: [1, 1.5, 1.5], opacity: [0.3, 0, 0] }}
                            transition={{ duration: 2, repeat: Infinity, delay: 0.5 }}
                        />
                    </>
                )}

                {/* Core dot */}
                <motion.div
                    className={`relative ${sizes.dot} rounded-full ${colors.bg} shadow-sm`}
                    animate={active ? { scale: [1, 1.1, 1] } : {}}
                    transition={{ duration: 1, repeat: Infinity }}
                />
            </div>

            {label && (
                <span className="text-xs text-muted-foreground">{label}</span>
            )}
        </div>
    );
}

// Compound component for status row
interface StatusRowProps {
    items: { label: string; active: boolean; color?: 'green' | 'red' | 'yellow' | 'blue' }[];
}

export function StatusRow({ items }: StatusRowProps) {
    return (
        <div className="flex flex-wrap gap-4">
            {items.map((item, idx) => (
                <motion.div
                    key={item.label}
                    initial={{ opacity: 0, x: -10 }}
                    animate={{ opacity: 1, x: 0 }}
                    transition={{ delay: idx * 0.1 }}
                >
                    <PulseIndicator
                        active={item.active}
                        color={item.color || (item.active ? 'green' : 'red')}
                        label={item.label}
                        size="sm"
                    />
                </motion.div>
            ))}
        </div>
    );
}

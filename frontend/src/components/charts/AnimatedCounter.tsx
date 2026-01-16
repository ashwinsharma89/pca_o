"use client";

import { useState, useEffect } from 'react';
import { motion, useSpring, useMotionValue } from 'framer-motion';

interface AnimatedCounterProps {
    value: number;
    prefix?: string;
    suffix?: string;
    duration?: number;
    decimals?: number;
    className?: string;
    trend?: 'up' | 'down' | 'neutral';
    trendValue?: number;
}

export function AnimatedCounter({
    value,
    prefix = '',
    suffix = '',
    duration = 2,
    decimals = 0,
    className = '',
    trend,
    trendValue
}: AnimatedCounterProps) {
    const [displayValue, setDisplayValue] = useState(value);
    const motionValue = useMotionValue(value);

    const spring = useSpring(motionValue, {
        stiffness: 100,
        damping: 30,
        duration: duration * 1000
    });

    useEffect(() => {
        motionValue.set(value);
    }, [value, motionValue]);

    useEffect(() => {
        const unsubscribe = spring.on('change', (v) => {
            setDisplayValue(v);
        });
        return () => unsubscribe();
    }, [spring]);

    const formatValue = (val: number): string => {
        if (val >= 1000000) {
            return (val / 1000000).toFixed(1) + 'M';
        } else if (val >= 1000) {
            return (val / 1000).toFixed(decimals > 0 ? 1 : 0) + 'k';
        }
        return val.toFixed(decimals);
    };

    const getTrendIcon = () => {
        if (trend === 'up') return '↑';
        if (trend === 'down') return '↓';
        return '→';
    };

    const getTrendColor = () => {
        if (trend === 'up') return 'text-green-500';
        if (trend === 'down') return 'text-red-500';
        return 'text-muted-foreground';
    };

    return (
        <div className={`flex flex-col ${className}`}>
            <motion.div
                initial={{ scale: 0.5, opacity: 0 }}
                animate={{ scale: 1, opacity: 1 }}
                className="flex items-baseline gap-1"
            >
                {prefix && <span className="text-muted-foreground">{prefix}</span>}
                <motion.span
                    className="font-bold tabular-nums"
                    key={value}
                >
                    {formatValue(displayValue)}
                </motion.span>
                {suffix && <span className="text-muted-foreground text-sm">{suffix}</span>}
            </motion.div>

            {trend && trendValue !== undefined && (
                <motion.div
                    className={`flex items-center gap-1 text-xs ${getTrendColor()}`}
                    initial={{ opacity: 0, y: 5 }}
                    animate={{ opacity: 1, y: 0 }}
                    transition={{ delay: 0.5 }}
                >
                    <span>{getTrendIcon()}</span>
                    <span>{Math.abs(trendValue).toFixed(1)}%</span>
                    <span className="text-muted-foreground">vs last period</span>
                </motion.div>
            )}
        </div>
    );
}

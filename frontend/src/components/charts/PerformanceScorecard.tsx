"use client";

import { motion } from 'framer-motion';

interface ScorecardItem {
    name: string;
    score: number; // 0-100
    metrics?: { label: string; value: string }[];
}

interface PerformanceScorecardProps {
    data: ScorecardItem[];
    title?: string;
}

const getGrade = (score: number): { letter: string; color: string; bg: string } => {
    if (score >= 90) return { letter: 'A+', color: 'text-emerald-600', bg: 'bg-emerald-500' };
    if (score >= 80) return { letter: 'A', color: 'text-emerald-500', bg: 'bg-emerald-400' };
    if (score >= 70) return { letter: 'B', color: 'text-blue-500', bg: 'bg-blue-400' };
    if (score >= 60) return { letter: 'C', color: 'text-amber-500', bg: 'bg-amber-400' };
    if (score >= 50) return { letter: 'D', color: 'text-orange-500', bg: 'bg-orange-400' };
    return { letter: 'F', color: 'text-red-500', bg: 'bg-red-400' };
};

export function PerformanceScorecard({ data, title }: PerformanceScorecardProps) {
    if (!data || data.length === 0) {
        return <div className="flex items-center justify-center h-full text-muted-foreground">No data</div>;
    }

    return (
        <div className="w-full h-full">
            {title && (
                <h3 className="text-sm font-medium mb-3 text-foreground">{title}</h3>
            )}
            <div className="space-y-3">
                {data.map((item, idx) => {
                    const grade = getGrade(item.score);

                    return (
                        <motion.div
                            key={item.name}
                            initial={{ opacity: 0, y: 10 }}
                            animate={{ opacity: 1, y: 0 }}
                            transition={{ delay: idx * 0.1 }}
                            className="flex items-center gap-4 p-3 rounded-lg bg-muted/30 hover:bg-muted/50 transition-colors"
                        >
                            {/* Grade badge */}
                            <motion.div
                                className={`w-12 h-12 rounded-xl ${grade.bg} flex items-center justify-center`}
                                initial={{ scale: 0, rotate: -180 }}
                                animate={{ scale: 1, rotate: 0 }}
                                transition={{ delay: idx * 0.1 + 0.2, type: 'spring', stiffness: 200 }}
                            >
                                <span className="text-white font-bold text-lg">{grade.letter}</span>
                            </motion.div>

                            {/* Info */}
                            <div className="flex-1 min-w-0">
                                <div className="flex items-center justify-between">
                                    <span className="font-medium text-sm truncate">{item.name}</span>
                                    <span className={`text-sm font-mono ${grade.color}`}>
                                        {item.score}/100
                                    </span>
                                </div>

                                {/* Progress bar */}
                                <div className="mt-1.5 h-1.5 bg-muted rounded-full overflow-hidden">
                                    <motion.div
                                        className={`h-full ${grade.bg} rounded-full`}
                                        initial={{ width: 0 }}
                                        animate={{ width: `${item.score}%` }}
                                        transition={{ delay: idx * 0.1 + 0.3, duration: 0.6 }}
                                    />
                                </div>

                                {/* Optional metrics */}
                                {item.metrics && (
                                    <div className="flex gap-4 mt-2">
                                        {item.metrics.map((m, mIdx) => (
                                            <div key={mIdx} className="text-xs text-muted-foreground">
                                                <span>{m.label}: </span>
                                                <span className="font-medium text-foreground">{m.value}</span>
                                            </div>
                                        ))}
                                    </div>
                                )}
                            </div>
                        </motion.div>
                    );
                })}
            </div>
        </div>
    );
}

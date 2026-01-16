"use client";

import React, { createContext, useContext, useState, ReactNode } from 'react';

interface DateRange {
    start: string | null;
    end: string | null;
}

interface ComparisonPeriod {
    enabled: boolean;
    type: 'week' | 'month' | 'custom';
    customDays?: number;
}

interface DrillDownState {
    active: boolean;
    dimension: 'platform' | 'channel' | 'device' | null;
    value: string | null;
}

interface DashboardContextType {
    // Crossfilter state
    selectedDateRange: DateRange;
    setSelectedDateRange: (range: DateRange) => void;

    // Drill-down state
    drillDown: DrillDownState;
    setDrillDown: (state: DrillDownState) => void;
    clearDrillDown: () => void;

    // Comparison mode
    comparison: ComparisonPeriod;
    setComparison: (comparison: ComparisonPeriod) => void;

    // Selected metric for highlighting
    highlightedMetric: string | null;
    setHighlightedMetric: (metric: string | null) => void;
}

const DashboardContext = createContext<DashboardContextType | undefined>(undefined);

export function DashboardProvider({ children }: { children: ReactNode }) {
    const [selectedDateRange, setSelectedDateRange] = useState<DateRange>({ start: null, end: null });
    const [drillDown, setDrillDown] = useState<DrillDownState>({ active: false, dimension: null, value: null });
    const [comparison, setComparison] = useState<ComparisonPeriod>({ enabled: false, type: 'week' });
    const [highlightedMetric, setHighlightedMetric] = useState<string | null>(null);

    const clearDrillDown = () => {
        setDrillDown({ active: false, dimension: null, value: null });
    };

    return (
        <DashboardContext.Provider value={{
            selectedDateRange,
            setSelectedDateRange,
            drillDown,
            setDrillDown,
            clearDrillDown,
            comparison,
            setComparison,
            highlightedMetric,
            setHighlightedMetric
        }}>
            {children}
        </DashboardContext.Provider>
    );
}

export function useDashboard() {
    const context = useContext(DashboardContext);
    if (!context) {
        throw new Error('useDashboard must be used within a DashboardProvider');
    }
    return context;
}

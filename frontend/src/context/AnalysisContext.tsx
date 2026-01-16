"use client";

import React, { createContext, useContext, useState, useEffect } from 'react';
import { api } from '@/lib/api';

interface AnalysisConfig {
    use_rag_summary: boolean;
    include_benchmarks: boolean;
    analysis_depth: 'quick' | 'standard' | 'deep';
    include_recommendations: boolean;
}

export interface AnalysisResult {
    executive_summary?: {
        brief?: string;
        detailed?: string;
        rag_metadata?: {
            tokens_input: number;
            tokens_output: number;
            model: string;
            latency: number;
            retrieval_count?: number;
        };
    };
    insights?: string[];
    recommendations?: any[];
    metrics?: {
        by_platform?: Record<string, any>;
    };
    benchmarks?: Record<string, any>;
}

interface AnalysisContextType {
    analyzing: boolean;
    analysisResult: AnalysisResult | null;
    config: AnalysisConfig;
    setConfig: React.Dispatch<React.SetStateAction<AnalysisConfig>>;
    runAutoAnalysis: () => Promise<void>;
    clearAnalysis: () => void;
}

const AnalysisContext = createContext<AnalysisContextType | undefined>(undefined);

export function AnalysisProvider({ children }: { children: React.ReactNode }) {
    const [analyzing, setAnalyzing] = useState(false);
    const [analysisResult, setAnalysisResult] = useState<AnalysisResult | null>(null);
    const [config, setConfig] = useState<AnalysisConfig>({
        use_rag_summary: true,
        include_benchmarks: true,
        analysis_depth: 'standard',
        include_recommendations: true
    });

    // Persistence: load from localStorage on mount
    useEffect(() => {
        const savedResult = localStorage.getItem('pca_analysis_result');
        if (savedResult) {
            setAnalysisResult(JSON.parse(savedResult) as AnalysisResult);
        }

        const isAnalyzing = localStorage.getItem('pca_is_analyzing');
        if (isAnalyzing === 'true') {
            // If it was analyzing when we left, it likely finished or failed.
            // We can't easily resume a fetch, but we can reset the state.
            localStorage.removeItem('pca_is_analyzing');
        }
    }, []);

    const runAutoAnalysis = async () => {
        setAnalyzing(true);
        localStorage.setItem('pca_is_analyzing', 'true');
        try {
            const result = await api.post('/campaigns/analyze/global', config) as AnalysisResult;
            setAnalysisResult(result);
            localStorage.setItem('pca_analysis_result', JSON.stringify(result));
        } catch (error) {
            console.error("Analysis failed", error);
        } finally {
            setAnalyzing(false);
            localStorage.removeItem('pca_is_analyzing');
        }
    };

    const clearAnalysis = () => {
        setAnalysisResult(null);
        localStorage.removeItem('pca_analysis_result');
    };

    return (
        <AnalysisContext.Provider value={{
            analyzing,
            analysisResult,
            config,
            setConfig,
            runAutoAnalysis,
            clearAnalysis
        }}>
            {children}
        </AnalysisContext.Provider>
    );
}

export function useAnalysis() {
    const context = useContext(AnalysisContext);
    if (context === undefined) {
        throw new Error('useAnalysis must be used within an AnalysisProvider');
    }
    return context;
}

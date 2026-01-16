export interface RegressionV3Response {
    success: boolean;
    error?: string;
    model: {
        type: string;
        reason: string;
        confidence: string;
    };
    performance: {
        r2_train: number;
        r2_test: number;
        mae: number;
        rmse: number;
        mape: number;
        smape: number;
        train_test_gap: number;
        interpretation: string;
    };
    diagnostics: {
        multicollinearity: {
            features: Array<{
                feature: string;
                vif: number;
                status: string;
                recommendation: string;
            }>;
            summary: {
                max_vif: number;
                status: string;
                message: string;
            };
        };
        correlation: {
            high_correlations: Array<{
                feature_1: string;
                feature_2: string;
                correlation: number;
                interpretation: string;
            }>;
            summary: {
                total_pairs: number;
                threshold: number;
                message: string;
            };
        };
        residuals: {
            distribution: {
                mean: number;
                std: number;
                skewness: number;
                kurtosis: number;
            };
            normality_test: {
                shapiro_p_value: number;
                is_normal: boolean;
                interpretation: string;
            };
        };
    };
    feature_insights: Array<{
        rank: number;
        feature: string;
        interpretation: string;
        action: string;
        impact: string;
    }>;
    explanations?: {
        summary: Array<{
            feature: string;
            mean_abs_shap: number;
        }>;
        raw_values?: number[][];
        base_value?: number;
    };
    predictions: {
        sample: Array<{
            actual: number;
            predicted: number;
            residual: number;
            lower_bound: number;
            upper_bound: number;
        }>;
        residual_stats: {
            mean: number;
            std: number;
            min: number;
            max: number;
        };
    };
    executive_summary: string;
    coefficients?: Record<string, number>;
    feature_stats?: Record<string, {
        min: number;
        max: number;
        mean: number;
        std: number;
    }>;
}

"""
Feature Analyzer - Multicollinearity Detection & Feature Diagnostics

Detects and reports:
- VIF (Variance Inflation Factor) for multicollinearity
- Correlation analysis
- Feature importance validation

Critical for marketing mix models where spend/impressions/clicks are correlated.

Author: Senior ML Expert (Google Ads Platform experience)
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
import pandas as pd
import numpy as np
from scipy.stats import pearsonr


@dataclass
class VIFResult:
    """VIF analysis result for a single feature."""
    feature: str
    vif: float
    status: str  # "Good", "Moderate", "High"
    recommendation: str


@dataclass
class CorrelationPair:
    """High correlation pair."""
    feature_1: str
    feature_2: str
    correlation: float
    p_value: float


class FeatureAnalyzer:
    """
    Analyze features for multicollinearity and correlation issues.
    
    Key for marketing data where:
    - Spend ↔ Impressions (r > 0.9)
    - Impressions ↔ Clicks (r > 0.8)
    - Clicks ↔ Conversions (r > 0.7)
    """
    
    @staticmethod
    def compute_vif(X: pd.DataFrame, threshold_moderate: float = 5.0, threshold_high: float = 10.0) -> Dict:
        """
        Compute Variance Inflation Factor for all features.
        
        VIF measures how much variance is inflated due to multicollinearity:
        - VIF = 1: No correlation
        - VIF < 5: Low multicollinearity (acceptable)
        - VIF 5-10: Moderate multicollinearity (use Ridge)
        - VIF > 10: High multicollinearity (remove feature or use strong regularization)
        
        Args:
            X: Feature matrix
            threshold_moderate: VIF threshold for moderate concern
            threshold_high: VIF threshold for high concern
            
        Returns:
            Dict with VIF results and recommendations
        """
        from statsmodels.stats.outliers_influence import variance_inflation_factor
        
        vif_results = []
        
        for i, col in enumerate(X.columns):
            try:
                vif = variance_inflation_factor(X.values, i)
                
                # Classify VIF
                if vif < threshold_moderate:
                    status = "Good"
                    recommendation = "No action needed"
                elif vif < threshold_high:
                    status = "Moderate"
                    recommendation = "Use Ridge regression (already applied)"
                else:
                    status = "High"
                    recommendation = f"Consider removing {col} or using stronger regularization"
                
                vif_results.append(VIFResult(
                    feature=col,
                    vif=vif,
                    status=status,
                    recommendation=recommendation
                ))
            except Exception as e:
                # Handle edge cases (e.g., perfect multicollinearity)
                vif_results.append(VIFResult(
                    feature=col,
                    vif=999.9,
                    status="Error",
                    recommendation=f"Could not compute VIF: {str(e)}"
                ))
        
        # Sort by VIF descending
        vif_results.sort(key=lambda x: x.vif, reverse=True)
        
        # Overall assessment
        # Bug Fix: Include all VIFs (even extreme ones) in max_vif check
        all_vifs = [r.vif for r in vif_results]
        max_vif = max(all_vifs) if all_vifs else 0.0
        
        high_vif_count = sum(1 for r in vif_results if r.vif > threshold_high)
        
        if high_vif_count > 0:
            overall_status = "High"
            overall_message = f"⚠️ High multicollinearity detected ({high_vif_count} features with VIF > {threshold_high})"
        elif max_vif < threshold_moderate:
            overall_status = "Good"
            overall_message = "No multicollinearity concerns"
        elif max_vif < threshold_high:
            overall_status = "Moderate"
            overall_message = f"Moderate multicollinearity detected (max VIF: {max_vif:.1f}). Ridge regularization handles this."
        else:
             # Fallback
            overall_status = "High"
            overall_message = f"⚠️ High multicollinearity detected"
        
        return {
            "features": [
                {
                    "feature": r.feature,
                    "vif": round(r.vif, 2),
                    "status": r.status,
                    "recommendation": r.recommendation
                }
                for r in vif_results
            ],
            "summary": {
                "max_vif": round(max_vif, 2),
                "high_vif_count": high_vif_count,
                "status": overall_status,
                "message": overall_message
            }
        }
    
    @staticmethod
    def analyze_correlation(
        X: pd.DataFrame,
        threshold: float = 0.7,
        include_p_values: bool = True
    ) -> Dict:
        """
        Analyze feature correlations and identify high-correlation pairs.
        
        Args:
            X: Feature matrix
            threshold: Correlation threshold for flagging (default 0.7)
            include_p_values: Whether to compute p-values (slower)
            
        Returns:
            Dict with correlation matrix and high-correlation pairs
        """
        corr_matrix = X.corr()
        
        # Find high-correlation pairs
        high_corr_pairs = []
        
        for i in range(len(corr_matrix.columns)):
            for j in range(i+1, len(corr_matrix.columns)):
                corr_val = corr_matrix.iloc[i, j]
                
                if abs(corr_val) > threshold:
                    feat_1 = corr_matrix.columns[i]
                    feat_2 = corr_matrix.columns[j]
                    
                    # Compute p-value if requested
                    p_value = None
                    if include_p_values:
                        try:
                            _, p_value = pearsonr(X[feat_1], X[feat_2])
                        except:
                            p_value = None
                    
                    high_corr_pairs.append(CorrelationPair(
                        feature_1=feat_1,
                        feature_2=feat_2,
                        correlation=corr_val,
                        p_value=p_value
                    ))
        
        # Sort by absolute correlation
        high_corr_pairs.sort(key=lambda x: abs(x.correlation), reverse=True)
        
        return {
            "matrix": corr_matrix.round(3).to_dict(),
            "high_correlations": [
                {
                    "feature_1": pair.feature_1,
                    "feature_2": pair.feature_2,
                    "correlation": round(pair.correlation, 3),
                    "p_value": round(pair.p_value, 4) if pair.p_value else None,
                    "interpretation": FeatureAnalyzer._interpret_correlation(pair.correlation)
                }
                for pair in high_corr_pairs
            ],
            "summary": {
                "total_pairs": len(high_corr_pairs),
                "threshold": threshold,
                "message": f"Found {len(high_corr_pairs)} feature pairs with |r| > {threshold}"
            }
        }
    
    @staticmethod
    def _interpret_correlation(corr: float) -> str:
        """Interpret correlation strength."""
        abs_corr = abs(corr)
        direction = "positive" if corr > 0 else "negative"
        
        if abs_corr > 0.9:
            strength = "very strong"
        elif abs_corr > 0.7:
            strength = "strong"
        elif abs_corr > 0.5:
            strength = "moderate"
        else:
            strength = "weak"
        
        return f"{strength} {direction} correlation"
    
    @staticmethod
    def check_feature_coverage(
        X_train: pd.DataFrame,
        X_test: pd.DataFrame,
        threshold: float = 0.95
    ) -> Dict:
        """
        Check if test data is within training data range (avoid extrapolation).
        
        Args:
            X_train: Training features
            X_test: Test features
            threshold: Percentage of test samples that should be in range
            
        Returns:
            Dict with coverage analysis per feature
        """
        coverage_results = []
        
        for col in X_train.columns:
            train_min = X_train[col].min()
            train_max = X_train[col].max()
            
            # Check how many test samples are in range
            in_range = ((X_test[col] >= train_min) & (X_test[col] <= train_max)).sum()
            total = len(X_test)
            coverage_pct = in_range / total
            
            # Flag extrapolation risk
            if coverage_pct < threshold:
                status = "Warning"
                message = f"⚠️ {(1-coverage_pct)*100:.1f}% of test data outside training range"
            else:
                status = "Good"
                message = "Test data within training range"
            
            coverage_results.append({
                "feature": col,
                "train_range": [round(train_min, 2), round(train_max, 2)],
                "test_range": [round(X_test[col].min(), 2), round(X_test[col].max(), 2)],
                "coverage_pct": round(coverage_pct * 100, 1),
                "status": status,
                "message": message
            })
        
        # Overall coverage
        avg_coverage = np.mean([r["coverage_pct"] for r in coverage_results])
        
        return {
            "features": coverage_results,
            "summary": {
                "avg_coverage_pct": round(avg_coverage, 1),
                "features_with_warnings": sum(1 for r in coverage_results if r["status"] == "Warning"),
                "message": f"Average coverage: {avg_coverage:.1f}%"
            }
        }
    
    @staticmethod
    def validate_feature_quality(X: pd.DataFrame) -> Dict:
        """
        Validate feature quality (missing values, variance, outliers).
        
        Args:
            X: Feature matrix
            
        Returns:
            Dict with quality checks per feature
        """
        quality_results = []
        
        for col in X.columns:
            # Missing values
            missing_pct = X[col].isna().sum() / len(X) * 100
            
            # Variance
            variance = X[col].var()
            
            # Outliers (using IQR method)
            Q1 = X[col].quantile(0.25)
            Q3 = X[col].quantile(0.75)
            IQR = Q3 - Q1
            outlier_count = ((X[col] < Q1 - 1.5*IQR) | (X[col] > Q3 + 1.5*IQR)).sum()
            outlier_pct = outlier_count / len(X) * 100
            
            # Overall status
            issues = []
            if missing_pct > 5:
                issues.append(f"{missing_pct:.1f}% missing")
            if variance < 0.01:
                issues.append("Low variance")
            if outlier_pct > 10:
                issues.append(f"{outlier_pct:.1f}% outliers")
            
            status = "Warning" if issues else "Good"
            
            quality_results.append({
                "feature": col,
                "missing_pct": round(missing_pct, 2),
                "variance": round(variance, 4),
                "outlier_pct": round(outlier_pct, 2),
                "status": status,
                "issues": issues if issues else ["None"]
            })
        
        return {
            "features": quality_results,
            "summary": {
                "features_with_issues": sum(1 for r in quality_results if r["status"] == "Warning"),
                "total_features": len(quality_results)
            }
        }

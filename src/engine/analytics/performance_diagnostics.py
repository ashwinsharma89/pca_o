"""
Smart Performance Diagnostics Module

Provides causal analysis and driver analysis for marketing performance metrics.
Uses ML models (XGBoost, SHAP) to identify root causes and key drivers of metric changes.

Integrates with advanced causal analysis for comprehensive diagnostics.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Any, Optional
from dataclasses import dataclass
from datetime import datetime, timedelta
import logging

# Import advanced causal analysis
from src.engine.analytics.causal_analysis import (
    CausalAnalysisEngine,
    CausalAnalysisResult,
    DecompositionMethod,
    ComponentContribution
)

# ML imports
try:
    import xgboost as xgb
    import shap
    SHAP_AVAILABLE = True
except ImportError:
    SHAP_AVAILABLE = False
    logging.warning("SHAP/XGBoost not available. Install with: pip install xgboost shap")

from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from scipy import stats

logger = logging.getLogger(__name__)


@dataclass
class CausalBreakdown:
    """Breakdown of metric change into causal components."""
    metric: str
    total_change: float
    total_change_pct: float
    components: Dict[str, float]  # component -> contribution
    component_pcts: Dict[str, float]  # component -> % contribution
    period_comparison: Dict[str, Any]  # before/after stats
    root_cause: str  # primary driver
    confidence: float  # 0-1


@dataclass
class DriverAnalysis:
    """ML-based driver analysis results."""
    target_metric: str
    feature_importance: Dict[str, float]  # feature -> importance score
    shap_values: Optional[Dict[str, float]]  # feature -> SHAP value
    top_drivers: List[Tuple[str, float, str]]  # (feature, impact, direction)
    model_score: float  # R² or similar
    insights: List[str]  # human-readable insights


class PerformanceDiagnostics:
    """
    Smart Performance Diagnostics Engine
    
    Provides:
    1. Causal Analysis - Root cause breakdown of metric changes (uses CausalAnalysisEngine)
    2. Driver Analysis - ML-based identification of key performance drivers
    
    Integrates advanced causal analysis with driver analysis for comprehensive diagnostics.
    """
    
    def __init__(self):
        self.scaler = StandardScaler()
        self.models = {}  # metric -> trained model
        self.causal_engine = CausalAnalysisEngine()  # Advanced causal analysis engine
        
    def analyze_metric_change(
        self,
        df: pd.DataFrame,
        metric: str,
        date_col: str = 'Date',
        split_date: Optional[str] = None,
        lookback_days: int = 30,
        use_advanced: bool = True,
        include_ml: bool = True,
        include_attribution: bool = True
    ) -> CausalAnalysisResult:
        """
        Analyze what caused a metric to change between two periods.
        
        Now uses the advanced CausalAnalysisEngine for comprehensive analysis.
        
        Args:
            df: DataFrame with campaign data
            metric: Target metric (e.g., 'ROAS', 'CPA', 'CTR')
            date_col: Date column name
            split_date: Date to split before/after (default: midpoint)
            lookback_days: Days to look back for comparison
            use_advanced: Use advanced causal engine (recommended)
            include_ml: Include ML-based analysis
            include_attribution: Include channel/platform attribution
            
        Returns:
            CausalAnalysisResult with comprehensive root cause analysis
        """
        try:
            if use_advanced:
                # Use advanced causal analysis engine
                result = self.causal_engine.analyze(
                    df=df,
                    metric=metric,
                    date_col=date_col,
                    split_date=split_date,
                    lookback_days=lookback_days,
                    method=DecompositionMethod.HYBRID,
                    include_ml=include_ml,
                    include_attribution=include_attribution
                )
                return result
            else:
                # Legacy method - convert to CausalAnalysisResult
                breakdown = self._legacy_analyze_metric_change(
                    df, metric, date_col, split_date, lookback_days
                )
                return self._convert_breakdown_to_result(breakdown)
            
        except Exception as e:
            logger.error(f"Error in metric change analysis: {e}", exc_info=True)
            return self.causal_engine._create_empty_result(metric)
    
    def _legacy_analyze_metric_change(
        self,
        df: pd.DataFrame,
        metric: str,
        date_col: str = 'Date',
        split_date: Optional[str] = None,
        lookback_days: int = 30
    ) -> CausalBreakdown:
        """Legacy method for backward compatibility."""
        try:
            # Ensure date column is datetime
            df = df.copy()
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
            df = df.dropna(subset=[date_col])
            
            # Determine split date
            if split_date is None:
                # Use midpoint
                min_date = df[date_col].min()
                max_date = df[date_col].max()
                split_date = min_date + (max_date - min_date) / 2
            else:
                split_date = pd.to_datetime(split_date)
            
            # Split into before/after periods
            before_df = df[df[date_col] < split_date].tail(lookback_days)
            after_df = df[df[date_col] >= split_date].head(lookback_days)
            
            if len(before_df) == 0 or len(after_df) == 0:
                logger.warning(f"Insufficient data for period comparison")
                return self._create_empty_breakdown(metric)
            
            # Calculate metric change
            breakdown = self._calculate_causal_breakdown(
                before_df, after_df, metric
            )
            
            return breakdown
            
        except Exception as e:
            logger.error(f"Error in metric change analysis: {e}", exc_info=True)
            return self._create_empty_breakdown(metric)
    
    def _calculate_causal_breakdown(
        self,
        before_df: pd.DataFrame,
        after_df: pd.DataFrame,
        metric: str
    ) -> CausalBreakdown:
        """Calculate causal breakdown for metric change."""
        
        # Define metric decomposition formulas
        metric_formulas = {
            'ROAS': self._decompose_roas,
            'CPA': self._decompose_cpa,
            'CTR': self._decompose_ctr,
            'CVR': self._decompose_cvr,
            'CPC': self._decompose_cpc,
            'CPM': self._decompose_cpm,
        }
        
        # Calculate before/after values
        before_value = self._calculate_metric(before_df, metric)
        after_value = self._calculate_metric(after_df, metric)
        
        total_change = after_value - before_value
        total_change_pct = (total_change / before_value * 100) if before_value != 0 else 0
        
        # Get decomposition function
        decompose_fn = metric_formulas.get(metric, self._decompose_generic)
        
        # Calculate component contributions
        components = decompose_fn(before_df, after_df)
        
        # Calculate percentage contributions
        component_pcts = {}
        total_abs_contribution = sum(abs(v) for v in components.values())
        
        for comp, value in components.items():
            if total_abs_contribution > 0:
                component_pcts[comp] = (abs(value) / total_abs_contribution) * 100
            else:
                component_pcts[comp] = 0
        
        # Identify root cause (largest contributor)
        root_cause = max(components.items(), key=lambda x: abs(x[1]))[0] if components else "Unknown"
        
        # Calculate confidence based on data quality
        confidence = self._calculate_confidence(before_df, after_df, metric)
        
        return CausalBreakdown(
            metric=metric,
            total_change=total_change,
            total_change_pct=total_change_pct,
            components=components,
            component_pcts=component_pcts,
            period_comparison={
                'before': before_value,
                'after': after_value,
                'before_count': len(before_df),
                'after_count': len(after_df)
            },
            root_cause=root_cause,
            confidence=confidence
        )
    
    def _decompose_roas(self, before_df: pd.DataFrame, after_df: pd.DataFrame) -> Dict[str, float]:
        """Decompose ROAS change: ROAS = Revenue / Spend = (Conversions * AOV) / Spend"""
        
        components = {}
        
        # Calculate component changes
        before_conv = before_df['Conversions'].sum() if 'Conversions' in before_df.columns else 0
        after_conv = after_df['Conversions'].sum() if 'Conversions' in after_df.columns else 0
        
        before_spend = before_df['Spend'].sum() if 'Spend' in before_df.columns else 1
        after_spend = after_df['Spend'].sum() if 'Spend' in after_df.columns else 1
        
        before_revenue = before_df['Revenue'].sum() if 'Revenue' in before_df.columns else before_conv * 50
        after_revenue = after_df['Revenue'].sum() if 'Revenue' in after_df.columns else after_conv * 50
        
        before_aov = before_revenue / before_conv if before_conv > 0 else 0
        after_aov = after_revenue / after_conv if after_conv > 0 else 0
        
        # Contribution from conversion volume change
        conv_contribution = ((after_conv - before_conv) * before_aov) / before_spend if before_spend > 0 else 0
        
        # Contribution from AOV change
        aov_contribution = (after_conv * (after_aov - before_aov)) / before_spend if before_spend > 0 else 0
        
        # Contribution from spend change
        spend_contribution = -(after_revenue * (after_spend - before_spend)) / (before_spend * after_spend) if before_spend * after_spend > 0 else 0
        
        components['Conversion Volume'] = conv_contribution
        components['Average Order Value'] = aov_contribution
        components['Spend Efficiency'] = spend_contribution
        
        return components
    
    def _decompose_cpa(self, before_df: pd.DataFrame, after_df: pd.DataFrame) -> Dict[str, float]:
        """Decompose CPA change: CPA = Spend / Conversions"""
        
        components = {}
        
        before_spend = before_df['Spend'].sum() if 'Spend' in before_df.columns else 0
        after_spend = after_df['Spend'].sum() if 'Spend' in after_df.columns else 0
        
        before_conv = before_df['Conversions'].sum() if 'Conversions' in before_df.columns else 1
        after_conv = after_df['Conversions'].sum() if 'Conversions' in after_df.columns else 1
        
        before_clicks = before_df['Clicks'].sum() if 'Clicks' in before_df.columns else 0
        after_clicks = after_df['Clicks'].sum() if 'Clicks' in after_df.columns else 0
        
        # CPA = (Spend / Clicks) * (Clicks / Conversions) = CPC * (1/CVR)
        before_cpc = before_spend / before_clicks if before_clicks > 0 else 0
        after_cpc = after_spend / after_clicks if after_clicks > 0 else 0
        
        before_cvr = before_conv / before_clicks if before_clicks > 0 else 0
        after_cvr = after_conv / after_clicks if after_clicks > 0 else 0
        
        # Contribution from CPC change
        cpc_contribution = (after_cpc - before_cpc) / before_cvr if before_cvr > 0 else 0
        
        # Contribution from CVR change
        cvr_contribution = -before_cpc * (after_cvr - before_cvr) / (before_cvr * after_cvr) if before_cvr * after_cvr > 0 else 0
        
        components['Cost Per Click (CPC)'] = cpc_contribution
        components['Conversion Rate (CVR)'] = cvr_contribution
        
        return components
    
    def _decompose_ctr(self, before_df: pd.DataFrame, after_df: pd.DataFrame) -> Dict[str, float]:
        """Decompose CTR change: CTR = Clicks / Impressions"""
        
        components = {}
        
        before_clicks = before_df['Clicks'].sum() if 'Clicks' in before_df.columns else 0
        after_clicks = after_df['Clicks'].sum() if 'Clicks' in after_df.columns else 0
        
        before_impr = before_df['Impressions'].sum() if 'Impressions' in before_df.columns else 1
        after_impr = after_df['Impressions'].sum() if 'Impressions' in after_df.columns else 1
        
        # Contribution from click volume
        click_contribution = (after_clicks - before_clicks) / before_impr if before_impr > 0 else 0
        
        # Contribution from impression volume
        impr_contribution = -(before_clicks * (after_impr - before_impr)) / (before_impr * after_impr) if before_impr * after_impr > 0 else 0
        
        components['Click Volume'] = click_contribution
        components['Impression Volume'] = impr_contribution
        
        return components
    
    def _decompose_cvr(self, before_df: pd.DataFrame, after_df: pd.DataFrame) -> Dict[str, float]:
        """Decompose CVR change: CVR = Conversions / Clicks"""
        
        components = {}
        
        before_conv = before_df['Conversions'].sum() if 'Conversions' in before_df.columns else 0
        after_conv = after_df['Conversions'].sum() if 'Conversions' in after_df.columns else 0
        
        before_clicks = before_df['Clicks'].sum() if 'Clicks' in before_df.columns else 1
        after_clicks = after_df['Clicks'].sum() if 'Clicks' in after_df.columns else 1
        
        components['Conversion Volume'] = (after_conv - before_conv) / before_clicks if before_clicks > 0 else 0
        components['Click Volume'] = -(before_conv * (after_clicks - before_clicks)) / (before_clicks * after_clicks) if before_clicks * after_clicks > 0 else 0
        
        return components
    
    def _decompose_cpc(self, before_df: pd.DataFrame, after_df: pd.DataFrame) -> Dict[str, float]:
        """Decompose CPC change: CPC = Spend / Clicks"""
        
        components = {}
        
        before_spend = before_df['Spend'].sum() if 'Spend' in before_df.columns else 0
        after_spend = after_df['Spend'].sum() if 'Spend' in after_df.columns else 0
        
        before_clicks = before_df['Clicks'].sum() if 'Clicks' in before_df.columns else 1
        after_clicks = after_df['Clicks'].sum() if 'Clicks' in after_df.columns else 1
        
        components['Spend Change'] = (after_spend - before_spend) / before_clicks if before_clicks > 0 else 0
        components['Click Volume'] = -(before_spend * (after_clicks - before_clicks)) / (before_clicks * after_clicks) if before_clicks * after_clicks > 0 else 0
        
        return components
    
    def _decompose_cpm(self, before_df: pd.DataFrame, after_df: pd.DataFrame) -> Dict[str, float]:
        """Decompose CPM change: CPM = (Spend / Impressions) * 1000"""
        
        components = {}
        
        before_spend = before_df['Spend'].sum() if 'Spend' in before_df.columns else 0
        after_spend = after_df['Spend'].sum() if 'Spend' in after_df.columns else 0
        
        before_impr = before_df['Impressions'].sum() if 'Impressions' in before_df.columns else 1
        after_impr = after_df['Impressions'].sum() if 'Impressions' in after_df.columns else 1
        
        components['Spend Change'] = ((after_spend - before_spend) / before_impr) * 1000 if before_impr > 0 else 0
        components['Impression Volume'] = -(before_spend * (after_impr - before_impr) * 1000) / (before_impr * after_impr) if before_impr * after_impr > 0 else 0
        
        return components
    
    def _decompose_generic(self, before_df: pd.DataFrame, after_df: pd.DataFrame) -> Dict[str, float]:
        """Generic decomposition for unknown metrics."""
        return {'Unknown Factor': 0.0}
    
    def _calculate_metric(self, df: pd.DataFrame, metric: str) -> float:
        """Calculate metric value from dataframe."""
        
        if metric in df.columns:
            return df[metric].mean()
        
        # Calculate derived metrics
        metric_calcs = {
            'ROAS': lambda d: (d['Revenue'].sum() / d['Spend'].sum()) if 'Revenue' in d.columns and 'Spend' in d.columns and d['Spend'].sum() > 0 else 0,
            'CPA': lambda d: (d['Spend'].sum() / d['Conversions'].sum()) if 'Conversions' in d.columns and d['Conversions'].sum() > 0 else 0,
            'CTR': lambda d: (d['Clicks'].sum() / d['Impressions'].sum() * 100) if 'Impressions' in d.columns and d['Impressions'].sum() > 0 else 0,
            'CVR': lambda d: (d['Conversions'].sum() / d['Clicks'].sum() * 100) if 'Clicks' in d.columns and d['Clicks'].sum() > 0 else 0,
            'CPC': lambda d: (d['Spend'].sum() / d['Clicks'].sum()) if 'Clicks' in d.columns and d['Clicks'].sum() > 0 else 0,
            'CPM': lambda d: (d['Spend'].sum() / d['Impressions'].sum() * 1000) if 'Impressions' in d.columns and d['Impressions'].sum() > 0 else 0,
        }
        
        calc_fn = metric_calcs.get(metric)
        if calc_fn:
            return calc_fn(df)
        
        return 0.0
    
    def _calculate_confidence(self, before_df: pd.DataFrame, after_df: pd.DataFrame, metric: str) -> float:
        """Calculate confidence score based on data quality and statistical significance."""
        
        # Factors affecting confidence:
        # 1. Sample size
        # 2. Data variance
        # 3. Statistical significance
        
        min_sample_size = 10
        sample_score = min(1.0, (len(before_df) + len(after_df)) / (2 * min_sample_size))
        
        # Try to calculate statistical significance
        try:
            before_values = before_df[metric].values if metric in before_df.columns else []
            after_values = after_df[metric].values if metric in after_df.columns else []
            
            if len(before_values) > 1 and len(after_values) > 1:
                _, p_value = stats.ttest_ind(before_values, after_values)
                sig_score = 1.0 - p_value  # Lower p-value = higher confidence
            else:
                sig_score = 0.5
        except Exception as e:
            logger.debug(f"Statistical test failed: {e}")
            sig_score = 0.5
        
        # Combined confidence score
        confidence = (sample_score * 0.4 + sig_score * 0.6)
        
        return round(confidence, 2)
    
    def _create_empty_breakdown(self, metric: str) -> CausalBreakdown:
        """Create empty breakdown for error cases."""
        return CausalBreakdown(
            metric=metric,
            total_change=0.0,
            total_change_pct=0.0,
            components={},
            component_pcts={},
            period_comparison={},
            root_cause="Insufficient Data",
            confidence=0.0
        )
    
    def _convert_breakdown_to_result(self, breakdown: CausalBreakdown) -> CausalAnalysisResult:
        """Convert legacy CausalBreakdown to new CausalAnalysisResult format."""
        
        # Convert components dict to ComponentContribution list
        contributions = []
        for comp_name, comp_value in breakdown.components.items():
            pct = breakdown.component_pcts.get(comp_name, 0)
            contributions.append(ComponentContribution(
                component=comp_name,
                absolute_change=comp_value,
                percentage_contribution=pct,
                before_value=0.0,  # Not available in legacy format
                after_value=0.0,
                delta=comp_value,
                delta_pct=0.0,
                impact_direction="positive" if comp_value > 0 else "negative",
                actionability="medium"
            ))
        
        # Identify primary and secondary drivers
        sorted_contribs = sorted(contributions, key=lambda x: abs(x.absolute_change), reverse=True)
        primary = sorted_contribs[0] if sorted_contribs else None
        secondary = sorted_contribs[1:4] if len(sorted_contribs) > 1 else []
        
        return CausalAnalysisResult(
            metric=breakdown.metric,
            before_value=breakdown.period_comparison.get('before', 0),
            after_value=breakdown.period_comparison.get('after', 0),
            total_change=breakdown.total_change,
            total_change_pct=breakdown.total_change_pct,
            contributions=contributions,
            primary_driver=primary,
            secondary_drivers=secondary,
            confidence=breakdown.confidence,
            method="legacy",
            insights=[f"Root cause: {breakdown.root_cause}"],
            recommendations=[]
        )
    
    def comprehensive_diagnostics(
        self,
        df: pd.DataFrame,
        metric: str,
        date_col: str = 'Date',
        split_date: Optional[str] = None,
        lookback_days: int = 30
    ) -> Dict[str, Any]:
        """
        Run comprehensive diagnostics combining causal analysis and driver analysis.
        
        Args:
            df: Campaign data
            metric: Target metric
            date_col: Date column name
            split_date: Split date for before/after comparison
            lookback_days: Days to analyze
            
        Returns:
            Dict with both causal_analysis and driver_analysis results
        """
        
        results = {}
        
        # 1. Causal Analysis
        try:
            causal_result = self.analyze_metric_change(
                df=df,
                metric=metric,
                date_col=date_col,
                split_date=split_date,
                lookback_days=lookback_days,
                use_advanced=True,
                include_ml=True,
                include_attribution=True
            )
            results['causal_analysis'] = causal_result
        except Exception as e:
            logger.error(f"Causal analysis failed: {e}")
            results['causal_analysis'] = None
        
        # 2. Driver Analysis
        try:
            driver_result = self.analyze_drivers(
                df=df,
                target_metric=metric,
                feature_cols=None,  # Auto-detect
                categorical_cols=['Platform', 'Channel'] if 'Platform' in df.columns else None
            )
            results['driver_analysis'] = driver_result
        except Exception as e:
            logger.error(f"Driver analysis failed: {e}")
            results['driver_analysis'] = None
        
        # 3. Generate combined insights
        results['combined_insights'] = self._generate_combined_insights(
            results.get('causal_analysis'),
            results.get('driver_analysis')
        )
        
        return results
    
    def _generate_combined_insights(
        self,
        causal_result: Optional[CausalAnalysisResult],
        driver_result: Optional[DriverAnalysis]
    ) -> List[str]:
        """Generate insights combining causal and driver analysis."""
        
        insights = []
        
        if causal_result and causal_result.primary_driver:
            insights.append(
                f"🎯 **Causal Analysis:** {causal_result.primary_driver.component} "
                f"is the primary cause ({causal_result.primary_driver.percentage_contribution:.0f}% contribution)"
            )
        
        if driver_result and driver_result.top_drivers:
            top_driver = driver_result.top_drivers[0]
            insights.append(
                f"🔍 **Driver Analysis:** {top_driver[0]} is the top ML-identified driver "
                f"(importance: {top_driver[1]:.3f})"
            )
        
        # Cross-validate findings
        if causal_result and driver_result:
            causal_components = [c.component for c in causal_result.contributions]
            driver_features = [d[0] for d in driver_result.top_drivers]
            
            # Find overlapping factors
            overlapping = set(causal_components) & set(driver_features)
            if overlapping:
                insights.append(
                    f"✅ **Validated:** Both analyses agree on: {', '.join(overlapping)}"
                )
        
        if causal_result and causal_result.recommendations:
            insights.append(f"💡 **Top Recommendation:** {causal_result.recommendations[0]}")
        
        return insights
    
    def analyze_drivers(
        self,
        df: pd.DataFrame,
        target_metric: str,
        feature_cols: Optional[List[str]] = None,
        categorical_cols: Optional[List[str]] = None
    ) -> DriverAnalysis:
        """
        ML-based driver analysis using XGBoost and SHAP.
        
        Args:
            df: DataFrame with campaign data
            target_metric: Target metric to analyze
            feature_cols: List of feature columns (auto-detect if None)
            categorical_cols: List of categorical columns for encoding
            
        Returns:
            DriverAnalysis with feature importance and insights
        """
        
        if not SHAP_AVAILABLE:
            logger.warning("SHAP not available, returning basic analysis")
            return self._basic_driver_analysis(df, target_metric, feature_cols)
        
        try:
            # Prepare data
            X, y, feature_names = self._prepare_ml_data(
                df, target_metric, feature_cols, categorical_cols
            )
            
            if len(X) < 10:
                logger.warning("Insufficient data for ML analysis")
                return self._basic_driver_analysis(df, target_metric, feature_cols)
            
            # Train XGBoost model
            X_train, X_test, y_train, y_test = train_test_split(
                X, y, test_size=0.2, random_state=42
            )
            
            model = xgb.XGBRegressor(
                n_estimators=100,
                max_depth=5,
                learning_rate=0.1,
                random_state=42
            )
            
            model.fit(X_train, y_train)
            
            # Calculate model score
            model_score = model.score(X_test, y_test)
            
            # Get feature importance
            feature_importance = dict(zip(feature_names, model.feature_importances_))
            
            # Calculate SHAP values
            explainer = shap.TreeExplainer(model)
            shap_values = explainer.shap_values(X)
            
            # Average absolute SHAP values per feature
            shap_importance = {}
            for i, feature in enumerate(feature_names):
                shap_importance[feature] = float(np.abs(shap_values[:, i]).mean())
            
            # Identify top drivers
            top_drivers = self._identify_top_drivers(
                feature_importance, shap_importance, df, feature_names
            )
            
            # Generate insights
            insights = self._generate_driver_insights(
                target_metric, top_drivers, feature_importance, model_score
            )
            
            return DriverAnalysis(
                target_metric=target_metric,
                feature_importance=feature_importance,
                shap_values=shap_importance,
                top_drivers=top_drivers,
                model_score=model_score,
                insights=insights
            )
            
        except Exception as e:
            logger.error(f"Error in driver analysis: {e}", exc_info=True)
            return self._basic_driver_analysis(df, target_metric, feature_cols)
    
    def _prepare_ml_data(
        self,
        df: pd.DataFrame,
        target_metric: str,
        feature_cols: Optional[List[str]],
        categorical_cols: Optional[List[str]]
    ) -> Tuple[np.ndarray, np.ndarray, List[str]]:
        """Prepare data for ML model."""
        
        df = df.copy()
        
        # Auto-detect features if not provided
        if feature_cols is None:
            numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
            feature_cols = [col for col in numeric_cols if col != target_metric]
        
        # Calculate target if it's a derived metric
        if target_metric not in df.columns:
            df[target_metric] = df.apply(
                lambda row: self._calculate_metric(pd.DataFrame([row]), target_metric),
                axis=1
            )
        
        # Handle categorical columns
        if categorical_cols:
            df = pd.get_dummies(df, columns=categorical_cols, drop_first=True)
            # Update feature_cols with new dummy columns
            dummy_cols = [col for col in df.columns if any(cat in col for cat in categorical_cols)]
            feature_cols = [col for col in feature_cols if col not in categorical_cols] + dummy_cols
        
        # Filter to available columns
        available_features = [col for col in feature_cols if col in df.columns]
        
        # Remove rows with missing values
        df_clean = df[available_features + [target_metric]].dropna()
        
        X = df_clean[available_features].values
        y = df_clean[target_metric].values
        
        # Scale features
        X = self.scaler.fit_transform(X)
        
        return X, y, available_features
    
    def _identify_top_drivers(
        self,
        feature_importance: Dict[str, float],
        shap_importance: Dict[str, float],
        df: pd.DataFrame,
        feature_names: List[str]
    ) -> List[Tuple[str, float, str]]:
        """Identify top drivers with direction and impact."""
        
        # Combine importance scores
        combined_importance = {}
        for feature in feature_names:
            fi_score = feature_importance.get(feature, 0)
            shap_score = shap_importance.get(feature, 0)
            combined_importance[feature] = (fi_score + shap_score) / 2
        
        # Sort by importance
        sorted_features = sorted(
            combined_importance.items(),
            key=lambda x: x[1],
            reverse=True
        )
        
        # Get top 5 drivers with direction
        top_drivers = []
        for feature, importance in sorted_features[:5]:
            if feature in df.columns:
                # Determine direction (positive/negative correlation)
                correlation = df[feature].corr(df[feature]) if feature in df.columns else 0
                direction = "↑ Positive" if correlation > 0 else "↓ Negative"
            else:
                direction = "Unknown"
            
            top_drivers.append((feature, importance, direction))
        
        return top_drivers
    
    def _generate_driver_insights(
        self,
        target_metric: str,
        top_drivers: List[Tuple[str, float, str]],
        feature_importance: Dict[str, float],
        model_score: float
    ) -> List[str]:
        """Generate human-readable insights from driver analysis."""
        
        insights = []
        
        # Model quality insight
        if model_score > 0.7:
            insights.append(f"✅ High confidence model (R² = {model_score:.2%})")
        elif model_score > 0.4:
            insights.append(f"⚠️ Moderate confidence model (R² = {model_score:.2%})")
        else:
            insights.append(f"⚠️ Low confidence model (R² = {model_score:.2%}) - results may be unreliable")
        
        # Top driver insights
        if top_drivers:
            top_feature, top_importance, top_direction = top_drivers[0]
            pct_contribution = (top_importance / sum(feature_importance.values())) * 100
            
            insights.append(
                f"🎯 {pct_contribution:.0f}% of {target_metric} variation explained by {top_feature}"
            )
            
            # Second driver
            if len(top_drivers) > 1:
                second_feature, second_importance, _ = top_drivers[1]
                second_pct = (second_importance / sum(feature_importance.values())) * 100
                insights.append(
                    f"📊 {second_feature} is the second key driver ({second_pct:.0f}% contribution)"
                )
        
        # Actionability insight
        actionable_features = [f for f, _, _ in top_drivers if any(
            keyword in f.lower() for keyword in ['spend', 'bid', 'budget', 'audience', 'creative']
        )]
        
        if actionable_features:
            insights.append(
                f"💡 Actionable levers identified: {', '.join(actionable_features[:3])}"
            )
        
        return insights
    
    def _basic_driver_analysis(
        self,
        df: pd.DataFrame,
        target_metric: str,
        feature_cols: Optional[List[str]]
    ) -> DriverAnalysis:
        """Fallback basic driver analysis without ML."""
        
        if feature_cols is None:
            numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
            feature_cols = [col for col in numeric_cols if col != target_metric]
        
        # Calculate correlations
        correlations = {}
        for col in feature_cols:
            if col in df.columns and target_metric in df.columns:
                try:
                    corr = df[col].corr(df[target_metric])
                    if not np.isnan(corr):
                        correlations[col] = abs(corr)
                except Exception as e:
                    logger.debug(f"Correlation calculation failed for {col}: {e}")
        
        # Sort by correlation
        sorted_corr = sorted(correlations.items(), key=lambda x: x[1], reverse=True)
        
        # Create top drivers
        top_drivers = [
            (feature, corr, "Correlation-based")
            for feature, corr in sorted_corr[:5]
        ]
        
        insights = [
            "⚠️ Using correlation-based analysis (install xgboost and shap for advanced ML analysis)",
            f"📊 Top correlated feature: {sorted_corr[0][0]}" if sorted_corr else "No correlations found"
        ]
        
        return DriverAnalysis(
            target_metric=target_metric,
            feature_importance=correlations,
            shap_values=None,
            top_drivers=top_drivers,
            model_score=0.0,
            insights=insights
        )

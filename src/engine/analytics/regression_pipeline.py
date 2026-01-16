"""
Regression Pipeline + Insight Agent

Option A Architecture:
- Deterministic pipeline: DataPrep → Features → Train → Validate
- Single LLM InsightAgent at the end for business recommendations

This approach is efficient (1 LLM call) and deterministic where possible.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from loguru import logger
import json

from src.engine.analytics.data_prep_layer import DataPrepLayer
from src.engine.analytics.models.model_comparison import ModelComparisonEngine
from src.engine.analytics.models.validation_suite import ModelValidationSuite


@dataclass
class PipelineResult:
    """Complete pipeline result for insight agent."""
    success: bool
    target_col: str
    target_type: str  # 'conversions' or 'roas'
    
    # Model results
    best_model: str
    model_comparison: pd.DataFrame
    feature_importance: pd.DataFrame
    
    # Metrics
    r2_test: float
    rmse_test: float
    mape_test: Optional[float]
    
    # Top drivers
    top_drivers: List[Dict[str, Any]]
    
    # Validation summary
    validation_passed: bool
    validation_warnings: List[str]
    
    # Predictions
    predictions_sample: pd.DataFrame
    underperformers: List[str]
    
    # Platform insights (if available)
    platform_comparison: Optional[Dict[str, float]] = None
    
    # Raw data for LLM
    coefficients: Dict[str, float] = field(default_factory=dict)


class RegressionPipeline:
    """
    Unified regression pipeline that runs:
    1. Data preparation (cleaning, missing values, outliers)
    2. Feature engineering (derived ratios, temporal features)
    3. Model training (all 7 models, selects best)
    4. Validation (quality checks)
    
    Returns structured results for InsightAgent.
    """
    
    def __init__(
        self,
        models_to_run: Optional[List[str]] = None,
        quick_mode: bool = False,
        enable_validation: bool = True
    ):
        self.models_to_run = models_to_run or [
            'OLS', 'Ridge', 'Lasso', 'Elastic Net', 'Bayesian', 'Random Forest'
        ]
        self.quick_mode = quick_mode
        self.enable_validation = enable_validation
        
        self.data_prep = DataPrepLayer(enable_feature_engineering=True)
        self.comparison_engine = ModelComparisonEngine(
            models_to_run=self.models_to_run,
            quick_mode=quick_mode
        )
        self.validator = ModelValidationSuite()
    
    def run(
        self,
        df: pd.DataFrame,
        target_col: str,
        feature_cols: List[str],
        target_type: str = 'conversions',
        test_size: float = 0.2,
        campaign_id_col: Optional[str] = None,
        platform_col: Optional[str] = None,
        encode_dimensions: Optional[List[str]] = None,
        drop_first: bool = True
    ) -> PipelineResult:
        """
        Run complete regression pipeline.
        
        Args:
            df: Input DataFrame
            target_col: Target variable column
            feature_cols: Numeric feature columns to use
            target_type: 'conversions' or 'roas'
            test_size: Test set proportion
            campaign_id_col: Optional campaign ID column
            platform_col: Optional platform column
            encode_dimensions: Categorical dimensions to one-hot encode as predictors
                             (e.g., ['funnel', 'platform', 'objective'])
            drop_first: Drop first category to avoid multicollinearity (default True)
        """
        logger.info(f"RegressionPipeline: Starting with {len(df)} rows, target={target_col}")
        
        try:
            # Step 1: Data Preparation
            logger.info("Step 1/4: Data Preparation")
            df_clean, prep_metadata = self.data_prep.prepare(df)
            
            # Get feature columns (some may have been engineered)
            available_features = [c for c in feature_cols if c in df_clean.columns]
            
            # Add engineered features
            engineered = ['ctr', 'cpm', 'cpc', 'conversion_rate', 'cpa', 
                         'day_of_week', 'is_weekend', 'week_of_month']
            for feat in engineered:
                if feat in df_clean.columns and feat not in available_features:
                    available_features.append(feat)
            
            # Step 1.5: Encode categorical dimensions as predictors
            dimension_feature_names = []
            if encode_dimensions:
                logger.info(f"Encoding dimensions as predictors: {encode_dimensions}")
                df_clean, dimension_feature_names = self._encode_dimensions(
                    df_clean, encode_dimensions, drop_first=drop_first
                )
                available_features.extend(dimension_feature_names)
                logger.info(f"Added {len(dimension_feature_names)} dimension features")
            
            # Step 2: Train/Test Split
            from sklearn.model_selection import train_test_split
            
            X = df_clean[available_features].fillna(0)
            y = df_clean[target_col].fillna(0)
            
            X_train, X_test, y_train, y_test = train_test_split(
                X, y, test_size=test_size, random_state=42
            )
            
            # Get campaign IDs and platforms for test set
            campaign_ids = None
            platforms = None
            if campaign_id_col and campaign_id_col in df_clean.columns:
                _, campaign_ids, _, _ = train_test_split(
                    df_clean[campaign_id_col], y, test_size=test_size, random_state=42
                )
            if platform_col and platform_col in df_clean.columns:
                _, platforms, _, _ = train_test_split(
                    df_clean[platform_col], y, test_size=test_size, random_state=42
                )
            
            logger.info(f"Step 2/4: Train/Test split ({len(X_train)}/{len(X_test)})")
            
            # Step 3: Model Training & Comparison
            logger.info("Step 3/4: Training models")
            comparison_result = self.comparison_engine.run_comparison(
                X_train, y_train, X_test, y_test,
                target_type=target_type,
                campaign_ids=campaign_ids,
                platforms=platforms
            )
            
            # Step 4: Validation
            validation_passed = True
            validation_warnings = []
            
            if self.enable_validation:
                logger.info("Step 4/4: Running validation")
                
                self.validator.check_data_quality(df_clean, target_col)
                
                # Get best model predictions for validation
                best_model_name = comparison_result.best_model
                best_result = comparison_result.model_results.get(best_model_name)
                
                if best_result:
                    # Recompute predictions on train and test
                    train_pred = np.zeros(len(y_train))  # Placeholder
                    test_pred = best_result.predictions if best_result.predictions is not None else np.zeros(len(y_test))
                    
                    if len(test_pred) == len(y_test):
                        residuals = y_test.values - test_pred
                        self.validator.check_model_performance(
                            y_train.values, y_test.values, 
                            train_pred, test_pred, residuals
                        )
                        self.validator.check_residual_diagnostics(
                            residuals, test_pred, X_test, platforms
                        )
                
                report = self.validator.generate_report()
                validation_passed = report.production_ready
                validation_warnings = [
                    f"{r.check_name}: {r.message}" 
                    for r in (report.data_quality + report.model_performance)
                    if r.status in ['warning', 'failed']
                ]
            
            # Extract top drivers
            top_drivers = []
            for _, row in comparison_result.feature_importance.head(5).iterrows():
                top_drivers.append({
                    'feature': row['feature'],
                    'rank': int(row.get('rank', 0)),
                    'avg_importance': float(row.get('avg_importance', 0))
                })
            
            # Get coefficients from best linear model
            coefficients = {}
            for model_name in ['OLS', 'Ridge', 'Lasso', 'Elastic Net']:
                if model_name in comparison_result.model_results:
                    model_result = comparison_result.model_results[model_name]
                    for feat in model_result.feature_importance:
                        if feat['feature'] not in coefficients:
                            coefficients[feat['feature']] = feat.get('importance', 0)
                    break
            
            # Platform comparison
            platform_comparison = None
            if platforms is not None and 'platform' in comparison_result.predictions_df.columns:
                platform_means = comparison_result.predictions_df.groupby('platform')['actual'].mean()
                platform_comparison = platform_means.to_dict()
            
            # Underperformers
            underperformers = []
            if 'campaign_id' in comparison_result.predictions_df.columns:
                worst = comparison_result.predictions_df.nlargest(5, 'abs_error')
                underperformers = worst['campaign_id'].tolist()
            
            # Get best model metrics
            best_result = comparison_result.model_results.get(comparison_result.best_model)
            
            result = PipelineResult(
                success=True,
                target_col=target_col,
                target_type=target_type,
                best_model=comparison_result.best_model,
                model_comparison=comparison_result.comparison_table,
                feature_importance=comparison_result.feature_importance,
                r2_test=best_result.r2_test if best_result else 0,
                rmse_test=best_result.rmse_test if best_result else 0,
                mape_test=best_result.mape_test if best_result else None,
                top_drivers=top_drivers,
                validation_passed=validation_passed,
                validation_warnings=validation_warnings,
                predictions_sample=comparison_result.predictions_df.head(10),
                underperformers=underperformers,
                platform_comparison=platform_comparison,
                coefficients=coefficients
            )
            
            logger.info(f"Pipeline complete: Best model={result.best_model}, R²={result.r2_test:.3f}")
            return result
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            return PipelineResult(
                success=False,
                target_col=target_col,
                target_type=target_type,
                best_model='None',
                model_comparison=pd.DataFrame(),
                feature_importance=pd.DataFrame(),
                r2_test=0,
                rmse_test=0,
                mape_test=None,
                top_drivers=[],
                validation_passed=False,
                validation_warnings=[str(e)],
                predictions_sample=pd.DataFrame(),
                underperformers=[],
                coefficients={}
            )
    
    def _encode_dimensions(
        self,
        df: pd.DataFrame,
        dimensions: List[str],
        drop_first: bool = True
    ) -> Tuple[pd.DataFrame, List[str]]:
        """
        One-hot encode categorical dimensions as regression predictors.
        
        Args:
            df: Input DataFrame
            dimensions: List of categorical column names to encode
            drop_first: Drop first category to avoid multicollinearity
        
        Returns:
            Tuple of (DataFrame with encoded columns, list of new column names)
        """
        new_feature_names = []
        df_encoded = df.copy()
        
        for dim in dimensions:
            if dim not in df.columns:
                logger.warning(f"Dimension '{dim}' not found in data, skipping")
                continue
            
            # Get unique values (excluding NaN)
            unique_vals = df[dim].dropna().unique()
            
            if len(unique_vals) < 2:
                logger.warning(f"Dimension '{dim}' has <2 unique values, skipping")
                continue
            
            if len(unique_vals) > 50:
                logger.warning(f"Dimension '{dim}' has {len(unique_vals)} values (>50), skipping")
                continue
            
            # One-hot encode
            dummies = pd.get_dummies(df[dim], prefix=dim, drop_first=drop_first)
            
            # Add to DataFrame
            for col in dummies.columns:
                df_encoded[col] = dummies[col].astype(float)
                new_feature_names.append(col)
            
            logger.debug(f"Encoded '{dim}' -> {dummies.columns.tolist()}")
        
        return df_encoded, new_feature_names
    
    @staticmethod
    def get_dimension_coefficients(
        coefficients: Dict[str, float],
        dimension_prefix: str
    ) -> Dict[str, float]:
        """
        Extract coefficients for a specific dimension.
        
        Example:
            get_dimension_coefficients(coeffs, 'funnel')
            -> {'funnel_conversion': 15.3, 'funnel_consideration': 8.2}
        """
        return {
            k: v for k, v in coefficients.items()
            if k.startswith(dimension_prefix + '_')
        }


class InsightAgent:
    """
    LLM-powered agent that generates business insights from pipeline results.
    
    This is the ONLY LLM call in the pipeline.
    """
    
    SYSTEM_PROMPT = """You are a marketing analytics expert generating insights from regression analysis.

Given the model results, generate:
1. An executive summary (2-3 sentences)
2. Top 3 actionable recommendations
3. Key driver interpretation
4. Platform comparison insights (if available)

Be specific with numbers and metrics. Write for a marketing manager audience."""

    def __init__(self, llm_client=None):
        """
        Initialize insight agent.
        
        Args:
            llm_client: Optional LLM client (will use default if not provided)
        """
        self.llm_client = llm_client
    
    def generate_insights(self, pipeline_result: PipelineResult) -> Dict[str, Any]:
        """
        Generate business insights from pipeline results.
        
        Returns structured insights dictionary.
        """
        if not pipeline_result.success:
            return {
                'executive_summary': 'Analysis failed. Please check data quality.',
                'recommendations': ['Review input data', 'Check for missing values'],
                'driver_insights': [],
                'platform_insights': None
            }
        
        # Build context for LLM
        context = self._build_context(pipeline_result)
        
        # Try LLM call, fallback to rule-based
        try:
            insights = self._generate_with_llm(context, pipeline_result)
        except Exception as e:
            logger.warning(f"LLM failed, using rule-based insights: {e}")
            insights = self._generate_rule_based(pipeline_result)
        
        return insights
    
    def _build_context(self, result: PipelineResult) -> str:
        """Build context string for LLM."""
        context = f"""
## Regression Analysis Results

**Target**: {result.target_col} ({result.target_type})
**Best Model**: {result.best_model}
**R² Score**: {result.r2_test:.3f} ({result.r2_test*100:.1f}% variance explained)
**RMSE**: {result.rmse_test:.2f}

### Top Drivers (by importance):
"""
        for driver in result.top_drivers[:5]:
            context += f"- {driver['feature']} (rank #{driver['rank']})\n"
        
        if result.platform_comparison:
            context += "\n### Platform Performance:\n"
            for platform, value in sorted(result.platform_comparison.items(), key=lambda x: -x[1]):
                context += f"- {platform}: avg {result.target_col} = {value:.2f}\n"
        
        if result.validation_warnings:
            context += "\n### Warnings:\n"
            for warning in result.validation_warnings[:3]:
                context += f"- {warning}\n"
        
        return context
    
    def _generate_with_llm(self, context: str, result: PipelineResult) -> Dict[str, Any]:
        """Generate insights using LLM."""
        if self.llm_client is None:
            # Try to import and use default LLM
            try:
                from src.engine.analytics.llm_service import LLMService
                self.llm_client = LLMService()
            except:
                raise Exception("No LLM client available")
        
        prompt = f"""{context}

Generate insights in this JSON format:
{{
    "executive_summary": "2-3 sentence summary",
    "recommendations": ["rec1", "rec2", "rec3"],
    "driver_insights": ["insight about top driver 1", "insight about driver 2"],
    "platform_insights": "comparison insight or null"
}}"""

        response = self.llm_client.generate(
            prompt=prompt,
            system_prompt=self.SYSTEM_PROMPT
        )
        
        # Parse JSON from response
        try:
            # Find JSON in response
            start = response.find('{')
            end = response.rfind('}') + 1
            if start != -1 and end > start:
                return json.loads(response[start:end])
        except:
            pass
        
        # Fallback to rule-based
        return self._generate_rule_based(result)
    
    def _generate_rule_based(self, result: PipelineResult) -> Dict[str, Any]:
        """Generate insights using rules (no LLM)."""
        
        # Executive summary
        quality = 'excellent' if result.r2_test > 0.7 else 'good' if result.r2_test > 0.5 else 'moderate'
        summary = (
            f"The {result.best_model} model explains {result.r2_test*100:.1f}% of {result.target_col} variance "
            f"with {quality} predictive power. "
        )
        
        if result.top_drivers:
            top_driver = result.top_drivers[0]['feature']
            summary += f"'{top_driver}' is the primary driver of performance."
        
        # Recommendations
        recommendations = []
        
        if result.top_drivers:
            recommendations.append(
                f"Optimize '{result.top_drivers[0]['feature']}' - strongest impact on {result.target_col}"
            )
        
        if result.platform_comparison:
            best_platform = max(result.platform_comparison, key=result.platform_comparison.get)
            worst_platform = min(result.platform_comparison, key=result.platform_comparison.get)
            diff = ((result.platform_comparison[best_platform] / 
                    result.platform_comparison[worst_platform]) - 1) * 100
            recommendations.append(
                f"Consider reallocating budget from {worst_platform} to {best_platform} ({diff:.0f}% higher {result.target_col})"
            )
        
        if result.underperformers:
            recommendations.append(
                f"Review underperforming campaigns: {', '.join(result.underperformers[:3])}"
            )
        
        if not result.validation_passed:
            recommendations.append("Address data quality warnings before production deployment")
        
        # Driver insights
        driver_insights = []
        for driver in result.top_drivers[:3]:
            coef = result.coefficients.get(driver['feature'], 0)
            if coef != 0:
                direction = 'increases' if coef > 0 else 'decreases'
                driver_insights.append(
                    f"Higher '{driver['feature']}' {direction} {result.target_col}"
                )
        
        # Platform insights
        platform_insights = None
        if result.platform_comparison and len(result.platform_comparison) > 1:
            sorted_platforms = sorted(result.platform_comparison.items(), key=lambda x: -x[1])
            platform_insights = (
                f"Platform ranking by {result.target_col}: " +
                " > ".join([f"{p[0]} ({p[1]:.1f})" for p in sorted_platforms])
            )
        
        return {
            'executive_summary': summary,
            'recommendations': recommendations[:3],
            'driver_insights': driver_insights,
            'platform_insights': platform_insights
        }


def run_regression_with_insights(
    df: pd.DataFrame,
    target_col: str,
    feature_cols: List[str],
    **kwargs
) -> Tuple[PipelineResult, Dict[str, Any]]:
    """
    Convenience function to run full pipeline with insights.
    
    Returns:
        Tuple of (pipeline_result, insights)
    """
    pipeline = RegressionPipeline(**kwargs)
    result = pipeline.run(df, target_col, feature_cols)
    
    agent = InsightAgent()
    insights = agent.generate_insights(result)
    
    return result, insights

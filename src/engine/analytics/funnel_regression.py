"""
Funnel-Aware Regression Engine

Trains separate regression models per marketing funnel stage:
- Upper Funnel (Awareness): Target = Impressions/Reach, Features = CPM, Frequency
- Middle Funnel (Engagement): Target = Clicks/Video Views, Features = CTR, VTR
- Bottom Funnel (Conversion): Target = Conversions/ROAS, Features = CPA, Conv Rate

Each funnel gets its own model, coefficients, and actionable recommendations.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from loguru import logger

from sklearn.linear_model import LinearRegression, Ridge, Lasso
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import r2_score, mean_absolute_error
from sklearn.preprocessing import StandardScaler

from src.core.utils.column_mapping import find_column
from src.engine.analytics.data_prep_layer import DataPrepLayer


@dataclass
class FunnelConfig:
    """Configuration for a specific funnel stage."""
    name: str
    target_metrics: List[str]  # Primary target options
    priority_features: List[str]  # Features most relevant to this funnel
    secondary_features: List[str]  # Additional features to include
    kpi_focus: str  # What metric to optimize
    objective_keywords: List[str]  # Keywords to detect this funnel from objective


class FunnelRegressionEngine:
    """
    Funnel-aware regression engine that trains separate models per funnel stage.
    
    Usage:
        engine = FunnelRegressionEngine()
        results = engine.fit_all(df, objective_col='objective')
        
        # Results contain per-funnel models, coefficients, and recommendations
    """
    
    # ==========================================================================
    # FUNNEL CONFIGURATIONS
    # ==========================================================================
    FUNNEL_CONFIGS = {
        'upper': FunnelConfig(
            name='Upper Funnel (Awareness)',
            target_metrics=['impressions', 'reach'],
            priority_features=['spend', 'cpm', 'frequency'],
            secondary_features=['day_of_week', 'is_weekend'],
            kpi_focus='CPM',
            objective_keywords=['awareness', 'reach', 'brand', 'video views', 'impressions']
        ),
        'middle': FunnelConfig(
            name='Middle Funnel (Engagement)',
            target_metrics=['clicks', 'video_views', 'engagement'],
            priority_features=['spend', 'impressions', 'ctr', 'engagement_rate'],
            secondary_features=['frequency', 'day_of_week'],
            kpi_focus='CTR',
            objective_keywords=['traffic', 'engagement', 'clicks', 'consideration', 'video']
        ),
        'bottom': FunnelConfig(
            name='Bottom Funnel (Conversion)',
            target_metrics=['conversions', 'revenue', 'roas'],
            priority_features=['spend', 'clicks', 'cpa', 'conversion_rate'],
            secondary_features=['ctr', 'impressions'],
            kpi_focus='CPA',
            objective_keywords=['conversions', 'purchase', 'sales', 'leads', 'app installs', 'roas']
        )
    }
    
    # Objective to funnel mapping (common ad platform objectives)
    OBJECTIVE_FUNNEL_MAP = {
        # Upper funnel
        'awareness': 'upper', 'brand awareness': 'upper', 'reach': 'upper',
        'video views': 'upper', 'impressions': 'upper',
        # Middle funnel
        'traffic': 'middle', 'engagement': 'middle', 'clicks': 'middle',
        'consideration': 'middle', 'app engagement': 'middle',
        'video engagement': 'middle', 'post engagement': 'middle',
        # Bottom funnel
        'conversions': 'bottom', 'sales': 'bottom', 'leads': 'bottom',
        'app installs': 'bottom', 'catalog sales': 'bottom',
        'store traffic': 'bottom', 'purchases': 'bottom'
    }
    
    def __init__(
        self,
        model_type: str = 'ridge',
        enable_scaling: bool = True,
        min_samples_per_funnel: int = 30
    ):
        """
        Initialize FunnelRegressionEngine.
        
        Args:
            model_type: 'linear', 'ridge', 'lasso', or 'random_forest'
            enable_scaling: Whether to scale features
            min_samples_per_funnel: Minimum samples required to train a funnel model
        """
        self.model_type = model_type
        self.enable_scaling = enable_scaling
        self.min_samples_per_funnel = min_samples_per_funnel
        
        # State
        self.models: Dict[str, Any] = {}
        self.scalers: Dict[str, StandardScaler] = {}
        self.results: Dict[str, Dict[str, Any]] = {}
        self.data_prep = DataPrepLayer(enable_feature_engineering=True)
        
    def detect_funnel(self, objective: str) -> str:
        """
        Detect funnel stage from campaign objective string.
        
        Args:
            objective: Campaign objective (e.g., 'Conversions', 'Brand Awareness')
            
        Returns:
            Funnel stage: 'upper', 'middle', or 'bottom'
        """
        if not objective or pd.isna(objective):
            return 'bottom'  # Default to bottom funnel
        
        obj_lower = str(objective).lower().strip()
        
        # Direct lookup
        if obj_lower in self.OBJECTIVE_FUNNEL_MAP:
            return self.OBJECTIVE_FUNNEL_MAP[obj_lower]
        
        # Keyword matching
        for keyword, funnel in self.OBJECTIVE_FUNNEL_MAP.items():
            if keyword in obj_lower:
                return funnel
        
        # Check funnel config keywords
        for funnel_key, config in self.FUNNEL_CONFIGS.items():
            for keyword in config.objective_keywords:
                if keyword in obj_lower:
                    return funnel_key
        
        return 'bottom'  # Default
    
    def assign_funnels(
        self,
        df: pd.DataFrame,
        objective_col: str = 'objective',
        funnel_col: str = 'funnel_stage'
    ) -> pd.DataFrame:
        """
        Assign funnel stages to all rows based on objective or existing funnel column.
        """
        df = df.copy()
        
        # Check if funnel already exists
        existing_funnel = find_column(df, funnel_col)
        if existing_funnel and existing_funnel in df.columns:
            # Normalize existing funnel values
            df['_funnel'] = df[existing_funnel].str.lower().map(
                lambda x: 'upper' if 'upper' in str(x) or 'awareness' in str(x)
                else 'middle' if 'middle' in str(x) or 'engagement' in str(x)
                else 'bottom'
            )
        else:
            # Detect from objective
            obj_col = find_column(df, objective_col) or objective_col
            if obj_col in df.columns:
                df['_funnel'] = df[obj_col].apply(self.detect_funnel)
            else:
                df['_funnel'] = 'bottom'  # Default all to bottom
        
        # Log distribution
        funnel_counts = df['_funnel'].value_counts()
        logger.info(f"Funnel distribution: {funnel_counts.to_dict()}")
        
        return df
    
    def _get_model(self):
        """Get a fresh model instance based on model_type."""
        if self.model_type == 'ridge':
            return Ridge(alpha=1.0)
        elif self.model_type == 'lasso':
            return Lasso(alpha=0.1)
        elif self.model_type == 'random_forest':
            return RandomForestRegressor(n_estimators=100, random_state=42)
        else:
            return LinearRegression()
    
    def _select_target(self, df: pd.DataFrame, config: FunnelConfig) -> Optional[str]:
        """Select the best available target metric for this funnel."""
        for target in config.target_metrics:
            col = find_column(df, target)
            if col and col in df.columns and df[col].notna().sum() > 0:
                return col
        return None
    
    def _select_features(self, df: pd.DataFrame, config: FunnelConfig) -> List[str]:
        """Select available features for this funnel."""
        features = []
        
        for feat in config.priority_features + config.secondary_features:
            col = find_column(df, feat)
            if col and col in df.columns:
                features.append(col)
        
        # Add platform indicators if available
        platform_cols = [c for c in df.columns if c.startswith('platform_')]
        features.extend(platform_cols)
        
        return list(set(features))
    
    def fit_funnel(
        self,
        df: pd.DataFrame,
        funnel_key: str
    ) -> Dict[str, Any]:
        """
        Train a regression model for a specific funnel.
        
        Returns:
            Dictionary with model, metrics, coefficients, and recommendations
        """
        config = self.FUNNEL_CONFIGS[funnel_key]
        
        # Select target
        target_col = self._select_target(df, config)
        if not target_col:
            return {"error": f"No valid target found for {config.name}"}
        
        # Select features
        feature_cols = self._select_features(df, config)
        if len(feature_cols) < 2:
            return {"error": f"Insufficient features for {config.name}"}
        
        # Prepare data
        X = df[feature_cols].fillna(0)
        y = df[target_col].fillna(0)
        
        # Remove zero-variance columns
        X = X.loc[:, X.std() > 0]
        feature_cols = list(X.columns)
        
        if len(X) < self.min_samples_per_funnel:
            return {"error": f"Insufficient samples ({len(X)}) for {config.name}"}
        
        # Scale if enabled
        if self.enable_scaling:
            scaler = StandardScaler()
            X_scaled = pd.DataFrame(
                scaler.fit_transform(X),
                columns=feature_cols,
                index=X.index
            )
            self.scalers[funnel_key] = scaler
        else:
            X_scaled = X
        
        # Train model
        model = self._get_model()
        model.fit(X_scaled, y)
        self.models[funnel_key] = model
        
        # Predictions and metrics
        predictions = model.predict(X_scaled)
        r2 = r2_score(y, predictions)
        mae = mean_absolute_error(y, predictions)
        
        # Extract coefficients
        coefficients = []
        if hasattr(model, 'coef_'):
            for feat, coef in zip(feature_cols, model.coef_):
                coefficients.append({
                    'feature': feat,
                    'coefficient': float(coef),
                    'impact': 'positive' if coef > 0 else 'negative',
                    'magnitude': abs(float(coef))
                })
        elif hasattr(model, 'feature_importances_'):
            for feat, imp in zip(feature_cols, model.feature_importances_):
                coefficients.append({
                    'feature': feat,
                    'importance': float(imp),
                    'impact': 'importance',
                    'magnitude': float(imp)
                })
        
        # Sort by magnitude
        coefficients.sort(key=lambda x: x['magnitude'], reverse=True)
        
        # Generate recommendations
        recommendations = self._generate_recommendations(funnel_key, coefficients, r2)
        
        result = {
            'funnel': funnel_key,
            'funnel_name': config.name,
            'target': target_col,
            'features': feature_cols,
            'sample_size': len(X),
            'metrics': {
                'r2_score': round(r2, 4),
                'mae': round(mae, 2),
                'kpi_focus': config.kpi_focus
            },
            'coefficients': coefficients[:10],  # Top 10
            'recommendations': recommendations
        }
        
        self.results[funnel_key] = result
        return result
    
    def _generate_recommendations(
        self,
        funnel_key: str,
        coefficients: List[Dict],
        r2: float
    ) -> List[Dict[str, str]]:
        """Generate actionable recommendations based on model results."""
        recommendations = []
        config = self.FUNNEL_CONFIGS[funnel_key]
        
        if not coefficients:
            return recommendations
        
        # Top driver
        top_driver = coefficients[0]
        if top_driver['impact'] == 'positive' or top_driver.get('importance', 0) > 0:
            recommendations.append({
                'type': 'scale',
                'priority': 'high',
                'message': f"Scale {top_driver['feature']} - strongest driver of {config.name} performance"
            })
        
        # Check for spend efficiency
        spend_coef = next((c for c in coefficients if 'spend' in c['feature'].lower()), None)
        if spend_coef:
            if spend_coef['coefficient'] > 0:
                recommendations.append({
                    'type': 'invest',
                    'priority': 'medium',
                    'message': f"Increase spend allocation for {config.name} campaigns"
                })
            else:
                recommendations.append({
                    'type': 'optimize',
                    'priority': 'high',
                    'message': f"Optimize {config.name} spend - diminishing returns detected"
                })
        
        # Model quality recommendation
        if r2 < 0.3:
            recommendations.append({
                'type': 'data',
                'priority': 'medium',
                'message': f"Low model fit (R²={r2:.2f}) - consider adding more features or data"
            })
        elif r2 > 0.7:
            recommendations.append({
                'type': 'confidence',
                'priority': 'info',
                'message': f"High model confidence (R²={r2:.2f}) - predictions are reliable"
            })
        
        # Funnel-specific recommendations
        if funnel_key == 'upper':
            recommendations.append({
                'type': 'strategy',
                'priority': 'info',
                'message': "Focus on reach and frequency optimization for awareness impact"
            })
        elif funnel_key == 'middle':
            recommendations.append({
                'type': 'strategy',
                'priority': 'info',
                'message': "A/B test creatives to improve CTR and engagement rates"
            })
        elif funnel_key == 'bottom':
            recommendations.append({
                'type': 'strategy',
                'priority': 'info',
                'message': "Monitor CPA trends and retarget high-intent audiences"
            })
        
        return recommendations
    
    def fit_all(
        self,
        df: pd.DataFrame,
        objective_col: str = 'objective'
    ) -> Dict[str, Any]:
        """
        Train models for all funnels present in the data.
        
        Args:
            df: Prepared DataFrame with features
            objective_col: Column containing campaign objective
            
        Returns:
            Dictionary with results per funnel and overall summary
        """
        logger.info(f"FunnelRegressionEngine: Starting fit on {len(df)} rows")
        
        # Prepare data if not already done
        if 'ctr' not in df.columns:
            df, _ = self.data_prep.prepare(df)
        
        # Assign funnels
        df = self.assign_funnels(df, objective_col)
        
        # Train each funnel
        all_results = {}
        for funnel_key in self.FUNNEL_CONFIGS.keys():
            funnel_df = df[df['_funnel'] == funnel_key]
            
            if len(funnel_df) < self.min_samples_per_funnel:
                logger.warning(f"Skipping {funnel_key}: only {len(funnel_df)} samples")
                all_results[funnel_key] = {
                    'error': f'Insufficient samples ({len(funnel_df)})',
                    'funnel_name': self.FUNNEL_CONFIGS[funnel_key].name
                }
                continue
            
            result = self.fit_funnel(funnel_df, funnel_key)
            all_results[funnel_key] = result
            
            if 'error' not in result:
                logger.info(f"{funnel_key}: R²={result['metrics']['r2_score']:.3f}, n={result['sample_size']}")
        
        # Build summary
        summary = {
            'total_rows': len(df),
            'funnel_distribution': df['_funnel'].value_counts().to_dict(),
            'models_trained': sum(1 for r in all_results.values() if 'error' not in r),
            'overall_recommendations': self._generate_overall_recommendations(all_results)
        }
        
        return {
            'success': True,
            'funnels': all_results,
            'summary': summary
        }
    
    def _generate_overall_recommendations(self, results: Dict) -> List[str]:
        """Generate cross-funnel recommendations."""
        recs = []
        
        # Compare R² across funnels
        r2_scores = {
            k: v['metrics']['r2_score']
            for k, v in results.items()
            if 'metrics' in v
        }
        
        if r2_scores:
            best_funnel = max(r2_scores, key=r2_scores.get)
            worst_funnel = min(r2_scores, key=r2_scores.get)
            
            if r2_scores[best_funnel] - r2_scores[worst_funnel] > 0.2:
                recs.append(f"Focus analytics on {best_funnel} funnel - highest predictability")
        
        # Check for funnel imbalance
        for k, v in results.items():
            if 'error' in v and 'Insufficient' in v['error']:
                recs.append(f"Collect more data for {k} funnel campaigns")
        
        return recs
    
    def predict(
        self,
        df: pd.DataFrame,
        objective_col: str = 'objective'
    ) -> pd.DataFrame:
        """
        Generate predictions using trained funnel models.
        """
        if not self.models:
            raise ValueError("No models trained. Call fit_all() first.")
        
        df = df.copy()
        df = self.assign_funnels(df, objective_col)
        df['predicted'] = np.nan
        
        for funnel_key, model in self.models.items():
            if funnel_key not in self.results:
                continue
                
            mask = df['_funnel'] == funnel_key
            if mask.sum() == 0:
                continue
            
            feature_cols = self.results[funnel_key]['features']
            X = df.loc[mask, feature_cols].fillna(0)
            
            if self.enable_scaling and funnel_key in self.scalers:
                X = pd.DataFrame(
                    self.scalers[funnel_key].transform(X),
                    columns=feature_cols,
                    index=X.index
                )
            
            df.loc[mask, 'predicted'] = model.predict(X)
        
        return df


# Convenience function
def get_funnel_regression_engine(**kwargs) -> FunnelRegressionEngine:
    """Create a FunnelRegressionEngine instance."""
    return FunnelRegressionEngine(**kwargs)

"""
Campaign-Specific Feature Engineering

Critical additions for marketing campaign regression:
- Time-series features (lag, rolling averages, temporal splits)
- Budget pacing & spend patterns
- Learning phase detection
- Creative fatigue metrics
- Cross-campaign features
- Leakage detection
- Zero conversion handling
- Model ensemble
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from loguru import logger
from datetime import datetime, timedelta


# =============================================================================
# 1. TEMPORAL FEATURES & VALIDATION
# =============================================================================

class TemporalFeatureEngine:
    """
    Time-series features for campaign data.
    
    Features:
    - Lag features (yesterday's CTR, etc.)
    - Rolling averages (7-day, 30-day)
    - Day-over-day changes
    - Campaign age bins
    """
    
    @staticmethod
    def add_lag_features(
        df: pd.DataFrame,
        metrics: List[str] = ['ctr', 'cpm', 'conversions', 'roas'],
        lags: List[int] = [1, 7],
        group_col: str = 'campaign'
    ) -> pd.DataFrame:
        """Add lag features (previous day values)."""
        df = df.copy()
        
        for metric in metrics:
            if metric not in df.columns:
                continue
            
            for lag in lags:
                col_name = f'{metric}_lag_{lag}d'
                if group_col in df.columns:
                    df[col_name] = df.groupby(group_col)[metric].shift(lag)
                else:
                    df[col_name] = df[metric].shift(lag)
        
        return df
    
    @staticmethod
    def add_rolling_features(
        df: pd.DataFrame,
        metrics: List[str] = ['ctr', 'conversions', 'spend'],
        windows: List[int] = [7, 30],
        group_col: str = 'campaign'
    ) -> pd.DataFrame:
        """Add rolling average features."""
        df = df.copy()
        
        for metric in metrics:
            if metric not in df.columns:
                continue
            
            for window in windows:
                col_name = f'{metric}_rolling_{window}d_avg'
                if group_col in df.columns:
                    df[col_name] = df.groupby(group_col)[metric].transform(
                        lambda x: x.rolling(window, min_periods=1).mean()
                    )
                else:
                    df[col_name] = df[metric].rolling(window, min_periods=1).mean()
        
        return df
    
    @staticmethod
    def add_delta_features(
        df: pd.DataFrame,
        metrics: List[str] = ['spend', 'ctr', 'impressions'],
        group_col: str = 'campaign'
    ) -> pd.DataFrame:
        """Add day-over-day change features."""
        df = df.copy()
        
        for metric in metrics:
            if metric not in df.columns:
                continue
            
            col_name = f'{metric}_delta_1d'
            if group_col in df.columns:
                df[col_name] = df.groupby(group_col)[metric].diff()
            else:
                df[col_name] = df[metric].diff()
            
            # Percentage change
            pct_col = f'{metric}_pct_change_1d'
            df[pct_col] = df[col_name] / (df[metric].shift(1) + 1e-9) * 100
        
        return df
    
    @staticmethod
    def add_campaign_age_features(
        df: pd.DataFrame,
        date_col: str = 'date',
        campaign_col: str = 'campaign'
    ) -> pd.DataFrame:
        """Add campaign age bins."""
        df = df.copy()
        
        if date_col not in df.columns:
            return df
        
        df[date_col] = pd.to_datetime(df[date_col])
        
        if campaign_col in df.columns:
            # Days since campaign start
            campaign_start = df.groupby(campaign_col)[date_col].transform('min')
            df['campaign_age_days'] = (df[date_col] - campaign_start).dt.days
        else:
            df['campaign_age_days'] = (df[date_col] - df[date_col].min()).dt.days
        
        # Age bins
        df['campaign_phase'] = pd.cut(
            df['campaign_age_days'],
            bins=[-1, 7, 30, 90, float('inf')],
            labels=['learning', 'optimization', 'mature', 'long_running']
        )
        
        return df


def temporal_train_test_split(
    df: pd.DataFrame,
    date_col: str = 'date',
    train_ratio: float = 0.7,
    val_ratio: float = 0.15
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Chronological train/validation/test split.
    
    Example: Train on weeks 1-12, validate on 13-14, test on 15-16
    """
    df = df.sort_values(date_col).copy()
    n = len(df)
    
    train_end = int(n * train_ratio)
    val_end = int(n * (train_ratio + val_ratio))
    
    train = df.iloc[:train_end]
    val = df.iloc[train_end:val_end]
    test = df.iloc[val_end:]
    
    logger.info(f"Temporal split: train={len(train)}, val={len(val)}, test={len(test)}")
    
    return train, val, test


# =============================================================================
# 2. BUDGET PACING FEATURES
# =============================================================================

class BudgetPacingFeatures:
    """Budget utilization and pacing features."""
    
    @staticmethod
    def add_features(
        df: pd.DataFrame,
        spend_col: str = 'spend',
        budget_col: str = 'budget',
        date_col: str = 'date',
        campaign_col: str = 'campaign'
    ) -> pd.DataFrame:
        """Add budget pacing features."""
        df = df.copy()
        
        # Budget utilization
        if budget_col in df.columns and spend_col in df.columns:
            if campaign_col in df.columns:
                df['cumulative_spend'] = df.groupby(campaign_col)[spend_col].cumsum()
            else:
                df['cumulative_spend'] = df[spend_col].cumsum()
            
            df['budget_utilization_pct'] = (df['cumulative_spend'] / (df[budget_col] + 1e-9)) * 100
        
        # Spend velocity
        if spend_col in df.columns:
            if campaign_col in df.columns:
                avg_spend = df.groupby(campaign_col)[spend_col].transform('mean')
            else:
                avg_spend = df[spend_col].mean()
            
            df['spend_velocity'] = df[spend_col] / (avg_spend + 1e-9)
        
        # End of month flag
        if date_col in df.columns:
            df[date_col] = pd.to_datetime(df[date_col])
            df['is_end_of_month'] = df[date_col].dt.day >= 25
            df['day_of_month'] = df[date_col].dt.day
        
        return df


# =============================================================================
# 3. LEARNING PHASE DETECTION
# =============================================================================

class LearningPhaseDetector:
    """
    Detect platform learning phase.
    
    Learning phase typically:
    - First 50 conversions
    - First 7 days
    - After significant edit
    """
    
    LEARNING_CONVERSIONS = 50
    LEARNING_DAYS = 7
    
    @staticmethod
    def add_features(
        df: pd.DataFrame,
        conversions_col: str = 'conversions',
        date_col: str = 'date',
        campaign_col: str = 'campaign'
    ) -> pd.DataFrame:
        """Add learning phase features."""
        df = df.copy()
        
        if campaign_col not in df.columns:
            return df
        
        # Cumulative conversions
        if conversions_col in df.columns:
            df['cumulative_conversions'] = df.groupby(campaign_col)[conversions_col].cumsum()
            df['is_pre_50_conversions'] = df['cumulative_conversions'] < LearningPhaseDetector.LEARNING_CONVERSIONS
        
        # Days since start
        if date_col in df.columns:
            df[date_col] = pd.to_datetime(df[date_col])
            start_date = df.groupby(campaign_col)[date_col].transform('min')
            df['days_since_start'] = (df[date_col] - start_date).dt.days
            df['is_first_7_days'] = df['days_since_start'] < LearningPhaseDetector.LEARNING_DAYS
        
        # Combined learning flag
        if 'is_pre_50_conversions' in df.columns and 'is_first_7_days' in df.columns:
            df['is_in_learning_phase'] = df['is_pre_50_conversions'] | df['is_first_7_days']
        elif 'is_first_7_days' in df.columns:
            df['is_in_learning_phase'] = df['is_first_7_days']
        
        return df


# =============================================================================
# 4. CREATIVE FATIGUE
# =============================================================================

class CreativeFatigueFeatures:
    """Track creative fatigue indicators."""
    
    @staticmethod
    def add_features(
        df: pd.DataFrame,
        creative_col: str = 'creative_id',
        impressions_col: str = 'impressions',
        date_col: str = 'date'
    ) -> pd.DataFrame:
        """Add creative fatigue features."""
        df = df.copy()
        
        if creative_col not in df.columns:
            return df
        
        if date_col in df.columns:
            df[date_col] = pd.to_datetime(df[date_col])
            
            # Creative age
            creative_launch = df.groupby(creative_col)[date_col].transform('min')
            df['creative_age_days'] = (df[date_col] - creative_launch).dt.days
        
        if impressions_col in df.columns:
            # Cumulative impressions per creative
            df['creative_impressions_cumulative'] = df.groupby(creative_col)[impressions_col].cumsum()
        
        return df


# =============================================================================
# 5. DATA LEAKAGE DETECTION
# =============================================================================

class LeakageDetector:
    """Detect potential data leakage in features."""
    
    HIGH_RISK_FEATURES = [
        'conversion_rate', 'cost_per_conversion', 'cpa', 'cpc_actual',
        'roas', 'return_on_ad_spend', 'revenue', 'total_revenue'
    ]
    
    CORRELATION_THRESHOLD = 0.95
    
    @staticmethod
    def check(
        df: pd.DataFrame,
        target_col: str,
        feature_cols: List[str]
    ) -> Dict[str, Any]:
        """Check for potential leakage."""
        warnings = []
        flagged_features = []
        
        # Check known leaky features
        for feat in feature_cols:
            if feat.lower() in [f.lower() for f in LeakageDetector.HIGH_RISK_FEATURES]:
                warnings.append({
                    'feature': feat,
                    'reason': 'Known high-risk feature',
                    'action': 'Remove or review carefully'
                })
                flagged_features.append(feat)
        
        # Check high correlation
        if target_col in df.columns:
            for feat in feature_cols:
                if feat in df.columns and feat not in flagged_features:
                    try:
                        corr = df[feat].corr(df[target_col])
                        if abs(corr) > LeakageDetector.CORRELATION_THRESHOLD:
                            warnings.append({
                                'feature': feat,
                                'correlation': corr,
                                'reason': f'Very high correlation ({corr:.2f}) with target',
                                'action': 'Likely leakage - remove'
                            })
                            flagged_features.append(feat)
                    except:
                        pass
        
        return {
            'has_leakage': len(flagged_features) > 0,
            'flagged_features': flagged_features,
            'warnings': warnings,
            'safe_features': [f for f in feature_cols if f not in flagged_features]
        }


# =============================================================================
# 6. ZERO CONVERSION HANDLING
# =============================================================================

class ZeroConversionHandler:
    """Handle campaigns with zero conversions."""
    
    @staticmethod
    def analyze(
        df: pd.DataFrame,
        conversions_col: str = 'conversions'
    ) -> Dict[str, Any]:
        """Analyze zero conversion distribution."""
        if conversions_col not in df.columns:
            return {'status': 'column_not_found'}
        
        zero_pct = (df[conversions_col] == 0).mean()
        
        if zero_pct > 0.30:
            recommendation = 'two_stage_model'
            description = 'Use two-stage: 1) Classification (will convert?), 2) Regression (how many?)'
        elif zero_pct > 0.10:
            recommendation = 'zero_inflated'
            description = 'Consider Zero-Inflated Poisson or log(x+1) transform'
        else:
            recommendation = 'log_transform'
            description = 'Use log(conversions + 1) transformation'
        
        return {
            'zero_pct': zero_pct,
            'recommendation': recommendation,
            'description': description,
            'total_rows': len(df),
            'zero_rows': int((df[conversions_col] == 0).sum())
        }
    
    @staticmethod
    def apply_transform(
        df: pd.DataFrame,
        conversions_col: str = 'conversions',
        method: str = 'log'
    ) -> pd.DataFrame:
        """Apply transformation to handle zeros."""
        df = df.copy()
        
        if conversions_col not in df.columns:
            return df
        
        if method == 'log':
            df[f'{conversions_col}_transformed'] = np.log1p(df[conversions_col])
        elif method == 'sqrt':
            df[f'{conversions_col}_transformed'] = np.sqrt(df[conversions_col])
        elif method == 'binary':
            df['has_conversions'] = (df[conversions_col] > 0).astype(int)
        
        return df


# =============================================================================
# 7. PLATFORM QUIRKS
# =============================================================================

class PlatformQuirksHandler:
    """Handle platform-specific data quirks."""
    
    PLATFORM_QUIRKS = {
        'meta': {
            'estimated_impressions': True,
            'typical_ctr': (0.5, 2.0),
            'quality_score': 4
        },
        'google': {
            'search_display_mixed': True,
            'typical_ctr': (1.0, 5.0),
            'quality_score': 5
        },
        'dv360': {
            'viewable_vs_served': True,
            'typical_ctr': (0.1, 0.5),
            'quality_score': 3
        },
        'linkedin': {
            'low_volume': True,
            'typical_ctr': (0.3, 0.8),
            'quality_score': 4
        },
        'snapchat': {
            'swipe_ups': True,
            'typical_ctr': (0.3, 1.0),
            'quality_score': 3
        }
    }
    
    @staticmethod
    def add_quality_scores(
        df: pd.DataFrame,
        platform_col: str = 'platform'
    ) -> pd.DataFrame:
        """Add platform metric quality scores."""
        df = df.copy()
        
        if platform_col not in df.columns:
            return df
        
        def get_quality(platform):
            platform_lower = str(platform).lower()
            for key, quirks in PlatformQuirksHandler.PLATFORM_QUIRKS.items():
                if key in platform_lower:
                    return quirks['quality_score']
            return 3  # Default
        
        df['platform_quality_score'] = df[platform_col].apply(get_quality)
        
        return df


# =============================================================================
# 8. MODEL ENSEMBLE
# =============================================================================

@dataclass
class EnsembleConfig:
    """Configuration for model ensemble."""
    xgboost_weight: float = 0.4
    elastic_net_weight: float = 0.3
    random_forest_weight: float = 0.2
    ridge_weight: float = 0.1


class ModelEnsemble:
    """
    Weighted ensemble of multiple models.
    
    Default weights:
    - 40% XGBoost (complex patterns)
    - 30% Elastic Net (stability)
    - 20% Random Forest (outlier handling)
    - 10% Ridge (interpretability)
    """
    
    def __init__(self, config: Optional[EnsembleConfig] = None):
        self.config = config or EnsembleConfig()
        self.models = {}
        self.weights = {}
    
    def fit(
        self,
        X_train: pd.DataFrame,
        y_train: pd.Series,
        X_val: Optional[pd.DataFrame] = None,
        y_val: Optional[pd.Series] = None
    ):
        """Fit all ensemble models."""
        from sklearn.linear_model import Ridge, ElasticNet
        from sklearn.ensemble import RandomForestRegressor
        
        X_clean = X_train.fillna(0)
        y_clean = y_train.fillna(0)
        
        # Ridge
        self.models['ridge'] = Ridge(alpha=1.0)
        self.models['ridge'].fit(X_clean, y_clean)
        self.weights['ridge'] = self.config.ridge_weight
        
        # Elastic Net
        self.models['elastic_net'] = ElasticNet(alpha=0.1, l1_ratio=0.5)
        self.models['elastic_net'].fit(X_clean, y_clean)
        self.weights['elastic_net'] = self.config.elastic_net_weight
        
        # Random Forest
        self.models['random_forest'] = RandomForestRegressor(n_estimators=100, random_state=42)
        self.models['random_forest'].fit(X_clean, y_clean)
        self.weights['random_forest'] = self.config.random_forest_weight
        
        # XGBoost
        try:
            from xgboost import XGBRegressor
            self.models['xgboost'] = XGBRegressor(n_estimators=100, learning_rate=0.1, verbosity=0)
            self.models['xgboost'].fit(X_clean, y_clean)
            self.weights['xgboost'] = self.config.xgboost_weight
        except ImportError:
            # Redistribute weight
            self.weights['random_forest'] += self.config.xgboost_weight / 2
            self.weights['elastic_net'] += self.config.xgboost_weight / 2
        
        # Adjust weights if validation set provided
        if X_val is not None and y_val is not None:
            self._optimize_weights(X_val, y_val)
        
        logger.info(f"Ensemble fitted with models: {list(self.models.keys())}")
    
    def _optimize_weights(self, X_val: pd.DataFrame, y_val: pd.Series):
        """Optimize weights based on validation performance."""
        from sklearn.metrics import r2_score
        
        scores = {}
        X_clean = X_val.fillna(0)
        y_clean = y_val.fillna(0)
        
        for name, model in self.models.items():
            pred = model.predict(X_clean)
            scores[name] = max(0, r2_score(y_clean, pred))
        
        total = sum(scores.values()) or 1
        for name in self.weights:
            if name in scores:
                self.weights[name] = scores[name] / total
        
        logger.debug(f"Optimized weights: {self.weights}")
    
    def predict(self, X: pd.DataFrame) -> np.ndarray:
        """Make weighted ensemble prediction."""
        X_clean = X.fillna(0)
        
        predictions = np.zeros(len(X))
        total_weight = sum(self.weights.values())
        
        for name, model in self.models.items():
            weight = self.weights.get(name, 0) / total_weight
            predictions += weight * model.predict(X_clean)
        
        return predictions


# =============================================================================
# INTEGRATED FEATURE PIPELINE
# =============================================================================

class CampaignFeaturePipeline:
    """
    Complete campaign feature engineering pipeline.
    
    Combines all campaign-specific features.
    """
    
    def __init__(
        self,
        add_temporal: bool = True,
        add_budget: bool = True,
        add_learning_phase: bool = True,
        add_creative: bool = False,
        check_leakage: bool = True
    ):
        self.add_temporal = add_temporal
        self.add_budget = add_budget
        self.add_learning_phase = add_learning_phase
        self.add_creative = add_creative
        self.check_leakage = check_leakage
        
        self.temporal_engine = TemporalFeatureEngine()
        self.leakage_detector = LeakageDetector()
    
    def transform(
        self,
        df: pd.DataFrame,
        target_col: str,
        feature_cols: List[str]
    ) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """Apply all feature engineering."""
        metadata = {'features_added': [], 'warnings': []}
        
        # Temporal features
        if self.add_temporal:
            df = self.temporal_engine.add_lag_features(df)
            df = self.temporal_engine.add_rolling_features(df)
            df = self.temporal_engine.add_delta_features(df)
            df = self.temporal_engine.add_campaign_age_features(df)
            metadata['features_added'].extend(['lag', 'rolling', 'delta', 'campaign_age'])
        
        # Budget pacing
        if self.add_budget:
            df = BudgetPacingFeatures.add_features(df)
            metadata['features_added'].append('budget_pacing')
        
        # Learning phase
        if self.add_learning_phase:
            df = LearningPhaseDetector.add_features(df)
            metadata['features_added'].append('learning_phase')
        
        # Creative fatigue
        if self.add_creative:
            df = CreativeFatigueFeatures.add_features(df)
            metadata['features_added'].append('creative_fatigue')
        
        # Platform quality
        df = PlatformQuirksHandler.add_quality_scores(df)
        metadata['features_added'].append('platform_quality')
        
        # Leakage check
        if self.check_leakage:
            leakage_result = self.leakage_detector.check(df, target_col, feature_cols)
            metadata['leakage_check'] = leakage_result
            if leakage_result['has_leakage']:
                metadata['warnings'].append(f"Potential leakage: {leakage_result['flagged_features']}")
        
        # Zero conversion analysis
        zero_analysis = ZeroConversionHandler.analyze(df)
        metadata['zero_conversion_analysis'] = zero_analysis
        
        return df, metadata

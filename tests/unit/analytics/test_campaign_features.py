"""
Unit tests for Campaign Feature Engineering

Tests:
- TemporalFeatureEngine
- BudgetPacingFeatures
- LearningPhaseDetector
- LeakageDetector
- ZeroConversionHandler
- ModelEnsemble
- CampaignFeaturePipeline
"""

import pytest
import pandas as pd
import numpy as np


# =============================================================================
# FIXTURES
# =============================================================================

@pytest.fixture
def campaign_data():
    """Generate campaign data for testing."""
    np.random.seed(42)
    n = 100
    
    return pd.DataFrame({
        'date': pd.date_range('2024-01-01', periods=n),
        'campaign': ['camp_A'] * 50 + ['camp_B'] * 50,
        'spend': np.random.uniform(100, 1000, n),
        'impressions': np.random.uniform(10000, 100000, n),
        'clicks': np.random.uniform(100, 1000, n),
        'conversions': np.random.randint(0, 20, n),
        'ctr': np.random.uniform(0.5, 3.0, n),
        'platform': np.random.choice(['meta', 'google', 'dv360'], n),
        'budget': np.random.uniform(5000, 10000, n)
    })


# =============================================================================
# TEMPORAL FEATURES TESTS
# =============================================================================

class TestTemporalFeatureEngine:
    """Tests for TemporalFeatureEngine."""
    
    def test_lag_features(self, campaign_data):
        """Test lag feature generation."""
        from src.engine.analytics.campaign_features import TemporalFeatureEngine
        
        engine = TemporalFeatureEngine()
        result = engine.add_lag_features(campaign_data, metrics=['ctr'], lags=[1, 7])
        
        assert 'ctr_lag_1d' in result.columns
        assert 'ctr_lag_7d' in result.columns
    
    def test_rolling_features(self, campaign_data):
        """Test rolling average feature generation."""
        from src.engine.analytics.campaign_features import TemporalFeatureEngine
        
        engine = TemporalFeatureEngine()
        result = engine.add_rolling_features(campaign_data, metrics=['spend'], windows=[7])
        
        assert 'spend_rolling_7d_avg' in result.columns
    
    def test_delta_features(self, campaign_data):
        """Test day-over-day change features."""
        from src.engine.analytics.campaign_features import TemporalFeatureEngine
        
        engine = TemporalFeatureEngine()
        result = engine.add_delta_features(campaign_data, metrics=['spend'])
        
        assert 'spend_delta_1d' in result.columns
        assert 'spend_pct_change_1d' in result.columns
    
    def test_campaign_age_features(self, campaign_data):
        """Test campaign age binning."""
        from src.engine.analytics.campaign_features import TemporalFeatureEngine
        
        engine = TemporalFeatureEngine()
        result = engine.add_campaign_age_features(campaign_data)
        
        assert 'campaign_age_days' in result.columns
        assert 'campaign_phase' in result.columns
        
        # Check age phases exist
        phases = result['campaign_phase'].unique()
        assert len(phases) > 0


# =============================================================================
# TEMPORAL SPLIT TESTS
# =============================================================================

class TestTemporalSplit:
    """Tests for temporal train/test split."""
    
    def test_temporal_split_sizes(self, campaign_data):
        """Test temporal split produces correct sizes."""
        from src.engine.analytics.campaign_features import temporal_train_test_split
        
        train, val, test = temporal_train_test_split(campaign_data, train_ratio=0.7, val_ratio=0.15)
        
        assert len(train) == 70
        assert len(val) == 15
        assert len(test) == 15
    
    def test_temporal_split_chronological(self, campaign_data):
        """Test temporal split is chronological."""
        from src.engine.analytics.campaign_features import temporal_train_test_split
        
        train, val, test = temporal_train_test_split(campaign_data)
        
        # Train should end before val starts
        assert train['date'].max() <= val['date'].min()
        # Val should end before test starts
        assert val['date'].max() <= test['date'].min()


# =============================================================================
# BUDGET PACING TESTS
# =============================================================================

class TestBudgetPacingFeatures:
    """Tests for BudgetPacingFeatures."""
    
    def test_budget_utilization(self, campaign_data):
        """Test budget utilization calculation."""
        from src.engine.analytics.campaign_features import BudgetPacingFeatures
        
        result = BudgetPacingFeatures.add_features(campaign_data)
        
        assert 'cumulative_spend' in result.columns
        assert 'budget_utilization_pct' in result.columns
    
    def test_spend_velocity(self, campaign_data):
        """Test spend velocity calculation."""
        from src.engine.analytics.campaign_features import BudgetPacingFeatures
        
        result = BudgetPacingFeatures.add_features(campaign_data)
        
        assert 'spend_velocity' in result.columns
    
    def test_end_of_month_flag(self, campaign_data):
        """Test end of month flag."""
        from src.engine.analytics.campaign_features import BudgetPacingFeatures
        
        result = BudgetPacingFeatures.add_features(campaign_data)
        
        assert 'is_end_of_month' in result.columns
        assert result['is_end_of_month'].dtype == bool


# =============================================================================
# LEARNING PHASE TESTS
# =============================================================================

class TestLearningPhaseDetector:
    """Tests for LearningPhaseDetector."""
    
    def test_learning_phase_detection(self, campaign_data):
        """Test learning phase flag is added."""
        from src.engine.analytics.campaign_features import LearningPhaseDetector
        
        result = LearningPhaseDetector.add_features(campaign_data)
        
        assert 'is_in_learning_phase' in result.columns
    
    def test_cumulative_conversions(self, campaign_data):
        """Test cumulative conversions calculation."""
        from src.engine.analytics.campaign_features import LearningPhaseDetector
        
        result = LearningPhaseDetector.add_features(campaign_data)
        
        assert 'cumulative_conversions' in result.columns
        # Cumulative should increase
        assert result['cumulative_conversions'].iloc[-1] >= result['cumulative_conversions'].iloc[0]


# =============================================================================
# LEAKAGE DETECTION TESTS
# =============================================================================

class TestLeakageDetector:
    """Tests for LeakageDetector."""
    
    def test_detects_known_leaky_features(self, campaign_data):
        """Test detector flags known leaky features."""
        from src.engine.analytics.campaign_features import LeakageDetector
        
        campaign_data['conversion_rate'] = campaign_data['conversions'] / (campaign_data['clicks'] + 1)
        
        result = LeakageDetector.check(
            campaign_data, 
            'conversions', 
            ['spend', 'clicks', 'conversion_rate']
        )
        
        assert result['has_leakage'] == True
        assert 'conversion_rate' in result['flagged_features']
    
    def test_safe_features_returned(self, campaign_data):
        """Test safe features list is returned."""
        from src.engine.analytics.campaign_features import LeakageDetector
        
        result = LeakageDetector.check(
            campaign_data, 
            'conversions', 
            ['spend', 'clicks']
        )
        
        assert 'safe_features' in result
        assert 'spend' in result['safe_features'] or 'clicks' in result['safe_features']


# =============================================================================
# ZERO CONVERSION HANDLER TESTS
# =============================================================================

class TestZeroConversionHandler:
    """Tests for ZeroConversionHandler."""
    
    def test_analyze_zero_distribution(self, campaign_data):
        """Test zero conversion analysis."""
        from src.engine.analytics.campaign_features import ZeroConversionHandler
        
        result = ZeroConversionHandler.analyze(campaign_data)
        
        assert 'zero_pct' in result
        assert 'recommendation' in result
        assert result['recommendation'] in ['log_transform', 'zero_inflated', 'two_stage_model']
    
    def test_log_transform(self, campaign_data):
        """Test log transformation."""
        from src.engine.analytics.campaign_features import ZeroConversionHandler
        
        result = ZeroConversionHandler.apply_transform(campaign_data, method='log')
        
        assert 'conversions_transformed' in result.columns
    
    def test_binary_transform(self, campaign_data):
        """Test binary transformation."""
        from src.engine.analytics.campaign_features import ZeroConversionHandler
        
        result = ZeroConversionHandler.apply_transform(campaign_data, method='binary')
        
        assert 'has_conversions' in result.columns


# =============================================================================
# MODEL ENSEMBLE TESTS
# =============================================================================

class TestModelEnsemble:
    """Tests for ModelEnsemble."""
    
    def test_ensemble_fit(self, campaign_data):
        """Test ensemble fitting."""
        from src.engine.analytics.campaign_features import ModelEnsemble
        
        X = campaign_data[['spend', 'impressions', 'clicks']]
        y = campaign_data['conversions']
        
        ensemble = ModelEnsemble()
        ensemble.fit(X, y)
        
        assert len(ensemble.models) > 0
    
    def test_ensemble_predict(self, campaign_data):
        """Test ensemble prediction."""
        from src.engine.analytics.campaign_features import ModelEnsemble
        
        X = campaign_data[['spend', 'impressions', 'clicks']]
        y = campaign_data['conversions']
        
        ensemble = ModelEnsemble()
        ensemble.fit(X, y)
        
        predictions = ensemble.predict(X)
        
        assert len(predictions) == len(X)
    
    def test_ensemble_weights_sum_to_one(self, campaign_data):
        """Test ensemble weights are normalized."""
        from src.engine.analytics.campaign_features import ModelEnsemble
        
        X = campaign_data[['spend', 'impressions', 'clicks']]
        y = campaign_data['conversions']
        
        ensemble = ModelEnsemble()
        ensemble.fit(X, y)
        
        # Weights should be approximately 1.0
        total_weight = sum(ensemble.weights.values())
        assert 0.99 <= total_weight <= 1.01


# =============================================================================
# FULL PIPELINE TESTS
# =============================================================================

class TestCampaignFeaturePipeline:
    """Tests for CampaignFeaturePipeline."""
    
    def test_pipeline_transform(self, campaign_data):
        """Test full pipeline transformation."""
        from src.engine.analytics.campaign_features import CampaignFeaturePipeline
        
        pipeline = CampaignFeaturePipeline()
        result, metadata = pipeline.transform(campaign_data, 'conversions', ['spend', 'clicks'])
        
        assert 'features_added' in metadata
        assert len(metadata['features_added']) > 0
    
    def test_pipeline_metadata(self, campaign_data):
        """Test pipeline returns useful metadata."""
        from src.engine.analytics.campaign_features import CampaignFeaturePipeline
        
        pipeline = CampaignFeaturePipeline()
        result, metadata = pipeline.transform(campaign_data, 'conversions', ['spend', 'clicks'])
        
        assert 'leakage_check' in metadata
        assert 'zero_conversion_analysis' in metadata


# =============================================================================
# RUN TESTS
# =============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])

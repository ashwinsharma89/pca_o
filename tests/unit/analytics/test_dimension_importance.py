"""
Unit tests for Dimension Importance Framework

Tests:
- DimensionImportanceFramework
- PermutationDimensionImportance
- Auto-detection of dimensions
- Effect size calculations
- Interaction analysis
"""

import pytest
import pandas as pd
import numpy as np


# =============================================================================
# FIXTURES
# =============================================================================

@pytest.fixture
def dimension_data():
    """Generate data with known dimension effects."""
    np.random.seed(42)
    n = 500
    
    df = pd.DataFrame({
        'platform': np.random.choice(['meta', 'google', 'dv360'], n, p=[0.4, 0.4, 0.2]),
        'objective': np.random.choice(['awareness', 'conversions', 'traffic'], n),
        'device': np.random.choice(['mobile', 'desktop', 'tablet'], n),
        'audience': np.random.choice(['broad', 'custom', 'lookalike'], n),
        'spend': np.random.uniform(100, 1000, n),
        'impressions': np.random.uniform(10000, 100000, n),
    })
    
    # Add target with known effects
    base = np.random.uniform(10, 50, n)
    platform_effect = df['platform'].map({'meta': 10, 'google': 15, 'dv360': 5})
    objective_effect = df['objective'].map({'conversions': 20, 'traffic': 10, 'awareness': 5})
    df['conversions'] = base + platform_effect + objective_effect + np.random.normal(0, 5, n)
    
    return df


@pytest.fixture
def minimal_dimension_data():
    """Minimal data for edge cases."""
    return pd.DataFrame({
        'platform': ['meta', 'google', 'meta', 'google'],
        'conversions': [10, 20, 15, 25]
    })


# =============================================================================
# DIMENSION IMPORTANCE FRAMEWORK TESTS
# =============================================================================

class TestDimensionImportanceFramework:
    """Tests for DimensionImportanceFramework."""
    
    def test_analyze_basic(self, dimension_data):
        """Test basic dimension analysis runs."""
        from src.engine.analytics.dimension_importance import DimensionImportanceFramework
        
        framework = DimensionImportanceFramework()
        report = framework.analyze(dimension_data, 'conversions')
        
        assert report.success == True
        assert len(report.results) > 0
    
    def test_detects_strong_dimensions(self, dimension_data):
        """Test framework identifies dimensions with strong effects."""
        from src.engine.analytics.dimension_importance import DimensionImportanceFramework
        
        framework = DimensionImportanceFramework()
        report = framework.analyze(dimension_data, 'conversions')
        
        # Objective should have large effect (we engineered it that way)
        objective_result = next((r for r in report.results if r.dimension == 'objective'), None)
        
        assert objective_result is not None
        assert objective_result.effect_interpretation in ['Large', 'Medium']
    
    def test_rankings_ordered_by_importance(self, dimension_data):
        """Test rankings are ordered by importance score."""
        from src.engine.analytics.dimension_importance import DimensionImportanceFramework
        
        framework = DimensionImportanceFramework()
        report = framework.analyze(dimension_data, 'conversions')
        
        if len(report.results) > 1:
            scores = [r.importance_score for r in report.results]
            assert scores == sorted(scores, reverse=True)
    
    def test_generates_recommendations(self, dimension_data):
        """Test framework generates recommendations."""
        from src.engine.analytics.dimension_importance import DimensionImportanceFramework
        
        framework = DimensionImportanceFramework()
        report = framework.analyze(dimension_data, 'conversions')
        
        assert len(report.recommendations) > 0
    
    def test_top_values_populated(self, dimension_data):
        """Test top values are populated for each dimension."""
        from src.engine.analytics.dimension_importance import DimensionImportanceFramework
        
        framework = DimensionImportanceFramework()
        report = framework.analyze(dimension_data, 'conversions')
        
        for result in report.results:
            assert len(result.top_values) > 0
            assert 'value' in result.top_values[0]
            assert 'mean' in result.top_values[0]
    
    def test_explicit_dimensions(self, dimension_data):
        """Test analysis with explicitly specified dimensions."""
        from src.engine.analytics.dimension_importance import DimensionImportanceFramework
        
        framework = DimensionImportanceFramework()
        report = framework.analyze(
            dimension_data, 
            'conversions', 
            dimension_cols=['platform', 'objective']
        )
        
        dimension_names = [r.dimension for r in report.results]
        
        assert 'platform' in dimension_names or 'objective' in dimension_names
    
    def test_missing_target_handled(self, dimension_data):
        """Test graceful handling of missing target column."""
        from src.engine.analytics.dimension_importance import DimensionImportanceFramework
        
        framework = DimensionImportanceFramework()
        report = framework.analyze(dimension_data, 'nonexistent_column')
        
        assert report.success == False


# =============================================================================
# PERMUTATION IMPORTANCE TESTS
# =============================================================================

class TestPermutationImportance:
    """Tests for PermutationDimensionImportance."""
    
    def test_permutation_basic(self, dimension_data):
        """Test permutation importance calculation."""
        from src.engine.analytics.dimension_importance import PermutationDimensionImportance
        
        perm = PermutationDimensionImportance(n_repeats=3)
        result = perm.calculate(
            dimension_data, 
            'conversions', 
            ['platform', 'objective']
        )
        
        assert not result.empty
        assert 'dimension' in result.columns
        assert 'importance_mean' in result.columns
    
    def test_permutation_ranking(self, dimension_data):
        """Test permutation importance produces sensible rankings."""
        from src.engine.analytics.dimension_importance import PermutationDimensionImportance
        
        perm = PermutationDimensionImportance(n_repeats=5)
        result = perm.calculate(
            dimension_data, 
            'conversions', 
            ['platform', 'objective', 'device']
        )
        
        if not result.empty:
            # Importance values should be non-negative (mostly)
            assert result['importance_mean'].min() >= -0.5  # Allow small negative due to noise


# =============================================================================
# AUTO-DETECTION TESTS
# =============================================================================

class TestDimensionAutoDetection:
    """Tests for dimension auto-detection."""
    
    def test_detects_common_dimension_names(self, dimension_data):
        """Test framework detects common dimension column names."""
        from src.engine.analytics.dimension_importance import DimensionImportanceFramework
        
        framework = DimensionImportanceFramework()
        detected = framework._detect_dimensions(dimension_data)
        
        # Should detect platform, objective, etc.
        assert 'platform' in detected or 'objective' in detected
    
    def test_detects_categorical_columns(self):
        """Test framework detects categorical columns by type."""
        from src.engine.analytics.dimension_importance import DimensionImportanceFramework
        
        df = pd.DataFrame({
            'category_col': ['A', 'B', 'C'] * 100,
            'numeric_col': range(300)
        })
        
        framework = DimensionImportanceFramework()
        detected = framework._detect_dimensions(df)
        
        assert 'category_col' in detected
        assert 'numeric_col' not in detected


# =============================================================================
# INTERACTION ANALYSIS TESTS
# =============================================================================

class TestInteractionAnalysis:
    """Tests for dimension interaction analysis."""
    
    def test_interaction_detection(self, dimension_data):
        """Test interaction analysis runs."""
        from src.engine.analytics.dimension_importance import DimensionImportanceFramework
        
        framework = DimensionImportanceFramework()
        report = framework.analyze(dimension_data, 'conversions')
        
        # May or may not find significant interactions
        assert 'interactions' in dir(report) or report.interactions is not None
    
    def test_interaction_format(self, dimension_data):
        """Test interaction results have expected format."""
        from src.engine.analytics.dimension_importance import DimensionImportanceFramework
        
        framework = DimensionImportanceFramework()
        report = framework.analyze(dimension_data, 'conversions')
        
        if report.interactions:
            inter = report.interactions[0]
            assert 'dimension_1' in inter
            assert 'dimension_2' in inter
            assert 'f_statistic' in inter


# =============================================================================
# CONVENIENCE FUNCTION TESTS
# =============================================================================

class TestConvenienceFunctions:
    """Tests for convenience functions."""
    
    def test_analyze_dimension_importance(self, dimension_data):
        """Test convenience function works."""
        from src.engine.analytics.dimension_importance import analyze_dimension_importance
        
        report = analyze_dimension_importance(dimension_data, 'conversions')
        
        assert report.success == True
    
    def test_get_dimension_rankings(self, dimension_data):
        """Test get_dimension_rankings returns DataFrame."""
        from src.engine.analytics.dimension_importance import get_dimension_rankings
        
        rankings = get_dimension_rankings(dimension_data, 'conversions')
        
        assert isinstance(rankings, pd.DataFrame)
        assert 'Dimension' in rankings.columns


# =============================================================================
# RUN TESTS
# =============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])

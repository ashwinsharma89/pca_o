import pytest
import pandas as pd
import numpy as np
from unittest.mock import MagicMock, patch
from src.engine.analytics.regression.shap_explainer import ShapExplainer

class TestShapExplainer:
    
    @patch("shap.TreeExplainer")
    def test_compute_tree_shap(self, MockTreeExplainer):
        """Test SHAP computation for Tree models (Random Forest/XGBoost)"""
        # Setup mock
        mock_explainer = MagicMock()
        # Create a mock object that simulates the shap_values ndarray
        mock_shap_values = np.array([[0.1, 0.2], [-0.1, 0.3]])
        
        # When explainer(X) is called, it returns shap_values
        mock_explainer.return_value = mock_shap_values
        MockTreeExplainer.return_value = mock_explainer
        
        # Mock Data
        X_bg = pd.DataFrame({'a': [1,2], 'b': [3,4]})
        X_ex = pd.DataFrame({'a': [1,2], 'b': [3,4]})
        model = MagicMock()
        
        # Execute
        result = ShapExplainer.compute_shap_values(
            model=model,
            X_background=X_bg,
            X_explain=X_ex,
            feature_names=['a', 'b'],
            model_type="tree"
        )
        
        # Verify Structure
        assert "summary" in result
        assert "raw_values" in result
        assert "base_value" in result
        
        # Verify Summary Calculation
        # Feature 'a': abs(0.1) + abs(-0.1) = 0.2 / 2 = 0.1
        # Feature 'b': abs(0.2) + abs(0.3) = 0.5 / 2 = 0.25
        # 'b' should be first (rank 1)
        
        assert len(result["summary"]) == 2
        assert result["summary"][0]["feature"] == "b"
        assert result["summary"][0]["mean_abs_shap"] == 0.25
        
        assert result["summary"][1]["feature"] == "a"
        assert result["summary"][1]["mean_abs_shap"] == 0.1

    @patch("shap.LinearExplainer")
    def test_compute_linear_shap(self, MockLinearExplainer):
        """Test SHAP computation for Linear models"""
        # LinearExplainer returns an Explanation object, not just array
        mock_explanation = MagicMock()
        mock_explanation.values = np.array([[0.5, 0.5], [0.5, 0.5]])
        mock_explanation.base_values = np.array([0.1, 0.2])
        
        mock_explainer = MagicMock()
        mock_explainer.return_value = mock_explanation
        MockLinearExplainer.return_value = mock_explainer
        
        # Mock Data
        X_bg = pd.DataFrame({'a': [1,2], 'b': [3,4]})
        X_ex = pd.DataFrame({'a': [1,2], 'b': [3,4]})
        model = MagicMock()
        
        # Execute
        result = ShapExplainer.compute_shap_values(
            model=model,
            X_background=X_bg,
            X_explain=X_ex,
            feature_names=['a', 'b'],
            model_type="linear"
        )
        
        assert "summary" in result
        # Both features have same importance (0.5), so order might vary but both present
        assert len(result["summary"]) == 2
        assert result["summary"][0]["mean_abs_shap"] == 0.5

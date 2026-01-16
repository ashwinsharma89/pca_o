import shap
import numpy as np
import pandas as pd
from typing import Dict, Any, List, Optional
from loguru import logger

class ShapExplainer:
    """Helper for SHAP (Shapley Additive Explanations) calculations."""
    
    @staticmethod
    def compute_shap_values(
        model: Any, 
        X_background: np.ndarray, 
        X_explain: np.ndarray,
        feature_names: List[str],
        model_type: str = "tree"
    ) -> Dict[str, Any]:
        """
        Compute SHAP values for model interpretation.
        
        Args:
            model: Trained model object (sklearn or xgboost)
            X_background: Background dataset (training sample)
            X_explain: Dataset to explain (test sample)
            feature_names: List of feature names
            model_type: 'tree' (RF/XGB) or 'linear' (Ridge/OLS/ElasticNet)
            
        Returns:
            Dict containing summary importance and raw values
        """
        try:
            logger.info(f"Calculating SHAP values (type={model_type})...")
            
            # Performance optimization: Limit samples
            # SHAP can be slow, so we explain a representative subset
            max_samples = 200
            if len(X_explain) > max_samples:
                X_explain_subset = X_explain[:max_samples]
            else:
                X_explain_subset = X_explain
                
            # Background samples (needed for Linear/Kernel)
            if len(X_background) > 100:
                X_background_subset = shap.utils.sample(X_background, 100)
            else:
                X_background_subset = X_background

            explainer = None
            shap_values = None
            
            if model_type == "tree":
                # TreeExplainer is fast and exact for trees
                # XGBoost and sklearn Random Forest are supported
                explainer = shap.TreeExplainer(model)
                shap_values = explainer.shap_values(X_explain_subset)
                
            elif model_type == "linear":
                # LinearExplainer for Ridge, OLS, ElasticNet
                explainer = shap.LinearExplainer(model, X_background_subset)
                shap_values = explainer.shap_values(X_explain_subset)
                
            else:
                logger.warning(f"SHAP not supported for model type: {model_type}")
                return {}

            # Handle SHAP output format inconsistencies
            # Some explainers return list (classification, multi-output), others array
            vals = shap_values
            if isinstance(shap_values, list):
                vals = shap_values[0] # Assume single output regression
                
            # Ensure native float types (avoid float32 JSON issues)
            vals = np.array(vals, dtype=float)
            
            # Calculate mean absolute SHAP (global feature importance)
            mean_abs_shap = np.mean(np.abs(vals), axis=0)
            
            # Create feature ranking
            summary_data = []
            for i, feat in enumerate(feature_names):
                summary_data.append({
                    "feature": feat,
                    "mean_abs_shap": float(mean_abs_shap[i])
                })
            
            # Sort by importance
            summary_data.sort(key=lambda x: x["mean_abs_shap"], reverse=True)
            
            base_value = 0.0
            if hasattr(explainer, 'expected_value'):
                # Handle scalar or array expected_value
                ev = explainer.expected_value
                if isinstance(ev, np.ndarray):
                    base_value = float(ev[0]) if len(ev) > 0 else 0.0
                else:
                    base_value = float(ev)

            return {
                "summary": summary_data,
                "raw_values": vals.tolist(), # Limited to max_samples
                "base_value": base_value,
                "feature_names": feature_names
            }
            
        except Exception as e:
            logger.error(f"SHAP calculation failed: {e}")
            # Return empty dict on failure so pipeline doesn't crash
            return {}

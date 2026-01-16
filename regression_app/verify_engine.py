
import pandas as pd
import numpy as np
import sys
import os

# Ensure we can import from the directory
sys.path.append(os.path.join(os.getcwd(), "regression_app", "regression_app"))
from engine.pipeline import RegressionPipeline

def test_pipeline():
    print("--- Starting Regression Engine Verification ---")
    
    # 1. Create Synthetic Data with Intentional Leakage
    np.random.seed(42)
    n_samples = 200
    
    # Feature 1: Spend
    spend = np.random.uniform(1000, 5000, n_samples)
    
    # Feature 2: Impressions (HIGHLY correlated with Spend - Variance Inflation)
    # Imp = Spend * 10 + Noise
    impressions = spend * 10 + np.random.normal(0, 100, n_samples)
    
    # Feature 3: Random Noise
    random_feat = np.random.normal(0, 1, n_samples)
    
    # Target: Conversions
    # Conversions = Spend * 0.05 + Noise
    conversions = spend * 0.05 + np.random.normal(0, 10, n_samples)
    
    df = pd.DataFrame({
        "spend": spend,
        "impressions": impressions,
        "random_feat": random_feat,
        "conversions": conversions
    })
    
    print("\nData Created. 'spend' and 'impressions' are collinear.")
    
    # 2. Run Pipeline
    print("Running RegressionPipeline...")
    pipeline = RegressionPipeline(models_to_run=["Ridge"], quick_mode=True)
    
    result = pipeline.run(
        df=df,
        target="conversions",
        features=["spend", "impressions", "random_feat"],
        test_size=0.2
    )
    
    # 3. Verify Results
    print(f"\nModel Selected: {result.best_model_name}")
    print(f"R2 Score: {result.metrics.r2_test:.4f}")
    
    # Check VIF Analysis
    print("\n--- VIF Analysis (Should detect multicollinearity) ---")
    print(result.vif_analysis['summary']['message'])
    for feat in result.vif_analysis['features']:
        print(f"Feature: {feat['feature']}, VIF: {feat['vif']}, Status: {feat['status']}")
        
    # Check Warnings
    has_high_vif = result.vif_analysis['summary']['status'] == 'High'
    if has_high_vif:
        print("\nSUCCESS: VIF Logic correctly detected multicollinearity.")
    else:
        print("\nFAILURE: VIF Logic failed to detect multicollinearity.")

if __name__ == "__main__":
    test_pipeline()

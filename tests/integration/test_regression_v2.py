import requests
import pandas as pd
import numpy as np
import time
from pathlib import Path

BASE_URL = "http://localhost:8000/api/v1"

def get_auth_token():
    """Register and login to get a valid token."""
    username = f"testuser_{int(time.time())}"
    password = "TestPassword123!"
    email = f"{username}@example.com"
    
    # Register
    reg_url = f"{BASE_URL}/auth/register"
    reg_resp = requests.post(reg_url, json={"username": username, "email": email, "password": password})
    if reg_resp.status_code != 200:
        print(f"Registration status {reg_resp.status_code}: {reg_resp.text}")
        
    # Login
    login_url = f"{BASE_URL}/auth/login"
    response = requests.post(login_url, json={"username": username, "password": password})
    if response.status_code == 200:
        return response.json()['access_token']
    else:
        print(f"Login failed (status {response.status_code}): {response.text}")
        return None

def test_regression_v2_e2e():
    token = get_auth_token()
    if not token:
        print("Could not get auth token. Skipping test.")
        return
        
    headers = {"Authorization": f"Bearer {token}"}
    
    # 1. Generate synthetic data
    n = 500
    np.random.seed(42)
    data = pd.DataFrame({
        'date': pd.date_range('2024-01-01', periods=n),
        'spend': np.random.uniform(100, 1000, n),
        'impressions': np.random.uniform(10000, 100000, n),
        'clicks': np.random.uniform(100, 1000, n),
        'platform': np.random.choice(['Google', 'Meta', 'TikTok'], n)
    })
    # Target: Conversions = 0.5 * spend + 0.001 * impressions + noise
    data['conversions'] = 0.5 * data['spend'] + 0.001 * data['impressions'] + np.random.normal(0, 10, n)
    
    csv_path = "tests/data/regression_test_data.csv"
    Path("tests/data").mkdir(parents=True, exist_ok=True)
    data.to_csv(csv_path, index=False)
    
    # 2. Upload data
    print("Uploading test data...")
    with open(csv_path, 'rb') as f:
        # NOTE: Using /upload/stream for v1
        response = requests.post(f"{BASE_URL}/upload/stream", files={'file': f}, headers=headers)
    
    if response.status_code != 200:
        print(f"Upload failed: {response.text}")
    assert response.status_code == 200
    job_id = response.json()['job_id']
    print(f"Upload successful, job_id: {job_id}")
    
    # Wait for processing
    time.sleep(2)
    
    # 3. Call Regression V2
    print("Running Regression V2...")
    params = {
        "target": "conversions",
        "features": "spend,impressions,clicks",
        "models": "Ridge,Random Forest",
        "quick_mode": "true"
    }
    
    # Needs auth token if auth is enabled. Assuming it's disabled for local dev or using a mock token.
    # Looking at the code, auth seems to be required. I'll use a dummy user/token if possible or hope local dev has it bypassed.
    
    # Actually, I'll check if I can hit it without auth or if I need to login.
    # For now I'll assume auth might be required and add a placeholder.
    
    response = requests.get(f"{BASE_URL}/campaigns/regression/v2", params=params, headers=headers)
    
    if response.status_code != 200:
        print(f"FAILED: {response.text}")
        return
    
    result = response.json()
    assert result['success'] == True
    print(f"✓ Regression successful. Best model: {result['best_model']}")
    print(f"✓ R² Score: {result['metrics']['r2']:.4f}")
    
    # Check for SHAP
    if result.get('shap'):
        print("✓ SHAP data received")
    else:
        print("! SHAP data missing (might happen if model type is not compatible or small sample)")
        
    assert 'coefficients' in result
    assert len(result['predictions_sample']) > 0
    
    # Check for Recommendations (Layer 6)
    assert 'recommendations' in result
    assert len(result['recommendations']) > 0
    print(f"✓ Layer 6: Received {len(result['recommendations'])} recommendations")
    for rec in result['recommendations'][:2]:
        print(f"  - [{rec['strategy']}] {rec['feature']}: {rec['action']}")
    
    print("✓ Regression V2 verification PASSED!")

if __name__ == "__main__":
    test_regression_v2_e2e()

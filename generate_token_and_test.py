import sys
import os
import requests
import json

# Add src to path
sys.path.append(os.getcwd())

from dotenv import load_dotenv
load_dotenv()

from src.interface.api.middleware.auth import create_access_token

def generate_token():
    # Create token for user 'ashwin'
    token_data = {"sub": "ashwin", "role": "admin", "tier": "enterprise"}
    token = create_access_token(data=token_data)
    return token

def test_endpoint(url, token, description):
    print(f"Testing {description} ({url})...")
    headers = {"Authorization": f"Bearer {token}"}
    try:
        response = requests.get(url, headers=headers)
        print(f"Status: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            keys = list(data.keys()) if isinstance(data, dict) else f"List[{len(data)}]"
            print(f"SUCCESS. Keys: {keys}")
            if "summary_groups" in data:
                print(f"summary_groups: {json.dumps(data['summary_groups'], indent=2)}")
        else:
            print(f"FAILURE: {response.text[:500]}")
    except Exception as e:
        print(f"ERROR: {e}")
    print("-" * 30)

if __name__ == "__main__":
    try:
        token = generate_token()
        print(f"Generated Token: {token[:20]}...")
        
        base_url = "http://127.0.0.1:8000/api/v1"
        
        # Test endpoints
        test_endpoint(f"{base_url}/campaigns/dimensions", token, "Dimensions (Visualizations)")
        test_endpoint(f"{base_url}/campaigns/dashboard-stats?platforms=Meta,Google", token, "Dashboard Stats (with params)")
        test_endpoint(f"{base_url}/campaigns/dashboard-stats", token, "Dashboard Stats (no params)")
        
    except Exception as e:
        print(f"Setup Error: {e}")

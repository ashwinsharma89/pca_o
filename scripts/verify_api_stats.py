
import requests
import json
import sys

# Login first to get token
login_url = "http://localhost:8000/api/v1/auth/login"
stats_url = "http://localhost:8000/api/v1/analytics/dashboard-stats"
login_data = {"username": "ashwin", "password": "password123"}

try:
    print(f"Logging in to {login_url}...")
    session = requests.Session()
    resp = session.post(login_url, json=login_data) # JSON for Pydantic model
    if resp.status_code != 200:
        print(f"Login failed: {resp.text}")
        sys.exit(1)
    
    token = resp.json().get("access_token")
    headers = {"Authorization": f"Bearer {token}"}
    
    print(f"Fetching stats from {stats_url}...")
    resp = session.get(stats_url, headers=headers)
    
    if resp.status_code == 200:
        print("✅ Success!")
        print(json.dumps(resp.json(), indent=2))
        
        # Check if values are zero
        data = resp.json()
        if data.get('total_spend', 0) == 0 and data.get('total_impressions', 0) == 0:
            print("⚠️ WARNING: Data returned but all values are ZERO. Column mapping issue likely.")
        else:
            print("🎉 Data looks good!")
    else:
        print(f"❌ Failed: {resp.status_code} - {resp.text}")

except Exception as e:
    print(f"❌ Error: {e}")

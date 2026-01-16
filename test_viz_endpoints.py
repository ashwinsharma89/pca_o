import requests
import sys

def test_endpoint(url, description):
    print(f"Testing {description} ({url})...")
    try:
        response = requests.get(url)
        print(f"Status: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            # print(f"Data keys: {data.keys() if isinstance(data, dict) else 'List'}")
            print("SUCCESS")
        else:
            print(f"FAILURE: {response.text[:200]}")
    except Exception as e:
        print(f"ERROR: {e}")
    print("-" * 30)

base_url = "http://127.0.0.1:8000/api/v1"

# Test endpoints used by Visualizations 2
test_endpoint(f"{base_url}/campaigns/dimensions", "Dimensions (Visualizations)")
test_endpoint(f"{base_url}/campaigns/dashboard-stats", "Dashboard Stats")
test_endpoint(f"{base_url}/campaigns/filters", "Filters")
test_endpoint(f"{base_url}/campaigns/schema", "Schema")

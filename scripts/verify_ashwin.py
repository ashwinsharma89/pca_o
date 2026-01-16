import requests
import json
import os

def verify():
    url = "http://localhost:8000/api/v1/user/me"
    payload = {
        "username": "ashwin",
        "password": "Pca12345!"
    }
    headers = {'Content-Type': 'application/json'}
    
    # 1. Login
    print(f"Logging in ashwin at http://localhost:8000/api/v1/auth/login...")
    login_url = "http://localhost:8000/api/v1/auth/login"
    try:
        response = requests.post(login_url, json=payload, headers=headers)
        if response.status_code == 200:
            print("✅ Login successful!")
            data = response.json()
            token = data.get("access_token")
            
            # 2. Get Me
            print(f"Verifying token with /me endpoint...")
            me_headers = {'Authorization': f'Bearer {token}'}
            me_response = requests.get("http://localhost:8000/api/v1/auth/me", headers=me_headers)
            if me_response.status_code == 200:
                print("✅ Token valid! User verified.")
                print(json.dumps(me_response.json(), indent=2))
                return True
            else:
                print(f"❌ /me failed with status {me_response.status_code}")
                print(me_response.text)
                return False
        else:
            print(f"❌ Login failed with status {response.status_code}")
            print(response.text)
            return False
    except Exception as e:
        print(f"❌ Connection error: {e}")
        return False

if __name__ == "__main__":
    verify()

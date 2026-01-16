import requests
import json
import os
from typing import Dict, Any

class PCAClient:
    """Example client for the PCA Agent API."""
    
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url
        self.token = None

    def authenticate(self, username: str, password: str):
        """Authenticate and store JWT token."""
        url = f"{self.base_url}/api/v1/auth/login"
        payload = {"username": username, "password": password}
        response = requests.post(url, data=payload)
        
        if response.status_code == 200:
            data = response.json()
            if data.get("mfa_required"):
                print("MFA Required! Please verify TOTP.")
                return data
            self.token = data.get("access_token")
            print("Successfully authenticated.")
            return data
        else:
            print(f"Authentication failed: {response.text}")
            return None

    def get_campaigns(self):
        """Get list of campaigns."""
        if not self.token:
            print("Not authenticated.")
            return None
        
        url = f"{self.base_url}/api/v1/campaigns"
        headers = {"Authorization": f"Bearer {self.token}"}
        response = requests.get(url, headers=headers)
        return response.json()

    def upload_data(self, file_path: str):
        """Upload campaign data file."""
        if not self.token:
            print("Not authenticated.")
            return None
            
        url = f"{self.base_url}/api/v1/campaigns/upload"
        headers = {"Authorization": f"Bearer {self.token}"}
        
        with open(file_path, 'rb') as f:
            files = {'file': (os.path.basename(file_path), f, 'text/csv')}
            response = requests.post(url, headers=headers, files=files)
            
        return response.json()

if __name__ == "__main__":
    client = PCAClient()
    # Usage:
    # client.authenticate("admin", "admin123")
    # campaigns = client.get_campaigns()
    # print(json.dumps(campaigns, indent=2))

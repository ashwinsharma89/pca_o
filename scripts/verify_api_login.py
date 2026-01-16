
import requests
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

URL = "http://localhost:8001/api/v1/auth/login"
PAYLOAD = {
    "username": "test@example.com",
    "password": "testpassword123"
}

def verify_login():
    logger.info(f"Target URL: {URL}")
    logger.info(f"Payload: {PAYLOAD}")
    
    try:
        response = requests.post(URL, json=PAYLOAD, timeout=5)
        logger.info(f"Status Code: {response.status_code}")
        logger.info(f"Response Body: {response.text}")
        
        if response.status_code == 200:
            logger.info("✅ Login SUCCESS")
            token = response.json().get("access_token")
            logger.info(f"Access Token: {token[:10]}...")
        else:
            logger.error("❌ Login FAILED")
            
    except Exception as e:
        logger.error(f"Request failed: {e}")

if __name__ == "__main__":
    verify_login()

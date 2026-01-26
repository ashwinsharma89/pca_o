import sys
import os
import logging

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.engine.analytics.llm_service import LLMService
# Load env vars manually if not loaded (llm_service load_dotenv might handle it, but good to be sure)
from dotenv import load_dotenv
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("TestGemini")

def test_connection():
    print("--- Starting Gemini Connection Test ---")
    try:
        logger.info("Initializing LLMService...")
        service = LLMService()
        
        print(f"Configuration:")
        print(f"  Primary Provider: {service.primary_provider}")
        print(f"  Gemini Model: {service.gemini_model}")
        print(f"  Gemini API Key Present: {bool(service.gemini_api_key)}")
        
        if service.primary_provider != 'gemini':
            print("WARNING: Primary provider is NOT gemini. Check your .env file.")
        
        prompt = "Hello! Please respond with 'Connection Successful' if you can read this."
        logger.info(f"Sending prompt: {prompt}")
        
        response = service.generate_completion(prompt)
        print(f"\nResponse Received:\n{response}\n")
        
        if response and len(response) > 0:
            print("✅ Connection Verification PASSED")
        else:
            print("❌ Connection Verification FAILED: Empty response")
            
    except Exception as e:
        print(f"❌ Connection Verification FAILED with error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_connection()

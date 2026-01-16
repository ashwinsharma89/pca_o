import os
from langsmith import Client
from dotenv import load_dotenv
from loguru import logger

def debug_langsmith():
    load_dotenv()
    api_key = os.getenv("LANGCHAIN_API_KEY")
    project = os.getenv("LANGCHAIN_PROJECT")
    tracing = os.getenv("LANGCHAIN_TRACING_V2")
    
    if not api_key:
        print("❌ LANGCHAIN_API_KEY is missing!")
        return
        
    print(f"Checking LangSmith for project: {project}")
    print(f"API Key (masked): {api_key[:10]}...{api_key[-4:] if len(api_key)>4 else ''}")
    print(f"Tracing enabled: {tracing}")

    client = Client()
    
    try:
        print("\n1. Testing list_datasets...")
        datasets = list(client.list_datasets(limit=5))
        print(f"✅ Success! Found {len(datasets)} datasets.")
        for ds in datasets:
            print(f"   - {ds.name}")
            
        print("\n2. Testing create_dataset (temporary)...")
        test_ds_name = "PCA_Agent_Test_DeleteMe"
        try:
            # Check if exists first
            client.read_dataset(dataset_name=test_ds_name)
            print(f"   Dataset {test_ds_name} already exists.")
        except:
            ds = client.create_dataset(dataset_name=test_ds_name, description="Test dataset")
            print(f"✅ Success! Created dataset {test_ds_name}.")
            # Cleanup
            client.delete_dataset(dataset_id=ds.id)
            print("   Deleted test dataset.")
            
    except Exception as e:
        print(f"❌ Error during test: {e}")
        if "Forbidden" in str(e):
            print("\n💡 This 403 Forbidden error strongly suggests the API Key is either:")
            print("   a) Invalid or expired.")
            print("   b) Lacks 'write' permissions/organization access.")
            print("   c) BELONGS TO A DIFFERENT ORGANIZATION than the one you are trying to access.")

if __name__ == "__main__":
    debug_langsmith()

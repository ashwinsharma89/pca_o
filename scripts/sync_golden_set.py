import json
import os
from langsmith import Client
from loguru import logger
from dotenv import load_dotenv

def sync_golden_set():
    """Sync golden queries from JSON to LangSmith dataset."""
    load_dotenv()
    try:
        # 1. Initialize LangSmith Client
        # client = Client()  # Uses ENV vars: LANGCHAIN_API_KEY, LANGCHAIN_PROJECT
        
        # Check for API key
        if not os.getenv("LANGCHAIN_API_KEY"):
            logger.error("LANGCHAIN_API_KEY not found in environment")
            return

        client = Client()
        
        # 2. Path to golden queries
        path = "tests/evaluation/golden_queries.json"
        if not os.path.exists(path):
            logger.error(f"Golden queries file not found: {path}")
            return
            
        with open(path, 'r') as f:
            data = json.load(f)
            scenarios = data.get('scenarios', [])
            
        dataset_name = "PCA Golden Set"
        
        # 3. Create or get dataset
        try:
            dataset = client.read_dataset(dataset_name=dataset_name)
            logger.info(f"Using existing dataset: {dataset_name}")
        except:
            dataset = client.create_dataset(
                dataset_name=dataset_name,
                description="Golden Set queries for PCA Agent evaluation"
            )
            logger.info(f"Created new dataset: {dataset_name}")
            
        # 4. Sync examples
        existing_examples = client.list_examples(dataset_id=dataset.id)
        existing_questions = {e.inputs.get("question") for e in existing_examples}
        
        new_count = 0
        for scenario in scenarios:
            question = scenario.get('question')
            if question not in existing_questions:
                client.create_example(
                    inputs={"question": question},
                    outputs={"expected_category": scenario.get('category'), "id": scenario.get('id')},
                    dataset_id=dataset.id
                )
                new_count += 1
                
        logger.info(f"Sync complete. Added {new_count} new examples. Total in dataset: {len(scenarios)}")
        print(f"✅ LangSmith Sync Complete: {dataset_name} ({len(scenarios)} examples)")

    except Exception as e:
        logger.error(f"Failed to sync golden set: {e}")

if __name__ == "__main__":
    sync_golden_set()

import json
import os
import sys
from dotenv import load_dotenv
load_dotenv()
import pytest
import pandas as pd
from loguru import logger
from typing import List, Dict, Any

# Add src to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from src.platform.query_engine.nl_to_sql import NaturalLanguageQueryEngine

def load_golden_queries():
    path = os.path.join(os.path.dirname(__file__), 'golden_queries.json')
    with open(path, 'r') as f:
        return json.load(f)['scenarios']

from langsmith import Client, traceable
from langsmith.evaluation import evaluate

def run_langsmith_evaluation():
    """Run the evaluation using LangSmith's evaluate function."""
    client = Client()
    dataset_name = "PCA Golden Set"
    
    def target(inputs: dict):
        engine = NaturalLanguageQueryEngine(api_key=os.getenv('OPENAI_API_KEY'))
        # Load sample data
        parquet_path = "data/campaigns.parquet"
        if os.path.exists(parquet_path):
            engine.load_parquet_data(parquet_path, table_name="all_campaigns")
        return engine.ask(inputs["question"])

    logger.info(f"Starting LangSmith evaluation on dataset: {dataset_name}")
    results = evaluate(
        target,
        data=dataset_name,
        experiment_prefix="PCA-Agent-Eval",
    )
    logger.info("Evaluation complete. Check results in LangSmith UI.")
    return results

class TestEvaluator:
    @classmethod
    def setup_class(cls):
        api_key = os.getenv('OPENAI_API_KEY')
        if not api_key:
            pytest.skip("OPENAI_API_KEY not found")
        
        cls.engine = NaturalLanguageQueryEngine(api_key=api_key)
        
        # Load sample data for testing
        parquet_path = "data/campaigns.parquet"
        if os.path.exists(parquet_path):
            cls.engine.load_parquet_data(parquet_path, table_name="all_campaigns")
            logger.info(f"Loaded {parquet_path} into evaluation engine")
        else:
            logger.warning(f"Parquet file {parquet_path} not found. Some tests may fail.")

    @pytest.mark.parametrize("scenario", load_golden_queries())
    def test_scenario(self, scenario):
        logger.info(f"Evaluating Scenario {scenario['id']}: {scenario['question']}")
        
        result = self.engine.ask(scenario['question'])
        
        # Assertions
        assert result.success is True, f"Query failed for {scenario['id']}: {getattr(result, 'error', 'Unknown error')}"
        assert result.sql, f"No SQL generated for {scenario['id']}"
        assert result.data is not None, f"No data returned for {scenario['id']}"
        
        # Log SQL for inspection in report
        logger.info(f"Generated SQL for {scenario['id']}: {result.sql}")

if __name__ == "__main__":
    # Run tests directly if called as script
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--langsmith", action="store_true", help="Run LangSmith evaluation")
    args, unknown = parser.parse_known_args()
    
    if args.langsmith:
        run_langsmith_evaluation()
    else:
        pytest.main([__file__, "-v", "-s"] + unknown)

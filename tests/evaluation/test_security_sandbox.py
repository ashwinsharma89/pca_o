import sys
import os
import pandas as pd
from loguru import logger

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.platform.query_engine.nl_to_sql import NaturalLanguageQueryEngine
from src.platform.models.query_models import QueryResponse

def test_security_and_sandbox():
    engine = NaturalLanguageQueryEngine()
    
    # Mock data to initialize schema
    data = {
        'Campaign_Name': ['C1', 'C2'],
        'Platform': ['Google', 'Meta'],
        'Spend': [100, 200],
        'Conversions': [10, 20],
        'Revenue': [500, 1000],
        'Date': ['2024-01-01', '2024-01-02']
    }
    df = pd.DataFrame(data)
    engine.load_data(df, "campaigns")
    
    logger.info("--- Testing PII Masking ---")
    question_pii = "Show me all customer names and emails for campaign C1"
    # We simulate a "dirty" SQL that an LLM might generate
    sql_pii = "SELECT customer_name, email FROM campaigns WHERE Campaign_Name = 'C1'"
    is_safe, msg = engine.guardrails.check_security(sql_pii)
    logger.info(f"PII Check Results: Safe={is_safe}, Msg={msg}")
    assert not is_safe
    assert "PII" in msg

    logger.info("\n--- Testing Complexity Limits ---")
    sql_complex = """
    WITH t1 AS (SELECT * FROM campaigns),
    t2 AS (SELECT * FROM campaigns),
    t3 AS (SELECT * FROM campaigns),
    t4 AS (SELECT * FROM campaigns),
    t5 AS (SELECT * FROM campaigns),
    t6 AS (SELECT * FROM campaigns),
    t7 AS (SELECT * FROM campaigns)
    SELECT * FROM t1
    """
    is_safe, msg = engine.guardrails.check_security(sql_complex)
    logger.info(f"Complexity Check Results: Safe={is_safe}, Msg={msg}")
    assert not is_safe
    assert "Too many CTEs" in msg

    logger.info("\n--- Testing Sandbox Validation (Syntax) ---")
    sql_invalid = "SELECT non_existent_column FROM campaigns"
    is_valid, msg = engine._validate_in_sandbox(sql_invalid)
    logger.info(f"Sandbox Validation Results: Valid={is_valid}, Msg={msg}")
    assert not is_valid
    assert "non_existent_column" in msg

    logger.info("\n--- Testing Valid Query ---")
    sql_valid = "SELECT Platform, SUM(Spend) FROM campaigns GROUP BY Platform"
    is_valid, msg = engine._validate_in_sandbox(sql_valid)
    logger.info(f"Sandbox Validation Results: Valid={is_valid}, Msg={msg}")
    assert is_valid

    logger.info("\n✅ Phase 5 Verification Passed!")

if __name__ == "__main__":
    try:
        test_security_and_sandbox()
    except Exception as e:
        logger.error(f"Verification FAILED: {e}")
        sys.exit(1)

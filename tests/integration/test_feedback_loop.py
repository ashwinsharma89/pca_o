import sys
from unittest.mock import MagicMock

# Force mock psycopg2 before anything else
mock_psycopg_mod = MagicMock()
sys.modules["psycopg2"] = mock_psycopg_mod
sys.modules["psycopg2.extras"] = MagicMock()
sys.modules["psycopg2.extensions"] = MagicMock()

import pytest
from unittest.mock import patch

# Global mocks for sqlalchemy
mock_sqlalchemy = patch('sqlalchemy.create_engine').start()

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from src.core.database.duckdb_manager import get_duckdb_manager
from src.engine.analytics.regression.pipeline import RegressionPipeline, RegressionResult
from src.engine.analytics.regression.recommendation_engine import RecommendationEngineV2
from src.engine.analytics.feedback_loop import OutcomeTracker

@pytest.fixture(scope="session")
def engine():
    """Override conftest.py engine to return a mock."""
    return MagicMock()

@pytest.fixture(autouse=True)
def mock_sql_db():
    """Mock PostgreSQL to prevent connection errors in tests."""
    with patch('src.core.database.connection.get_db_manager') as mock_get:
        mock_mgr = MagicMock()
        mock_get.return_value = mock_mgr
        yield mock_mgr

@pytest.fixture
def sample_data():
    """Generate synthetic data for two periods."""
    np.random.seed(42)
    dates = [datetime(2025, 1, 1) + timedelta(days=i) for i in range(30)]
    
    # Pre-implementation (Days 0-14): Stable performance
    # Post-implementation (Days 15-29): Higher spend -> MUCH Higher conversions
    data = {
        'Date': dates,
        'Spend': [100 + np.random.normal(0, 2) for _ in range(15)] + [200 + np.random.normal(0, 5) for _ in range(15)],
        'Conversions': [10 + np.random.normal(0, 0.5) for _ in range(15)] + [40 + np.random.normal(0, 1) for _ in range(15)],
        'Platform': ['Google Ads'] * 30,

        'year': [2025] * 30,
        'month': [1] * 30
    }
    return pd.DataFrame(data)

def test_feedback_loop_end_to_end(sample_data):
    db = get_duckdb_manager()
    db.clear_data()
    db.save_campaigns(sample_data)
    
    # 1. Run Analysis to generate a recommendation
    pipeline = RegressionPipeline()
    result = pipeline.run(sample_data, target='Conversions', features=['Spend'])
    
    recs = RecommendationEngineV2.generate(result)
    assert len(recs) > 0
    rec_id = recs[0]['id']
    
    # 2. Mock User Feedback (Implemented on Day 15)
    with db.connection() as conn:
        conn.execute("""
            INSERT INTO recommendation_feedback (recommendation_id, user_action, implementation_date)
            VALUES (?, 'Implemented', '2025-01-15')
        """, (rec_id,))
        
    # 3. Track Outcome
    # OutcomeTracker._calculate_impact uses 7-day windows before/after
    summary = OutcomeTracker.get_attribution_summary()
    
    assert len(summary) > 0
    impact = summary[0]
    assert impact['recommendation_id'] == rec_id
    assert impact['pre_efficiency'] > 0
    assert impact['post_efficiency'] > impact['pre_efficiency']
    assert impact['roi_percent'] > 0
    assert impact['status'] == "Positive Impact"
    
    print(f"\nFeedback Loop Verified:")
    print(f"Rec ID: {rec_id}")
    print(f"Efficiency Lift: {impact['roi_percent']}%")

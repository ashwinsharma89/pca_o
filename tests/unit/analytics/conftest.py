"""
Pytest configuration for analytics unit tests.
Fixtures specific to analytics testing that don't require database.
"""

import pytest
import pandas as pd
import numpy as np


# Skip database-dependent fixtures
@pytest.fixture(autouse=True)
def no_db_dependency(monkeypatch):
    """Prevent database connections in unit tests."""
    pass


@pytest.fixture
def sample_campaign_data():
    """Standard campaign data for testing."""
    np.random.seed(42)
    n = 200
    
    return pd.DataFrame({
        'date': pd.date_range('2024-01-01', periods=n),
        'campaign': np.random.choice(['camp_A', 'camp_B', 'camp_C'], n),
        'platform': np.random.choice(['meta', 'google', 'dv360'], n),
        'objective': np.random.choice(['awareness', 'conversions', 'traffic'], n),
        'device': np.random.choice(['mobile', 'desktop', 'tablet'], n),
        'spend': np.random.uniform(100, 1000, n),
        'impressions': np.random.uniform(10000, 100000, n),
        'clicks': np.random.uniform(100, 1000, n),
        'conversions': np.random.randint(0, 50, n),
        'budget': np.random.uniform(5000, 10000, n)
    })

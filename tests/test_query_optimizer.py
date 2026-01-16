"""
Tests for Query Optimizer
"""
import pytest
import pandas as pd
import duckdb
from src.platform.query_engine.query_optimizer import QueryOptimizer, QueryPlan


@pytest.fixture
def sample_data():
    """Create sample campaign data."""
    return pd.DataFrame({
        'campaign_id': range(1, 101),
        'campaign_name': [f'Campaign {i}' for i in range(1, 101)],
        'platform': ['Facebook'] * 50 + ['Google'] * 50,
        'spend': [100 + i * 10 for i in range(100)],
        'impressions': [1000 + i * 100 for i in range(100)],
        'clicks': [50 + i * 5 for i in range(100)],
        'conversions': [5 + i for i in range(100)]
    })


@pytest.fixture
def optimizer(sample_data):
    """Create optimizer with sample data."""
    conn = duckdb.connect(':memory:')
    conn.register('campaigns', sample_data)
    return QueryOptimizer(conn)


def test_analyze_query(optimizer):
    """Test query plan analysis."""
    sql = "SELECT * FROM campaigns WHERE platform = 'Facebook'"
    plan = optimizer.analyze_query(sql)
    
    assert isinstance(plan, QueryPlan)
    assert plan.execution_time >= 0
    assert plan.raw_plan is not None


def test_suggest_optimizations_seq_scan(optimizer):
    """Test optimization suggestions for sequential scan."""
    sql = "SELECT * FROM campaigns"
    plan = optimizer.analyze_query(sql)
    suggestions = optimizer.suggest_optimizations(plan, sql)
    
    assert len(suggestions) > 0
    # Should suggest avoiding SELECT *
    assert any('SELECT *' in s for s in suggestions)


def test_suggest_optimizations_good_query(optimizer):
    """Test optimization suggestions for well-optimized query."""
    sql = "SELECT campaign_name, SUM(spend) FROM campaigns GROUP BY campaign_name LIMIT 10"
    plan = optimizer.analyze_query(sql)
    suggestions = optimizer.suggest_optimizations(plan, sql)
    
    # Should have positive feedback
    assert any('well-optimized' in s.lower() or 'looks good' in s.lower() for s in suggestions)


def test_optimize_query_adds_limit(optimizer):
    """Test automatic query optimization."""
    sql = "SELECT * FROM campaigns ORDER BY spend DESC"
    optimized, changes = optimizer.optimize_query(sql)
    
    assert 'LIMIT' in optimized.upper()
    assert len(changes) > 0


def test_get_query_stats(optimizer):
    """Test comprehensive query statistics."""
    sql = "SELECT platform, SUM(spend) as total_spend FROM campaigns GROUP BY platform"
    stats = optimizer.get_query_stats(sql)
    
    assert 'execution_time' in stats
    assert 'estimated_rows' in stats
    assert 'cost' in stats
    assert 'optimization_suggestions' in stats
    assert isinstance(stats['optimization_suggestions'], list)


def test_optimization_history(optimizer):
    """Test optimization history tracking."""
    sql1 = "SELECT * FROM campaigns LIMIT 10"
    sql2 = "SELECT platform, COUNT(*) FROM campaigns GROUP BY platform"
    
    optimizer.get_query_stats(sql1)
    optimizer.get_query_stats(sql2)
    
    assert len(optimizer.optimization_history) == 2
    
    report = optimizer.get_optimization_report()
    assert 'Total queries analyzed: 2' in report


def test_query_plan_cost_extraction():
    """Test cost extraction from query plan."""
    plan_text = "QUERY PLAN\nSeq Scan on campaigns (cost=0.00..100.00 rows=1000)"
    plan = QueryPlan(plan_text, 0.5)
    
    assert plan.cost > 0
    assert plan.estimated_rows > 0


def test_complex_query_suggestions(optimizer):
    """Test suggestions for complex queries."""
    sql = """
    SELECT c1.campaign_name, 
           SUM(c1.spend) as total_spend,
           AVG(c1.clicks) as avg_clicks,
           COUNT(c1.conversions) as conv_count,
           SUM(c1.impressions) as total_impr,
           MAX(c1.spend) as max_spend
    FROM campaigns c1
    WHERE c1.platform = 'Facebook'
    """
    
    plan = optimizer.analyze_query(sql)
    suggestions = optimizer.suggest_optimizations(plan, sql)
    
    # Should suggest CTEs for complex aggregations
    assert any('CTE' in s or 'WITH' in s for s in suggestions)


if __name__ == '__main__':
    pytest.main([__file__, '-v'])

"""
Tests for Multi-Table Manager
"""
import pytest
import pandas as pd
import duckdb
from src.platform.query_engine.multi_table_manager import MultiTableManager, TableRelationship


@pytest.fixture
def campaigns_data():
    """Create sample campaigns data."""
    return pd.DataFrame({
        'campaign_id': range(1, 11),
        'campaign_name': [f'Campaign {i}' for i in range(1, 11)],
        'product_id': [1, 1, 2, 2, 3, 3, 4, 4, 5, 5],
        'spend': [100 + i * 10 for i in range(10)],
        'impressions': [1000 + i * 100 for i in range(10)]
    })


@pytest.fixture
def products_data():
    """Create sample products data."""
    return pd.DataFrame({
        'product_id': range(1, 6),
        'product_name': ['Product A', 'Product B', 'Product C', 'Product D', 'Product E'],
        'category': ['Electronics', 'Electronics', 'Clothing', 'Clothing', 'Home']
    })


@pytest.fixture
def manager(campaigns_data, products_data):
    """Create multi-table manager with sample data."""
    conn = duckdb.connect(':memory:')
    mgr = MultiTableManager(conn)
    mgr.register_table('campaigns', campaigns_data, primary_key='campaign_id')
    mgr.register_table('products', products_data, primary_key='product_id')
    return mgr


def test_register_table(manager):
    """Test table registration."""
    assert 'campaigns' in manager.tables
    assert 'products' in manager.tables
    assert manager.tables['campaigns']['row_count'] == 10
    assert manager.tables['products']['row_count'] == 5


def test_auto_detect_relationships(manager):
    """Test automatic relationship detection."""
    detected = manager.auto_detect_relationships()
    
    assert len(detected) > 0
    # Should detect product_id relationship
    assert any(
        rel['on_column'] == 'product_id' or 
        rel['table1_column'] == 'product_id' or 
        rel['table2_column'] == 'product_id'
        for rel in detected
    )


def test_define_relationship(manager):
    """Test manual relationship definition."""
    rel = manager.define_relationship(
        table1='campaigns',
        table2='products',
        join_type='LEFT',
        on_column='product_id'
    )
    
    assert isinstance(rel, TableRelationship)
    assert rel.table1 == 'campaigns'
    assert rel.table2 == 'products'
    assert rel.join_type == 'LEFT'
    assert not rel.auto_detected


def test_validate_relationship(manager):
    """Test relationship validation."""
    rel = manager.define_relationship(
        table1='campaigns',
        table2='products',
        join_type='LEFT',
        on_column='product_id'
    )
    
    is_valid, message = manager.validate_relationship(rel)
    
    assert is_valid
    assert 'Valid relationship' in message
    assert rel.validated


def test_validate_invalid_relationship(manager):
    """Test validation of invalid relationship."""
    rel = TableRelationship(
        table1='campaigns',
        table2='products',
        join_type='INNER',
        on_column='invalid_column'
    )
    
    is_valid, message = manager.validate_relationship(rel)
    
    assert not is_valid
    assert 'not found' in message.lower()


def test_get_schema_with_relationships(manager):
    """Test schema description with relationships."""
    manager.define_relationship(
        table1='campaigns',
        table2='products',
        join_type='LEFT',
        on_column='product_id'
    )
    
    schema = manager.get_schema_with_relationships()
    
    assert 'campaigns' in schema
    assert 'products' in schema
    assert 'Relationships' in schema
    assert 'product_id' in schema


def test_get_join_hints_for_llm(manager):
    """Test join hints generation for LLM."""
    manager.define_relationship(
        table1='campaigns',
        table2='products',
        join_type='LEFT',
        on_column='product_id'
    )
    
    hints = manager.get_join_hints_for_llm()
    
    assert 'JOIN' in hints
    assert 'campaigns' in hints
    assert 'products' in hints
    assert 'Example' in hints


def test_suggest_joins_for_question(manager):
    """Test join suggestions based on question."""
    manager.define_relationship(
        table1='campaigns',
        table2='products',
        join_type='LEFT',
        on_column='product_id'
    )
    
    question = "Show me campaign performance by product category"
    relevant = manager.suggest_joins_for_question(question)
    
    assert len(relevant) > 0
    assert any(rel.table2 == 'products' for rel in relevant)


def test_table_relationship_to_sql():
    """Test SQL generation from relationship."""
    rel = TableRelationship(
        table1='campaigns',
        table2='products',
        join_type='LEFT',
        on_column='product_id'
    )
    
    sql = rel.to_sql('c', 'p')
    
    assert 'LEFT JOIN' in sql
    assert 'products p' in sql
    assert 'c.product_id = p.product_id' in sql


def test_get_all_relationships(manager):
    """Test retrieving all relationships."""
    manager.define_relationship(
        table1='campaigns',
        table2='products',
        join_type='LEFT',
        on_column='product_id'
    )
    
    all_rels = manager.get_all_relationships()
    
    assert len(all_rels) > 0
    assert isinstance(all_rels[0], dict)


def test_get_validated_relationships_only(manager):
    """Test retrieving only validated relationships."""
    # Add validated relationship
    rel1 = manager.define_relationship(
        table1='campaigns',
        table2='products',
        join_type='LEFT',
        on_column='product_id'
    )
    manager.validate_relationship(rel1)
    
    # Add unvalidated relationship
    rel2 = TableRelationship(
        table1='campaigns',
        table2='products',
        join_type='INNER',
        on_column='test',
        auto_detected=True
    )
    manager.relationships.append(rel2)
    
    validated = manager.get_all_relationships(validated_only=True)
    
    assert len(validated) == 1
    assert validated[0]['validated']


if __name__ == '__main__':
    pytest.main([__file__, '-v'])

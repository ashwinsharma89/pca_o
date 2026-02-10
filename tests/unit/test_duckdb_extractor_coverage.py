"""
Tests for DuckDBExtractor (Phase B.1).
Verifies extraction with filters and batching.
"""

import pytest
import pandas as pd
import duckdb
import os
from pathlib import Path
from src.kg_rag.etl.extractors.duckdb_extractor import DuckDBExtractor

@pytest.fixture
def mock_db(tmp_path):
    """Create a temporary DuckDB database for testing."""
    db_path = tmp_path / "test_extract.duckdb"
    conn = duckdb.connect(str(db_path))
    
    # Create campaigns table
    conn.execute("""
        CREATE TABLE campaigns (
            campaign_id VARCHAR,
            campaign_name VARCHAR,
            platform VARCHAR,
            "Amount Spent" DOUBLE,
            "Link Clicks" INTEGER,
            date DATE
        )
    """)
    conn.execute("INSERT INTO campaigns VALUES ('C1', 'C1 Name', 'Google', 100.0, 50, '2024-01-01')")
    conn.execute("INSERT INTO campaigns VALUES ('C2', 'C2 Name', 'Meta', 200.0, 100, '2024-01-02')")
    conn.execute("INSERT INTO campaigns VALUES ('C3', 'C3 Name', 'Google', 300.0, 150, '2024-01-03')")
    
    # Create metrics table
    conn.execute("""
        CREATE TABLE metrics (
            campaign_id VARCHAR,
            date DATE,
            spend DOUBLE,
            conversions INTEGER
        )
    """)
    conn.execute("INSERT INTO metrics VALUES ('C1', '2024-01-01', 100.0, 5)")
    conn.execute("INSERT INTO metrics VALUES ('C2', '2024-01-02', 200.0, 10)")
    
    conn.close()
    return db_path

class TestDuckDBExtractor:
    """Unit tests for DuckDBExtractor extraction logic."""

    def test_get_tables_and_columns(self, mock_db):
        """Verify table and column metadata extraction."""
        extractor = DuckDBExtractor(str(mock_db))
        tables = extractor.get_tables()
        assert "campaigns" in tables
        assert "metrics" in tables
        
        columns = extractor.get_columns("campaigns")
        assert "campaign_id" in columns
        assert "platform" in columns

    def test_extract_campaigns_no_filter(self, mock_db):
        """Verify extraction of all campaigns."""
        extractor = DuckDBExtractor(str(mock_db))
        batches = list(extractor.extract_campaigns(batch_size=2))
        
        assert len(batches) == 2 # 3 records total
        assert len(batches[0]) == 2
        assert len(batches[1]) == 1
        
        # Verify content
        all_ids = [c['campaign_id'] for b in batches for c in b]
        assert set(all_ids) == {'C1', 'C2', 'C3'}

    def test_extract_campaigns_with_platform_filter(self, mock_db):
        """Verify platform filtering."""
        extractor = DuckDBExtractor(str(mock_db))
        batches = list(extractor.extract_campaigns(platform_filter="Google"))
        
        all_records = [c for b in batches for c in b]
        assert len(all_records) == 2
        assert all(r['platform'] == "Google" for r in all_records)

    def test_extract_metrics_with_date_filter(self, mock_db):
        """Verify metric extraction with date ranges."""
        from datetime import date
        extractor = DuckDBExtractor(str(mock_db))
        
        # Filter for only C1 date
        batches = list(extractor.extract_metrics(
            date_from=date(2024, 1, 1), 
            date_to=date(2024, 1, 1)
        ))
        
        all_metrics = [m for b in batches for m in b]
        assert len(all_metrics) == 1
        assert all_metrics[0]['campaign_id'] == 'C1'

    def test_get_date_range(self, mock_db):
        """Verify date range detection."""
        extractor = DuckDBExtractor(str(mock_db))
        dr = extractor.get_date_range("metrics")
        assert dr['min'].isoformat() == "2024-01-01"
        assert dr['max'].isoformat() == "2024-01-02"

    def test_detect_platform(self, mock_db):
        """Verify platform detection from columns."""
        extractor = DuckDBExtractor(str(mock_db))
        # This calls column_resolver.detect_platform
        # Since 'platform' column exists, it should work
        platform = extractor.detect_platform("campaigns")
        assert platform is not None

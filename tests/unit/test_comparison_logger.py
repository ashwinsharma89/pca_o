"""
Unit tests for Comparison Logger.
Tests RAG vs Standard comparison logging and analysis.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
import tempfile
import os
import json


class TestComparisonLoggerInit:
    """Tests for ComparisonLogger initialization."""
    
    def test_logger_creates_directories(self):
        """Test logger creates required directories."""
        with tempfile.TemporaryDirectory() as tmpdir:
            from src.core.utils.comparison_logger import ComparisonLogger
            
            logger = ComparisonLogger(log_dir=tmpdir)
            
            assert os.path.exists(os.path.join(tmpdir, "json"))
            assert os.path.exists(os.path.join(tmpdir, "csv"))
    
    def test_logger_creates_csv_files(self):
        """Test logger creates CSV files with headers."""
        with tempfile.TemporaryDirectory() as tmpdir:
            from src.core.utils.comparison_logger import ComparisonLogger
            
            logger = ComparisonLogger(log_dir=tmpdir)
            
            assert logger.metrics_csv.exists()
            assert logger.feedback_csv.exists()


class TestLogComparison:
    """Tests for log_comparison method."""
    
    @pytest.fixture
    def logger(self):
        """Create logger with temp directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            from src.core.utils.comparison_logger import ComparisonLogger
            yield ComparisonLogger(log_dir=tmpdir)
    
    def test_log_comparison_creates_json(self):
        """Test logging comparison creates JSON file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            from src.core.utils.comparison_logger import ComparisonLogger
            
            logger = ComparisonLogger(log_dir=tmpdir)
            
            standard_result = {
                "summary_brief": "Standard summary",
                "tokens_input": 100,
                "tokens_output": 50,
                "cost": 0.01,
                "latency": 1.0
            }
            
            rag_result = {
                "summary_brief": "RAG summary",
                "tokens_input": 150,
                "tokens_output": 75,
                "cost": 0.015,
                "latency": 1.5,
                "knowledge_sources": ["source1", "source2"]
            }
            
            json_path = logger.log_comparison(
                session_id="test-session-123",
                campaign_id="campaign-456",
                standard_result=standard_result,
                rag_result=rag_result
            )
            
            assert os.path.exists(json_path)
            
            with open(json_path) as f:
                data = json.load(f)
            
            assert data["session_id"] == "test-session-123"
            assert data["campaign_id"] == "campaign-456"
    
    def test_log_comparison_calculates_increases(self):
        """Test logging calculates percentage increases."""
        with tempfile.TemporaryDirectory() as tmpdir:
            from src.core.utils.comparison_logger import ComparisonLogger
            
            logger = ComparisonLogger(log_dir=tmpdir)
            
            standard_result = {"tokens_input": 100, "cost": 0.01, "latency": 1.0}
            rag_result = {"tokens_input": 150, "cost": 0.015, "latency": 1.5}
            
            json_path = logger.log_comparison(
                session_id="test",
                campaign_id="camp",
                standard_result=standard_result,
                rag_result=rag_result
            )
            
            with open(json_path) as f:
                data = json.load(f)
            
            assert data["comparison"]["token_increase_pct"] == pytest.approx(50.0)
            assert data["comparison"]["cost_increase_pct"] == pytest.approx(50.0)
            assert data["comparison"]["latency_increase_pct"] == pytest.approx(50.0)
    
    def test_log_comparison_appends_to_csv(self):
        """Test logging appends to CSV file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            from src.core.utils.comparison_logger import ComparisonLogger
            import pandas as pd
            
            logger = ComparisonLogger(log_dir=tmpdir)
            
            # Log two comparisons
            logger.log_comparison(
                session_id="session1",
                campaign_id="camp1",
                standard_result={"tokens_input": 100},
                rag_result={"tokens_input": 150}
            )
            
            logger.log_comparison(
                session_id="session2",
                campaign_id="camp2",
                standard_result={"tokens_input": 200},
                rag_result={"tokens_input": 300}
            )
            
            df = pd.read_csv(logger.metrics_csv)
            assert len(df) == 2


class TestLogFeedback:
    """Tests for log_feedback method."""
    
    def test_log_feedback(self):
        """Test logging user feedback."""
        with tempfile.TemporaryDirectory() as tmpdir:
            from src.core.utils.comparison_logger import ComparisonLogger
            import pandas as pd
            
            logger = ComparisonLogger(log_dir=tmpdir)
            
            logger.log_feedback(
                session_id="test-session",
                campaign_id="test-campaign",
                user_preference="rag",
                quality_rating=4,
                usefulness_rating=5,
                comments="RAG provided better insights"
            )
            
            df = pd.read_csv(logger.feedback_csv)
            assert len(df) == 1
            assert df.iloc[0]["user_preference"] == "rag"
            assert df.iloc[0]["quality_rating"] == 4


class TestSummaryStats:
    """Tests for get_summary_stats method."""
    
    def test_summary_stats_empty(self):
        """Test summary stats with no data."""
        with tempfile.TemporaryDirectory() as tmpdir:
            from src.core.utils.comparison_logger import ComparisonLogger
            
            logger = ComparisonLogger(log_dir=tmpdir)
            stats = logger.get_summary_stats()
            
            assert "message" in stats or "total_comparisons" in stats
    
    def test_summary_stats_with_data(self):
        """Test summary stats with comparison data."""
        with tempfile.TemporaryDirectory() as tmpdir:
            from src.core.utils.comparison_logger import ComparisonLogger
            
            logger = ComparisonLogger(log_dir=tmpdir)
            
            # Log some comparisons
            for i in range(5):
                logger.log_comparison(
                    session_id=f"session{i}",
                    campaign_id=f"camp{i}",
                    standard_result={"tokens_input": 100, "cost": 0.01, "latency": 1.0},
                    rag_result={"tokens_input": 150, "cost": 0.015, "latency": 1.5, "knowledge_sources": ["s1"]}
                )
            
            stats = logger.get_summary_stats()
            
            assert stats["total_comparisons"] == 5
            assert "avg_token_increase_pct" in stats
            assert "avg_cost_increase_pct" in stats


class TestExportReport:
    """Tests for export_analysis_report method."""
    
    def test_export_report(self):
        """Test exporting analysis report."""
        with tempfile.TemporaryDirectory() as tmpdir:
            from src.core.utils.comparison_logger import ComparisonLogger
            
            logger = ComparisonLogger(log_dir=tmpdir)
            
            # Log some data
            logger.log_comparison(
                session_id="test",
                campaign_id="camp",
                standard_result={"tokens_input": 100, "cost": 0.01, "latency": 1.0},
                rag_result={"tokens_input": 150, "cost": 0.015, "latency": 1.5}
            )
            
            report_path = logger.export_analysis_report()
            
            assert os.path.exists(report_path)
            
            with open(report_path, encoding='utf-8') as f:
                content = f.read()
            
            assert "RAG vs Standard" in content
            assert "Summary Statistics" in content


class TestPercentageIncrease:
    """Tests for percentage increase calculation."""
    
    def test_percentage_increase_normal(self):
        """Test normal percentage increase."""
        from src.core.utils.comparison_logger import ComparisonLogger
        
        result = ComparisonLogger._calc_percentage_increase(100, 150)
        assert result == 50.0
    
    def test_percentage_increase_zero_baseline(self):
        """Test percentage increase with zero baseline."""
        from src.core.utils.comparison_logger import ComparisonLogger
        
        result = ComparisonLogger._calc_percentage_increase(0, 100)
        assert result == 0.0
    
    def test_percentage_decrease(self):
        """Test percentage decrease."""
        from src.core.utils.comparison_logger import ComparisonLogger
        
        result = ComparisonLogger._calc_percentage_increase(100, 80)
        assert result == -20.0

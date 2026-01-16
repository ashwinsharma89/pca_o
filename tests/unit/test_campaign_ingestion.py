"""
Unit tests for Campaign Ingestion Service.
Tests data validation, normalization, and ingestion pipeline.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
import tempfile
import os


class TestValidateCampaignDataframe:
    """Tests for validate_campaign_dataframe function."""
    
    def test_valid_dataframe(self):
        """Test validation of valid dataframe."""
        from src.engine.services.campaign_ingestion import validate_campaign_dataframe
        
        df = pd.DataFrame({
            "Campaign_Name": ["Campaign A", "Campaign B"],
            "Platform": ["Google", "Meta"],
            "Spend": [1000, 2000],
            "Conversions": [50, 100],
            "Revenue": [5000, 10000],
            "Date": ["2024-01-01", "2024-01-02"],
            "Placement": ["Search", "Feed"]
        })
        
        report = validate_campaign_dataframe(df)
        
        assert report["missing_required"] == []
        assert report["missing_recommended"] == []
        assert report["alerts"] == []
    
    def test_missing_required_columns(self):
        """Test detection of missing required columns."""
        from src.engine.services.campaign_ingestion import validate_campaign_dataframe
        
        df = pd.DataFrame({
            "Campaign_Name": ["Campaign A"],
            # Missing Platform and Spend
        })
        
        report = validate_campaign_dataframe(df)
        
        assert "Platform" in report["missing_required"]
        assert "Spend" in report["missing_required"]
    
    def test_missing_recommended_columns(self):
        """Test detection of missing recommended columns."""
        from src.engine.services.campaign_ingestion import validate_campaign_dataframe
        
        df = pd.DataFrame({
            "Campaign_Name": ["Campaign A"],
            "Platform": ["Google"],
            "Spend": [1000]
            # Missing Conversions, Revenue, Date, Placement
        })
        
        report = validate_campaign_dataframe(df)
        
        assert "Conversions" in report["missing_recommended"]
        assert "Revenue" in report["missing_recommended"]
    
    def test_negative_spend_alert(self):
        """Test alert for negative spend values."""
        from src.engine.services.campaign_ingestion import validate_campaign_dataframe
        
        df = pd.DataFrame({
            "Campaign_Name": ["Campaign A", "Campaign B"],
            "Platform": ["Google", "Meta"],
            "Spend": [1000, -500]  # Negative spend
        })
        
        report = validate_campaign_dataframe(df)
        
        assert any("negative" in alert.lower() and "spend" in alert.lower() 
                   for alert in report["alerts"])
    
    def test_negative_conversions_alert(self):
        """Test alert for negative conversions."""
        from src.engine.services.campaign_ingestion import validate_campaign_dataframe
        
        df = pd.DataFrame({
            "Campaign_Name": ["Campaign A"],
            "Platform": ["Google"],
            "Spend": [1000],
            "Conversions": [-10]  # Negative conversions
        })
        
        report = validate_campaign_dataframe(df)
        
        assert any("conversions" in alert.lower() for alert in report["alerts"])
    
    def test_duplicate_rows_alert(self):
        """Test alert for duplicate rows."""
        from src.engine.services.campaign_ingestion import validate_campaign_dataframe
        
        df = pd.DataFrame({
            "Campaign_Name": ["Campaign A", "Campaign A"],
            "Platform": ["Google", "Google"],
            "Spend": [1000, 1000]
        })
        
        report = validate_campaign_dataframe(df)
        
        assert any("duplicate" in alert.lower() for alert in report["alerts"])


class TestProcessCampaignDataframe:
    """Tests for process_campaign_dataframe function."""
    
    def test_process_adds_metadata(self):
        """Test processing adds row and column counts."""
        from src.engine.services.campaign_ingestion import process_campaign_dataframe
        
        df = pd.DataFrame({
            "Campaign_Name": ["A", "B", "C"],
            "Platform": ["Google", "Meta", "LinkedIn"],
            "Spend": [100, 200, 300]
        })
        
        processed_df, metadata, validation = process_campaign_dataframe(df)
        
        assert metadata["rows"] == 3
        assert metadata["columns"] >= 3
    
    def test_process_returns_validation_status_ok(self):
        """Test processing returns ok status for valid data."""
        from src.engine.services.campaign_ingestion import process_campaign_dataframe
        
        df = pd.DataFrame({
            "Campaign_Name": ["A"],
            "Platform": ["Google"],
            "Spend": [100],
            "Conversions": [10],
            "Revenue": [500],
            "Date": ["2024-01-01"],
            "Placement": ["Search"]
        })
        
        _, _, validation = process_campaign_dataframe(df)
        
        assert validation["status"] == "ok"
    
    def test_process_returns_validation_status_warn(self):
        """Test processing returns warn status for alerts."""
        from src.engine.services.campaign_ingestion import process_campaign_dataframe
        
        df = pd.DataFrame({
            "Campaign_Name": ["A"],
            "Platform": ["Google"],
            "Spend": [100]
            # Missing recommended columns
        })
        
        _, _, validation = process_campaign_dataframe(df)
        
        assert validation["status"] == "warn"
    
    def test_process_returns_validation_status_error(self):
        """Test processing returns error status for missing required."""
        from src.engine.services.campaign_ingestion import process_campaign_dataframe
        
        df = pd.DataFrame({
            "Campaign_Name": ["A"]
            # Missing Platform and Spend
        })
        
        _, _, validation = process_campaign_dataframe(df)
        
        assert validation["status"] == "error"
    
    def test_process_preserves_custom_metadata(self):
        """Test processing preserves custom metadata."""
        from src.engine.services.campaign_ingestion import process_campaign_dataframe
        
        df = pd.DataFrame({
            "Campaign_Name": ["A"],
            "Platform": ["Google"],
            "Spend": [100]
        })
        
        custom_metadata = {"source": "test", "user": "admin"}
        
        _, metadata, _ = process_campaign_dataframe(df, custom_metadata)
        
        assert metadata["source"] == "test"
        assert metadata["user"] == "admin"


class TestIngestCampaignData:
    """Tests for ingest_campaign_data function."""
    
    def test_ingest_success(self):
        """Test successful data ingestion."""
        mock_file = Mock()
        mock_file.name = "test.csv"
        mock_file.type = "text/csv"
        mock_file.size = 1024
        
        mock_df = pd.DataFrame({
            "Campaign_Name": ["A"],
            "Platform": ["Google"],
            "Spend": [100]
        })
        
        with patch("src.engine.services.campaign_ingestion.DataLoader") as mock_loader:
            mock_loader.load_from_streamlit_upload.return_value = (mock_df, None)
            
            from src.engine.services.campaign_ingestion import ingest_campaign_data
            
            df, metadata, validation = ingest_campaign_data(mock_file)
            
            assert df is not None
            assert metadata["file_name"] == "test.csv"
            assert "error" not in validation
    
    def test_ingest_failure(self):
        """Test failed data ingestion."""
        mock_file = Mock()
        mock_file.name = "bad.csv"
        mock_file.type = "text/csv"
        mock_file.size = 0
        
        with patch("src.engine.services.campaign_ingestion.DataLoader") as mock_loader:
            mock_loader.load_from_streamlit_upload.return_value = (None, "Invalid file format")
            
            from src.engine.services.campaign_ingestion import ingest_campaign_data
            
            df, metadata, validation = ingest_campaign_data(mock_file)
            
            assert df is None
            assert "error" in validation
    
    def test_ingest_with_source(self):
        """Test ingestion with custom source."""
        mock_file = Mock()
        mock_file.name = "test.csv"
        mock_file.type = "text/csv"
        mock_file.size = 1024
        
        mock_df = pd.DataFrame({
            "Campaign_Name": ["A"],
            "Platform": ["Google"],
            "Spend": [100]
        })
        
        with patch("src.engine.services.campaign_ingestion.DataLoader") as mock_loader:
            mock_loader.load_from_streamlit_upload.return_value = (mock_df, None)
            
            from src.engine.services.campaign_ingestion import ingest_campaign_data
            
            _, metadata, _ = ingest_campaign_data(mock_file, source="custom_source")
            
            assert metadata["source"] == "custom_source"


class TestLoadSampleCampaignData:
    """Tests for load_sample_campaign_data function."""
    
    def test_load_sample_not_found(self):
        """Test loading sample when file doesn't exist."""
        with patch("src.engine.services.campaign_ingestion.SAMPLE_DATA_PATH") as mock_path:
            mock_path.exists.return_value = False
            
            from src.engine.services.campaign_ingestion import load_sample_campaign_data
            
            df, error = load_sample_campaign_data()
            
            assert df is None
            assert "not found" in error.lower()
    
    def test_load_sample_success(self):
        """Test successful sample data loading."""
        mock_df = pd.DataFrame({
            "Campaign_Name": ["Sample A", "Sample B"],
            "Platform": ["Google", "Meta"],
            "Spend": [1000, 2000]
        })
        
        with patch("src.engine.services.campaign_ingestion.SAMPLE_DATA_PATH") as mock_path:
            mock_path.exists.return_value = True
            mock_path.name = "sample.csv"
            
            with patch("src.engine.services.campaign_ingestion.DataLoader") as mock_loader:
                mock_loader.load_csv.return_value = (mock_df, None)
                
                from src.engine.services.campaign_ingestion import load_sample_campaign_data
                
                df, error = load_sample_campaign_data()
                
                assert df is not None
                assert error is None
    
    def test_load_sample_error(self):
        """Test sample loading with error."""
        with patch("src.engine.services.campaign_ingestion.SAMPLE_DATA_PATH") as mock_path:
            mock_path.exists.return_value = True
            
            with patch("src.engine.services.campaign_ingestion.DataLoader") as mock_loader:
                mock_loader.load_csv.return_value = (None, "File corrupted")
                
                from src.engine.services.campaign_ingestion import load_sample_campaign_data
                
                df, error = load_sample_campaign_data()
                
                assert df is None
                assert error == "File corrupted"


class TestConstants:
    """Tests for module constants."""
    
    def test_required_columns(self):
        """Test required columns are defined."""
        from src.engine.services.campaign_ingestion import REQUIRED_COLUMNS
        
        assert "Campaign_Name" in REQUIRED_COLUMNS
        assert "Platform" in REQUIRED_COLUMNS
        assert "Spend" in REQUIRED_COLUMNS
    
    def test_recommended_columns(self):
        """Test recommended columns are defined."""
        from src.engine.services.campaign_ingestion import RECOMMENDED_COLUMNS
        
        assert "Conversions" in RECOMMENDED_COLUMNS
        assert "Revenue" in RECOMMENDED_COLUMNS

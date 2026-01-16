import pandas as pd
import pytest

from src.core.utils.data_validator import DataValidator


@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "Campaign Name": ["Spring Sale", "Summer Sale"],
        "Total Spend": ["$1,000", "$2,500"],
        "Conv": ["100", "250"],
        "Funnel": ["Upper Funnel", "Lower"],
        "Date Start": ["13-01-2024", "2024/01/25"],
    })


def test_column_normalization_and_numeric_cast(sample_df):
    validator = DataValidator()
    cleaned, report = validator.validate_and_clean_dataframe(sample_df)

    assert "Campaign_Name" in cleaned.columns
    assert "Spend" in cleaned.columns
    assert "Conversions" in cleaned.columns
    assert cleaned["Spend"].dtype.kind == "f"
    assert cleaned["Conversions"].dtype.kind == "f"

    # Check that column mappings were recorded
    assert "Column Mappings" in report["conversions"]
    assert "columns renamed" in report["conversions"]["Column Mappings"]


def test_funnel_stage_normalization(sample_df):
    validator = DataValidator()
    cleaned, _ = validator.validate_and_clean_dataframe(sample_df)

    assert cleaned["Funnel_Stage"].tolist() == ["Awareness", "Conversion"]


def test_date_parsing_success(sample_df):
    validator = DataValidator()
    cleaned, report = validator.validate_and_clean_dataframe(sample_df)

    assert cleaned["Date"].dtype == "datetime64[ns]"
    assert report["columns"]["Date"]["null_count"] == 0


def test_partial_missing_data():
    validator = DataValidator()
    df = pd.DataFrame({"Funnel": ["Unknown"], "Spend": ["N/A"]})
    cleaned, report = validator.validate_and_clean_dataframe(df)

    assert "Funnel_Stage" in cleaned.columns
    assert cleaned["Funnel_Stage"].iloc[0] == "Unknown"
    assert report["warnings"] == []

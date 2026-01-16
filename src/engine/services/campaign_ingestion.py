"""Enterprise-grade campaign data ingestion helpers."""
from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional, Tuple

import pandas as pd
from loguru import logger

from src.core.utils.data_loader import DataLoader, normalize_campaign_dataframe

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
SAMPLE_DATA_PATH = DATA_DIR / "historical_campaigns_sample.csv"

REQUIRED_COLUMNS = ["Campaign_Name", "Platform", "Spend"]
RECOMMENDED_COLUMNS = ["Conversions", "Revenue", "Date", "Placement"]


def validate_campaign_dataframe(df: pd.DataFrame) -> Dict[str, list]:
    """Validate essential columns and data quality for enterprise ingestion."""
    report = {
        "missing_required": [col for col in REQUIRED_COLUMNS if col not in df.columns],
        "missing_recommended": [col for col in RECOMMENDED_COLUMNS if col not in df.columns],
        "alerts": [],
    }

    if "Spend" in df.columns and (df["Spend"].fillna(0) < 0).any():
        report["alerts"].append("Detected negative values in Spend column")
    if "Conversions" in df.columns and (df["Conversions"].fillna(0) < 0).any():
        report["alerts"].append("Conversions column contains negative values")
    if df.duplicated().any():
        report["alerts"].append("Duplicate rows detected")

    return report


def process_campaign_dataframe(
    df: pd.DataFrame,
    metadata: Optional[Dict] = None,
) -> Tuple[pd.DataFrame, Dict, Dict]:
    """Normalize, validate, and log metadata for a campaign dataframe."""
    metadata = metadata or {}
    df = normalize_campaign_dataframe(df)
    metadata.update({"rows": len(df), "columns": len(df.columns)})

    validation = validate_campaign_dataframe(df)
    if validation["missing_required"]:
        validation["status"] = "error"
    elif validation["alerts"] or validation["missing_recommended"]:
        validation["status"] = "warn"
    else:
        validation["status"] = "ok"

    logger.info(
        "Data ingestion completed",
        **metadata,
        validation_status=validation["status"],
        missing_required=len(validation["missing_required"]),
        missing_recommended=len(validation["missing_recommended"]),
        alerts=len(validation["alerts"]),
    )

    return df, metadata, validation


def ingest_campaign_data(uploaded_file, source: str = "auto_analysis") -> Tuple[Optional[pd.DataFrame], Dict, Dict]:
    """Centralized ingestion pipeline with logging and validation."""
    metadata = {
        "source": source,
        "file_name": getattr(uploaded_file, "name", "unknown"),
        "file_type": getattr(uploaded_file, "type", "unknown"),
        "file_size": getattr(uploaded_file, "size", 0),
    }

    df, error = DataLoader.load_from_streamlit_upload(uploaded_file, validate=True, fix_column_names=False)
    if error:
        logger.error("Data ingestion failed", **metadata, error=error)
        return None, metadata, {"error": error}

    return process_campaign_dataframe(df, metadata)


def load_sample_campaign_data() -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """Load bundled sample dataset for demos."""
    if not SAMPLE_DATA_PATH.exists():
        return None, "Sample dataset not found"

    df, error = DataLoader.load_csv(SAMPLE_DATA_PATH, validate=False, fix_column_names=False)
    if error:
        return None, error

    df, _, _ = process_campaign_dataframe(df, {"source": "sample", "file_name": SAMPLE_DATA_PATH.name})
    return df, None

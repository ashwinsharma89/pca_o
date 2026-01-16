"""Run ingestion regression tests for streamlit_app_hitl enterprise loader."""
import os
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from streamlit_app_hitl import _process_campaign_dataframe

DATA_DIR = ROOT / "tmp_test_data"
DATA_DIR.mkdir(exist_ok=True)


def log_result(name, validation, metadata):
    print("-" * 90)
    print(f"TEST: {name}")
    print(f"Status: {validation['status']}")
    if validation["missing_required"]:
        print(f"Missing required: {validation['missing_required']}")
    if validation["missing_recommended"]:
        print(f"Missing recommended: {validation['missing_recommended']}")
    if validation["alerts"]:
        print(f"Alerts: {validation['alerts']}")
    print(f"Rows: {metadata.get('rows')} | Columns: {metadata.get('columns')}")
    print("-")


def run_dataframe_test(name, df: pd.DataFrame):
    df_out, metadata, validation = _process_campaign_dataframe(df, {"source": name})
    log_result(name, validation, metadata)
    return df_out, metadata, validation


def run_file_test(name, df: pd.DataFrame, file_type: str):
    if file_type == "csv":
        path = DATA_DIR / f"{name}.csv"
        df.to_csv(path, index=False)
        df_loaded = pd.read_csv(path)
    elif file_type == "excel":
        path = DATA_DIR / f"{name}.xlsx"
        with pd.ExcelWriter(path, engine="xlsxwriter") as writer:
            df.to_excel(writer, sheet_name="Sheet1", index=False)
            (df.iloc[:2]).to_excel(writer, sheet_name="Summary", index=False)
        df_loaded = pd.read_excel(path, sheet_name="Sheet1")
    else:
        raise ValueError("Unsupported file type")

    return run_dataframe_test(name, df_loaded)


def main():
    # 1. Baseline sample CSV
    sample_path = Path(r"C:\Users\asharm08\OneDrive - dentsu\Desktop\sample_data.csv")
    df_sample = pd.read_csv(sample_path)
    run_dataframe_test("sample_csv", df_sample)

    # 2. Excel multi-sheet
    run_file_test("excel_multisheet", df_sample, "excel")

    # 3. Missing Platform column
    df_missing = df_sample.drop(columns=["channel"], errors="ignore")
    run_dataframe_test("missing_platform", df_missing)

    # 4. Negative spend values
    df_negative = df_sample.copy()
    df_negative.loc[0, "spend"] = -1000
    run_dataframe_test("negative_spend", df_negative)

    # 5. Empty dataset
    df_empty = pd.DataFrame(columns=df_sample.columns)
    run_dataframe_test("empty_dataset", df_empty)

    # 6. Duplicate rows
    df_dupes = pd.concat([df_sample, df_sample.iloc[[0]]], ignore_index=True)
    run_dataframe_test("duplicate_rows", df_dupes)

    # 7. Missing Spend and Conversions (recommended columns)
    df_missing_metrics = df_sample.drop(columns=["spend", "conversions"], errors="ignore")
    run_dataframe_test("missing_metrics", df_missing_metrics)


if __name__ == "__main__":
    main()

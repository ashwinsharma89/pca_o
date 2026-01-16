# Reporting Automation Agent Integration Guide

This guide explains how to use the new **Campaign Pacing Tracker Auto-Update** agent to transform normalized campaign data (CSV/DB extract) into an updated Excel pacing workbook or downstream PPTX.

## Files

- `scripts/reporting/update_pacing_tracker.py` – Python utility that ingests standardized CSV data and updates the pacing Excel template automatically (daily + weekly tabs).
- `campaign_pacing_tracker.xlsx` – Template with formulas and pacing visualizations (place in `templates/` or `data/` as needed).
- `multi_platform_reporting_template.csv` – Example CSV produced by the reporting agent (100 rows across Google, Bing, Meta, YouTube, Snapchat for Jul–Dec 2024).

## Quick Start

```bash
python scripts/reporting/update_pacing_tracker.py \
    data/multi_platform_reporting_template.csv \
    templates/campaign_pacing_tracker.xlsx \
    outputs/campaign_pacing_tracker_updated.xlsx
```

### Script Arguments
| Argument | Required | Description |
| --- | --- | --- |
| `csv` | ✅ | Path to normalized campaign CSV (must include Date, Platform, Impressions, Clicks, Spend_USD, Conversions, Revenue_USD). |
| `template` | ✅ | Path to the pacing Excel template (Daily Pacing + Weekly Pacing sheets). |
| `--output` | optional | Output path for updated workbook (defaults to overwriting the template). |

## Automation Flow
1. **Data ingestion** – Pull data from DB/API and normalize to CSV schema (Date, Platform, Channels, etc.).
2. **Run agent** – Call `update_pacing_tracker.update_pacing_tracker(csv, template, output)` from Python or invoke the CLI.
3. **Excel update** – Script populates daily & weekly tabs, refreshes formulas, and updates campaign header metadata.
4. **Downstream outputs** – Updated workbook can feed PPTX generation (e.g., existing reporting workflows or `streamlit_reporting.py`).

## Integration Options
- **Python import**:
  ```python
  from scripts.reporting.update_pacing_tracker import update_pacing_tracker
  update_pacing_tracker("data.csv", "template.xlsx", "outputs/updated.xlsx")
  ```
- **Subprocess**:
  ```python
  subprocess.run([
      "python", "scripts/reporting/update_pacing_tracker.py",
      "data.csv", "template.xlsx", "outputs/updated.xlsx"
  ], check=True)
  ```
- **LangGraph / PCA Agent**: add tool node that wraps the function to automate XLSX generation before PPTX export.

## Output Summary
The script prints totals and returns a dict containing:
- `records_processed`, `daily_records`, `weekly_records`
- `start_date`, `end_date`, `total_spend`, `total_revenue`, `platforms`
- `output_file`

## Next Steps
- Schedule the agent (cron or orchestration) to refresh pacing trackers daily.
- Extend `update_pacing_tracker.py` to push results into PPTX via existing reporting modules (`streamlit_reporting.py`).
- Add alerting logic (e.g., ROAS or pacing index thresholds) before distributing reports.

# Regression Analytics Suite

A comprehensive regression analysis framework for marketing campaign data.

## Quick Start

```python
# Simple regression
from src.engine.analytics.regression_pipeline import RegressionPipeline, InsightAgent

pipeline = RegressionPipeline(models_to_run=['Ridge', 'Lasso'], quick_mode=True)
result = pipeline.run(df, 'conversions', ['spend', 'clicks', 'impressions'])

# Generate insights
agent = InsightAgent()
insights = agent.generate_insights(result)
print(insights['executive_summary'])
```

## API Endpoints

### 1. Enhanced Regression (`/regression/v2`)

**Endpoint:** `GET /api/v1/regression/v2`

**Parameters:**

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `target` | string | Yes | - | Target metric (e.g., 'conversions', 'roas') |
| `features` | string | Yes | - | Comma-separated features (e.g., 'spend,clicks') |
| `models` | string | No | "OLS,Ridge,Lasso,Bayesian" | Models to run |
| `prefer_interpretability` | bool | No | false | Prefer linear model over max accuracy |
| `quick_mode` | bool | No | true | Use faster hyperparameter search |
| `export_excel` | bool | No | false | Generate Excel dashboard |
| `start_date` | string | No | - | Filter start date (YYYY-MM-DD) |
| `end_date` | string | No | - | Filter end date (YYYY-MM-DD) |
| `platforms` | string | No | - | Comma-separated platforms to filter |

**Example Request:**
```bash
curl -X GET "http://localhost:8000/api/v1/regression/v2?\
target=conversions&\
features=spend,impressions,clicks&\
models=Ridge,Lasso,XGBoost&\
quick_mode=true" \
-H "Authorization: Bearer $TOKEN"
```

**Example Response:**
```json
{
  "success": true,
  "model": {
    "type": "Ridge",
    "r2": 0.76,
    "rmse": 115.3,
    "target": "conversions",
    "target_type": "conversions"
  },
  "insights": {
    "executive_summary": "Ridge regression achieves 76% R² explaining conversion variance...",
    "recommendations": [
      "Maintain current spend allocation on Meta",
      "Consider increasing Google investment"
    ]
  },
  "model_comparison": [
    {"Model": "Ridge", "R² (Test)": 0.76, "RMSE": 115.3},
    {"Model": "Lasso", "R² (Test)": 0.74, "RMSE": 120.1}
  ],
  "feature_importance": [
    {"feature": "spend", "rank": 1, "avg_importance": 0.45},
    {"feature": "clicks", "rank": 2, "avg_importance": 0.32}
  ]
}
```

---

### 2. Dimension Importance (`/dimension-importance`)

**Endpoint:** `GET /api/v1/dimension-importance`

**Parameters:**

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `target` | string | Yes | - | Target metric to analyze |
| `dimensions` | string | No | - | Comma-separated dimensions (auto-detect if not provided) |
| `include_interactions` | bool | No | true | Include dimension interaction analysis |
| `start_date` | string | No | - | Filter start date |
| `end_date` | string | No | - | Filter end date |
| `platforms` | string | No | - | Comma-separated platforms to filter |

**Example Request:**
```bash
curl -X GET "http://localhost:8000/api/v1/dimension-importance?\
target=conversions&\
include_interactions=true" \
-H "Authorization: Bearer $TOKEN"
```

**Example Response:**
```json
{
  "success": true,
  "target_metric": "conversions",
  "sample_size": 15000,
  "dimensions_analyzed": ["platform", "objective", "device", "audience"],
  "rankings": [
    {"Rank": 1, "Dimension": "objective", "Importance Score": 52, "Effect": "Large"},
    {"Rank": 2, "Dimension": "platform", "Importance Score": 43, "Effect": "Small"}
  ],
  "dimension_details": [
    {
      "dimension": "objective",
      "importance_score": 52,
      "effect_size": 0.149,
      "effect": "Large",
      "top_values": [
        {"value": "conversions", "mean": 60.3, "count": 5000}
      ],
      "recommendation": "Critical driver. Focus on 'conversions' for best performance."
    }
  ],
  "interactions": [
    {"dimension_1": "platform", "dimension_2": "objective", "f_statistic": 16.5}
  ],
  "recommendations": [
    "Primary driver: 'objective' (importance=52%). Top performing: 'conversions'."
  ]
}
```

---

## Available Models

| Model | Use Case | Training Time |
|-------|----------|---------------|
| OLS | Baseline, interpretability | < 1s |
| Ridge | Multicollinearity, all features | < 1s |
| Lasso | Feature selection | < 1s |
| Elastic Net | Balance of Ridge + Lasso | < 1s |
| Bayesian | Uncertainty estimates | < 5s |
| Random Forest | Non-linear patterns | < 30s |
| XGBoost | Maximum accuracy | < 60s |

## Model Selection Logic

1. **Primary:** Highest R² on test set
2. **Tiebreaker 1:** Lowest RMSE (if R² within 2%)
3. **Tiebreaker 2:** Lowest training time
4. **Override:** `prefer_interpretability=true` → select from linear models

## Campaign Features

```python
from src.engine.analytics.campaign_features import CampaignFeaturePipeline

pipeline = CampaignFeaturePipeline()
df_enriched, metadata = pipeline.transform(df, 'conversions', feature_cols)

# Features added:
# - Temporal: ctr_lag_1d, spend_rolling_7d_avg, campaign_phase
# - Budget: budget_utilization_pct, spend_velocity
# - Learning: is_in_learning_phase
# - Platform: platform_quality_score
```

## Error Handling

| Error | Condition | API Response |
|-------|-----------|--------------|
| Insufficient data | < 1000 rows | `success: false, error: "Insufficient data"` |
| Poor model fit | R² < 0.2 | Returns warning in `warnings` array |
| Missing target | Column not found | `success: false, error: "Target not found"` |

## Testing

```bash
# Run all analytics tests
pytest tests/unit/analytics/ -v

# Run specific test file
pytest tests/unit/analytics/test_regression_pipeline.py -v

# Run with coverage
pytest tests/unit/analytics/ --cov=src/engine/analytics
```

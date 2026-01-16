# 🔬 Smart Performance Diagnostics Guide

## Overview

The Smart Performance Diagnostics layer provides advanced causal analysis and ML-powered driver analysis to help you understand **what's really driving your campaign performance**.

## Features

### 1. 🎯 Causal Analysis

**Purpose:** Answer questions like:
- "What caused ROAS to drop?"
- "Which lever contributed most to CPA increase?"
- "Why did CTR improve this week?"

**How it works:**
- Compares two time periods (before/after a split date)
- Breaks down metric changes into component contributions
- Uses mathematical decomposition to identify root causes

**Supported Metrics:**
- **ROAS** → Decomposed into: Conversion Volume, AOV, Spend Efficiency
- **CPA** → Decomposed into: CPC, CVR
- **CTR** → Decomposed into: Click Volume, Impression Volume
- **CVR** → Decomposed into: Conversion Volume, Click Volume
- **CPC** → Decomposed into: Spend Change, Click Volume
- **CPM** → Decomposed into: Spend Change, Impression Volume

**Example Output:**
```
Metric: ROAS
Total Change: +0.85 (+42.5%)
Root Cause: Conversion Volume (65% contribution)

Component Breakdown:
- Conversion Volume: +0.55 (65%)
- Average Order Value: +0.20 (24%)
- Spend Efficiency: +0.10 (11%)

Confidence: 85%
```

### 2. 🔍 Driver Analysis

**Purpose:** Use ML to identify what actually moves your metrics
- "Which features have the biggest impact on ROAS?"
- "What's driving CPA changes?"
- "How much did audience quality contribute to performance?"

**How it works:**
1. **XGBoost Model:** Trains a gradient boosting model on your data
2. **SHAP Values:** Calculates feature importance using SHAP (SHapley Additive exPlanations)
3. **Feature Ranking:** Identifies top drivers with quantified impact

**Example Output:**
```
Target Metric: ROAS
Model Quality: 🟢 78% (R²)

Top Drivers:
1. Audience_Quality (Meta) - 45% contribution ↑ Positive
2. Creative_Type - 25% contribution ↑ Positive
3. Bid_Strategy - 18% contribution ↓ Negative
4. Day_of_Week - 8% contribution ↑ Positive
5. Device_Type - 4% contribution ↑ Positive

Key Insights:
✅ High confidence model (R² = 78%)
🎯 45% of ROAS variation explained by Audience_Quality
📊 Creative_Type is the second key driver (25% contribution)
💡 Actionable levers identified: Audience_Quality, Bid_Strategy
```

## Usage Guide

### Step 1: Navigate to Diagnostics

1. Upload your campaign data
2. Go to **Diagnostics** page in the sidebar
3. Choose between **Causal Analysis** or **Driver Analysis**

### Step 2: Causal Analysis

**Configuration:**
1. **Select Metric:** Choose the metric you want to analyze (ROAS, CPA, CTR, etc.)
2. **Date Column:** Select your date column
3. **Lookback Period:** Set how many days to compare (default: 30)
4. **Split Date:** 
   - Auto-detect (recommended): Automatically finds the midpoint
   - Manual: Choose a specific date to split before/after

**Run Analysis:**
- Click "🔍 Analyze Root Cause"
- View results:
  - **Waterfall Chart:** Visual breakdown of components
  - **Component Table:** Detailed contribution percentages
  - **Key Insights:** Actionable recommendations

### Step 3: Driver Analysis

**Configuration:**
1. **Target Metric:** Select the metric to analyze
2. **Feature Selection:**
   - Auto-select (recommended): Automatically chooses relevant features
   - Manual: Select specific features to include
3. **Categorical Features:** Optionally include categorical variables (Platform, Campaign Type, etc.)

**Run Analysis:**
- Click "🚀 Analyze Drivers"
- View results:
  - **Model Quality:** R² score showing prediction accuracy
  - **Feature Importance Chart:** Visual ranking of drivers
  - **Top Drivers Table:** Detailed impact scores
  - **SHAP Values:** Advanced ML explanations (if available)

## Technical Details

### Causal Decomposition Formulas

**ROAS Decomposition:**
```
ROAS = Revenue / Spend = (Conversions × AOV) / Spend

Components:
- Conversion Volume Impact = (ΔConversions × AOV_before) / Spend_before
- AOV Impact = (Conversions_after × ΔAOV) / Spend_before
- Spend Efficiency = -(Revenue_after × ΔSpend) / (Spend_before × Spend_after)
```

**CPA Decomposition:**
```
CPA = Spend / Conversions = CPC / CVR

Components:
- CPC Impact = ΔCPC / CVR_before
- CVR Impact = -CPC_before × ΔCVR / (CVR_before × CVR_after)
```

### ML Model Details

**XGBoost Configuration:**
- n_estimators: 100
- max_depth: 5
- learning_rate: 0.1
- Objective: Regression

**SHAP Explanation:**
- Uses TreeExplainer for fast computation
- Calculates average absolute SHAP values per feature
- Provides both global (feature importance) and local (per-prediction) explanations

**Confidence Scoring:**
- Based on sample size and statistical significance
- Uses t-test for period comparison
- Score range: 0-1 (0% - 100%)

## Best Practices

### For Causal Analysis:

1. **Choose Meaningful Split Dates:**
   - Campaign launch dates
   - Major optimization changes
   - External events (holidays, market changes)

2. **Sufficient Data:**
   - Minimum 10 days per period
   - More data = higher confidence

3. **Interpret Confidence:**
   - 🟢 > 70%: High confidence
   - 🟡 40-70%: Moderate confidence
   - 🔴 < 40%: Low confidence (need more data)

### For Driver Analysis:

1. **Feature Selection:**
   - Include actionable features (spend, bids, audience)
   - Avoid highly correlated features
   - Use categorical encoding for non-numeric features

2. **Model Quality:**
   - R² > 70%: Excellent model
   - R² 40-70%: Good model
   - R² < 40%: Consider adding more features or data

3. **Actionability:**
   - Focus on top 3-5 drivers
   - Prioritize features you can control
   - Use insights to inform optimization strategy

## Example Use Cases

### Use Case 1: ROAS Drop Investigation

**Scenario:** ROAS dropped from 3.5 to 2.8 last week

**Steps:**
1. Go to Causal Analysis
2. Select ROAS as metric
3. Set split date to start of last week
4. Run analysis

**Result:**
```
Root Cause: Conversion Volume (-60% contribution)
- Conversions dropped 25%
- Spend increased 10%
- AOV remained stable

Action: Investigate conversion funnel and landing page performance
```

### Use Case 2: CPA Optimization

**Scenario:** Want to understand what drives CPA to optimize it

**Steps:**
1. Go to Driver Analysis
2. Select CPA as target metric
3. Include features: Platform, Audience, Creative_Type, Bid_Strategy
4. Run analysis

**Result:**
```
Top Drivers:
1. Bid_Strategy: 35% impact
2. Audience_Quality: 28% impact
3. Creative_Type: 20% impact

Action: Test different bid strategies and refine audience targeting
```

### Use Case 3: Platform Performance Comparison

**Scenario:** Meta performing better than Google - why?

**Steps:**
1. Filter data to Meta only
2. Run Driver Analysis on ROAS
3. Repeat for Google
4. Compare top drivers

**Result:**
```
Meta Top Driver: Audience_Quality (45%)
Google Top Driver: Keyword_Relevance (38%)

Action: Apply Meta's audience insights to Google campaigns
```

## Installation

### Required Libraries:

```bash
pip install -r requirements_diagnostics.txt
```

This installs:
- xgboost (ML model)
- shap (Feature importance)
- scikit-learn (Data preprocessing)
- scipy (Statistical tests)

### Fallback Mode:

If SHAP/XGBoost are not installed:
- Causal Analysis: Works normally
- Driver Analysis: Falls back to correlation-based analysis

## Troubleshooting

### Issue: "Insufficient data for period comparison"

**Solution:**
- Increase lookback period
- Check date column format
- Ensure data covers the selected time range

### Issue: "Low confidence model (R² < 40%)"

**Solution:**
- Add more features
- Collect more data
- Check for data quality issues
- Remove highly correlated features

### Issue: "SHAP not available"

**Solution:**
```bash
pip install xgboost shap
```

### Issue: "Error in causal analysis"

**Solution:**
- Check that required columns exist (Spend, Clicks, Conversions, etc.)
- Verify data types (numeric columns should be numeric)
- Remove rows with missing values

## API Reference

### PerformanceDiagnostics Class

```python
from src.analytics.performance_diagnostics import PerformanceDiagnostics

diagnostics = PerformanceDiagnostics()

# Causal Analysis
breakdown = diagnostics.analyze_metric_change(
    df=campaign_df,
    metric='ROAS',
    date_col='Date',
    split_date='2024-01-15',
    lookback_days=30
)

# Driver Analysis
analysis = diagnostics.analyze_drivers(
    df=campaign_df,
    target_metric='ROAS',
    feature_cols=['Spend', 'Clicks', 'Impressions'],
    categorical_cols=['Platform', 'Campaign_Type']
)
```

## Future Enhancements

- [ ] Multi-period comparison (compare 3+ periods)
- [ ] Automated anomaly detection
- [ ] Predictive "what-if" scenarios
- [ ] Integration with budget optimization
- [ ] Custom metric decomposition formulas
- [ ] Export diagnostic reports to PDF
- [ ] Scheduled diagnostic alerts

## Support

For issues or questions:
1. Check this guide
2. Review example use cases
3. Check logs in `logs/` directory
4. Contact support team

---

**Version:** 1.0  
**Last Updated:** December 2024  
**Author:** PCA Agent Team

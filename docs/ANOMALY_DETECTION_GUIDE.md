# Anomaly Detection Guide - IsolationForest

## Overview

The PCA Agent includes advanced anomaly detection using **IsolationForest**, a powerful machine learning algorithm for detecting outliers in data.

---

## Features

✅ **IsolationForest Algorithm**: ML-based anomaly detection  
✅ **Time Series Detection**: Z-score based anomaly detection  
✅ **Pattern Detection**: Detect sudden spikes, gradual drift, oscillations  
✅ **Multivariate Analysis**: Detect anomalies across multiple correlated metrics  
✅ **Automatic Baseline**: Calculate and track baseline statistics  
✅ **Severity Scoring**: Critical, high, medium, low severity levels  

---

## Quick Start

### 1. Python API

```python
from src.ml.anomaly_detection import anomaly_detector
import pandas as pd

# Prepare historical data
historical_data = pd.DataFrame({
    'timestamp': [...],
    'value': [1000, 1050, 980, 1020, ...]  # Normal values
})

# Train detector
anomaly_detector.train_metric_detector(
    metric_name="campaign_spend",
    historical_data=historical_data,
    contamination=0.1  # Expect 10% anomalies
)

# Detect anomalies in new data
new_data = pd.DataFrame({
    'timestamp': [datetime.now()],
    'value': [5000]  # Suspicious value
})

result = anomaly_detector.detect_anomaly(
    metric_name="campaign_spend",
    current_data=new_data
)

if result['is_anomaly']:
    print(f"⚠️ Anomaly detected! Severity: {result['severity']}")
```

### 2. REST API

```bash
# Train detector
curl -X POST http://localhost:8000/api/anomaly/train \
  -H "Content-Type: application/json" \
  -d '{
    "metric_name": "campaign_spend",
    "historical_data": [
      {"timestamp": "2024-01-01", "value": 1000},
      {"timestamp": "2024-01-02", "value": 1050},
      {"timestamp": "2024-01-03", "value": 980}
    ],
    "contamination": 0.1
  }'

# Detect anomalies
curl -X POST http://localhost:8000/api/anomaly/detect \
  -H "Content-Type: application/json" \
  -d '{
    "metric_name": "campaign_spend",
    "current_data": [
      {"timestamp": "2024-01-04", "value": 5000}
    ]
  }'
```

---

## Use Cases

### 1. Campaign Spend Monitoring

Detect unusual spending patterns:

```python
# Train on historical spend
anomaly_detector.train_metric_detector(
    metric_name="campaign_spend",
    historical_data=spend_history,
    contamination=0.05
)

# Monitor daily spend
result = anomaly_detector.detect_anomaly(
    metric_name="campaign_spend",
    current_data=today_spend
)

if result['is_anomaly']:
    alert_team(f"Unusual spend: ${result['current_value']}")
```

### 2. Performance Monitoring

Detect API performance issues:

```python
# Train on response times
anomaly_detector.train_metric_detector(
    metric_name="api_response_time",
    historical_data=response_times,
    contamination=0.1
)

# Monitor current performance
result = anomaly_detector.detect_anomaly(
    metric_name="api_response_time",
    current_data=current_response_time
)

if result['severity'] in ['critical', 'high']:
    trigger_alert("API performance degraded")
```

### 3. CTR Anomaly Detection

Detect unusual click-through rates:

```python
# Time series detection
anomalies = anomaly_detector.detect_time_series_anomalies(
    metric_name="ctr",
    time_series=ctr_data,
    window_size=10
)

for anomaly in anomalies:
    if anomaly['severity'] == 'high':
        investigate_campaign(anomaly['timestamp'])
```

### 4. Multi-Metric Analysis

Detect correlated anomalies:

```python
# Detect when spend is high but clicks are low
result = anomaly_detector.detect_multivariate_anomalies(
    data=campaign_data,
    feature_columns=['spend', 'clicks', 'conversions']
)

for anomaly in result['anomalies']:
    print(f"Anomalous pattern at index {anomaly['index']}")
    print(f"Spend: ${anomaly['values']['spend']}")
    print(f"Clicks: {anomaly['values']['clicks']}")
```

---

## Detection Methods

### 1. IsolationForest (ML-Based)

Best for: General anomaly detection

```python
anomaly_detector.train_metric_detector(
    metric_name="metric_name",
    historical_data=data,
    contamination=0.1  # Expected anomaly rate
)

result = anomaly_detector.detect_anomaly(
    metric_name="metric_name",
    current_data=new_data
)
```

**How it works**: Isolates anomalies by randomly selecting features and split values. Anomalies are easier to isolate (require fewer splits).

### 2. Z-Score (Statistical)

Best for: Time series data

```python
anomalies = anomaly_detector.detect_time_series_anomalies(
    metric_name="metric_name",
    time_series=data,
    window_size=10  # Rolling window
)
```

**How it works**: Calculates z-scores using rolling mean and std. Points with |z-score| > 3 are anomalies.

### 3. Pattern Detection

Best for: Specific patterns

```python
# Detect sudden spikes
spikes = anomaly_detector.detect_pattern_anomalies(
    data=data,
    pattern_type="sudden_spike"
)

# Detect gradual drift
drift = anomaly_detector.detect_pattern_anomalies(
    data=data,
    pattern_type="gradual_drift"
)
```

**Patterns supported**:
- `sudden_spike`: Sudden increases/decreases
- `gradual_drift`: Trending changes over time
- `missing_data`: Data gaps
- `oscillation`: Periodic patterns

### 4. Multivariate Detection

Best for: Correlated metrics

```python
result = anomaly_detector.detect_multivariate_anomalies(
    data=data,
    feature_columns=['metric1', 'metric2', 'metric3']
)
```

**How it works**: Analyzes multiple metrics together to find unusual combinations.

---

## API Reference

### Train Detector

```bash
POST /api/anomaly/train
```

**Request**:
```json
{
  "metric_name": "campaign_spend",
  "historical_data": [
    {"timestamp": "2024-01-01", "value": 1000},
    {"timestamp": "2024-01-02", "value": 1050}
  ],
  "contamination": 0.1
}
```

**Response**:
```json
{
  "success": true,
  "message": "Detector trained for campaign_spend",
  "samples": 30,
  "contamination": 0.1
}
```

### Detect Anomaly

```bash
POST /api/anomaly/detect
```

**Request**:
```json
{
  "metric_name": "campaign_spend",
  "current_data": [
    {"timestamp": "2024-01-03", "value": 5000}
  ]
}
```

**Response**:
```json
{
  "success": true,
  "is_anomaly": true,
  "anomaly_score": -0.45,
  "current_value": 5000,
  "baseline_mean": 1000,
  "deviation_from_mean": 4.5,
  "severity": "critical"
}
```

### Time Series Detection

```bash
POST /api/anomaly/detect/timeseries
```

### Pattern Detection

```bash
POST /api/anomaly/detect/pattern
```

### Multivariate Detection

```bash
POST /api/anomaly/detect/multivariate
```

### List Models

```bash
GET /api/anomaly/models
```

---

## Configuration

### Contamination Parameter

The `contamination` parameter controls the expected proportion of anomalies:

- `0.01` (1%): Very strict, only extreme outliers
- `0.05` (5%): Moderate, typical for most use cases
- `0.10` (10%): Lenient, catches more potential anomalies
- `0.20` (20%): Very lenient, may have false positives

**Recommendation**: Start with 0.1 and adjust based on results.

### Window Size (Time Series)

The `window_size` parameter for time series detection:

- Small (5-10): Sensitive to short-term changes
- Medium (10-20): Balanced
- Large (20-50): Focuses on long-term trends

---

## Best Practices

### 1. Train on Clean Data

```python
# Remove known anomalies from training data
clean_data = historical_data[historical_data['value'] < threshold]

anomaly_detector.train_metric_detector(
    metric_name="metric",
    historical_data=clean_data
)
```

### 2. Regular Retraining

```python
# Retrain weekly with latest data
if days_since_training > 7:
    anomaly_detector.train_metric_detector(
        metric_name="metric",
        historical_data=last_30_days_data
    )
```

### 3. Combine Methods

```python
# Use multiple detection methods
ml_result = anomaly_detector.detect_anomaly(...)
ts_anomalies = anomaly_detector.detect_time_series_anomalies(...)

# Alert if both methods agree
if ml_result['is_anomaly'] and len(ts_anomalies) > 0:
    high_confidence_alert()
```

### 4. Set Appropriate Thresholds

```python
# Different severity levels require different actions
if result['severity'] == 'critical':
    immediate_alert()
elif result['severity'] == 'high':
    schedule_investigation()
elif result['severity'] == 'medium':
    log_for_review()
```

---

## Examples

Run the examples:

```bash
python examples/anomaly_detection_example.py
```

This will demonstrate:
1. Campaign spend anomaly detection
2. Time series anomaly detection
3. Pattern anomaly detection
4. Multivariate anomaly detection
5. Real-time monitoring simulation

---

## Troubleshooting

### Issue: Too Many False Positives

**Solution**: Increase contamination parameter

```python
anomaly_detector.train_metric_detector(
    metric_name="metric",
    historical_data=data,
    contamination=0.15  # Increased from 0.1
)
```

### Issue: Missing Real Anomalies

**Solution**: Decrease contamination parameter

```python
contamination=0.05  # Decreased from 0.1
```

### Issue: Model Not Found

**Solution**: Train the model first

```python
# Check if model exists
if "metric_name" not in anomaly_detector.models:
    anomaly_detector.train_metric_detector(...)
```

---

## Conclusion

The IsolationForest-based anomaly detection provides:

✅ **Accurate**: ML-powered detection  
✅ **Fast**: Real-time detection capability  
✅ **Flexible**: Multiple detection methods  
✅ **Scalable**: Handles large datasets  
✅ **Easy to Use**: Simple API  

**Status**: ✅ Production-ready anomaly detection!

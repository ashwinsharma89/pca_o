"""
Anomaly Detection Example
Practical examples using IsolationForest for campaign metrics
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from src.ml.anomaly_detection import anomaly_detector

# ============================================================================
# Example 1: Detect Anomalies in Campaign Spend
# ============================================================================

def example_1_campaign_spend_anomalies():
    """Detect anomalies in campaign spend data."""
    print("=" * 70)
    print("Example 1: Campaign Spend Anomaly Detection")
    print("=" * 70)
    
    # Generate sample historical data (30 days)
    dates = pd.date_range(end=datetime.now(), periods=30, freq='D')
    
    # Normal spend pattern with some noise
    normal_spend = np.random.normal(1000, 100, 30)
    
    # Add a few anomalies
    normal_spend[10] = 5000  # Sudden spike
    normal_spend[20] = 100   # Sudden drop
    
    historical_data = pd.DataFrame({
        'timestamp': dates,
        'value': normal_spend
    })
    
    print("\nHistorical Data (last 5 days):")
    print(historical_data.tail())
    
    # Train detector
    print("\n🔄 Training anomaly detector...")
    anomaly_detector.train_metric_detector(
        metric_name="campaign_spend",
        historical_data=historical_data,
        contamination=0.1  # Expect 10% anomalies
    )
    print("✅ Training complete!")
    
    # Test with new data
    new_data = pd.DataFrame({
        'timestamp': [datetime.now()],
        'value': [4500]  # Suspicious high spend
    })
    
    print("\n🔍 Detecting anomalies in new data...")
    result = anomaly_detector.detect_anomaly(
        metric_name="campaign_spend",
        current_data=new_data
    )
    
    print(f"\nResults:")
    print(f"  Is Anomaly: {result['is_anomaly']}")
    print(f"  Current Value: ${result['current_value']:.2f}")
    print(f"  Baseline Mean: ${result['baseline_mean']:.2f}")
    print(f"  Deviation: {result['deviation_from_mean']:.2f}σ")
    print(f"  Severity: {result['severity']}")
    
    if result['is_anomaly']:
        print(f"\n⚠️  ALERT: Anomalous spend detected!")
        print(f"  Expected: ~${result['baseline_mean']:.2f}")
        print(f"  Actual: ${result['current_value']:.2f}")
    
    print()

# ============================================================================
# Example 2: Detect Time Series Anomalies
# ============================================================================

def example_2_time_series_anomalies():
    """Detect anomalies in time series data."""
    print("=" * 70)
    print("Example 2: Time Series Anomaly Detection")
    print("=" * 70)
    
    # Generate time series with anomalies
    dates = pd.date_range(end=datetime.now(), periods=100, freq='H')
    
    # Normal pattern: sine wave + noise
    normal_pattern = 1000 + 200 * np.sin(np.linspace(0, 4*np.pi, 100)) + np.random.normal(0, 20, 100)
    
    # Add anomalies
    normal_pattern[30] = 2000  # Spike
    normal_pattern[60] = 200   # Drop
    normal_pattern[80] = 1800  # Another spike
    
    time_series = pd.DataFrame({
        'timestamp': dates,
        'value': normal_pattern
    })
    
    print("\n🔍 Detecting time series anomalies...")
    anomalies = anomaly_detector.detect_time_series_anomalies(
        metric_name="ctr",
        time_series=time_series,
        window_size=10
    )
    
    print(f"\n✅ Found {len(anomalies)} anomalies:")
    
    for i, anomaly in enumerate(anomalies[:5], 1):  # Show first 5
        print(f"\n  Anomaly {i}:")
        print(f"    Timestamp: {anomaly['timestamp']}")
        print(f"    Value: {anomaly['value']:.2f}")
        print(f"    Expected: {anomaly['expected_value']:.2f}")
        print(f"    Z-Score: {anomaly['z_score']:.2f}")
        print(f"    Severity: {anomaly['severity']}")
    
    print()

# ============================================================================
# Example 3: Detect Pattern Anomalies
# ============================================================================

def example_3_pattern_anomalies():
    """Detect specific pattern anomalies."""
    print("=" * 70)
    print("Example 3: Pattern Anomaly Detection")
    print("=" * 70)
    
    # Generate data with sudden spike
    dates = pd.date_range(end=datetime.now(), periods=50, freq='D')
    values = np.random.normal(1000, 50, 50)
    
    # Create sudden spike
    values[25] = values[24] * 2.5  # 150% increase
    
    data = pd.DataFrame({
        'timestamp': dates,
        'value': values
    })
    
    print("\n🔍 Detecting sudden spikes...")
    spikes = anomaly_detector.detect_pattern_anomalies(
        data=data,
        pattern_type="sudden_spike"
    )
    
    if spikes:
        print(f"\n⚠️  Found {len(spikes)} sudden spikes:")
        for spike in spikes:
            print(f"\n  Spike Details:")
            print(f"    Value: {spike['value']:.2f}")
            print(f"    Rate of Change: {spike['rate_of_change']*100:.1f}%")
            print(f"    Severity: {spike['severity']}")
    else:
        print("\n✅ No sudden spikes detected")
    
    print()

# ============================================================================
# Example 4: Multivariate Anomaly Detection
# ============================================================================

def example_4_multivariate_anomalies():
    """Detect anomalies across multiple correlated metrics."""
    print("=" * 70)
    print("Example 4: Multivariate Anomaly Detection")
    print("=" * 70)
    
    # Generate correlated metrics
    n_samples = 100
    dates = pd.date_range(end=datetime.now(), periods=n_samples, freq='H')
    
    # Normal correlated data
    spend = np.random.normal(1000, 100, n_samples)
    clicks = spend * 0.1 + np.random.normal(0, 10, n_samples)  # Correlated with spend
    conversions = clicks * 0.05 + np.random.normal(0, 2, n_samples)  # Correlated with clicks
    
    # Add anomalies (high spend but low clicks)
    spend[50] = 2000
    clicks[50] = 50  # Should be ~200
    conversions[50] = 2  # Should be ~10
    
    data = pd.DataFrame({
        'timestamp': dates,
        'spend': spend,
        'clicks': clicks,
        'conversions': conversions
    })
    
    print("\n🔍 Detecting multivariate anomalies...")
    result = anomaly_detector.detect_multivariate_anomalies(
        data=data,
        feature_columns=['spend', 'clicks', 'conversions']
    )
    
    print(f"\nResults:")
    print(f"  Total Points: {result['total_points']}")
    print(f"  Anomalies Detected: {result['anomalies_detected']}")
    print(f"  Anomaly Rate: {result['anomaly_rate']:.1f}%")
    
    if result['anomalies']:
        print(f"\n⚠️  Anomalous data points:")
        for anomaly in result['anomalies'][:3]:  # Show first 3
            print(f"\n  Index {anomaly['index']}:")
            print(f"    Anomaly Score: {anomaly['anomaly_score']:.3f}")
            print(f"    Spend: ${anomaly['values']['spend']:.2f}")
            print(f"    Clicks: {anomaly['values']['clicks']:.0f}")
            print(f"    Conversions: {anomaly['values']['conversions']:.0f}")
    
    print()

# ============================================================================
# Example 5: Real-Time Anomaly Monitoring
# ============================================================================

def example_5_realtime_monitoring():
    """Simulate real-time anomaly monitoring."""
    print("=" * 70)
    print("Example 5: Real-Time Anomaly Monitoring")
    print("=" * 70)
    
    # Train on historical data
    dates = pd.date_range(end=datetime.now() - timedelta(hours=1), periods=100, freq='H')
    historical_values = np.random.normal(1000, 100, 100)
    
    historical_data = pd.DataFrame({
        'timestamp': dates,
        'value': historical_values
    })
    
    print("\n🔄 Training detector on historical data...")
    anomaly_detector.train_metric_detector(
        metric_name="api_response_time",
        historical_data=historical_data,
        contamination=0.05
    )
    print("✅ Training complete!")
    
    # Simulate real-time monitoring
    print("\n🔍 Monitoring incoming data...")
    print("-" * 70)
    
    # Simulate 10 data points
    for i in range(10):
        # Generate new data point (mostly normal, some anomalies)
        if i == 5:
            value = 2500  # Anomaly
        elif i == 8:
            value = 3000  # Another anomaly
        else:
            value = np.random.normal(1000, 100)
        
        new_data = pd.DataFrame({
            'timestamp': [datetime.now()],
            'value': [value]
        })
        
        result = anomaly_detector.detect_anomaly(
            metric_name="api_response_time",
            current_data=new_data
        )
        
        status = "🚨 ANOMALY" if result['is_anomaly'] else "✅ Normal"
        print(f"Point {i+1}: {value:.0f}ms - {status} (severity: {result['severity']})")
    
    print()

# ============================================================================
# Run All Examples
# ============================================================================

if __name__ == "__main__":
    print("\n" + "=" * 70)
    print("Anomaly Detection Examples - IsolationForest")
    print("=" * 70)
    print()
    
    # Run all examples
    example_1_campaign_spend_anomalies()
    example_2_time_series_anomalies()
    example_3_pattern_anomalies()
    example_4_multivariate_anomalies()
    example_5_realtime_monitoring()
    
    print("=" * 70)
    print("All examples completed!")
    print("=" * 70)

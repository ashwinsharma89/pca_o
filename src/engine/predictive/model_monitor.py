"""
ML Model Monitoring
Tracks model performance, data drift, and prediction quality
"""
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from collections import deque
import numpy as np
import logging

logger = logging.getLogger(__name__)


@dataclass
class PredictionLog:
    """Log of a single prediction"""
    model_id: str
    model_version: str
    input_features: Dict[str, Any]
    prediction: Any
    actual: Optional[Any] = None
    latency_ms: float = 0
    timestamp: datetime = field(default_factory=datetime.utcnow)


class ModelMonitor:
    """
    Monitor ML model performance and health
    
    Features:
    - Prediction latency tracking
    - Accuracy/error monitoring
    - Data drift detection
    - Alerting on degradation
    """
    
    WINDOW_SIZE = 1000  # Keep last N predictions
    
    def __init__(self, model_id: str, model_version: str):
        self.model_id = model_id
        self.model_version = model_version
        self.predictions: deque = deque(maxlen=self.WINDOW_SIZE)
        self.latencies: deque = deque(maxlen=self.WINDOW_SIZE)
        self.errors: deque = deque(maxlen=self.WINDOW_SIZE)
        self.feature_stats: Dict[str, Dict] = {}
        self._baseline_stats: Dict[str, Dict] = {}
    
    def log_prediction(
        self,
        input_features: Dict[str, Any],
        prediction: Any,
        latency_ms: float,
        actual: Any = None
    ) -> PredictionLog:
        """Log a prediction for monitoring"""
        log = PredictionLog(
            model_id=self.model_id,
            model_version=self.model_version,
            input_features=input_features,
            prediction=prediction,
            actual=actual,
            latency_ms=latency_ms
        )
        
        self.predictions.append(log)
        self.latencies.append(latency_ms)
        
        if actual is not None:
            error = abs(prediction - actual) if isinstance(prediction, (int, float)) else (prediction != actual)
            self.errors.append(error)
        
        # Update feature stats
        self._update_feature_stats(input_features)
        
        return log
    
    def _update_feature_stats(self, features: Dict[str, Any]):
        """Update running statistics for features"""
        for name, value in features.items():
            if not isinstance(value, (int, float)):
                continue
            
            if name not in self.feature_stats:
                self.feature_stats[name] = {"values": deque(maxlen=self.WINDOW_SIZE)}
            
            self.feature_stats[name]["values"].append(value)
    
    def set_baseline(self):
        """Set current statistics as baseline for drift detection"""
        for name, stats in self.feature_stats.items():
            values = list(stats["values"])
            if values:
                self._baseline_stats[name] = {
                    "mean": np.mean(values),
                    "std": np.std(values),
                    "min": np.min(values),
                    "max": np.max(values)
                }
        logger.info(f"Baseline set for model {self.model_id}")
    
    def get_latency_stats(self) -> Dict:
        """Get latency statistics"""
        if not self.latencies:
            return {"error": "No data"}
        
        latencies = list(self.latencies)
        return {
            "count": len(latencies),
            "mean_ms": np.mean(latencies),
            "p50_ms": np.percentile(latencies, 50),
            "p95_ms": np.percentile(latencies, 95),
            "p99_ms": np.percentile(latencies, 99),
            "max_ms": np.max(latencies)
        }
    
    def get_error_stats(self) -> Dict:
        """Get error statistics"""
        if not self.errors:
            return {"error": "No labeled data"}
        
        errors = list(self.errors)
        return {
            "count": len(errors),
            "mean_error": np.mean(errors),
            "std_error": np.std(errors),
            "max_error": np.max(errors)
        }
    
    def detect_drift(self, threshold: float = 2.0) -> Dict[str, Dict]:
        """
        Detect data drift using multiple statistical methods
        
        Args:
            threshold: Number of standard deviations for drift alert
            
        Returns:
            Dict of features with drift detected
        """
        drift_report = {}
        
        for name, stats in self.feature_stats.items():
            if name not in self._baseline_stats:
                continue
            
            baseline = self._baseline_stats[name]
            current_values = list(stats["values"])
            
            if not current_values or baseline["std"] == 0:
                continue
            
            current_mean = np.mean(current_values)
            z_score = abs(current_mean - baseline["mean"]) / baseline["std"]
            
            # PSI (Population Stability Index)
            psi = self._calculate_psi(
                list(stats["values"]), 
                baseline.get("baseline_values", current_values)
            )
            
            # KL Divergence
            kl_div = self._calculate_kl_divergence(
                current_values,
                baseline.get("baseline_values", current_values)
            )
            
            if z_score > threshold or psi > 0.2:
                drift_report[name] = {
                    "baseline_mean": baseline["mean"],
                    "current_mean": current_mean,
                    "z_score": z_score,
                    "psi": psi,
                    "kl_divergence": kl_div,
                    "drift_detected": True,
                    "drift_severity": self._classify_drift_severity(psi, z_score)
                }
        
        if drift_report:
            logger.warning(f"Data drift detected in {len(drift_report)} features")
        
        return drift_report
    
    def _calculate_psi(self, actual: List[float], expected: List[float], buckets: int = 10) -> float:
        """
        Calculate Population Stability Index (PSI)
        
        PSI < 0.1: No significant change
        0.1 <= PSI < 0.2: Moderate change
        PSI >= 0.2: Significant change
        """
        if not actual or not expected:
            return 0.0
        
        try:
            # Create histogram bins
            breakpoints = np.linspace(
                min(min(actual), min(expected)),
                max(max(actual), max(expected)),
                buckets + 1
            )
            
            # Count frequencies
            actual_counts = np.histogram(actual, bins=breakpoints)[0]
            expected_counts = np.histogram(expected, bins=breakpoints)[0]
            
            # Convert to proportions
            actual_props = actual_counts / len(actual) + 1e-10
            expected_props = expected_counts / len(expected) + 1e-10
            
            # Calculate PSI
            psi = np.sum((actual_props - expected_props) * np.log(actual_props / expected_props))
            return float(psi)
        except Exception:
            return 0.0
    
    def _calculate_kl_divergence(self, p: List[float], q: List[float], buckets: int = 10) -> float:
        """Calculate KL Divergence between two distributions"""
        if not p or not q:
            return 0.0
        
        try:
            breakpoints = np.linspace(min(min(p), min(q)), max(max(p), max(q)), buckets + 1)
            
            p_hist = np.histogram(p, bins=breakpoints)[0] / len(p) + 1e-10
            q_hist = np.histogram(q, bins=breakpoints)[0] / len(q) + 1e-10
            
            kl = np.sum(p_hist * np.log(p_hist / q_hist))
            return float(kl)
        except Exception:
            return 0.0
    
    def _classify_drift_severity(self, psi: float, z_score: float) -> str:
        """Classify drift severity based on PSI and z-score"""
        if psi >= 0.25 or z_score >= 3.0:
            return "critical"
        elif psi >= 0.1 or z_score >= 2.0:
            return "warning"
        else:
            return "info"
    
    def track_accuracy(self, y_true: float, y_pred: float):
        """Track prediction accuracy over time"""
        if not hasattr(self, 'accuracy_history'):
            self.accuracy_history = deque(maxlen=self.WINDOW_SIZE)
        
        error = abs(y_true - y_pred)
        relative_error = error / (abs(y_true) + 1e-10)
        within_10pct = relative_error <= 0.1
        
        self.accuracy_history.append({
            "y_true": y_true,
            "y_pred": y_pred,
            "error": error,
            "relative_error": relative_error,
            "within_10pct": within_10pct,
            "timestamp": datetime.utcnow()
        })
    
    def get_accuracy_trend(self, window: int = 100) -> Dict:
        """Get accuracy trend over recent predictions"""
        if not hasattr(self, 'accuracy_history') or not self.accuracy_history:
            return {"error": "No accuracy data"}
        
        recent = list(self.accuracy_history)[-window:]
        
        errors = [r["relative_error"] for r in recent]
        within_10pct = [r["within_10pct"] for r in recent]
        
        return {
            "sample_size": len(recent),
            "mean_relative_error": float(np.mean(errors)),
            "median_relative_error": float(np.median(errors)),
            "within_10pct_rate": float(np.mean(within_10pct)),
            "error_trend": "improving" if len(errors) > 10 and np.mean(errors[:10]) > np.mean(errors[-10:]) else "stable"
        }
    
    def export_prometheus_metrics(self) -> str:
        """Export metrics in Prometheus format"""
        metrics = []
        
        # Latency metrics
        latency = self.get_latency_stats()
        if "error" not in latency:
            metrics.append(f'pca_model_latency_mean_ms{{model_id="{self.model_id}"}} {latency["mean_ms"]:.2f}')
            metrics.append(f'pca_model_latency_p95_ms{{model_id="{self.model_id}"}} {latency["p95_ms"]:.2f}')
            metrics.append(f'pca_model_latency_p99_ms{{model_id="{self.model_id}"}} {latency["p99_ms"]:.2f}')
        
        # Prediction count
        metrics.append(f'pca_model_predictions_total{{model_id="{self.model_id}"}} {len(self.predictions)}')
        
        # Drift metrics
        drift = self.detect_drift()
        drift_count = len([d for d in drift.values() if d.get("drift_detected")])
        metrics.append(f'pca_model_drift_features{{model_id="{self.model_id}"}} {drift_count}')
        
        # Accuracy metrics (if available)
        if hasattr(self, 'accuracy_history') and self.accuracy_history:
            acc_trend = self.get_accuracy_trend()
            if "error" not in acc_trend:
                metrics.append(f'pca_model_within_10pct_rate{{model_id="{self.model_id}"}} {acc_trend["within_10pct_rate"]:.4f}')
        
        return "\n".join(metrics)

    
    def get_health_report(self) -> Dict:
        """Get overall model health report"""
        return {
            "model_id": self.model_id,
            "model_version": self.model_version,
            "predictions_logged": len(self.predictions),
            "latency": self.get_latency_stats(),
            "errors": self.get_error_stats() if self.errors else None,
            "drift": self.detect_drift(),
            "timestamp": datetime.utcnow().isoformat()
        }


class ModelMonitorRegistry:
    """Registry of model monitors"""
    
    def __init__(self):
        self.monitors: Dict[str, ModelMonitor] = {}
    
    def get_or_create(self, model_id: str, model_version: str) -> ModelMonitor:
        key = f"{model_id}:{model_version}"
        if key not in self.monitors:
            self.monitors[key] = ModelMonitor(model_id, model_version)
        return self.monitors[key]
    
    def get_all_health_reports(self) -> List[Dict]:
        return [m.get_health_report() for m in self.monitors.values()]


# Global registry
_monitor_registry: Optional[ModelMonitorRegistry] = None

def get_model_monitor(model_id: str, model_version: str) -> ModelMonitor:
    global _monitor_registry
    if _monitor_registry is None:
        _monitor_registry = ModelMonitorRegistry()
    return _monitor_registry.get_or_create(model_id, model_version)

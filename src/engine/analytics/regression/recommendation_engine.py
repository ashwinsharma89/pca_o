"""
Recommendation Engine V2 - Actionable Insights from Regression Output

Translates complex ML coefficients into structured "Scale/Hold/Cut" strategies.
Uses 4 dimensions for decision making:
1. Direction (Coefficient sign)
2. Importance (SHAP value)
3. Confidence (Model performance + VIF)
4. Efficiency (ROI proxy)

Author: Senior ML Expert
"""

from dataclasses import dataclass
from typing import List, Dict, Any, Optional
import pandas as pd
from loguru import logger
from .pipeline import RegressionResult

# Phase 4: Observability
from src.core.utils.opentelemetry_config import get_tracer
from src.core.utils.observability import metrics

tracer = get_tracer(__name__)


@dataclass
class Recommendation:
    """A structured marketing recommendation."""
    feature: str
    strategy: str  # "Scale", "Hold", "Cut", "Optimize"
    impact_score: float  # 0-100
    reasoning: str
    action: str
    confidence: str  # "High", "Moderate", "Low"

class RecommendationEngineV2:
    """
    Advanced recommendation engine that turns regression results into 
    budget allocation strategies.
    """

    @staticmethod
    def generate(result: RegressionResult) -> List[Dict[str, Any]]:
        """
        Generate a list of actionable recommendations from regression results.
        """
        if tracer:
            with tracer.start_as_current_span("RecommendationEngineV2.generate") as span:
                recs = RecommendationEngineV2._generate(result)
                span.set_attribute("recommendations.count", len(recs))
                return recs
        else:
            return RecommendationEngineV2._generate(result)

    @staticmethod
    def _generate(result: RegressionResult) -> List[Dict[str, Any]]:
        """Internal generation logic."""
        recommendations = []

        
        # 1. Evaluate Model Confidence
        model_reliability = "High" if result.metrics.r2_test > 0.7 else "Moderate" if result.metrics.r2_test > 0.4 else "Low"
        
        # 2. Analyze individual features
        # We focus on 'spend' related features primarily for budget allocation
        for feat, coef in result.coefficients.items():
            # Skip non-spend features for budget strategy (keep it simple for now)
            # or handle them as 'Optimize'
            is_budget_lever = "spend" in feat.lower()
            
            # Get importance from SHAP if available, else use absolute coefficient
            importance = 0
            if result.shap_data and "summary" in result.shap_data:
                # Find feature in shap summary
                for s in result.shap_data["summary"]:
                    if s["feature"] == feat:
                        importance = s["mean_abs_shap"]
                        break
            
            # If no SHAP, use a normalized version of coefficient impact
            if importance == 0:
                importance = abs(coef)

            # Check for collinearity risk
            vif = result.vif_analysis.get(feat, 1.0)
            collinearity_risk = vif > 5.0
            
            # Determine Strategy
            strategy = "Hold"
            reasoning = ""
            action = ""
            impact_score = min(100, importance * 10) # Simple scaling

            if coef > 0:
                if importance > 0.1 and not collinearity_risk:
                    strategy = "Scale"
                    reasoning = f"{feat} shows a strong positive correlation with {result.best_model_name} predictions."
                    action = f"Increase {feat} budget cautiously (+10-15%) to capture additional volume."
                else:
                    strategy = "Hold"
                    reasoning = f"{feat} has positive impact but confidence is limited by { 'multicollinearity' if collinearity_risk else 'low importance'}."
                    action = f"Maintain {feat} spend. Model shows positive but unstable impact."
            elif coef < 0:
                if importance > 0.05:
                    strategy = "Cut"
                    reasoning = f"{feat} appears to have diminishing returns or negative correlation in current spend levels."
                    action = f"Reduce {feat} budget by 20% or audit creative/audience performance."
                else:
                    strategy = "Optimize"
                    reasoning = f"{feat} has negligible or slightly negative impact."
                    action = f"Re-evaluate {feat} targeting. Impact is currently below statistically significant thresholds."
            else:
                strategy = "Hold"
                reasoning = f"No significant impact detected for {feat} in the current period."
                action = f"Maintain baseline spend while collecting more data."

            # Adjust confidence based on VIF and Model R2
            feat_confidence = model_reliability
            if collinearity_risk:
                feat_confidence = "Low"

            recommendations.append({
                "feature": feat,
                "strategy": strategy,
                "impact_score": round(float(impact_score), 2),
                "reasoning": reasoning,
                "action": action,
                "confidence": feat_confidence
            })

        # IDs and Metrics
        import uuid
        for rec in recommendations:
            rec["id"] = str(uuid.uuid4())
            # Phase 4 Metrics: Strategy Distribution
            metrics.increment("marketing_recommendation_total", labels={"strategy": rec["strategy"]})
            
        # Layer 9: Persist recommendations
        RecommendationEngineV2._persist(recommendations, result)
            
        return recommendations

    @staticmethod
    def _persist(recs: List[Dict[str, Any]], result: RegressionResult):
        """Persist recommendations to DuckDB for tracking."""
        try:
            from src.core.database.duckdb_manager import get_duckdb_manager
            import json
            db = get_duckdb_manager()
            with db.connection() as conn:
                for rec in recs:
                    # Filter details for specific recommendation
                    vif_val = result.vif_analysis.get(rec["feature"], 1.0)
                    details = json.dumps({
                        "r2": float(result.metrics.r2_test),
                        "vif": float(vif_val),
                        "best_model": result.best_model_name,
                        "reasoning": rec["reasoning"]
                    })
                    conn.execute("""
                        INSERT INTO recommendation_history 
                        (id, timestamp, target_metric, feature_name, suggested_strategy, recommendation_score, details)
                        VALUES (?, CURRENT_TIMESTAMP, ?, ?, ?, ?, ?)
                    """, (
                        rec["id"], 
                        result.target_col, 
                        rec["feature"], 
                        rec["strategy"], 
                        float(rec["impact_score"]), 
                        details
                    ))
            logger.info(f"Persisted {len(recs)} recommendations to DuckDB")
        except Exception as e:
            logger.error(f"Failed to persist recommendations: {e}")



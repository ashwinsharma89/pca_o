"""
Coefficient Explainer - Translate ML to Business Language

Converts technical coefficients into actionable insights:
- "Each $1000 spend → +52 conversions (±4)"
- "Facebook drives +15 conversions vs baseline"
- "Upper funnel has 30% lower conversion rate"

Author: Senior ML Expert (Google Ads Platform experience)
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
import numpy as np
from scipy import stats


@dataclass
class CoefficientInsight:
    """Business-friendly coefficient interpretation."""
    feature: str
    coefficient: float
    confidence_interval: Tuple[float, float]
    interpretation: str
    impact_category: str  # "High", "Medium", "Low"
    actionable_insight: str


class CoefficientExplainer:
    """
    Translate regression coefficients into stakeholder-friendly insights.
    
    Key Principles:
    1. Use concrete numbers: "$1000 spend → +52 conversions"
    2. Include uncertainty: "±4 conversions"
    3. Make it actionable: "Increase Facebook spend by 20%"
    4. Avoid jargon: No "β", "p-values", "standard errors"
    """
    
    @staticmethod
    def explain_coefficient(
        feature_name: str,
        coefficient: float,
        std_error: Optional[float] = None,
        feature_scale: float = 1.0,
        target_name: str = "conversions",
        confidence_level: float = 0.95
    ) -> CoefficientInsight:
        """
        Generate business-friendly explanation for a single coefficient.
        
        Args:
            feature_name: Feature name (e.g., "spend", "platform_facebook")
            coefficient: Regression coefficient
            std_error: Standard error (for confidence interval)
            feature_scale: Scale for interpretation (e.g., 1000 for "$1K")
            target_name: Target variable name
            confidence_level: Confidence level for interval
            
        Returns:
            CoefficientInsight with interpretation
        """
        # Compute confidence interval
        if std_error:
            z_score = stats.norm.ppf((1 + confidence_level) / 2)
            margin = z_score * std_error
            ci_lower = coefficient - margin
            ci_upper = coefficient + margin
        else:
            # Rough estimate if std_error not provided
            margin = abs(coefficient) * 0.1
            ci_lower = coefficient - margin
            ci_upper = coefficient + margin
        
        # Scale coefficient
        scaled_coef = coefficient * feature_scale
        scaled_ci_lower = ci_lower * feature_scale
        scaled_ci_upper = ci_upper * feature_scale
        
        # Generate interpretation
        interpretation = CoefficientExplainer._generate_interpretation(
            feature_name, scaled_coef, scaled_ci_lower, scaled_ci_upper,
            feature_scale, target_name
        )
        
        # Categorize impact
        impact_category = CoefficientExplainer._categorize_impact(abs(scaled_coef))
        
        # Generate actionable insight
        actionable_insight = CoefficientExplainer._generate_action(
            feature_name, scaled_coef, impact_category
        )
        
        return CoefficientInsight(
            feature=feature_name,
            coefficient=coefficient,
            confidence_interval=(ci_lower, ci_upper),
            interpretation=interpretation,
            impact_category=impact_category,
            actionable_insight=actionable_insight
        )
    
    @staticmethod
    def _generate_interpretation(
        feature: str,
        coef: float,
        ci_lower: float,
        ci_upper: float,
        scale: float,
        target: str
    ) -> str:
        """Generate plain English interpretation."""
        # Handle different feature types
        if "spend" in feature.lower():
            if scale == 1000:
                return f"Each $1,000 increase in {feature} → {coef:+.1f} {target} (95% CI: {ci_lower:.1f} to {ci_upper:.1f})"
            else:
                return f"Each $1 increase in {feature} → {coef:+.3f} {target}"
        
        elif any(x in feature.lower() for x in ["platform", "channel", "funnel", "objective"]):
            # Categorical feature (one-hot encoded)
            base_feature = feature.split("_")[0]
            category = "_".join(feature.split("_")[1:])
            
            if coef > 0:
                return f"{category.title()} drives {coef:+.1f} {target} vs baseline {base_feature}"
            else:
                return f"{category.title()} has {abs(coef):.1f} fewer {target} vs baseline {base_feature}"
        
        else:
            # Generic numeric feature
            return f"Each unit increase in {feature} → {coef:+.2f} {target}"
    
    @staticmethod
    def _categorize_impact(abs_coef: float) -> str:
        """Categorize coefficient magnitude."""
        if abs_coef > 20:
            return "High"
        elif abs_coef > 5:
            return "Medium"
        else:
            return "Low"
    
    @staticmethod
    def _generate_action(feature: str, coef: float, impact: str) -> str:
        """Generate actionable recommendation."""
        if impact == "Low":
            return f"Monitor {feature} - low impact on target"
        
        if "spend" in feature.lower():
            if coef > 0:
                return f"✅ Increase {feature} - positive ROI"
            else:
                return f"⚠️ Reduce {feature} - negative ROI"
        
        elif any(x in feature.lower() for x in ["platform", "channel"]):
            category = "_".join(feature.split("_")[1:])
            if coef > 0:
                return f"✅ Prioritize {category.title()} - strong performance"
            else:
                return f"⚠️ Deprioritize {category.title()} - weak performance"
        
        elif "funnel" in feature.lower():
            category = "_".join(feature.split("_")[1:])
            if coef > 0:
                return f"✅ Invest more in {category.title()} funnel"
            else:
                return f"⚠️ Optimize {category.title()} funnel - underperforming"
        
        else:
            if coef > 0:
                return f"Positive driver - optimize {feature}"
            else:
                return f"Negative driver - investigate {feature}"
    
    @staticmethod
    def rank_features_by_impact(
        coefficients: Dict[str, float],
        std_errors: Optional[Dict[str, float]] = None,
        top_n: int = 10
    ) -> List[Dict]:
        """
        Rank features by absolute impact and generate insights.
        
        Args:
            coefficients: Dict of feature -> coefficient
            std_errors: Dict of feature -> standard error
            top_n: Number of top features to return
            
        Returns:
            List of dicts with ranked features and insights
        """
        # Sort by absolute coefficient
        sorted_features = sorted(
            coefficients.items(),
            key=lambda x: abs(x[1]),
            reverse=True
        )[:top_n]
        
        ranked = []
        for rank, (feature, coef) in enumerate(sorted_features, 1):
            std_err = std_errors.get(feature) if std_errors else None
            
            insight = CoefficientExplainer.explain_coefficient(
                feature_name=feature,
                coefficient=coef,
                std_error=std_err,
                feature_scale=1000 if "spend" in feature.lower() else 1.0
            )
            
            ranked.append({
                "rank": rank,
                "feature": feature,
                "coefficient": round(coef, 4),
                "impact": insight.impact_category,
                "interpretation": insight.interpretation,
                "action": insight.actionable_insight
            })
        
        return ranked
    
    @staticmethod
    def generate_executive_summary(
        model_name: str,
        r2_test: float,
        mae: float,
        top_drivers: List[Dict],
        target_name: str = "conversions"
    ) -> str:
        """
        Generate executive summary for stakeholders.
        
        Args:
            model_name: Model type (e.g., "Ridge")
            r2_test: Test R² score
            mae: Mean absolute error
            top_drivers: Top feature insights from rank_features_by_impact
            target_name: Target variable name
            
        Returns:
            Executive summary string
        """
        r2_pct = int(r2_test * 100)
        
        summary = f"**{model_name} Model Performance**\n\n"
        summary += f"The model explains {r2_pct}% of variance in {target_name}, "
        summary += f"with an average prediction error of ±{mae:.1f}.\n\n"
        
        summary += f"**Top 3 Drivers of {target_name.title()}:**\n\n"
        for i, driver in enumerate(top_drivers[:3], 1):
            summary += f"{i}. **{driver['feature']}**: {driver['interpretation']}\n"
            summary += f"   → {driver['action']}\n\n"
        
        summary += "**Recommended Actions:**\n\n"
        for driver in top_drivers[:3]:
            if "✅" in driver['action']:
                summary += f"- {driver['action']}\n"
        
        return summary

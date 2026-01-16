"""
Causal Analysis Knowledge Base with RAG Integration

Integrates domain knowledge with the causal analysis module to provide:
- Context-aware recommendations
- Best practice guidance
- Method selection assistance
- Interpretation support
"""

import os
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


@dataclass
class KnowledgeChunk:
    """Represents a chunk of knowledge from the knowledge base."""
    content: str
    category: str
    relevance_score: float
    source: str


class CausalKnowledgeBase:
    """
    Knowledge base for causal analysis with RAG capabilities.
    
    Provides domain knowledge to enhance causal analysis with:
    - Method recommendations
    - Best practices
    - Interpretation guidance
    - Common pitfalls
    """
    
    def __init__(self, kb_path: Optional[str] = None):
        """
        Initialize knowledge base.
        
        Args:
            kb_path: Path to knowledge base markdown file
        """
        self.kb_path = kb_path or self._get_default_kb_path()
        self.knowledge = self._load_knowledge()
        
    def _get_default_kb_path(self) -> str:
        """Get default knowledge base path."""
        current_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(os.path.dirname(current_dir))
        return os.path.join(project_root, "knowledge_base", "causal_analysis_knowledge.md")
    
    def _load_knowledge(self) -> Dict[str, Any]:
        """Load and structure knowledge base."""
        
        knowledge = {
            "methods": self._get_causal_methods(),
            "metrics": self._get_metric_guidance(),
            "pitfalls": self._get_common_pitfalls(),
            "best_practices": self._get_best_practices(),
            "interpretations": self._get_interpretation_templates(),
            "recommendations": self._get_recommendation_templates()
        }
        
        return knowledge
    
    def _get_causal_methods(self) -> Dict[str, Dict[str, Any]]:
        """Get information about causal analysis methods."""
        return {
            "ab_testing": {
                "name": "A/B Testing (Randomized Controlled Trial)",
                "when_to_use": [
                    "Testing website changes",
                    "Email campaign variations",
                    "Ad creative testing",
                    "Large sample size available"
                ],
                "pros": [
                    "Gold standard for causality",
                    "Clear cause-effect relationship",
                    "Statistical rigor"
                ],
                "cons": [
                    "Requires large sample sizes",
                    "Not always feasible",
                    "May take time to reach significance"
                ],
                "requirements": {
                    "min_sample_size": 1000,
                    "randomization": True,
                    "control_group": True
                }
            },
            "difference_in_differences": {
                "name": "Difference-in-Differences (DiD)",
                "when_to_use": [
                    "Geographic experiments",
                    "Time-based interventions",
                    "When randomization isn't possible",
                    "Parallel trends assumption holds"
                ],
                "pros": [
                    "No randomization needed",
                    "Controls for time-invariant differences",
                    "Intuitive interpretation"
                ],
                "cons": [
                    "Requires parallel trends assumption",
                    "Sensitive to specification",
                    "May miss dynamic effects"
                ],
                "requirements": {
                    "min_periods": 10,
                    "parallel_trends": True,
                    "control_group": True
                }
            },
            "synthetic_control": {
                "name": "Synthetic Control Method",
                "when_to_use": [
                    "Only one treatment unit",
                    "No natural control group",
                    "Long time series available",
                    "Multiple potential controls"
                ],
                "pros": [
                    "Works with single treatment unit",
                    "Transparent weighting",
                    "Good for policy evaluation"
                ],
                "cons": [
                    "Requires many pre-periods",
                    "Sensitive to donor pool selection",
                    "Complex implementation"
                ],
                "requirements": {
                    "min_periods": 20,
                    "min_donors": 5,
                    "good_pre_fit": True
                }
            },
            "regression_discontinuity": {
                "name": "Regression Discontinuity Design",
                "when_to_use": [
                    "Treatment assigned at threshold",
                    "Clear cutoff point",
                    "Sufficient observations near cutoff"
                ],
                "pros": [
                    "Strong causal identification",
                    "No randomization needed",
                    "Transparent design"
                ],
                "cons": [
                    "Limited to threshold effects",
                    "Requires large samples near cutoff",
                    "External validity concerns"
                ],
                "requirements": {
                    "clear_threshold": True,
                    "observations_near_cutoff": 100,
                    "no_manipulation": True
                }
            },
            "propensity_score_matching": {
                "name": "Propensity Score Matching",
                "when_to_use": [
                    "Observational data",
                    "Many confounding variables",
                    "Need comparable groups",
                    "Selection bias present"
                ],
                "pros": [
                    "Balances covariates",
                    "Intuitive approach",
                    "Reduces bias"
                ],
                "cons": [
                    "Assumes no unobserved confounders",
                    "Matching quality varies",
                    "Reduces sample size"
                ],
                "requirements": {
                    "rich_covariates": True,
                    "common_support": True,
                    "balance_check": True
                }
            }
        }
    
    def _get_metric_guidance(self) -> Dict[str, Dict[str, Any]]:
        """Get guidance for different marketing metrics."""
        return {
            "ROAS": {
                "traditional": "Revenue / Spend",
                "causal": "Incremental Revenue / Spend",
                "components": [
                    "Conversion Volume",
                    "Average Order Value (AOV)",
                    "Spend Efficiency",
                    "Conversion Rate (CVR)",
                    "Click-Through Rate (CTR)",
                    "Cost Per Click (CPC)"
                ],
                "interpretation": "Causal ROAS shows true incremental return, excluding organic conversions",
                "common_pitfall": "Traditional ROAS overestimates by including conversions that would have happened anyway"
            },
            "CPA": {
                "traditional": "Spend / Conversions",
                "causal": "Spend / Incremental Conversions",
                "components": [
                    "Cost Per Click (CPC)",
                    "Conversion Rate (CVR)",
                    "Click-Through Rate (CTR)",
                    "Spend Level"
                ],
                "interpretation": "Causal CPA reflects true cost of acquiring incremental customers",
                "common_pitfall": "Traditional CPA underestimates by counting non-incremental conversions"
            },
            "CTR": {
                "traditional": "Clicks / Impressions",
                "causal": "Incremental Clicks / Impressions",
                "components": [
                    "Click Volume",
                    "Impression Volume"
                ],
                "interpretation": "Causal CTR measures true engagement lift from creative or targeting",
                "common_pitfall": "High CTR doesn't mean causal impact if clicks would have happened on organic results"
            },
            "CVR": {
                "traditional": "Conversions / Clicks",
                "causal": "Incremental Conversions / Clicks",
                "components": [
                    "Conversion Volume",
                    "Click Volume"
                ],
                "interpretation": "Causal CVR shows true conversion lift from landing page or offer",
                "common_pitfall": "Selection bias if high-intent users are more likely to click"
            }
        }
    
    def _get_common_pitfalls(self) -> List[Dict[str, str]]:
        """Get common pitfalls in causal analysis."""
        return [
            {
                "pitfall": "Confounding Variables",
                "description": "Other factors drive the change, not your intervention",
                "example": "Launched campaign in December → Sales increased, but December is holiday season",
                "solution": "Use control group experiencing same seasonality, include time controls"
            },
            {
                "pitfall": "Selection Bias",
                "description": "Treatment group differs systematically from control",
                "example": "Sent email to engaged users → High conversion, but engaged users convert more anyway",
                "solution": "Randomize treatment assignment, use propensity score matching"
            },
            {
                "pitfall": "Reverse Causality",
                "description": "Effect causes the treatment, not vice versa",
                "example": "High-value customers get VIP treatment → High retention, but they were already valuable",
                "solution": "Use instrumental variables, exploit natural experiments"
            },
            {
                "pitfall": "Measurement Error",
                "description": "Inaccurate data leads to wrong conclusions",
                "example": "Tracking pixels fire inconsistently, attribution data incomplete",
                "solution": "Validate data sources, use multiple metrics, check data quality"
            },
            {
                "pitfall": "Insufficient Power",
                "description": "Sample size too small to detect true effect",
                "example": "A/B test with 100 users per group can't detect 5% lift",
                "solution": "Calculate required sample size beforehand, run longer experiments"
            },
            {
                "pitfall": "Spillover Effects",
                "description": "Treatment affects control group",
                "example": "TV ads in treatment markets seen by control market residents",
                "solution": "Use geographically separated markets, account for spillover in model"
            }
        ]
    
    def _get_best_practices(self) -> List[Dict[str, Any]]:
        """Get best practices for causal analysis."""
        return [
            {
                "practice": "Start with Clear Hypotheses",
                "description": "Define what you expect to happen before analysis",
                "steps": [
                    "Specify expected direction of effect",
                    "Estimate magnitude of expected impact",
                    "Document assumptions",
                    "Define success criteria"
                ]
            },
            {
                "practice": "Design for Causality from the Start",
                "description": "Build experiments with causal inference in mind",
                "steps": [
                    "Build in control groups",
                    "Randomize when possible",
                    "Collect pre-intervention data",
                    "Plan for sufficient sample size"
                ]
            },
            {
                "practice": "Use Multiple Methods",
                "description": "Triangulate findings across different approaches",
                "steps": [
                    "Apply 2-3 different methods",
                    "Check robustness of results",
                    "Compare effect sizes",
                    "Build confidence through consistency"
                ]
            },
            {
                "practice": "Communicate Uncertainty",
                "description": "Be transparent about limitations and confidence",
                "steps": [
                    "Report confidence intervals",
                    "Discuss assumptions",
                    "Acknowledge limitations",
                    "Quantify uncertainty"
                ]
            },
            {
                "practice": "Validate Assumptions",
                "description": "Test key assumptions before interpreting results",
                "steps": [
                    "Check parallel trends (DiD)",
                    "Verify common support (PSM)",
                    "Test for spillover effects",
                    "Assess balance (matching)"
                ]
            }
        ]
    
    def _get_interpretation_templates(self) -> Dict[str, str]:
        """Get templates for interpreting results."""
        return {
            "positive_impact": (
                "The {metric} increased by {absolute_change} ({percent_change}% lift) due to {intervention}. "
                "This represents {incremental_value} in incremental value. "
                "The effect is statistically significant (p < {p_value}) with {confidence}% confidence."
            ),
            "negative_impact": (
                "The {metric} decreased by {absolute_change} ({percent_change}% decline) due to {intervention}. "
                "This represents {incremental_value} in lost value. "
                "The effect is statistically significant (p < {p_value}) with {confidence}% confidence."
            ),
            "no_impact": (
                "No statistically significant causal impact detected on {metric} from {intervention}. "
                "The observed change of {absolute_change} ({percent_change}%) is within the margin of error "
                "and cannot be distinguished from random variation (p = {p_value})."
            ),
            "component_breakdown": (
                "{component} contributed {contribution_pct}% of the total change in {metric}, "
                "representing {absolute_contribution} in absolute terms. "
                "This component changed from {before_value} to {after_value} ({delta_pct}% change)."
            )
        }
    
    def _get_recommendation_templates(self) -> Dict[str, List[str]]:
        """Get recommendation templates based on findings."""
        return {
            "high_cpc": [
                "Reduce CPC through bid optimization and improved Quality Score",
                "Review keyword targeting to eliminate low-performing terms",
                "Test ad copy variations to improve relevance and CTR",
                "Consider automated bidding strategies to optimize costs"
            ],
            "low_cvr": [
                "Improve landing page experience and load times",
                "Review offer strength and value proposition",
                "Test different call-to-action messaging",
                "Analyze audience targeting for better fit"
            ],
            "low_ctr": [
                "Test new ad creative and messaging",
                "Improve ad relevance to search intent",
                "Review ad extensions and formatting",
                "Refine audience targeting"
            ],
            "high_spend": [
                "Review budget allocation across campaigns",
                "Identify and pause underperforming campaigns",
                "Reallocate budget to high-ROAS initiatives",
                "Implement spend caps on low-performers"
            ],
            "platform_negative": [
                "Review {platform} campaign performance in detail",
                "Consider pausing or reducing spend on {platform}",
                "Analyze audience overlap with other platforms",
                "Test different creative or targeting on {platform}"
            ],
            "seasonality": [
                "Account for seasonal trends in future analysis",
                "Build seasonal baselines for comparison",
                "Adjust expectations based on time of year",
                "Plan campaigns around seasonal patterns"
            ]
        }
    
    def get_method_recommendation(
        self,
        data_characteristics: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Recommend causal analysis method based on data characteristics.
        
        Args:
            data_characteristics: Dict with keys like:
                - has_randomization: bool
                - has_control_group: bool
                - num_treatment_units: int
                - num_time_periods: int
                - has_threshold: bool
                
        Returns:
            Recommended method with reasoning
        """
        
        methods = self.knowledge["methods"]
        recommendations = []
        
        # Check each method's requirements
        for method_key, method_info in methods.items():
            score = 0
            reasons = []
            
            # A/B Testing
            if method_key == "ab_testing":
                if data_characteristics.get("has_randomization"):
                    score += 10
                    reasons.append("Randomization available (gold standard)")
                if data_characteristics.get("sample_size", 0) >= 1000:
                    score += 5
                    reasons.append("Sufficient sample size")
            
            # Difference-in-Differences
            elif method_key == "difference_in_differences":
                if data_characteristics.get("has_control_group"):
                    score += 8
                    reasons.append("Control group available")
                if data_characteristics.get("num_time_periods", 0) >= 10:
                    score += 5
                    reasons.append("Sufficient time periods")
                if not data_characteristics.get("has_randomization"):
                    score += 3
                    reasons.append("Good alternative when randomization not possible")
            
            # Synthetic Control
            elif method_key == "synthetic_control":
                if data_characteristics.get("num_treatment_units", 0) == 1:
                    score += 10
                    reasons.append("Ideal for single treatment unit")
                if data_characteristics.get("num_time_periods", 0) >= 20:
                    score += 5
                    reasons.append("Long time series available")
                if data_characteristics.get("num_potential_controls", 0) >= 5:
                    score += 3
                    reasons.append("Multiple potential controls")
            
            # Regression Discontinuity
            elif method_key == "regression_discontinuity":
                if data_characteristics.get("has_threshold"):
                    score += 10
                    reasons.append("Clear threshold for treatment assignment")
                if data_characteristics.get("observations_near_cutoff", 0) >= 100:
                    score += 5
                    reasons.append("Sufficient observations near cutoff")
            
            # Propensity Score Matching
            elif method_key == "propensity_score_matching":
                if data_characteristics.get("has_rich_covariates"):
                    score += 8
                    reasons.append("Rich covariate data available")
                if data_characteristics.get("selection_bias_suspected"):
                    score += 5
                    reasons.append("Addresses selection bias")
                if not data_characteristics.get("has_randomization"):
                    score += 3
                    reasons.append("Good for observational data")
            
            if score > 0:
                recommendations.append({
                    "method": method_info["name"],
                    "method_key": method_key,
                    "score": score,
                    "reasons": reasons,
                    "pros": method_info["pros"],
                    "cons": method_info["cons"],
                    "when_to_use": method_info["when_to_use"]
                })
        
        # Sort by score
        recommendations.sort(key=lambda x: x["score"], reverse=True)
        
        return {
            "primary_recommendation": recommendations[0] if recommendations else None,
            "alternative_methods": recommendations[1:3] if len(recommendations) > 1 else [],
            "all_options": recommendations
        }
    
    def get_interpretation_guidance(
        self,
        metric: str,
        change: float,
        change_pct: float,
        primary_driver: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get interpretation guidance for analysis results.
        
        Args:
            metric: Metric name (e.g., "ROAS", "CPA")
            change: Absolute change
            change_pct: Percentage change
            primary_driver: Primary driver component
            
        Returns:
            Interpretation guidance and insights
        """
        
        metric_info = self.knowledge["metrics"].get(metric, {})
        
        # Determine impact direction
        if abs(change_pct) < 5:
            impact_type = "no_impact"
        elif change > 0:
            impact_type = "positive_impact" if metric in ["ROAS", "CTR", "CVR"] else "negative_impact"
        else:
            impact_type = "negative_impact" if metric in ["ROAS", "CTR", "CVR"] else "positive_impact"
        
        # Get interpretation template
        template = self.knowledge["interpretations"].get(impact_type, "")
        
        # Generate insights
        insights = []
        
        if metric_info:
            insights.append(f"**Traditional {metric}:** {metric_info.get('traditional', 'N/A')}")
            insights.append(f"**Causal {metric}:** {metric_info.get('causal', 'N/A')}")
            insights.append(f"**Key Insight:** {metric_info.get('interpretation', '')}")
            
            if metric_info.get("common_pitfall"):
                insights.append(f"⚠️ **Watch out:** {metric_info['common_pitfall']}")
        
        if primary_driver:
            insights.append(f"🎯 **Primary Driver:** {primary_driver} is the main cause of this change")
        
        return {
            "impact_type": impact_type,
            "template": template,
            "insights": insights,
            "metric_info": metric_info,
            "components": metric_info.get("components", [])
        }
    
    def get_recommendations(
        self,
        findings: Dict[str, Any]
    ) -> List[str]:
        """
        Generate actionable recommendations based on findings.
        
        Args:
            findings: Dict with analysis findings including:
                - primary_driver: str
                - metric: str
                - change_direction: str
                - platform_issues: List[str]
                
        Returns:
            List of actionable recommendations
        """
        
        recommendations = []
        rec_templates = self.knowledge["recommendations"]
        
        # Get primary driver
        primary_driver = findings.get("primary_driver", "").lower()
        
        # Match to recommendation templates
        if "cpc" in primary_driver or "cost per click" in primary_driver:
            recommendations.extend(rec_templates["high_cpc"])
        
        if "cvr" in primary_driver or "conversion rate" in primary_driver:
            recommendations.extend(rec_templates["low_cvr"])
        
        if "ctr" in primary_driver or "click-through" in primary_driver:
            recommendations.extend(rec_templates["low_ctr"])
        
        if "spend" in primary_driver:
            recommendations.extend(rec_templates["high_spend"])
        
        # Platform-specific recommendations
        if findings.get("platform_issues"):
            for platform in findings["platform_issues"]:
                platform_recs = [
                    rec.format(platform=platform)
                    for rec in rec_templates["platform_negative"]
                ]
                recommendations.extend(platform_recs)
        
        # Add seasonality note if relevant
        if findings.get("seasonality_detected"):
            recommendations.extend(rec_templates["seasonality"])
        
        # Deduplicate and prioritize
        recommendations = list(dict.fromkeys(recommendations))  # Remove duplicates
        
        return recommendations[:5]  # Top 5 recommendations
    
    def get_pitfall_warnings(
        self,
        analysis_context: Dict[str, Any]
    ) -> List[Dict[str, str]]:
        """
        Get relevant pitfall warnings based on analysis context.
        
        Args:
            analysis_context: Context about the analysis setup
            
        Returns:
            List of relevant pitfalls to watch for
        """
        
        relevant_pitfalls = []
        all_pitfalls = self.knowledge["pitfalls"]
        
        # Check for confounding variables
        if analysis_context.get("seasonal_period"):
            relevant_pitfalls.append(all_pitfalls[0])  # Confounding
        
        # Check for selection bias
        if not analysis_context.get("randomized"):
            relevant_pitfalls.append(all_pitfalls[1])  # Selection bias
        
        # Check for measurement issues
        if analysis_context.get("data_quality_concerns"):
            relevant_pitfalls.append(all_pitfalls[3])  # Measurement error
        
        # Check for power issues
        if analysis_context.get("sample_size", 0) < 1000:
            relevant_pitfalls.append(all_pitfalls[4])  # Insufficient power
        
        # Check for spillover
        if analysis_context.get("geographic_proximity"):
            relevant_pitfalls.append(all_pitfalls[5])  # Spillover
        
        return relevant_pitfalls
    
    def enhance_causal_result(
        self,
        causal_result: Any,
        analysis_context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Enhance causal analysis result with knowledge base insights.
        
        Args:
            causal_result: CausalAnalysisResult object
            analysis_context: Optional context about the analysis
            
        Returns:
            Enhanced result with additional insights and recommendations
        """
        
        # Get interpretation guidance
        interpretation = self.get_interpretation_guidance(
            metric=causal_result.metric,
            change=causal_result.total_change,
            change_pct=causal_result.total_change_pct,
            primary_driver=causal_result.primary_driver.component if causal_result.primary_driver else None
        )
        
        # Get recommendations
        findings = {
            "primary_driver": causal_result.primary_driver.component if causal_result.primary_driver else "",
            "metric": causal_result.metric,
            "change_direction": "increase" if causal_result.total_change > 0 else "decrease",
            "platform_issues": [
                platform for platform, contrib in (causal_result.platform_attribution or {}).items()
                if contrib < 0
            ]
        }
        recommendations = self.get_recommendations(findings)
        
        # Get pitfall warnings
        pitfall_warnings = []
        if analysis_context:
            pitfall_warnings = self.get_pitfall_warnings(analysis_context)
        
        # Get method recommendation for future analysis
        data_chars = {
            "has_control_group": True,
            "num_time_periods": analysis_context.get("num_periods", 30) if analysis_context else 30,
            "sample_size": analysis_context.get("sample_size", 1000) if analysis_context else 1000
        }
        method_rec = self.get_method_recommendation(data_chars)
        
        return {
            "original_result": causal_result,
            "interpretation": interpretation,
            "enhanced_recommendations": recommendations,
            "pitfall_warnings": pitfall_warnings,
            "method_recommendations": method_rec,
            "best_practices": self.knowledge["best_practices"][:3]  # Top 3
        }


# Singleton instance
_kb_instance = None

def get_knowledge_base() -> CausalKnowledgeBase:
    """Get singleton instance of knowledge base."""
    global _kb_instance
    if _kb_instance is None:
        _kb_instance = CausalKnowledgeBase()
    return _kb_instance

"""
Advanced Causal Analysis Module for Marketing Performance

Implements comprehensive causal analysis with:
1. Formula-based KPI decomposition (mathematical)
2. Attribution-based causal contribution (by channel/platform)
3. ML-based causal impact (XGBoost + SHAP)
4. Knowledge base integration (RAG-enhanced insights)

Provides quantified cause-and-effect analysis with actionable insights.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import logging
from scipy import stats

# Import knowledge base for RAG integration
try:
    from src.platform.knowledge.causal_kb_rag import get_knowledge_base
    KB_AVAILABLE = True
except ImportError:
    KB_AVAILABLE = False
    logging.warning("Knowledge base not available. Install dependencies or check path.")

try:
    import xgboost as xgb
    import shap
    SHAP_AVAILABLE = True
except ImportError:
    SHAP_AVAILABLE = False

from scipy import stats
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LinearRegression

logger = logging.getLogger(__name__)


class DecompositionMethod(Enum):
    """Methods for KPI decomposition."""
    ADDITIVE = "additive"
    MULTIPLICATIVE = "multiplicative"
    SHAPLEY = "shapley"
    HYBRID = "hybrid"


@dataclass
class ComponentContribution:
    """Contribution of a single component to metric change."""
    component: str
    absolute_change: float
    percentage_contribution: float
    before_value: float
    after_value: float
    delta: float
    delta_pct: float
    impact_direction: str  # "positive", "negative", "neutral"
    actionability: str  # "high", "medium", "low"
    
    def __str__(self):
        sign = "+" if self.absolute_change >= 0 else ""
        return f"{self.component}: {sign}${self.absolute_change:.2f} ({self.percentage_contribution:.1f}%)"


@dataclass
class CausalAnalysisResult:
    """Complete causal analysis result."""
    metric: str
    before_value: float
    after_value: float
    total_change: float
    total_change_pct: float
    
    # Component contributions
    contributions: List[ComponentContribution]
    primary_driver: ComponentContribution
    secondary_drivers: List[ComponentContribution]
    
    # Attribution
    channel_attribution: Dict[str, float] = field(default_factory=dict)
    platform_attribution: Dict[str, float] = field(default_factory=dict)
    
    # ML insights
    ml_drivers: Optional[Dict[str, float]] = None
    shap_values: Optional[Dict[str, float]] = None
    
    # Metadata
    confidence: float = 0.0
    method: str = "formula_based"
    period_before: str = ""
    period_after: str = ""
    
    # Insights
    insights: List[str] = field(default_factory=list)
    recommendations: List[str] = field(default_factory=list)


class CausalAnalysisEngine:
    """
    Advanced Causal Analysis Engine
    
    Provides three types of causal analysis:
    1. Formula-based decomposition
    2. Attribution-based contribution
    3. ML-based causal impact
    """
    
    def __init__(self):
        self.scaler = StandardScaler()
        self.decomposition_cache = {}
        
    def analyze(
        self,
        df: pd.DataFrame,
        metric: str,
        date_col: str = 'Date',
        split_date: Optional[str] = None,
        lookback_days: int = 30,
        method: DecompositionMethod = DecompositionMethod.HYBRID,
        include_ml: bool = True,
        include_attribution: bool = True
    ) -> CausalAnalysisResult:
        """
        Comprehensive causal analysis of metric change.
        
        Args:
            df: Campaign data
            metric: Target metric (ROAS, CPA, CTR, etc.)
            date_col: Date column name
            split_date: Date to split before/after
            lookback_days: Days to analyze in each period
            method: Decomposition method
            include_ml: Include ML-based analysis
            include_attribution: Include channel/platform attribution
            
        Returns:
            CausalAnalysisResult with complete breakdown
        """
        
        try:
            # Prepare data
            df = df.copy()
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
            df = df.dropna(subset=[date_col])
            
            # Determine split date
            if split_date is None:
                min_date = df[date_col].min()
                max_date = df[date_col].max()
                split_date = min_date + (max_date - min_date) / 2
            else:
                split_date = pd.to_datetime(split_date)
            
            # Split periods
            before_df = df[df[date_col] < split_date].tail(lookback_days)
            after_df = df[df[date_col] >= split_date].head(lookback_days)
            
            if len(before_df) == 0 or len(after_df) == 0:
                logger.warning("Insufficient data for causal analysis")
                return self._create_empty_result(metric)
            
            # Calculate metric values
            before_value = self._calculate_metric(before_df, metric)
            after_value = self._calculate_metric(after_df, metric)
            total_change = after_value - before_value
            total_change_pct = (total_change / before_value * 100) if before_value != 0 else 0
            
            # 1. Formula-based decomposition
            contributions = self._decompose_metric(
                before_df, after_df, metric, method
            )
            
            # 2. Attribution analysis
            channel_attr = {}
            platform_attr = {}
            if include_attribution:
                channel_attr = self._calculate_channel_attribution(
                    before_df, after_df, metric
                )
                platform_attr = self._calculate_platform_attribution(
                    before_df, after_df, metric
                )
            
            # 3. ML-based analysis
            ml_drivers = None
            shap_vals = None
            if include_ml and SHAP_AVAILABLE:
                ml_drivers, shap_vals = self._ml_causal_impact(
                    df, metric, split_date
                )
            
            # Identify primary and secondary drivers
            sorted_contributions = sorted(
                contributions,
                key=lambda x: abs(x.absolute_change),
                reverse=True
            )
            
            primary_driver = sorted_contributions[0] if sorted_contributions else None
            secondary_drivers = sorted_contributions[1:4] if len(sorted_contributions) > 1 else []
            
            # Generate insights and recommendations
            insights = self._generate_insights(
                metric, total_change, total_change_pct,
                contributions, channel_attr, platform_attr
            )
            
            recommendations = self._generate_recommendations(
                metric, contributions, channel_attr, platform_attr
            )
            
            # Calculate confidence
            confidence = self._calculate_confidence(before_df, after_df, metric)
            
            result = CausalAnalysisResult(
                metric=metric,
                before_value=before_value,
                after_value=after_value,
                total_change=total_change,
                total_change_pct=total_change_pct,
                contributions=contributions,
                primary_driver=primary_driver,
                secondary_drivers=secondary_drivers,
                channel_attribution=channel_attr,
                platform_attribution=platform_attr,
                ml_drivers=ml_drivers,
                shap_values=shap_vals,
                confidence=confidence,
                method=method.value,
                period_before=f"{before_df[date_col].min().date()} to {before_df[date_col].max().date()}",
                period_after=f"{after_df[date_col].min().date()} to {after_df[date_col].max().date()}",
                insights=insights,
                recommendations=recommendations
            )
            
            # Enhance with knowledge base if available
            if KB_AVAILABLE:
                result = self._enhance_with_knowledge_base(result, {
                    "num_periods": lookback_days,
                    "sample_size": len(before_df) + len(after_df),
                    "has_control_group": False,  # Single group analysis
                    "method": method.value
                })
            
            return result
            
        except Exception as e:
            logger.error(f"Error in causal analysis: {e}", exc_info=True)
            return self._create_empty_result(metric)
    
    def _enhance_with_knowledge_base(
        self,
        result: CausalAnalysisResult,
        analysis_context: Dict[str, Any]
    ) -> CausalAnalysisResult:
        """Enhance result with knowledge base insights."""
        
        try:
            kb = get_knowledge_base()
            
            # Get enhanced insights and recommendations
            enhanced = kb.enhance_causal_result(result, analysis_context)
            
            # Add KB-enhanced recommendations (prepend to existing)
            kb_recs = enhanced.get("enhanced_recommendations", [])
            result.recommendations = kb_recs[:3] + result.recommendations
            
            # Add interpretation insights
            interpretation = enhanced.get("interpretation", {})
            kb_insights = interpretation.get("insights", [])
            result.insights = kb_insights[:2] + result.insights
            
            # Add pitfall warnings as insights
            pitfalls = enhanced.get("pitfall_warnings", [])
            if pitfalls:
                result.insights.append(
                    f"⚠️ **Watch out for:** {pitfalls[0]['pitfall']} - {pitfalls[0]['solution']}"
                )
            
            logger.info("Enhanced causal result with knowledge base")
            
        except Exception as e:
            logger.warning(f"Could not enhance with knowledge base: {e}")
        
        return result
    
    def _decompose_metric(
        self,
        before_df: pd.DataFrame,
        after_df: pd.DataFrame,
        metric: str,
        method: DecompositionMethod
    ) -> List[ComponentContribution]:
        """Decompose metric into component contributions."""
        
        # Metric-specific decomposition
        decomposers = {
            'ROAS': self._decompose_roas,
            'CPA': self._decompose_cpa,
            'CTR': self._decompose_ctr,
            'CVR': self._decompose_cvr,
            'CPC': self._decompose_cpc,
            'CPM': self._decompose_cpm,
            'Revenue': self._decompose_revenue,
            'Spend': self._decompose_spend,
        }
        
        decomposer = decomposers.get(metric, self._decompose_generic)
        
        if method == DecompositionMethod.SHAPLEY:
            return self._shapley_decomposition(before_df, after_df, metric)
        else:
            return decomposer(before_df, after_df)
    
    def _decompose_roas(
        self,
        before_df: pd.DataFrame,
        after_df: pd.DataFrame
    ) -> List[ComponentContribution]:
        """
        Decompose ROAS change.
        
        ROAS = Revenue / Spend
             = (Conversions × AOV) / Spend
             = (Clicks × CVR × AOV) / Spend
             = (Impressions × CTR × CVR × AOV) / Spend
        """
        
        contributions = []
        
        # Calculate components - BEFORE
        before_spend = before_df['Spend'].sum() if 'Spend' in before_df.columns else 1
        before_conv = before_df['Conversions'].sum() if 'Conversions' in before_df.columns else 0
        before_clicks = before_df['Clicks'].sum() if 'Clicks' in before_df.columns else 0
        before_impr = before_df['Impressions'].sum() if 'Impressions' in before_df.columns else 0
        before_revenue = before_df['Revenue'].sum() if 'Revenue' in before_df.columns else before_conv * 50
        
        before_aov = before_revenue / before_conv if before_conv > 0 else 0
        before_cvr = before_conv / before_clicks if before_clicks > 0 else 0
        before_ctr = before_clicks / before_impr if before_impr > 0 else 0
        before_cpc = before_spend / before_clicks if before_clicks > 0 else 0
        
        # Calculate components - AFTER
        after_spend = after_df['Spend'].sum() if 'Spend' in after_df.columns else 1
        after_conv = after_df['Conversions'].sum() if 'Conversions' in after_df.columns else 0
        after_clicks = after_df['Clicks'].sum() if 'Clicks' in after_df.columns else 0
        after_impr = after_df['Impressions'].sum() if 'Impressions' in after_df.columns else 0
        after_revenue = after_df['Revenue'].sum() if 'Revenue' in after_df.columns else after_conv * 50
        
        after_aov = after_revenue / after_conv if after_conv > 0 else 0
        after_cvr = after_conv / after_clicks if after_clicks > 0 else 0
        after_ctr = after_clicks / after_impr if after_impr > 0 else 0
        after_cpc = after_spend / after_clicks if after_clicks > 0 else 0
        
        # Component 1: Conversion Volume Impact
        conv_delta = after_conv - before_conv
        conv_contribution = (conv_delta * before_aov) / before_spend if before_spend > 0 else 0
        contributions.append(ComponentContribution(
            component="Conversion Volume",
            absolute_change=conv_contribution,
            percentage_contribution=0,  # Will calculate later
            before_value=before_conv,
            after_value=after_conv,
            delta=conv_delta,
            delta_pct=(conv_delta / before_conv * 100) if before_conv > 0 else 0,
            impact_direction="positive" if conv_contribution > 0 else "negative",
            actionability="high"
        ))
        
        # Component 2: AOV Impact
        aov_delta = after_aov - before_aov
        aov_contribution = (after_conv * aov_delta) / before_spend if before_spend > 0 else 0
        contributions.append(ComponentContribution(
            component="Average Order Value (AOV)",
            absolute_change=aov_contribution,
            percentage_contribution=0,
            before_value=before_aov,
            after_value=after_aov,
            delta=aov_delta,
            delta_pct=(aov_delta / before_aov * 100) if before_aov > 0 else 0,
            impact_direction="positive" if aov_contribution > 0 else "negative",
            actionability="medium"
        ))
        
        # Component 3: Spend Efficiency
        spend_delta = after_spend - before_spend
        spend_contribution = -(after_revenue * spend_delta) / (before_spend * after_spend) if before_spend * after_spend > 0 else 0
        contributions.append(ComponentContribution(
            component="Spend Level",
            absolute_change=spend_contribution,
            percentage_contribution=0,
            before_value=before_spend,
            after_value=after_spend,
            delta=spend_delta,
            delta_pct=(spend_delta / before_spend * 100) if before_spend > 0 else 0,
            impact_direction="positive" if spend_contribution > 0 else "negative",
            actionability="high"
        ))
        
        # Component 4: CVR Impact (conversion rate)
        cvr_delta = after_cvr - before_cvr
        cvr_contribution = (before_clicks * cvr_delta * before_aov) / before_spend if before_spend > 0 else 0
        contributions.append(ComponentContribution(
            component="Conversion Rate (CVR)",
            absolute_change=cvr_contribution,
            percentage_contribution=0,
            before_value=before_cvr * 100,
            after_value=after_cvr * 100,
            delta=cvr_delta * 100,
            delta_pct=(cvr_delta / before_cvr * 100) if before_cvr > 0 else 0,
            impact_direction="positive" if cvr_contribution > 0 else "negative",
            actionability="high"
        ))
        
        # Component 5: CTR Impact (click-through rate)
        ctr_delta = after_ctr - before_ctr
        ctr_contribution = (before_impr * ctr_delta * before_cvr * before_aov) / before_spend if before_spend > 0 else 0
        contributions.append(ComponentContribution(
            component="Click-Through Rate (CTR)",
            absolute_change=ctr_contribution,
            percentage_contribution=0,
            before_value=before_ctr * 100,
            after_value=after_ctr * 100,
            delta=ctr_delta * 100,
            delta_pct=(ctr_delta / before_ctr * 100) if before_ctr > 0 else 0,
            impact_direction="positive" if ctr_contribution > 0 else "negative",
            actionability="medium"
        ))
        
        # Component 6: CPC Impact
        cpc_delta = after_cpc - before_cpc
        cpc_contribution = -(before_clicks * cpc_delta * before_cvr * before_aov) / (before_spend * before_cpc) if before_spend * before_cpc > 0 else 0
        contributions.append(ComponentContribution(
            component="Cost Per Click (CPC)",
            absolute_change=cpc_contribution,
            percentage_contribution=0,
            before_value=before_cpc,
            after_value=after_cpc,
            delta=cpc_delta,
            delta_pct=(cpc_delta / before_cpc * 100) if before_cpc > 0 else 0,
            impact_direction="positive" if cpc_contribution > 0 else "negative",
            actionability="high"
        ))
        
        # Calculate percentage contributions
        total_abs_contribution = sum(abs(c.absolute_change) for c in contributions)
        if total_abs_contribution > 0:
            for contrib in contributions:
                contrib.percentage_contribution = (abs(contrib.absolute_change) / total_abs_contribution) * 100
        
        return contributions
    
    def _decompose_cpa(
        self,
        before_df: pd.DataFrame,
        after_df: pd.DataFrame
    ) -> List[ComponentContribution]:
        """
        Decompose CPA change.
        
        CPA = Spend / Conversions
            = (Spend / Clicks) × (Clicks / Conversions)
            = CPC / CVR
        """
        
        contributions = []
        
        # BEFORE
        before_spend = before_df['Spend'].sum() if 'Spend' in before_df.columns else 0
        before_conv = before_df['Conversions'].sum() if 'Conversions' in before_df.columns else 1
        before_clicks = before_df['Clicks'].sum() if 'Clicks' in before_df.columns else 0
        before_impr = before_df['Impressions'].sum() if 'Impressions' in before_df.columns else 0
        
        before_cpc = before_spend / before_clicks if before_clicks > 0 else 0
        before_cvr = before_conv / before_clicks if before_clicks > 0 else 0
        before_ctr = before_clicks / before_impr if before_impr > 0 else 0
        
        # AFTER
        after_spend = after_df['Spend'].sum() if 'Spend' in after_df.columns else 0
        after_conv = after_df['Conversions'].sum() if 'Conversions' in after_df.columns else 1
        after_clicks = after_df['Clicks'].sum() if 'Clicks' in after_df.columns else 0
        after_impr = after_df['Impressions'].sum() if 'Impressions' in after_df.columns else 0
        
        after_cpc = after_spend / after_clicks if after_clicks > 0 else 0
        after_cvr = after_conv / after_clicks if after_clicks > 0 else 0
        after_ctr = after_clicks / after_impr if after_impr > 0 else 0
        
        # Component 1: CPC Impact
        cpc_delta = after_cpc - before_cpc
        cpc_contribution = cpc_delta / before_cvr if before_cvr > 0 else 0
        contributions.append(ComponentContribution(
            component="Cost Per Click (CPC)",
            absolute_change=cpc_contribution,
            percentage_contribution=0,
            before_value=before_cpc,
            after_value=after_cpc,
            delta=cpc_delta,
            delta_pct=(cpc_delta / before_cpc * 100) if before_cpc > 0 else 0,
            impact_direction="positive" if cpc_contribution < 0 else "negative",  # Lower CPC is better for CPA
            actionability="high"
        ))
        
        # Component 2: CVR Impact
        cvr_delta = after_cvr - before_cvr
        cvr_contribution = -before_cpc * cvr_delta / (before_cvr * after_cvr) if before_cvr * after_cvr > 0 else 0
        contributions.append(ComponentContribution(
            component="Conversion Rate (CVR)",
            absolute_change=cvr_contribution,
            percentage_contribution=0,
            before_value=before_cvr * 100,
            after_value=after_cvr * 100,
            delta=cvr_delta * 100,
            delta_pct=(cvr_delta / before_cvr * 100) if before_cvr > 0 else 0,
            impact_direction="positive" if cvr_contribution < 0 else "negative",  # Higher CVR is better for CPA
            actionability="high"
        ))
        
        # Component 3: CTR Impact (indirect)
        ctr_delta = after_ctr - before_ctr
        ctr_contribution = 0  # CTR affects CPA indirectly through quality score
        contributions.append(ComponentContribution(
            component="Click-Through Rate (CTR)",
            absolute_change=ctr_contribution,
            percentage_contribution=0,
            before_value=before_ctr * 100,
            after_value=after_ctr * 100,
            delta=ctr_delta * 100,
            delta_pct=(ctr_delta / before_ctr * 100) if before_ctr > 0 else 0,
            impact_direction="neutral",
            actionability="medium"
        ))
        
        # Component 4: Spend Level
        spend_delta = after_spend - before_spend
        spend_contribution = spend_delta / after_conv if after_conv > 0 else 0
        contributions.append(ComponentContribution(
            component="Spend Level",
            absolute_change=spend_contribution,
            percentage_contribution=0,
            before_value=before_spend,
            after_value=after_spend,
            delta=spend_delta,
            delta_pct=(spend_delta / before_spend * 100) if before_spend > 0 else 0,
            impact_direction="positive" if spend_contribution < 0 else "negative",
            actionability="high"
        ))
        
        # Calculate percentage contributions
        total_abs_contribution = sum(abs(c.absolute_change) for c in contributions)
        if total_abs_contribution > 0:
            for contrib in contributions:
                contrib.percentage_contribution = (abs(contrib.absolute_change) / total_abs_contribution) * 100
        
        return contributions
    
    def _decompose_ctr(self, before_df: pd.DataFrame, after_df: pd.DataFrame) -> List[ComponentContribution]:
        """Decompose CTR: CTR = Clicks / Impressions"""
        contributions = []
        
        before_clicks = before_df['Clicks'].sum() if 'Clicks' in before_df.columns else 0
        after_clicks = after_df['Clicks'].sum() if 'Clicks' in after_df.columns else 0
        before_impr = before_df['Impressions'].sum() if 'Impressions' in before_df.columns else 1
        after_impr = after_df['Impressions'].sum() if 'Impressions' in after_df.columns else 1
        
        click_delta = after_clicks - before_clicks
        impr_delta = after_impr - before_impr
        
        click_contribution = (click_delta / before_impr) * 100 if before_impr > 0 else 0
        impr_contribution = -(before_clicks * impr_delta / (before_impr * after_impr)) * 100 if before_impr * after_impr > 0 else 0
        
        contributions.append(ComponentContribution(
            component="Click Volume",
            absolute_change=click_contribution,
            percentage_contribution=0,
            before_value=before_clicks,
            after_value=after_clicks,
            delta=click_delta,
            delta_pct=(click_delta / before_clicks * 100) if before_clicks > 0 else 0,
            impact_direction="positive" if click_contribution > 0 else "negative",
            actionability="medium"
        ))
        
        contributions.append(ComponentContribution(
            component="Impression Volume",
            absolute_change=impr_contribution,
            percentage_contribution=0,
            before_value=before_impr,
            after_value=after_impr,
            delta=impr_delta,
            delta_pct=(impr_delta / before_impr * 100) if before_impr > 0 else 0,
            impact_direction="positive" if impr_contribution > 0 else "negative",
            actionability="low"
        ))
        
        total_abs = sum(abs(c.absolute_change) for c in contributions)
        if total_abs > 0:
            for c in contributions:
                c.percentage_contribution = (abs(c.absolute_change) / total_abs) * 100
        
        return contributions
    
    def _decompose_cvr(self, before_df: pd.DataFrame, after_df: pd.DataFrame) -> List[ComponentContribution]:
        """Decompose CVR: CVR = Conversions / Clicks"""
        contributions = []
        
        before_conv = before_df['Conversions'].sum() if 'Conversions' in before_df.columns else 0
        after_conv = after_df['Conversions'].sum() if 'Conversions' in after_df.columns else 0
        before_clicks = before_df['Clicks'].sum() if 'Clicks' in before_df.columns else 1
        after_clicks = after_df['Clicks'].sum() if 'Clicks' in after_df.columns else 1
        
        conv_delta = after_conv - before_conv
        click_delta = after_clicks - before_clicks
        
        conv_contribution = (conv_delta / before_clicks) * 100 if before_clicks > 0 else 0
        click_contribution = -(before_conv * click_delta / (before_clicks * after_clicks)) * 100 if before_clicks * after_clicks > 0 else 0
        
        contributions.append(ComponentContribution(
            component="Conversion Volume",
            absolute_change=conv_contribution,
            percentage_contribution=0,
            before_value=before_conv,
            after_value=after_conv,
            delta=conv_delta,
            delta_pct=(conv_delta / before_conv * 100) if before_conv > 0 else 0,
            impact_direction="positive" if conv_contribution > 0 else "negative",
            actionability="high"
        ))
        
        contributions.append(ComponentContribution(
            component="Click Volume",
            absolute_change=click_contribution,
            percentage_contribution=0,
            before_value=before_clicks,
            after_value=after_clicks,
            delta=click_delta,
            delta_pct=(click_delta / before_clicks * 100) if before_clicks > 0 else 0,
            impact_direction="positive" if click_contribution > 0 else "negative",
            actionability="medium"
        ))
        
        total_abs = sum(abs(c.absolute_change) for c in contributions)
        if total_abs > 0:
            for c in contributions:
                c.percentage_contribution = (abs(c.absolute_change) / total_abs) * 100
        
        return contributions
    
    def _decompose_cpc(self, before_df: pd.DataFrame, after_df: pd.DataFrame) -> List[ComponentContribution]:
        """Decompose CPC: CPC = Spend / Clicks"""
        contributions = []
        
        before_spend = before_df['Spend'].sum() if 'Spend' in before_df.columns else 0
        after_spend = after_df['Spend'].sum() if 'Spend' in after_df.columns else 0
        before_clicks = before_df['Clicks'].sum() if 'Clicks' in before_df.columns else 1
        after_clicks = after_df['Clicks'].sum() if 'Clicks' in after_df.columns else 1
        
        spend_delta = after_spend - before_spend
        click_delta = after_clicks - before_clicks
        
        spend_contribution = spend_delta / before_clicks if before_clicks > 0 else 0
        click_contribution = -(before_spend * click_delta / (before_clicks * after_clicks)) if before_clicks * after_clicks > 0 else 0
        
        contributions.append(ComponentContribution(
            component="Spend Change",
            absolute_change=spend_contribution,
            percentage_contribution=0,
            before_value=before_spend,
            after_value=after_spend,
            delta=spend_delta,
            delta_pct=(spend_delta / before_spend * 100) if before_spend > 0 else 0,
            impact_direction="positive" if spend_contribution < 0 else "negative",
            actionability="high"
        ))
        
        contributions.append(ComponentContribution(
            component="Click Volume",
            absolute_change=click_contribution,
            percentage_contribution=0,
            before_value=before_clicks,
            after_value=after_clicks,
            delta=click_delta,
            delta_pct=(click_delta / before_clicks * 100) if before_clicks > 0 else 0,
            impact_direction="positive" if click_contribution < 0 else "negative",
            actionability="medium"
        ))
        
        total_abs = sum(abs(c.absolute_change) for c in contributions)
        if total_abs > 0:
            for c in contributions:
                c.percentage_contribution = (abs(c.absolute_change) / total_abs) * 100
        
        return contributions
    
    def _decompose_cpm(self, before_df: pd.DataFrame, after_df: pd.DataFrame) -> List[ComponentContribution]:
        """Decompose CPM: CPM = (Spend / Impressions) × 1000"""
        contributions = []
        
        before_spend = before_df['Spend'].sum() if 'Spend' in before_df.columns else 0
        after_spend = after_df['Spend'].sum() if 'Spend' in after_df.columns else 0
        before_impr = before_df['Impressions'].sum() if 'Impressions' in before_df.columns else 1
        after_impr = after_df['Impressions'].sum() if 'Impressions' in after_df.columns else 1
        
        spend_delta = after_spend - before_spend
        impr_delta = after_impr - before_impr
        
        spend_contribution = (spend_delta / before_impr) * 1000 if before_impr > 0 else 0
        impr_contribution = -(before_spend * impr_delta * 1000 / (before_impr * after_impr)) if before_impr * after_impr > 0 else 0
        
        contributions.append(ComponentContribution(
            component="Spend Change",
            absolute_change=spend_contribution,
            percentage_contribution=0,
            before_value=before_spend,
            after_value=after_spend,
            delta=spend_delta,
            delta_pct=(spend_delta / before_spend * 100) if before_spend > 0 else 0,
            impact_direction="positive" if spend_contribution < 0 else "negative",
            actionability="high"
        ))
        
        contributions.append(ComponentContribution(
            component="Impression Volume",
            absolute_change=impr_contribution,
            percentage_contribution=0,
            before_value=before_impr,
            after_value=after_impr,
            delta=impr_delta,
            delta_pct=(impr_delta / before_impr * 100) if before_impr > 0 else 0,
            impact_direction="positive" if impr_contribution < 0 else "negative",
            actionability="low"
        ))
        
        total_abs = sum(abs(c.absolute_change) for c in contributions)
        if total_abs > 0:
            for c in contributions:
                c.percentage_contribution = (abs(c.absolute_change) / total_abs) * 100
        
        return contributions
    
    def _decompose_revenue(self, before_df: pd.DataFrame, after_df: pd.DataFrame) -> List[ComponentContribution]:
        """Decompose Revenue: Revenue = Conversions × AOV"""
        contributions = []
        
        before_conv = before_df['Conversions'].sum() if 'Conversions' in before_df.columns else 0
        after_conv = after_df['Conversions'].sum() if 'Conversions' in after_df.columns else 0
        before_revenue = before_df['Revenue'].sum() if 'Revenue' in before_df.columns else 0
        after_revenue = after_df['Revenue'].sum() if 'Revenue' in after_df.columns else 0
        
        before_aov = before_revenue / before_conv if before_conv > 0 else 0
        after_aov = after_revenue / after_conv if after_conv > 0 else 0
        
        conv_delta = after_conv - before_conv
        aov_delta = after_aov - before_aov
        
        conv_contribution = conv_delta * before_aov
        aov_contribution = after_conv * aov_delta
        
        contributions.append(ComponentContribution(
            component="Conversion Volume",
            absolute_change=conv_contribution,
            percentage_contribution=0,
            before_value=before_conv,
            after_value=after_conv,
            delta=conv_delta,
            delta_pct=(conv_delta / before_conv * 100) if before_conv > 0 else 0,
            impact_direction="positive" if conv_contribution > 0 else "negative",
            actionability="high"
        ))
        
        contributions.append(ComponentContribution(
            component="Average Order Value",
            absolute_change=aov_contribution,
            percentage_contribution=0,
            before_value=before_aov,
            after_value=after_aov,
            delta=aov_delta,
            delta_pct=(aov_delta / before_aov * 100) if before_aov > 0 else 0,
            impact_direction="positive" if aov_contribution > 0 else "negative",
            actionability="medium"
        ))
        
        total_abs = sum(abs(c.absolute_change) for c in contributions)
        if total_abs > 0:
            for c in contributions:
                c.percentage_contribution = (abs(c.absolute_change) / total_abs) * 100
        
        return contributions
    
    def _decompose_spend(self, before_df: pd.DataFrame, after_df: pd.DataFrame) -> List[ComponentContribution]:
        """Decompose Spend by channel/platform"""
        contributions = []
        
        # Aggregate by platform if available
        if 'Platform' in before_df.columns and 'Platform' in after_df.columns:
            platforms = set(before_df['Platform'].unique()) | set(after_df['Platform'].unique())
            
            for platform in platforms:
                before_spend = before_df[before_df['Platform'] == platform]['Spend'].sum() if 'Spend' in before_df.columns else 0
                after_spend = after_df[after_df['Platform'] == platform]['Spend'].sum() if 'Spend' in after_df.columns else 0
                delta = after_spend - before_spend
                
                contributions.append(ComponentContribution(
                    component=f"{platform} Spend",
                    absolute_change=delta,
                    percentage_contribution=0,
                    before_value=before_spend,
                    after_value=after_spend,
                    delta=delta,
                    delta_pct=(delta / before_spend * 100) if before_spend > 0 else 0,
                    impact_direction="positive" if delta > 0 else "negative",
                    actionability="high"
                ))
        
        total_abs = sum(abs(c.absolute_change) for c in contributions)
        if total_abs > 0:
            for c in contributions:
                c.percentage_contribution = (abs(c.absolute_change) / total_abs) * 100
        
        return contributions
    
    def _decompose_generic(self, before_df: pd.DataFrame, after_df: pd.DataFrame) -> List[ComponentContribution]:
        """Generic decomposition for unknown metrics"""
        return [ComponentContribution(
            component="Unknown Factor",
            absolute_change=0.0,
            percentage_contribution=100.0,
            before_value=0.0,
            after_value=0.0,
            delta=0.0,
            delta_pct=0.0,
            impact_direction="neutral",
            actionability="low"
        )]
    
    def _shapley_decomposition(
        self,
        before_df: pd.DataFrame,
        after_df: pd.DataFrame,
        metric: str
    ) -> List[ComponentContribution]:
        """
        Shapley value-based decomposition (cooperative game theory).
        Fairly distributes the total change among components.
        """
        # This is a simplified version - full Shapley requires all subset combinations
        # For now, fall back to standard decomposition
        return self._decompose_metric(before_df, after_df, metric, DecompositionMethod.HYBRID)
    
    def _calculate_channel_attribution(
        self,
        before_df: pd.DataFrame,
        after_df: pd.DataFrame,
        metric: str
    ) -> Dict[str, float]:
        """Calculate channel-level attribution to metric change."""
        
        attribution = {}
        
        if 'Channel' not in before_df.columns or 'Channel' not in after_df.columns:
            return attribution
        
        channels = set(before_df['Channel'].unique()) | set(after_df['Channel'].unique())
        
        for channel in channels:
            before_val = self._calculate_metric(
                before_df[before_df['Channel'] == channel], metric
            )
            after_val = self._calculate_metric(
                after_df[after_df['Channel'] == channel], metric
            )
            
            contribution = after_val - before_val
            attribution[channel] = contribution
        
        return attribution
    
    def _calculate_platform_attribution(
        self,
        before_df: pd.DataFrame,
        after_df: pd.DataFrame,
        metric: str
    ) -> Dict[str, float]:
        """Calculate platform-level attribution to metric change."""
        
        attribution = {}
        
        if 'Platform' not in before_df.columns or 'Platform' not in after_df.columns:
            return attribution
        
        platforms = set(before_df['Platform'].unique()) | set(after_df['Platform'].unique())
        
        for platform in platforms:
            before_val = self._calculate_metric(
                before_df[before_df['Platform'] == platform], metric
            )
            after_val = self._calculate_metric(
                after_df[after_df['Platform'] == platform], metric
            )
            
            contribution = after_val - before_val
            attribution[platform] = contribution
        
        return attribution
    
    def _ml_causal_impact(
        self,
        df: pd.DataFrame,
        metric: str,
        split_date: pd.Timestamp
    ) -> Tuple[Optional[Dict[str, float]], Optional[Dict[str, float]]]:
        """ML-based causal impact analysis using XGBoost + SHAP."""
        
        if not SHAP_AVAILABLE:
            return None, None
        
        try:
            # Prepare features
            numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
            feature_cols = [col for col in numeric_cols if col != metric and col in df.columns]
            
            if len(feature_cols) < 2:
                return None, None
            
            # Calculate target metric if needed
            df_copy = df.copy()
            if metric not in df_copy.columns:
                df_copy[metric] = df_copy.apply(
                    lambda row: self._calculate_metric(pd.DataFrame([row]), metric),
                    axis=1
                )
            
            # Remove NaN
            df_clean = df_copy[feature_cols + [metric]].dropna()
            
            if len(df_clean) < 20:
                return None, None
            
            X = df_clean[feature_cols].values
            y = df_clean[metric].values
            
            # Scale
            X = self.scaler.fit_transform(X)
            
            # Train model
            model = xgb.XGBRegressor(n_estimators=50, max_depth=4, random_state=42)
            model.fit(X, y)
            
            # Feature importance
            feature_importance = dict(zip(feature_cols, model.feature_importances_))
            
            # SHAP values
            explainer = shap.TreeExplainer(model)
            shap_values = explainer.shap_values(X)
            shap_importance = {}
            for i, feature in enumerate(feature_cols):
                shap_importance[feature] = float(np.abs(shap_values[:, i]).mean())
            
            return feature_importance, shap_importance
            
        except Exception as e:
            logger.error(f"ML causal impact error: {e}")
            return None, None
    
    def _calculate_metric(self, df: pd.DataFrame, metric: str) -> float:
        """Calculate metric value from dataframe."""
        
        if metric in df.columns:
            return df[metric].mean()
        
        # Calculate derived metrics
        metric_calcs = {
            'ROAS': lambda d: (d['Revenue'].sum() / d['Spend'].sum()) if 'Revenue' in d.columns and 'Spend' in d.columns and d['Spend'].sum() > 0 else 0,
            'CPA': lambda d: (d['Spend'].sum() / d['Conversions'].sum()) if 'Conversions' in d.columns and d['Conversions'].sum() > 0 else 0,
            'CTR': lambda d: (d['Clicks'].sum() / d['Impressions'].sum() * 100) if 'Impressions' in d.columns and d['Impressions'].sum() > 0 else 0,
            'CVR': lambda d: (d['Conversions'].sum() / d['Clicks'].sum() * 100) if 'Clicks' in d.columns and d['Clicks'].sum() > 0 else 0,
            'CPC': lambda d: (d['Spend'].sum() / d['Clicks'].sum()) if 'Clicks' in d.columns and d['Clicks'].sum() > 0 else 0,
            'CPM': lambda d: (d['Spend'].sum() / d['Impressions'].sum() * 1000) if 'Impressions' in d.columns and d['Impressions'].sum() > 0 else 0,
            'Revenue': lambda d: d['Revenue'].sum() if 'Revenue' in d.columns else 0,
            'Spend': lambda d: d['Spend'].sum() if 'Spend' in d.columns else 0,
        }
        
        calc_fn = metric_calcs.get(metric)
        if calc_fn:
            return calc_fn(df)
        
        return 0.0
    
    def _calculate_confidence(self, before_df: pd.DataFrame, after_df: pd.DataFrame, metric: str) -> float:
        """Calculate confidence score."""
        
        min_sample = 10
        sample_score = min(1.0, (len(before_df) + len(after_df)) / (2 * min_sample))
        
        try:
            before_vals = [self._calculate_metric(pd.DataFrame([row]), metric) for _, row in before_df.iterrows()]
            after_vals = [self._calculate_metric(pd.DataFrame([row]), metric) for _, row in after_df.iterrows()]
            
            if len(before_vals) > 1 and len(after_vals) > 1:
                _, p_value = stats.ttest_ind(before_vals, after_vals)
                sig_score = 1.0 - p_value
            else:
                sig_score = 0.5
        except:
            sig_score = 0.5
        
        return round(sample_score * 0.4 + sig_score * 0.6, 2)
    
    def _generate_insights(
        self,
        metric: str,
        total_change: float,
        total_change_pct: float,
        contributions: List[ComponentContribution],
        channel_attr: Dict[str, float],
        platform_attr: Dict[str, float]
    ) -> List[str]:
        """Generate human-readable insights."""
        
        insights = []
        
        # Overall change
        direction = "increased" if total_change > 0 else "decreased"
        insights.append(
            f"**{metric} {direction} by {abs(total_change):.2f} ({abs(total_change_pct):.1f}%)**"
        )
        
        # Primary driver
        if contributions:
            primary = contributions[0]
            insights.append(
                f"🎯 Primary driver: **{primary.component}** contributed {primary.percentage_contribution:.0f}% "
                f"({'+' if primary.absolute_change >= 0 else ''}{primary.absolute_change:.2f})"
            )
        
        # Top 3 contributors
        top_3 = sorted(contributions, key=lambda x: abs(x.absolute_change), reverse=True)[:3]
        if len(top_3) > 1:
            breakdown = ", ".join([
                f"{c.component} ({'+' if c.absolute_change >= 0 else ''}{c.absolute_change:.2f})"
                for c in top_3
            ])
            insights.append(f"📊 Top contributors: {breakdown}")
        
        # Platform attribution
        if platform_attr:
            sorted_platforms = sorted(platform_attr.items(), key=lambda x: abs(x[1]), reverse=True)
            if sorted_platforms:
                top_platform, top_contrib = sorted_platforms[0]
                insights.append(
                    f"🏆 **{top_platform}** had the largest platform impact "
                    f"({'+' if top_contrib >= 0 else ''}{top_contrib:.2f})"
                )
        
        return insights
    
    def _generate_recommendations(
        self,
        metric: str,
        contributions: List[ComponentContribution],
        channel_attr: Dict[str, float],
        platform_attr: Dict[str, float]
    ) -> List[str]:
        """Generate actionable recommendations."""
        
        recommendations = []
        
        # Focus on high-actionability components
        actionable = [c for c in contributions if c.actionability == "high"]
        actionable.sort(key=lambda x: abs(x.absolute_change), reverse=True)
        
        for contrib in actionable[:3]:
            if contrib.impact_direction == "negative":
                if "CPC" in contrib.component:
                    recommendations.append(
                        f"💡 **Reduce CPC**: {contrib.component} increased by {abs(contrib.delta_pct):.1f}%. "
                        "Consider bid optimization or improving Quality Score."
                    )
                elif "CVR" in contrib.component or "Conversion" in contrib.component:
                    recommendations.append(
                        f"💡 **Improve Conversion Rate**: {contrib.component} declined by {abs(contrib.delta_pct):.1f}%. "
                        "Review landing pages, offers, and audience targeting."
                    )
                elif "Spend" in contrib.component:
                    recommendations.append(
                        f"💡 **Optimize Spend**: {contrib.component} increased by {abs(contrib.delta_pct):.1f}%. "
                        "Review budget allocation and pause underperforming campaigns."
                    )
            elif contrib.impact_direction == "positive":
                recommendations.append(
                    f"✅ **Scale Success**: {contrib.component} improved by {abs(contrib.delta_pct):.1f}%. "
                    "Consider increasing investment in this area."
                )
        
        # Platform-specific recommendations
        if platform_attr:
            worst_platform = min(platform_attr.items(), key=lambda x: x[1])
            if worst_platform[1] < 0:
                recommendations.append(
                    f"⚠️ **Review {worst_platform[0]}**: This platform contributed negatively "
                    f"({worst_platform[1]:.2f}). Consider pausing or optimizing."
                )
        
        return recommendations[:5]  # Top 5 recommendations
    
    def _create_empty_result(self, metric: str) -> CausalAnalysisResult:
        """Create empty result for error cases."""
        return CausalAnalysisResult(
            metric=metric,
            before_value=0.0,
            after_value=0.0,
            total_change=0.0,
            total_change_pct=0.0,
            contributions=[],
            primary_driver=None,
            secondary_drivers=[],
            insights=["Insufficient data for causal analysis"],
            recommendations=[]
        )

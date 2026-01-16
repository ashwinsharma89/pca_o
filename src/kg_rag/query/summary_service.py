"""
Summary Service for KG-RAG Auto-Summary Page.

Generates comprehensive, data-driven performance summaries.
"""

import logging
from typing import Dict, Any, List
from datetime import datetime

from src.kg_rag.client.connection import get_neo4j_connection

logger = logging.getLogger(__name__)


class SummaryService:
    """Service to generate reason-based performance summaries."""
    
    # Dimensions to analyze
    DIMENSIONS = {
        "platform": {"property": "platform", "label": "Platform"},
        "channel": {"property": "channel", "label": "Channel"},
        "funnel": {"property": "funnel", "label": "Funnel Stage"},
        "device": {"property": "device_types", "label": "Device", "is_list": True},
        "age": {"property": "age_range", "label": "Age Group"},
    }
    
    # Funnel-specific KPI definitions (domain knowledge)
    # Upper Funnel: Awareness -> High Reach/Impressions, frequency doesn't matter
    # Middle Funnel: Engagement -> CTR, VTR (Video Through Rate)
    # Lower Funnel: Conversion -> ROAS, CPA, Conversions
    FUNNEL_KPIS = {
        "Upper": {"primary_kpi": "reach", "secondary_kpi": "impressions", "goal": "Awareness", "good_direction": "high"},
        "Middle": {"primary_kpi": "ctr", "secondary_kpi": "vtr", "goal": "Engagement", "good_direction": "high"},
        "Lower": {"primary_kpi": "roas", "secondary_kpi": "cpa", "goal": "Conversion", "good_direction": "high_roas_low_cpa"},
        "Conversion": {"primary_kpi": "roas", "secondary_kpi": "cpa", "goal": "Conversion", "good_direction": "high_roas_low_cpa"},
    }
    
    def __init__(self):
        self._conn = get_neo4j_connection()
    
    def generate_summary(self) -> Dict[str, Any]:
        """Generate complete performance summary."""
        breakdowns = {}
        all_segments = []
        
        for dim_key, dim_config in self.DIMENSIONS.items():
            data = self._get_dimension_breakdown(dim_config)
            breakdowns[f"{dim_key}_breakdown"] = data
            for row in data:
                row["dimension"] = dim_key
                all_segments.append(row)
        
        # Calculate overall average ROAS for comparison
        avg_roas = self._calculate_weighted_avg(all_segments, "roas", "spend")
        avg_cpa = self._calculate_weighted_avg([s for s in all_segments if s.get("cpa", 0) > 0], "cpa", "spend")
        
        what_worked = self._identify_top_performers(all_segments, avg_roas, avg_cpa)
        what_didnt = self._identify_underperformers(all_segments, avg_roas, avg_cpa)
        optimizations = self._generate_optimizations(all_segments, avg_roas, avg_cpa)
        
        # Generate funnel-specific insights using correct KPIs
        funnel_insights = self._generate_funnel_insights(breakdowns.get("funnel_breakdown", []))
        
        return {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            **breakdowns,
            "what_worked": what_worked,
            "what_didnt_work": what_didnt,
            "optimizations": optimizations,
            "funnel_insights": funnel_insights,
            "averages": {"roas": round(avg_roas, 2), "cpa": round(avg_cpa, 2)}
        }
    
    def _generate_funnel_insights(self, funnel_data: List[Dict]) -> List[Dict]:
        """Generate insights using funnel-specific KPIs."""
        insights = []
        
        for funnel in funnel_data:
            name = funnel.get("name", "")
            # Normalize name for lookup (handle variations)
            stage_key = None
            if "upper" in name.lower():
                stage_key = "Upper"
            elif "middle" in name.lower():
                stage_key = "Middle" 
            elif "lower" in name.lower() or "conv" in name.lower():
                stage_key = "Lower"
            
            if not stage_key:
                continue
                
            config = self.FUNNEL_KPIS.get(stage_key, {})
            kpi = config.get("primary_kpi", "roas")
            goal = config.get("goal", "Performance")
            
            # Calculate the appropriate KPI value
            spend = funnel.get("spend", 0)
            impressions = funnel.get("impressions", 0)
            clicks = funnel.get("clicks", 0)
            conversions = funnel.get("conversions", 0)
            revenue = funnel.get("revenue", 0)
            
            # Compute metrics
            ctr = (clicks / impressions * 100) if impressions > 0 else 0
            roas = (revenue / spend) if spend > 0 else 0
            cpa = (spend / conversions) if conversions > 0 else 0
            
            # Build insight based on stage
            if stage_key == "Upper":
                # Awareness: High reach/impressions
                insights.append({
                    "stage": name,
                    "goal": "Awareness",
                    "kpi": "Impressions",
                    "value": f"{impressions:,}",
                    "spend": round(spend, 2),
                    "insight": f"Upper Funnel delivering {impressions:,} impressions. Goal is reach & awareness.",
                    "status": "good" if impressions > 1000000 else "needs_attention"
                })
            elif stage_key == "Middle":
                # Engagement: CTR/VTR
                insights.append({
                    "stage": name,
                    "goal": "Engagement", 
                    "kpi": "CTR",
                    "value": f"{ctr:.2f}%",
                    "spend": round(spend, 2),
                    "insight": f"Middle Funnel CTR is {ctr:.2f}%. Target higher engagement rates.",
                    "status": "good" if ctr > 1.0 else "needs_attention"
                })
            else:  # Lower
                # Conversion: ROAS/CPA
                insights.append({
                    "stage": name,
                    "goal": "Conversion",
                    "kpi": "ROAS",
                    "value": f"{roas:.2f}x",
                    "secondary_kpi": "CPA",
                    "secondary_value": f"${cpa:.2f}",
                    "spend": round(spend, 2),
                    "insight": f"Lower Funnel ROAS is {roas:.2f}x with CPA ${cpa:.2f}. Optimize for conversions.",
                    "status": "good" if roas > 3.0 else "needs_attention"
                })
        
        return insights
    
    def _get_dimension_breakdown(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Get performance breakdown for a dimension."""
        prop = config["property"]
        is_list = config.get("is_list", False)
        
        if is_list:
            dims_clause = f"UNWIND m.{prop} AS segment WITH segment, m"
        else:
            dims_clause = f"WITH m.{prop} AS segment, m"
        
        query = f"""
        MATCH (c:Campaign)-[:HAS_PERFORMANCE]->(m:Metric)
        {dims_clause}
        WHERE segment IS NOT NULL
        WITH segment,
             SUM(m.spend) AS spend,
             SUM(m.revenue) AS revenue,
             SUM(m.conversions) AS conversions,
             SUM(m.clicks) AS clicks,
             SUM(m.impressions) AS impressions
        WHERE spend > 100
        RETURN segment AS name,
               spend,
               revenue,
               conversions,
               clicks,
               impressions,
               CASE WHEN spend > 0 THEN revenue / spend ELSE 0 END AS roas,
               CASE WHEN conversions > 0 THEN spend / conversions ELSE 0 END AS cpa
        ORDER BY spend DESC
        LIMIT 10
        """
        
        results = self._conn.execute_query(query, {})
        return [dict(r) for r in results]
    
    def _calculate_weighted_avg(self, segments: List[Dict], metric: str, weight: str) -> float:
        """Calculate weighted average of a metric."""
        total_weighted = sum(s.get(metric, 0) * s.get(weight, 0) for s in segments)
        total_weight = sum(s.get(weight, 0) for s in segments)
        return total_weighted / total_weight if total_weight > 0 else 0
    
    def _identify_top_performers(self, segments: List[Dict], avg_roas: float, avg_cpa: float) -> List[Dict]:
        """Identify segments that performed well with data-backed reasons."""
        top = []
        
        # Helper: Check if segment is Upper funnel (should NOT be evaluated on ROAS/CPA)
        def is_upper_funnel(s: Dict) -> bool:
            name = str(s.get("name", "")).lower()
            return s.get("dimension") == "funnel" and "upper" in name
        
        # High ROAS performers (exclude Upper funnel - they're for awareness, not conversions)
        roas_candidates = [s for s in segments 
                         if s.get("roas", 0) > avg_roas * 1.1 
                         and s.get("spend", 0) > 1000 
                         and not is_upper_funnel(s)]
        high_roas = sorted(roas_candidates, key=lambda x: x["roas"], reverse=True)[:3]
        
        for s in high_roas:
            pct_above = ((s["roas"] / avg_roas) - 1) * 100 if avg_roas > 0 else 0
            top.append({
                "segment": s["name"],
                "dimension": s.get("dimension", "unknown"),
                "metric": "ROAS",
                "value": round(s["roas"], 2),
                "vs_avg": f"+{pct_above:.0f}%",
                "spend": round(s["spend"], 2),
                "reason": f"ROAS of {s['roas']:.2f} is {pct_above:.0f}% above average ({avg_roas:.2f})."
            })
        
        # Low CPA performers (exclude Upper funnel)
        cpa_candidates = [s for s in segments 
                        if s.get("cpa", 0) > 0 
                        and s.get("cpa", 0) < avg_cpa * 0.9 
                        and s.get("spend", 0) > 1000
                        and not is_upper_funnel(s)]
        low_cpa = sorted(cpa_candidates, key=lambda x: x["cpa"])[:2]
        
        for s in low_cpa:
            pct_below = (1 - (s["cpa"] / avg_cpa)) * 100 if avg_cpa > 0 else 0
            top.append({
                "segment": s["name"],
                "dimension": s.get("dimension", "unknown"),
                "metric": "CPA",
                "value": round(s["cpa"], 2),
                "vs_avg": f"-{pct_below:.0f}%",
                "spend": round(s["spend"], 2),
                "reason": f"CPA of ${s['cpa']:.2f} is {pct_below:.0f}% below average (${avg_cpa:.2f})."
            })
        
        return top
    
    def _identify_underperformers(self, segments: List[Dict], avg_roas: float, avg_cpa: float) -> List[Dict]:
        """Identify underperforming segments with data-backed reasons."""
        under = []
        
        # Helper: Check if segment is Upper funnel (should NOT be evaluated on ROAS/CPA)
        def is_upper_funnel(s: Dict) -> bool:
            name = str(s.get("name", "")).lower()
            return s.get("dimension") == "funnel" and "upper" in name
        
        # Low ROAS with high spend (exclude Upper funnel)
        roas_candidates = [s for s in segments 
                         if s.get("roas", 0) < avg_roas * 0.8 
                         and s.get("spend", 0) > 5000
                         and not is_upper_funnel(s)]
        low_roas = sorted(roas_candidates, key=lambda x: x["spend"], reverse=True)[:3]
        
        for s in low_roas:
            pct_below = (1 - (s["roas"] / avg_roas)) * 100 if avg_roas > 0 else 0
            under.append({
                "segment": s["name"],
                "dimension": s.get("dimension", "unknown"),
                "metric": "ROAS",
                "value": round(s["roas"], 2),
                "vs_avg": f"-{pct_below:.0f}%",
                "spend": round(s["spend"], 2),
                "reason": f"ROAS of {s['roas']:.2f} is {pct_below:.0f}% below average with ${s['spend']:,.0f} spend."
            })
        
        # High CPA (exclude Upper funnel)
        cpa_candidates = [s for s in segments 
                        if s.get("cpa", 0) > avg_cpa * 1.3 
                        and s.get("spend", 0) > 5000
                        and not is_upper_funnel(s)]
        high_cpa = sorted(cpa_candidates, key=lambda x: x["cpa"], reverse=True)[:2]
        
        for s in high_cpa:
            pct_above = ((s["cpa"] / avg_cpa) - 1) * 100 if avg_cpa > 0 else 0
            under.append({
                "segment": s["name"],
                "dimension": s.get("dimension", "unknown"),
                "metric": "CPA",
                "value": round(s["cpa"], 2),
                "vs_avg": f"+{pct_above:.0f}%",
                "spend": round(s["spend"], 2),
                "reason": f"CPA of ${s['cpa']:.2f} is {pct_above:.0f}% above average (${avg_cpa:.2f})."
            })
        
        return under
    
    def _generate_optimizations(self, segments: List[Dict], avg_roas: float, avg_cpa: float) -> List[Dict]:
        """Generate actionable optimizations with data-backed reasons."""
        opts = []
        
        # Helper: Check if segment is Upper funnel (should NOT be evaluated on ROAS/CPA)
        def is_upper_funnel(s: Dict) -> bool:
            name = str(s.get("name", "")).lower()
            return s.get("dimension") == "funnel" and "upper" in name
        
        # SCALE: High ROAS, Low Spend (opportunity) - exclude Upper funnel
        scale_candidates = [s for s in segments 
                          if s.get("roas", 0) > avg_roas * 1.1 
                          and s.get("spend", 0) < 50000
                          and not is_upper_funnel(s)]
        scale_candidates = sorted(scale_candidates, key=lambda x: x["roas"], reverse=True)[:2]
        for s in scale_candidates:
            opts.append({
                "action": "SCALE",
                "segment": s["name"],
                "dimension": s.get("dimension", "unknown"),
                "reason": f"High ROAS ({s['roas']:.2f}) with only ${s['spend']:,.0f} spend. Opportunity to scale."
            })
        
        # CUT: Low ROAS, High Spend (waste)
        cut_candidates = sorted([s for s in segments if s.get("roas", 0) < avg_roas * 0.7 and s.get("spend", 0) > 50000],
                               key=lambda x: x["spend"], reverse=True)[:2]
        for s in cut_candidates:
            opts.append({
                "action": "CUT",
                "segment": s["name"],
                "dimension": s.get("dimension", "unknown"),
                "reason": f"Low ROAS ({s['roas']:.2f}) with ${s['spend']:,.0f} spend. Consider reducing budget."
            })
        
        # HOLD: Average performance
        hold_candidates = [s for s in segments if avg_roas * 0.9 <= s.get("roas", 0) <= avg_roas * 1.1][:2]
        for s in hold_candidates:
            opts.append({
                "action": "HOLD",
                "segment": s["name"],
                "dimension": s.get("dimension", "unknown"),
                "reason": f"ROAS ({s['roas']:.2f}) is near average. Maintain current investment."
            })
        
        return opts

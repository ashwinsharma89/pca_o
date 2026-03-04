from typing import Dict, Any, List, Optional
from src.kg_rag.client.connection import get_kuzu_connection

class OptimizationTemplate:
    """Template for performance optimization analysis."""
    
    # Map friendly dimension names to node properties
    DIMENSION_MAP = {
        "channel": "channel",
        "platform": "platform",
        "funnel": "funnel",
        "device": "device_types",
        "age": "age_range",
        "demographic": "age_range",
        "gender": "gender_targeting",
        "geo": "geo_countries",
        "location": "geo_countries",
        "placement": "placement",
        "ad_type": "ad_type"
    }
    
    def run(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Run optimization analysis.
        
        Args:
            context: Query context (dimension, platform, etc.)
            
        Returns:
            Optimization results and recommendations
        """
        dimension = context.get("dimension")
        platform_id = context.get("platform_id")
        
        # Determine property to aggregate by
        prop = self.DIMENSION_MAP.get(dimension, dimension)
        
        # Build Cypher Query
        # Handle list properties (unwind) vs scalar
        is_list = prop in ["device_types", "geo_countries"]
        
        dims_clause = f"UNWIND m.{prop} AS segment WITH segment, m" if is_list else f"WITH m.{prop} AS segment, m"
        
        filters = []
        params = {}
        
        if platform_id:
            filters.append("m.platform = $platform_id")
            params["platform_id"] = platform_id
            
        where_clause = "WHERE segment IS NOT NULL"
        if filters:
            where_clause += " AND " + " AND ".join(filters)
        
        query = f"""
        MATCH (m:Metric)
        {dims_clause}
        {where_clause}
        WITH segment,
             SUM(m.spend) AS spend,
             SUM(m.revenue) AS revenue,
             SUM(m.conversions) AS conversions,
             SUM(m.clicks) AS clicks,
             SUM(m.impressions) AS impressions
        WHERE spend > 0
        RETURN segment, spend, revenue, conversions, clicks, impressions,
               CASE WHEN spend > 0 THEN revenue / spend ELSE 0 END AS roas,
               CASE WHEN conversions > 0 THEN spend / conversions ELSE 0 END AS cpa
        ORDER BY roas DESC
        """
        
        conn = get_kuzu_connection()
        results = conn.execute_query(query, params)
        
        recommendations = self._generate_recommendations(results, dimension)
        
        return {
            "success": True,
            "dimension": dimension,
            "data": results,
            "recommendations": recommendations,
            "summary": f"Analyzed {len(results)} {dimension} segments."
        }
        
    def _generate_recommendations(self, results: List[Dict[str, Any]], dimension: str) -> List[str]:
        """Generate text optimization recommendations."""
        if not results:
            return ["Not enough data to generate recommendations."]
            
        recs = []
        
        # Sorts
        top_roas = sorted(results, key=lambda x: x['roas'], reverse=True)
        top_cpa = sorted([r for r in results if r['conversions'] > 0], key=lambda x: x['cpa'])
        high_spend = sorted(results, key=lambda x: x['spend'], reverse=True)
        
        # 1. Best Performers (High ROAS, Significant Spend)
        best = [r for r in top_roas if r['spend'] > 1000][:3]
        if best:
            names = ", ".join([str(r['segment']) for r in best])
            recs.append(f"🏆 Top Performers: **{names}** are generating the highest Return on Ad Spend (ROAS). Consider scaling budget here.")
            
        # 2. Underperformers (High Spend, Low ROAS)
        avg_roas = sum(r['roas'] * r['spend'] for r in results) / sum(r['spend'] for r in results) if results else 0
        wasted = [r for r in high_spend if r['roas'] < avg_roas * 0.7][:3]
        if wasted:
            names = ", ".join([str(r['segment']) for r in wasted])
            recs.append(f"⚠️ Optimization Needed: **{names}** have high spend but below-average ROAS. Review creatives or lower bids.")
            
        # 3. Efficiency Opportunities (Low CPA)
        efficient = [r for r in top_cpa if r['spend'] < 500][:3]
        if efficient:
            names = ", ".join([str(r['segment']) for r in efficient])
            recs.append(f"📈 Opportunity: **{names}** are converting efficiently (Low CPA) but have low spend. Test increasing volume.")
            
        return recs

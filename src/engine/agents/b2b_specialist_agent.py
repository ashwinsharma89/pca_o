"""
B2B Specialist Agent
Applies B2B-specific context, benchmarks, and analysis overlays
"""

from typing import Dict, Any, Optional, List
import pandas as pd
import numpy as np
from loguru import logger

from src.platform.models.campaign import CampaignContext, BusinessModel, TargetAudienceLevel


class B2BSpecialistAgent:
    """Applies B2B-specific context and benchmarks to campaign analysis"""
    
    def __init__(self, rag_retriever=None):
        """
        Initialize B2B specialist agent
        
        Args:
            rag_retriever: Optional RAG retriever for knowledge base access
        """
        self.rag = rag_retriever
        self.b2b_benchmarks = self._load_b2b_benchmarks()
        self.b2c_benchmarks = self._load_b2c_benchmarks()
        logger.info("Initialized B2B Specialist Agent")
    
    def enhance_analysis(self, base_insights: Dict[str, Any], 
                        campaign_context: Optional[CampaignContext] = None,
                        campaign_data: Optional[pd.DataFrame] = None) -> Dict[str, Any]:
        """
        Enhance base analysis with B2B or B2C specific insights
        
        Args:
            base_insights: Base campaign analysis insights
            campaign_context: Business context for the campaign
            campaign_data: Raw campaign data for additional analysis
            
        Returns:
            Enhanced insights with business model specific analysis
        """
        if not campaign_context:
            # Try to infer business model from data
            campaign_context = self._infer_campaign_context(campaign_data)
        
        if campaign_context.business_model == BusinessModel.B2B:
            return self._enhance_b2b_analysis(base_insights, campaign_context, campaign_data)
        elif campaign_context.business_model == BusinessModel.B2C:
            return self._enhance_b2c_analysis(base_insights, campaign_context, campaign_data)
        elif campaign_context.business_model == BusinessModel.B2B2C:
            return self._enhance_hybrid_analysis(base_insights, campaign_context, campaign_data)
        
        return base_insights
    
    def _enhance_b2b_analysis(self, base_insights: Dict[str, Any],
                             campaign_context: CampaignContext,
                             campaign_data: Optional[pd.DataFrame]) -> Dict[str, Any]:
        """
        Add B2B-specific analysis overlay
        
        Args:
            base_insights: Base insights
            campaign_context: B2B campaign context
            campaign_data: Raw campaign data
            
        Returns:
            Enhanced insights with B2B analysis
        """
        logger.info(f"Enhancing analysis with B2B context: {campaign_context.industry_vertical}")
        
        # B2B-specific analysis
        b2b_insights = {
            'business_model': 'B2B',
            'industry_vertical': campaign_context.industry_vertical,
            'context_summary': campaign_context.get_context_summary(),
            'lead_quality_analysis': self._analyze_lead_quality(base_insights, campaign_context, campaign_data),
            'pipeline_contribution': self._estimate_pipeline_impact(base_insights, campaign_context),
            'account_based_metrics': self._analyze_account_engagement(base_insights, campaign_data),
            'sales_cycle_alignment': self._check_sales_cycle_fit(base_insights, campaign_context),
            'audience_seniority_analysis': self._analyze_audience_seniority(campaign_context, campaign_data),
            'b2b_benchmarks': self._get_relevant_benchmarks(campaign_context, 'b2b')
        }
        
        # Retrieve B2B best practices from knowledge base
        if self.rag:
            try:
                context = self.rag.retrieve(
                    query=f"B2B {campaign_context.industry_vertical} campaign optimization best practices",
                    filters={'category': 'b2b', 'priority': 1}
                )
                b2b_insights['knowledge_base_context'] = context
            except Exception as e:
                logger.warning(f"Could not retrieve RAG context: {e}")
        
        # Merge with base insights
        enhanced_insights = self._merge_insights(base_insights, b2b_insights)
        
        # Add B2B-specific recommendations
        enhanced_insights['recommendations'] = self._generate_b2b_recommendations(
            enhanced_insights, 
            campaign_context
        )
        
        return enhanced_insights
    
    def _enhance_b2c_analysis(self, base_insights: Dict[str, Any],
                             campaign_context: CampaignContext,
                             campaign_data: Optional[pd.DataFrame]) -> Dict[str, Any]:
        """
        Add B2C-specific analysis overlay
        
        Args:
            base_insights: Base insights
            campaign_context: B2C campaign context
            campaign_data: Raw campaign data
            
        Returns:
            Enhanced insights with B2C analysis
        """
        logger.info(f"Enhancing analysis with B2C context: {campaign_context.industry_vertical}")
        
        # B2C-specific analysis
        b2c_insights = {
            'business_model': 'B2C',
            'industry_vertical': campaign_context.industry_vertical,
            'context_summary': campaign_context.get_context_summary(),
            'purchase_behavior_analysis': self._analyze_purchase_behavior(base_insights, campaign_context, campaign_data),
            'customer_acquisition_efficiency': self._analyze_cac_efficiency(base_insights, campaign_context),
            'lifetime_value_analysis': self._analyze_ltv(base_insights, campaign_context),
            'conversion_funnel_analysis': self._analyze_b2c_funnel(base_insights, campaign_data),
            'b2c_benchmarks': self._get_relevant_benchmarks(campaign_context, 'b2c')
        }
        
        # Merge with base insights
        enhanced_insights = self._merge_insights(base_insights, b2c_insights)
        
        # Add B2C-specific recommendations
        enhanced_insights['recommendations'] = self._generate_b2c_recommendations(
            enhanced_insights,
            campaign_context
        )
        
        return enhanced_insights
    
    def _enhance_hybrid_analysis(self, base_insights: Dict[str, Any],
                                 campaign_context: CampaignContext,
                                 campaign_data: Optional[pd.DataFrame]) -> Dict[str, Any]:
        """
        Add hybrid B2B2C analysis
        
        Args:
            base_insights: Base insights
            campaign_context: Hybrid campaign context
            campaign_data: Raw campaign data
            
        Returns:
            Enhanced insights with hybrid analysis
        """
        # Apply both B2B and B2C analysis
        b2b_enhanced = self._enhance_b2b_analysis(base_insights, campaign_context, campaign_data)
        b2c_enhanced = self._enhance_b2c_analysis(base_insights, campaign_context, campaign_data)
        
        # Merge both analyses
        hybrid_insights = {
            'business_model': 'B2B2C',
            'b2b_analysis': b2b_enhanced,
            'b2c_analysis': b2c_enhanced,
            'hybrid_recommendations': self._generate_hybrid_recommendations(b2b_enhanced, b2c_enhanced)
        }
        
        return self._merge_insights(base_insights, hybrid_insights)
    
    # B2B Analysis Methods
    
    def _analyze_lead_quality(self, insights: Dict[str, Any],
                              context: CampaignContext,
                              data: Optional[pd.DataFrame]) -> Dict[str, Any]:
        """Analyze lead quality metrics for B2B"""
        analysis = {
            'metric': 'Lead Quality',
            'findings': []
        }
        
        # Calculate lead quality indicators
        if data is not None and 'Conversions' in data.columns:
            total_leads = data['Conversions'].sum()
            
            # Estimate MQL rate (if not available, use industry benchmark)
            mql_rate = 0.25  # Default 25% MQL rate
            estimated_mqls = total_leads * mql_rate
            
            # Estimate SQL rate
            sql_rate = 0.30  # 30% of MQLs become SQLs
            estimated_sqls = estimated_mqls * sql_rate
            
            analysis['total_leads'] = int(total_leads)
            analysis['estimated_mqls'] = int(estimated_mqls)
            analysis['estimated_sqls'] = int(estimated_sqls)
            analysis['mql_rate'] = f"{mql_rate*100:.0f}%"
            analysis['sql_rate'] = f"{sql_rate*100:.0f}%"
            
            # Calculate cost per SQL
            if 'Spend' in data.columns:
                total_spend = data['Spend'].sum()
                cost_per_lead = total_spend / total_leads if total_leads > 0 else 0
                cost_per_sql = total_spend / estimated_sqls if estimated_sqls > 0 else 0
                
                analysis['cost_per_lead'] = f"${cost_per_lead:.2f}"
                analysis['cost_per_sql'] = f"${cost_per_sql:.2f}"
                
                # Compare to target CAC
                if context.target_cac:
                    if cost_per_sql <= context.target_cac:
                        analysis['findings'].append(f"✅ Cost per SQL (${cost_per_sql:.2f}) is within target CAC (${context.target_cac:.2f})")
                        analysis['status'] = 'good'
                    else:
                        analysis['findings'].append(f"⚠️ Cost per SQL (${cost_per_sql:.2f}) exceeds target CAC (${context.target_cac:.2f})")
                        analysis['status'] = 'needs_improvement'
                        analysis['recommendation'] = "Focus on lead quality over quantity, optimize targeting for decision-makers"
        
        return analysis
    
    def _estimate_pipeline_impact(self, insights: Dict[str, Any],
                                  context: CampaignContext) -> Dict[str, Any]:
        """Estimate pipeline contribution"""
        analysis = {
            'metric': 'Pipeline Impact',
            'findings': []
        }
        
        # Get conversion data from insights
        metrics = insights.get('metrics', {})
        conversions = metrics.get('total_conversions', 0)
        
        if conversions > 0 and context.average_deal_size:
            # Estimate pipeline value
            sql_rate = 0.30  # 30% of MQLs become SQLs
            opportunity_rate = 0.40  # 40% of SQLs become opportunities
            close_rate = 0.25  # 25% of opportunities close
            
            estimated_sqls = conversions * 0.25 * sql_rate
            estimated_opportunities = estimated_sqls * opportunity_rate
            estimated_closed_deals = estimated_opportunities * close_rate
            
            pipeline_value = estimated_opportunities * context.average_deal_size
            revenue_impact = estimated_closed_deals * context.average_deal_size
            
            analysis['estimated_pipeline_value'] = f"${pipeline_value:,.0f}"
            analysis['estimated_revenue'] = f"${revenue_impact:,.0f}"
            analysis['estimated_opportunities'] = int(estimated_opportunities)
            analysis['estimated_closed_deals'] = int(estimated_closed_deals)
            
            analysis['findings'].append(f"💰 Estimated pipeline contribution: ${pipeline_value:,.0f}")
            analysis['findings'].append(f"💵 Estimated revenue impact: ${revenue_impact:,.0f}")
            
            # Calculate ROI
            spend = metrics.get('total_spend', 0)
            if spend > 0:
                roi = ((revenue_impact - spend) / spend) * 100
                analysis['roi'] = f"{roi:.1f}%"
                analysis['findings'].append(f"📈 Estimated ROI: {roi:.1f}%")
        
        return analysis
    
    def _analyze_account_engagement(self, insights: Dict[str, Any],
                                   data: Optional[pd.DataFrame]) -> Dict[str, Any]:
        """Analyze account-based engagement metrics"""
        analysis = {
            'metric': 'Account Engagement',
            'findings': []
        }
        
        if data is not None:
            # Look for account-level indicators
            account_cols = [col for col in data.columns if 'account' in col.lower() or 'company' in col.lower()]
            
            if account_cols:
                account_col = account_cols[0]
                unique_accounts = data[account_col].nunique()
                
                analysis['unique_accounts_reached'] = unique_accounts
                analysis['findings'].append(f"🎯 Reached {unique_accounts} unique accounts")
                
                # Account engagement distribution
                if 'Impressions' in data.columns:
                    account_impressions = data.groupby(account_col)['Impressions'].sum()
                    avg_impressions_per_account = account_impressions.mean()
                    
                    analysis['avg_impressions_per_account'] = f"{avg_impressions_per_account:.0f}"
                    analysis['findings'].append(f"📊 Average {avg_impressions_per_account:.0f} impressions per account")
            else:
                analysis['status'] = 'unavailable'
                analysis['findings'].append("ℹ️ Account-level data not available")
        
        return analysis
    
    def _check_sales_cycle_fit(self, insights: Dict[str, Any],
                               context: CampaignContext) -> Dict[str, Any]:
        """Check if campaign aligns with sales cycle"""
        analysis = {
            'metric': 'Sales Cycle Alignment',
            'findings': []
        }
        
        if context.sales_cycle_length:
            # Analyze if campaign duration and strategy align with sales cycle
            sales_cycle_days = context.sales_cycle_length
            
            if sales_cycle_days < 30:
                analysis['cycle_type'] = 'Short'
                analysis['findings'].append(f"⚡ Short sales cycle ({sales_cycle_days} days) - focus on direct response")
                analysis['recommendation'] = "Prioritize bottom-funnel tactics, demo requests, free trials"
            elif sales_cycle_days < 90:
                analysis['cycle_type'] = 'Medium'
                analysis['findings'].append(f"📅 Medium sales cycle ({sales_cycle_days} days) - balance awareness and conversion")
                analysis['recommendation'] = "Mix of educational content and conversion-focused campaigns"
            else:
                analysis['cycle_type'] = 'Long'
                analysis['findings'].append(f"⏳ Long sales cycle ({sales_cycle_days} days) - focus on nurture")
                analysis['recommendation'] = "Emphasize thought leadership, multi-touch nurture, account-based strategies"
            
            analysis['sales_cycle_days'] = sales_cycle_days
        
        return analysis
    
    def _analyze_audience_seniority(self, context: CampaignContext,
                                   data: Optional[pd.DataFrame]) -> Dict[str, Any]:
        """Analyze targeting by audience seniority level"""
        analysis = {
            'metric': 'Audience Seniority',
            'findings': []
        }
        
        if context.target_audience_level:
            level = context.target_audience_level
            analysis['target_level'] = level.value
            
            # Provide seniority-specific recommendations
            if level == TargetAudienceLevel.C_SUITE:
                analysis['findings'].append("👔 Targeting C-suite: Focus on strategic value, ROI, and executive content")
                analysis['recommendation'] = "Use LinkedIn, industry publications, executive events"
                analysis['expected_cpc'] = "High ($15-30)"
                analysis['expected_conversion_rate'] = "Low (1-3%)"
            elif level == TargetAudienceLevel.VP_DIRECTOR:
                analysis['findings'].append("📊 Targeting VP/Director: Balance strategic and tactical messaging")
                analysis['recommendation'] = "Mix of LinkedIn, search, and industry content"
                analysis['expected_cpc'] = "Medium-High ($8-15)"
                analysis['expected_conversion_rate'] = "Medium (3-5%)"
            elif level == TargetAudienceLevel.MANAGER:
                analysis['findings'].append("🎯 Targeting Managers: Focus on practical solutions and efficiency")
                analysis['recommendation'] = "Search, LinkedIn, how-to content"
                analysis['expected_cpc'] = "Medium ($5-10)"
                analysis['expected_conversion_rate'] = "Medium-High (5-8%)"
            elif level == TargetAudienceLevel.INDIVIDUAL_CONTRIBUTOR:
                analysis['findings'].append("👤 Targeting Individual Contributors: Emphasize ease of use and features")
                analysis['recommendation'] = "Broad targeting, product-focused content"
                analysis['expected_cpc'] = "Low-Medium ($3-8)"
                analysis['expected_conversion_rate'] = "High (8-12%)"
        
        return analysis
    
    # B2C Analysis Methods
    
    def _analyze_purchase_behavior(self, insights: Dict[str, Any],
                                   context: CampaignContext,
                                   data: Optional[pd.DataFrame]) -> Dict[str, Any]:
        """Analyze B2C purchase behavior patterns"""
        analysis = {
            'metric': 'Purchase Behavior',
            'findings': []
        }
        
        if context.purchase_frequency:
            analysis['purchase_frequency'] = context.purchase_frequency
            analysis['findings'].append(f"🔄 Purchase frequency: {context.purchase_frequency}")
            
            # Frequency-specific recommendations
            if 'weekly' in context.purchase_frequency.lower():
                analysis['recommendation'] = "Focus on retention and repeat purchase campaigns"
            elif 'monthly' in context.purchase_frequency.lower():
                analysis['recommendation'] = "Balance acquisition and retention"
            else:
                analysis['recommendation'] = "Prioritize new customer acquisition"
        
        if context.average_order_value:
            analysis['average_order_value'] = f"${context.average_order_value:.2f}"
            analysis['findings'].append(f"💰 Average order value: ${context.average_order_value:.2f}")
        
        return analysis
    
    def _analyze_cac_efficiency(self, insights: Dict[str, Any],
                               context: CampaignContext) -> Dict[str, Any]:
        """Analyze customer acquisition cost efficiency"""
        analysis = {
            'metric': 'CAC Efficiency',
            'findings': []
        }
        
        metrics = insights.get('metrics', {})
        spend = metrics.get('total_spend', 0)
        conversions = metrics.get('total_conversions', 0)
        
        if conversions > 0:
            actual_cac = spend / conversions
            analysis['actual_cac'] = f"${actual_cac:.2f}"
            
            if context.target_cac:
                cac_efficiency = (context.target_cac / actual_cac) * 100
                analysis['cac_efficiency'] = f"{cac_efficiency:.1f}%"
                
                if actual_cac <= context.target_cac:
                    analysis['status'] = 'excellent'
                    analysis['findings'].append(f"✅ CAC (${actual_cac:.2f}) is within target (${context.target_cac:.2f})")
                else:
                    analysis['status'] = 'needs_improvement'
                    analysis['findings'].append(f"⚠️ CAC (${actual_cac:.2f}) exceeds target (${context.target_cac:.2f})")
                    analysis['recommendation'] = "Optimize conversion funnel, improve targeting, test creative variations"
        
        return analysis
    
    def _analyze_ltv(self, insights: Dict[str, Any],
                    context: CampaignContext) -> Dict[str, Any]:
        """Analyze lifetime value metrics"""
        analysis = {
            'metric': 'Lifetime Value',
            'findings': []
        }
        
        if context.customer_lifetime_value:
            analysis['ltv'] = f"${context.customer_lifetime_value:.2f}"
            
            metrics = insights.get('metrics', {})
            spend = metrics.get('total_spend', 0)
            conversions = metrics.get('total_conversions', 0)
            
            if conversions > 0:
                cac = spend / conversions
                ltv_cac_ratio = context.customer_lifetime_value / cac if cac > 0 else 0
                
                analysis['ltv_cac_ratio'] = f"{ltv_cac_ratio:.2f}:1"
                
                if ltv_cac_ratio >= 3:
                    analysis['status'] = 'excellent'
                    analysis['findings'].append(f"✅ Excellent LTV:CAC ratio ({ltv_cac_ratio:.2f}:1)")
                elif ltv_cac_ratio >= 2:
                    analysis['status'] = 'good'
                    analysis['findings'].append(f"✅ Good LTV:CAC ratio ({ltv_cac_ratio:.2f}:1)")
                else:
                    analysis['status'] = 'poor'
                    analysis['findings'].append(f"⚠️ Low LTV:CAC ratio ({ltv_cac_ratio:.2f}:1) - need to improve efficiency")
                    analysis['recommendation'] = "Reduce CAC or increase LTV through retention and upsell strategies"
        
        return analysis
    
    def _analyze_b2c_funnel(self, insights: Dict[str, Any],
                           data: Optional[pd.DataFrame]) -> Dict[str, Any]:
        """Analyze B2C conversion funnel"""
        analysis = {
            'metric': 'Conversion Funnel',
            'findings': []
        }
        
        if data is not None:
            # Calculate funnel metrics
            if all(col in data.columns for col in ['Impressions', 'Clicks', 'Conversions']):
                total_impressions = data['Impressions'].sum()
                total_clicks = data['Clicks'].sum()
                total_conversions = data['Conversions'].sum()
                
                ctr = (total_clicks / total_impressions * 100) if total_impressions > 0 else 0
                conversion_rate = (total_conversions / total_clicks * 100) if total_clicks > 0 else 0
                
                analysis['ctr'] = f"{ctr:.2f}%"
                analysis['conversion_rate'] = f"{conversion_rate:.2f}%"
                
                # Identify funnel bottleneck
                if ctr < 1.0:
                    analysis['bottleneck'] = 'Top of funnel (CTR)'
                    analysis['findings'].append("⚠️ Low CTR - improve ad creative and targeting")
                elif conversion_rate < 2.0:
                    analysis['bottleneck'] = 'Bottom of funnel (Conversion)'
                    analysis['findings'].append("⚠️ Low conversion rate - optimize landing page and offer")
                else:
                    analysis['findings'].append("✅ Healthy funnel performance")
        
        return analysis
    
    # Recommendation Generation
    
    def _generate_b2b_recommendations(self, insights: Dict[str, Any],
                                     context: CampaignContext) -> List[Dict[str, str]]:
        """Generate B2B-specific recommendations"""
        recommendations = []
        
        # Lead quality recommendations
        lead_analysis = insights.get('lead_quality_analysis', {})
        if lead_analysis.get('status') == 'needs_improvement':
            recommendations.append({
                'priority': 'high',
                'category': 'Lead Quality',
                'recommendation': lead_analysis.get('recommendation', 'Improve lead quality through better targeting')
            })
        
        # Sales cycle recommendations
        cycle_analysis = insights.get('sales_cycle_alignment', {})
        if cycle_analysis.get('recommendation'):
            recommendations.append({
                'priority': 'medium',
                'category': 'Sales Cycle',
                'recommendation': cycle_analysis['recommendation']
            })
        
        # Audience seniority recommendations
        seniority_analysis = insights.get('audience_seniority_analysis', {})
        if seniority_analysis.get('recommendation'):
            recommendations.append({
                'priority': 'medium',
                'category': 'Audience Targeting',
                'recommendation': seniority_analysis['recommendation']
            })
        
        return recommendations
    
    def _generate_b2c_recommendations(self, insights: Dict[str, Any],
                                     context: CampaignContext) -> List[Dict[str, str]]:
        """Generate B2C-specific recommendations"""
        recommendations = []
        
        # CAC efficiency recommendations
        cac_analysis = insights.get('customer_acquisition_efficiency', {})
        if cac_analysis.get('status') == 'needs_improvement':
            recommendations.append({
                'priority': 'high',
                'category': 'CAC Efficiency',
                'recommendation': cac_analysis.get('recommendation', 'Reduce customer acquisition costs')
            })
        
        # LTV recommendations
        ltv_analysis = insights.get('lifetime_value_analysis', {})
        if ltv_analysis.get('status') == 'poor':
            recommendations.append({
                'priority': 'high',
                'category': 'LTV:CAC Ratio',
                'recommendation': ltv_analysis.get('recommendation', 'Improve LTV:CAC ratio')
            })
        
        # Funnel recommendations
        funnel_analysis = insights.get('conversion_funnel_analysis', {})
        if funnel_analysis.get('bottleneck'):
            recommendations.append({
                'priority': 'high',
                'category': 'Conversion Funnel',
                'recommendation': f"Optimize {funnel_analysis['bottleneck']}"
            })
        
        return recommendations
    
    def _generate_hybrid_recommendations(self, b2b_insights: Dict[str, Any],
                                        b2c_insights: Dict[str, Any]) -> List[Dict[str, str]]:
        """Generate recommendations for hybrid B2B2C model"""
        # Combine recommendations from both models
        b2b_recs = b2b_insights.get('recommendations', [])
        b2c_recs = b2c_insights.get('recommendations', [])
        
        return b2b_recs + b2c_recs
    
    # Utility Methods
    
    def _load_b2b_benchmarks(self) -> Dict[str, Any]:
        """Load B2B industry benchmarks"""
        return {
            'linkedin': {
                'ctr': {'excellent': 0.008, 'good': 0.005, 'poor': 0.003},
                'cpc': {'excellent': 5, 'acceptable': 8, 'high': 12},
                'lead_quality_rate': {'excellent': 0.4, 'good': 0.25, 'poor': 0.15},
                'mql_to_sql_rate': {'excellent': 0.35, 'good': 0.25, 'poor': 0.15}
            },
            'google_search_b2b': {
                'ctr': {'excellent': 0.05, 'good': 0.03, 'poor': 0.02},
                'cpc': {'excellent': 4, 'acceptable': 7, 'high': 15},
                'conversion_rate': {'excellent': 0.08, 'good': 0.05, 'poor': 0.03}
            },
            'general': {
                'ltv_cac_ratio': {'excellent': 3.0, 'good': 2.0, 'poor': 1.0},
                'cac_payback_months': {'excellent': 12, 'acceptable': 18, 'poor': 24}
            }
        }
    
    def _load_b2c_benchmarks(self) -> Dict[str, Any]:
        """Load B2C industry benchmarks"""
        return {
            'meta': {
                'ctr': {'excellent': 0.015, 'good': 0.009, 'poor': 0.005},
                'cpc': {'excellent': 0.50, 'acceptable': 1.00, 'high': 2.00},
                'conversion_rate': {'excellent': 0.05, 'good': 0.025, 'poor': 0.01}
            },
            'google_search_b2c': {
                'ctr': {'excellent': 0.05, 'good': 0.035, 'poor': 0.02},
                'cpc': {'excellent': 1.00, 'acceptable': 2.50, 'high': 5.00},
                'conversion_rate': {'excellent': 0.06, 'good': 0.04, 'poor': 0.02}
            },
            'general': {
                'ltv_cac_ratio': {'excellent': 3.0, 'good': 2.0, 'poor': 1.0},
                'repeat_purchase_rate': {'excellent': 0.30, 'good': 0.20, 'poor': 0.10}
            }
        }
    
    def _get_relevant_benchmarks(self, context: CampaignContext,
                                 model_type: str) -> Dict[str, Any]:
        """Get relevant benchmarks based on context"""
        benchmarks = self.b2b_benchmarks if model_type == 'b2b' else self.b2c_benchmarks
        
        # Return general benchmarks plus any industry-specific ones
        return {
            'general': benchmarks.get('general', {}),
            'linkedin': benchmarks.get('linkedin', {}),
            'google_search': benchmarks.get(f'google_search_{model_type}', {}),
            'meta': benchmarks.get('meta', {})
        }
    
    def _infer_campaign_context(self, data: Optional[pd.DataFrame]) -> CampaignContext:
        """Infer campaign context from data if not provided"""
        # Default to B2C if cannot determine
        return CampaignContext(
            business_model=BusinessModel.B2C,
            industry_vertical="Unknown"
        )
    
    def _merge_insights(self, base_insights: Dict[str, Any],
                       overlay_insights: Dict[str, Any]) -> Dict[str, Any]:
        """Merge base insights with business model specific insights"""
        merged = base_insights.copy()
        
        # Add business model specific insights
        merged['business_model_analysis'] = overlay_insights
        
        # Merge recommendations if present
        if 'recommendations' in overlay_insights:
            base_recs = merged.get('recommendations', [])
            overlay_recs = overlay_insights['recommendations']
            merged['recommendations'] = base_recs + overlay_recs
        
        return merged

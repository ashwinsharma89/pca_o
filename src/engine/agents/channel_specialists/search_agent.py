"""
Search Channel Specialist Agent
Specializes in Google Ads, Bing, DV360 search analysis
"""

from typing import Dict, Any, List
import pandas as pd
import numpy as np
from loguru import logger

from .base_specialist import BaseChannelSpecialist


class SearchBenchmarks:
    """Industry benchmarks for search advertising"""
    
    BENCHMARKS = {
        'ctr': 0.035,  # 3.5% average CTR
        'quality_score': 7.0,  # Average quality score
        'conversion_rate': 0.04,  # 4% conversion rate
        'cpc': 2.50,  # Average CPC
        'impression_share': 0.70,  # 70% impression share
        'roas': 4.0,  # 4:1 ROAS
        'cpa': 50.0  # $50 CPA
    }
    
    @classmethod
    def get_benchmark(cls, metric: str) -> float:
        """Get benchmark value for a metric"""
        return cls.BENCHMARKS.get(metric.lower(), 0)


class SearchChannelAgent(BaseChannelSpecialist):
    """Specializes in Google Ads, Bing, DV360 search analysis"""
    
    def __init__(self, rag_retriever=None):
        super().__init__(rag_retriever)
        self.benchmarks = SearchBenchmarks()
    
    def analyze(self, campaign_data: pd.DataFrame) -> Dict[str, Any]:
        """
        Perform search-specific analysis
        
        Args:
            campaign_data: Campaign performance data
            
        Returns:
            Dictionary with search-specific insights
        """
        logger.info("Starting search channel analysis")
        
        insights = {
            'platform': self.detect_platform(campaign_data),
            'quality_score_analysis': self._analyze_quality_scores(campaign_data),
            'auction_insights': self._analyze_auction_metrics(campaign_data),
            'keyword_performance': self._analyze_keywords(campaign_data),
            'impression_share_gaps': self._analyze_impression_share(campaign_data),
            'match_type_efficiency': self._analyze_match_types(campaign_data),
            'search_term_analysis': self._analyze_search_terms(campaign_data)
        }
        
        # Retrieve search-specific best practices
        objective = campaign_data.get('Campaign_Objective', ['performance'])[0] if 'Campaign_Objective' in campaign_data.columns else 'performance'
        context = self.retrieve_knowledge(
            query=f"search campaign optimization {objective}",
            filters={'category': 'search', 'priority': 1}
        )
        
        # Generate recommendations
        insights['recommendations'] = self._generate_recommendations(insights, context)
        insights['overall_health'] = self._calculate_overall_health(insights)
        
        logger.info(f"Search analysis complete. Health: {insights['overall_health']}")
        return insights
    
    def get_benchmarks(self) -> Dict[str, float]:
        """Get search-specific benchmarks"""
        return SearchBenchmarks.BENCHMARKS
    
    def _analyze_quality_scores(self, data: pd.DataFrame) -> Dict[str, Any]:
        """
        Analyze Quality Score metrics (Google Ads specific)
        
        Args:
            data: Campaign data
            
        Returns:
            Quality score analysis
        """
        analysis = {
            'metric': 'Quality Score',
            'status': 'unknown',
            'findings': []
        }
        
        # Check if Quality Score column exists
        qs_cols = [col for col in data.columns if 'quality' in col.lower() and 'score' in col.lower()]
        
        if not qs_cols:
            analysis['status'] = 'unavailable'
            analysis['findings'].append("Quality Score data not available in dataset")
            return analysis
        
        qs_col = qs_cols[0]
        avg_qs = data[qs_col].mean()
        benchmark = self.benchmarks.get_benchmark('quality_score')
        
        analysis['average_score'] = round(avg_qs, 2)
        analysis['benchmark'] = benchmark
        analysis['status'] = self._calculate_metric_health(avg_qs, benchmark, higher_is_better=True)
        
        # Distribution analysis
        if avg_qs < 5:
            analysis['findings'].append("⚠️ Low Quality Scores detected - review ad relevance and landing page experience")
            analysis['recommendation'] = "Improve ad copy relevance, landing page experience, and expected CTR"
        elif avg_qs >= 8:
            analysis['findings'].append("✅ Excellent Quality Scores - maintain current ad quality")
        else:
            analysis['findings'].append("Quality Scores are average - opportunity for improvement")
        
        # Low QS keywords
        low_qs = data[data[qs_col] < 5]
        if len(low_qs) > 0:
            analysis['low_qs_count'] = len(low_qs)
            analysis['findings'].append(f"{len(low_qs)} keywords with QS < 5 need attention")
        
        return analysis
    
    def _analyze_auction_metrics(self, data: pd.DataFrame) -> Dict[str, Any]:
        """
        Analyze auction insights and competitive metrics
        
        Args:
            data: Campaign data
            
        Returns:
            Auction insights analysis
        """
        analysis = {
            'metric': 'Auction Performance',
            'findings': []
        }
        
        # Check for CPC trends
        if 'CPC' in data.columns or 'Avg_CPC' in data.columns:
            cpc_col = 'CPC' if 'CPC' in data.columns else 'Avg_CPC'
            avg_cpc = data[cpc_col].mean()
            benchmark_cpc = self.benchmarks.get_benchmark('cpc')
            
            analysis['avg_cpc'] = round(avg_cpc, 2)
            analysis['cpc_vs_benchmark'] = round((avg_cpc / benchmark_cpc - 1) * 100, 1)
            
            if avg_cpc > benchmark_cpc * 1.2:
                analysis['findings'].append(f"⚠️ CPC ${avg_cpc:.2f} is {analysis['cpc_vs_benchmark']}% above benchmark")
                analysis['recommendation'] = "Review bid strategy and Quality Score to reduce CPCs"
            else:
                analysis['findings'].append(f"✅ CPC is competitive at ${avg_cpc:.2f}")
        
        # Check for impression share data
        is_cols = [col for col in data.columns if 'impression' in col.lower() and 'share' in col.lower()]
        if is_cols:
            is_col = is_cols[0]
            avg_is = data[is_col].mean()
            analysis['impression_share'] = round(avg_is * 100, 1) if avg_is <= 1 else round(avg_is, 1)
            
            if avg_is < 0.5:
                analysis['findings'].append("⚠️ Low impression share - missing significant auction opportunities")
        
        return analysis
    
    def _analyze_keywords(self, data: pd.DataFrame) -> Dict[str, Any]:
        """
        Analyze keyword performance
        
        Args:
            data: Campaign data
            
        Returns:
            Keyword performance analysis
        """
        analysis = {
            'metric': 'Keyword Performance',
            'findings': []
        }
        
        # Check for keyword column
        keyword_cols = [col for col in data.columns if 'keyword' in col.lower()]
        
        if not keyword_cols:
            analysis['status'] = 'unavailable'
            return analysis
        
        keyword_col = keyword_cols[0]
        total_keywords = data[keyword_col].nunique()
        analysis['total_keywords'] = total_keywords
        
        # Performance distribution
        if 'CTR' in data.columns:
            high_performers = data[data['CTR'] > 0.05]
            low_performers = data[data['CTR'] < 0.01]
            
            analysis['high_performers'] = len(high_performers)
            analysis['low_performers'] = len(low_performers)
            
            if len(low_performers) > total_keywords * 0.3:
                analysis['findings'].append(f"⚠️ {len(low_performers)} keywords ({len(low_performers)/total_keywords*100:.1f}%) have very low CTR")
                analysis['recommendation'] = "Pause or optimize low-performing keywords"
        
        # Spend concentration
        if 'Spend' in data.columns:
            top_10_spend = data.nlargest(10, 'Spend')['Spend'].sum()
            total_spend = data['Spend'].sum()
            concentration = (top_10_spend / total_spend * 100) if total_spend > 0 else 0
            
            analysis['top_10_spend_concentration'] = round(concentration, 1)
            
            if concentration > 80:
                analysis['findings'].append(f"⚠️ Top 10 keywords account for {concentration:.1f}% of spend - high concentration risk")
        
        return analysis
    
    def _analyze_impression_share(self, data: pd.DataFrame) -> Dict[str, Any]:
        """
        Analyze impression share and lost opportunities
        
        Args:
            data: Campaign data
            
        Returns:
            Impression share analysis
        """
        analysis = {
            'metric': 'Impression Share',
            'findings': []
        }
        
        # Look for impression share columns
        is_cols = [col for col in data.columns if 'impression' in col.lower() and 'share' in col.lower()]
        
        if not is_cols:
            analysis['status'] = 'unavailable'
            return analysis
        
        is_col = is_cols[0]
        avg_is = data[is_col].mean()
        
        # Convert to percentage if needed
        if avg_is <= 1:
            avg_is_pct = avg_is * 100
        else:
            avg_is_pct = avg_is
        
        analysis['average_impression_share'] = round(avg_is_pct, 1)
        benchmark = self.benchmarks.get_benchmark('impression_share') * 100
        analysis['benchmark'] = benchmark
        
        gap = benchmark - avg_is_pct
        
        if gap > 20:
            analysis['status'] = 'poor'
            analysis['findings'].append(f"⚠️ Missing {gap:.1f}% of available impressions")
            analysis['recommendation'] = "Increase budgets or improve ad rank to capture more impressions"
        elif gap > 10:
            analysis['status'] = 'average'
            analysis['findings'].append(f"Moderate impression share gap of {gap:.1f}%")
        else:
            analysis['status'] = 'good'
            analysis['findings'].append(f"✅ Strong impression share at {avg_is_pct:.1f}%")
        
        # Check for lost IS due to budget
        budget_cols = [col for col in data.columns if 'budget' in col.lower() and 'lost' in col.lower()]
        if budget_cols:
            lost_budget = data[budget_cols[0]].mean()
            if lost_budget > 0.2:
                analysis['findings'].append(f"⚠️ Losing {lost_budget*100:.1f}% IS due to budget constraints")
        
        return analysis
    
    def _analyze_match_types(self, data: pd.DataFrame) -> Dict[str, Any]:
        """
        Analyze keyword match type efficiency
        
        Args:
            data: Campaign data
            
        Returns:
            Match type analysis
        """
        analysis = {
            'metric': 'Match Type Efficiency',
            'findings': []
        }
        
        # Look for match type column
        match_cols = [col for col in data.columns if 'match' in col.lower() and 'type' in col.lower()]
        
        if not match_cols:
            analysis['status'] = 'unavailable'
            return analysis
        
        match_col = match_cols[0]
        match_distribution = data[match_col].value_counts(normalize=True)
        
        analysis['distribution'] = match_distribution.to_dict()
        
        # Check for broad match usage
        broad_keywords = ['broad', 'modified']
        broad_pct = sum(match_distribution.get(mt, 0) for mt in match_distribution.index 
                       if any(b in str(mt).lower() for b in broad_keywords))
        
        if broad_pct > 0.5:
            analysis['findings'].append(f"⚠️ {broad_pct*100:.1f}% of keywords use broad match - may have low relevance")
            analysis['recommendation'] = "Consider shifting to phrase or exact match for better control"
        
        # Performance by match type
        if 'CTR' in data.columns and 'Conversions' in data.columns:
            perf_by_match = data.groupby(match_col).agg({
                'CTR': 'mean',
                'Conversions': 'sum'
            })
            
            best_match_type = perf_by_match['CTR'].idxmax()
            analysis['best_performing_match_type'] = best_match_type
            analysis['findings'].append(f"✅ {best_match_type} match type shows best CTR performance")
        
        return analysis
    
    def _analyze_search_terms(self, data: pd.DataFrame) -> Dict[str, Any]:
        """
        Analyze search term performance and relevance
        
        Args:
            data: Campaign data
            
        Returns:
            Search term analysis
        """
        analysis = {
            'metric': 'Search Term Performance',
            'findings': []
        }
        
        # Look for search term column
        st_cols = [col for col in data.columns if 'search' in col.lower() and 'term' in col.lower()]
        
        if not st_cols:
            analysis['status'] = 'unavailable'
            return analysis
        
        st_col = st_cols[0]
        total_terms = data[st_col].nunique()
        analysis['total_search_terms'] = total_terms
        
        # Identify negative keyword opportunities
        if 'Conversions' in data.columns:
            zero_conv_terms = data[data['Conversions'] == 0]
            if len(zero_conv_terms) > 0:
                analysis['zero_conversion_terms'] = len(zero_conv_terms)
                analysis['findings'].append(f"⚠️ {len(zero_conv_terms)} search terms with 0 conversions - review for negative keywords")
        
        return analysis
    
    def _calculate_overall_health(self, insights: Dict[str, Any]) -> str:
        """
        Calculate overall search campaign health
        
        Args:
            insights: All analysis insights
            
        Returns:
            Health status
        """
        health_scores = []
        
        for key, value in insights.items():
            if isinstance(value, dict) and 'status' in value:
                status = value['status']
                if status == 'excellent':
                    health_scores.append(4)
                elif status == 'good':
                    health_scores.append(3)
                elif status == 'average':
                    health_scores.append(2)
                elif status == 'poor':
                    health_scores.append(1)
        
        if not health_scores:
            return 'unknown'
        
        avg_score = sum(health_scores) / len(health_scores)
        
        if avg_score >= 3.5:
            return 'excellent'
        elif avg_score >= 2.5:
            return 'good'
        elif avg_score >= 1.5:
            return 'average'
        else:
            return 'needs_improvement'

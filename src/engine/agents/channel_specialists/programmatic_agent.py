"""
Programmatic Channel Specialist Agent
Specializes in DV360, CM360 programmatic display analysis
"""

from typing import Dict, Any, List
import pandas as pd
import numpy as np
from loguru import logger

from .base_specialist import BaseChannelSpecialist


class ProgrammaticBenchmarks:
    """Industry benchmarks for programmatic advertising"""
    
    BENCHMARKS = {
        'viewability': 0.70,  # 70% viewability rate
        'ctr': 0.0046,  # 0.46% CTR for display
        'cpm': 2.80,  # $2.80 CPM
        'video_completion_rate': 0.70,  # 70% VCR
        'brand_safety_score': 0.95,  # 95% brand safe
        'invalid_traffic': 0.02,  # 2% IVT threshold
        'time_in_view': 15.0  # 15 seconds average
    }
    
    @classmethod
    def get_benchmark(cls, metric: str) -> float:
        """Get benchmark value for a metric"""
        return cls.BENCHMARKS.get(metric.lower(), 0)


class ProgrammaticAgent(BaseChannelSpecialist):
    """Specializes in DV360, CM360 programmatic display"""
    
    def __init__(self, rag_retriever=None):
        super().__init__(rag_retriever)
        self.benchmarks = ProgrammaticBenchmarks()
    
    def analyze(self, campaign_data: pd.DataFrame) -> Dict[str, Any]:
        """
        Perform programmatic-specific analysis
        
        Args:
            campaign_data: Campaign performance data
            
        Returns:
            Dictionary with programmatic-specific insights
        """
        logger.info("Starting programmatic channel analysis")
        
        platform = self.detect_platform(campaign_data)
        
        insights = {
            'platform': platform,
            'viewability_analysis': self._analyze_viewability(campaign_data),
            'brand_safety': self._check_brand_safety(campaign_data),
            'placement_performance': self._analyze_placements(campaign_data),
            'supply_path': self._analyze_supply_path(campaign_data),
            'fraud_detection': self._detect_invalid_traffic(campaign_data),
            'video_performance': self._analyze_video_metrics(campaign_data),
            'inventory_quality': self._analyze_inventory_quality(campaign_data)
        }
        
        # Programmatic-specific RAG retrieval
        context = self.retrieve_knowledge(
            query=f"programmatic {platform} viewability optimization brand safety",
            filters={'category': 'programmatic'}
        )
        
        # Generate recommendations
        insights['recommendations'] = self._generate_recommendations(insights, context)
        insights['overall_health'] = self._calculate_overall_health(insights)
        
        logger.info(f"Programmatic analysis complete. Platform: {platform}, Health: {insights['overall_health']}")
        return insights
    
    def get_benchmarks(self) -> Dict[str, float]:
        """Get programmatic-specific benchmarks"""
        return ProgrammaticBenchmarks.BENCHMARKS
    
    def _analyze_viewability(self, data: pd.DataFrame) -> Dict[str, Any]:
        """
        Analyze viewability metrics
        
        Args:
            data: Campaign data
            
        Returns:
            Viewability analysis
        """
        analysis = {
            'metric': 'Viewability',
            'status': 'unknown',
            'findings': []
        }
        
        # Look for viewability columns
        viewability_cols = [col for col in data.columns if 'viewab' in col.lower() or 'measurable' in col.lower()]
        
        if not viewability_cols:
            analysis['status'] = 'unavailable'
            analysis['findings'].append("⚠️ Viewability data not available - critical for programmatic")
            return analysis
        
        viewability_col = viewability_cols[0]
        
        # Calculate average viewability
        avg_viewability = data[viewability_col].mean()
        
        # Convert to percentage if needed
        if avg_viewability <= 1:
            avg_viewability_pct = avg_viewability * 100
        else:
            avg_viewability_pct = avg_viewability
        
        analysis['average_viewability'] = round(avg_viewability_pct, 1)
        benchmark = self.benchmarks.get_benchmark('viewability') * 100
        analysis['benchmark'] = benchmark
        
        analysis['status'] = self._calculate_metric_health(
            avg_viewability_pct, 
            benchmark, 
            higher_is_better=True
        )
        
        if avg_viewability_pct < 50:
            analysis['findings'].append(f"🚨 Critical: Very low viewability ({avg_viewability_pct:.1f}%)")
            analysis['recommendation'] = "Urgent: Review placement strategy, implement viewability targeting, audit supply sources"
        elif avg_viewability_pct < 65:
            analysis['findings'].append(f"⚠️ Below standard viewability ({avg_viewability_pct:.1f}%)")
            analysis['recommendation'] = "Optimize for viewability: use above-the-fold placements, quality exchanges"
        else:
            analysis['findings'].append(f"✅ Good viewability rate: {avg_viewability_pct:.1f}%")
        
        # Check measurable impressions
        measurable_cols = [col for col in data.columns if 'measurable' in col.lower()]
        if measurable_cols and 'Impressions' in data.columns:
            measurable_rate = (data[measurable_cols[0]].sum() / data['Impressions'].sum()) * 100
            analysis['measurable_rate'] = round(measurable_rate, 1)
            
            if measurable_rate < 80:
                analysis['findings'].append(f"⚠️ Low measurable rate ({measurable_rate:.1f}%) - limited visibility into performance")
        
        # Viewability by placement type
        placement_cols = [col for col in data.columns if 'placement' in col.lower() or 'site' in col.lower()]
        if placement_cols:
            placement_viewability = data.groupby(placement_cols[0])[viewability_col].mean().sort_values(ascending=False)
            
            if len(placement_viewability) > 0:
                best_placement = placement_viewability.index[0]
                worst_placement = placement_viewability.index[-1]
                
                analysis['best_viewability_placement'] = str(best_placement)
                analysis['worst_viewability_placement'] = str(worst_placement)
        
        return analysis
    
    def _check_brand_safety(self, data: pd.DataFrame) -> Dict[str, Any]:
        """
        Check brand safety compliance
        
        Args:
            data: Campaign data
            
        Returns:
            Brand safety analysis
        """
        analysis = {
            'metric': 'Brand Safety',
            'status': 'unknown',
            'findings': []
        }
        
        # Look for brand safety indicators
        safety_cols = [col for col in data.columns if any(term in col.lower() for term in ['brand', 'safety', 'suitable', 'verification'])]
        
        if not safety_cols:
            analysis['status'] = 'unavailable'
            analysis['findings'].append("⚠️ Brand safety verification not detected - recommend implementing")
            analysis['recommendation'] = "Implement brand safety tools (IAS, DoubleVerify, MOAT)"
            return analysis
        
        safety_col = safety_cols[0]
        
        # Analyze brand safety score/rate
        avg_safety = data[safety_col].mean()
        
        if avg_safety <= 1:
            avg_safety_pct = avg_safety * 100
        else:
            avg_safety_pct = avg_safety
        
        analysis['brand_safety_score'] = round(avg_safety_pct, 1)
        benchmark = self.benchmarks.get_benchmark('brand_safety_score') * 100
        
        if avg_safety_pct >= 95:
            analysis['status'] = 'excellent'
            analysis['findings'].append(f"✅ Excellent brand safety: {avg_safety_pct:.1f}%")
        elif avg_safety_pct >= 90:
            analysis['status'] = 'good'
            analysis['findings'].append(f"✅ Good brand safety: {avg_safety_pct:.1f}%")
        else:
            analysis['status'] = 'needs_improvement'
            analysis['findings'].append(f"⚠️ Brand safety concerns: {avg_safety_pct:.1f}%")
            analysis['recommendation'] = "Tighten brand safety controls, review blocklists, audit placements"
        
        # Check for unsafe placements
        if 'Impressions' in data.columns:
            total_impressions = data['Impressions'].sum()
            
            # Identify potentially unsafe placements (if safety score is low)
            if avg_safety <= 1:
                unsafe_threshold = 0.85
            else:
                unsafe_threshold = 85
            
            unsafe_placements = data[data[safety_col] < unsafe_threshold]
            if len(unsafe_placements) > 0:
                unsafe_impressions = unsafe_placements['Impressions'].sum()
                unsafe_pct = (unsafe_impressions / total_impressions) * 100
                
                analysis['unsafe_impression_pct'] = round(unsafe_pct, 2)
                analysis['findings'].append(f"⚠️ {unsafe_pct:.1f}% of impressions on lower safety score placements")
        
        return analysis
    
    def _analyze_placements(self, data: pd.DataFrame) -> Dict[str, Any]:
        """
        Analyze placement/site performance
        
        Args:
            data: Campaign data
            
        Returns:
            Placement analysis
        """
        analysis = {
            'metric': 'Placement Performance',
            'findings': []
        }
        
        # Look for placement columns
        placement_cols = [col for col in data.columns if any(term in col.lower() for term in ['placement', 'site', 'domain', 'publisher'])]
        
        if not placement_cols:
            analysis['status'] = 'unavailable'
            return analysis
        
        placement_col = placement_cols[0]
        total_placements = data[placement_col].nunique()
        analysis['total_placements'] = total_placements
        
        # Performance distribution
        if 'Conversions' in data.columns and 'Spend' in data.columns:
            placement_perf = data.groupby(placement_col).agg({
                'Conversions': 'sum',
                'Spend': 'sum',
                'Impressions': 'sum'
            })
            
            placement_perf['CPA'] = placement_perf['Spend'] / placement_perf['Conversions'].replace(0, np.nan)
            placement_perf['Conversion_Rate'] = (placement_perf['Conversions'] / placement_perf['Impressions']) * 100
            
            # Identify top and bottom performers
            top_placements = placement_perf.nlargest(5, 'Conversions')
            zero_conv_placements = placement_perf[placement_perf['Conversions'] == 0]
            
            analysis['top_5_placements'] = top_placements.index.tolist()
            analysis['zero_conversion_placements'] = len(zero_conv_placements)
            
            if len(zero_conv_placements) > 0:
                zero_conv_spend = zero_conv_placements['Spend'].sum()
                total_spend = placement_perf['Spend'].sum()
                waste_pct = (zero_conv_spend / total_spend) * 100
                
                analysis['wasted_spend_pct'] = round(waste_pct, 1)
                analysis['findings'].append(f"⚠️ {len(zero_conv_placements)} placements ({waste_pct:.1f}% of spend) with 0 conversions")
                analysis['recommendation'] = "Block non-converting placements and reallocate budget"
            
            # Spend concentration
            top_10_spend = placement_perf.nlargest(10, 'Spend')['Spend'].sum()
            concentration = (top_10_spend / placement_perf['Spend'].sum()) * 100
            analysis['top_10_spend_concentration'] = round(concentration, 1)
            
            if concentration < 30:
                analysis['findings'].append(f"ℹ️ Low spend concentration ({concentration:.1f}%) - budget spread thin")
                analysis['recommendation'] = "Consolidate spend on top-performing placements"
        
        return analysis
    
    def _analyze_supply_path(self, data: pd.DataFrame) -> Dict[str, Any]:
        """
        Analyze supply path optimization
        
        Args:
            data: Campaign data
            
        Returns:
            Supply path analysis
        """
        analysis = {
            'metric': 'Supply Path',
            'findings': []
        }
        
        # Look for exchange/SSP columns
        exchange_cols = [col for col in data.columns if any(term in col.lower() for term in ['exchange', 'ssp', 'supply'])]
        
        if not exchange_cols:
            analysis['status'] = 'unavailable'
            return analysis
        
        exchange_col = exchange_cols[0]
        total_exchanges = data[exchange_col].nunique()
        analysis['total_exchanges'] = total_exchanges
        
        # Performance by exchange
        if 'CPM' in data.columns or ('Spend' in data.columns and 'Impressions' in data.columns):
            if 'CPM' not in data.columns:
                data['CPM'] = (data['Spend'] / data['Impressions']) * 1000
            
            exchange_perf = data.groupby(exchange_col).agg({
                'CPM': 'mean',
                'Spend': 'sum'
            }).sort_values('CPM')
            
            # Identify cost-efficient exchanges
            efficient_exchanges = exchange_perf[exchange_perf['CPM'] < exchange_perf['CPM'].median()]
            
            analysis['cost_efficient_exchanges'] = len(efficient_exchanges)
            analysis['findings'].append(f"✅ {len(efficient_exchanges)} cost-efficient supply sources identified")
            
            # Check for high-cost exchanges
            high_cost_threshold = exchange_perf['CPM'].quantile(0.75)
            high_cost_exchanges = exchange_perf[exchange_perf['CPM'] > high_cost_threshold]
            
            if len(high_cost_exchanges) > 0:
                high_cost_spend_pct = (high_cost_exchanges['Spend'].sum() / exchange_perf['Spend'].sum()) * 100
                analysis['findings'].append(f"⚠️ {high_cost_spend_pct:.1f}% of spend on high-cost exchanges")
                analysis['recommendation'] = "Optimize supply path - shift to cost-efficient exchanges"
        
        return analysis
    
    def _detect_invalid_traffic(self, data: pd.DataFrame) -> Dict[str, Any]:
        """
        Detect invalid traffic (IVT) / fraud indicators
        
        Args:
            data: Campaign data
            
        Returns:
            IVT/fraud analysis
        """
        analysis = {
            'metric': 'Invalid Traffic',
            'status': 'unknown',
            'findings': []
        }
        
        # Look for IVT columns
        ivt_cols = [col for col in data.columns if any(term in col.lower() for term in ['invalid', 'ivt', 'fraud', 'suspicious'])]
        
        if not ivt_cols:
            analysis['status'] = 'unavailable'
            analysis['findings'].append("ℹ️ IVT detection not available - recommend implementing fraud prevention")
            return analysis
        
        ivt_col = ivt_cols[0]
        
        # Calculate IVT rate
        if 'Impressions' in data.columns:
            total_impressions = data['Impressions'].sum()
            ivt_impressions = data[ivt_col].sum()
            
            ivt_rate = (ivt_impressions / total_impressions) * 100 if total_impressions > 0 else 0
            analysis['ivt_rate'] = round(ivt_rate, 2)
            
            benchmark = self.benchmarks.get_benchmark('invalid_traffic') * 100
            analysis['benchmark'] = benchmark
            
            if ivt_rate > 5:
                analysis['status'] = 'critical'
                analysis['findings'].append(f"🚨 High IVT rate ({ivt_rate:.2f}%) - significant fraud risk")
                analysis['recommendation'] = "Urgent: Implement fraud prevention, audit supply sources, block suspicious placements"
            elif ivt_rate > 2:
                analysis['status'] = 'concerning'
                analysis['findings'].append(f"⚠️ Elevated IVT rate ({ivt_rate:.2f}%)")
                analysis['recommendation'] = "Review traffic quality and implement stricter filters"
            else:
                analysis['status'] = 'healthy'
                analysis['findings'].append(f"✅ Low IVT rate: {ivt_rate:.2f}%")
        
        return analysis
    
    def _analyze_video_metrics(self, data: pd.DataFrame) -> Dict[str, Any]:
        """
        Analyze video-specific metrics (for video campaigns)
        
        Args:
            data: Campaign data
            
        Returns:
            Video performance analysis
        """
        analysis = {
            'metric': 'Video Performance',
            'findings': []
        }
        
        # Look for video completion columns
        completion_cols = [col for col in data.columns if 'completion' in col.lower() or 'vcr' in col.lower()]
        
        if not completion_cols:
            analysis['status'] = 'not_applicable'
            return analysis
        
        completion_col = completion_cols[0]
        avg_completion = data[completion_col].mean()
        
        # Convert to percentage if needed
        if avg_completion <= 1:
            avg_completion_pct = avg_completion * 100
        else:
            avg_completion_pct = avg_completion
        
        analysis['video_completion_rate'] = round(avg_completion_pct, 1)
        benchmark = self.benchmarks.get_benchmark('video_completion_rate') * 100
        analysis['benchmark'] = benchmark
        
        if avg_completion_pct < 50:
            analysis['findings'].append(f"⚠️ Low video completion rate ({avg_completion_pct:.1f}%)")
            analysis['recommendation'] = "Review video creative length and engagement, test shorter formats"
        else:
            analysis['findings'].append(f"✅ Good video completion: {avg_completion_pct:.1f}%")
        
        # Check for quartile metrics
        quartile_cols = [col for col in data.columns if 'quartile' in col.lower()]
        if quartile_cols:
            analysis['findings'].append("✅ Quartile tracking enabled for detailed video insights")
        
        return analysis
    
    def _analyze_inventory_quality(self, data: pd.DataFrame) -> Dict[str, Any]:
        """
        Analyze overall inventory quality
        
        Args:
            data: Campaign data
            
        Returns:
            Inventory quality analysis
        """
        analysis = {
            'metric': 'Inventory Quality',
            'findings': [],
            'quality_score': 0
        }
        
        quality_factors = []
        
        # Factor 1: Viewability
        viewability_cols = [col for col in data.columns if 'viewab' in col.lower()]
        if viewability_cols:
            avg_viewability = data[viewability_cols[0]].mean()
            if avg_viewability > 0.7 or avg_viewability > 70:
                quality_factors.append(1)
            else:
                quality_factors.append(0)
        
        # Factor 2: Brand Safety
        safety_cols = [col for col in data.columns if 'safety' in col.lower() or 'suitable' in col.lower()]
        if safety_cols:
            avg_safety = data[safety_cols[0]].mean()
            if avg_safety > 0.9 or avg_safety > 90:
                quality_factors.append(1)
            else:
                quality_factors.append(0)
        
        # Factor 3: Low IVT
        ivt_cols = [col for col in data.columns if 'invalid' in col.lower() or 'ivt' in col.lower()]
        if ivt_cols and 'Impressions' in data.columns:
            ivt_rate = (data[ivt_cols[0]].sum() / data['Impressions'].sum())
            if ivt_rate < 0.02:
                quality_factors.append(1)
            else:
                quality_factors.append(0)
        
        # Calculate overall quality score
        if quality_factors:
            quality_score = (sum(quality_factors) / len(quality_factors)) * 100
            analysis['quality_score'] = round(quality_score, 1)
            
            if quality_score >= 80:
                analysis['status'] = 'premium'
                analysis['findings'].append(f"✅ Premium inventory quality ({quality_score:.0f}/100)")
            elif quality_score >= 60:
                analysis['status'] = 'good'
                analysis['findings'].append(f"✅ Good inventory quality ({quality_score:.0f}/100)")
            else:
                analysis['status'] = 'needs_improvement'
                analysis['findings'].append(f"⚠️ Inventory quality needs improvement ({quality_score:.0f}/100)")
                analysis['recommendation'] = "Implement stricter quality controls and premium inventory targeting"
        
        return analysis
    
    def _calculate_overall_health(self, insights: Dict[str, Any]) -> str:
        """Calculate overall programmatic campaign health"""
        health_scores = []
        
        # Weight critical metrics more heavily
        critical_metrics = ['viewability_analysis', 'brand_safety', 'fraud_detection']
        
        for key, value in insights.items():
            if isinstance(value, dict) and 'status' in value:
                status = value['status']
                weight = 2 if key in critical_metrics else 1
                
                if status in ['excellent', 'premium', 'healthy']:
                    health_scores.extend([4] * weight)
                elif status == 'good':
                    health_scores.extend([3] * weight)
                elif status in ['average', 'concerning']:
                    health_scores.extend([2] * weight)
                elif status in ['poor', 'critical', 'needs_improvement']:
                    health_scores.extend([1] * weight)
        
        if not health_scores:
            return 'unknown'
        
        avg_score = sum(health_scores) / len(health_scores)
        
        if avg_score >= 3.5:
            return 'excellent'
        elif avg_score >= 2.5:
            return 'good'
        elif avg_score >= 1.5:
            return 'needs_optimization'
        else:
            return 'critical_issues'

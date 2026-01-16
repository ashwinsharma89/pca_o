"""
Dynamic Benchmark Engine
Context-aware benchmark retrieval and application with industry, region, and objective adjustments
"""

from typing import Dict, Any, Optional, List
import json
from pathlib import Path
from loguru import logger


class DynamicBenchmarkEngine:
    """Context-aware benchmark retrieval and application"""
    
    def __init__(self, rag_retriever=None):
        """
        Initialize dynamic benchmark engine
        
        Args:
            rag_retriever: Optional RAG retriever for knowledge base access
        """
        self.rag = rag_retriever
        self.benchmark_db = self._load_benchmarks()
        self.regional_multipliers = self._load_regional_multipliers()
        self.objective_adjustments = self._load_objective_adjustments()
        logger.info("Initialized Dynamic Benchmark Engine")
    
    def get_contextual_benchmarks(self,
                                   channel: str,
                                   business_model: str,
                                   industry: str,
                                   objective: Optional[str] = None,
                                   region: Optional[str] = None) -> Dict[str, Any]:
        """
        Retrieve benchmarks with full context
        
        Args:
            channel: Advertising channel (google_search, linkedin, meta, etc.)
            business_model: B2B, B2C, or B2B2C
            industry: Industry vertical (SaaS, E-commerce, etc.)
            objective: Campaign objective (awareness, conversion, etc.)
            region: Geographic region (North America, Europe, etc.)
            
        Returns:
            Dictionary with contextual benchmarks and interpretation guidance
        """
        logger.info(f"Retrieving benchmarks for {business_model} {industry} on {channel}")
        
        # Start with base channel benchmarks
        base_key = f"{channel.lower()}_{business_model.lower()}"
        industry_key = industry.lower().replace(" ", "_").replace("-", "_")
        
        # Get base benchmarks
        benchmarks = self._get_base_benchmarks(base_key, industry_key)
        
        # Apply objective adjustments
        if objective:
            benchmarks = self._apply_objective_adjustments(benchmarks, objective, business_model)
        
        # Apply regional adjustments
        if region:
            regional_adjustment = self._get_regional_multiplier(channel, region)
            benchmarks = self._apply_multiplier(benchmarks, regional_adjustment)
        
        # Retrieve industry-specific context from RAG if available
        industry_context = None
        if self.rag:
            try:
                industry_context = self.rag.retrieve(
                    query=f"{industry} {channel} performance benchmarks {business_model}",
                    filters={'category': 'benchmarks', 'industry': industry}
                )
            except Exception as e:
                logger.warning(f"Could not retrieve RAG context: {e}")
        
        # Generate interpretation guidance
        interpretation = self._generate_interpretation(
            benchmarks, 
            business_model, 
            industry, 
            objective,
            region
        )
        
        return {
            'benchmarks': benchmarks,
            'context': f"{business_model} {industry} campaigns on {channel}",
            'region': region or 'Global',
            'objective': objective or 'General',
            'interpretation_guidance': interpretation,
            'industry_context': industry_context,
            'benchmark_source': self._get_benchmark_source(base_key, industry_key)
        }
    
    def compare_to_benchmarks(self,
                             actual_metrics: Dict[str, float],
                             contextual_benchmarks: Dict[str, Any]) -> Dict[str, Any]:
        """
        Compare actual metrics to contextual benchmarks
        
        Args:
            actual_metrics: Dictionary of actual metric values
            contextual_benchmarks: Output from get_contextual_benchmarks
            
        Returns:
            Comparison results with performance assessment
        """
        benchmarks = contextual_benchmarks['benchmarks']
        comparisons = {}
        
        for metric, actual_value in actual_metrics.items():
            if metric in benchmarks:
                benchmark_ranges = benchmarks[metric]
                assessment = self._assess_performance(actual_value, benchmark_ranges, metric)
                comparisons[metric] = {
                    'actual': actual_value,
                    'benchmarks': benchmark_ranges,
                    'assessment': assessment['level'],
                    'message': assessment['message'],
                    'gap': assessment['gap']
                }
        
        # Overall performance score
        overall_score = self._calculate_overall_score(comparisons)
        
        return {
            'comparisons': comparisons,
            'overall_score': overall_score,
            'overall_assessment': self._get_overall_assessment(overall_score),
            'context': contextual_benchmarks['context']
        }
    
    def _load_benchmarks(self) -> Dict[str, Any]:
        """Load comprehensive benchmark database"""
        return {
            # Google Search - B2B
            'google_search_b2b': {
                'saas': {
                    'ctr': {'excellent': 0.06, 'good': 0.04, 'average': 0.03, 'needs_work': 0.025},
                    'cpc': {'excellent': 3, 'good': 6, 'acceptable': 9, 'high': 12},
                    'conv_rate': {'excellent': 0.08, 'good': 0.05, 'average': 0.03, 'needs_work': 0.02},
                    'quality_score': {'excellent': 8, 'good': 7, 'average': 6, 'poor': 5},
                    'impression_share': {'excellent': 0.80, 'good': 0.70, 'average': 0.60, 'poor': 0.50}
                },
                'financial_services': {
                    'ctr': {'excellent': 0.05, 'good': 0.03, 'average': 0.02, 'needs_work': 0.015},
                    'cpc': {'excellent': 8, 'good': 15, 'acceptable': 20, 'high': 25},
                    'conv_rate': {'excellent': 0.06, 'good': 0.04, 'average': 0.025, 'needs_work': 0.015},
                    'quality_score': {'excellent': 8, 'good': 7, 'average': 6, 'poor': 5}
                },
                'healthcare': {
                    'ctr': {'excellent': 0.045, 'good': 0.03, 'average': 0.02, 'needs_work': 0.015},
                    'cpc': {'excellent': 5, 'good': 10, 'acceptable': 15, 'high': 20},
                    'conv_rate': {'excellent': 0.07, 'good': 0.045, 'average': 0.03, 'needs_work': 0.02}
                },
                'default': {
                    'ctr': {'excellent': 0.05, 'good': 0.03, 'average': 0.02, 'needs_work': 0.015},
                    'cpc': {'excellent': 4, 'good': 7, 'acceptable': 10, 'high': 15},
                    'conv_rate': {'excellent': 0.07, 'good': 0.05, 'average': 0.03, 'needs_work': 0.02}
                }
            },
            
            # Google Search - B2C
            'google_search_b2c': {
                'e_commerce': {
                    'ctr': {'excellent': 0.05, 'good': 0.035, 'average': 0.025, 'needs_work': 0.02},
                    'cpc': {'excellent': 1.0, 'good': 2.5, 'acceptable': 4.0, 'high': 5.0},
                    'conv_rate': {'excellent': 0.06, 'good': 0.04, 'average': 0.025, 'needs_work': 0.015},
                    'roas': {'excellent': 5.0, 'good': 3.0, 'average': 2.0, 'needs_work': 1.5}
                },
                'retail': {
                    'ctr': {'excellent': 0.045, 'good': 0.03, 'average': 0.02, 'needs_work': 0.015},
                    'cpc': {'excellent': 0.8, 'good': 1.5, 'acceptable': 2.5, 'high': 4.0},
                    'conv_rate': {'excellent': 0.05, 'good': 0.035, 'average': 0.02, 'needs_work': 0.01},
                    'roas': {'excellent': 4.5, 'good': 2.8, 'average': 1.8, 'needs_work': 1.2}
                },
                'auto': {
                    'ctr': {'excellent': 0.04, 'good': 0.025, 'average': 0.018, 'needs_work': 0.012},
                    'cpc': {'excellent': 2.0, 'good': 4.0, 'acceptable': 6.0, 'high': 8.0},
                    'conv_rate': {'excellent': 0.04, 'good': 0.025, 'average': 0.015, 'needs_work': 0.01}
                },
                'default': {
                    'ctr': {'excellent': 0.045, 'good': 0.03, 'average': 0.02, 'needs_work': 0.015},
                    'cpc': {'excellent': 1.0, 'good': 2.0, 'acceptable': 3.5, 'high': 5.0},
                    'conv_rate': {'excellent': 0.05, 'good': 0.035, 'average': 0.02, 'needs_work': 0.01}
                }
            },
            
            # LinkedIn - B2B
            'linkedin_b2b': {
                'saas': {
                    'ctr': {'excellent': 0.009, 'good': 0.006, 'average': 0.004, 'needs_work': 0.003},
                    'cpc': {'excellent': 6, 'good': 10, 'acceptable': 14, 'high': 18},
                    'conv_rate': {'excellent': 0.06, 'good': 0.04, 'average': 0.025, 'needs_work': 0.015},
                    'lead_quality_rate': {'excellent': 0.45, 'good': 0.30, 'average': 0.20, 'poor': 0.15}
                },
                'financial_services': {
                    'ctr': {'excellent': 0.008, 'good': 0.005, 'average': 0.003, 'needs_work': 0.002},
                    'cpc': {'excellent': 10, 'good': 15, 'acceptable': 20, 'high': 25},
                    'conv_rate': {'excellent': 0.05, 'good': 0.035, 'average': 0.02, 'needs_work': 0.01},
                    'lead_quality_rate': {'excellent': 0.50, 'good': 0.35, 'average': 0.25, 'poor': 0.15}
                },
                'healthcare': {
                    'ctr': {'excellent': 0.007, 'good': 0.005, 'average': 0.003, 'needs_work': 0.002},
                    'cpc': {'excellent': 8, 'good': 12, 'acceptable': 16, 'high': 20},
                    'conv_rate': {'excellent': 0.055, 'good': 0.038, 'average': 0.022, 'needs_work': 0.012}
                },
                'default': {
                    'ctr': {'excellent': 0.008, 'good': 0.005, 'average': 0.003, 'needs_work': 0.002},
                    'cpc': {'excellent': 7, 'good': 11, 'acceptable': 15, 'high': 20},
                    'conv_rate': {'excellent': 0.055, 'good': 0.038, 'average': 0.022, 'needs_work': 0.012}
                }
            },
            
            # Meta - B2C
            'meta_b2c': {
                'e_commerce': {
                    'ctr': {'excellent': 0.015, 'good': 0.01, 'average': 0.007, 'needs_work': 0.005},
                    'cpc': {'excellent': 0.5, 'good': 1.0, 'acceptable': 1.5, 'high': 2.0},
                    'conv_rate': {'excellent': 0.05, 'good': 0.03, 'average': 0.02, 'needs_work': 0.01},
                    'roas': {'excellent': 4.0, 'good': 2.5, 'average': 1.8, 'needs_work': 1.2},
                    'frequency': {'excellent': 2.5, 'good': 3.0, 'acceptable': 3.5, 'high': 4.0}
                },
                'retail': {
                    'ctr': {'excellent': 0.014, 'good': 0.009, 'average': 0.006, 'needs_work': 0.004},
                    'cpc': {'excellent': 0.6, 'good': 1.2, 'acceptable': 1.8, 'high': 2.5},
                    'conv_rate': {'excellent': 0.045, 'good': 0.028, 'average': 0.018, 'needs_work': 0.01},
                    'roas': {'excellent': 3.8, 'good': 2.3, 'average': 1.6, 'needs_work': 1.0}
                },
                'auto': {
                    'ctr': {'excellent': 0.012, 'good': 0.008, 'average': 0.005, 'needs_work': 0.003},
                    'cpc': {'excellent': 1.0, 'good': 2.0, 'acceptable': 3.0, 'high': 4.0},
                    'conv_rate': {'excellent': 0.03, 'good': 0.02, 'average': 0.012, 'needs_work': 0.008}
                },
                'default': {
                    'ctr': {'excellent': 0.013, 'good': 0.009, 'average': 0.006, 'needs_work': 0.004},
                    'cpc': {'excellent': 0.7, 'good': 1.3, 'acceptable': 2.0, 'high': 3.0},
                    'conv_rate': {'excellent': 0.04, 'good': 0.025, 'average': 0.015, 'needs_work': 0.008}
                }
            },
            
            # Meta - B2B
            'meta_b2b': {
                'saas': {
                    'ctr': {'excellent': 0.012, 'good': 0.008, 'average': 0.005, 'needs_work': 0.003},
                    'cpc': {'excellent': 3, 'good': 6, 'acceptable': 9, 'high': 12},
                    'conv_rate': {'excellent': 0.04, 'good': 0.025, 'average': 0.015, 'needs_work': 0.01}
                },
                'default': {
                    'ctr': {'excellent': 0.01, 'good': 0.007, 'average': 0.004, 'needs_work': 0.002},
                    'cpc': {'excellent': 4, 'good': 7, 'acceptable': 10, 'high': 15},
                    'conv_rate': {'excellent': 0.035, 'good': 0.022, 'average': 0.013, 'needs_work': 0.008}
                }
            },
            
            # DV360 - Programmatic
            'dv360_programmatic': {
                'default': {
                    'viewability': {'excellent': 0.75, 'good': 0.70, 'average': 0.65, 'poor': 0.60},
                    'brand_safety': {'excellent': 0.98, 'good': 0.95, 'average': 0.92, 'poor': 0.90},
                    'ctr': {'excellent': 0.015, 'good': 0.01, 'average': 0.007, 'needs_work': 0.005},
                    'cpm': {'excellent': 2.5, 'good': 3.5, 'acceptable': 5.0, 'high': 7.0},
                    'ivt_rate': {'excellent': 0.01, 'good': 0.02, 'acceptable': 0.03, 'high': 0.05}
                }
            }
        }
    
    def _load_regional_multipliers(self) -> Dict[str, Dict[str, float]]:
        """Load regional adjustment multipliers"""
        return {
            'google_search': {
                'North America': {'cpc': 1.0, 'ctr': 1.0, 'conv_rate': 1.0},
                'Europe': {'cpc': 0.85, 'ctr': 0.95, 'conv_rate': 0.92},
                'Asia Pacific': {'cpc': 0.70, 'ctr': 0.88, 'conv_rate': 0.85},
                'Latin America': {'cpc': 0.60, 'ctr': 0.82, 'conv_rate': 0.78},
                'Middle East': {'cpc': 0.75, 'ctr': 0.85, 'conv_rate': 0.80}
            },
            'linkedin': {
                'North America': {'cpc': 1.0, 'ctr': 1.0, 'conv_rate': 1.0},
                'Europe': {'cpc': 0.90, 'ctr': 0.93, 'conv_rate': 0.95},
                'Asia Pacific': {'cpc': 0.75, 'ctr': 0.85, 'conv_rate': 0.88},
                'Latin America': {'cpc': 0.65, 'ctr': 0.78, 'conv_rate': 0.75}
            },
            'meta': {
                'North America': {'cpc': 1.0, 'ctr': 1.0, 'conv_rate': 1.0},
                'Europe': {'cpc': 0.88, 'ctr': 0.92, 'conv_rate': 0.94},
                'Asia Pacific': {'cpc': 0.65, 'ctr': 0.85, 'conv_rate': 0.82},
                'Latin America': {'cpc': 0.55, 'ctr': 0.80, 'conv_rate': 0.75}
            }
        }
    
    def _load_objective_adjustments(self) -> Dict[str, Dict[str, float]]:
        """Load campaign objective adjustments"""
        return {
            'awareness': {
                'ctr': 0.7,  # Lower CTR expected for awareness
                'cpc': 0.8,  # Lower CPC acceptable
                'conv_rate': 0.5  # Much lower conversion rate acceptable
            },
            'consideration': {
                'ctr': 0.85,
                'cpc': 0.9,
                'conv_rate': 0.75
            },
            'conversion': {
                'ctr': 1.0,  # Standard expectations
                'cpc': 1.0,
                'conv_rate': 1.0
            },
            'lead_generation': {
                'ctr': 0.9,
                'cpc': 1.1,  # Higher CPC acceptable for quality leads
                'conv_rate': 1.0
            }
        }
    
    def _get_base_benchmarks(self, base_key: str, industry_key: str) -> Dict[str, Any]:
        """Get base benchmarks for channel and industry"""
        if base_key in self.benchmark_db:
            channel_benchmarks = self.benchmark_db[base_key]
            # Try specific industry first, fall back to default
            if industry_key in channel_benchmarks:
                return channel_benchmarks[industry_key].copy()
            elif 'default' in channel_benchmarks:
                return channel_benchmarks['default'].copy()
        
        # Return empty dict if no benchmarks found
        logger.warning(f"No benchmarks found for {base_key} / {industry_key}")
        return {}
    
    def _apply_objective_adjustments(self, benchmarks: Dict[str, Any],
                                    objective: str,
                                    business_model: str) -> Dict[str, Any]:
        """Apply objective-based adjustments to benchmarks"""
        objective_key = objective.lower().replace(" ", "_")
        
        if objective_key not in self.objective_adjustments:
            return benchmarks
        
        adjustments = self.objective_adjustments[objective_key]
        adjusted = benchmarks.copy()
        
        for metric, ranges in benchmarks.items():
            if metric in adjustments:
                multiplier = adjustments[metric]
                adjusted[metric] = {
                    level: value * multiplier
                    for level, value in ranges.items()
                }
        
        return adjusted
    
    def _get_regional_multiplier(self, channel: str, region: str) -> Dict[str, float]:
        """Get regional multiplier for channel"""
        channel_key = channel.lower().split('_')[0]  # Extract base channel name
        
        if channel_key in self.regional_multipliers:
            return self.regional_multipliers[channel_key].get(region, {'cpc': 1.0, 'ctr': 1.0, 'conv_rate': 1.0})
        
        return {'cpc': 1.0, 'ctr': 1.0, 'conv_rate': 1.0}
    
    def _apply_multiplier(self, benchmarks: Dict[str, Any],
                         multipliers: Dict[str, float]) -> Dict[str, Any]:
        """Apply regional multipliers to benchmarks"""
        adjusted = benchmarks.copy()
        
        for metric, ranges in benchmarks.items():
            if metric in multipliers:
                multiplier = multipliers[metric]
                adjusted[metric] = {
                    level: value * multiplier
                    for level, value in ranges.items()
                }
        
        return adjusted
    
    def _generate_interpretation(self, benchmarks: Dict[str, Any],
                                business_model: str,
                                industry: str,
                                objective: Optional[str],
                                region: Optional[str]) -> str:
        """Generate interpretation guidance for benchmarks"""
        context_parts = [f"{business_model} {industry} campaigns"]
        
        if objective:
            context_parts.append(f"with {objective} objective")
        
        if region:
            context_parts.append(f"in {region}")
        
        context = " ".join(context_parts)
        
        interpretation = f"These benchmarks are tailored for {context}. "
        
        # Add objective-specific guidance
        if objective:
            obj_lower = objective.lower()
            if 'awareness' in obj_lower:
                interpretation += "For awareness campaigns, focus on reach and impressions over conversion metrics. "
            elif 'conversion' in obj_lower:
                interpretation += "For conversion campaigns, prioritize conversion rate and ROAS over CTR. "
            elif 'lead' in obj_lower:
                interpretation += "For lead generation, focus on lead quality and cost per qualified lead. "
        
        # Add business model guidance
        if business_model == 'B2B':
            interpretation += "B2B campaigns typically have lower CTRs but higher CPCs due to targeting decision-makers. Focus on lead quality over volume."
        elif business_model == 'B2C':
            interpretation += "B2C campaigns should optimize for volume and efficiency. Monitor ROAS and customer acquisition cost closely."
        
        return interpretation
    
    def _get_benchmark_source(self, base_key: str, industry_key: str) -> str:
        """Get source information for benchmarks"""
        if base_key in self.benchmark_db and industry_key in self.benchmark_db[base_key]:
            return f"Industry-specific benchmarks for {industry_key}"
        elif base_key in self.benchmark_db and 'default' in self.benchmark_db[base_key]:
            return f"General benchmarks for {base_key}"
        else:
            return "Default benchmarks"
    
    def _assess_performance(self, actual: float,
                           benchmark_ranges: Dict[str, float],
                           metric: str) -> Dict[str, Any]:
        """Assess performance against benchmark ranges"""
        # Determine if higher or lower is better
        lower_is_better = metric in ['cpc', 'cpm', 'cpa', 'cac', 'ivt_rate', 'frequency']
        
        if lower_is_better:
            # For cost metrics, lower is better
            if 'excellent' in benchmark_ranges and actual <= benchmark_ranges['excellent']:
                return {
                    'level': 'excellent',
                    'message': f"Excellent performance - well below benchmark",
                    'gap': benchmark_ranges['excellent'] - actual
                }
            elif 'good' in benchmark_ranges and actual <= benchmark_ranges['good']:
                return {
                    'level': 'good',
                    'message': f"Good performance - below benchmark",
                    'gap': benchmark_ranges['good'] - actual
                }
            elif 'acceptable' in benchmark_ranges and actual <= benchmark_ranges['acceptable']:
                return {
                    'level': 'average',
                    'message': f"Average performance - near benchmark",
                    'gap': benchmark_ranges['acceptable'] - actual
                }
            else:
                return {
                    'level': 'needs_work',
                    'message': f"Below benchmark - needs improvement",
                    'gap': actual - benchmark_ranges.get('high', benchmark_ranges.get('acceptable', 0))
                }
        else:
            # For performance metrics, higher is better
            if 'excellent' in benchmark_ranges and actual >= benchmark_ranges['excellent']:
                return {
                    'level': 'excellent',
                    'message': f"Excellent performance - exceeds benchmark",
                    'gap': actual - benchmark_ranges['excellent']
                }
            elif 'good' in benchmark_ranges and actual >= benchmark_ranges['good']:
                return {
                    'level': 'good',
                    'message': f"Good performance - meets benchmark",
                    'gap': actual - benchmark_ranges['good']
                }
            elif 'average' in benchmark_ranges and actual >= benchmark_ranges['average']:
                return {
                    'level': 'average',
                    'message': f"Average performance - near benchmark",
                    'gap': actual - benchmark_ranges['average']
                }
            else:
                return {
                    'level': 'needs_work',
                    'message': f"Below benchmark - needs improvement",
                    'gap': benchmark_ranges.get('needs_work', benchmark_ranges.get('poor', 0)) - actual
                }
    
    def _calculate_overall_score(self, comparisons: Dict[str, Any]) -> float:
        """Calculate overall performance score (0-100)"""
        if not comparisons:
            return 0.0
        
        level_scores = {
            'excellent': 100,
            'good': 75,
            'average': 50,
            'needs_work': 25
        }
        
        scores = [level_scores.get(comp['assessment'], 0) for comp in comparisons.values()]
        return sum(scores) / len(scores) if scores else 0.0
    
    def _get_overall_assessment(self, score: float) -> str:
        """Get overall assessment based on score"""
        if score >= 90:
            return "Excellent - Significantly outperforming benchmarks"
        elif score >= 75:
            return "Good - Meeting or exceeding most benchmarks"
        elif score >= 50:
            return "Average - Performance in line with benchmarks"
        elif score >= 25:
            return "Needs Improvement - Below benchmarks in multiple areas"
        else:
            return "Critical - Significantly underperforming benchmarks"
    
    def export_benchmarks(self, output_path: str):
        """Export benchmark database to JSON file"""
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_file, 'w') as f:
            json.dump(self.benchmark_db, f, indent=2)
        
        logger.info(f"Exported benchmarks to {output_path}")
    
    def get_available_contexts(self) -> Dict[str, List[str]]:
        """Get list of available benchmark contexts"""
        contexts = {
            'channels': [],
            'business_models': ['B2B', 'B2C', 'B2B2C'],
            'industries': set(),
            'regions': list(self.regional_multipliers.get('google_search', {}).keys())
        }
        
        for key in self.benchmark_db.keys():
            channel = key.split('_')[0]
            if channel not in contexts['channels']:
                contexts['channels'].append(channel)
            
            for industry in self.benchmark_db[key].keys():
                if industry != 'default':
                    contexts['industries'].add(industry.replace('_', ' ').title())
        
        contexts['industries'] = sorted(list(contexts['industries']))
        
        return contexts

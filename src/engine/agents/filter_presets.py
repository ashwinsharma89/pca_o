"""
Filter Presets for Common Analysis Scenarios
Pre-configured filter sets for typical marketing analysis use cases
"""

from typing import Dict, List, Optional, Any
from loguru import logger
from .visualization_filters import FilterType


class FilterPresets:
    """Pre-configured filter sets for common analysis scenarios"""
    
    # Define all available presets
    PRESETS = {
        # Performance Analysis Presets
        'top_performers': {
            'name': '⭐ Top Performers',
            'description': 'Show top 20% of campaigns by ROAS',
            'category': 'Performance Analysis',
            'use_case': 'Identify and analyze best performing campaigns',
            'filters': {
                'performance_tier': {
                    'type': FilterType.PERFORMANCE_TIER,
                    'tier': 'top',
                    'metric': 'roas'
                }
            }
        },
        
        'bottom_performers': {
            'name': '⚠️ Bottom Performers',
            'description': 'Show bottom 20% of campaigns by ROAS',
            'category': 'Performance Analysis',
            'use_case': 'Identify underperforming campaigns for optimization',
            'filters': {
                'performance_tier': {
                    'type': FilterType.PERFORMANCE_TIER,
                    'tier': 'bottom',
                    'metric': 'roas'
                }
            }
        },
        
        'recent_top_performers': {
            'name': '🌟 Recent Top Performers',
            'description': 'Last 30 days, top 20% by ROAS',
            'category': 'Performance Analysis',
            'use_case': 'Focus on recent high performers',
            'filters': {
                'date_preset': {
                    'type': FilterType.DATE_PRESET,
                    'preset': 'last_30_days'
                },
                'performance_tier': {
                    'type': FilterType.PERFORMANCE_TIER,
                    'tier': 'top',
                    'metric': 'roas'
                }
            }
        },
        
        # Optimization Opportunities
        'opportunities': {
            'name': '💡 Optimization Opportunities',
            'description': 'High spend campaigns performing below benchmark',
            'category': 'Optimization',
            'use_case': 'Find campaigns with optimization potential',
            'filters': {
                'metric_thresholds': {
                    'type': FilterType.METRIC_THRESHOLD,
                    'conditions': [
                        {'metric': 'spend', 'operator': '>', 'value': 1000}
                    ]
                },
                'benchmark_relative': {
                    'type': FilterType.BENCHMARK_RELATIVE,
                    'comparison': 'below',
                    'benchmarks': {}  # Will be filled from context
                }
            }
        },
        
        'high_spend_low_roas': {
            'name': '💰 High Spend, Low ROAS',
            'description': 'Spend > $1000, ROAS < 2.0',
            'category': 'Optimization',
            'use_case': 'Identify inefficient budget allocation',
            'filters': {
                'metric_thresholds': {
                    'type': FilterType.METRIC_THRESHOLD,
                    'conditions': [
                        {'metric': 'spend', 'operator': '>', 'value': 1000},
                        {'metric': 'roas', 'operator': '<', 'value': 2.0}
                    ]
                }
            }
        },
        
        'low_ctr_high_spend': {
            'name': '📉 Low CTR, High Spend',
            'description': 'CTR < 2%, Spend > $500',
            'category': 'Optimization',
            'use_case': 'Find campaigns with poor engagement',
            'filters': {
                'metric_thresholds': {
                    'type': FilterType.METRIC_THRESHOLD,
                    'conditions': [
                        {'metric': 'ctr', 'operator': '<', 'value': 0.02},
                        {'metric': 'spend', 'operator': '>', 'value': 500}
                    ]
                }
            }
        },
        
        # Time-Based Analysis
        'recent_anomalies': {
            'name': '🔍 Recent Anomalies',
            'description': 'Last 7 days with unusual performance',
            'category': 'Time-Based Analysis',
            'use_case': 'Detect recent performance anomalies',
            'filters': {
                'date_preset': {
                    'type': FilterType.DATE_PRESET,
                    'preset': 'last_7_days'
                },
                'anomaly': {
                    'type': FilterType.ANOMALY,
                    'mode': 'anomalies_only',
                    'metric': 'roas'
                }
            }
        },
        
        'last_week': {
            'name': '📅 Last Week',
            'description': 'Performance from last 7 days',
            'category': 'Time-Based Analysis',
            'use_case': 'Recent performance analysis',
            'filters': {
                'date_preset': {
                    'type': FilterType.DATE_PRESET,
                    'preset': 'last_7_days'
                }
            }
        },
        
        'last_month': {
            'name': '📆 Last Month',
            'description': 'Performance from last 30 days',
            'category': 'Time-Based Analysis',
            'use_case': 'Monthly performance review',
            'filters': {
                'date_preset': {
                    'type': FilterType.DATE_PRESET,
                    'preset': 'last_30_days'
                }
            }
        },
        
        'last_quarter': {
            'name': '📊 Last Quarter',
            'description': 'Performance from last 90 days',
            'category': 'Time-Based Analysis',
            'use_case': 'Quarterly performance review',
            'filters': {
                'date_preset': {
                    'type': FilterType.DATE_PRESET,
                    'preset': 'last_90_days'
                }
            }
        },
        
        # Device-Based Analysis
        'mobile_performance': {
            'name': '📱 Mobile Performance',
            'description': 'Mobile device campaigns only',
            'category': 'Device Analysis',
            'use_case': 'Analyze mobile-specific performance',
            'filters': {
                'device': {
                    'type': FilterType.DEVICE,
                    'column': 'device',
                    'values': ['Mobile']
                }
            }
        },
        
        'mobile_high_ctr': {
            'name': '📱 Mobile High CTR',
            'description': 'Mobile devices with CTR > 4%',
            'category': 'Device Analysis',
            'use_case': 'Find high-performing mobile campaigns',
            'filters': {
                'device': {
                    'type': FilterType.DEVICE,
                    'column': 'device',
                    'values': ['Mobile']
                },
                'metric_thresholds': {
                    'type': FilterType.METRIC_THRESHOLD,
                    'conditions': [
                        {'metric': 'ctr', 'operator': '>', 'value': 0.04}
                    ]
                }
            }
        },
        
        'desktop_performance': {
            'name': '💻 Desktop Performance',
            'description': 'Desktop device campaigns only',
            'category': 'Device Analysis',
            'use_case': 'Analyze desktop-specific performance',
            'filters': {
                'device': {
                    'type': FilterType.DEVICE,
                    'column': 'device',
                    'values': ['Desktop']
                }
            }
        },
        
        # Funnel-Based Analysis
        'high_intent': {
            'name': '🎯 High Intent / Bottom Funnel',
            'description': 'Conversion-focused campaigns',
            'category': 'Funnel Analysis',
            'use_case': 'Analyze bottom-of-funnel performance',
            'filters': {
                'funnel_stage': {
                    'type': FilterType.FUNNEL_STAGE,
                    'column': 'funnel_stage',
                    'values': ['decision', 'conversion']
                }
            }
        },
        
        'awareness_stage': {
            'name': '👁️ Awareness Stage',
            'description': 'Top-of-funnel campaigns',
            'category': 'Funnel Analysis',
            'use_case': 'Analyze awareness campaign performance',
            'filters': {
                'funnel_stage': {
                    'type': FilterType.FUNNEL_STAGE,
                    'column': 'funnel_stage',
                    'values': ['awareness']
                }
            }
        },
        
        'consideration_stage': {
            'name': '🤔 Consideration Stage',
            'description': 'Middle-of-funnel campaigns',
            'category': 'Funnel Analysis',
            'use_case': 'Analyze consideration campaign performance',
            'filters': {
                'funnel_stage': {
                    'type': FilterType.FUNNEL_STAGE,
                    'column': 'funnel_stage',
                    'values': ['consideration']
                }
            }
        },
        
        # B2B-Specific Presets
        'b2b_qualified_leads': {
            'name': '👔 B2B Qualified Leads',
            'description': 'B2B campaigns with high lead quality',
            'category': 'B2B Analysis',
            'use_case': 'Focus on high-quality B2B leads',
            'filters': {
                'metric_thresholds': {
                    'type': FilterType.METRIC_THRESHOLD,
                    'conditions': [
                        {'metric': 'lead_quality_score', 'operator': '>=', 'value': 0.7}
                    ]
                }
            }
        },
        
        'b2b_high_value': {
            'name': '💎 B2B High Value',
            'description': 'B2B campaigns with high deal value',
            'category': 'B2B Analysis',
            'use_case': 'Focus on high-value B2B opportunities',
            'filters': {
                'metric_thresholds': {
                    'type': FilterType.METRIC_THRESHOLD,
                    'conditions': [
                        {'metric': 'avg_deal_value', 'operator': '>', 'value': 10000}
                    ]
                }
            }
        },
        
        # Channel-Specific Presets
        'paid_search_only': {
            'name': '🔍 Paid Search Only',
            'description': 'Google Ads search campaigns',
            'category': 'Channel Analysis',
            'use_case': 'Analyze paid search performance',
            'filters': {
                'channel': {
                    'type': FilterType.CHANNEL,
                    'column': 'channel',
                    'values': ['Google Ads', 'Google Search']
                }
            }
        },
        
        'social_media_only': {
            'name': '📱 Social Media Only',
            'description': 'Meta, LinkedIn, TikTok campaigns',
            'category': 'Channel Analysis',
            'use_case': 'Analyze social media performance',
            'filters': {
                'channel': {
                    'type': FilterType.CHANNEL,
                    'column': 'channel',
                    'values': ['Meta', 'LinkedIn', 'TikTok', 'Facebook', 'Instagram']
                }
            }
        },
        
        # Quality-Based Presets
        'high_quality_traffic': {
            'name': '✨ High Quality Traffic',
            'description': 'CTR > 3%, Conversion Rate > 5%',
            'category': 'Quality Analysis',
            'use_case': 'Find high-quality traffic sources',
            'filters': {
                'metric_thresholds': {
                    'type': FilterType.METRIC_THRESHOLD,
                    'conditions': [
                        {'metric': 'ctr', 'operator': '>', 'value': 0.03},
                        {'metric': 'conversion_rate', 'operator': '>', 'value': 0.05}
                    ]
                }
            }
        },
        
        'low_quality_traffic': {
            'name': '⚡ Low Quality Traffic',
            'description': 'CTR < 1.5%, Conversion Rate < 2%',
            'category': 'Quality Analysis',
            'use_case': 'Identify low-quality traffic for optimization',
            'filters': {
                'metric_thresholds': {
                    'type': FilterType.METRIC_THRESHOLD,
                    'conditions': [
                        {'metric': 'ctr', 'operator': '<', 'value': 0.015},
                        {'metric': 'conversion_rate', 'operator': '<', 'value': 0.02}
                    ]
                }
            }
        },
        
        # Budget-Based Presets
        'high_budget_campaigns': {
            'name': '💰 High Budget Campaigns',
            'description': 'Daily spend > $500',
            'category': 'Budget Analysis',
            'use_case': 'Focus on high-budget campaigns',
            'filters': {
                'metric_thresholds': {
                    'type': FilterType.METRIC_THRESHOLD,
                    'conditions': [
                        {'metric': 'spend', 'operator': '>', 'value': 500}
                    ]
                }
            }
        },
        
        'low_budget_high_roas': {
            'name': '🎯 Low Budget, High ROAS',
            'description': 'Spend < $200, ROAS > 3.0',
            'category': 'Budget Analysis',
            'use_case': 'Find efficient low-budget campaigns',
            'filters': {
                'metric_thresholds': {
                    'type': FilterType.METRIC_THRESHOLD,
                    'conditions': [
                        {'metric': 'spend', 'operator': '<', 'value': 200},
                        {'metric': 'roas', 'operator': '>', 'value': 3.0}
                    ]
                }
            }
        }
    }
    
    @staticmethod
    def get_preset(preset_name: str, 
                   data: Optional[Any] = None,
                   context: Optional[Dict] = None) -> Optional[Dict]:
        """
        Get predefined filter configuration
        
        Args:
            preset_name: Name of the preset
            data: Campaign data (optional, for dynamic adjustments)
            context: Campaign context (optional, for benchmark values)
        
        Returns:
            Preset configuration dictionary or None
        """
        
        preset = FilterPresets.PRESETS.get(preset_name)
        
        if not preset:
            logger.warning(f"Preset '{preset_name}' not found")
            return None
        
        # Clone the preset to avoid modifying the original
        preset_copy = {
            'name': preset['name'],
            'description': preset['description'],
            'category': preset['category'],
            'use_case': preset['use_case'],
            'filters': preset['filters'].copy()
        }
        
        # Apply context-specific adjustments
        if context:
            # Fill in benchmark values if available
            if 'benchmark_relative' in preset_copy['filters']:
                benchmarks = context.get('benchmarks', {})
                preset_copy['filters']['benchmark_relative']['benchmarks'] = benchmarks
        
        logger.info(f"Retrieved preset: {preset_name}")
        return preset_copy
    
    @staticmethod
    def get_presets_by_category(category: Optional[str] = None) -> Dict[str, Dict]:
        """
        Get all presets, optionally filtered by category
        
        Args:
            category: Optional category filter
        
        Returns:
            Dictionary of presets
        """
        
        if category:
            return {
                name: preset 
                for name, preset in FilterPresets.PRESETS.items()
                if preset['category'] == category
            }
        
        return FilterPresets.PRESETS
    
    @staticmethod
    def get_categories() -> List[str]:
        """Get list of all preset categories"""
        
        categories = set(preset['category'] for preset in FilterPresets.PRESETS.values())
        return sorted(list(categories))
    
    @staticmethod
    def search_presets(search_term: str) -> Dict[str, Dict]:
        """
        Search presets by name, description, or use case
        
        Args:
            search_term: Search term
        
        Returns:
            Dictionary of matching presets
        """
        
        search_term_lower = search_term.lower()
        
        matching_presets = {}
        for name, preset in FilterPresets.PRESETS.items():
            if (search_term_lower in preset['name'].lower() or
                search_term_lower in preset['description'].lower() or
                search_term_lower in preset['use_case'].lower()):
                matching_presets[name] = preset
        
        return matching_presets
    
    @staticmethod
    def get_preset_names() -> List[str]:
        """Get list of all preset names"""
        return list(FilterPresets.PRESETS.keys())
    
    @staticmethod
    def get_recommended_presets(context: Dict) -> List[str]:
        """
        Get recommended presets based on context
        
        Args:
            context: Campaign context
        
        Returns:
            List of recommended preset names
        """
        
        recommendations = []
        
        # B2B context
        if context.get('business_model') == 'B2B':
            recommendations.extend(['b2b_qualified_leads', 'b2b_high_value', 'high_intent'])
        
        # B2C context
        elif context.get('business_model') == 'B2C':
            recommendations.extend(['mobile_performance', 'social_media_only'])
        
        # Always recommend these
        recommendations.extend(['recent_top_performers', 'opportunities', 'recent_anomalies'])
        
        # If benchmarks available
        if context.get('benchmarks'):
            recommendations.insert(0, 'opportunities')
        
        # Remove duplicates while preserving order
        seen = set()
        unique_recommendations = []
        for item in recommendations:
            if item not in seen:
                seen.add(item)
                unique_recommendations.append(item)
        
        return unique_recommendations[:5]  # Return top 5

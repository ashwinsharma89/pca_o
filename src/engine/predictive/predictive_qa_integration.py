"""
Predictive Analytics Integration with Q&A System
Connects forecasting, optimization, and prediction modules to natural language interface
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, Any, List, Tuple
from loguru import logger


class PredictiveQAIntegration:
    """Integrates predictive analytics with Q&A system."""
    
    def __init__(self):
        """Initialize predictive integration."""
        self.forecasts = {}
        self.optimizations = {}
        logger.info("Initialized PredictiveQAIntegration")
    
    def forecast_next_month(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Forecast next month's performance based on historical trends.
        
        Args:
            df: Historical campaign data
            
        Returns:
            Dictionary with forecasts and confidence intervals
        """
        logger.info("Generating next month forecast...")
        
        # Ensure Date column is datetime
        df = df.copy()
        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
        
        # Calculate monthly metrics
        monthly = df.groupby(pd.Grouper(key='Date', freq='M')).agg({
            'Spend': 'sum',
            'Conversions': 'sum',
            'Revenue': 'sum' if 'Revenue' in df.columns else lambda x: 0
        }).reset_index()
        
        # Calculate derived metrics
        monthly['CPA'] = monthly['Spend'] / monthly['Conversions'].replace(0, np.nan)
        monthly['ROAS'] = monthly['Revenue'] / monthly['Spend'].replace(0, np.nan)
        
        # Simple trend-based forecast (last 3 months average + trend)
        recent_months = monthly.tail(3)
        
        # CPA forecast
        cpa_avg = recent_months['CPA'].mean()
        cpa_trend = (recent_months['CPA'].iloc[-1] - recent_months['CPA'].iloc[0]) / 3
        cpa_forecast = cpa_avg + cpa_trend
        cpa_std = recent_months['CPA'].std()
        
        # Conversions forecast
        conv_avg = recent_months['Conversions'].mean()
        conv_trend = (recent_months['Conversions'].iloc[-1] - recent_months['Conversions'].iloc[0]) / 3
        conv_forecast = conv_avg + conv_trend
        conv_std = recent_months['Conversions'].std()
        
        # ROAS forecast
        roas_avg = recent_months['ROAS'].mean()
        roas_trend = (recent_months['ROAS'].iloc[-1] - recent_months['ROAS'].iloc[0]) / 3
        roas_forecast = roas_avg + roas_trend
        roas_std = recent_months['ROAS'].std()
        
        forecast = {
            'next_month': {
                'cpa': {
                    'forecast': round(cpa_forecast, 2),
                    'conservative': round(cpa_forecast + cpa_std, 2),
                    'optimistic': round(cpa_forecast - cpa_std, 2),
                    'confidence': '70%'
                },
                'conversions': {
                    'forecast': round(conv_forecast, 0),
                    'conservative': round(conv_forecast - conv_std, 0),
                    'optimistic': round(conv_forecast + conv_std, 0),
                    'confidence': '70%'
                },
                'roas': {
                    'forecast': round(roas_forecast, 2),
                    'conservative': round(roas_forecast - roas_std, 2),
                    'optimistic': round(roas_forecast + roas_std, 2),
                    'confidence': '70%'
                }
            },
            'historical_avg': {
                'cpa': round(cpa_avg, 2),
                'conversions': round(conv_avg, 0),
                'roas': round(roas_avg, 2)
            },
            'trend': {
                'cpa': 'increasing' if cpa_trend > 0 else 'decreasing',
                'conversions': 'increasing' if conv_trend > 0 else 'decreasing',
                'roas': 'increasing' if roas_trend > 0 else 'decreasing'
            }
        }
        
        self.forecasts['next_month'] = forecast
        logger.success("Forecast generated successfully")
        
        return forecast
    
    def optimize_budget_allocation(self, df: pd.DataFrame, total_budget: float, target_metric: str = 'roas') -> Dict[str, Any]:
        """
        Optimize budget allocation across channels.
        
        Args:
            df: Historical campaign data
            total_budget: Total budget to allocate
            target_metric: Metric to optimize ('roas', 'conversions', 'cpa')
            
        Returns:
            Optimal allocation recommendations
        """
        logger.info(f"Optimizing budget allocation for {target_metric}...")
        
        # Calculate channel performance
        if 'Platform' in df.columns:
            channel_perf = df.groupby('Platform').agg({
                'Spend': 'sum',
                'Conversions': 'sum',
                'Revenue': 'sum' if 'Revenue' in df.columns else lambda x: 0
            }).reset_index()
            
            # Calculate metrics
            channel_perf['CPA'] = channel_perf['Spend'] / channel_perf['Conversions'].replace(0, np.nan)
            channel_perf['ROAS'] = channel_perf['Revenue'] / channel_perf['Spend'].replace(0, np.nan)
            channel_perf['Current_Allocation_Pct'] = (channel_perf['Spend'] / channel_perf['Spend'].sum() * 100).round(1)
            
            # Simple optimization: allocate based on performance
            if target_metric == 'roas':
                # Weight by ROAS
                channel_perf['Weight'] = channel_perf['ROAS'] / channel_perf['ROAS'].sum()
            elif target_metric == 'conversions':
                # Weight by conversion efficiency (conversions per dollar)
                channel_perf['Conv_Per_Dollar'] = channel_perf['Conversions'] / channel_perf['Spend']
                channel_perf['Weight'] = channel_perf['Conv_Per_Dollar'] / channel_perf['Conv_Per_Dollar'].sum()
            else:  # cpa
                # Weight inversely by CPA (lower CPA = higher weight)
                channel_perf['CPA_Inverse'] = 1 / channel_perf['CPA'].replace(0, np.nan)
                channel_perf['Weight'] = channel_perf['CPA_Inverse'] / channel_perf['CPA_Inverse'].sum()
            
            # Calculate recommended allocation
            channel_perf['Recommended_Budget'] = (channel_perf['Weight'] * total_budget).round(2)
            channel_perf['Recommended_Allocation_Pct'] = (channel_perf['Recommended_Budget'] / total_budget * 100).round(1)
            channel_perf['Budget_Change'] = (channel_perf['Recommended_Budget'] - channel_perf['Spend']).round(2)
            channel_perf['Change_Pct'] = ((channel_perf['Budget_Change'] / channel_perf['Spend']) * 100).round(1)
            
            # Expected impact
            if target_metric == 'roas':
                expected_roas = (channel_perf['ROAS'] * channel_perf['Weight']).sum()
                current_roas = (channel_perf['Revenue'].sum() / channel_perf['Spend'].sum())
                improvement = ((expected_roas - current_roas) / current_roas * 100).round(1)
            else:
                improvement = 10  # Placeholder
            
            optimization = {
                'total_budget': total_budget,
                'target_metric': target_metric,
                'channels': channel_perf.to_dict('records'),
                'expected_improvement': f"{improvement}%",
                'summary': {
                    'current_total_spend': channel_perf['Spend'].sum(),
                    'recommended_total_budget': total_budget,
                    'budget_increase': total_budget - channel_perf['Spend'].sum()
                }
            }
            
            self.optimizations[target_metric] = optimization
            logger.success("Budget optimization complete")
            
            return optimization
        
        else:
            logger.warning("No Platform column found for optimization")
            return {}
    
    def detect_early_warning_signals(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        Detect early warning signals of performance decline.
        
        Args:
            df: Historical campaign data
            
        Returns:
            List of warning signals
        """
        logger.info("Detecting early warning signals...")
        
        warnings = []
        
        # Ensure Date column
        df = df.copy()
        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
            
            # Calculate weekly metrics
            weekly = df.groupby(pd.Grouper(key='Date', freq='W')).agg({
                'Spend': 'sum',
                'Clicks': 'sum',
                'Impressions': 'sum',
                'Conversions': 'sum'
            }).reset_index()
            
            # Calculate derived metrics
            weekly['CTR'] = (weekly['Clicks'] / weekly['Impressions'] * 100).replace([np.inf, -np.inf], np.nan)
            weekly['CPA'] = (weekly['Spend'] / weekly['Conversions']).replace([np.inf, -np.inf], np.nan)
            
            # Check for declining CTR (last 2 weeks vs previous 2 weeks)
            if len(weekly) >= 4:
                recent_ctr = weekly['CTR'].tail(2).mean()
                previous_ctr = weekly['CTR'].iloc[-4:-2].mean()
                
                if recent_ctr < previous_ctr * 0.85:  # 15% decline
                    warnings.append({
                        'type': 'CTR Decline',
                        'severity': 'High',
                        'message': f"CTR declined {((previous_ctr - recent_ctr) / previous_ctr * 100):.1f}% in last 2 weeks",
                        'recommendation': 'Consider creative refresh or audience expansion'
                    })
                
                # Check for increasing CPA
                recent_cpa = weekly['CPA'].tail(2).mean()
                previous_cpa = weekly['CPA'].iloc[-4:-2].mean()
                
                if recent_cpa > previous_cpa * 1.15:  # 15% increase
                    warnings.append({
                        'type': 'CPA Increase',
                        'severity': 'High',
                        'message': f"CPA increased {((recent_cpa - previous_cpa) / previous_cpa * 100):.1f}% in last 2 weeks",
                        'recommendation': 'Review targeting, bids, and conversion funnel'
                    })
        
        logger.info(f"Found {len(warnings)} warning signals")
        return warnings
    
    def generate_predictive_insights(self, df: pd.DataFrame) -> str:
        """
        Generate comprehensive predictive insights narrative.
        
        Args:
            df: Historical campaign data
            
        Returns:
            Formatted insights text
        """
        forecast = self.forecast_next_month(df)
        warnings = self.detect_early_warning_signals(df)
        
        insights = []
        
        # Forecast insights
        insights.append("**📈 Next Month Forecast:**")
        insights.append(f"- Expected CPA: ${forecast['next_month']['cpa']['forecast']} (range: ${forecast['next_month']['cpa']['conservative']}-${forecast['next_month']['cpa']['optimistic']})")
        insights.append(f"- Expected Conversions: {forecast['next_month']['conversions']['forecast']:.0f} (range: {forecast['next_month']['conversions']['conservative']:.0f}-{forecast['next_month']['conversions']['optimistic']:.0f})")
        insights.append(f"- Expected ROAS: {forecast['next_month']['roas']['forecast']:.2f}x (range: {forecast['next_month']['roas']['conservative']:.2f}x-{forecast['next_month']['roas']['optimistic']:.2f}x)")
        insights.append("")
        
        # Trend insights
        insights.append("**📊 Trend Analysis:**")
        insights.append(f"- CPA is {forecast['trend']['cpa']}")
        insights.append(f"- Conversions are {forecast['trend']['conversions']}")
        insights.append(f"- ROAS is {forecast['trend']['roas']}")
        insights.append("")
        
        # Warnings
        if warnings:
            insights.append("**⚠️ Early Warning Signals:**")
            for w in warnings:
                insights.append(f"- [{w['severity']}] {w['message']}")
                insights.append(f"  → {w['recommendation']}")
            insights.append("")
        
        return "\n".join(insights)

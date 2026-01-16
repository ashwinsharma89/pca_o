"""
Budget Allocation Optimizer
Recommends optimal channel mix for maximum ROI
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple
from scipy.optimize import minimize, LinearConstraint
from loguru import logger


class BudgetAllocationOptimizer:
    """
    Optimize budget allocation across channels for maximum ROAS
    """
    
    def __init__(self, historical_data: pd.DataFrame):
        """
        Initialize with historical campaign performance data
        
        Args:
            historical_data: DataFrame with columns:
                - channel, budget, roas, conversions, cpa
        """
        self.historical_data = historical_data
        self.channel_performance = self._analyze_channel_performance()
        self.saturation_curves = self._calculate_saturation_curves()
        
    def _analyze_channel_performance(self) -> Dict:
        """
        Analyze historical performance by channel
        """
        logger.info("Analyzing historical channel performance")
        
        performance = {}
        
        for channel in self.historical_data['channel'].unique():
            channel_data = self.historical_data[
                self.historical_data['channel'] == channel
            ]
            
            performance[channel] = {
                'avg_roas': channel_data['roas'].mean(),
                'std_roas': channel_data['roas'].std(),
                'avg_cpa': channel_data['cpa'].mean(),
                'avg_conv_rate': channel_data['conversion_rate'].mean(),
                'total_spend': channel_data['budget'].sum(),
                'total_conversions': channel_data['conversions'].sum(),
                'campaign_count': len(channel_data)
            }
        
        return performance
    
    def _calculate_saturation_curves(self) -> Dict:
        """
        Calculate diminishing returns curves for each channel
        """
        logger.info("Calculating saturation curves")
        
        saturation = {}
        
        for channel in self.historical_data['channel'].unique():
            channel_data = self.historical_data[
                self.historical_data['channel'] == channel
            ].sort_values('budget')
            
            # Fit logarithmic saturation curve
            # ROAS = a * log(budget + 1) + b
            if len(channel_data) >= 3:
                budgets = channel_data['budget'].values
                roas_values = channel_data['roas'].values
                
                # Simple saturation model
                # Find point where ROAS drops by 20%
                peak_roas = roas_values.max()
                saturation_threshold = peak_roas * 0.8
                
                saturation_point = budgets[
                    roas_values <= saturation_threshold
                ].min() if any(roas_values <= saturation_threshold) else budgets.max()
                
                saturation[channel] = {
                    'saturation_point': saturation_point,
                    'peak_roas': peak_roas,
                    'optimal_range': (budgets.min(), saturation_point)
                }
            else:
                # Default values if insufficient data
                saturation[channel] = {
                    'saturation_point': 500000,
                    'peak_roas': self.channel_performance[channel]['avg_roas'],
                    'optimal_range': (50000, 500000)
                }
        
        return saturation
    
    def optimize_allocation(
        self,
        total_budget: float,
        campaign_goal: str = 'roas',
        constraints: Dict = None
    ) -> Dict:
        """
        Optimize budget allocation across channels
        
        Args:
            total_budget: Total budget to allocate
            campaign_goal: 'roas', 'conversions', or 'awareness'
            constraints: Dict with min/max spend per channel
            
        Returns:
            Dict with recommended allocation
        """
        logger.info(f"Optimizing ${total_budget:,.0f} budget for {campaign_goal} goal")
        
        channels = list(self.channel_performance.keys())
        n_channels = len(channels)
        
        # Default constraints
        if constraints is None:
            constraints = {
                'min_spend_per_channel': 50000,
                'max_spend_per_channel': total_budget * 0.5,
                'min_channels': 2,
                'max_channels': n_channels
            }
        
        # Ensure all required constraints exist
        if 'max_spend_per_channel' not in constraints:
            constraints['max_spend_per_channel'] = total_budget * 0.5
        if 'min_channels' not in constraints:
            constraints['min_channels'] = 2
        if 'max_channels' not in constraints:
            constraints['max_channels'] = n_channels
        
        # Objective function (negative because we minimize)
        def objective(allocation):
            total_value = 0
            for i, channel in enumerate(channels):
                budget = allocation[i]
                
                # Get expected ROAS with saturation effect
                expected_roas = self._calculate_expected_roas(
                    channel, budget
                )
                
                if campaign_goal == 'roas':
                    total_value += budget * expected_roas
                elif campaign_goal == 'conversions':
                    expected_conversions = budget / self.channel_performance[channel]['avg_cpa']
                    total_value += expected_conversions
                elif campaign_goal == 'awareness':
                    # Prioritize reach-efficient channels
                    cpm = self.channel_performance[channel].get('avg_cpm', 10)
                    impressions = (budget / cpm) * 1000
                    total_value += impressions
            
            return -total_value  # Negative for minimization
        
        # Constraints
        constraints_list = []
        
        # Budget constraint: sum = total_budget
        constraints_list.append({
            'type': 'eq',
            'fun': lambda x: np.sum(x) - total_budget
        })
        
        # Bounds for each channel
        bounds = []
        for channel in channels:
            min_spend = constraints['min_spend_per_channel']
            max_spend = min(
                constraints['max_spend_per_channel'],
                self.saturation_curves[channel]['saturation_point']
            )
            bounds.append((min_spend, max_spend))
        
        # Initial guess (equal allocation)
        x0 = np.array([total_budget / n_channels] * n_channels)
        
        # Optimize
        result = minimize(
            objective,
            x0,
            method='SLSQP',
            bounds=bounds,
            constraints=constraints_list
        )
        
        if not result.success:
            logger.warning(f"Optimization did not converge: {result.message}")
        
        # Build recommendation
        allocation = {}
        total_expected_value = 0
        
        for i, channel in enumerate(channels):
            budget = result.x[i]
            expected_roas = self._calculate_expected_roas(channel, budget)
            expected_revenue = budget * expected_roas
            expected_conversions = budget / self.channel_performance[channel]['avg_cpa']
            
            allocation[channel] = {
                'recommended_budget': round(budget, 2),
                'percentage_of_total': round(budget / total_budget * 100, 1),
                'expected_roas': round(expected_roas, 2),
                'expected_revenue': round(expected_revenue, 2),
                'expected_conversions': int(expected_conversions),
                'confidence_interval': self._calculate_confidence_interval(
                    channel, expected_roas
                ),
                'saturation_risk': self._assess_saturation_risk(channel, budget)
            }
            
            total_expected_value += expected_revenue
        
        # Overall metrics
        overall_roas = total_expected_value / total_budget
        
        return {
            'total_budget': total_budget,
            'campaign_goal': campaign_goal,
            'allocation': allocation,
            'overall_metrics': {
                'expected_total_revenue': round(total_expected_value, 2),
                'expected_overall_roas': round(overall_roas, 2),
                'expected_total_conversions': sum(
                    a['expected_conversions'] for a in allocation.values()
                ),
                'optimization_status': 'success' if result.success else 'partial'
            },
            'recommendations': self._generate_allocation_recommendations(allocation)
        }
    
    def _calculate_expected_roas(self, channel: str, budget: float) -> float:
        """
        Calculate expected ROAS with saturation effect
        """
        base_roas = self.channel_performance[channel]['avg_roas']
        saturation_point = self.saturation_curves[channel]['saturation_point']
        
        # Apply saturation curve
        if budget <= saturation_point:
            # Linear up to saturation point
            saturation_factor = 1.0
        else:
            # Logarithmic decay after saturation
            excess = budget - saturation_point
            saturation_factor = 1.0 - (0.3 * np.log1p(excess / saturation_point))
            saturation_factor = max(0.5, saturation_factor)  # Floor at 50% efficiency
        
        return base_roas * saturation_factor
    
    def _calculate_confidence_interval(
        self,
        channel: str,
        expected_roas: float
    ) -> Tuple[float, float]:
        """
        Calculate 95% confidence interval for ROAS prediction
        """
        std_roas = self.channel_performance[channel]['std_roas']
        
        # 95% CI = mean ± 1.96 * std
        lower = expected_roas - (1.96 * std_roas)
        upper = expected_roas + (1.96 * std_roas)
        
        return (round(lower, 2), round(upper, 2))
    
    def _assess_saturation_risk(self, channel: str, budget: float) -> str:
        """
        Assess risk of hitting saturation point
        """
        saturation_point = self.saturation_curves[channel]['saturation_point']
        
        if budget < saturation_point * 0.7:
            return 'low'
        elif budget < saturation_point:
            return 'medium'
        else:
            return 'high'
    
    def _generate_allocation_recommendations(
        self,
        allocation: Dict
    ) -> List[Dict]:
        """
        Generate recommendations based on allocation
        """
        recommendations = []
        
        # Check for over-concentration
        max_allocation = max(a['percentage_of_total'] for a in allocation.values())
        if max_allocation > 50:
            recommendations.append({
                'type': 'diversification',
                'priority': 'medium',
                'message': f"Over {max_allocation}% allocated to single channel - consider diversification",
                'impact': 'Reduce risk of channel-specific issues'
            })
        
        # Check for saturation risks
        for channel, alloc in allocation.items():
            if alloc['saturation_risk'] == 'high':
                recommendations.append({
                    'type': 'saturation_warning',
                    'priority': 'high',
                    'message': f"{channel} budget exceeds saturation point",
                    'impact': 'Diminishing returns expected - consider reallocating'
                })
        
        # Identify high-performing channels
        sorted_channels = sorted(
            allocation.items(),
            key=lambda x: x[1]['expected_roas'],
            reverse=True
        )
        
        best_channel = sorted_channels[0]
        if best_channel[1]['percentage_of_total'] < 30:
            recommendations.append({
                'type': 'opportunity',
                'priority': 'medium',
                'message': f"Consider increasing {best_channel[0]} allocation (highest expected ROAS: {best_channel[1]['expected_roas']})",
                'impact': 'Potential for higher overall ROAS'
            })
        
        return recommendations
    
    def simulate_scenarios(
        self,
        total_budget: float,
        scenarios: List[Dict]
    ) -> Dict:
        """
        Simulate multiple budget allocation scenarios
        
        Args:
            total_budget: Total budget
            scenarios: List of allocation scenarios to test
            
        Returns:
            Dict comparing scenarios
        """
        logger.info(f"Simulating {len(scenarios)} budget allocation scenarios")
        
        results = {}
        
        for i, scenario in enumerate(scenarios):
            scenario_name = scenario.get('name', f'Scenario_{i+1}')
            allocation = scenario['allocation']
            
            # Calculate expected outcomes
            total_revenue = 0
            total_conversions = 0
            
            for channel, budget in allocation.items():
                expected_roas = self._calculate_expected_roas(channel, budget)
                revenue = budget * expected_roas
                conversions = budget / self.channel_performance[channel]['avg_cpa']
                
                total_revenue += revenue
                total_conversions += conversions
            
            results[scenario_name] = {
                'allocation': allocation,
                'expected_revenue': round(total_revenue, 2),
                'expected_roas': round(total_revenue / total_budget, 2),
                'expected_conversions': int(total_conversions),
                'risk_level': self._assess_scenario_risk(allocation)
            }
        
        # Rank scenarios
        ranked = sorted(
            results.items(),
            key=lambda x: x[1]['expected_roas'],
            reverse=True
        )
        
        return {
            'scenarios': results,
            'ranked': [r[0] for r in ranked],
            'best_scenario': ranked[0][0],
            'comparison': self._compare_scenarios(results)
        }
    
    def _assess_scenario_risk(self, allocation: Dict) -> str:
        """
        Assess overall risk of allocation scenario
        """
        # Check concentration
        total = sum(allocation.values())
        max_pct = max(allocation.values()) / total
        
        # Check saturation
        saturation_risks = []
        for channel, budget in allocation.items():
            risk = self._assess_saturation_risk(channel, budget)
            saturation_risks.append(risk)
        
        if max_pct > 0.6 or 'high' in saturation_risks:
            return 'high'
        elif max_pct > 0.4 or 'medium' in saturation_risks:
            return 'medium'
        else:
            return 'low'
    
    def _compare_scenarios(self, scenarios: Dict) -> Dict:
        """
        Compare scenarios side-by-side
        """
        comparison = {
            'best_roas': max(s['expected_roas'] for s in scenarios.values()),
            'worst_roas': min(s['expected_roas'] for s in scenarios.values()),
            'roas_range': max(s['expected_roas'] for s in scenarios.values()) - 
                         min(s['expected_roas'] for s in scenarios.values()),
            'lowest_risk': min(scenarios.items(), key=lambda x: x[1]['risk_level'])[0]
        }
        
        return comparison


if __name__ == "__main__":
    # Example usage
    
    # Create sample historical data
    historical_data = pd.DataFrame({
        'channel': ['Meta', 'Google', 'LinkedIn', 'Display'] * 20,
        'budget': np.random.uniform(50000, 500000, 80),
        'roas': np.random.uniform(2.0, 6.0, 80),
        'cpa': np.random.uniform(30, 100, 80),
        'conversion_rate': np.random.uniform(1.0, 5.0, 80),
        'conversions': np.random.randint(100, 1000, 80)
    })
    
    # Initialize optimizer
    optimizer = BudgetAllocationOptimizer(historical_data)
    
    # Optimize allocation
    result = optimizer.optimize_allocation(
        total_budget=1000000,
        campaign_goal='roas'
    )
    
    print("\n=== Budget Allocation Optimization ===")
    print(f"Total Budget: ${result['total_budget']:,.0f}")
    print(f"Expected Overall ROAS: {result['overall_metrics']['expected_overall_roas']}")
    print(f"\nRecommended Allocation:")
    for channel, alloc in result['allocation'].items():
        print(f"\n{channel}:")
        print(f"  Budget: ${alloc['recommended_budget']:,.0f} ({alloc['percentage_of_total']}%)")
        print(f"  Expected ROAS: {alloc['expected_roas']}")
        print(f"  Expected Revenue: ${alloc['expected_revenue']:,.0f}")
        print(f"  Saturation Risk: {alloc['saturation_risk']}")

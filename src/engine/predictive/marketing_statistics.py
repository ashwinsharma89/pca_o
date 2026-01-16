"""
Marketing Statistics Module for Digital Marketing Analytics
Provides correlation analysis, A/B testing, and statistical significance tests
"""
import numpy as np
import pandas as pd
from typing import Dict, List, Tuple, Optional, Any, Union
from dataclasses import dataclass, field
from datetime import datetime
from scipy import stats
from scipy.stats import pearsonr, spearmanr, kendalltau, ttest_ind, chi2_contingency, mannwhitneyu
import logging

logger = logging.getLogger(__name__)


@dataclass
class CorrelationResult:
    """Result of a correlation analysis"""
    metric_1: str
    metric_2: str
    pearson_r: float
    pearson_p: float
    spearman_r: float
    spearman_p: float
    strength: str  # 'weak', 'moderate', 'strong'
    direction: str  # 'positive', 'negative', 'none'
    significant: bool
    sample_size: int


@dataclass
class ABTestResult:
    """Result of A/B test statistical analysis"""
    control_name: str
    treatment_name: str
    metric_name: str
    control_mean: float
    treatment_mean: float
    lift: float  # Percentage improvement
    p_value: float
    confidence_level: float
    significant: bool
    power: float
    sample_sizes: Tuple[int, int]
    test_type: str  # 't-test', 'chi-square', 'mann-whitney'
    recommendation: str


class MarketingStatistics:
    """
    Statistical analysis toolkit for digital marketing data
    
    Features:
    - Correlation analysis (Spend-ROAS, CTR-Conversions, etc.)
    - A/B testing with significance tests
    - Confidence intervals for KPIs
    - Trend detection
    """
    
    # Common marketing metric pairs for correlation
    METRIC_PAIRS = [
        ('spend', 'conversions'),
        ('spend', 'roas'),
        ('impressions', 'clicks'),
        ('clicks', 'conversions'),
        ('ctr', 'conversion_rate'),
        ('cpc', 'cpa'),
        ('frequency', 'ctr'),
        ('ad_position', 'ctr')
    ]
    
    # Column name mappings (handle different naming conventions)
    COLUMN_MAPPINGS = {
        'spend': ['Spend', 'spend', 'Total Spent', 'Total_Spent', 'Cost'],
        'conversions': ['Conversions', 'conversions', 'Site Visit', 'Purchase'],
        'roas': ['ROAS', 'roas', 'Return_on_Ad_Spend'],
        'impressions': ['Impressions', 'impressions', 'Views'],
        'clicks': ['Clicks', 'clicks', 'Link_Clicks'],
        'ctr': ['CTR', 'ctr', 'Click_Through_Rate'],
        'cpc': ['CPC', 'cpc', 'Cost_Per_Click'],
        'cpa': ['CPA', 'cpa', 'Cost_Per_Acquisition'],
        'conversion_rate': ['Conversion_Rate', 'Conv_Rate', 'CVR']
    }
    
    def __init__(self, df: pd.DataFrame):
        """
        Initialize with a DataFrame of marketing data
        
        Args:
            df: DataFrame with marketing campaign data
        """
        self.df = df
        self._column_map = self._build_column_map()
    
    def _build_column_map(self) -> Dict[str, str]:
        """Map standard metric names to actual column names in DataFrame"""
        col_map = {}
        for metric, possible_names in self.COLUMN_MAPPINGS.items():
            for name in possible_names:
                if name in self.df.columns:
                    col_map[metric] = name
                    break
        return col_map
    
    def _get_column(self, metric: str) -> Optional[str]:
        """Get actual column name for a metric"""
        return self._column_map.get(metric)
    
    def compute_correlation_matrix(
        self,
        metrics: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Compute correlation matrix for marketing metrics
        
        Args:
            metrics: List of metrics to analyze (uses defaults if None)
            
        Returns:
            DataFrame with correlation coefficients
        """
        if metrics is None:
            # Use all available mapped metrics
            metrics = [m for m in self.COLUMN_MAPPINGS.keys() if m in self._column_map]
        
        # Get actual column names
        columns = [self._column_map[m] for m in metrics if m in self._column_map]
        
        if len(columns) < 2:
            logger.warning("Not enough columns for correlation analysis")
            return pd.DataFrame()
        
        # Filter to numeric columns only
        numeric_df = self.df[columns].select_dtypes(include=[np.number])
        
        return numeric_df.corr()
    
    def analyze_correlation(
        self,
        metric_1: str,
        metric_2: str,
        significance_level: float = 0.05
    ) -> Optional[CorrelationResult]:
        """
        Analyze correlation between two marketing metrics
        
        Args:
            metric_1: First metric name
            metric_2: Second metric name
            significance_level: P-value threshold for significance
            
        Returns:
            CorrelationResult with detailed analysis
        """
        col1 = self._get_column(metric_1)
        col2 = self._get_column(metric_2)
        
        if not col1 or not col2:
            logger.warning(f"Columns not found: {metric_1}={col1}, {metric_2}={col2}")
            return None
        
        # Get clean data
        data = self.df[[col1, col2]].dropna()
        if len(data) < 10:
            logger.warning(f"Insufficient data for correlation: {len(data)} rows")
            return None
        
        x, y = data[col1].values, data[col2].values
        
        # Pearson correlation (linear)
        pearson_r, pearson_p = pearsonr(x, y)
        
        # Spearman correlation (monotonic)
        spearman_r, spearman_p = spearmanr(x, y)
        
        # Determine strength
        abs_r = abs(pearson_r)
        if abs_r >= 0.7:
            strength = 'strong'
        elif abs_r >= 0.4:
            strength = 'moderate'
        else:
            strength = 'weak'
        
        # Determine direction
        if abs_r < 0.1:
            direction = 'none'
        elif pearson_r > 0:
            direction = 'positive'
        else:
            direction = 'negative'
        
        return CorrelationResult(
            metric_1=metric_1,
            metric_2=metric_2,
            pearson_r=float(pearson_r),
            pearson_p=float(pearson_p),
            spearman_r=float(spearman_r),
            spearman_p=float(spearman_p),
            strength=strength,
            direction=direction,
            significant=pearson_p < significance_level,
            sample_size=len(data)
        )
    
    def analyze_all_correlations(
        self,
        significance_level: float = 0.05
    ) -> List[CorrelationResult]:
        """
        Analyze all common marketing metric correlations
        
        Returns:
            List of CorrelationResult for each valid metric pair
        """
        results = []
        
        for metric_1, metric_2 in self.METRIC_PAIRS:
            result = self.analyze_correlation(metric_1, metric_2, significance_level)
            if result:
                results.append(result)
        
        # Sort by absolute correlation strength
        results.sort(key=lambda x: abs(x.pearson_r), reverse=True)
        
        logger.info(f"Analyzed {len(results)} metric correlations")
        return results
    
    def run_ab_test(
        self,
        control: pd.Series,
        treatment: pd.Series,
        metric_name: str,
        confidence_level: float = 0.95,
        test_type: str = 'auto'
    ) -> ABTestResult:
        """
        Run statistical A/B test on marketing data
        
        Args:
            control: Control group metric values
            treatment: Treatment group metric values
            metric_name: Name of the metric being tested
            confidence_level: Desired confidence level (e.g., 0.95)
            test_type: 'ttest', 'mannwhitney', 'chi-square', or 'auto'
            
        Returns:
            ABTestResult with statistical analysis
        """
        control = control.dropna()
        treatment = treatment.dropna()
        
        control_mean = float(control.mean())
        treatment_mean = float(treatment.mean())
        lift = ((treatment_mean - control_mean) / control_mean * 100) if control_mean != 0 else 0
        
        # Auto-select test type
        if test_type == 'auto':
            # Use Mann-Whitney for non-normal distributions
            _, p_normal_control = stats.normaltest(control) if len(control) >= 8 else (0, 0)
            _, p_normal_treatment = stats.normaltest(treatment) if len(treatment) >= 8 else (0, 0)
            
            if p_normal_control < 0.05 or p_normal_treatment < 0.05:
                test_type = 'mannwhitney'
            else:
                test_type = 'ttest'
        
        # Run the appropriate test
        if test_type == 'ttest':
            statistic, p_value = ttest_ind(control, treatment)
        elif test_type == 'mannwhitney':
            statistic, p_value = mannwhitneyu(control, treatment, alternative='two-sided')
        else:
            statistic, p_value = ttest_ind(control, treatment)
        
        significant = p_value < (1 - confidence_level)
        
        # Calculate power (simplified)
        pooled_std = np.sqrt((control.var() + treatment.var()) / 2)
        effect_size = abs(treatment_mean - control_mean) / (pooled_std + 1e-10)
        # Approximate power calculation
        power = 1 - stats.norm.cdf(1.96 - effect_size * np.sqrt(len(control) * len(treatment) / (len(control) + len(treatment))))
        
        # Generate recommendation
        if significant and lift > 0:
            recommendation = f"✅ Roll out treatment: {lift:.1f}% lift is statistically significant (p={p_value:.4f})"
        elif significant and lift < 0:
            recommendation = f"❌ Keep control: Treatment shows {abs(lift):.1f}% decrease (p={p_value:.4f})"
        elif not significant and power < 0.8:
            recommendation = f"⏳ Continue test: Need more data for adequate power ({power:.0%} power)"
        else:
            recommendation = f"➡️ No significant difference detected (p={p_value:.4f})"
        
        return ABTestResult(
            control_name='Control',
            treatment_name='Treatment',
            metric_name=metric_name,
            control_mean=control_mean,
            treatment_mean=treatment_mean,
            lift=float(lift),
            p_value=float(p_value),
            confidence_level=confidence_level,
            significant=significant,
            power=float(power),
            sample_sizes=(len(control), len(treatment)),
            test_type=test_type,
            recommendation=recommendation
        )
    
    def compute_confidence_interval(
        self,
        metric: str,
        confidence_level: float = 0.95
    ) -> Dict[str, float]:
        """
        Compute confidence interval for a marketing metric
        
        Args:
            metric: Metric name
            confidence_level: Desired confidence level
            
        Returns:
            Dict with mean, lower, upper bounds
        """
        col = self._get_column(metric)
        if not col:
            return {"error": f"Metric {metric} not found"}
        
        data = self.df[col].dropna()
        if len(data) < 2:
            return {"error": "Insufficient data"}
        
        mean = float(data.mean())
        std = float(data.std())
        n = len(data)
        
        # t-distribution for small samples
        t_critical = stats.t.ppf((1 + confidence_level) / 2, n - 1)
        margin = t_critical * (std / np.sqrt(n))
        
        return {
            "metric": metric,
            "mean": mean,
            "lower": mean - margin,
            "upper": mean + margin,
            "margin_of_error": margin,
            "confidence_level": confidence_level,
            "sample_size": n
        }
    
    def detect_trend(
        self,
        metric: str,
        date_column: str = 'Date'
    ) -> Dict[str, Any]:
        """
        Detect trend in a marketing metric using Mann-Kendall test
        
        Args:
            metric: Metric name to analyze
            date_column: Date column name
            
        Returns:
            Dict with trend analysis results
        """
        col = self._get_column(metric)
        if not col:
            return {"error": f"Metric {metric} not found"}
        
        # Find date column
        date_col = None
        for possible in [date_column, 'date', 'Date', 'DATE', 'Timestamp']:
            if possible in self.df.columns:
                date_col = possible
                break
        
        if not date_col:
            return {"error": "Date column not found"}
        
        # Prepare data
        data = self.df[[date_col, col]].dropna()
        data = data.sort_values(date_col)
        values = data[col].values
        
        if len(values) < 10:
            return {"error": "Insufficient data for trend analysis"}
        
        # Mann-Kendall test
        n = len(values)
        s = 0
        for i in range(n - 1):
            for j in range(i + 1, n):
                s += np.sign(values[j] - values[i])
        
        # Variance
        unique, counts = np.unique(values, return_counts=True)
        g = len(unique)
        tp = sum(t * (t - 1) * (2 * t + 5) for t in counts if t > 1)
        var_s = (n * (n - 1) * (2 * n + 5) - tp) / 18
        
        # Z-statistic
        if s > 0:
            z = (s - 1) / np.sqrt(var_s)
        elif s < 0:
            z = (s + 1) / np.sqrt(var_s)
        else:
            z = 0
        
        p_value = 2 * (1 - stats.norm.cdf(abs(z)))
        
        # Determine trend
        if p_value < 0.05:
            if s > 0:
                trend = 'increasing'
            else:
                trend = 'decreasing'
        else:
            trend = 'no_trend'
        
        # Trend strength (Sen's slope)
        slopes = []
        for i in range(n - 1):
            for j in range(i + 1, n):
                slopes.append((values[j] - values[i]) / (j - i))
        
        sen_slope = float(np.median(slopes)) if slopes else 0
        
        return {
            "metric": metric,
            "trend": trend,
            "z_statistic": float(z),
            "p_value": float(p_value),
            "significant": p_value < 0.05,
            "sen_slope": sen_slope,
            "sample_size": n,
            "interpretation": self._interpret_trend(trend, sen_slope, metric)
        }
    
    def _interpret_trend(self, trend: str, slope: float, metric: str) -> str:
        """Generate human-readable trend interpretation"""
        if trend == 'increasing':
            return f"{metric} is showing a statistically significant upward trend (avg change: {slope:+.2f} per period)"
        elif trend == 'decreasing':
            return f"{metric} is showing a statistically significant downward trend (avg change: {slope:+.2f} per period)"
        else:
            return f"{metric} shows no significant trend over the analyzed period"
    
    def generate_summary_report(self) -> Dict[str, Any]:
        """
        Generate comprehensive statistical summary for marketing data
        
        Returns:
            Dict with all statistical analyses
        """
        report = {
            "timestamp": datetime.utcnow().isoformat(),
            "sample_size": len(self.df),
            "correlations": [],
            "confidence_intervals": {},
            "trends": {}
        }
        
        # Top correlations
        correlations = self.analyze_all_correlations()
        report["correlations"] = [
            {
                "metrics": f"{c.metric_1} vs {c.metric_2}",
                "correlation": c.pearson_r,
                "strength": c.strength,
                "significant": c.significant
            }
            for c in correlations[:5]  # Top 5
        ]
        
        # Confidence intervals for key metrics
        for metric in ['spend', 'roas', 'ctr', 'cpa']:
            ci = self.compute_confidence_interval(metric)
            if "error" not in ci:
                report["confidence_intervals"][metric] = ci
        
        # Trend analysis
        for metric in ['spend', 'conversions', 'roas']:
            trend = self.detect_trend(metric)
            if "error" not in trend:
                report["trends"][metric] = trend
        
        return report

"""
A/B Testing Framework for Agent Effectiveness.

Allows testing different agent variants, LLM models, prompts, and algorithms.
"""

import time
import random
from typing import Dict, Any, List, Optional, Callable
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import statistics
from scipy import stats as scipy_stats

from loguru import logger


class VariantType(Enum):
    """Type of A/B test variant."""
    
    LLM_MODEL = "llm_model"
    PROMPT = "prompt"
    ALGORITHM = "algorithm"
    FEATURE = "feature"
    PARAMETER = "parameter"


@dataclass
class Variant:
    """A/B test variant configuration."""
    
    name: str
    variant_type: VariantType
    config: Dict[str, Any]
    traffic_percentage: float = 50.0  # Percentage of traffic to route to this variant
    
    def __post_init__(self):
        """Validate variant."""
        if not 0 <= self.traffic_percentage <= 100:
            raise ValueError("Traffic percentage must be between 0 and 100")


@dataclass
class TestResult:
    """Result from a single test execution."""
    
    variant_name: str
    execution_time: float
    success: bool
    accuracy_score: Optional[float] = None
    quality_score: Optional[float] = None
    user_satisfaction: Optional[float] = None
    timestamp: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ABTestStatistics:
    """Statistical analysis of A/B test results."""
    
    variant_name: str
    sample_size: int
    success_rate: float
    avg_execution_time: float
    avg_accuracy: float
    avg_quality: float
    p_value: Optional[float] = None
    confidence_interval: Optional[tuple] = None
    is_significant: bool = False
    winner: bool = False


class ABTest:
    """
    A/B test for comparing agent variants.
    
    Supports multiple variants, statistical significance testing,
    and automated rollout decisions.
    """
    
    def __init__(
        self,
        test_name: str,
        variants: List[Variant],
        min_sample_size: int = 100,
        significance_level: float = 0.05
    ):
        """
        Initialize A/B test.
        
        Args:
            test_name: Name of the test
            variants: List of variants to test
            min_sample_size: Minimum samples before statistical analysis
            significance_level: P-value threshold for significance
        """
        self.test_name = test_name
        self.variants = {v.name: v for v in variants}
        self.min_sample_size = min_sample_size
        self.significance_level = significance_level
        
        # Results storage
        self.results: Dict[str, List[TestResult]] = {v.name: [] for v in variants}
        
        # Validate traffic percentages
        total_traffic = sum(v.traffic_percentage for v in variants)
        if abs(total_traffic - 100.0) > 0.01:
            raise ValueError(f"Traffic percentages must sum to 100, got {total_traffic}")
        
        logger.info(f"Initialized A/B test: {test_name} with {len(variants)} variants")
    
    def select_variant(self) -> Variant:
        """
        Select a variant based on traffic allocation.
        
        Returns:
            Selected variant
        """
        rand = random.random() * 100
        cumulative = 0
        
        for variant in self.variants.values():
            cumulative += variant.traffic_percentage
            if rand <= cumulative:
                return variant
        
        # Fallback to first variant
        return list(self.variants.values())[0]
    
    def record_result(self, result: TestResult):
        """
        Record a test result.
        
        Args:
            result: Test result to record
        """
        if result.variant_name not in self.results:
            logger.warning(f"Unknown variant: {result.variant_name}")
            return
        
        self.results[result.variant_name].append(result)
        logger.debug(f"Recorded result for variant {result.variant_name}")
    
    def get_statistics(self) -> Dict[str, ABTestStatistics]:
        """
        Calculate statistics for all variants.
        
        Returns:
            Dictionary of variant statistics
        """
        stats_dict = {}
        
        for variant_name, results in self.results.items():
            if not results:
                continue
            
            # Calculate basic metrics
            sample_size = len(results)
            success_rate = sum(1 for r in results if r.success) / sample_size * 100
            avg_execution_time = statistics.mean(r.execution_time for r in results)
            
            # Calculate accuracy (if available)
            accuracy_scores = [r.accuracy_score for r in results if r.accuracy_score is not None]
            avg_accuracy = statistics.mean(accuracy_scores) if accuracy_scores else 0.0
            
            # Calculate quality (if available)
            quality_scores = [r.quality_score for r in results if r.quality_score is not None]
            avg_quality = statistics.mean(quality_scores) if quality_scores else 0.0
            
            stats_dict[variant_name] = ABTestStatistics(
                variant_name=variant_name,
                sample_size=sample_size,
                success_rate=success_rate,
                avg_execution_time=avg_execution_time,
                avg_accuracy=avg_accuracy,
                avg_quality=avg_quality
            )
        
        # Perform statistical significance testing
        if len(stats_dict) >= 2 and all(s.sample_size >= self.min_sample_size for s in stats_dict.values()):
            self._calculate_significance(stats_dict)
        
        return stats_dict
    
    def _calculate_significance(self, stats_dict: Dict[str, ABTestStatistics]):
        """
        Calculate statistical significance between variants.
        
        Args:
            stats_dict: Dictionary of variant statistics
        """
        # Get control variant (first variant)
        control_name = list(self.variants.keys())[0]
        control_results = self.results[control_name]
        
        # Compare each variant to control
        for variant_name, variant_stats in stats_dict.items():
            if variant_name == control_name:
                continue
            
            variant_results = self.results[variant_name]
            
            # Compare success rates using chi-square test
            control_successes = sum(1 for r in control_results if r.success)
            variant_successes = sum(1 for r in variant_results if r.success)
            
            contingency_table = [
                [control_successes, len(control_results) - control_successes],
                [variant_successes, len(variant_results) - variant_successes]
            ]
            
            chi2, p_value, dof, expected = scipy_stats.chi2_contingency(contingency_table)
            variant_stats.p_value = p_value
            variant_stats.is_significant = p_value < self.significance_level
            
            # Calculate confidence interval for success rate difference
            if variant_stats.is_significant:
                # Simple confidence interval calculation
                diff = variant_stats.success_rate - stats_dict[control_name].success_rate
                se = ((variant_stats.success_rate * (100 - variant_stats.success_rate) / variant_stats.sample_size) +
                      (stats_dict[control_name].success_rate * (100 - stats_dict[control_name].success_rate) / stats_dict[control_name].sample_size)) ** 0.5
                ci = (diff - 1.96 * se, diff + 1.96 * se)
                variant_stats.confidence_interval = ci
        
        # Determine winner
        best_variant = max(stats_dict.values(), key=lambda s: s.success_rate)
        if best_variant.is_significant:
            best_variant.winner = True
    
    def should_rollout(self, variant_name: str) -> bool:
        """
        Determine if a variant should be rolled out.
        
        Args:
            variant_name: Variant to check
        
        Returns:
            True if variant should be rolled out
        """
        stats = self.get_statistics()
        
        if variant_name not in stats:
            return False
        
        variant_stats = stats[variant_name]
        
        # Check if we have enough samples
        if variant_stats.sample_size < self.min_sample_size:
            logger.info(f"Not enough samples for {variant_name}: {variant_stats.sample_size}/{self.min_sample_size}")
            return False
        
        # Check if variant is winner
        if not variant_stats.winner:
            logger.info(f"Variant {variant_name} is not the winner")
            return False
        
        # Check if improvement is significant
        if not variant_stats.is_significant:
            logger.info(f"Variant {variant_name} improvement is not statistically significant")
            return False
        
        logger.info(f"Variant {variant_name} is ready for rollout!")
        return True
    
    def get_report(self) -> str:
        """
        Generate a text report of test results.
        
        Returns:
            Formatted report string
        """
        stats = self.get_statistics()
        
        lines = [
            "=" * 70,
            f"A/B Test Report: {self.test_name}",
            "=" * 70,
            ""
        ]
        
        for variant_name, variant_stats in stats.items():
            winner_icon = "🏆" if variant_stats.winner else ""
            sig_icon = "✅" if variant_stats.is_significant else "⚠️"
            
            lines.extend([
                f"{winner_icon} Variant: {variant_name}",
                f"├─ Sample Size: {variant_stats.sample_size}",
                f"├─ Success Rate: {variant_stats.success_rate:.2f}%",
                f"├─ Avg Execution Time: {variant_stats.avg_execution_time:.3f}s",
                f"├─ Avg Accuracy: {variant_stats.avg_accuracy:.2f}%",
                f"├─ Avg Quality: {variant_stats.avg_quality:.2f}%",
            ])
            
            if variant_stats.p_value is not None:
                lines.append(f"├─ P-value: {variant_stats.p_value:.4f} {sig_icon}")
            
            if variant_stats.confidence_interval:
                ci_low, ci_high = variant_stats.confidence_interval
                lines.append(f"├─ 95% CI: [{ci_low:.2f}%, {ci_high:.2f}%]")
            
            lines.append(f"└─ Significant: {variant_stats.is_significant}")
            lines.append("")
        
        # Recommendation
        winner = next((s for s in stats.values() if s.winner), None)
        if winner:
            lines.extend([
                "=" * 70,
                f"🏆 RECOMMENDATION: Roll out {winner.variant_name}",
                f"   Improvement: {winner.success_rate:.2f}% success rate",
                f"   P-value: {winner.p_value:.4f}",
                "=" * 70
            ])
        else:
            lines.extend([
                "=" * 70,
                "⚠️  No clear winner yet. Continue testing.",
                "=" * 70
            ])
        
        return "\n".join(lines)


class ABTestManager:
    """
    Manager for multiple A/B tests.
    
    Coordinates multiple tests and provides centralized reporting.
    """
    
    def __init__(self):
        """Initialize A/B test manager."""
        self.tests: Dict[str, ABTest] = {}
        logger.info("Initialized ABTestManager")
    
    def create_test(
        self,
        test_name: str,
        variants: List[Variant],
        min_sample_size: int = 100,
        significance_level: float = 0.05
    ) -> ABTest:
        """
        Create a new A/B test.
        
        Args:
            test_name: Name of the test
            variants: List of variants
            min_sample_size: Minimum samples for analysis
            significance_level: P-value threshold
        
        Returns:
            Created A/B test
        """
        if test_name in self.tests:
            logger.warning(f"Test {test_name} already exists, returning existing test")
            return self.tests[test_name]
        
        test = ABTest(test_name, variants, min_sample_size, significance_level)
        self.tests[test_name] = test
        logger.info(f"Created A/B test: {test_name}")
        return test
    
    def get_test(self, test_name: str) -> Optional[ABTest]:
        """Get a test by name."""
        return self.tests.get(test_name)
    
    def get_all_reports(self) -> str:
        """Get reports for all tests."""
        if not self.tests:
            return "No A/B tests running"
        
        reports = []
        for test in self.tests.values():
            reports.append(test.get_report())
        
        return "\n\n".join(reports)


# Global manager instance
_manager: Optional[ABTestManager] = None


def get_ab_test_manager() -> ABTestManager:
    """Get global A/B test manager instance."""
    global _manager
    if _manager is None:
        _manager = ABTestManager()
    return _manager


def ab_test(test_name: str):
    """
    Decorator for A/B testing agent functions.
    
    Args:
        test_name: Name of the A/B test
    
    Example:
        @ab_test("reasoning_agent_llm_test")
        def analyze_campaign(data, variant_config):
            # Use variant_config to determine behavior
            return result
    """
    def decorator(func: Callable) -> Callable:
        def wrapper(*args, **kwargs):
            manager = get_ab_test_manager()
            test = manager.get_test(test_name)
            
            if test is None:
                # No test configured, run normally
                return func(*args, **kwargs)
            
            # Select variant
            variant = test.select_variant()
            
            # Add variant config to kwargs
            kwargs['variant_config'] = variant.config
            
            # Execute and measure
            start_time = time.time()
            success = False
            result = None
            
            try:
                result = func(*args, **kwargs)
                success = True
            except Exception as e:
                logger.error(f"Error in A/B test {test_name}, variant {variant.name}: {e}")
                raise
            finally:
                execution_time = time.time() - start_time
                
                # Record result
                test_result = TestResult(
                    variant_name=variant.name,
                    execution_time=execution_time,
                    success=success
                )
                test.record_result(test_result)
            
            return result
        
        return wrapper
    return decorator

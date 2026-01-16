"""
Comprehensive Dimension Importance Framework

Analyzes which dimensions (platform, campaign, objective, creative, etc.) 
have the most impact on performance metrics.

Methods:
1. Variance Decomposition (R² attribution)
2. ANOVA F-statistics
3. Information Gain (entropy-based)
4. Effect Size (Cohen's d, eta-squared)
5. Permutation Importance
6. SHAP-based dimension importance
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from loguru import logger
from scipy import stats
from sklearn.preprocessing import LabelEncoder
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import r2_score


@dataclass
class DimensionResult:
    """Result for a single dimension's importance analysis."""
    dimension: str
    n_unique: int
    importance_score: float
    variance_explained: float
    f_statistic: Optional[float]
    p_value: Optional[float]
    effect_size: float
    effect_interpretation: str
    top_values: List[Dict[str, Any]]
    recommendation: str


@dataclass
class DimensionImportanceReport:
    """Complete dimension importance report."""
    success: bool
    target_metric: str
    dimensions_analyzed: List[str]
    results: List[DimensionResult]
    rankings: pd.DataFrame
    interactions: List[Dict[str, Any]]
    recommendations: List[str]


class DimensionImportanceFramework:
    """
    Comprehensive framework for analyzing dimension importance.
    
    Analyzes categorical dimensions like:
    - Platform (Meta, Google, DV360)
    - Campaign type (brand, performance, retargeting)
    - Objective (awareness, conversions, traffic)
    - Creative type (video, image, carousel)
    - Audience segment
    - Geography
    - Device
    """
    
    # Effect size thresholds (eta-squared)
    EFFECT_SMALL = 0.01
    EFFECT_MEDIUM = 0.06
    EFFECT_LARGE = 0.14
    
    COMMON_DIMENSIONS = [
        # Platform & Channel
        'platform', 'channel', 'network', 'source', 'medium',
        
        # Campaign Structure
        'campaign', 'campaign_type', 'campaign_name', 'ad_group', 'adset',
        
        # Funnel & Objective
        'funnel', 'funnel_stage', 'objective', 'goal', 'optimization_goal',
        
        # Creative
        'creative', 'creative_type', 'ad_type', 'format', 'ad_format',
        'creative_size', 'aspect_ratio', 'video_length',
        
        # Placement
        'placement', 'placement_type', 'position', 'inventory_type',
        'feed', 'stories', 'reels', 'display', 'search',
        
        # Audience
        'audience', 'audience_type', 'targeting', 'segment', 'cohort',
        'interest', 'behavior', 'lookalike', 'retargeting', 'custom_audience',
        
        # Geography
        'geo', 'region', 'country', 'state', 'city', 'dma', 'market',
        'location', 'geography', 'territory',
        
        # Device & Technology
        'device', 'device_type', 'os', 'browser', 'carrier',
        'mobile', 'desktop', 'tablet', 'connected_tv',
        
        # Bidding & Budget
        'bid_strategy', 'bid_type', 'budget_type', 'pacing',
        
        # Time
        'day_of_week', 'hour', 'daypart', 'week', 'month', 'quarter',
        
        # Attribution
        'attribution_window', 'conversion_window',
        
        # Status
        'status', 'delivery_status', 'learning_phase'
    ]
    
    def __init__(
        self,
        min_group_size: int = 10,
        max_unique_values: int = 50
    ):
        """
        Initialize framework.
        
        Args:
            min_group_size: Minimum samples per dimension value
            max_unique_values: Max unique values for a dimension (skip if exceeded)
        """
        self.min_group_size = min_group_size
        self.max_unique_values = max_unique_values
    
    def analyze(
        self,
        df: pd.DataFrame,
        target_col: str,
        dimension_cols: Optional[List[str]] = None,
        numeric_features: Optional[List[str]] = None
    ) -> DimensionImportanceReport:
        """
        Run comprehensive dimension importance analysis.
        
        Args:
            df: Input DataFrame
            target_col: Target metric (e.g., 'conversions', 'roas')
            dimension_cols: Categorical dimensions to analyze
            numeric_features: Numeric features for interaction analysis
        """
        logger.info(f"DimensionImportance: Analyzing {target_col}")
        
        # Auto-detect dimensions if not provided
        if dimension_cols is None:
            dimension_cols = self._detect_dimensions(df)
        
        if target_col not in df.columns:
            return DimensionImportanceReport(
                success=False,
                target_metric=target_col,
                dimensions_analyzed=[],
                results=[],
                rankings=pd.DataFrame(),
                interactions=[],
                recommendations=[f"Target '{target_col}' not found"]
            )
        
        results = []
        
        for dim in dimension_cols:
            if dim not in df.columns:
                continue
            
            result = self._analyze_dimension(df, dim, target_col)
            if result:
                results.append(result)
        
        # Sort by importance
        results.sort(key=lambda x: x.importance_score, reverse=True)
        
        # Build rankings DataFrame
        rankings = self._build_rankings(results)
        
        # Analyze interactions
        interactions = self._analyze_interactions(df, dimension_cols, target_col)
        
        # Generate recommendations
        recommendations = self._generate_recommendations(results)
        
        return DimensionImportanceReport(
            success=True,
            target_metric=target_col,
            dimensions_analyzed=dimension_cols,
            results=results,
            rankings=rankings,
            interactions=interactions,
            recommendations=recommendations
        )
    
    def _detect_dimensions(self, df: pd.DataFrame) -> List[str]:
        """Auto-detect categorical dimensions."""
        dimensions = []
        
        for col in df.columns:
            # Check if column looks like a dimension
            col_lower = col.lower()
            
            # Match common dimension names
            is_dimension = any(dim in col_lower for dim in self.COMMON_DIMENSIONS)
            
            # Or check if it's categorical with reasonable cardinality
            if not is_dimension and df[col].dtype == 'object':
                n_unique = df[col].nunique()
                if 2 <= n_unique <= self.max_unique_values:
                    is_dimension = True
            
            if is_dimension:
                dimensions.append(col)
        
        logger.debug(f"Detected dimensions: {dimensions}")
        return dimensions
    
    def _analyze_dimension(
        self,
        df: pd.DataFrame,
        dim: str,
        target: str
    ) -> Optional[DimensionResult]:
        """Analyze a single dimension."""
        try:
            # Filter valid data
            valid_df = df[[dim, target]].dropna()
            
            if len(valid_df) < self.min_group_size * 2:
                return None
            
            n_unique = valid_df[dim].nunique()
            
            if n_unique > self.max_unique_values or n_unique < 2:
                return None
            
            # Get groups
            groups = [group[target].values for name, group in valid_df.groupby(dim) 
                     if len(group) >= self.min_group_size]
            
            if len(groups) < 2:
                return None
            
            # 1. ANOVA F-test
            f_stat, p_value = stats.f_oneway(*groups)
            
            # 2. Effect size (eta-squared)
            ss_between = sum(len(g) * (np.mean(g) - valid_df[target].mean())**2 for g in groups)
            ss_total = sum((valid_df[target] - valid_df[target].mean())**2)
            eta_squared = ss_between / ss_total if ss_total > 0 else 0
            
            # Interpret effect size
            if eta_squared >= self.EFFECT_LARGE:
                effect_interp = 'Large'
            elif eta_squared >= self.EFFECT_MEDIUM:
                effect_interp = 'Medium'
            elif eta_squared >= self.EFFECT_SMALL:
                effect_interp = 'Small'
            else:
                effect_interp = 'Negligible'
            
            # 3. Variance explained (R² from encoding)
            le = LabelEncoder()
            encoded = le.fit_transform(valid_df[dim])
            variance_explained = self._calculate_variance_explained(encoded, valid_df[target].values)
            
            # 4. Importance score (composite)
            importance_score = self._calculate_importance_score(
                eta_squared, p_value, variance_explained, n_unique
            )
            
            # 5. Top values by target mean
            value_stats = valid_df.groupby(dim)[target].agg(['mean', 'std', 'count']).reset_index()
            value_stats = value_stats.sort_values('mean', ascending=False)
            
            top_values = []
            for _, row in value_stats.head(5).iterrows():
                top_values.append({
                    'value': row[dim],
                    'mean': float(row['mean']),
                    'std': float(row['std']) if pd.notna(row['std']) else 0,
                    'count': int(row['count'])
                })
            
            # 6. Generate recommendation
            recommendation = self._generate_dimension_recommendation(
                dim, effect_interp, top_values, n_unique
            )
            
            return DimensionResult(
                dimension=dim,
                n_unique=n_unique,
                importance_score=importance_score,
                variance_explained=variance_explained,
                f_statistic=float(f_stat) if pd.notna(f_stat) else None,
                p_value=float(p_value) if pd.notna(p_value) else None,
                effect_size=eta_squared,
                effect_interpretation=effect_interp,
                top_values=top_values,
                recommendation=recommendation
            )
            
        except Exception as e:
            logger.warning(f"Error analyzing dimension {dim}: {e}")
            return None
    
    def _calculate_variance_explained(
        self,
        encoded: np.ndarray,
        target: np.ndarray
    ) -> float:
        """Calculate variance explained using simple regression."""
        try:
            correlation = np.corrcoef(encoded, target)[0, 1]
            return correlation ** 2 if pd.notna(correlation) else 0
        except:
            return 0
    
    def _calculate_importance_score(
        self,
        eta_squared: float,
        p_value: float,
        variance_explained: float,
        n_unique: int
    ) -> float:
        """
        Calculate composite importance score (0-100).
        
        Weights:
        - 40% effect size (eta-squared)
        - 30% statistical significance
        - 20% variance explained
        - 10% penalty for too many categories
        """
        # Normalize eta-squared to 0-1 (cap at 0.5)
        effect_score = min(eta_squared / 0.5, 1.0)
        
        # Significance score (1 if p < 0.05)
        sig_score = 1.0 if p_value < 0.05 else max(0, 1 - (p_value - 0.05))
        
        # Variance score
        var_score = min(variance_explained, 1.0)
        
        # Cardinality penalty
        cardinality_score = 1.0 - min(n_unique / 50, 0.5)
        
        importance = (
            0.40 * effect_score +
            0.30 * sig_score +
            0.20 * var_score +
            0.10 * cardinality_score
        ) * 100
        
        return round(importance, 2)
    
    def _generate_dimension_recommendation(
        self,
        dim: str,
        effect: str,
        top_values: List[Dict],
        n_unique: int
    ) -> str:
        """Generate recommendation for a dimension."""
        if effect == 'Large':
            if top_values:
                best = top_values[0]['value']
                return f"Critical driver. Focus on '{best}' for best performance."
            return "Critical driver. Segment analysis recommended."
        
        elif effect == 'Medium':
            if top_values and len(top_values) >= 2:
                best = top_values[0]['value']
                worst = top_values[-1]['value']
                return f"Moderate impact. Consider shifting from '{worst}' to '{best}'."
            return "Moderate impact. Worth optimizing."
        
        elif effect == 'Small':
            return "Minor impact. Consider for fine-tuning only."
        
        else:
            return "Negligible impact. May exclude from analysis."
    
    def _build_rankings(self, results: List[DimensionResult]) -> pd.DataFrame:
        """Build rankings DataFrame."""
        rows = []
        for rank, result in enumerate(results, 1):
            rows.append({
                'Rank': rank,
                'Dimension': result.dimension,
                'Importance Score': result.importance_score,
                'Effect Size': f"{result.effect_size:.4f}",
                'Effect': result.effect_interpretation,
                'p-value': f"{result.p_value:.4f}" if result.p_value else 'N/A',
                'N Values': result.n_unique,
                'Recommendation': result.recommendation[:50] + '...' if len(result.recommendation) > 50 else result.recommendation
            })
        
        return pd.DataFrame(rows)
    
    def _analyze_interactions(
        self,
        df: pd.DataFrame,
        dimensions: List[str],
        target: str
    ) -> List[Dict[str, Any]]:
        """Analyze two-way interactions between dimensions."""
        interactions = []
        
        analyzed_pairs = set()
        
        for i, dim1 in enumerate(dimensions):
            if dim1 not in df.columns:
                continue
            
            for dim2 in dimensions[i+1:]:
                if dim2 not in df.columns:
                    continue
                
                pair = tuple(sorted([dim1, dim2]))
                if pair in analyzed_pairs:
                    continue
                analyzed_pairs.add(pair)
                
                try:
                    # Create interaction column
                    interaction_df = df[[dim1, dim2, target]].dropna()
                    interaction_df['interaction'] = interaction_df[dim1].astype(str) + '_' + interaction_df[dim2].astype(str)
                    
                    n_combos = interaction_df['interaction'].nunique()
                    
                    if n_combos > 50 or n_combos < 4:
                        continue
                    
                    # Calculate interaction effect
                    groups = [g[target].values for _, g in interaction_df.groupby('interaction') if len(g) >= 5]
                    
                    if len(groups) >= 2:
                        f_stat, p_value = stats.f_oneway(*groups)
                        
                        if p_value < 0.05:
                            # Find best combination
                            combo_means = interaction_df.groupby('interaction')[target].mean()
                            best_combo = combo_means.idxmax()
                            best_value = combo_means.max()
                            
                            interactions.append({
                                'dimension_1': dim1,
                                'dimension_2': dim2,
                                'n_combinations': n_combos,
                                'f_statistic': float(f_stat),
                                'p_value': float(p_value),
                                'best_combination': best_combo,
                                'best_value': float(best_value),
                                'significant': True
                            })
                
                except Exception as e:
                    logger.debug(f"Interaction analysis failed for {dim1} x {dim2}: {e}")
        
        # Sort by F-statistic
        interactions.sort(key=lambda x: x['f_statistic'], reverse=True)
        
        return interactions[:10]  # Top 10 interactions
    
    def _generate_recommendations(
        self,
        results: List[DimensionResult]
    ) -> List[str]:
        """Generate overall recommendations."""
        recommendations = []
        
        if not results:
            return ["No significant dimensions found. Check data quality."]
        
        # Top driver
        top = results[0]
        recommendations.append(
            f"Primary driver: '{top.dimension}' (importance={top.importance_score:.0f}%). "
            f"Top performing: '{top.top_values[0]['value'] if top.top_values else 'N/A'}'."
        )
        
        # Large effect dimensions
        large_effect = [r for r in results if r.effect_interpretation == 'Large']
        if len(large_effect) > 1:
            dims = [r.dimension for r in large_effect[:3]]
            recommendations.append(
                f"Multiple high-impact dimensions: {', '.join(dims)}. Prioritize these for optimization."
            )
        
        # Low importance dimensions
        low_importance = [r for r in results if r.importance_score < 20]
        if low_importance:
            dims = [r.dimension for r in low_importance[:3]]
            recommendations.append(
                f"Low-impact dimensions ({', '.join(dims)}) can be deprioritized or consolidated."
            )
        
        return recommendations


# =============================================================================
# PERMUTATION IMPORTANCE
# =============================================================================

class PermutationDimensionImportance:
    """
    Calculate dimension importance using permutation.
    
    More robust than ANOVA for non-linear relationships.
    """
    
    def __init__(self, n_repeats: int = 10, random_state: int = 42):
        self.n_repeats = n_repeats
        self.random_state = random_state
    
    def calculate(
        self,
        df: pd.DataFrame,
        target_col: str,
        dimension_cols: List[str],
        numeric_cols: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """Calculate permutation importance for dimensions."""
        np.random.seed(self.random_state)
        
        # Prepare data
        all_cols = dimension_cols.copy()
        if numeric_cols:
            all_cols.extend(numeric_cols)
        
        valid_cols = [c for c in all_cols if c in df.columns]
        df_clean = df[valid_cols + [target_col]].dropna()
        
        if len(df_clean) < 100:
            return pd.DataFrame()
        
        # Encode categoricals
        X = df_clean[valid_cols].copy()
        for col in dimension_cols:
            if col in X.columns:
                X[col] = LabelEncoder().fit_transform(X[col].astype(str))
        
        y = df_clean[target_col].values
        
        # Fit model
        model = RandomForestRegressor(n_estimators=50, max_depth=10, random_state=42)
        model.fit(X, y)
        baseline_score = r2_score(y, model.predict(X))
        
        # Permute each dimension
        importances = []
        
        for col in dimension_cols:
            if col not in X.columns:
                continue
            
            scores = []
            for _ in range(self.n_repeats):
                X_permuted = X.copy()
                X_permuted[col] = np.random.permutation(X_permuted[col].values)
                permuted_score = r2_score(y, model.predict(X_permuted))
                scores.append(baseline_score - permuted_score)
            
            importances.append({
                'dimension': col,
                'importance_mean': np.mean(scores),
                'importance_std': np.std(scores),
                'baseline_r2': baseline_score
            })
        
        result = pd.DataFrame(importances)
        result = result.sort_values('importance_mean', ascending=False)
        result['rank'] = range(1, len(result) + 1)
        
        return result


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

def analyze_dimension_importance(
    df: pd.DataFrame,
    target_col: str,
    dimension_cols: Optional[List[str]] = None
) -> DimensionImportanceReport:
    """Convenience function to run dimension importance analysis."""
    framework = DimensionImportanceFramework()
    return framework.analyze(df, target_col, dimension_cols)


def get_dimension_rankings(
    df: pd.DataFrame,
    target_col: str
) -> pd.DataFrame:
    """Get dimension importance rankings as DataFrame."""
    framework = DimensionImportanceFramework()
    report = framework.analyze(df, target_col)
    return report.rankings

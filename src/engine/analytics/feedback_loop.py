"""
Outcome Tracker - Performance Attribution for Recommended Actions
Attributes real-world gains/losses to previously implemented recommendations.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from loguru import logger
from src.core.database.duckdb_manager import get_duckdb_manager
from src.core.schema.columns import Columns

class OutcomeTracker:
    """
    Automated attribution system for Closed-Loop Analytics.
    Compares 'Before' vs 'After' performance for implemented recommendations.
    """

    @staticmethod
    def get_attribution_summary() -> List[Dict[str, Any]]:
        """
        Calculate ROI for all implemented recommendations that have enough future data.
        """
        try:
            db = get_duckdb_manager()
            with db.connection() as conn:
                # 1. Get implemented recommendations with feedback
                query = """
                    SELECT 
                        h.id as recommendation_id,
                        h.target_metric,
                        h.feature_name,
                        h.suggested_strategy,
                        h.recommendation_score,
                        f.implementation_date,
                        f.user_action
                    FROM recommendation_history h
                    JOIN recommendation_feedback f ON h.id = f.recommendation_id
                    WHERE f.user_action = 'Implemented'
                """
                implemented_recs = conn.execute(query).df()
                
                if implemented_recs.empty:
                    return []

                # 2. For each recommendation, calculate impact
                attribution_results = []
                for _, rec in implemented_recs.iterrows():
                    impact = OutcomeTracker._calculate_impact(rec)
                    if impact:
                        attribution_results.append(impact)
                        
                        # Persist the actual impact back to the feedback table
                        conn.execute("""
                            UPDATE recommendation_feedback 
                            SET actual_impact_observed = ? 
                            WHERE recommendation_id = ?
                        """, (impact['roi_percent'], rec['recommendation_id']))
                
                return attribution_results
        except Exception as e:
            logger.error(f"Attribution failed: {e}")
            return []

    @staticmethod
    def _calculate_impact(rec: pd.Series) -> Optional[Dict[str, Any]]:
        """
        Calculates the delta in performance for a specific feature.
        Uses a 7-day window 'Before' and 'After' the implementation date.
        """
        try:
            db = get_duckdb_manager()
            impl_date = pd.to_datetime(rec['implementation_date'])
            
            # Windows for comparison
            # We want to see if performance changed after the implementation
            pre_start = impl_date - timedelta(days=14)
            pre_end = impl_date - timedelta(days=1)
            post_start = impl_date + timedelta(days=1)
            post_end = impl_date + timedelta(days=14)

            # Get full dataset to compute baseline
            # Note: We query specifically for the feature and target
            df = db.get_campaigns()
            if df.empty:
                return None

            date_col = 'date' # Normalized in DuckDBManager
            target = rec['target_metric']
            feature = rec['feature_name']
            
            # Ensure target and feature exist in data (case-insensitive)
            available_cols = {c.lower(): c for c in df.columns}
            
            # Also handle date column variations
            actual_date_col = available_cols.get('date', available_cols.get('timestamp', 'date'))
            
            target_lower = target.lower()
            feature_lower = feature.lower()
            
            if target_lower not in available_cols or feature_lower not in available_cols:
                logger.warning(f"Target '{target}' or Feature '{feature}' missing from {list(df.columns)}")
                return None
            
            # Use the actual column names from the DF
            actual_target = available_cols[target_lower]
            actual_feature = available_cols[feature_lower]

            # Split data
            pre_df = df[(df[actual_date_col] >= pre_start) & (df[actual_date_col] <= pre_end)]
            post_df = df[(df[actual_date_col] >= post_start) & (df[actual_date_col] <= post_end)]

            logger.info(f"Impact calc: Pre-rows={len(pre_df)}, Post-rows={len(post_df)} using col '{actual_date_col}'")


            if len(pre_df) < 3 or len(post_df) < 3:
                # Not enough data for meaningful comparison yet
                return None

            # Calculate metrics
            pre_target_sum = pre_df[actual_target].sum()
            pre_feature_sum = pre_df[actual_feature].sum()
            
            post_target_sum = post_df[actual_target].sum()
            post_feature_sum = post_df[actual_feature].sum()


            pre_efficiency = pre_target_sum / pre_feature_sum if pre_feature_sum > 0 else 0
            post_efficiency = post_target_sum / post_feature_sum if post_feature_sum > 0 else 0
            
            roi_delta = post_efficiency - pre_efficiency
            roi_percent = (roi_delta / pre_efficiency * 100) if pre_efficiency > 0 else 0

            logger.info(f"Efficiency: Pre={pre_efficiency:.4f}, Post={post_efficiency:.4f}, ROI={roi_percent:.2f}%")


            return {
                "recommendation_id": rec['recommendation_id'],
                "feature": feature,
                "strategy": rec['suggested_strategy'],
                "pre_efficiency": round(float(pre_efficiency), 4),
                "post_efficiency": round(float(post_efficiency), 4),
                "roi_percent": round(float(roi_percent), 2),
                "status": "Positive Impact" if roi_percent > 5 else "Negative Impact" if roi_percent < -5 else "Neutral"
            }
        except Exception as e:
            logger.debug(f"Could not calculate impact for {rec['recommendation_id']}: {e} | Columns: {list(df.columns) if 'df' in locals() else 'N/A'}")
            return None


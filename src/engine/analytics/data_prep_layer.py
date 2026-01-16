"""
Enhanced Data Preparation Layer for Regression Analysis

Production-grade data preparation with:
1. Unified schema mapping (Meta, Google Ads, DV360, LinkedIn, Snapchat)
2. Data cleaning (duplicates, zero-spend, date validation)
3. Variable-type-specific missing value strategies
4. Outlier detection and treatment (IQR, Z-score, 99th percentile capping)
5. Feature engineering (derived ratios, temporal, platform indicators, interactions)
6. Feature scaling (StandardScaler)
7. Train/validation/test split with stratification
8. Multicollinearity check (VIF)
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional, Any
from loguru import logger
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split

from src.core.utils.column_mapping import (
    METRIC_COLUMN_ALIASES,
    find_column,
    normalize_column_names
)


class DataPrepLayer:
    """
    Production-grade data preparation for regression/ML analysis.
    
    Supports ~50K rows from Meta, Google Ads, DV360, LinkedIn, Snapchat.
    """
    
    # ==========================================================================
    # PLATFORM FIELD MAPPINGS
    # ==========================================================================
    PLATFORM_FIELD_MAPPINGS = {
        'meta': {
            'campaign_name': 'campaign_id', 'spend': 'spend', 'impressions': 'impressions',
            'clicks': 'clicks', 'conversions': 'conversions', 'reach': 'reach'
        },
        'google_ads': {
            'campaign': 'campaign_id', 'cost': 'spend', 'impressions': 'impressions',
            'clicks': 'clicks', 'conversions': 'conversions'
        },
        'dv360': {
            'insertion_order': 'campaign_id', 'cost': 'spend', 'impressions': 'impressions',
            'clicks': 'clicks', 'total_conversions': 'conversions'
        },
        'linkedin': {
            'campaign_name': 'campaign_id', 'cost': 'spend', 'impressions': 'impressions',
            'clicks': 'clicks', 'leads': 'conversions'
        },
        'snapchat': {
            'campaign_name': 'campaign_id', 'spend': 'spend', 'impressions': 'impressions',
            'swipes': 'clicks', 'conversions': 'conversions'
        },
        'cm360': {
            'campaign': 'campaign_id', 'media_cost': 'spend', 'impressions': 'impressions',
            'clicks': 'clicks', 'total_conversions': 'conversions'
        }
    }
    
    # ==========================================================================
    # MISSING VALUE THRESHOLDS BY VARIABLE TYPE
    # ==========================================================================
    MISSING_THRESHOLDS = {
        'spend': {'strategy': 'zero', 'drop_threshold': 0.30},
        'impressions': {'strategy': 'forward_fill', 'drop_threshold': 0.30},
        'clicks': {'strategy': 'zero', 'drop_threshold': 0.30},
        'conversions': {'strategy': 'zero', 'drop_threshold': 1.0},  # Keep all
        'roas': {'strategy': 'skip', 'drop_threshold': 1.0},  # Derived
        'engagement': {'strategy': 'mean', 'drop_threshold': 0.20, 'flag': True},
        'categorical': {'strategy': 'mode', 'drop_threshold': 0.50}
    }
    
    # Metrics for outlier detection
    OUTLIER_CHECK_METRICS = {'spend', 'conversions', 'roas', 'ctr', 'frequency'}
    
    def __init__(
        self,
        missing_threshold: float = 0.30,
        outlier_method: str = 'iqr',
        cap_percentile: float = 0.99,
        enable_feature_engineering: bool = True,
        enable_scaling: bool = True,
        enable_vif_check: bool = True,
        vif_threshold: float = 10.0
    ):
        """
        Initialize DataPrepLayer.
        
        Args:
            missing_threshold: Default threshold for excluding columns
            outlier_method: 'iqr' or 'zscore'
            cap_percentile: Percentile to cap outliers (default 99th)
            enable_feature_engineering: Whether to create derived features
            enable_scaling: Whether to apply StandardScaler
            enable_vif_check: Whether to check multicollinearity
            vif_threshold: VIF threshold for dropping features
        """
        self.missing_threshold = missing_threshold
        self.outlier_method = outlier_method
        self.cap_percentile = cap_percentile
        self.enable_feature_engineering = enable_feature_engineering
        self.enable_scaling = enable_scaling
        self.enable_vif_check = enable_vif_check
        self.vif_threshold = vif_threshold
        
        # State tracking
        self.excluded_columns: List[str] = []
        self.column_mappings: Dict[str, str] = {}
        self.missing_report: Dict[str, float] = {}
        self.outlier_report: Dict[str, int] = {}
        self.vif_report: Dict[str, float] = {}
        self.scaler: Optional[StandardScaler] = None
        self.feature_names: List[str] = []
        
    def prepare(
        self,
        df: pd.DataFrame,
        time_col: str = 'date',
        group_col: Optional[str] = 'campaign',
        target_col: Optional[str] = None,
        feature_cols: Optional[List[str]] = None,
        platform_col: Optional[str] = 'platform'
    ) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Main entry point for comprehensive data preparation.
        """
        if df is None or df.empty:
            logger.warning("DataPrepLayer received empty DataFrame")
            return pd.DataFrame(), {"error": "Empty DataFrame"}
        
        logger.info(f"DataPrepLayer: Starting preparation of {len(df)} rows, {len(df.columns)} columns")
        
        # Step 1: Schema standardization
        df = self._apply_platform_schema(df, platform_col)
        
        # Step 2: Data cleaning
        df = self._clean_data(df, time_col, group_col, platform_col)
        
        # Step 3: Variable-type specific missing value handling
        df = self._handle_missing_values(df, target_col)
        
        # Step 4: Outlier detection and treatment
        df = self._handle_outliers(df)
        
        # Step 5: Feature engineering
        if self.enable_feature_engineering:
            df = self._engineer_features(df, time_col, platform_col)
        
        # Step 6: Final cleanup
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        df[numeric_cols] = df[numeric_cols].fillna(0)
        
        # Build metadata
        metadata = {
            "rows_processed": len(df),
            "columns_mapped": self.column_mappings,
            "excluded_columns": self.excluded_columns,
            "missing_report": self.missing_report,
            "outlier_report": self.outlier_report,
            "features_created": self.feature_names
        }
        
        logger.info(f"DataPrepLayer: Complete. {len(df)} rows, {len(df.columns)} columns")
        
        return df, metadata
    
    def prepare_for_modeling(
        self,
        df: pd.DataFrame,
        target_col: str,
        feature_cols: List[str],
        test_size: float = 0.15,
        val_size: float = 0.15,
        stratify_col: Optional[str] = 'platform',
        time_aware: bool = False,
        time_col: str = 'date'
    ) -> Dict[str, Any]:
        """
        Prepare data for modeling with train/val/test split, scaling, and VIF check.
        
        Returns:
            Dictionary with X_train, X_val, X_test, y_train, y_val, y_test, scaler, vif_report
        """
        # Ensure features exist
        available_features = [f for f in feature_cols if f in df.columns]
        if not available_features:
            return {"error": "No valid features found"}
        
        X = df[available_features].copy()
        y = df[target_col].copy() if target_col in df.columns else None
        
        if y is None:
            return {"error": f"Target column '{target_col}' not found"}
        
        # VIF check for multicollinearity
        if self.enable_vif_check:
            X, self.vif_report = self._check_multicollinearity(X)
            available_features = list(X.columns)
        
        # Train/val/test split
        if time_aware and time_col in df.columns:
            # Chronological split
            df_sorted = df.sort_values(time_col)
            n = len(df_sorted)
            train_end = int(n * (1 - test_size - val_size))
            val_end = int(n * (1 - test_size))
            
            X_train = X.iloc[:train_end]
            X_val = X.iloc[train_end:val_end]
            X_test = X.iloc[val_end:]
            y_train = y.iloc[:train_end]
            y_val = y.iloc[train_end:val_end]
            y_test = y.iloc[val_end:]
        else:
            # Stratified split
            stratify = df[stratify_col] if stratify_col and stratify_col in df.columns else None
            
            X_temp, X_test, y_temp, y_test = train_test_split(
                X, y, test_size=test_size, stratify=stratify, random_state=42
            )
            
            val_ratio = val_size / (1 - test_size)
            stratify_temp = df.loc[X_temp.index, stratify_col] if stratify_col and stratify_col in df.columns else None
            
            X_train, X_val, y_train, y_val = train_test_split(
                X_temp, y_temp, test_size=val_ratio, stratify=stratify_temp, random_state=42
            )
        
        # Feature scaling (fit on train only)
        if self.enable_scaling:
            self.scaler = StandardScaler()
            X_train_scaled = pd.DataFrame(
                self.scaler.fit_transform(X_train),
                columns=available_features,
                index=X_train.index
            )
            X_val_scaled = pd.DataFrame(
                self.scaler.transform(X_val),
                columns=available_features,
                index=X_val.index
            )
            X_test_scaled = pd.DataFrame(
                self.scaler.transform(X_test),
                columns=available_features,
                index=X_test.index
            )
        else:
            X_train_scaled, X_val_scaled, X_test_scaled = X_train, X_val, X_test
        
        return {
            "X_train": X_train_scaled,
            "X_val": X_val_scaled,
            "X_test": X_test_scaled,
            "y_train": y_train,
            "y_val": y_val,
            "y_test": y_test,
            "feature_names": available_features,
            "scaler": self.scaler,
            "vif_report": self.vif_report,
            "split_sizes": {
                "train": len(X_train),
                "val": len(X_val),
                "test": len(X_test)
            }
        }
    
    # ==========================================================================
    # SCHEMA STANDARDIZATION
    # ==========================================================================
    def _apply_platform_schema(self, df: pd.DataFrame, platform_col: str) -> pd.DataFrame:
        """Apply platform-specific field mappings to create unified schema."""
        df = df.copy()
        
        # First apply general column normalization
        df = normalize_column_names(df)
        
        # Find platform column
        actual_platform_col = find_column(df, platform_col) or platform_col
        
        if actual_platform_col not in df.columns:
            logger.debug("No platform column found, applying general mappings only")
            return df
        
        # Get unique platforms
        platforms = df[actual_platform_col].dropna().str.lower().unique()
        
        for platform in platforms:
            if platform not in self.PLATFORM_FIELD_MAPPINGS:
                continue
                
            mappings = self.PLATFORM_FIELD_MAPPINGS[platform]
            platform_mask = df[actual_platform_col].str.lower() == platform
            
            for source_col, target_col in mappings.items():
                source_actual = find_column(df, source_col)
                if source_actual and source_actual in df.columns:
                    if target_col not in df.columns:
                        df[target_col] = np.nan
                    df.loc[platform_mask, target_col] = df.loc[platform_mask, source_actual]
                    self.column_mappings[f"{platform}.{source_col}"] = target_col
        
        logger.info(f"Applied platform schema mappings: {len(self.column_mappings)} fields mapped")
        return df
    
    # ==========================================================================
    # DATA CLEANING
    # ==========================================================================
    def _clean_data(
        self,
        df: pd.DataFrame,
        time_col: str,
        group_col: Optional[str],
        platform_col: str
    ) -> pd.DataFrame:
        """Clean data: duplicates, zero-spend, date validation."""
        df = df.copy()
        initial_rows = len(df)
        
        # Resolve column names
        date_col = find_column(df, time_col) or time_col
        campaign_col = find_column(df, group_col) if group_col else None
        plat_col = find_column(df, platform_col) or platform_col
        
        # 1. Remove duplicates on (date, campaign_id, platform)
        dedup_cols = [c for c in [date_col, campaign_col, plat_col] if c and c in df.columns]
        if dedup_cols:
            before_dedup = len(df)
            df = df.drop_duplicates(subset=dedup_cols, keep='last')
            logger.debug(f"Removed {before_dedup - len(df)} duplicate rows")
        
        # 2. Date validation
        if date_col and date_col in df.columns:
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
            invalid_dates = df[date_col].isna().sum()
            if invalid_dates > 0:
                logger.warning(f"Found {invalid_dates} rows with invalid dates")
        
        # 3. Handle zero-spend campaigns (flag, don't remove)
        spend_col = find_column(df, 'spend')
        if spend_col and spend_col in df.columns:
            df['_zero_spend'] = (df[spend_col] == 0) | (df[spend_col].isna())
            zero_spend_count = df['_zero_spend'].sum()
            logger.debug(f"Flagged {zero_spend_count} zero-spend rows")
        
        # 4. Platform validation
        valid_platforms = set(self.PLATFORM_FIELD_MAPPINGS.keys())
        if plat_col and plat_col in df.columns:
            df['_platform_valid'] = df[plat_col].str.lower().isin(valid_platforms)
        
        logger.info(f"Data cleaning: {initial_rows} -> {len(df)} rows")
        return df
    
    # ==========================================================================
    # MISSING VALUE HANDLING
    # ==========================================================================
    def _handle_missing_values(self, df: pd.DataFrame, target_col: Optional[str]) -> pd.DataFrame:
        """Apply variable-type-specific missing value strategies."""
        df = df.copy()
        self.missing_report = {}
        self.excluded_columns = []
        
        for col in df.columns:
            if col.startswith('_'):  # Skip internal columns
                continue
                
            missing_pct = df[col].isna().mean()
            self.missing_report[col] = round(missing_pct * 100, 2)
            
            # Determine variable type and strategy
            col_lower = col.lower()
            
            if col_lower in ['spend', 'cost']:
                strategy = 'zero'
                threshold = 0.30
            elif col_lower in ['impressions', 'clicks', 'reach']:
                strategy = 'forward_fill'
                threshold = 0.30
            elif col_lower in ['conversions', 'leads', 'purchases']:
                strategy = 'zero'
                threshold = 1.0  # Never drop
            elif col_lower in ['roas']:
                strategy = 'skip'
                threshold = 1.0
            elif col_lower in ['engagement', 'engagement_rate', 'video_views']:
                strategy = 'mean'
                threshold = 0.20
            elif df[col].dtype == 'object':
                strategy = 'mode'
                threshold = 0.50
            else:
                strategy = 'zero'
                threshold = self.missing_threshold
            
            # Check if should be excluded
            if missing_pct > threshold:
                if target_col and col.lower() == target_col.lower():
                    logger.warning(f"'{col}' has {missing_pct*100:.1f}% missing but is target (protected)")
                else:
                    self.excluded_columns.append(col)
                    logger.warning(f"'{col}' excluded: {missing_pct*100:.1f}% missing > {threshold*100:.0f}%")
                    continue
            
            # Apply strategy
            if strategy == 'zero':
                df[col] = df[col].fillna(0)
            elif strategy == 'forward_fill':
                df[col] = df[col].ffill().fillna(0)
            elif strategy == 'mean' and pd.api.types.is_numeric_dtype(df[col]):
                df[col] = df[col].fillna(df[col].mean())
            elif strategy == 'mode' and df[col].dtype == 'object':
                mode_val = df[col].mode().iloc[0] if not df[col].mode().empty else 'Unknown'
                df[col] = df[col].fillna(mode_val)
        
        return df
    
    # ==========================================================================
    # OUTLIER DETECTION
    # ==========================================================================
    def _handle_outliers(self, df: pd.DataFrame) -> pd.DataFrame:
        """Detect and cap outliers at 99th percentile."""
        df = df.copy()
        self.outlier_report = {}
        
        for metric in self.OUTLIER_CHECK_METRICS:
            col = find_column(df, metric)
            if not col or col not in df.columns:
                continue
            
            if not pd.api.types.is_numeric_dtype(df[col]):
                continue
            
            if self.outlier_method == 'iqr':
                Q1 = df[col].quantile(0.25)
                Q3 = df[col].quantile(0.75)
                IQR = Q3 - Q1
                lower = Q1 - 1.5 * IQR
                upper = Q3 + 1.5 * IQR
                outliers = (df[col] < lower) | (df[col] > upper)
            else:  # zscore
                z_scores = np.abs((df[col] - df[col].mean()) / df[col].std())
                outliers = z_scores > 3
            
            outlier_count = outliers.sum()
            self.outlier_report[col] = int(outlier_count)
            
            if outlier_count > 0:
                cap_value = df[col].quantile(self.cap_percentile)
                df.loc[outliers, col] = df.loc[outliers, col].clip(upper=cap_value)
                logger.debug(f"Capped {outlier_count} outliers in '{col}' at {cap_value:.2f}")
        
        return df
    
    # ==========================================================================
    # FEATURE ENGINEERING
    # ==========================================================================
    def _engineer_features(self, df: pd.DataFrame, time_col: str, platform_col: str) -> pd.DataFrame:
        """Create derived features, temporal features, and platform indicators."""
        df = df.copy()
        self.feature_names = []
        
        # Resolve columns
        spend_col = find_column(df, 'spend')
        impr_col = find_column(df, 'impressions')
        clicks_col = find_column(df, 'clicks')
        conv_col = find_column(df, 'conversions')
        date_col = find_column(df, time_col) or time_col
        plat_col = find_column(df, platform_col) or platform_col
        
        # 1. Derived Ratios
        if impr_col and impr_col in df.columns:
            impr = df[impr_col].replace(0, np.nan)
            
            if clicks_col and clicks_col in df.columns:
                df['ctr'] = (df[clicks_col] / impr * 100).fillna(0)
                self.feature_names.append('ctr')
            
            if spend_col and spend_col in df.columns:
                df['cpm'] = (df[spend_col] / impr * 1000).fillna(0)
                self.feature_names.append('cpm')
        
        if clicks_col and clicks_col in df.columns:
            clicks = df[clicks_col].replace(0, np.nan)
            
            if spend_col and spend_col in df.columns:
                df['cpc'] = (df[spend_col] / clicks).fillna(0)
                self.feature_names.append('cpc')
            
            if conv_col and conv_col in df.columns:
                df['conversion_rate'] = (df[conv_col] / clicks * 100).fillna(0)
                self.feature_names.append('conversion_rate')
        
        if spend_col and conv_col and spend_col in df.columns and conv_col in df.columns:
            spend = df[spend_col].replace(0, np.nan)
            df['cpa'] = (df[spend_col] / df[conv_col].replace(0, np.nan)).fillna(0)
            self.feature_names.append('cpa')
        
        # 2. Temporal Features
        if date_col and date_col in df.columns:
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
            
            df['day_of_week'] = df[date_col].dt.dayofweek + 1  # Monday=1
            df['week_of_month'] = (df[date_col].dt.day - 1) // 7 + 1
            df['is_weekend'] = (df[date_col].dt.dayofweek >= 5).astype(int)
            df['month'] = df[date_col].dt.month
            
            self.feature_names.extend(['day_of_week', 'week_of_month', 'is_weekend', 'month'])
        
        # 3. Platform Indicators (one-hot encoding)
        if plat_col and plat_col in df.columns:
            platform_dummies = pd.get_dummies(df[plat_col], prefix='platform', dummy_na=False)
            df = pd.concat([df, platform_dummies], axis=1)
            self.feature_names.extend(list(platform_dummies.columns))
        
        logger.info(f"Feature engineering: created {len(self.feature_names)} features")
        return df
    
    # ==========================================================================
    # MULTICOLLINEARITY CHECK
    # ==========================================================================
    def _check_multicollinearity(self, X: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, float]]:
        """Check VIF and drop features with VIF > threshold."""
        try:
            from statsmodels.stats.outliers_influence import variance_inflation_factor
        except ImportError:
            logger.warning("statsmodels not installed, skipping VIF check")
            return X, {}
        
        X_numeric = X.select_dtypes(include=[np.number]).copy()
        X_numeric = X_numeric.replace([np.inf, -np.inf], np.nan).fillna(0)
        
        if X_numeric.shape[1] < 2:
            return X, {}
        
        vif_data = {}
        dropped = []
        
        # Iteratively check and drop high VIF features
        while True:
            if X_numeric.shape[1] < 2:
                break
                
            vif_values = []
            for i in range(X_numeric.shape[1]):
                try:
                    vif = variance_inflation_factor(X_numeric.values, i)
                    vif_values.append(vif)
                except:
                    vif_values.append(0)
            
            max_vif_idx = np.argmax(vif_values)
            max_vif = vif_values[max_vif_idx]
            
            if max_vif > self.vif_threshold:
                col_to_drop = X_numeric.columns[max_vif_idx]
                dropped.append(col_to_drop)
                vif_data[col_to_drop] = max_vif
                X_numeric = X_numeric.drop(columns=[col_to_drop])
                logger.debug(f"Dropped '{col_to_drop}' with VIF={max_vif:.2f}")
            else:
                break
        
        # Store final VIF values
        for i, col in enumerate(X_numeric.columns):
            try:
                vif_data[col] = variance_inflation_factor(X_numeric.values, i)
            except:
                vif_data[col] = 0
        
        if dropped:
            logger.info(f"VIF check: dropped {len(dropped)} features with VIF > {self.vif_threshold}")
        
        return X[X_numeric.columns], vif_data
    
    def get_full_report(self) -> Dict[str, Any]:
        """Get comprehensive report of all data prep steps."""
        return {
            "missing": {
                "excluded_columns": self.excluded_columns,
                "missing_percentages": self.missing_report
            },
            "outliers": self.outlier_report,
            "multicollinearity": self.vif_report,
            "schema_mappings": self.column_mappings,
            "features_created": self.feature_names
        }


# Singleton
_data_prep_layer: Optional[DataPrepLayer] = None

def get_data_prep_layer() -> DataPrepLayer:
    """Get or create the DataPrepLayer singleton."""
    global _data_prep_layer
    if _data_prep_layer is None:
        _data_prep_layer = DataPrepLayer()
    return _data_prep_layer

"""
Model Validation Suite

Comprehensive validation and quality checks for regression models:
- Data quality checks (pre-modeling)
- Model performance checks
- Residual diagnostics
- Stability checks
- Business logic validation
- Production readiness checklist
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from loguru import logger

from scipy import stats
from sklearn.model_selection import cross_val_score, KFold
from sklearn.metrics import r2_score, mean_squared_error
import statsmodels.api as sm
from statsmodels.stats.diagnostic import het_breuschpagan
from statsmodels.stats.outliers_influence import variance_inflation_factor


@dataclass
class ValidationResult:
    """Result of a single validation check."""
    check_name: str
    passed: bool
    status: str  # 'good', 'warning', 'failed'
    value: Any
    threshold: Any
    message: str
    action: Optional[str] = None


@dataclass
class ValidationReport:
    """Complete validation report."""
    data_quality: List[ValidationResult]
    model_performance: List[ValidationResult]
    residual_diagnostics: List[ValidationResult]
    stability_checks: List[ValidationResult]
    business_logic: List[ValidationResult]
    production_ready: bool
    summary: Dict[str, int]  # counts by status


class ModelValidationSuite:
    """
    Comprehensive validation suite for regression models.
    """
    
    # Data Quality Thresholds
    MISSING_DATA_THRESHOLD = 0.30
    DUPLICATE_THRESHOLD = 0.05
    OUTLIER_THRESHOLD = 0.01
    CORRELATION_THRESHOLD = 0.90
    
    # Performance Thresholds
    GOOD_R2 = 0.60
    WARNING_R2 = 0.40
    TRAIN_TEST_GAP_GOOD = 0.10
    TRAIN_TEST_GAP_WARNING = 0.20
    RMSE_GOOD_PCT = 0.15  # 15% of mean
    RMSE_WARNING_PCT = 0.30
    
    # Stability Thresholds
    COEF_STABILITY_THRESHOLD = 0.10  # 10% std
    CV_STABILITY_THRESHOLD = 0.05  # 5% std
    
    def __init__(self):
        self.results = ValidationReport(
            data_quality=[],
            model_performance=[],
            residual_diagnostics=[],
            stability_checks=[],
            business_logic=[],
            production_ready=False,
            summary={}
        )
    
    # ==========================================================================
    # 4.1 DATA QUALITY CHECKS
    # ==========================================================================
    
    def check_data_quality(
        self,
        df: pd.DataFrame,
        target_col: Optional[str] = None,
        key_cols: Optional[List[str]] = None
    ) -> List[ValidationResult]:
        """Run all data quality checks."""
        results = []
        
        # 1. Missing data
        results.append(self._check_missing_data(df))
        
        # 2. Duplicate rows
        results.append(self._check_duplicates(df, key_cols))
        
        # 3. Outliers
        results.extend(self._check_outliers(df))
        
        # 4. Zero variance
        results.extend(self._check_zero_variance(df))
        
        # 5. High correlation
        results.extend(self._check_high_correlation(df))
        
        # 6. Data leakage (if target provided)
        if target_col:
            results.extend(self._check_data_leakage(df, target_col))
        
        self.results.data_quality = results
        return results
    
    def _check_missing_data(self, df: pd.DataFrame) -> ValidationResult:
        """Check for excessive missing data."""
        missing_pct = df.isnull().mean()
        max_missing = missing_pct.max()
        worst_col = missing_pct.idxmax() if max_missing > 0 else None
        
        if max_missing > self.MISSING_DATA_THRESHOLD:
            return ValidationResult(
                check_name='Missing Data',
                passed=False,
                status='failed',
                value=f"{max_missing*100:.1f}%",
                threshold=f"{self.MISSING_DATA_THRESHOLD*100}%",
                message=f"Column '{worst_col}' has {max_missing*100:.1f}% missing",
                action="Drop column or impute"
            )
        elif max_missing > 0.10:
            return ValidationResult(
                check_name='Missing Data',
                passed=True,
                status='warning',
                value=f"{max_missing*100:.1f}%",
                threshold=f"{self.MISSING_DATA_THRESHOLD*100}%",
                message=f"Column '{worst_col}' has {max_missing*100:.1f}% missing",
                action="Consider imputation"
            )
        return ValidationResult(
            check_name='Missing Data',
            passed=True,
            status='good',
            value=f"{max_missing*100:.1f}%",
            threshold=f"{self.MISSING_DATA_THRESHOLD*100}%",
            message="Missing data within acceptable limits"
        )
    
    def _check_duplicates(self, df: pd.DataFrame, key_cols: Optional[List[str]]) -> ValidationResult:
        """Check for duplicate rows."""
        if key_cols:
            dup_count = df.duplicated(subset=key_cols).sum()
        else:
            dup_count = df.duplicated().sum()
        
        dup_pct = dup_count / len(df)
        
        if dup_pct > self.DUPLICATE_THRESHOLD:
            return ValidationResult(
                check_name='Duplicate Rows',
                passed=False,
                status='failed',
                value=f"{dup_pct*100:.1f}% ({dup_count} rows)",
                threshold=f"{self.DUPLICATE_THRESHOLD*100}%",
                message=f"Found {dup_count} duplicate rows",
                action="Investigate and deduplicate"
            )
        return ValidationResult(
            check_name='Duplicate Rows',
            passed=True,
            status='good',
            value=f"{dup_pct*100:.1f}% ({dup_count} rows)",
            threshold=f"{self.DUPLICATE_THRESHOLD*100}%",
            message="Duplicate count acceptable"
        )
    
    def _check_outliers(self, df: pd.DataFrame) -> List[ValidationResult]:
        """Check for outliers using IQR."""
        results = []
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        
        for col in numeric_cols:
            Q1 = df[col].quantile(0.25)
            Q3 = df[col].quantile(0.75)
            IQR = Q3 - Q1
            
            outlier_mask = (df[col] < Q1 - 1.5 * IQR) | (df[col] > Q3 + 1.5 * IQR)
            outlier_pct = outlier_mask.mean()
            
            if outlier_pct > self.OUTLIER_THRESHOLD:
                results.append(ValidationResult(
                    check_name=f'Outliers ({col})',
                    passed=True,
                    status='warning',
                    value=f"{outlier_pct*100:.1f}%",
                    threshold=f"{self.OUTLIER_THRESHOLD*100}%",
                    message=f"Column has {outlier_pct*100:.1f}% outliers",
                    action="Cap at 99th percentile"
                ))
        
        if not results:
            results.append(ValidationResult(
                check_name='Outliers',
                passed=True,
                status='good',
                value='< 1%',
                threshold=f"{self.OUTLIER_THRESHOLD*100}%",
                message="Outliers within acceptable limits"
            ))
        
        return results
    
    def _check_zero_variance(self, df: pd.DataFrame) -> List[ValidationResult]:
        """Check for zero variance columns."""
        results = []
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        
        zero_var_cols = [col for col in numeric_cols if df[col].std() == 0]
        
        if zero_var_cols:
            for col in zero_var_cols:
                results.append(ValidationResult(
                    check_name=f'Zero Variance ({col})',
                    passed=False,
                    status='failed',
                    value='std = 0',
                    threshold='std > 0',
                    message=f"Column '{col}' has no variance",
                    action="Drop column"
                ))
        else:
            results.append(ValidationResult(
                check_name='Zero Variance',
                passed=True,
                status='good',
                value='All cols have variance',
                threshold='std > 0',
                message="No zero variance columns"
            ))
        
        return results
    
    def _check_high_correlation(self, df: pd.DataFrame) -> List[ValidationResult]:
        """Check for highly correlated features."""
        results = []
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        
        if len(numeric_cols) < 2:
            return results
        
        corr_matrix = df[numeric_cols].corr().abs()
        upper = corr_matrix.where(np.triu(np.ones(corr_matrix.shape), k=1).astype(bool))
        
        high_corr_pairs = []
        for col in upper.columns:
            high_corr = upper[col][upper[col] > self.CORRELATION_THRESHOLD]
            for idx in high_corr.index:
                high_corr_pairs.append((idx, col, high_corr[idx]))
        
        if high_corr_pairs:
            for pair in high_corr_pairs[:3]:  # Top 3
                results.append(ValidationResult(
                    check_name=f'High Correlation',
                    passed=True,
                    status='warning',
                    value=f"r = {pair[2]:.2f}",
                    threshold=f"r < {self.CORRELATION_THRESHOLD}",
                    message=f"'{pair[0]}' and '{pair[1]}' highly correlated",
                    action="Consider dropping one"
                ))
        else:
            results.append(ValidationResult(
                check_name='High Correlation',
                passed=True,
                status='good',
                value='No high correlations',
                threshold=f"r < {self.CORRELATION_THRESHOLD}",
                message="No problematic correlations"
            ))
        
        return results
    
    def _check_data_leakage(self, df: pd.DataFrame, target_col: str) -> List[ValidationResult]:
        """Check for potential data leakage."""
        results = []
        numeric_cols = [c for c in df.select_dtypes(include=[np.number]).columns if c != target_col]
        
        if target_col in df.columns:
            for col in numeric_cols:
                corr = df[col].corr(df[target_col])
                if abs(corr) > 0.95:
                    results.append(ValidationResult(
                        check_name=f'Data Leakage ({col})',
                        passed=False,
                        status='failed',
                        value=f"r = {corr:.2f}",
                        threshold='r < 0.95',
                        message=f"'{col}' may leak target information",
                        action="Remove feature"
                    ))
        
        if not results:
            results.append(ValidationResult(
                check_name='Data Leakage',
                passed=True,
                status='good',
                value='No leakage detected',
                threshold='r < 0.95 with target',
                message="No obvious data leakage"
            ))
        
        return results
    
    # ==========================================================================
    # 4.2 MODEL PERFORMANCE CHECKS
    # ==========================================================================
    
    def check_model_performance(
        self,
        y_train: np.ndarray,
        y_test: np.ndarray,
        train_pred: np.ndarray,
        test_pred: np.ndarray,
        residuals: np.ndarray,
        X_test: Optional[pd.DataFrame] = None
    ) -> List[ValidationResult]:
        """Run model performance checks."""
        results = []
        
        # Calculate metrics
        r2_train = r2_score(y_train, train_pred)
        r2_test = r2_score(y_test, test_pred)
        rmse_test = np.sqrt(mean_squared_error(y_test, test_pred))
        y_mean = np.mean(y_test)
        
        # 1. R² test
        if r2_test > self.GOOD_R2:
            status = 'good'
        elif r2_test > self.WARNING_R2:
            status = 'warning'
        else:
            status = 'failed'
        
        results.append(ValidationResult(
            check_name='R² (Test)',
            passed=r2_test > self.WARNING_R2,
            status=status,
            value=f"{r2_test:.3f}",
            threshold=f"> {self.GOOD_R2}",
            message=f"Model explains {r2_test*100:.1f}% of variance",
            action="Feature engineering" if status == 'failed' else None
        ))
        
        # 2. Train-test gap
        gap = r2_train - r2_test
        if gap < self.TRAIN_TEST_GAP_GOOD:
            status = 'good'
        elif gap < self.TRAIN_TEST_GAP_WARNING:
            status = 'warning'
        else:
            status = 'failed'
        
        results.append(ValidationResult(
            check_name='Train-Test Gap',
            passed=gap < self.TRAIN_TEST_GAP_WARNING,
            status=status,
            value=f"{gap:.3f}",
            threshold=f"< {self.TRAIN_TEST_GAP_GOOD}",
            message=f"R² gap (train - test) = {gap:.3f}",
            action="Increase regularization" if status == 'failed' else None
        ))
        
        # 3. RMSE relative to mean
        rmse_pct = rmse_test / y_mean if y_mean != 0 else float('inf')
        if rmse_pct < self.RMSE_GOOD_PCT:
            status = 'good'
        elif rmse_pct < self.RMSE_WARNING_PCT:
            status = 'warning'
        else:
            status = 'failed'
        
        results.append(ValidationResult(
            check_name='RMSE (% of mean)',
            passed=rmse_pct < self.RMSE_WARNING_PCT,
            status=status,
            value=f"{rmse_pct*100:.1f}%",
            threshold=f"< {self.RMSE_GOOD_PCT*100}%",
            message=f"RMSE = {rmse_test:.2f} ({rmse_pct*100:.1f}% of mean)",
            action="Try non-linear model" if status == 'failed' else None
        ))
        
        # 4. Residual normality (Shapiro-Wilk)
        sample = residuals[:5000] if len(residuals) > 5000 else residuals
        try:
            stat, p_value = stats.shapiro(sample)
            if p_value > 0.05:
                status = 'good'
            elif p_value > 0.01:
                status = 'warning'
            else:
                status = 'failed'
            
            results.append(ValidationResult(
                check_name='Residual Normality',
                passed=p_value > 0.01,
                status=status,
                value=f"p = {p_value:.4f}",
                threshold='p > 0.05',
                message="Residuals appear normal" if status == 'good' else "Residuals may not be normal",
                action="Transform target" if status == 'failed' else None
            ))
        except:
            pass
        
        # 5. Homoscedasticity (Breusch-Pagan)
        if X_test is not None:
            try:
                X_with_const = sm.add_constant(X_test.fillna(0))
                bp_stat, bp_p, _, _ = het_breuschpagan(residuals, X_with_const)
                
                if bp_p > 0.05:
                    status = 'good'
                elif bp_p > 0.01:
                    status = 'warning'
                else:
                    status = 'failed'
                
                results.append(ValidationResult(
                    check_name='Homoscedasticity',
                    passed=bp_p > 0.01,
                    status=status,
                    value=f"p = {bp_p:.4f}",
                    threshold='p > 0.05',
                    message="Constant variance" if status == 'good' else "Heteroscedasticity detected",
                    action="Transform features" if status == 'failed' else None
                ))
            except:
                pass
        
        self.results.model_performance = results
        return results
    
    # ==========================================================================
    # 4.3 RESIDUAL DIAGNOSTICS
    # ==========================================================================
    
    def check_residual_diagnostics(
        self,
        residuals: np.ndarray,
        predictions: np.ndarray,
        features: Optional[pd.DataFrame] = None,
        platforms: Optional[pd.Series] = None,
        dates: Optional[pd.Series] = None
    ) -> List[ValidationResult]:
        """Run residual diagnostic checks."""
        results = []
        
        # 1. Residual vs Predicted (check for funnel pattern)
        correlation = np.corrcoef(predictions, np.abs(residuals))[0, 1]
        if abs(correlation) < 0.2:
            status = 'good'
        elif abs(correlation) < 0.4:
            status = 'warning'
        else:
            status = 'failed'
        
        results.append(ValidationResult(
            check_name='Residual vs Predicted',
            passed=abs(correlation) < 0.4,
            status=status,
            value=f"r = {correlation:.3f}",
            threshold='|r| < 0.2',
            message="Random scatter (good)" if status == 'good' else "Pattern detected in residuals",
            action="Check for non-linearity" if status == 'failed' else None
        ))
        
        # 2. Residual skewness
        skewness = stats.skew(residuals)
        if abs(skewness) < 0.5:
            status = 'good'
        elif abs(skewness) < 1.0:
            status = 'warning'
        else:
            status = 'failed'
        
        results.append(ValidationResult(
            check_name='Residual Skewness',
            passed=abs(skewness) < 1.0,
            status=status,
            value=f"{skewness:.3f}",
            threshold='|skew| < 0.5',
            message="Symmetric residuals" if status == 'good' else "Skewed residuals"
        ))
        
        # 3. Platform bias (if available)
        if platforms is not None:
            platform_means = pd.DataFrame({'residual': residuals, 'platform': platforms}).groupby('platform')['residual'].mean()
            max_bias = platform_means.abs().max()
            biased_platform = platform_means.abs().idxmax()
            
            if max_bias < residuals.std() * 0.3:
                status = 'good'
            elif max_bias < residuals.std() * 0.5:
                status = 'warning'
            else:
                status = 'failed'
            
            results.append(ValidationResult(
                check_name='Platform Bias',
                passed=max_bias < residuals.std() * 0.5,
                status=status,
                value=f"{biased_platform}: {max_bias:.2f}",
                threshold='< 30% of residual std',
                message="No platform bias" if status == 'good' else f"Bias detected for {biased_platform}"
            ))
        
        self.results.residual_diagnostics = results
        return results
    
    # ==========================================================================
    # 4.4 STABILITY CHECKS
    # ==========================================================================
    
    def check_stability(
        self,
        model_class,
        X: pd.DataFrame,
        y: pd.Series,
        n_seeds: int = 5,
        n_cv_folds: int = 10
    ) -> List[ValidationResult]:
        """Run stability checks."""
        results = []
        
        # 1. Coefficient stability across random seeds
        cv_scores = []
        kf = KFold(n_splits=min(n_cv_folds, len(X)), shuffle=True, random_state=42)
        
        try:
            from sklearn.linear_model import Ridge
            model = Ridge(alpha=1.0)
            scores = cross_val_score(model, X.fillna(0), y.fillna(0), cv=kf, scoring='r2')
            
            cv_mean = scores.mean()
            cv_std = scores.std()
            cv_relative_std = cv_std / cv_mean if cv_mean != 0 else float('inf')
            
            if cv_relative_std < self.CV_STABILITY_THRESHOLD:
                status = 'good'
            elif cv_relative_std < self.CV_STABILITY_THRESHOLD * 2:
                status = 'warning'
            else:
                status = 'failed'
            
            results.append(ValidationResult(
                check_name='Cross-Validation Stability',
                passed=cv_relative_std < self.CV_STABILITY_THRESHOLD * 2,
                status=status,
                value=f"CV std = {cv_std:.4f} ({cv_relative_std*100:.1f}%)",
                threshold=f"< {self.CV_STABILITY_THRESHOLD*100}% of mean",
                message=f"CV R² = {cv_mean:.3f} ± {cv_std:.3f}",
                action="Check for outliers" if status == 'failed' else None
            ))
        except Exception as e:
            logger.warning(f"CV stability check failed: {e}")
        
        self.results.stability_checks = results
        return results
    
    # ==========================================================================
    # 4.5 BUSINESS LOGIC VALIDATION
    # ==========================================================================
    
    def check_business_logic(
        self,
        coefficients: Dict[str, float],
        expected_signs: Optional[Dict[str, str]] = None
    ) -> List[ValidationResult]:
        """Run business logic validation."""
        results = []
        
        # Default expected signs for marketing
        if expected_signs is None:
            expected_signs = {
                'spend': 'positive',
                'impressions': 'positive',
                'clicks': 'positive',
                'ctr': 'positive',
                'conversions': 'positive'
            }
        
        # 1. Sign checks
        sign_violations = []
        for feature, expected in expected_signs.items():
            if feature in coefficients:
                coef = coefficients[feature]
                actual_sign = 'positive' if coef > 0 else 'negative'
                if actual_sign != expected:
                    sign_violations.append((feature, expected, actual_sign, coef))
        
        if sign_violations:
            for feat, expected, actual, coef in sign_violations:
                results.append(ValidationResult(
                    check_name=f'Sign Check ({feat})',
                    passed=False,
                    status='warning',
                    value=f"{actual} ({coef:.4f})",
                    threshold=expected,
                    message=f"'{feat}' has unexpected {actual} coefficient",
                    action="Review feature or data quality"
                ))
        else:
            results.append(ValidationResult(
                check_name='Sign Checks',
                passed=True,
                status='good',
                value='All signs as expected',
                threshold='Match business intuition',
                message="Coefficients align with business logic"
            ))
        
        # 2. Magnitude check
        if 'spend' in coefficients:
            spend_coef = coefficients['spend']
            if abs(spend_coef) > 1.0:
                results.append(ValidationResult(
                    check_name='Magnitude Check (spend)',
                    passed=True,
                    status='warning',
                    value=f"{spend_coef:.4f}",
                    threshold='Reasonable range',
                    message="Spend coefficient seems high - verify units"
                ))
        
        self.results.business_logic = results
        return results
    
    # ==========================================================================
    # 4.6 PRODUCTION READINESS
    # ==========================================================================
    
    def generate_report(self) -> ValidationReport:
        """Generate final validation report."""
        # Count by status
        all_checks = (
            self.results.data_quality +
            self.results.model_performance +
            self.results.residual_diagnostics +
            self.results.stability_checks +
            self.results.business_logic
        )
        
        summary = {'good': 0, 'warning': 0, 'failed': 0}
        for check in all_checks:
            summary[check.status] = summary.get(check.status, 0) + 1
        
        # Production ready if no failures
        self.results.production_ready = summary.get('failed', 0) == 0
        self.results.summary = summary
        
        return self.results
    
    def print_report(self):
        """Print validation report."""
        report = self.generate_report()
        
        print("\n" + "="*60)
        print("MODEL VALIDATION REPORT")
        print("="*60)
        
        sections = [
            ("Data Quality", report.data_quality),
            ("Model Performance", report.model_performance),
            ("Residual Diagnostics", report.residual_diagnostics),
            ("Stability", report.stability_checks),
            ("Business Logic", report.business_logic)
        ]
        
        for section_name, checks in sections:
            if checks:
                print(f"\n{section_name}:")
                print("-"*40)
                for check in checks:
                    emoji = '✅' if check.status == 'good' else '⚠️' if check.status == 'warning' else '❌'
                    print(f"  {emoji} {check.check_name}: {check.value}")
                    if check.action:
                        print(f"     → {check.action}")
        
        print("\n" + "="*60)
        print(f"SUMMARY: ✅ {report.summary.get('good', 0)} | ⚠️ {report.summary.get('warning', 0)} | ❌ {report.summary.get('failed', 0)}")
        print(f"PRODUCTION READY: {'✅ YES' if report.production_ready else '❌ NO'}")
        print("="*60)


def run_full_validation(
    df: pd.DataFrame,
    y_train: np.ndarray,
    y_test: np.ndarray,
    train_pred: np.ndarray,
    test_pred: np.ndarray,
    target_col: str = None,
    print_report: bool = True
) -> ValidationReport:
    """Run complete validation suite."""
    suite = ModelValidationSuite()
    
    # Data quality
    suite.check_data_quality(df, target_col)
    
    # Model performance
    residuals = y_test - test_pred
    suite.check_model_performance(y_train, y_test, train_pred, test_pred, residuals)
    
    # Residual diagnostics
    suite.check_residual_diagnostics(residuals, test_pred)
    
    if print_report:
        suite.print_report()
    
    return suite.generate_report()

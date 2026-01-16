"""
OLS (Ordinary Least Squares) Regression Model

Production implementation with full statistical diagnostics:
- Coefficients with standard errors and p-values
- R², Adjusted R², AIC, BIC
- Validation checks: Shapiro-Wilk, Breusch-Pagan, Durbin-Watson, VIF
- Warning flags for model quality issues
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from loguru import logger

import statsmodels.api as sm
from statsmodels.stats.diagnostic import het_breuschpagan
from statsmodels.stats.stattools import durbin_watson
from statsmodels.stats.outliers_influence import variance_inflation_factor
from scipy import stats


@dataclass
class OLSResult:
    """Result container for OLS regression."""
    success: bool
    coefficients: List[Dict[str, Any]]
    metrics: Dict[str, float]
    diagnostics: Dict[str, Any]
    warnings: List[Dict[str, str]]
    residuals: Optional[np.ndarray] = None
    predictions: Optional[np.ndarray] = None


class OLSRegressionModel:
    """
    OLS Regression with comprehensive statistical output.
    
    Features:
    - Coefficient estimates with standard errors and p-values
    - Model fit metrics: R², Adjusted R², AIC, BIC, F-statistic
    - Residual diagnostics: Shapiro-Wilk, Breusch-Pagan, Durbin-Watson
    - Multicollinearity check: VIF
    - Automatic warning generation
    """
    
    # Thresholds for warnings
    R2_LOW_THRESHOLD = 0.30
    PVALUE_THRESHOLD = 0.05
    VIF_THRESHOLD = 10.0
    DURBIN_WATSON_LOW = 1.5
    DURBIN_WATSON_HIGH = 2.5
    
    def __init__(
        self,
        add_constant: bool = True,
        significance_level: float = 0.05
    ):
        """
        Initialize OLS model.
        
        Args:
            add_constant: Whether to add intercept term
            significance_level: Alpha for significance testing (default 0.05)
        """
        self.add_constant = add_constant
        self.significance_level = significance_level
        self.model = None
        self.results = None
        self.feature_names: List[str] = []
        
    def fit(
        self,
        X: pd.DataFrame,
        y: pd.Series,
        feature_names: Optional[List[str]] = None
    ) -> OLSResult:
        """
        Fit OLS model and compute all diagnostics.
        
        Args:
            X: Feature matrix (should be scaled)
            y: Target variable
            feature_names: Optional list of feature names
            
        Returns:
            OLSResult with coefficients, metrics, diagnostics, and warnings
        """
        try:
            # Store feature names
            self.feature_names = feature_names or list(X.columns)
            
            # Prepare data
            X_clean = X.fillna(0).replace([np.inf, -np.inf], 0)
            y_clean = y.fillna(0)
            
            # Add constant for intercept
            if self.add_constant:
                X_with_const = sm.add_constant(X_clean)
            else:
                X_with_const = X_clean
            
            # Fit OLS model
            self.model = sm.OLS(y_clean, X_with_const)
            self.results = self.model.fit()
            
            # Extract coefficients
            coefficients = self._extract_coefficients()
            
            # Compute metrics
            metrics = self._compute_metrics()
            
            # Run diagnostics
            diagnostics = self._run_diagnostics(X_clean, y_clean)
            
            # Generate warnings
            warnings = self._generate_warnings(coefficients, metrics, diagnostics)
            
            logger.info(f"OLS fit complete: R²={metrics['r2']:.4f}, Adj R²={metrics['adj_r2']:.4f}")
            
            return OLSResult(
                success=True,
                coefficients=coefficients,
                metrics=metrics,
                diagnostics=diagnostics,
                warnings=warnings,
                residuals=self.results.resid.values,
                predictions=self.results.fittedvalues.values
            )
            
        except Exception as e:
            logger.error(f"OLS fit failed: {e}")
            return OLSResult(
                success=False,
                coefficients=[],
                metrics={},
                diagnostics={},
                warnings=[{'type': 'error', 'message': str(e)}]
            )
    
    def _extract_coefficients(self) -> List[Dict[str, Any]]:
        """Extract coefficients with standard errors, t-stats, and p-values."""
        coefficients = []
        
        params = self.results.params
        std_errors = self.results.bse
        t_values = self.results.tvalues
        p_values = self.results.pvalues
        conf_int = self.results.conf_int()
        
        for i, name in enumerate(params.index):
            is_significant = p_values.iloc[i] < self.significance_level
            
            coef_dict = {
                'feature': name,
                'coefficient': float(params.iloc[i]),
                'std_error': float(std_errors.iloc[i]),
                't_statistic': float(t_values.iloc[i]),
                'p_value': float(p_values.iloc[i]),
                'ci_lower': float(conf_int.iloc[i, 0]),
                'ci_upper': float(conf_int.iloc[i, 1]),
                'significant': is_significant,
                'impact': 'positive' if params.iloc[i] > 0 else 'negative'
            }
            coefficients.append(coef_dict)
        
        # Sort by absolute coefficient value (excluding constant)
        non_const = [c for c in coefficients if c['feature'] != 'const']
        const = [c for c in coefficients if c['feature'] == 'const']
        non_const.sort(key=lambda x: abs(x['coefficient']), reverse=True)
        
        return const + non_const
    
    def _compute_metrics(self) -> Dict[str, float]:
        """Compute R², Adjusted R², AIC, BIC, F-statistic."""
        return {
            'r2': float(self.results.rsquared),
            'adj_r2': float(self.results.rsquared_adj),
            'aic': float(self.results.aic),
            'bic': float(self.results.bic),
            'f_statistic': float(self.results.fvalue),
            'f_pvalue': float(self.results.f_pvalue),
            'log_likelihood': float(self.results.llf),
            'n_observations': int(self.results.nobs),
            'df_model': int(self.results.df_model),
            'df_residual': int(self.results.df_resid),
            'mse': float(self.results.mse_resid),
            'rmse': float(np.sqrt(self.results.mse_resid))
        }
    
    def _run_diagnostics(self, X: pd.DataFrame, y: pd.Series) -> Dict[str, Any]:
        """Run residual and model diagnostics."""
        diagnostics = {}
        residuals = self.results.resid
        
        # 1. Shapiro-Wilk test for residual normality
        # (use sample if n > 5000 for performance)
        resid_sample = residuals if len(residuals) <= 5000 else residuals.sample(5000, random_state=42)
        try:
            shapiro_stat, shapiro_p = stats.shapiro(resid_sample)
            diagnostics['shapiro_wilk'] = {
                'statistic': float(shapiro_stat),
                'p_value': float(shapiro_p),
                'normal_residuals': shapiro_p > self.significance_level,
                'interpretation': 'Residuals appear normal' if shapiro_p > 0.05 else 'Residuals may not be normal'
            }
        except Exception as e:
            diagnostics['shapiro_wilk'] = {'error': str(e)}
        
        # 2. Breusch-Pagan test for homoscedasticity
        try:
            X_with_const = sm.add_constant(X) if self.add_constant else X
            bp_stat, bp_p, _, _ = het_breuschpagan(residuals, X_with_const)
            diagnostics['breusch_pagan'] = {
                'statistic': float(bp_stat),
                'p_value': float(bp_p),
                'homoscedastic': bp_p > self.significance_level,
                'interpretation': 'Constant variance (good)' if bp_p > 0.05 else 'Heteroscedasticity detected'
            }
        except Exception as e:
            diagnostics['breusch_pagan'] = {'error': str(e)}
        
        # 3. Durbin-Watson test for autocorrelation
        try:
            dw_stat = durbin_watson(residuals)
            # DW ~ 2 means no autocorrelation, < 1.5 positive, > 2.5 negative
            autocorr_status = 'none'
            if dw_stat < self.DURBIN_WATSON_LOW:
                autocorr_status = 'positive'
            elif dw_stat > self.DURBIN_WATSON_HIGH:
                autocorr_status = 'negative'
                
            diagnostics['durbin_watson'] = {
                'statistic': float(dw_stat),
                'autocorrelation': autocorr_status,
                'interpretation': f'DW={dw_stat:.2f}: {"No" if autocorr_status == "none" else autocorr_status.capitalize()} autocorrelation'
            }
        except Exception as e:
            diagnostics['durbin_watson'] = {'error': str(e)}
        
        # 4. VIF for multicollinearity
        try:
            vif_data = []
            X_numeric = X.select_dtypes(include=[np.number])
            
            for i, col in enumerate(X_numeric.columns):
                vif_val = variance_inflation_factor(X_numeric.values, i)
                vif_data.append({
                    'feature': col,
                    'vif': float(vif_val) if not np.isinf(vif_val) else 999.0,
                    'high_collinearity': vif_val > self.VIF_THRESHOLD
                })
            
            vif_data.sort(key=lambda x: x['vif'], reverse=True)
            high_vif_count = sum(1 for v in vif_data if v['high_collinearity'])
            
            diagnostics['vif'] = {
                'features': vif_data[:10],  # Top 10
                'high_vif_count': high_vif_count,
                'max_vif': max(v['vif'] for v in vif_data) if vif_data else 0,
                'interpretation': f'{high_vif_count} features with VIF > 10' if high_vif_count > 0 else 'No multicollinearity issues'
            }
        except Exception as e:
            diagnostics['vif'] = {'error': str(e)}
        
        # 5. Residual statistics
        diagnostics['residuals'] = {
            'mean': float(residuals.mean()),
            'std': float(residuals.std()),
            'min': float(residuals.min()),
            'max': float(residuals.max()),
            'skewness': float(stats.skew(residuals)),
            'kurtosis': float(stats.kurtosis(residuals))
        }
        
        return diagnostics
    
    def _generate_warnings(
        self,
        coefficients: List[Dict],
        metrics: Dict[str, float],
        diagnostics: Dict[str, Any]
    ) -> List[Dict[str, str]]:
        """Generate warnings based on model quality."""
        warnings = []
        
        # 1. Low R²
        if metrics.get('r2', 0) < self.R2_LOW_THRESHOLD:
            warnings.append({
                'type': 'fit',
                'severity': 'high',
                'message': f"Low R² ({metrics['r2']:.3f}): Model explains < 30% of variance. Consider feature engineering."
            })
        
        # 2. Many insignificant features
        non_const_coefs = [c for c in coefficients if c['feature'] != 'const']
        insignificant_count = sum(1 for c in non_const_coefs if not c['significant'])
        if len(non_const_coefs) > 0 and insignificant_count / len(non_const_coefs) > 0.5:
            warnings.append({
                'type': 'significance',
                'severity': 'medium',
                'message': f"{insignificant_count}/{len(non_const_coefs)} features have p > 0.05. Consider feature selection."
            })
        
        # 3. High VIF (multicollinearity)
        vif_info = diagnostics.get('vif', {})
        if vif_info.get('high_vif_count', 0) > 0:
            warnings.append({
                'type': 'multicollinearity',
                'severity': 'high',
                'message': f"High VIF detected ({vif_info.get('high_vif_count')} features > 10). Use Ridge/Lasso instead."
            })
        
        # 4. Non-normal residuals
        shapiro = diagnostics.get('shapiro_wilk', {})
        if not shapiro.get('normal_residuals', True):
            warnings.append({
                'type': 'normality',
                'severity': 'medium',
                'message': "Residuals may not be normal. Consider transforming target or using robust methods."
            })
        
        # 5. Heteroscedasticity
        bp = diagnostics.get('breusch_pagan', {})
        if not bp.get('homoscedastic', True):
            warnings.append({
                'type': 'heteroscedasticity',
                'severity': 'medium',
                'message': "Heteroscedasticity detected. Consider weighted regression or robust standard errors."
            })
        
        # 6. Autocorrelation
        dw = diagnostics.get('durbin_watson', {})
        if dw.get('autocorrelation', 'none') != 'none':
            warnings.append({
                'type': 'autocorrelation',
                'severity': 'medium',
                'message': f"{dw['autocorrelation'].capitalize()} autocorrelation in residuals. Consider time-series models."
            })
        
        # 7. Overall model significance
        if metrics.get('f_pvalue', 0) > self.significance_level:
            warnings.append({
                'type': 'model_significance',
                'severity': 'high',
                'message': "F-test not significant. Model may not be better than mean prediction."
            })
        
        return warnings
    
    def predict(self, X: pd.DataFrame) -> np.ndarray:
        """Make predictions using fitted model."""
        if self.results is None:
            raise ValueError("Model not fitted. Call fit() first.")
        
        X_clean = X.fillna(0).replace([np.inf, -np.inf], 0)
        if self.add_constant:
            X_clean = sm.add_constant(X_clean)
        
        return self.results.predict(X_clean)
    
    def summary(self) -> str:
        """Return statsmodels summary table as string."""
        if self.results is None:
            return "Model not fitted"
        return str(self.results.summary())
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert results to dictionary for API response."""
        if self.results is None:
            return {'success': False, 'error': 'Model not fitted'}
        
        result = self.fit.__wrapped__  # Get last result
        return {
            'success': True,
            'model_type': 'OLS',
            'coefficients': [
                {k: v for k, v in c.items() if k != 'significant'}
                for c in self._extract_coefficients()
            ],
            'metrics': self._compute_metrics(),
            'diagnostics': {
                k: {kk: vv for kk, vv in v.items() if kk != 'features'}
                if isinstance(v, dict) else v
                for k, v in self._run_diagnostics(
                    pd.DataFrame(), pd.Series()
                ).items()
            }
        }


def run_ols_analysis(
    X: pd.DataFrame,
    y: pd.Series,
    **kwargs
) -> OLSResult:
    """
    Convenience function to run OLS analysis.
    
    Args:
        X: Feature matrix
        y: Target variable
        **kwargs: Additional arguments for OLSRegressionModel
        
    Returns:
        OLSResult with all diagnostics
    """
    model = OLSRegressionModel(**kwargs)
    return model.fit(X, y)

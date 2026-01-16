"""
Time Series Forecasting
Prophet and ARIMA models for budget and performance forecasting
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)

# Try to import forecasting libraries
try:
    from prophet import Prophet
    PROPHET_AVAILABLE = True
except ImportError:
    PROPHET_AVAILABLE = False
    logger.info("Prophet not installed, prophet forecasting disabled")

try:
    from statsmodels.tsa.arima.model import ARIMA
    from statsmodels.tsa.stattools import adfuller
    ARIMA_AVAILABLE = True
except ImportError:
    ARIMA_AVAILABLE = False
    logger.info("statsmodels not installed, ARIMA disabled")


@dataclass
class ForecastResult:
    """Forecasting result"""
    dates: List[datetime]
    values: List[float]
    lower_bound: List[float]
    upper_bound: List[float]
    model_type: str
    metrics: Dict[str, float]
    trend: str  # increasing, decreasing, stable


class TimeSeriesForecaster:
    """
    Time series forecasting for marketing metrics
    
    Models:
    - Prophet: Best for seasonal data with holidays
    - ARIMA: Best for stationary data with trends
    - Moving Average: Simple baseline
    
    Usage:
        forecaster = TimeSeriesForecaster()
        result = forecaster.forecast_spend(df, days=30)
    """
    
    def __init__(self, default_model: str = "auto"):
        self.default_model = default_model
    
    def forecast(
        self,
        df: pd.DataFrame,
        date_col: str,
        value_col: str,
        periods: int = 30,
        model: str = None,
        confidence: float = 0.95
    ) -> ForecastResult:
        """
        Generate forecast for a time series
        
        Args:
            df: DataFrame with date and value columns
            date_col: Name of date column
            value_col: Name of value column to forecast
            periods: Number of periods to forecast
            model: 'prophet', 'arima', 'moving_average', or 'auto'
            confidence: Confidence interval (0-1)
        """
        model = model or self.default_model
        
        # Prepare data
        data = df[[date_col, value_col]].copy()
        data.columns = ['ds', 'y']
        data['ds'] = pd.to_datetime(data['ds'])
        data = data.sort_values('ds').dropna()
        
        if len(data) < 10:
            return self._moving_average_forecast(data, periods, confidence)
        
        # Auto-select model
        if model == "auto":
            model = self._select_best_model(data)
        
        if model == "prophet" and PROPHET_AVAILABLE:
            return self._prophet_forecast(data, periods, confidence)
        elif model == "arima" and ARIMA_AVAILABLE:
            return self._arima_forecast(data, periods, confidence)
        else:
            return self._moving_average_forecast(data, periods, confidence)
    
    def _select_best_model(self, data: pd.DataFrame) -> str:
        """Auto-select best model based on data characteristics"""
        if len(data) < 30:
            return "moving_average"
        
        # Check for seasonality (weekly patterns)
        if len(data) >= 14:
            weekly_pattern = data.set_index('ds')['y'].resample('W').mean().std()
            daily_std = data['y'].std()
            has_seasonality = weekly_pattern > 0.1 * daily_std
            
            if has_seasonality and PROPHET_AVAILABLE:
                return "prophet"
        
        # Check stationarity for ARIMA
        if ARIMA_AVAILABLE:
            try:
                adf_result = adfuller(data['y'].values, autolag='AIC')
                is_stationary = adf_result[1] < 0.05
                if is_stationary:
                    return "arima"
            except Exception as e:
                logger.debug(f"ADF test failed: {e}")
        
        if PROPHET_AVAILABLE:
            return "prophet"
        
        return "moving_average"
    
    def _prophet_forecast(
        self,
        data: pd.DataFrame,
        periods: int,
        confidence: float
    ) -> ForecastResult:
        """Prophet forecasting"""
        model = Prophet(
            interval_width=confidence,
            daily_seasonality=True,
            weekly_seasonality=True,
            yearly_seasonality=len(data) > 365
        )
        model.fit(data)
        
        future = model.make_future_dataframe(periods=periods)
        forecast = model.predict(future)
        
        # Extract forecast for future dates only
        future_forecast = forecast.tail(periods)
        
        return ForecastResult(
            dates=future_forecast['ds'].tolist(),
            values=future_forecast['yhat'].tolist(),
            lower_bound=future_forecast['yhat_lower'].tolist(),
            upper_bound=future_forecast['yhat_upper'].tolist(),
            model_type="prophet",
            metrics=self._calculate_metrics(data['y'], model.predict(data)['yhat']),
            trend=self._detect_trend(future_forecast['yhat'].tolist())
        )
    
    def _arima_forecast(
        self,
        data: pd.DataFrame,
        periods: int,
        confidence: float
    ) -> ForecastResult:
        """ARIMA forecasting"""
        try:
            # Auto-select ARIMA parameters
            model = ARIMA(data['y'].values, order=(5, 1, 0))
            fitted = model.fit()
            
            # Forecast
            forecast = fitted.get_forecast(steps=periods)
            conf_int = forecast.conf_int(alpha=1-confidence)
            
            # Generate future dates
            last_date = data['ds'].max()
            future_dates = [last_date + timedelta(days=i+1) for i in range(periods)]
            
            return ForecastResult(
                dates=future_dates,
                values=forecast.predicted_mean.tolist(),
                lower_bound=conf_int.iloc[:, 0].tolist(),
                upper_bound=conf_int.iloc[:, 1].tolist(),
                model_type="arima",
                metrics={"aic": fitted.aic, "bic": fitted.bic},
                trend=self._detect_trend(forecast.predicted_mean.tolist())
            )
        except Exception as e:
            logger.warning(f"ARIMA failed: {e}, falling back to moving average")
            return self._moving_average_forecast(data, periods, confidence)
    
    def _moving_average_forecast(
        self,
        data: pd.DataFrame,
        periods: int,
        confidence: float
    ) -> ForecastResult:
        """Simple moving average forecast"""
        window = min(7, len(data))
        recent_mean = data['y'].tail(window).mean()
        recent_std = data['y'].tail(window).std()
        
        # Generate future dates
        last_date = data['ds'].max()
        future_dates = [last_date + timedelta(days=i+1) for i in range(periods)]
        
        # Add slight trend based on recent data
        if len(data) > 7:
            trend = (data['y'].tail(7).mean() - data['y'].tail(14).head(7).mean()) / 7
        else:
            trend = 0
        
        values = [recent_mean + trend * i for i in range(periods)]
        z_score = 1.96 if confidence >= 0.95 else 1.645
        
        return ForecastResult(
            dates=future_dates,
            values=values,
            lower_bound=[v - z_score * recent_std for v in values],
            upper_bound=[v + z_score * recent_std for v in values],
            model_type="moving_average",
            metrics={"window": window, "std": recent_std},
            trend=self._detect_trend(values)
        )
    
    def _calculate_metrics(self, actual: pd.Series, predicted: pd.Series) -> Dict:
        """Calculate forecast accuracy metrics"""
        mape = np.mean(np.abs((actual - predicted) / actual)) * 100
        rmse = np.sqrt(np.mean((actual - predicted) ** 2))
        mae = np.mean(np.abs(actual - predicted))
        
        return {
            "mape": round(mape, 2),
            "rmse": round(rmse, 2),
            "mae": round(mae, 2)
        }
    
    def _detect_trend(self, values: List[float]) -> str:
        """Detect overall trend direction"""
        if len(values) < 2:
            return "stable"
        
        first_half = np.mean(values[:len(values)//2])
        second_half = np.mean(values[len(values)//2:])
        
        change_pct = (second_half - first_half) / first_half * 100 if first_half else 0
        
        if change_pct > 5:
            return "increasing"
        elif change_pct < -5:
            return "decreasing"
        else:
            return "stable"
    
    # =========================================================================
    # Marketing-Specific Forecasting
    # =========================================================================
    
    def forecast_spend(self, df: pd.DataFrame, days: int = 30) -> ForecastResult:
        """Forecast daily spend"""
        return self.forecast(df, "date", "spend", periods=days, model="auto")
    
    def forecast_conversions(self, df: pd.DataFrame, days: int = 30) -> ForecastResult:
        """Forecast daily conversions"""
        return self.forecast(df, "date", "conversions", periods=days, model="auto")
    
    def forecast_roas(self, df: pd.DataFrame, days: int = 30) -> ForecastResult:
        """Forecast ROAS"""
        return self.forecast(df, "date", "roas", periods=days, model="auto")
    
    def forecast_with_budget_scenarios(
        self,
        df: pd.DataFrame,
        budget_changes: List[float],  # [0.8, 1.0, 1.2] for -20%, same, +20%
        days: int = 30
    ) -> Dict[str, ForecastResult]:
        """
        Generate forecasts for different budget scenarios
        
        Args:
            df: Historical data
            budget_changes: List of budget multipliers
            days: Forecast horizon
        """
        base_forecast = self.forecast_spend(df, days)
        scenarios = {}
        
        for multiplier in budget_changes:
            label = f"{int((multiplier-1)*100):+d}%" if multiplier != 1 else "baseline"
            
            scenarios[label] = ForecastResult(
                dates=base_forecast.dates,
                values=[v * multiplier for v in base_forecast.values],
                lower_bound=[v * multiplier for v in base_forecast.lower_bound],
                upper_bound=[v * multiplier for v in base_forecast.upper_bound],
                model_type=base_forecast.model_type,
                metrics=base_forecast.metrics,
                trend=base_forecast.trend
            )
        
        return scenarios


# Global instance
_forecaster: Optional[TimeSeriesForecaster] = None

def get_forecaster() -> TimeSeriesForecaster:
    global _forecaster
    if _forecaster is None:
        _forecaster = TimeSeriesForecaster()
    return _forecaster

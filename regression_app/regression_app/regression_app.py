
import reflex as rx
import pandas as pd
import numpy as np
from io import BytesIO
from typing import List, Dict, Any, Optional

try:
    from sklearn.model_selection import train_test_split, cross_val_score
    from sklearn.linear_model import Ridge, LinearRegression, ElasticNet, BayesianRidge, SGDRegressor
    from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
    from sklearn.metrics import r2_score, mean_squared_error, mean_absolute_error
    from sklearn.preprocessing import StandardScaler
    from statsmodels.stats.outliers_influence import variance_inflation_factor
    from scipy import stats
    import xgboost as xgb
    # Simple VIF implementation to avoid heavy statsmodels dependency if possible, or lazy import
except ImportError:
    pass # Handle dependencies later

from .styles import GLASS_STYLE, PAGE_STYLE, HEADING_STYLE, COLORS, VIOLET_STYLE, EMERALD_STYLE, PURPLE_STYLE, RED_STYLE

# --- State Management ---

class RegressionState(rx.State):
    """Manages data, modeling configuration, and results."""
    
    # Data State
    has_data: bool = False
    columns: List[str] = []
    row_count: int = 0
    _df: pd.DataFrame = pd.DataFrame() # Internal dataframe
    
    # Configuration State
    target_col: str = ""
    feature_cols: List[str] = []
    feature_cols_count: int = 0
    selected_model: str = "Ridge"
    is_loading: bool = False
    
    # Model Results
    trained: bool = False
    r2_score: float = 0.0
    mae: float = 0.0
    rmse: float = 0.0
    coefficients: List[Dict[str, Any]] = []
    
    # V3 Diagnostics
    residuals: List[float] = []
    residual_hist: List[Dict[str, Any]] = []
    is_normal: bool = False
    normality_p: float = 0.0
    executive_summary: str = ""
    vif_data: List[Dict[str, Any]] = []
    leakage_warnings: List[str] = [] # New: specific warnings
    
    async def handle_upload(self, files: List[rx.UploadFile]):
        """Handle file upload and parse features."""
        for file in files:
            upload_data = await file.read()
            # Detect file type
            if file.filename.endswith('.csv'):
                self._df = pd.read_csv(BytesIO(upload_data))
            else:
                 self._df = pd.read_excel(BytesIO(upload_data))
            
            self.columns = self._df.columns.tolist()
            self.row_count = len(self._df)
            self.has_data = True
            
            # Reset config
            self.target_col = ""
            self.feature_cols = []
            self.feature_cols_count = 0
            self.trained = False
    
    def set_target(self, val: str):
        self.target_col = val
        self.trained = False
        
    def toggle_feature(self, col: str, checked: bool):
        if checked:
            if col not in self.feature_cols:
                self.feature_cols.append(col)
        else:
            if col in self.feature_cols:
                self.feature_cols.remove(col)
        self.feature_cols_count = len(self.feature_cols)
        self.trained = False
        
    def set_model(self, val: str):
        self.selected_model = val
        self.trained = False

    # Funnel Analysis State
    analysis_mode: str = "Standard" # "Standard" or "Funnel"
    mid_funnel_col: str = "Clicks" # Intermediate metric
    funnel_top_features: List[str] = [] # Drivers of Mid-Funnel (e.g. Spend)
    funnel_bottom_features: List[str] = [] # Drivers of Target (e.g. Clicks)
    
    funnel_results: Dict = {} # Stores results for both models

    def set_analysis_mode(self, mode: str):
        self.analysis_mode = mode

    def set_mid_funnel_col(self, col: str):
        self.mid_funnel_col = col

    def toggle_funnel_top_feature(self, feature: str):
        if feature in self.funnel_top_features:
            self.funnel_top_features.remove(feature)
        else:
            self.funnel_top_features.append(feature)

    def toggle_funnel_bottom_feature(self, feature: str):
        if feature in self.funnel_bottom_features:
            self.funnel_bottom_features.remove(feature)
        else:
            self.funnel_bottom_features.append(feature)

    def train_funnel_model(self):
        """Run Two-Stage Funnel Decomposition."""
        self.is_loading = True
        self.trained = False
        self.leakage_warnings = []
        yield
        
        try:
            from .engine.pipeline import RegressionPipeline
            
            # --- Model 1: Click Model (Top Funnel) ---
            # Target: Mid-Funnel Metric (e.g. Clicks)
            # Features: Top Funnel Drivers (e.g. Spend, Impressions)
            
            if not self.funnel_top_features:
                 self.leakage_warnings.append("Funnel Step 1: No features selected.")
                 self.is_loading = False
                 return

            pipe1 = RegressionPipeline(models_to_run=[self.selected_model], quick_mode=True)
            res1 = pipe1.run(
                self._df, 
                target=self.mid_funnel_col, 
                features=self.funnel_top_features,
                test_size=0.2
            )
            
            # --- Model 2: Conversion Model (Bottom Funnel) ---
            # Target: Final Target (e.g. Conversions)
            # Features: Mid-Funnel Metric + Bottom Drivers (e.g. Clicks + Geo)
            
            # Ensure Mid-Funnel Metric is included in features for Model 2
            model2_features = self.funnel_bottom_features + [self.mid_funnel_col]
            # Remove duplicates just in case
            model2_features = list(set(model2_features))
            
            pipe2 = RegressionPipeline(models_to_run=[self.selected_model], quick_mode=True)
            res2 = pipe2.run(
                self._df,
                target=self.target_col,
                features=model2_features,
                test_size=0.2
            )
            
            # Store Results
            self.funnel_results = {
                "step1": {
                    "name": "Click Model (Efficiency)",
                    "target": self.mid_funnel_col,
                    "r2": round(res1.metrics.r2_test, 4),
                    "summary": res1.executive_summary,
                    "drivers": [{"feature": x['feature'], "impact": x['coefficient']} for x in res1.coefficient_insights]
                },
                "step2": {
                    "name": "Conversion Model (Quality)",
                    "target": self.target_col,
                    "r2": round(res2.metrics.r2_test, 4),
                    "summary": res2.executive_summary,
                    "drivers": [{"feature": x['feature'], "impact": x['coefficient']} for x in res2.coefficient_insights]
                }
            }
            
            self.trained = True
            
        except Exception as e:
            print(f"Funnel Training failed: {e}")
            import traceback
            traceback.print_exc()
        finally:
            self.is_loading = False
            
    def train_model(self):
        """Dispatch to correct training method."""
        if self.analysis_mode == "Funnel":
            return self.train_funnel_model()
        
        # ... Standard Logic Below ...
        """Train the selected regression model."""
        if not self.has_data or not self.target_col or not self.feature_cols:
            return
            
        self.is_loading = True
        self.leakage_warnings = [] # Reset warnings
        yield
            
        
        try:
            # 1. Helper to use the ported engine
            from .engine.pipeline import RegressionPipeline
            
            # 2. Prepare Inputs
            final_features = self.feature_cols
            if not final_features:
                 self.is_loading = False
                 return

            # 3. Validation
            X = self._df[final_features]
            if len(X) < 50:
                 self.leakage_warnings.append(f"Small Dataset: Only {len(X)} rows.")

            # 4. Run Pipeline (The "Nuclear" Option)
            pipeline = RegressionPipeline(
                models_to_run=[self.selected_model],
                quick_mode=True
            )
            
            result = pipeline.run(
                df=self._df,
                target=self.target_col,
                features=final_features,
                test_size=0.2
            )
            
            # 5. Map Result back to State
            self.r2_score = round(result.metrics.r2_test, 4)
            self.mae = round(result.metrics.mae, 2)
            self.rmse = round(result.metrics.rmse, 2)
            self.executive_summary = result.executive_summary
            
            # Map Diagnostics
            self.is_normal = (result.residual_diagnostics.shapiro_p_value > 0.05)
            self.normality_p = float(result.residual_diagnostics.shapiro_p_value)
            self.residuals = result.predictions['residual'].tolist()[:500]
            
            # Histogram
            hist, bin_edges = np.histogram(result.predictions['residual'], bins=15)
            self.residual_hist = [{"range": f"{bin_edges[i]:.1f} to {bin_edges[i+1]:.1f}", "count": int(hist[i])} for i in range(len(hist))]
            
            # Coefficients
            self.coefficients = []
            for item in result.coefficient_insights:
                self.coefficients.append({
                    "feature": item['feature'],
                    "impact": item['coefficient'], # The display value
                    "real_impact": item['coefficient'] # Insights use real units
                })
            
            # Map Warnings from Pipeline Analysis
            if result.vif_analysis['summary']['status'] == 'High':
                 self.leakage_warnings.append(result.vif_analysis['summary']['message'])
                 
            for pair in result.correlation_analysis.get('high_correlations', []):
                if pair['correlation'] > 0.95:
                    self.leakage_warnings.append(f"High Correlation: {pair['feature_1']} & {pair['feature_2']} ({pair['correlation']})")

            # Leakage Check 
            if self.r2_score > 0.98:
                self.leakage_warnings.append(f"Suspiciously High R² ({self.r2_score}). Check for data leakage.")
                
            self.trained = True
            
        except Exception as e:
            print(f"Engine Training failed: {e}")
            import traceback
            traceback.print_exc()
        finally:
            self.is_loading = False

def conversion_warning():
    return rx.cond(
        RegressionState.leakage_warnings,
        rx.card(
            rx.vstack(
                rx.hstack(
                    rx.icon("triangle-alert", color="white", size=24),
                    rx.heading("Potential Overfitting Detected", size="4", color="white"),
                    spacing="2",
                    align_items="center"
                ),
                rx.foreach(
                    RegressionState.leakage_warnings,
                    lambda warning: rx.text(f"• {warning}", color="white", font_size="0.9rem")
                ),
                spacing="2",
            ),
            style=RED_STYLE,
            width="100%",
            margin_bottom="1.5rem",
        )
    )

# --- UI Components ---

def configuration_card():
    return rx.card(
        rx.vstack(
            rx.hstack(
                rx.icon("settings-2", size=24, color="#8b5cf6"),
                rx.heading("Configuration", size="4"),
                width="100%",
                align="center",
                spacing="2"
            ),
            rx.divider(margin_y="4"),
            
            rx.context_menu.root(
                rx.context_menu.trigger(
                    rx.tabs.root(
                        rx.tabs.list(
                            rx.tabs.trigger("Standard Regression", value="Standard"),
                            rx.tabs.trigger("Funnel Decomposition", value="Funnel"),
                        ),
                        rx.tabs.content(
                            rx.vstack(
                                rx.text("Target Metric", size="2", weight="bold", color_scheme="gray"),
                                rx.select(
                                    RegressionState.columns,
                                    value=RegressionState.target_col,
                                    on_change=RegressionState.set_target,
                                    width="100%",
                                    variant="surface",
                                    color_scheme="violet",
                                ),
                                rx.text("Features (Drivers)", size="2", weight="bold", color_scheme="gray"),
                                rx.scroll_area(
                                    rx.vstack(
                                        rx.foreach(
                                            RegressionState.columns,
                                            lambda col: rx.hstack(
                                                rx.checkbox(
                                                    on_change=lambda checked: RegressionState.toggle_feature(col),
                                                    color_scheme="violet"
                                                ),
                                                rx.text(col, size="2"),
                                            )
                                        ),
                                        spacing="2",
                                    ),
                                    type="always",
                                    scrollbars="vertical",
                                    style={"height": "150px"}
                                ),
                                spacing="4",
                                width="100%",
                                padding_y="4"
                            ),
                            value="Standard"
                        ),
                        rx.tabs.content(
                            rx.vstack(
                                rx.text("Step 1: Click Model (Efficiency)", size="3", weight="bold", color="violet"),
                                rx.text("Target: Mid-Funnel Metric", size="2", weight="bold"),
                                rx.select(
                                    RegressionState.columns,
                                    value=RegressionState.mid_funnel_col,
                                    on_change=RegressionState.set_mid_funnel_col,
                                    width="100%",
                                    placeholder="Select Clicks/Traffic Metric",
                                ),
                                rx.text("Drivers: Spend, Impressions", size="2", weight="bold"),
                                rx.scroll_area(
                                    rx.vstack(
                                        rx.foreach(
                                            RegressionState.columns,
                                            lambda col: rx.hstack(
                                                rx.checkbox(
                                                    on_change=lambda checked: RegressionState.toggle_funnel_top_feature(col),
                                                    color_scheme="indigo"
                                                ),
                                                rx.text(col, size="2"),
                                            )
                                        ),
                                        spacing="2",
                                    ),
                                    style={"height": "100px"}
                                ),
                                
                                rx.divider(),
                                
                                rx.text("Step 2: Conversion Model (Quality)", size="3", weight="bold", color="emerald"),
                                rx.text("Target: Final Metric", size="2", weight="bold"),
                                rx.select(
                                    RegressionState.columns,
                                    value=RegressionState.target_col,
                                    on_change=RegressionState.set_target,
                                    width="100%",
                                ),
                                rx.text("Drivers: Landing Page, etc (Mid-Metric auto-included)", size="2", weight="bold"),
                                rx.scroll_area(
                                    rx.vstack(
                                        rx.foreach(
                                            RegressionState.columns,
                                            lambda col: rx.hstack(
                                                rx.checkbox(
                                                    on_change=lambda checked: RegressionState.toggle_funnel_bottom_feature(col),
                                                    color_scheme="green"
                                                ),
                                                rx.text(col, size="2"),
                                            )
                                        ),
                                        spacing="2",
                                    ),
                                    style={"height": "100px"}
                                ),
                                spacing="4",
                                width="100%",
                                padding_y="4"
                            ),
                            value="Funnel"
                        ),
                        default_value="Standard",
                        on_change=RegressionState.set_analysis_mode,
                        width="100%"
                    ),
                ),
            ),

            rx.divider(margin_y="4"),
            
            rx.text("Model Type", size="2", weight="bold", color_scheme="gray"),
            rx.select(
                items=["Ridge", "Elastic Net", "Random Forest", "XGBoost", "OLS"],
                value=RegressionState.selected_model,
                on_change=RegressionState.set_model,
                width="100%",
                variant="surface",
                color_scheme="violet",
            ),
            
            rx.button(
                rx.hstack(
                    rx.cond(RegressionState.is_loading, rx.spinner(), rx.icon("zap", size=16)),
                    rx.text("Run Analysis"),
                    spacing="2"
                ),
                on_click=RegressionState.train_model,
                width="100%",
                variant="solid",
                color_scheme="violet",
                size="3",
                disabled=RegressionState.is_loading,
                margin_top="4"
            ),
             # Empty State / Upload
            rx.cond(
                ~RegressionState.has_data,
                rx.upload(
                    rx.center(
                        rx.vstack(
                            rx.icon("upload", size=24),
                            rx.text("Upload Dataset to Begin", font_weight="bold"),
                            rx.text("Drag & drop CSV/Excel", font_size="0.8rem"),
                            spacing="2",
                        ),
                        padding="2rem",
                    ),
                    id="upload1",
                    multiple=False,
                    accept={"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": [".xlsx"], "text/csv": [".csv"]},
                    border=f"1px dashed {COLORS['glass_border']}",
                    padding="0.5rem",
                    width="100%",
                ),
            ),
            rx.cond(
                ~RegressionState.has_data,
                rx.button("Load Data", on_click=RegressionState.handle_upload(rx.upload_files(upload_id="upload1")), width="100%", margin_top="0.5rem"),
            ),
        ),
        style=VIOLET_STYLE,
        width="100%",
    )

def main_content():
    return rx.container(
        rx.vstack(
            configuration_card(),
            
            rx.cond(
                RegressionState.trained,
                rx.vstack(
                    conversion_warning(),
                    
                    # 1. Executive Summary
                    rx.card(
                        rx.vstack(
                             rx.hstack(
                                rx.box(rx.icon("brain", color="white", size=20), padding="0.5rem", border_radius="0.5rem", background="purple"),
                                rx.vstack(
                                    rx.heading("Executive Summary", size="4"),
                                    rx.text("AI-generated insights", color="gray", font_size="0.8rem"),
                                    spacing="0",
                                ),
                                align_items="center",
                                spacing="3",
                                margin_bottom="0.5rem",
                             ),
                            rx.markdown(RegressionState.executive_summary),
                            spacing="2",
                        ),
                        style=PURPLE_STYLE,
                        width="100%",
                    ),
                
                    # 2. Model Health
                    rx.grid(
                        rx.card(
                            rx.vstack(
                                rx.text("R² Score", font_size="0.9rem", color="gray"),
                                rx.text(RegressionState.r2_score, font_size="2rem", font_weight="bold", color=COLORS["neon_violet"]),
                                rx.text("Variance Explained", font_size="0.8rem", color="gray"),
                                spacing="1",
                            ),
                            style=GLASS_STYLE,
                        ),
                        rx.card(
                             rx.vstack(
                                rx.text("RMSE", font_size="0.9rem", color="gray"),
                                rx.text(RegressionState.rmse, font_size="2rem", font_weight="bold", color=COLORS["neon_cyan"]),
                                rx.text("Root Mean Squared Error", font_size="0.8rem", color="gray"),
                                spacing="1",
                            ),
                             style=GLASS_STYLE,
                        ),
                        rx.card(
                             rx.vstack(
                                rx.text("Model Status", font_size="0.9rem", color="gray"),
                                rx.text(rx.cond(RegressionState.is_normal, "HEALTHY", "WARNING"), 
                                       font_size="2rem", 
                                       font_weight="bold", 
                                       color=rx.cond(RegressionState.is_normal, COLORS["success"], COLORS["warning"])),
                                rx.text("Residual Normality", font_size="0.8rem", color="gray"),
                                spacing="1",
                            ),
                             style=GLASS_STYLE,
                        ),
                        columns="3",
                        spacing="4",
                        width="100%",
                    ),
                    
                    # 3. Diagnostics Charts
                    rx.grid(
                        rx.card(
                            rx.heading("Residual Distribution", size="4", margin_bottom="1rem"),
                            rx.recharts.bar_chart(
                                rx.recharts.bar(data_key="count", fill=COLORS["neon_violet"]),
                                rx.recharts.x_axis(data_key="range", font_size="10px"),
                                rx.recharts.y_axis(),
                                rx.recharts.tooltip(),
                                data=RegressionState.residual_hist,
                                height=250,
                                width="100%",
                            ),
                            style=GLASS_STYLE,
                        ),
                        rx.card(
                             rx.vstack(
                                 rx.hstack(
                                    rx.box(rx.icon("trending-up", color="white", size=20), padding="0.5rem", border_radius="0.5rem", background=COLORS["success"]),
                                    rx.vstack(
                                        rx.heading("Top Drivers", size="4"),
                                        rx.text(f"Impact on {RegressionState.target_col}", color="gray", font_size="0.8rem"),
                                        spacing="0",
                                    ),
                                    align_items="center",
                                    spacing="3",
                                    margin_bottom="1rem",
                                 ),
                                rx.scroll_area(
                                    rx.vstack(
                                        rx.foreach(
                                            RegressionState.coefficients,
                                            lambda row: rx.hstack(
                                                rx.badge(row["feature"], variant="solid", color_scheme="gray"),
                                                rx.spacer(),
                                                rx.text(row["impact"], font_weight="bold"),
                                                width="100%",
                                                border_bottom=f"1px solid {COLORS['glass_border']}",
                                                padding_y="0.5rem",
                                                align_items="center",
                                            )
                                        ),
                                        width="100%",
                                    ),
                                    type="auto",
                                    style={"max_height": "250px"},
                                ),
                            ),
                            style=EMERALD_STYLE,
                        ),
                        columns="2",
                        spacing="4",
                        width="100%",
                    ),
                    
                    width="100%",
                    spacing="6",
                ),
            ),
            
            spacing="6",
            padding_top="2rem",
            padding_bottom="4rem",
        ),
        size="4",
    )

def index():
    return rx.box(
        main_content(),
        style=PAGE_STYLE,
        width="100%",
    )

app = rx.App(
    theme=rx.theme(
        appearance="dark",
        primary_color="violet",
        accent_color="cyan",
        radius="large",
    )
)
app.add_page(index)

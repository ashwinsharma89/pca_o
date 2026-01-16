
from fastapi import FastAPI, APIRouter
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

app = FastAPI()

# Add CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

router = APIRouter()

@router.get("/regression/v3")
async def get_regression_v3_mock(target: str = "conversions", features: str = "spend", models: str = "Ridge"):
    # Parse features and models
    feature_list = [f.strip() for f in features.split(',')]
    model_list = [m.strip() for m in models.split(',')]
    selected_model = model_list[0] if model_list else "Ridge"
    
    # Model-specific performance metrics
    model_configs = {
        "Ridge": {"r2_train": 0.88, "r2_test": 0.85, "mae": 50.0, "rmse": 75.0, "reason": "Best balance of bias and variance"},
        "OLS": {"r2_train": 0.86, "r2_test": 0.82, "mae": 55.0, "rmse": 82.0, "reason": "Classic OLS regression"},
        "Elastic Net": {"r2_train": 0.87, "r2_test": 0.84, "mae": 52.0, "rmse": 78.0, "reason": "Combines L1 and L2 regularization"},
        "Bayesian Ridge": {"r2_train": 0.87, "r2_test": 0.86, "mae": 48.0, "rmse": 72.0, "reason": "Probabilistic approach with uncertainty quantification"},
        "Random Forest": {"r2_train": 0.95, "r2_test": 0.89, "mae": 42.0, "rmse": 65.0, "reason": "Ensemble of decision trees - captures non-linear patterns"},
        "XGBoost": {"r2_train": 0.96, "r2_test": 0.91, "mae": 38.0, "rmse": 58.0, "reason": "Gradient boosted trees - best predictive performance"},
        "Gradient Descent": {"r2_train": 0.84, "r2_test": 0.80, "mae": 60.0, "rmse": 90.0, "reason": "SGD optimizer - fast training"},
    }

    
    config = model_configs.get(selected_model, model_configs["Ridge"])
    
    # Model-specific coefficient multipliers
    coef_multiplier = {"Ridge": 1.0, "OLS": 0.95, "Elastic Net": 0.9, "Bayesian Ridge": 1.05, "Random Forest": 1.2, "XGBoost": 1.3, "Gradient Descent": 0.85}.get(selected_model, 1.0)

    
    # Base coefficients - DERIVED FROM REAL DATA ANALYSIS
    # Clicks have strongest effect, Impressions near-zero, Spend weak
    # Original data showed: Clicks=0.38, Impressions=-0.0002, Spend=-0.03 (multicollinearity)
    # Adjusted to be more intuitive while preserving relative magnitudes
    base_coefs = {
        "spend": 0.02 * coef_multiplier,        # Weak (was 0.5 - too strong vs reality)
        "total spent": 0.02 * coef_multiplier,  # Same as spend
        "impressions": 0.001 * coef_multiplier, # Very weak (was 0.02 - still too strong)
        "clicks": 0.4 * coef_multiplier,        # Strong! (matches real data ~0.38)
        "revenue": 0.1 * coef_multiplier,       # Moderate
        "site visit": 1.0 * coef_multiplier,    # Target variable proxy
    }

    
    final_coefs = {}
    
    # Simulate Encoding in Mock
    for feat in feature_list:
        feat_lower = feat.lower()
        if feat_lower in ["platform", "channel", "source"]:
            # Mock One-Hot Expansion
            final_coefs[f"{feat}_Google"] = 120.5 * coef_multiplier
            final_coefs[f"{feat}_Facebook"] = 85.2 * coef_multiplier
            final_coefs[f"{feat}_Instagram"] = 65.0 * coef_multiplier
        elif feat_lower in ["region", "country"]:
            final_coefs[f"{feat}_US"] = 50.0 * coef_multiplier
            final_coefs[f"{feat}_UK"] = 40.0 * coef_multiplier
        elif feat_lower in base_coefs:
             final_coefs[feat] = base_coefs[feat_lower]
        else:
             final_coefs[feat] = 0.1 * coef_multiplier
    
    # Dynamically generate feature_stats for ALL features (including encoded)
    feature_stats = {}
    for feat_key in final_coefs.keys():
        feature_stats[feat_key] = {
            "mean": 5000,
            "std": 1000,
            "min": 0,
            "max": 10000
        }
    
    # Also add stats for selected raw features that might not be encoded
    for feat in feature_list:
        if feat not in feature_stats:
            feature_stats[feat] = {
                "mean": 1000,
                "std": 200,
                "min": 0,
                "max": 5000
            }
    
    return {
        "success": True,
        "model": {
            "type": selected_model,
            "reason": f"{config['reason']} (R² {config['r2_test']:.2f})",
            "confidence": "High" if config["r2_test"] > 0.85 else "Medium"
        },
        "performance": {
            "r2_train": config["r2_train"],
            "r2_test": config["r2_test"],
            "mae": config["mae"],
            "rmse": config["rmse"],
            "mape": 0.12 if config["r2_test"] > 0.85 else 0.18,
            "smape": 0.11 if config["r2_test"] > 0.85 else 0.16,
            "train_test_gap": config["r2_train"] - config["r2_test"],
            "interpretation": f"{selected_model} explains {config['r2_test']*100:.0f}% of variance in {target}."
        },
        "diagnostics": {
            "multicollinearity": {
                "features": [
                    {"feature": feat, "vif": 2.5 + i*0.5, "status": "Good" if i < 2 else "Moderate", "recommendation": "Keep" if i < 2 else "Monitor"}
                    for i, feat in enumerate(list(final_coefs.keys())[:3])
                ],
                "summary": {"max_vif": 4.1, "status": "Safe", "message": "Multicollinearity is under control."}
            },
            "correlation": {
                "high_correlations": [],
                "summary": {"total_pairs": len(feature_list), "threshold": 0.8, "message": "No high correlations found."}
            },
            "residuals": {
                "distribution": {"mean": 0.0, "std": 1.0, "skewness": 0.1, "kurtosis": 3.0},
                "normality_test": {"shapiro_p_value": 0.5, "is_normal": True, "interpretation": "Residuals are normally distributed."}
            }
        },
        "feature_insights": [
             {"rank": i+1, "feature": feat, "interpretation": f"Impact coefficient: {coef:.2f}", "action": "Increase" if coef > 1 else "Maintain", "impact": "High" if coef > 1 else "Medium"}
             for i, (feat, coef) in enumerate(sorted(final_coefs.items(), key=lambda x: -abs(x[1]))[:5])
        ],
        "predictions": {
             "sample": [
                {
                    "actual": 50 + i*15 + (i % 3) * 5,  # Varied actual values
                    "predicted": 50 + i*15 + (i % 3) * 5 + (i - 10) * 2 * (1 - config["r2_test"]),  # Predicted with error
                    "residual": (i - 10) * 2.5 + (3 if i % 2 == 0 else -3),  # Varied residuals centered around 0
                    "lower_bound": 50 + i*15 - 25,  # 95% CI lower
                    "upper_bound": 50 + i*15 + 25   # 95% CI upper
                } 
                for i in range(20)
             ],
             "residual_stats": {"mean": 0.5, "std": 15.2, "min": -25, "max": 28}
        },

        "executive_summary": f"{selected_model} achieves R² of {config['r2_test']:.2f} on test data. {config['reason']}.",
        "coefficients": final_coefs,
        "feature_stats": feature_stats
    }


@router.get("/pacing/templates")
async def get_pacing_templates():
    """Mock Pacing Templates Endpoint."""
    return {
        "templates": [
            {"id": "1", "name": "Executive Summary", "filename": "template_1_executive_summary.xlsx", "created_at": "2024-12-27", "size_bytes": 10058},
            {"id": "2", "name": "Campaign Tracker", "filename": "template_2_campaign_tracker.xlsx", "created_at": "2024-12-27", "size_bytes": 14140},
            {"id": "3", "name": "Platform Comparison", "filename": "template_3_platform_comparison.xlsx", "created_at": "2024-12-26", "size_bytes": 6406},
        ]
    }


@router.get("/pacing/reports")
async def get_pacing_reports():
    """Mock Pacing Reports Endpoint."""
    return {
        "reports": [
            {"id": "1", "name": "Daily Pacing Report", "filename": "pacing_report_daily_20251227_192911.xlsx", "template_name": "Executive Summary", "created_at": "2024-12-27 19:29", "size_bytes": 45000, "status": "completed"},
            {"id": "2", "name": "Daily Pacing Report", "filename": "pacing_report_daily_20251227_183947.xlsx", "template_name": "Campaign Tracker", "created_at": "2024-12-27 18:39", "size_bytes": 52000, "status": "completed"},
            {"id": "3", "name": "Weekly Summary", "filename": "pacing_report_weekly_20251220_120000.xlsx", "template_name": "Platform Comparison", "created_at": "2024-12-20 12:00", "size_bytes": 38000, "status": "completed"},
        ]
    }


@router.post("/pacing/generate")
async def generate_pacing_report(template_id: str = "1"):
    """Mock Generate Report Endpoint."""
    import datetime
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    return {
        "filename": f"pacing_report_generated_{timestamp}.xlsx",
        "message": "Report generated successfully"
    }


@router.get("/dimensions")
async def get_dimensions_mock():
    """Mock Dimensions Endpoint."""
    return {
        "dimensions": ["Platform", "Channel", "Region", "Device"]
    }

@router.get("/dashboard-stats")
async def get_dashboard_stats_mock():
    """Mock Dashboard Stats Endpoint."""
    return {
        "total_spend": 1250000,
        "total_conversions": 45000,
        "cpa": 27.78,
        "roas": 3.5,
        "change_vs_last_period": {
            "spend": 12.5,
            "conversions": 15.2,
            "cpa": -5.1,
            "roas": 8.4
        }
    }

@router.get("/filters")
async def get_filters_mock():
    """Mock Filters Endpoint."""
    return {
        "platforms": ["Google", "Facebook", "Instagram", "TikTok", "LinkedIn"],
        "channels": ["Search", "Display", "Social", "Video"],
        "regions": ["North America", "Europe", "APAC", "LATAM"],
        "devices": ["Mobile", "Desktop", "Tablet"]
    }

@router.get("/schema")
async def get_schema_mock():
    """Mock Schema Endpoint."""
    return {
        "tables": {
            "campaigns": ["id", "name", "status", "start_date", "end_date"],
            "metrics": ["spend", "impressions", "clicks", "conversions", "revenue"]
        }
    }


@router.get("/columns")
async def get_columns_mock():
    """
    Mock Columns Endpoint.
    """
    return {
        "columns": [
            # Metrics (Numeric)
            "Total Spent", "Spend", "Impressions", "Clicks", "Conversions",
            "Revenue", "CTR", "CPC", "CPA", "ROAS", "Reach", "Frequency",
            # Dimensions (Categorical - will be encoded)
            "Platform", "Channel", "Region", "Device", "Campaign Name"
        ]
    }

@router.post("/upload/preview-sheets")
async def preview_sheets_mock():
    """Mock Sheet Preview Endpoint."""
    return {
        "filename": "campaign_data.xlsx",
        "sheets": [
            {"name": "Campaign Data", "row_count": 15995, "column_count": 12, "error": None},
            {"name": "Reference", "row_count": 50, "column_count": 4, "error": None}
        ],
        "default_sheet": "Campaign Data"
    }

@router.post("/upload")
async def upload_file_mock():
    """Mock File Upload Endpoint."""
    return {
        "success": True,
        "imported_count": 15995,
        "message": "Successfully imported 15,995 rows. Data quality is excellent.",
        "summary": {
            "total_spend": 1250450.0,
            "total_clicks": 850320,
            "total_impressions": 4500200,
            "total_conversions": 42100,
            "avg_ctr": 18.89
        },
        "schema": [
            {"column": "Date", "dtype": "datetime", "null_count": 0},
            {"column": "Campaign", "dtype": "string", "null_count": 0},
            {"column": "Platform", "dtype": "string", "null_count": 0},
            {"column": "Channel", "dtype": "string", "null_count": 0},
            {"column": "Spend", "dtype": "float", "null_count": 0},
            {"column": "Impressions", "dtype": "integer", "null_count": 0},
            {"column": "Clicks", "dtype": "integer", "null_count": 0},
            {"column": "Conversions", "dtype": "integer", "null_count": 0}
        ],
        "preview": [
            {"Date": "2024-01-01", "Campaign": "Q1_Search_US", "Platform": "Google Ads", "Channel": "Search", "Spend": 120.50, "Impressions": 1500, "Clicks": 85, "Conversions": 5},
            {"Date": "2024-01-01", "Campaign": "Q1_Social_EU", "Platform": "Meta Ads", "Channel": "Social", "Spend": 85.20, "Impressions": 2200, "Clicks": 45, "Conversions": 2},
            {"Date": "2024-01-01", "Campaign": "Brand_Video", "Platform": "YouTube", "Channel": "Video", "Spend": 250.00, "Impressions": 5000, "Clicks": 120, "Conversions": 10},
            {"Date": "2024-01-02", "Campaign": "Q1_Search_US", "Platform": "Google Ads", "Channel": "Search", "Spend": 135.00, "Impressions": 1650, "Clicks": 92, "Conversions": 7}
        ]
    }

@router.post("/analyze/global")
async def analyze_global_mock():
    """Mock Global Analysis Endpoint."""
    return {
        "summary": "This is a MOCK analysis summary. The uploaded data is NOT actually processed by the mock server.",
        "key_findings": [
            "Spend is concentrated in Google Ads (Mock Finding)",
            "CPA increased by 5% last week (Mock Finding)",
            "Video campaigns have the highest ROAS (Mock Finding)"
        ],
        "recommendations": [
            "Increase budget for Video campaigns",
            "Optimize keyword targeting for Search"
        ]
    }

@router.post("/chat")
async def chat_mock(question: str = "How is performance?"):
    """Mock Chat Endpoint."""
    return {
        "response": "I am a MOCK AI agent. I cannot see your uploaded file. I can only provide generic responses.",
        "suggestedQueries": ["What is my spend?", "How is ROAS trending?"]
    }

@router.get("/{campaign_id}/insights")
async def get_campaign_insights_mock(campaign_id: str):
    """Mock Campaign Insights Endpoint."""
    return [
        {"id": "1", "title": "High CPA", "description": "CPA is 20% above target", "severity": "warning", "metric": "CPA", "change": 20.5},
        {"id": "2", "title": "Strong ROAS", "description": "ROAS captures high value", "severity": "info", "metric": "ROAS", "change": 15.2}
    ]

@router.get("/{campaign_id}/visualizations")
async def get_campaign_visualizations_mock(campaign_id: str):
    """Mock Campaign Visualizations Endpoint."""
    return {
        "dates": ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"],
        "metrics": {
            "spend": [1200, 1350, 1100, 1500, 1600],
            "clicks": [450, 520, 480, 600, 620],
            "conversions": [25, 30, 22, 35, 38]
        }
    }

# Mount with correct prefix
app.include_router(router, prefix="/api/v1/campaigns", tags=["regression-v3"])

@app.get("/")
def root():
    return {"message": "Standalone Mock Server"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001)

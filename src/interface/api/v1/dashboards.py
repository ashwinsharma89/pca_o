from fastapi import APIRouter, Depends, HTTPException, status
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field
from datetime import datetime
import uuid

from src.interface.api.middleware.auth import get_current_user
from src.core.database.repositories import CampaignRepository, AnalysisRepository
from src.core.database.user_models import User

router = APIRouter(prefix="/dashboards", tags=["dashboards"])

# --- Models ---

class Widget(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    type: str  # 'chart', 'metric', 'text', 'table'
    title: str
    layout: Dict[str, int]  # {x, y, w, h}
    content: Dict[str, Any]
    settings: Optional[Dict[str, Any]] = None

class Dashboard(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str
    description: Optional[str] = None
    widgets: List[Widget] = []
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    owner_id: str
    is_public: bool = False
    filters: Optional[Dict[str, Any]] = None

class DashboardCreate(BaseModel):
    name: str
    description: Optional[str] = None
    widgets: List[Widget] = []
    is_public: bool = False
    filters: Optional[Dict[str, Any]] = None

class DashboardUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    widgets: Optional[List[Widget]] = None
    is_public: Optional[bool] = None
    filters: Optional[Dict[str, Any]] = None

class DashboardTemplate(BaseModel):
    id: str
    name: str
    description: str
    category: str
    widgets: List[Dict[str, Any]]
    preview_url: Optional[str] = None

# --- In-Memory Storage (Simulated) ---
# In a real app, this would be a database
DASHBOARDS_DB: Dict[str, Dashboard] = {}

# --- Endpoints ---

@router.post("/", response_model=Dashboard, status_code=status.HTTP_201_CREATED)
async def create_dashboard(dashboard_in: DashboardCreate, current_user: User = Depends(get_current_user)):
    """Save a new dashboard."""
    dashboard = Dashboard(
        name=dashboard_in.name,
        description=dashboard_in.description,
        widgets=dashboard_in.widgets,
        owner_id=str(current_user.id),
        is_public=dashboard_in.is_public,
        filters=dashboard_in.filters
    )
    DASHBOARDS_DB[dashboard.id] = dashboard
    return dashboard

@router.get("/", response_model=List[Dashboard])
async def list_dashboards(current_user: User = Depends(get_current_user)):
    """List all dashboards owned by the user or public ones."""
    return [
        db for db in DASHBOARDS_DB.values() 
        if db.owner_id == str(current_user.id) or db.is_public
    ]

@router.get("/{id}", response_model=Dashboard)
async def get_dashboard(id: str, current_user: User = Depends(get_current_user)):
    """Get a specific dashboard by ID."""
    if id not in DASHBOARDS_DB:
        raise HTTPException(status_code=404, detail="Dashboard not found")
    
    dashboard = DASHBOARDS_DB[id]
    if dashboard.owner_id != str(current_user.id) and not dashboard.is_public:
        raise HTTPException(status_code=403, detail="Not authorized to access this dashboard")
    
    return dashboard

@router.put("/{id}", response_model=Dashboard)
async def update_dashboard(id: str, dashboard_in: DashboardUpdate, current_user: User = Depends(get_current_user)):
    """Update an existing dashboard."""
    if id not in DASHBOARDS_DB:
        raise HTTPException(status_code=404, detail="Dashboard not found")
    
    dashboard = DASHBOARDS_DB[id]
    if dashboard.owner_id != str(current_user.id):
        raise HTTPException(status_code=403, detail="Not authorized to update this dashboard")
    
    # Update fields
    update_data = dashboard_in.dict(exclude_unset=True)
    for field, value in update_data.items():
        setattr(dashboard, field, value)
    
    dashboard.updated_at = datetime.utcnow()
    DASHBOARDS_DB[id] = dashboard
    return dashboard

@router.delete("/{id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_dashboard(id: str, current_user: User = Depends(get_current_user)):
    """Delete a dashboard."""
    if id not in DASHBOARDS_DB:
        raise HTTPException(status_code=404, detail="Dashboard not found")
    
    dashboard = DASHBOARDS_DB[id]
    if dashboard.owner_id != str(current_user.id):
        raise HTTPException(status_code=403, detail="Not authorized to delete this dashboard")
    
    del DASHBOARDS_DB[id]
    return None

@router.get("/templates", response_model=List[DashboardTemplate])
async def get_dashboard_templates():
    """Get AI-powered dashboard templates."""
    return [
        DashboardTemplate(
            id="template_performance",
            name="Performance Overview",
            description="Complete analysis of ROI, CPA, and ROAS across all channels.",
            category="Analytics",
            widgets=[
                {"type": "metric", "title": "Total Spend", "layout": {"x": 0, "y": 0, "w": 3, "h": 2}},
                {"type": "chart", "title": "ROAS Trend", "layout": {"x": 3, "y": 0, "w": 9, "h": 4}},
                {"type": "table", "title": "Top Campaigns", "layout": {"x": 0, "y": 4, "w": 12, "h": 6}},
            ]
        ),
        DashboardTemplate(
            id="template_audience",
            name="Audience Insights",
            description="Deep dive into demographics and behavior patterns.",
            category="Insights",
            widgets=[
                {"type": "chart", "title": "Age Distribution", "layout": {"x": 0, "y": 0, "w": 6, "h": 4}},
                {"type": "chart", "title": "Geography", "layout": {"x": 6, "y": 0, "w": 6, "h": 4}},
            ]
        ),
        DashboardTemplate(
            id="template_executive",
            name="Executive Summary",
            description="High-level metrics for leadership and decision making.",
            category="Management",
            widgets=[
                {"type": "metric", "title": "Pipeline Value", "layout": {"x": 0, "y": 0, "w": 4, "h": 2}},
                {"type": "metric", "title": "CAC", "layout": {"x": 4, "y": 0, "w": 4, "h": 2}},
                {"type": "metric", "title": "LTV", "layout": {"x": 8, "y": 0, "w": 4, "h": 2}},
            ]
        )
    ]

@router.post("/widgets/{type}/data")
async def get_widget_data(type: str, config: Dict[str, Any], current_user: User = Depends(get_current_user)):
    """Get dynamic data for a specific widget type."""
    # Simulated data based on widget type
    if type == "metric":
        return {
            "value": 42500,
            "change": 12.5,
            "trend": [38000, 39500, 41000, 42500],
            "unit": "$"
        }
    elif type == "chart":
        return {
            "labels": ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"],
            "datasets": [
                {
                    "label": config.get("metric", "Value"),
                    "data": [120, 150, 180, 130, 210, 250, 190]
                }
            ]
        }
    elif type == "table":
        return {
            "columns": ["Campaign", "Spend", "CTR", "ROAS"],
            "rows": [
                ["Black Friday Sale", 5000, "2.5%", 4.2],
                ["Holiday Special", 3500, "1.8%", 3.5],
                ["Brand Awareness", 2000, "0.9%", 1.2],
            ]
        }
    else:
        return {"data": "Generic widget data"}

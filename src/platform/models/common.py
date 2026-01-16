
from typing import Optional, List, Dict, Any
from datetime import date
from pydantic import BaseModel, Field, validator
import bleach

def sanitize_string(v: str) -> str:
    if not v:
        return v
    return bleach.clean(v, tags=[], strip=True)

class PaginationRequest(BaseModel):
    """Base model for pagination parameters"""
    limit: int = Field(default=100, ge=1, le=1000, description="Max items per page")
    offset: int = Field(default=0, ge=0, description="Number of items to skip")

class DateRangeRequest(BaseModel):
    """Base model for date range filtering"""
    start_date: Optional[date] = Field(None, description="Start date (YYYY-MM-DD)")
    end_date: Optional[date] = Field(None, description="End date (YYYY-MM-DD)")
    
    @validator('end_date')
    def validate_date_range(cls, v, values):
        if v and 'start_date' in values and values['start_date']:
            if v < values['start_date']:
                raise ValueError('end_date must be after or equal to start_date')
        return v

class FileUploadRequest(BaseModel):
    """Validation for file upload parameters"""
    sheet_name: Optional[str] = Field(None, max_length=255, description="Excel sheet name")
    file_size_bytes: Optional[int] = Field(None, le=104857600, description="Max 100MB")
    file_extension: str = Field(..., pattern=r'^(csv|xlsx|xls)$', description="Allowed: csv, xlsx, xls")
    
    @validator('sheet_name')
    def sanitize_sheet_name(cls, v):
        if v:
            return sanitize_string(v)
        return v

class AttributionSettings(BaseModel):
    model_type: str = Field(default="Last Click", description="Attribution model used")
    lookback_window: Optional[str] = Field(None, description="Lookback window (e.g. 30 days)")

class PCAAnalysisInput(BaseModel):
    """
    Structured input for analysis, including data context and quality metadata.
    Does NOT replace the raw DataFrame, but describes it.
    """
    campaign_name: str = "Global Campaign Analysis"
    platforms: List[str] = []
    attribution: Optional[AttributionSettings] = None
    
    # Metadata about the dataframe columns (populated by the analyzer)
    has_revenue: bool = False
    has_funnel: bool = False
    has_historical: bool = False
    sample_size: int = 0
    date_range: str = "Unknown"
    
    class Config:
        arbitrary_types_allowed = True

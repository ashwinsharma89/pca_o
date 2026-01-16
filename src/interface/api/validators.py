"""
API Input Validators - Comprehensive validation for API inputs
Provides custom validators for business rules, file uploads, and security
"""
from pydantic import BaseModel, validator, Field
from typing import Optional, List
from datetime import date
from pathlib import Path
import re
from fastapi import UploadFile, HTTPException
import logging

logger = logging.getLogger(__name__)

# Constants
import os
MAX_UPLOAD_SIZE_MB = int(os.getenv("MAX_UPLOAD_SIZE_MB", "200"))
MAX_FILE_SIZE = MAX_UPLOAD_SIZE_MB * 1024 * 1024
ALLOWED_FILE_EXTENSIONS = {'.csv', '.xlsx', '.xls'}
ALLOWED_PLATFORMS = {'facebook', 'google', 'instagram', 'linkedin', 'twitter', 'tiktok', 'meta', 'youtube'}
MAX_BUDGET = 10_000_000  # $10M
MAX_NAME_LENGTH = 255
MIN_NAME_LENGTH = 2


class CampaignCreate(BaseModel):
    """Validated campaign creation request"""
    name: str = Field(..., min_length=MIN_NAME_LENGTH, max_length=MAX_NAME_LENGTH)
    platform: str
    budget: Optional[float] = Field(None, ge=0, le=MAX_BUDGET)
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    objective: Optional[str] = None
    
    @validator('name')
    def validate_name(cls, v):
        """Validate campaign name"""
        if not re.match(r'^[\w\s\-\._\&\+\(\)]+$', v, re.UNICODE):
            raise ValueError('Name contains invalid characters')
        return v.strip()
    
    @validator('platform')
    def validate_platform(cls, v):
        """Validate platform name"""
        platform_lower = v.lower().strip()
        if platform_lower not in ALLOWED_PLATFORMS:
            raise ValueError(f'Platform must be one of: {", ".join(sorted(ALLOWED_PLATFORMS))}')
        return platform_lower
    
    @validator('end_date')
    def validate_dates(cls, v, values):
        """Validate date range"""
        if v and values.get('start_date'):
            if v < values['start_date']:
                raise ValueError('End date must be after start date')
        return v


class CampaignUpdate(BaseModel):
    """Validated campaign update request"""
    name: Optional[str] = Field(None, min_length=MIN_NAME_LENGTH, max_length=MAX_NAME_LENGTH)
    platform: Optional[str] = None
    budget: Optional[float] = Field(None, ge=0, le=MAX_BUDGET)
    status: Optional[str] = None
    
    @validator('name')
    def validate_name(cls, v):
        if v and not re.match(r'^[\w\s\-\._\&\+\(\)]+$', v, re.UNICODE):
            raise ValueError('Name contains invalid characters')
        return v.strip() if v else v
    
    @validator('platform')
    def validate_platform(cls, v):
        if v:
            platform_lower = v.lower().strip()
            if platform_lower not in ALLOWED_PLATFORMS:
                raise ValueError(f'Platform must be one of: {", ".join(sorted(ALLOWED_PLATFORMS))}')
            return platform_lower
        return v
    
    @validator('status')
    def validate_status(cls, v):
        if v:
            allowed = {'active', 'paused', 'completed', 'draft'}
            if v.lower() not in allowed:
                raise ValueError(f'Status must be one of: {", ".join(sorted(allowed))}')
            return v.lower()
        return v


class ChatRequest(BaseModel):
    """Validated chat/Q&A request"""
    question: str = Field(..., min_length=3, max_length=1000)
    context: Optional[str] = Field(None, max_length=5000)
    
    @validator('question')
    def validate_question(cls, v):
        """Sanitize question input"""
        # Remove potential SQL injection patterns
        dangerous_patterns = [
            r';\s*DROP',
            r';\s*DELETE',
            r';\s*UPDATE',
            r'--',
            r'/\*.*\*/'
        ]
        for pattern in dangerous_patterns:
            if re.search(pattern, v, re.IGNORECASE):
                raise ValueError('Question contains invalid characters')
        return v.strip()


class DateRangeFilter(BaseModel):
    """Validated date range filter"""
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    
    @validator('end_date')
    def validate_range(cls, v, values):
        if v and values.get('start_date'):
            if v < values['start_date']:
                raise ValueError('End date must be after start date')
            # Max range of 1 year
            if (v - values['start_date']).days > 365:
                raise ValueError('Date range cannot exceed 1 year')
        return v


class PaginationParams(BaseModel):
    """Validated pagination parameters"""
    page: int = Field(1, ge=1, le=10000)
    page_size: int = Field(20, ge=1, le=100)


async def validate_file_upload(file: UploadFile) -> UploadFile:
    """
    Validate uploaded file for security and size
    
    Args:
        file: Uploaded file
        
    Returns:
        Validated file
        
    Raises:
        HTTPException: If validation fails
    """
    # Check filename
    if not file.filename:
        raise HTTPException(400, "Filename is required")
    
    # Check extension
    ext = Path(file.filename).suffix.lower()
    if ext not in ALLOWED_FILE_EXTENSIONS:
        raise HTTPException(
            400, 
            f"Invalid file type '{ext}'. Allowed: {', '.join(ALLOWED_FILE_EXTENSIONS)}"
        )
    
    # Check file size
    contents = await file.read()
    if len(contents) > MAX_FILE_SIZE:
        raise HTTPException(
            400, 
            f"File too large. Maximum size: {MAX_FILE_SIZE / (1024*1024):.1f}MB"
        )
    
    # Check for empty file
    if len(contents) == 0:
        raise HTTPException(400, "File is empty")
    
    # Reset file pointer
    await file.seek(0)
    
    logger.info(f"File validated: {file.filename} ({len(contents)} bytes)")
    return file


def sanitize_string(value: str, max_length: int = 255) -> str:
    """
    Sanitize string input
    
    Args:
        value: Input string
        max_length: Maximum length
        
    Returns:
        Sanitized string
    """
    if not value:
        return value
    
    # Trim whitespace
    value = value.strip()
    
    # Limit length
    if len(value) > max_length:
        value = value[:max_length]
    
    # Remove null bytes
    value = value.replace('\x00', '')
    
    return value


def validate_sort_field(field: str, allowed_fields: List[str]) -> str:
    """
    Validate sort field
    
    Args:
        field: Sort field name
        allowed_fields: List of allowed field names
        
    Returns:
        Validated field name
        
    Raises:
        HTTPException: If field is not allowed
    """
    field_lower = field.lower().strip()
    if field_lower not in allowed_fields:
        raise HTTPException(
            400,
            f"Invalid sort field '{field}'. Allowed: {', '.join(allowed_fields)}"
        )
    return field_lower


def validate_email(email: str) -> bool:
    """Validate email format."""
    if not email:
        return False
    return bool(re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', email))


def validate_date_range(start_date: str, end_date: str) -> bool:
    """Validate date range string."""
    try:
        from datetime import datetime
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        return start <= end
    except (ValueError, TypeError):
        return False


def validate_pagination(page: int, per_page: int) -> bool:
    """Validate pagination parameters."""
    return page >= 1 and 1 <= per_page <= 100

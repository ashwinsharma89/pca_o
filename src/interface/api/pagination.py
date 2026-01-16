"""
Pagination Helper - Advanced pagination with total counts and metadata
Provides paginated responses with sorting and filtering support
"""
from pydantic import BaseModel
from typing import Generic, TypeVar, List, Optional, Any
from enum import Enum
from math import ceil

T = TypeVar('T')


class SortOrder(str, Enum):
    """Sort order enum"""
    ASC = "asc"
    DESC = "desc"


class PaginatedResponse(BaseModel, Generic[T]):
    """
    Paginated response with metadata
    
    Includes:
    - items: List of items for current page
    - total: Total number of items
    - page: Current page number
    - page_size: Items per page
    - total_pages: Total number of pages
    - has_next: Whether there's a next page
    - has_prev: Whether there's a previous page
    """
    items: List[Any]
    total: int
    page: int
    page_size: int
    total_pages: int
    has_next: bool
    has_prev: bool
    next_page: Optional[int] = None
    prev_page: Optional[int] = None
    
    class Config:
        arbitrary_types_allowed = True


def create_paginated_response(
    items: List[Any],
    total: int,
    page: int,
    page_size: int
) -> PaginatedResponse:
    """
    Create a paginated response with metadata
    
    Args:
        items: List of items for current page
        total: Total number of items (before pagination)
        page: Current page number (1-indexed)
        page_size: Number of items per page
        
    Returns:
        PaginatedResponse with metadata
    """
    total_pages = ceil(total / page_size) if page_size > 0 else 0
    
    return PaginatedResponse(
        items=items,
        total=total,
        page=page,
        page_size=page_size,
        total_pages=total_pages,
        has_next=page < total_pages,
        has_prev=page > 1,
        next_page=page + 1 if page < total_pages else None,
        prev_page=page - 1 if page > 1 else None
    )


def paginate_query(query, page: int, page_size: int):
    """
    Apply pagination to SQLAlchemy query
    
    Args:
        query: SQLAlchemy query object
        page: Page number (1-indexed)
        page_size: Items per page
        
    Returns:
        Paginated query
    """
    offset = (page - 1) * page_size
    return query.offset(offset).limit(page_size)


def apply_sorting(query, model, sort_by: str, sort_order: SortOrder = SortOrder.DESC):
    """
    Apply sorting to SQLAlchemy query
    
    Args:
        query: SQLAlchemy query object
        model: SQLAlchemy model class
        sort_by: Field to sort by
        sort_order: Sort order (asc/desc)
        
    Returns:
        Sorted query
    """
    column = getattr(model, sort_by, None)
    if column is None:
        return query
    
    if sort_order == SortOrder.ASC:
        return query.order_by(column.asc())
    else:
        return query.order_by(column.desc())


def apply_filters(query, model, filters: dict):
    """
    Apply filters to SQLAlchemy query
    
    Args:
        query: SQLAlchemy query object
        model: SQLAlchemy model class
        filters: Dictionary of field: value filters
        
    Returns:
        Filtered query
    """
    for field, value in filters.items():
        if value is None:
            continue
        
        column = getattr(model, field, None)
        if column is None:
            continue
        
        # Handle different filter types
        if isinstance(value, dict):
            # Range filters
            if 'min' in value and value['min'] is not None:
                query = query.filter(column >= value['min'])
            if 'max' in value and value['max'] is not None:
                query = query.filter(column <= value['max'])
        elif isinstance(value, list):
            # IN filter
            query = query.filter(column.in_(value))
        elif isinstance(value, str) and value.startswith('%'):
            # LIKE filter
            query = query.filter(column.ilike(value))
        else:
            # Exact match
            query = query.filter(column == value)
    
    return query


class FilterBuilder:
    """
    Builder for constructing query filters
    
    Usage:
        filters = (FilterBuilder()
            .equals('platform', 'facebook')
            .range('spend', min_val=100, max_val=1000)
            .like('name', 'campaign')
            .date_range('date', start, end)
            .build())
    """
    
    def __init__(self):
        self.filters = {}
    
    def equals(self, field: str, value: Any):
        """Add equality filter"""
        if value is not None:
            self.filters[field] = value
        return self
    
    def range(self, field: str, min_val: Any = None, max_val: Any = None):
        """Add range filter"""
        if min_val is not None or max_val is not None:
            self.filters[field] = {'min': min_val, 'max': max_val}
        return self
    
    def like(self, field: str, value: str):
        """Add LIKE filter (case-insensitive)"""
        if value:
            self.filters[field] = f"%{value}%"
        return self
    
    def in_list(self, field: str, values: List[Any]):
        """Add IN filter"""
        if values:
            self.filters[field] = values
        return self
    
    def date_range(self, field: str, start = None, end = None):
        """Add date range filter"""
        return self.range(field, start, end)
    
    def build(self) -> dict:
        """Build filters dictionary"""
        return self.filters


# Common sort fields for campaigns
CAMPAIGN_SORT_FIELDS = [
    'date',
    'spend',
    'impressions',
    'clicks',
    'conversions',
    'ctr',
    'cpc',
    'cpm',
    'name',
    'platform',
    'created_at'
]

# Common filter fields for campaigns
CAMPAIGN_FILTER_FIELDS = [
    'platform',
    'objective',
    'status',
    'date',
    'spend',
    'impressions',
    'clicks'
]

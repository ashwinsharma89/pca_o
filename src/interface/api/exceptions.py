"""
Custom API Exceptions with Structured Error Codes.

Provides specific exception classes for different error scenarios.
"""

from typing import Optional, Dict, Any
from fastapi import HTTPException, status


class ErrorCode:
    """Structured error codes for API responses."""
    
    # Authentication errors (1000-1099)
    AUTH_INVALID_CREDENTIALS = "AUTH_1001"  # nosec B105
    AUTH_TOKEN_EXPIRED = "AUTH_1002"  # nosec B105
    AUTH_TOKEN_INVALID = "AUTH_1003"  # nosec B105
    AUTH_INSUFFICIENT_PERMISSIONS = "AUTH_1004"  # nosec B105
    AUTH_USER_NOT_FOUND = "AUTH_1005"
    AUTH_USER_ALREADY_EXISTS = "AUTH_1006"
    
    # Campaign errors (2000-2099)
    CAMPAIGN_NOT_FOUND = "CAMPAIGN_2001"
    CAMPAIGN_INVALID_STATUS = "CAMPAIGN_2002"
    CAMPAIGN_INVALID_DATES = "CAMPAIGN_2003"
    CAMPAIGN_ALREADY_EXISTS = "CAMPAIGN_2004"
    CAMPAIGN_CANNOT_DELETE = "CAMPAIGN_2005"
    
    # Data errors (3000-3099)
    DATA_VALIDATION_ERROR = "DATA_3001"
    DATA_MISSING_REQUIRED = "DATA_3002"
    DATA_INVALID_FORMAT = "DATA_3003"
    DATA_TOO_LARGE = "DATA_3004"
    
    # Database errors (4000-4099)
    DB_CONNECTION_ERROR = "DB_4001"
    DB_QUERY_ERROR = "DB_4002"
    DB_CONSTRAINT_VIOLATION = "DB_4003"
    DB_TRANSACTION_ERROR = "DB_4004"
    
    # Rate limiting errors (5000-5099)
    RATE_LIMIT_EXCEEDED = "RATE_5001"
    RATE_LIMIT_QUOTA_EXCEEDED = "RATE_5002"
    
    # Report errors (6000-6099)
    REPORT_NOT_FOUND = "REPORT_6001"
    REPORT_GENERATION_FAILED = "REPORT_6002"
    REPORT_INVALID_TEMPLATE = "REPORT_6003"
    REPORT_NOT_READY = "REPORT_6004"
    
    # Analysis errors (7000-7099)
    ANALYSIS_NOT_FOUND = "ANALYSIS_7001"
    ANALYSIS_IN_PROGRESS = "ANALYSIS_7002"
    ANALYSIS_FAILED = "ANALYSIS_7003"
    ANALYSIS_NO_DATA = "ANALYSIS_7004"
    
    # System errors (9000-9099)
    SYSTEM_INTERNAL_ERROR = "SYSTEM_9001"
    SYSTEM_SERVICE_UNAVAILABLE = "SYSTEM_9002"
    SYSTEM_MAINTENANCE = "SYSTEM_9003"


class APIException(HTTPException):
    """
    Base API exception with structured error response.
    
    Attributes:
        error_code: Structured error code
        message: Human-readable error message
        details: Additional error details
    """
    
    def __init__(
        self,
        status_code: int,
        error_code: str,
        message: str,
        details: Optional[Dict[str, Any]] = None
    ):
        self.error_code = error_code
        self.message = message
        self.details = details or {}
        
        super().__init__(
            status_code=status_code,
            detail={
                "error_code": error_code,
                "message": message,
                "details": self.details
            }
        )


# Authentication Exceptions

class InvalidCredentialsError(APIException):
    """Invalid username or password."""
    def __init__(self, details: Optional[Dict[str, Any]] = None):
        super().__init__(
            status_code=status.HTTP_401_UNAUTHORIZED,
            error_code=ErrorCode.AUTH_INVALID_CREDENTIALS,
            message="Invalid username or password",
            details=details
        )


class TokenExpiredError(APIException):
    """JWT token has expired."""
    def __init__(self, details: Optional[Dict[str, Any]] = None):
        super().__init__(
            status_code=status.HTTP_401_UNAUTHORIZED,
            error_code=ErrorCode.AUTH_TOKEN_EXPIRED,
            message="Authentication token has expired",
            details=details
        )


class TokenInvalidError(APIException):
    """JWT token is invalid."""
    def __init__(self, details: Optional[Dict[str, Any]] = None):
        super().__init__(
            status_code=status.HTTP_401_UNAUTHORIZED,
            error_code=ErrorCode.AUTH_TOKEN_INVALID,
            message="Invalid authentication token",
            details=details
        )


class InsufficientPermissionsError(APIException):
    """User lacks required permissions."""
    def __init__(self, required_role: str = None, details: Optional[Dict[str, Any]] = None):
        details = details or {}
        if required_role:
            details["required_role"] = required_role
        
        super().__init__(
            status_code=status.HTTP_403_FORBIDDEN,
            error_code=ErrorCode.AUTH_INSUFFICIENT_PERMISSIONS,
            message="Insufficient permissions to perform this action",
            details=details
        )


class UserNotFoundError(APIException):
    """User not found."""
    def __init__(self, username: str = None, details: Optional[Dict[str, Any]] = None):
        details = details or {}
        if username:
            details["username"] = username
        
        super().__init__(
            status_code=status.HTTP_404_NOT_FOUND,
            error_code=ErrorCode.AUTH_USER_NOT_FOUND,
            message="User not found",
            details=details
        )


class UserAlreadyExistsError(APIException):
    """User already exists."""
    def __init__(self, username: str = None, details: Optional[Dict[str, Any]] = None):
        details = details or {}
        if username:
            details["username"] = username
        
        super().__init__(
            status_code=status.HTTP_400_BAD_REQUEST,
            error_code=ErrorCode.AUTH_USER_ALREADY_EXISTS,
            message="User already exists",
            details=details
        )


# Campaign Exceptions

class CampaignNotFoundError(APIException):
    """Campaign not found."""
    def __init__(self, campaign_id: str = None, details: Optional[Dict[str, Any]] = None):
        details = details or {}
        if campaign_id:
            details["campaign_id"] = campaign_id
        
        super().__init__(
            status_code=status.HTTP_404_NOT_FOUND,
            error_code=ErrorCode.CAMPAIGN_NOT_FOUND,
            message="Campaign not found",
            details=details
        )


class CampaignInvalidStatusError(APIException):
    """Campaign has invalid status for operation."""
    def __init__(
        self,
        current_status: str = None,
        required_status: str = None,
        details: Optional[Dict[str, Any]] = None
    ):
        details = details or {}
        if current_status:
            details["current_status"] = current_status
        if required_status:
            details["required_status"] = required_status
        
        super().__init__(
            status_code=status.HTTP_400_BAD_REQUEST,
            error_code=ErrorCode.CAMPAIGN_INVALID_STATUS,
            message="Campaign status does not allow this operation",
            details=details
        )


class CampaignInvalidDatesError(APIException):
    """Campaign dates are invalid."""
    def __init__(self, details: Optional[Dict[str, Any]] = None):
        super().__init__(
            status_code=status.HTTP_400_BAD_REQUEST,
            error_code=ErrorCode.CAMPAIGN_INVALID_DATES,
            message="Invalid campaign dates (end date must be after start date)",
            details=details
        )


# Data Exceptions

class DataValidationError(APIException):
    """Data validation failed."""
    def __init__(self, field: str = None, details: Optional[Dict[str, Any]] = None):
        details = details or {}
        if field:
            details["field"] = field
        
        super().__init__(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            error_code=ErrorCode.DATA_VALIDATION_ERROR,
            message="Data validation failed",
            details=details
        )


class DataMissingRequiredError(APIException):
    """Required data is missing."""
    def __init__(self, required_fields: list = None, details: Optional[Dict[str, Any]] = None):
        details = details or {}
        if required_fields:
            details["required_fields"] = required_fields
        
        super().__init__(
            status_code=status.HTTP_400_BAD_REQUEST,
            error_code=ErrorCode.DATA_MISSING_REQUIRED,
            message="Required data is missing",
            details=details
        )


# Database Exceptions

class DatabaseConnectionError(APIException):
    """Database connection failed."""
    def __init__(self, details: Optional[Dict[str, Any]] = None):
        super().__init__(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            error_code=ErrorCode.DB_CONNECTION_ERROR,
            message="Database connection failed",
            details=details
        )


class DatabaseQueryError(APIException):
    """Database query failed."""
    def __init__(self, query: str = None, details: Optional[Dict[str, Any]] = None):
        details = details or {}
        if query:
            details["query"] = query[:100]  # Truncate for security
        
        super().__init__(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code=ErrorCode.DB_QUERY_ERROR,
            message="Database query failed",
            details=details
        )


# Rate Limiting Exceptions

class RateLimitExceededError(APIException):
    """Rate limit exceeded."""
    def __init__(self, limit: str = None, details: Optional[Dict[str, Any]] = None):
        details = details or {}
        if limit:
            details["limit"] = limit
        
        super().__init__(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            error_code=ErrorCode.RATE_LIMIT_EXCEEDED,
            message="Rate limit exceeded",
            details=details
        )


# Report Exceptions

class ReportNotFoundError(APIException):
    """Report not found."""
    def __init__(self, campaign_id: str = None, details: Optional[Dict[str, Any]] = None):
        details = details or {}
        if campaign_id:
            details["campaign_id"] = campaign_id
        
        super().__init__(
            status_code=status.HTTP_404_NOT_FOUND,
            error_code=ErrorCode.REPORT_NOT_FOUND,
            message="Report not found",
            details=details
        )


class ReportGenerationFailedError(APIException):
    """Report generation failed."""
    def __init__(self, reason: str = None, details: Optional[Dict[str, Any]] = None):
        details = details or {}
        if reason:
            details["reason"] = reason
        
        super().__init__(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code=ErrorCode.REPORT_GENERATION_FAILED,
            message="Report generation failed",
            details=details
        )


class ReportInvalidTemplateError(APIException):
    """Invalid report template."""
    def __init__(
        self,
        template: str = None,
        valid_templates: list = None,
        details: Optional[Dict[str, Any]] = None
    ):
        details = details or {}
        if template:
            details["template"] = template
        if valid_templates:
            details["valid_templates"] = valid_templates
        
        super().__init__(
            status_code=status.HTTP_400_BAD_REQUEST,
            error_code=ErrorCode.REPORT_INVALID_TEMPLATE,
            message="Invalid report template",
            details=details
        )


# Analysis Exceptions

class AnalysisNotFoundError(APIException):
    """Analysis not found."""
    def __init__(self, campaign_id: str = None, details: Optional[Dict[str, Any]] = None):
        details = details or {}
        if campaign_id:
            details["campaign_id"] = campaign_id
        
        super().__init__(
            status_code=status.HTTP_404_NOT_FOUND,
            error_code=ErrorCode.ANALYSIS_NOT_FOUND,
            message="Analysis not found",
            details=details
        )


class AnalysisFailedError(APIException):
    """Analysis failed."""
    def __init__(self, reason: str = None, details: Optional[Dict[str, Any]] = None):
        details = details or {}
        if reason:
            details["reason"] = reason
        
        super().__init__(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code=ErrorCode.ANALYSIS_FAILED,
            message="Analysis failed",
            details=details
        )


# System Exceptions

class SystemInternalError(APIException):
    """Internal system error."""
    def __init__(self, details: Optional[Dict[str, Any]] = None):
        super().__init__(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code=ErrorCode.SYSTEM_INTERNAL_ERROR,
            message="Internal server error",
            details=details
        )


class SystemServiceUnavailableError(APIException):
    """Service temporarily unavailable."""
    def __init__(self, service: str = None, details: Optional[Dict[str, Any]] = None):
        details = details or {}
        if service:
            details["service"] = service
        
        super().__init__(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            error_code=ErrorCode.SYSTEM_SERVICE_UNAVAILABLE,
            message="Service temporarily unavailable",
            details=details
        )


# Generic Exceptions for broader usage

class NotFoundError(APIException):
    """Generic Not Found error."""
    def __init__(self, resource: str = "Resource", resource_id: str = None, details: Optional[Dict[str, Any]] = None):
        details = details or {}
        if resource_id:
            details["resource_id"] = resource_id
        
        super().__init__(
            status_code=status.HTTP_404_NOT_FOUND,
            error_code="NOT_FOUND",
            message=f"{resource} not found",
            details=details
        )


class ValidationError(APIException):
    """Generic Validation error."""
    def __init__(self, field: str = None, message: str = None, details: Optional[Dict[str, Any]] = None):
        details = details or {}
        if field:
            details["field"] = field
            
        super().__init__(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            error_code=ErrorCode.DATA_VALIDATION_ERROR,
            message=message or "Validation failed",
            details=details
        )


class AuthenticationError(APIException):
    """Generic Authentication error."""
    def __init__(self, message: str = "Authentication failed", details: Optional[Dict[str, Any]] = None):
        super().__init__(
            status_code=status.HTTP_401_UNAUTHORIZED,
            error_code="AUTH_ERROR",
            message=message,
            details=details
        )


class ForbiddenError(APIException):
    """Generic Forbidden error."""
    def __init__(self, action: str = None, resource: str = None, details: Optional[Dict[str, Any]] = None):
        details = details or {}
        if action:
            details["action"] = action
        if resource:
            details["resource"] = resource
            
        super().__init__(
            status_code=status.HTTP_403_FORBIDDEN,
            error_code="FORBIDDEN",
            message=f"Permission denied to {action or 'access'} {resource or 'resource'}",
            details=details
        )


class InternalServerError(APIException):
    """Generic Internal Server error."""
    def __init__(self, message: str = "Internal server error", details: Optional[Dict[str, Any]] = None):
        super().__init__(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code=ErrorCode.SYSTEM_INTERNAL_ERROR,
            message=message,
            details=details
        )


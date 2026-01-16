"""
Enhanced Error Handling - Complete error handling with proper HTTP status codes
Provides comprehensive error handling for all API errors
"""
from fastapi import Request, status
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from pydantic import ValidationError
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)


class APIError(Exception):
    """Base API error with status code and details"""
    
    def __init__(
        self,
        message: str,
        status_code: int = status.HTTP_500_INTERNAL_SERVER_ERROR,
        details: Dict[str, Any] = None,
        error_code: str = None
    ):
        self.message = message
        self.status_code = status_code
        self.details = details or {}
        self.error_code = error_code or self.__class__.__name__
        super().__init__(self.message)


class AuthenticationError(APIError):
    """Authentication failed"""
    def __init__(self, message: str = "Authentication failed", details: Dict = None):
        super().__init__(message, status.HTTP_401_UNAUTHORIZED, details, "AUTH_FAILED")


class AuthorizationError(APIError):
    """Authorization failed (insufficient permissions)"""
    def __init__(self, message: str = "Insufficient permissions", details: Dict = None):
        super().__init__(message, status.HTTP_403_FORBIDDEN, details, "FORBIDDEN")


class NotFoundError(APIError):
    """Resource not found"""
    def __init__(self, message: str = "Resource not found", details: Dict = None):
        super().__init__(message, status.HTTP_404_NOT_FOUND, details, "NOT_FOUND")


class ValidationError(APIError):
    """Validation error"""
    def __init__(self, message: str = "Validation failed", details: Dict = None):
        super().__init__(message, status.HTTP_422_UNPROCESSABLE_ENTITY, details, "VALIDATION_ERROR")


class RateLimitError(APIError):
    """Rate limit exceeded"""
    def __init__(self, message: str = "Rate limit exceeded", details: Dict = None):
        super().__init__(message, status.HTTP_429_TOO_MANY_REQUESTS, details, "RATE_LIMIT_EXCEEDED")


class DatabaseError(APIError):
    """Database error"""
    def __init__(self, message: str = "Database error", details: Dict = None):
        super().__init__(message, status.HTTP_500_INTERNAL_SERVER_ERROR, details, "DATABASE_ERROR")


class ExternalServiceError(APIError):
    """External service error (LLM, etc.)"""
    def __init__(self, message: str = "External service error", details: Dict = None):
        super().__init__(message, status.HTTP_503_SERVICE_UNAVAILABLE, details, "SERVICE_UNAVAILABLE")


async def api_error_handler(request: Request, exc: APIError) -> JSONResponse:
    """Handle custom API errors"""
    logger.error(
        f"API Error: {exc.error_code} - {exc.message}",
        extra={
            "path": str(request.url),
            "method": request.method,
            "details": exc.details
        }
    )
    
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": exc.message,
            "code": exc.error_code,
            "details": exc.details,
            "path": str(request.url.path)
        }
    )


async def validation_error_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
    """Handle Pydantic validation errors"""
    logger.warning(f"Validation error: {exc.errors()}")
    
    # Ensure errors are JSON serializable
    def clean_errors(obj):
        if isinstance(obj, dict):
            return {k: clean_errors(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [clean_errors(item) for item in obj]
        elif isinstance(obj, bytes):
            return obj.decode('utf-8', errors='replace')
        return obj

    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={
            "error": "Validation failed",
            "code": "VALIDATION_ERROR",
            "details": clean_errors(exc.errors()),
            "path": str(request.url.path)
        }
    )


async def database_error_handler(request: Request, exc: SQLAlchemyError) -> JSONResponse:
    """Handle database errors"""
    logger.error(f"Database error: {str(exc)}", exc_info=True)
    
    # Check if it's an integrity error (duplicate, foreign key, etc.)
    if isinstance(exc, IntegrityError):
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content={
                "error": "Database constraint violation",
                "code": "INTEGRITY_ERROR",
                "message": "The operation violates database constraints"
            }
        )
    
    # Generic database error
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "error": "Database error occurred",
            "code": "DATABASE_ERROR",
            "message": "Please try again later"
        }
    )


async def generic_error_handler(request: Request, exc: Exception) -> JSONResponse:
    """Handle unexpected errors"""
    logger.error(f"Unexpected error: {str(exc)}", exc_info=True)
    
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "error": "Internal server error",
            "code": "INTERNAL_ERROR",
            "message": "An unexpected error occurred"
        }
    )


def setup_exception_handlers(app):
    """
    Setup all error handlers for the FastAPI app
    
    Args:
        app: FastAPI application instance
    """
    # Custom API errors
    app.add_exception_handler(APIError, api_error_handler)
    
    # Validation errors
    app.add_exception_handler(RequestValidationError, validation_error_handler)
    
    # Database errors
    app.add_exception_handler(SQLAlchemyError, database_error_handler)
    
    # Generic errors
    app.add_exception_handler(Exception, generic_error_handler)
    
    logger.info("Error handlers configured")

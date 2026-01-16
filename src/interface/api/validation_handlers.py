"""
Global Validation Error Handler

Catches Pydantic ValidationError and returns structured responses.
"""

from fastapi import Request, status
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from pydantic import ValidationError
from loguru import logger


async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """
    Handle Pydantic validation errors with structured response.
    
    Returns detailed error information for debugging while maintaining security.
    """
    errors = exc.errors()
    
    # Log validation failure
    logger.warning(
        f"Validation error on {request.method} {request.url.path}",
        extra={
            "errors": errors,
            "client": request.client.host if request.client else "unknown"
        }
    )
    
    # Format errors for response
    formatted_errors = [
        {
            "loc": list(error["loc"]),
            "msg": error["msg"],
            "type": error["type"]
        }
        for error in errors
    ]
    
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={
            "success": False,
            "error": {
                "code": "VALIDATION_ERROR",
                "message": "Request validation failed",
                "details": formatted_errors
            },
            "path": request.url.path,
            "method": request.method
        }
    )


def setup_validation_handlers(app):
    """
    Setup validation error handlers for the FastAPI app.
    
    Call this after creating the FastAPI app instance.
    """
    app.add_exception_handler(RequestValidationError, validation_exception_handler)
    logger.info("✅ Validation error handlers configured")

import time
from fastapi import Request
from loguru import logger

async def diagnostic_middleware(request: Request, call_next):
    start_time = time.time()
    logger.info(f"🔍 DIAGNOSTIC: Request started: {request.method} {request.url.path}")
    
    try:
        response = await call_next(request)
        duration = time.time() - start_time
        logger.info(f"🔍 DIAGNOSTIC: Request finished: {request.method} {request.url.path} in {duration:.4f}s")
        return response
    except Exception as e:
        duration = time.time() - start_time
        logger.error(f"🔍 DIAGNOSTIC: Request failed: {request.method} {request.url.path} in {duration:.4f}s with error: {e}")
        raise

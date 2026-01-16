"""
Production Logging Configuration
Features: Log rotation, centralized logging, structured JSON logs
"""
import os
import sys
import json
import logging
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any
import traceback


# ============================================================================
# Configuration
# ============================================================================

LOG_DIR = Path(os.getenv("LOG_DIR", "./logs"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FORMAT = os.getenv("LOG_FORMAT", "json")  # json or text
LOG_MAX_SIZE = int(os.getenv("LOG_MAX_SIZE", 10 * 1024 * 1024))  # 10MB
LOG_BACKUP_COUNT = int(os.getenv("LOG_BACKUP_COUNT", 10))
LOG_ROTATION_WHEN = os.getenv("LOG_ROTATION_WHEN", "midnight")  # midnight, h, d, w0-w6

# Centralized logging (ELK/Loki)
LOKI_URL = os.getenv("LOKI_URL", "")
ELASTICSEARCH_URL = os.getenv("ELASTICSEARCH_URL", "")

# Create log directory
LOG_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================================
# JSON Formatter for Structured Logging
# ============================================================================

class JSONFormatter(logging.Formatter):
    """
    JSON formatter for structured logging
    Compatible with ELK stack, Loki, CloudWatch, etc.
    """
    
    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }
        
        # Add extra fields
        if hasattr(record, "extra"):
            log_data.update(record.extra)
        
        # Add exception info
        if record.exc_info:
            log_data["exception"] = {
                "type": record.exc_info[0].__name__ if record.exc_info[0] else None,
                "message": str(record.exc_info[1]) if record.exc_info[1] else None,
                "traceback": traceback.format_exception(*record.exc_info)
            }
        
        # Add request context if available
        if hasattr(record, "request_id"):
            log_data["request_id"] = record.request_id
        if hasattr(record, "user_id"):
            log_data["user_id"] = record.user_id
        
        return json.dumps(log_data, default=str)


class TextFormatter(logging.Formatter):
    """Human-readable text formatter for development"""
    
    def __init__(self):
        super().__init__(
            fmt="%(asctime)s | %(levelname)-8s | %(name)s:%(funcName)s:%(lineno)d | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )


# ============================================================================
# Log Rotation Handlers
# ============================================================================

def create_rotating_file_handler(
    filename: str,
    max_bytes: int = LOG_MAX_SIZE,
    backup_count: int = LOG_BACKUP_COUNT,
    formatter: logging.Formatter = None
) -> RotatingFileHandler:
    """
    Create size-based rotating file handler
    
    Args:
        filename: Log file name
        max_bytes: Max file size before rotation
        backup_count: Number of backup files to keep
        formatter: Log formatter
    """
    handler = RotatingFileHandler(
        LOG_DIR / filename,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8"
    )
    handler.setFormatter(formatter or JSONFormatter())
    return handler


def create_timed_rotating_handler(
    filename: str,
    when: str = LOG_ROTATION_WHEN,
    backup_count: int = LOG_BACKUP_COUNT,
    formatter: logging.Formatter = None
) -> TimedRotatingFileHandler:
    """
    Create time-based rotating file handler
    
    Args:
        filename: Log file name
        when: Rotation interval (midnight, h, d, w0-w6)
        backup_count: Number of backup files to keep
        formatter: Log formatter
    """
    handler = TimedRotatingFileHandler(
        LOG_DIR / filename,
        when=when,
        backupCount=backup_count,
        encoding="utf-8"
    )
    handler.setFormatter(formatter or JSONFormatter())
    return handler


# ============================================================================
# Centralized Logging Handlers
# ============================================================================

class LokiHandler(logging.Handler):
    """
    Loki push handler for centralized logging
    Requires: pip install python-logging-loki
    """
    
    def __init__(self, url: str, tags: Dict[str, str] = None):
        super().__init__()
        self.url = url
        self.tags = tags or {"app": "pca-agent"}
        self._client = None
    
    def _get_client(self):
        if self._client is None:
            try:
                import logging_loki
                self._client = logging_loki.LokiHandler(
                    url=self.url,
                    tags=self.tags,
                    version="1"
                )
            except ImportError:
                logging.warning("python-logging-loki not installed")
        return self._client
    
    def emit(self, record: logging.LogRecord):
        client = self._get_client()
        if client:
            client.emit(record)


class ElasticsearchHandler(logging.Handler):
    """
    Elasticsearch handler for centralized logging
    Requires: pip install CMRESHandler
    """
    
    def __init__(self, url: str, index: str = "pca-agent-logs"):
        super().__init__()
        self.url = url
        self.index = index
        self._handler = None
    
    def _get_handler(self):
        if self._handler is None:
            try:
                from cmreslogging.handlers import CMRESHandler
                self._handler = CMRESHandler(
                    hosts=[{"host": self.url.split("://")[1].split(":")[0], "port": 9200}],
                    auth_type=CMRESHandler.AuthType.NO_AUTH,
                    es_index_name=self.index
                )
            except ImportError:
                logging.warning("CMRESHandler not installed")
        return self._handler
    
    def emit(self, record: logging.LogRecord):
        handler = self._get_handler()
        if handler:
            handler.emit(record)


# ============================================================================
# Logger Setup
# ============================================================================

def setup_logging(
    name: str = "pca",
    level: str = LOG_LEVEL,
    json_format: bool = None,
    console: bool = True,
    file: bool = True,
    centralized: bool = True
) -> logging.Logger:
    """
    Setup production-ready logging
    
    Args:
        name: Logger name
        level: Log level
        json_format: Use JSON format (default: based on LOG_FORMAT env)
        console: Enable console output
        file: Enable file output with rotation
        centralized: Enable centralized logging (if configured)
    
    Returns:
        Configured logger
    """
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper()))
    logger.handlers.clear()
    
    # Determine format
    use_json = json_format if json_format is not None else (LOG_FORMAT == "json")
    formatter = JSONFormatter() if use_json else TextFormatter()
    
    # Console handler
    if console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(TextFormatter())  # Always text for console
        logger.addHandler(console_handler)
    
    # File handlers with rotation
    if file:
        # Application logs (size-based rotation)
        app_handler = create_rotating_file_handler(
            "app.log",
            formatter=formatter
        )
        logger.addHandler(app_handler)
        
        # Error logs (time-based rotation)
        error_handler = create_timed_rotating_handler(
            "error.log",
            when="midnight",
            formatter=formatter
        )
        error_handler.setLevel(logging.ERROR)
        logger.addHandler(error_handler)
        
        # Access logs (separate file)
        access_handler = create_rotating_file_handler(
            "access.log",
            formatter=formatter
        )
        access_handler.addFilter(lambda r: r.name == "access")
        logger.addHandler(access_handler)
    
    # Centralized logging
    if centralized:
        if LOKI_URL:
            loki_handler = LokiHandler(LOKI_URL)
            loki_handler.setFormatter(formatter)
            logger.addHandler(loki_handler)
            logger.info(f"Loki logging enabled: {LOKI_URL}")
        
        if ELASTICSEARCH_URL:
            es_handler = ElasticsearchHandler(ELASTICSEARCH_URL)
            es_handler.setFormatter(formatter)
            logger.addHandler(es_handler)
            logger.info(f"Elasticsearch logging enabled: {ELASTICSEARCH_URL}")
    
    return logger


# ============================================================================
# Context Logger for Request Tracking
# ============================================================================

class ContextLogger:
    """
    Logger with context (request_id, user_id, etc.)
    
    Usage:
        logger = ContextLogger(request_id="abc123", user_id="user_1")
        logger.info("Processing request")
    """
    
    def __init__(
        self,
        name: str = "pca",
        request_id: str = None,
        user_id: str = None,
        **extra
    ):
        self.logger = logging.getLogger(name)
        self.context = {
            "request_id": request_id,
            "user_id": user_id,
            **extra
        }
    
    def _log(self, level: int, msg: str, *args, **kwargs):
        extra = kwargs.pop("extra", {})
        extra.update(self.context)
        self.logger.log(level, msg, *args, extra={"extra": extra}, **kwargs)
    
    def debug(self, msg: str, *args, **kwargs):
        self._log(logging.DEBUG, msg, *args, **kwargs)
    
    def info(self, msg: str, *args, **kwargs):
        self._log(logging.INFO, msg, *args, **kwargs)
    
    def warning(self, msg: str, *args, **kwargs):
        self._log(logging.WARNING, msg, *args, **kwargs)
    
    def error(self, msg: str, *args, **kwargs):
        self._log(logging.ERROR, msg, *args, **kwargs)
    
    def critical(self, msg: str, *args, **kwargs):
        self._log(logging.CRITICAL, msg, *args, **kwargs)
    
    def exception(self, msg: str, *args, **kwargs):
        kwargs["exc_info"] = True
        self._log(logging.ERROR, msg, *args, **kwargs)


# ============================================================================
# FastAPI Integration
# ============================================================================

def get_request_logger(request_id: str = None, user_id: str = None) -> ContextLogger:
    """Get logger with request context for FastAPI endpoints"""
    return ContextLogger(request_id=request_id, user_id=user_id)


# ============================================================================
# Initialize Default Logger
# ============================================================================

# Default logger instance
logger = setup_logging()

__all__ = [
    'setup_logging',
    'ContextLogger',
    'get_request_logger',
    'JSONFormatter',
    'TextFormatter',
    'logger'
]

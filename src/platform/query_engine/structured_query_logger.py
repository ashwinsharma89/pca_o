"""
Structured Query Logger
Comprehensive query logging with analytics and debugging
"""
import os
import json
import time
from datetime import datetime
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field, asdict
from pathlib import Path
from enum import Enum
import hashlib
import logging

logger = logging.getLogger(__name__)


class QueryStatus(str, Enum):
    PENDING = "pending"
    EXECUTING = "executing"
    SUCCESS = "success"
    FAILED = "failed"
    CACHED = "cached"


@dataclass
class QueryLog:
    """Structured query log entry"""
    id: str
    timestamp: datetime
    user_id: Optional[str]
    session_id: Optional[str]
    
    # Query details
    natural_language_query: str
    generated_sql: Optional[str]
    query_template: Optional[str]
    
    # Execution details
    status: QueryStatus
    execution_time_ms: float
    rows_returned: int
    
    # LLM details
    llm_provider: Optional[str]
    llm_model: Optional[str]
    llm_tokens_used: int
    llm_latency_ms: float
    
    # Results
    result_summary: Optional[str]
    error_message: Optional[str]
    
    # Context
    platform: Optional[str]
    date_range: Optional[str]
    metadata: Dict[str, Any] = field(default_factory=dict)


class StructuredQueryLogger:
    """
    Structured logging for all query operations
    
    Features:
    - Structured JSON logging
    - Query analytics
    - Performance tracking
    - Error analysis
    
    Usage:
        query_logger = StructuredQueryLogger()
        log_id = query_logger.start_query("What is my ROAS?", user_id="123")
        query_logger.set_sql(log_id, "SELECT * FROM campaigns")
        query_logger.complete_query(log_id, rows=100, execution_time=150)
    """
    
    LOG_DIR = Path(os.getenv("QUERY_LOG_DIR", "./logs/queries"))
    
    def __init__(self):
        self.LOG_DIR.mkdir(parents=True, exist_ok=True)
        self._active_queries: Dict[str, QueryLog] = {}
        self._query_count = 0
    
    def _generate_id(self) -> str:
        self._query_count += 1
        return f"q_{int(time.time())}_{self._query_count:04d}"
    
    # =========================================================================
    # Query Lifecycle
    # =========================================================================
    
    def start_query(
        self,
        natural_language_query: str,
        user_id: str = None,
        session_id: str = None,
        platform: str = None,
        date_range: str = None
    ) -> str:
        """Start logging a query"""
        query_id = self._generate_id()
        
        log = QueryLog(
            id=query_id,
            timestamp=datetime.utcnow(),
            user_id=user_id,
            session_id=session_id,
            natural_language_query=natural_language_query,
            generated_sql=None,
            query_template=None,
            status=QueryStatus.PENDING,
            execution_time_ms=0,
            rows_returned=0,
            llm_provider=None,
            llm_model=None,
            llm_tokens_used=0,
            llm_latency_ms=0,
            result_summary=None,
            error_message=None,
            platform=platform,
            date_range=date_range
        )
        
        self._active_queries[query_id] = log
        logger.info(f"Query started: {query_id}", extra={"query_id": query_id})
        return query_id
    
    def set_sql(self, query_id: str, sql: str, template: str = None):
        """Record generated SQL"""
        if query_id in self._active_queries:
            self._active_queries[query_id].generated_sql = sql
            self._active_queries[query_id].query_template = template
            self._active_queries[query_id].status = QueryStatus.EXECUTING
    
    def set_llm_details(
        self,
        query_id: str,
        provider: str,
        model: str,
        tokens: int,
        latency_ms: float
    ):
        """Record LLM usage"""
        if query_id in self._active_queries:
            log = self._active_queries[query_id]
            log.llm_provider = provider
            log.llm_model = model
            log.llm_tokens_used = tokens
            log.llm_latency_ms = latency_ms
    
    def complete_query(
        self,
        query_id: str,
        rows: int = 0,
        execution_time_ms: float = 0,
        result_summary: str = None,
        cached: bool = False
    ):
        """Complete a successful query"""
        if query_id in self._active_queries:
            log = self._active_queries[query_id]
            log.status = QueryStatus.CACHED if cached else QueryStatus.SUCCESS
            log.rows_returned = rows
            log.execution_time_ms = execution_time_ms
            log.result_summary = result_summary
            
            self._save_log(log)
            del self._active_queries[query_id]
            
            logger.info(
                f"Query completed: {query_id}",
                extra={
                    "query_id": query_id,
                    "rows": rows,
                    "time_ms": execution_time_ms,
                    "cached": cached
                }
            )
    
    def fail_query(self, query_id: str, error: str, execution_time_ms: float = 0):
        """Record a failed query"""
        if query_id in self._active_queries:
            log = self._active_queries[query_id]
            log.status = QueryStatus.FAILED
            log.error_message = error
            log.execution_time_ms = execution_time_ms
            
            self._save_log(log)
            del self._active_queries[query_id]
            
            logger.error(
                f"Query failed: {query_id}",
                extra={"query_id": query_id, "error": error}
            )
    
    # =========================================================================
    # Log Storage
    # =========================================================================
    
    def _save_log(self, log: QueryLog):
        """Save log to file"""
        date_str = log.timestamp.strftime("%Y-%m-%d")
        log_file = self.LOG_DIR / f"queries_{date_str}.jsonl"
        
        log_dict = asdict(log)
        log_dict["timestamp"] = log.timestamp.isoformat()
        log_dict["status"] = log.status.value
        
        with open(log_file, "a") as f:
            f.write(json.dumps(log_dict) + "\n")
    
    def get_logs(
        self,
        date: str = None,
        status: QueryStatus = None,
        user_id: str = None,
        limit: int = 100
    ) -> List[Dict]:
        """Retrieve query logs"""
        date = date or datetime.utcnow().strftime("%Y-%m-%d")
        log_file = self.LOG_DIR / f"queries_{date}.jsonl"
        
        if not log_file.exists():
            return []
        
        logs = []
        with open(log_file) as f:
            for line in f:
                if not line.strip():
                    continue
                log = json.loads(line)
                
                if status and log.get("status") != status.value:
                    continue
                if user_id and log.get("user_id") != user_id:
                    continue
                
                logs.append(log)
                
                if len(logs) >= limit:
                    break
        
        return logs
    
    # =========================================================================
    # Analytics
    # =========================================================================
    
    def get_analytics(self, date: str = None) -> Dict:
        """Get query analytics for a day"""
        logs = self.get_logs(date, limit=10000)
        
        if not logs:
            return {"date": date, "total_queries": 0}
        
        total = len(logs)
        successful = sum(1 for l in logs if l["status"] == "success")
        cached = sum(1 for l in logs if l["status"] == "cached")
        failed = sum(1 for l in logs if l["status"] == "failed")
        
        exec_times = [l["execution_time_ms"] for l in logs if l["execution_time_ms"] > 0]
        llm_tokens = sum(l["llm_tokens_used"] for l in logs)
        
        return {
            "date": date or datetime.utcnow().strftime("%Y-%m-%d"),
            "total_queries": total,
            "successful": successful,
            "cached": cached,
            "failed": failed,
            "cache_hit_rate": cached / total if total > 0 else 0,
            "success_rate": successful / total if total > 0 else 0,
            "avg_execution_time_ms": sum(exec_times) / len(exec_times) if exec_times else 0,
            "p95_execution_time_ms": sorted(exec_times)[int(len(exec_times) * 0.95)] if exec_times else 0,
            "total_llm_tokens": llm_tokens,
            "unique_users": len(set(l["user_id"] for l in logs if l["user_id"]))
        }
    
    def get_slow_queries(self, date: str = None, threshold_ms: float = 5000) -> List[Dict]:
        """Get slow queries"""
        logs = self.get_logs(date, limit=10000)
        return [l for l in logs if l["execution_time_ms"] > threshold_ms]
    
    def get_failed_queries(self, date: str = None) -> List[Dict]:
        """Get failed queries"""
        return self.get_logs(date, status=QueryStatus.FAILED)
    
    def get_common_queries(self, date: str = None, limit: int = 10) -> List[Dict]:
        """Get most common query patterns"""
        logs = self.get_logs(date, limit=10000)
        
        # Group by query hash
        query_counts = {}
        for log in logs:
            query_hash = hashlib.sha256(log["natural_language_query"].lower().encode()).hexdigest()[:8]
            if query_hash not in query_counts:
                query_counts[query_hash] = {
                    "query": log["natural_language_query"],
                    "count": 0,
                    "avg_time_ms": 0,
                    "total_time_ms": 0
                }
            query_counts[query_hash]["count"] += 1
            query_counts[query_hash]["total_time_ms"] += log["execution_time_ms"]
        
        # Calculate averages and sort
        for q in query_counts.values():
            q["avg_time_ms"] = q["total_time_ms"] / q["count"]
        
        sorted_queries = sorted(query_counts.values(), key=lambda x: x["count"], reverse=True)
        return sorted_queries[:limit]


# Global instance
_query_logger: Optional[StructuredQueryLogger] = None

def get_query_logger() -> StructuredQueryLogger:
    global _query_logger
    if _query_logger is None:
        _query_logger = StructuredQueryLogger()
    return _query_logger

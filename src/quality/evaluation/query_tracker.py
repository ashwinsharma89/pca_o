"""
Query Tracker - Evaluation Metrics & Traceability
Logs all queries, interpretations, and user feedback for analysis
"""
import os
import json
import uuid
import sqlite3
from datetime import datetime
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, asdict
from pathlib import Path
import pandas as pd
import logging

logger = logging.getLogger(__name__)


@dataclass
class QueryLog:
    """Data class for query log entries."""
    query_id: str
    user_id: str
    session_id: str
    timestamp: str
    original_query: str
    interpretations: str  # JSON string
    selected_interpretation_index: Optional[int]
    selected_interpretation: Optional[str]
    generated_sql: Optional[str]
    execution_time_ms: Optional[int]
    result_count: Optional[int]
    error_message: Optional[str]
    user_feedback: Optional[int]  # -1, 0, 1
    feedback_comment: Optional[str]


@dataclass
class QueryMetrics:
    """Data class for query metrics."""
    metric_id: str
    query_id: str
    metric_name: str
    metric_value: float
    timestamp: str


class QueryTracker:
    """Tracks queries, interpretations, and user feedback for evaluation."""
    
    def __init__(self, db_path: str = None):
        """
        Initialize the query tracker.
        
        Args:
            db_path: Path to SQLite database file (default: logs/query_tracker.db)
        """
        if db_path is None:
            logs_dir = Path("logs")
            logs_dir.mkdir(exist_ok=True)
            db_path = logs_dir / "query_tracker.db"
        
        self.db_path = str(db_path)
        self._init_database()
        logger.info(f"QueryTracker initialized with database: {self.db_path}")
    
    def _init_database(self):
        """Initialize the SQLite database with required tables."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create query_logs table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS query_logs (
                query_id TEXT PRIMARY KEY,
                user_id TEXT,
                session_id TEXT,
                timestamp TEXT,
                original_query TEXT,
                interpretations TEXT,
                selected_interpretation_index INTEGER,
                selected_interpretation TEXT,
                generated_sql TEXT,
                execution_time_ms INTEGER,
                result_count INTEGER,
                error_message TEXT,
                user_feedback INTEGER,
                feedback_comment TEXT
            )
        """)
        
        # Create query_metrics table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS query_metrics (
                metric_id TEXT PRIMARY KEY,
                query_id TEXT,
                metric_name TEXT,
                metric_value REAL,
                timestamp TEXT,
                FOREIGN KEY (query_id) REFERENCES query_logs(query_id)
            )
        """)
        
        # Create indexes
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON query_logs(timestamp)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_user_id ON query_logs(user_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_session_id ON query_logs(session_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_query_id ON query_metrics(query_id)")
        
        conn.commit()
        conn.close()
    
    def start_query(
        self,
        original_query: str,
        interpretations: List[Dict[str, Any]],
        user_id: str = "anonymous",
        session_id: str = None
    ) -> str:
        """
        Start tracking a new query.
        
        Args:
            original_query: The original natural language query
            interpretations: List of generated interpretations
            user_id: User identifier
            session_id: Session identifier
            
        Returns:
            query_id: Unique identifier for this query
        """
        query_id = str(uuid.uuid4())
        timestamp = datetime.now().isoformat()
        
        if session_id is None:
            session_id = str(uuid.uuid4())
        
        log_entry = QueryLog(
            query_id=query_id,
            user_id=user_id,
            session_id=session_id,
            timestamp=timestamp,
            original_query=original_query,
            interpretations=json.dumps(interpretations),
            selected_interpretation_index=None,
            selected_interpretation=None,
            generated_sql=None,
            execution_time_ms=None,
            result_count=None,
            error_message=None,
            user_feedback=None,
            feedback_comment=None
        )
        
        self._insert_query_log(log_entry)
        logger.info(f"Started tracking query: {query_id}")
        
        return query_id
    
    def update_query(
        self,
        query_id: str,
        selected_interpretation_index: int = None,
        selected_interpretation: str = None,
        generated_sql: str = None,
        execution_time_ms: int = None,
        result_count: int = None,
        error_message: str = None
    ):
        """Update query log with execution details."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        updates = []
        params = []
        
        if selected_interpretation_index is not None:
            updates.append("selected_interpretation_index = ?")
            params.append(selected_interpretation_index)
        
        if selected_interpretation is not None:
            updates.append("selected_interpretation = ?")
            params.append(selected_interpretation)
        
        if generated_sql is not None:
            updates.append("generated_sql = ?")
            params.append(generated_sql)
        
        if execution_time_ms is not None:
            updates.append("execution_time_ms = ?")
            params.append(execution_time_ms)
        
        if result_count is not None:
            updates.append("result_count = ?")
            params.append(result_count)
        
        if error_message is not None:
            updates.append("error_message = ?")
            params.append(error_message)
        
        if updates:
            params.append(query_id)
            sql = f"UPDATE query_logs SET {', '.join(updates)} WHERE query_id = ?"
            cursor.execute(sql, params)
            conn.commit()
        
        conn.close()
        logger.info(f"Updated query: {query_id}")
    
    def add_feedback(
        self,
        query_id: str,
        feedback: int,
        comment: str = None
    ):
        """
        Add user feedback for a query.
        
        Args:
            query_id: Query identifier
            feedback: -1 (thumbs down), 0 (neutral), 1 (thumbs up)
            comment: Optional feedback comment
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute(
            "UPDATE query_logs SET user_feedback = ?, feedback_comment = ? WHERE query_id = ?",
            (feedback, comment, query_id)
        )
        
        conn.commit()
        conn.close()
        logger.info(f"Added feedback for query: {query_id} - {feedback}")
    
    def log_metric(
        self,
        query_id: str,
        metric_name: str,
        metric_value: float
    ):
        """Log a metric for a query."""
        metric = QueryMetrics(
            metric_id=str(uuid.uuid4()),
            query_id=query_id,
            metric_name=metric_name,
            metric_value=metric_value,
            timestamp=datetime.now().isoformat()
        )
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO query_metrics VALUES (?, ?, ?, ?, ?)
        """, (
            metric.metric_id,
            metric.query_id,
            metric.metric_name,
            metric.metric_value,
            metric.timestamp
        ))
        
        conn.commit()
        conn.close()
    
    def _insert_query_log(self, log_entry: QueryLog):
        """Insert a query log entry into the database."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO query_logs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            log_entry.query_id,
            log_entry.user_id,
            log_entry.session_id,
            log_entry.timestamp,
            log_entry.original_query,
            log_entry.interpretations,
            log_entry.selected_interpretation_index,
            log_entry.selected_interpretation,
            log_entry.generated_sql,
            log_entry.execution_time_ms,
            log_entry.result_count,
            log_entry.error_message,
            log_entry.user_feedback,
            log_entry.feedback_comment
        ))
        
        conn.commit()
        conn.close()
    
    def get_query_log(self, query_id: str) -> Optional[Dict[str, Any]]:
        """Get a specific query log."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM query_logs WHERE query_id = ?", (query_id,))
        row = cursor.fetchone()
        
        conn.close()
        
        if row:
            return dict(row)
        return None
    
    def get_all_queries(self, limit: int = 100) -> pd.DataFrame:
        """Get all query logs as a DataFrame."""
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql_query(
            f"SELECT * FROM query_logs ORDER BY timestamp DESC LIMIT {limit}",
            conn
        )
        conn.close()
        return df
    
    def get_metrics_summary(self) -> Dict[str, Any]:
        """Get summary statistics of all queries."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Total queries
        cursor.execute("SELECT COUNT(*) FROM query_logs")
        total_queries = cursor.fetchone()[0]
        
        # Queries with feedback
        cursor.execute("SELECT COUNT(*) FROM query_logs WHERE user_feedback IS NOT NULL")
        queries_with_feedback = cursor.fetchone()[0]
        
        # Average feedback
        cursor.execute("SELECT AVG(user_feedback) FROM query_logs WHERE user_feedback IS NOT NULL")
        avg_feedback = cursor.fetchone()[0] or 0
        
        # Average execution time
        cursor.execute("SELECT AVG(execution_time_ms) FROM query_logs WHERE execution_time_ms IS NOT NULL")
        avg_execution_time = cursor.fetchone()[0] or 0
        
        # Success rate (queries without errors)
        cursor.execute("SELECT COUNT(*) FROM query_logs WHERE error_message IS NULL")
        successful_queries = cursor.fetchone()[0]
        success_rate = (successful_queries / total_queries * 100) if total_queries > 0 else 0
        
        # Interpretation accuracy (% of times first interpretation was selected)
        cursor.execute("SELECT COUNT(*) FROM query_logs WHERE selected_interpretation_index = 0")
        first_interpretation_selected = cursor.fetchone()[0]
        interpretation_accuracy = (first_interpretation_selected / total_queries * 100) if total_queries > 0 else 0
        
        conn.close()
        
        return {
            "total_queries": total_queries,
            "queries_with_feedback": queries_with_feedback,
            "avg_feedback": round(avg_feedback, 2),
            "avg_execution_time_ms": round(avg_execution_time, 2),
            "success_rate": round(success_rate, 2),
            "interpretation_accuracy": round(interpretation_accuracy, 2)
        }
    
    def export_to_csv(self, output_path: str = "query_logs_export.csv"):
        """Export all query logs to CSV."""
        df = self.get_all_queries(limit=999999)
        df.to_csv(output_path, index=False)
        logger.info(f"Exported {len(df)} queries to {output_path}")
        return output_path

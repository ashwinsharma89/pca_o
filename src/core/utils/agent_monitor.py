"""
Agent Performance Monitoring System.

Tracks agent execution time, accuracy, success rates, and resource usage.
"""

import time
import functools
from typing import Dict, Any, Callable, Optional
from datetime import datetime, timedelta
from collections import defaultdict, deque
from dataclasses import dataclass, field
import threading

from loguru import logger


@dataclass
class AgentMetrics:
    """Metrics for a single agent."""
    
    agent_name: str
    total_calls: int = 0
    successful_calls: int = 0
    failed_calls: int = 0
    total_execution_time: float = 0.0
    min_execution_time: float = float('inf')
    max_execution_time: float = 0.0
    recent_execution_times: deque = field(default_factory=lambda: deque(maxlen=100))
    accuracy_scores: deque = field(default_factory=lambda: deque(maxlen=100))
    last_execution: Optional[datetime] = None
    error_messages: deque = field(default_factory=lambda: deque(maxlen=10))
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate percentage."""
        if self.total_calls == 0:
            return 0.0
        return (self.successful_calls / self.total_calls) * 100
    
    @property
    def avg_execution_time(self) -> float:
        """Calculate average execution time."""
        if self.total_calls == 0:
            return 0.0
        return self.total_execution_time / self.total_calls
    
    @property
    def recent_avg_execution_time(self) -> float:
        """Calculate recent average execution time (last 100 calls)."""
        if not self.recent_execution_times:
            return 0.0
        return sum(self.recent_execution_times) / len(self.recent_execution_times)
    
    @property
    def avg_accuracy(self) -> float:
        """Calculate average accuracy score."""
        if not self.accuracy_scores:
            return 0.0
        return sum(self.accuracy_scores) / len(self.accuracy_scores)
    
    @property
    def status(self) -> str:
        """Determine agent health status."""
        if self.total_calls == 0:
            return "unknown"
        
        if self.success_rate < 90:
            return "unhealthy"
        elif self.success_rate < 95:
            return "degraded"
        else:
            return "healthy"


class AgentMonitor:
    """
    Centralized monitoring system for all agents.
    
    Tracks performance metrics, execution times, and success rates.
    """
    
    def __init__(self):
        """Initialize agent monitor."""
        self.metrics: Dict[str, AgentMetrics] = {}
        self.lock = threading.Lock()
        logger.info("Initialized AgentMonitor")
    
    def record_execution(
        self,
        agent_name: str,
        execution_time: float,
        success: bool,
        accuracy_score: Optional[float] = None,
        error_message: Optional[str] = None
    ):
        """
        Record an agent execution.
        
        Args:
            agent_name: Name of the agent
            execution_time: Execution time in seconds
            success: Whether execution was successful
            accuracy_score: Optional accuracy score (0-100)
            error_message: Optional error message if failed
        """
        with self.lock:
            if agent_name not in self.metrics:
                self.metrics[agent_name] = AgentMetrics(agent_name=agent_name)
            
            metrics = self.metrics[agent_name]
            metrics.total_calls += 1
            metrics.last_execution = datetime.now()
            
            if success:
                metrics.successful_calls += 1
            else:
                metrics.failed_calls += 1
                if error_message:
                    metrics.error_messages.append({
                        'timestamp': datetime.now().isoformat(),
                        'message': error_message
                    })
            
            # Update execution times
            metrics.total_execution_time += execution_time
            metrics.min_execution_time = min(metrics.min_execution_time, execution_time)
            metrics.max_execution_time = max(metrics.max_execution_time, execution_time)
            metrics.recent_execution_times.append(execution_time)
            
            # Update accuracy
            if accuracy_score is not None:
                metrics.accuracy_scores.append(accuracy_score)
    
    def get_metrics(self, agent_name: str) -> Optional[AgentMetrics]:
        """Get metrics for a specific agent."""
        return self.metrics.get(agent_name)
    
    def get_all_metrics(self) -> Dict[str, Dict[str, Any]]:
        """Get metrics for all agents."""
        with self.lock:
            return {
                name: {
                    'agent_name': metrics.agent_name,
                    'total_calls': metrics.total_calls,
                    'successful_calls': metrics.successful_calls,
                    'failed_calls': metrics.failed_calls,
                    'success_rate': round(metrics.success_rate, 2),
                    'avg_execution_time': round(metrics.avg_execution_time, 3),
                    'recent_avg_execution_time': round(metrics.recent_avg_execution_time, 3),
                    'min_execution_time': round(metrics.min_execution_time, 3) if metrics.min_execution_time != float('inf') else 0,
                    'max_execution_time': round(metrics.max_execution_time, 3),
                    'avg_accuracy': round(metrics.avg_accuracy, 2),
                    'status': metrics.status,
                    'last_execution': metrics.last_execution.isoformat() if metrics.last_execution else None,
                    'recent_errors': list(metrics.error_messages)
                }
                for name, metrics in self.metrics.items()
            }
    
    def get_dashboard(self) -> str:
        """Generate a text dashboard of all agent metrics."""
        all_metrics = self.get_all_metrics()
        
        if not all_metrics:
            return "No agent metrics available"
        
        lines = [
            "=" * 70,
            "Agent Performance Dashboard",
            "=" * 70,
            ""
        ]
        
        for name, metrics in sorted(all_metrics.items()):
            status_icon = {
                'healthy': '✅',
                'degraded': '⚠️',
                'unhealthy': '❌',
                'unknown': '❓'
            }.get(metrics['status'], '❓')
            
            lines.extend([
                f"{name}",
                f"├─ Status: {status_icon} {metrics['status'].upper()}",
                f"├─ Total Calls: {metrics['total_calls']}",
                f"├─ Success Rate: {metrics['success_rate']}%",
                f"├─ Avg Response Time: {metrics['avg_execution_time']:.3f}s",
                f"├─ Recent Avg Time: {metrics['recent_avg_execution_time']:.3f}s",
                f"├─ Min/Max Time: {metrics['min_execution_time']:.3f}s / {metrics['max_execution_time']:.3f}s",
                f"├─ Avg Accuracy: {metrics['avg_accuracy']:.1f}%",
                f"└─ Last Execution: {metrics['last_execution'] or 'Never'}",
                ""
            ])
        
        # Overall system health
        healthy_count = sum(1 for m in all_metrics.values() if m['status'] == 'healthy')
        total_count = len(all_metrics)
        overall_health = "HEALTHY" if healthy_count == total_count else "DEGRADED" if healthy_count > 0 else "UNHEALTHY"
        
        lines.extend([
            "=" * 70,
            f"Overall System Health: {overall_health} ({healthy_count}/{total_count} agents healthy)",
            "=" * 70
        ])
        
        return "\n".join(lines)
    
    def reset_metrics(self, agent_name: Optional[str] = None):
        """Reset metrics for a specific agent or all agents."""
        with self.lock:
            if agent_name:
                if agent_name in self.metrics:
                    del self.metrics[agent_name]
                    logger.info(f"Reset metrics for {agent_name}")
            else:
                self.metrics.clear()
                logger.info("Reset all agent metrics")


# Global monitor instance
_monitor: Optional[AgentMonitor] = None


def get_agent_monitor() -> AgentMonitor:
    """Get global agent monitor instance."""
    global _monitor
    if _monitor is None:
        _monitor = AgentMonitor()
    return _monitor


def monitor_agent(agent_name: Optional[str] = None):
    """
    Decorator to monitor agent execution.
    
    Args:
        agent_name: Optional agent name (defaults to function name)
    
    Example:
        @monitor_agent("vision_agent")
        def process_image(image):
            # Agent logic
            return result
    """
    def decorator(func: Callable) -> Callable:
        name = agent_name or func.__name__
        
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            monitor = get_agent_monitor()
            start_time = time.time()
            success = False
            error_message = None
            
            try:
                result = func(*args, **kwargs)
                success = True
                return result
            except Exception as e:
                error_message = str(e)
                raise
            finally:
                execution_time = time.time() - start_time
                monitor.record_execution(
                    agent_name=name,
                    execution_time=execution_time,
                    success=success,
                    error_message=error_message
                )
        
        return wrapper
    return decorator


def record_agent_accuracy(agent_name: str, accuracy_score: float):
    """
    Record accuracy score for an agent.
    
    Args:
        agent_name: Name of the agent
        accuracy_score: Accuracy score (0-100)
    """
    monitor = get_agent_monitor()
    with monitor.lock:
        if agent_name in monitor.metrics:
            monitor.metrics[agent_name].accuracy_scores.append(accuracy_score)

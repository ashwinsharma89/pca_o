"""
Workflow Monitor - Track and monitor workflow execution
Provides comprehensive workflow monitoring and metrics
"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional
from enum import Enum
import logging
import json

logger = logging.getLogger(__name__)


class WorkflowStatus(Enum):
    """Workflow execution status"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class WorkflowMetrics:
    """Workflow execution metrics"""
    workflow_id: str
    workflow_name: str
    status: WorkflowStatus
    start_time: datetime
    end_time: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    steps_completed: int = 0
    steps_total: int = 0
    error_message: Optional[str] = None
    metadata: Dict = field(default_factory=dict)


class WorkflowMonitor:
    """Monitor workflow execution with metrics and logging"""
    
    def __init__(self, log_file: str = "workflow_monitor.log"):
        self.metrics: Dict[str, WorkflowMetrics] = {}
        self.logger = logging.getLogger("workflow_monitor")
        
        # Setup file logging
        handler = logging.FileHandler(log_file)
        handler.setFormatter(
            logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        )
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)
    
    def start_workflow(
        self,
        workflow_id: str,
        workflow_name: str,
        total_steps: int,
        metadata: Dict = None
    ):
        """
        Start monitoring workflow
        
        Args:
            workflow_id: Unique workflow ID
            workflow_name: Workflow name
            total_steps: Total number of steps
            metadata: Additional metadata
        """
        self.metrics[workflow_id] = WorkflowMetrics(
            workflow_id=workflow_id,
            workflow_name=workflow_name,
            status=WorkflowStatus.RUNNING,
            start_time=datetime.now(),
            steps_total=total_steps,
            metadata=metadata or {}
        )
        
        self.logger.info(
            f"Workflow started: {workflow_name} (ID: {workflow_id}, Steps: {total_steps})"
        )
        logger.info(f"Workflow {workflow_id} started")
    
    def update_progress(self, workflow_id: str, steps_completed: int, message: str = None):
        """
        Update workflow progress
        
        Args:
            workflow_id: Workflow ID
            steps_completed: Number of completed steps
            message: Optional progress message
        """
        if workflow_id in self.metrics:
            metric = self.metrics[workflow_id]
            metric.steps_completed = steps_completed
            
            progress = (steps_completed / metric.steps_total) * 100 if metric.steps_total > 0 else 0
            
            log_msg = f"Workflow {workflow_id} progress: {progress:.1f}% ({steps_completed}/{metric.steps_total})"
            if message:
                log_msg += f" - {message}"
            
            self.logger.info(log_msg)
            logger.debug(log_msg)
    
    def complete_workflow(
        self,
        workflow_id: str,
        success: bool = True,
        error: str = None
    ):
        """
        Complete workflow
        
        Args:
            workflow_id: Workflow ID
            success: Whether workflow succeeded
            error: Error message if failed
        """
        if workflow_id in self.metrics:
            metric = self.metrics[workflow_id]
            metric.end_time = datetime.now()
            metric.duration_seconds = (metric.end_time - metric.start_time).total_seconds()
            metric.status = WorkflowStatus.COMPLETED if success else WorkflowStatus.FAILED
            metric.error_message = error
            
            log_msg = (
                f"Workflow {metric.workflow_name} (ID: {workflow_id}) "
                f"{metric.status.value} in {metric.duration_seconds:.2f}s"
            )
            if error:
                log_msg += f" - Error: {error}"
            
            self.logger.info(log_msg)
            logger.info(log_msg)
    
    def cancel_workflow(self, workflow_id: str, reason: str = None):
        """
        Cancel workflow
        
        Args:
            workflow_id: Workflow ID
            reason: Cancellation reason
        """
        if workflow_id in self.metrics:
            metric = self.metrics[workflow_id]
            metric.end_time = datetime.now()
            metric.duration_seconds = (metric.end_time - metric.start_time).total_seconds()
            metric.status = WorkflowStatus.CANCELLED
            metric.error_message = reason
            
            self.logger.info(
                f"Workflow {workflow_id} cancelled - {reason or 'No reason provided'}"
            )
    
    def get_metrics(self, workflow_id: str) -> Optional[WorkflowMetrics]:
        """Get workflow metrics"""
        return self.metrics.get(workflow_id)
    
    def get_all_metrics(self, status: WorkflowStatus = None) -> List[WorkflowMetrics]:
        """
        Get all workflow metrics
        
        Args:
            status: Filter by status (optional)
            
        Returns:
            List of workflow metrics
        """
        metrics = list(self.metrics.values())
        if status:
            metrics = [m for m in metrics if m.status == status]
        return metrics
    
    def get_summary(self) -> Dict:
        """
        Get workflow summary statistics
        
        Returns:
            Summary dictionary
        """
        all_metrics = list(self.metrics.values())
        
        return {
            "total_workflows": len(all_metrics),
            "running": len([m for m in all_metrics if m.status == WorkflowStatus.RUNNING]),
            "completed": len([m for m in all_metrics if m.status == WorkflowStatus.COMPLETED]),
            "failed": len([m for m in all_metrics if m.status == WorkflowStatus.FAILED]),
            "cancelled": len([m for m in all_metrics if m.status == WorkflowStatus.CANCELLED]),
            "average_duration": sum(
                m.duration_seconds for m in all_metrics 
                if m.duration_seconds is not None
            ) / len(all_metrics) if all_metrics else 0
        }


# Global workflow monitor instance
_global_monitor = None

def get_workflow_monitor() -> WorkflowMonitor:
    """Get global workflow monitor instance"""
    global _global_monitor
    if _global_monitor is None:
        _global_monitor = WorkflowMonitor()
    return _global_monitor

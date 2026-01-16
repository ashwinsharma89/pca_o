"""
User Behavior Analytics
Track and analyze user interactions, sessions, and patterns
"""

from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
import json
from pathlib import Path
from collections import defaultdict
from loguru import logger

@dataclass
class UserSession:
    """User session data."""
    session_id: str
    user_id: str
    start_time: datetime
    end_time: Optional[datetime]
    duration_seconds: int
    page_views: int
    actions: List[Dict[str, Any]]
    device: str
    browser: str
    ip_address: str

@dataclass
class UserAction:
    """Individual user action."""
    action_id: str
    user_id: str
    session_id: str
    action_type: str  # click, view, search, create, update, delete
    resource: str
    details: Dict[str, Any]
    timestamp: datetime

class UserBehaviorAnalytics:
    """Track and analyze user behavior."""
    
    def __init__(self, storage_path: str = "./data/user_analytics"):
        """Initialize user behavior analytics."""
        self.storage_path = Path(storage_path)
        self.storage_path.mkdir(parents=True, exist_ok=True)
        
        # In-memory caches
        self.active_sessions: Dict[str, UserSession] = {}
        self.user_actions: List[UserAction] = []
        
        logger.info("✅ User Behavior Analytics initialized")
    
    def start_session(
        self,
        session_id: str,
        user_id: str,
        device: str,
        browser: str,
        ip_address: str
    ) -> UserSession:
        """Start a new user session."""
        session = UserSession(
            session_id=session_id,
            user_id=user_id,
            start_time=datetime.utcnow(),
            end_time=None,
            duration_seconds=0,
            page_views=0,
            actions=[],
            device=device,
            browser=browser,
            ip_address=ip_address
        )
        
        self.active_sessions[session_id] = session
        logger.info(f"Started session {session_id} for user {user_id}")
        
        return session
    
    def end_session(self, session_id: str):
        """End a user session."""
        if session_id not in self.active_sessions:
            return
        
        session = self.active_sessions[session_id]
        session.end_time = datetime.utcnow()
        session.duration_seconds = int(
            (session.end_time - session.start_time).total_seconds()
        )
        
        # Save session
        self._save_session(session)
        
        # Remove from active sessions
        del self.active_sessions[session_id]
        
        logger.info(
            f"Ended session {session_id} "
            f"(duration: {session.duration_seconds}s)"
        )
    
    def track_action(
        self,
        action_id: str,
        user_id: str,
        session_id: str,
        action_type: str,
        resource: str,
        details: Dict[str, Any] = None
    ):
        """Track a user action."""
        action = UserAction(
            action_id=action_id,
            user_id=user_id,
            session_id=session_id,
            action_type=action_type,
            resource=resource,
            details=details or {},
            timestamp=datetime.utcnow()
        )
        
        self.user_actions.append(action)
        
        # Add to session if active
        if session_id in self.active_sessions:
            self.active_sessions[session_id].actions.append(asdict(action))
        
        # Save action
        self._save_action(action)
        
        logger.debug(f"Tracked action: {action_type} on {resource}")
    
    def track_page_view(
        self,
        session_id: str,
        page: str,
        referrer: Optional[str] = None
    ):
        """Track a page view."""
        if session_id in self.active_sessions:
            self.active_sessions[session_id].page_views += 1
        
        self.track_action(
            action_id=f"{session_id}_{datetime.utcnow().timestamp()}",
            user_id=self.active_sessions[session_id].user_id if session_id in self.active_sessions else "unknown",
            session_id=session_id,
            action_type="page_view",
            resource=page,
            details={"referrer": referrer}
        )
    
    def get_user_stats(self, user_id: str) -> Dict[str, Any]:
        """Get statistics for a user."""
        # Load user sessions
        sessions = self._load_user_sessions(user_id)
        
        if not sessions:
            return {
                "user_id": user_id,
                "total_sessions": 0,
                "total_actions": 0,
                "avg_session_duration": 0,
                "most_used_features": []
            }
        
        # Calculate stats
        total_sessions = len(sessions)
        total_duration = sum(s.duration_seconds for s in sessions)
        avg_duration = total_duration / total_sessions if total_sessions > 0 else 0
        
        # Count actions by type
        action_counts = defaultdict(int)
        for session in sessions:
            for action in session.actions:
                action_counts[action["action_type"]] += 1
        
        # Most used features
        most_used = sorted(
            action_counts.items(),
            key=lambda x: x[1],
            reverse=True
        )[:5]
        
        return {
            "user_id": user_id,
            "total_sessions": total_sessions,
            "total_actions": sum(action_counts.values()),
            "avg_session_duration": avg_duration,
            "most_used_features": [
                {"feature": feature, "count": count}
                for feature, count in most_used
            ],
            "action_breakdown": dict(action_counts)
        }
    
    def get_feature_usage(
        self,
        from_date: Optional[datetime] = None,
        to_date: Optional[datetime] = None
    ) -> Dict[str, int]:
        """Get feature usage statistics."""
        if from_date is None:
            from_date = datetime.utcnow() - timedelta(days=30)
        if to_date is None:
            to_date = datetime.utcnow()
        
        # Load actions in date range
        actions = self._load_actions_in_range(from_date, to_date)
        
        # Count by resource
        usage = defaultdict(int)
        for action in actions:
            usage[action.resource] += 1
        
        return dict(sorted(usage.items(), key=lambda x: x[1], reverse=True))
    
    def get_user_journey(self, session_id: str) -> List[Dict[str, Any]]:
        """Get user journey for a session."""
        session = self._load_session(session_id)
        
        if not session:
            return []
        
        return [
            {
                "timestamp": action["timestamp"],
                "action": action["action_type"],
                "resource": action["resource"],
                "details": action.get("details", {})
            }
            for action in session.actions
        ]
    
    def get_conversion_funnel(
        self,
        funnel_steps: List[str]
    ) -> Dict[str, Any]:
        """Analyze conversion funnel."""
        # Load recent sessions
        sessions = self._load_recent_sessions(days=30)
        
        funnel_data = {step: 0 for step in funnel_steps}
        total_users = len(set(s.user_id for s in sessions))
        
        for session in sessions:
            actions = [a["resource"] for a in session.actions]
            
            for i, step in enumerate(funnel_steps):
                if step in actions:
                    funnel_data[step] += 1
                else:
                    break  # User dropped off
        
        # Calculate conversion rates
        funnel_with_rates = []
        prev_count = total_users
        
        for step in funnel_steps:
            count = funnel_data[step]
            rate = (count / prev_count * 100) if prev_count > 0 else 0
            
            funnel_with_rates.append({
                "step": step,
                "users": count,
                "conversion_rate": rate,
                "drop_off": prev_count - count
            })
            
            prev_count = count
        
        return {
            "total_users": total_users,
            "funnel": funnel_with_rates
        }
    
    def get_cohort_analysis(
        self,
        cohort_by: str = "week"  # day, week, month
    ) -> Dict[str, Any]:
        """Perform cohort analysis."""
        sessions = self._load_recent_sessions(days=90)
        
        # Group users by cohort
        cohorts = defaultdict(set)
        
        for session in sessions:
            cohort_key = self._get_cohort_key(session.start_time, cohort_by)
            cohorts[cohort_key].add(session.user_id)
        
        # Calculate retention
        cohort_data = []
        
        for cohort_key in sorted(cohorts.keys()):
            cohort_users = cohorts[cohort_key]
            
            cohort_data.append({
                "cohort": cohort_key,
                "size": len(cohort_users),
                "retention": self._calculate_retention(cohort_users, cohort_key)
            })
        
        return {
            "cohort_by": cohort_by,
            "cohorts": cohort_data
        }
    
    def _get_cohort_key(self, date: datetime, cohort_by: str) -> str:
        """Get cohort key for a date."""
        if cohort_by == "day":
            return date.strftime("%Y-%m-%d")
        elif cohort_by == "week":
            return date.strftime("%Y-W%W")
        elif cohort_by == "month":
            return date.strftime("%Y-%m")
        return date.strftime("%Y-%m-%d")
    
    def _calculate_retention(
        self,
        cohort_users: set,
        cohort_key: str
    ) -> List[float]:
        """Calculate retention rates for a cohort."""
        # Simplified retention calculation
        # In production, would track actual return rates
        return [100.0, 80.0, 60.0, 40.0]  # Placeholder
    
    def _save_session(self, session: UserSession):
        """Save session to disk."""
        file_path = self.storage_path / f"session_{session.session_id}.json"
        with open(file_path, 'w') as f:
            json.dump(asdict(session), f, default=str)
    
    def _save_action(self, action: UserAction):
        """Save action to disk."""
        date_str = action.timestamp.strftime("%Y-%m-%d")
        file_path = self.storage_path / f"actions_{date_str}.jsonl"
        
        with open(file_path, 'a') as f:
            f.write(json.dumps(asdict(action), default=str) + "\n")
    
    def _load_session(self, session_id: str) -> Optional[UserSession]:
        """Load session from disk."""
        file_path = self.storage_path / f"session_{session_id}.json"
        
        if not file_path.exists():
            return None
        
        with open(file_path, 'r') as f:
            data = json.load(f)
            return UserSession(**data)
    
    def _load_user_sessions(self, user_id: str) -> List[UserSession]:
        """Load all sessions for a user."""
        sessions = []
        
        for file_path in self.storage_path.glob("session_*.json"):
            with open(file_path, 'r') as f:
                data = json.load(f)
                if data["user_id"] == user_id:
                    sessions.append(UserSession(**data))
        
        return sessions
    
    def _load_recent_sessions(self, days: int = 30) -> List[UserSession]:
        """Load recent sessions."""
        cutoff = datetime.utcnow() - timedelta(days=days)
        sessions = []
        
        for file_path in self.storage_path.glob("session_*.json"):
            with open(file_path, 'r') as f:
                data = json.load(f)
                start_time = datetime.fromisoformat(data["start_time"])
                
                if start_time >= cutoff:
                    sessions.append(UserSession(**data))
        
        return sessions
    
    def _load_actions_in_range(
        self,
        from_date: datetime,
        to_date: datetime
    ) -> List[UserAction]:
        """Load actions in date range."""
        actions = []
        current_date = from_date
        
        while current_date <= to_date:
            date_str = current_date.strftime("%Y-%m-%d")
            file_path = self.storage_path / f"actions_{date_str}.jsonl"
            
            if file_path.exists():
                with open(file_path, 'r') as f:
                    for line in f:
                        data = json.loads(line)
                        actions.append(UserAction(**data))
            
            current_date += timedelta(days=1)
        
        return actions


# Global instance
user_analytics = UserBehaviorAnalytics()

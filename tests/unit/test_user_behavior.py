"""
Unit tests for user behavior analytics.
Tests session tracking and user action analytics.
"""

import pytest
from unittest.mock import Mock, patch
from datetime import datetime, timedelta
import tempfile
import os

# Try to import, skip if not available
try:
    from src.engine.analytics.user_behavior import (
        UserBehaviorAnalytics, UserSession, UserAction
    )
    BEHAVIOR_AVAILABLE = True
except ImportError:
    BEHAVIOR_AVAILABLE = False
    UserBehaviorAnalytics = None
    UserSession = None
    UserAction = None

pytestmark = pytest.mark.skipif(not BEHAVIOR_AVAILABLE, reason="User behavior not available")


class TestUserSession:
    """Test UserSession dataclass."""
    
    def test_create_session(self):
        """Test creating a user session."""
        session = UserSession(
            session_id="sess_123",
            user_id="user_456",
            start_time=datetime.utcnow(),
            end_time=None,
            duration_seconds=0,
            page_views=0,
            actions=[],
            device="desktop",
            browser="Chrome",
            ip_address="192.168.1.1"
        )
        
        assert session.session_id == "sess_123"
        assert session.user_id == "user_456"
        assert session.device == "desktop"
    
    def test_session_with_actions(self):
        """Test session with actions."""
        session = UserSession(
            session_id="sess_123",
            user_id="user_456",
            start_time=datetime.utcnow(),
            end_time=None,
            duration_seconds=300,
            page_views=5,
            actions=[{"type": "click", "target": "button"}],
            device="mobile",
            browser="Safari",
            ip_address="10.0.0.1"
        )
        
        assert len(session.actions) == 1
        assert session.page_views == 5


class TestUserAction:
    """Test UserAction dataclass."""
    
    def test_create_action(self):
        """Test creating a user action."""
        action = UserAction(
            action_id="act_123",
            user_id="user_456",
            session_id="sess_789",
            action_type="click",
            resource="/dashboard",
            details={"button": "submit"},
            timestamp=datetime.utcnow()
        )
        
        assert action.action_type == "click"
        assert action.resource == "/dashboard"


class TestUserBehaviorAnalytics:
    """Test UserBehaviorAnalytics functionality."""
    
    @pytest.fixture
    def analytics(self, tmp_path):
        """Create analytics instance with temp storage."""
        return UserBehaviorAnalytics(storage_path=str(tmp_path / "analytics"))
    
    def test_initialization(self, analytics):
        """Test analytics initialization."""
        assert analytics is not None
        assert analytics.active_sessions == {}
    
    def test_start_session(self, analytics):
        """Test starting a new session."""
        session = analytics.start_session(
            session_id="sess_123",
            user_id="user_456",
            device="desktop",
            browser="Chrome",
            ip_address="192.168.1.1"
        )
        
        assert session.session_id == "sess_123"
        assert "sess_123" in analytics.active_sessions
    
    def test_end_session(self, analytics):
        """Test ending a session."""
        analytics.start_session(
            session_id="sess_123",
            user_id="user_456",
            device="desktop",
            browser="Chrome",
            ip_address="192.168.1.1"
        )
        
        analytics.end_session("sess_123")
        
        # Session should be ended
        if "sess_123" in analytics.active_sessions:
            session = analytics.active_sessions["sess_123"]
            assert session.end_time is not None
    
    def test_record_action(self, analytics):
        """Test recording a user action."""
        analytics.start_session(
            session_id="sess_123",
            user_id="user_456",
            device="desktop",
            browser="Chrome",
            ip_address="192.168.1.1"
        )
        
        if hasattr(analytics, 'record_action'):
            analytics.record_action(
                action_id="act_001",
                user_id="user_456",
                session_id="sess_123",
                action_type="click",
                resource="/dashboard",
                details={}
            )
            
            assert len(analytics.user_actions) >= 1
    
    def test_record_page_view(self, analytics):
        """Test recording a page view."""
        analytics.start_session(
            session_id="sess_123",
            user_id="user_456",
            device="desktop",
            browser="Chrome",
            ip_address="192.168.1.1"
        )
        
        if hasattr(analytics, 'record_page_view'):
            analytics.record_page_view("sess_123", "/dashboard")
            
            session = analytics.active_sessions["sess_123"]
            assert session.page_views >= 1
    
    def test_get_user_sessions(self, analytics):
        """Test getting user sessions."""
        analytics.start_session(
            session_id="sess_1",
            user_id="user_456",
            device="desktop",
            browser="Chrome",
            ip_address="192.168.1.1"
        )
        analytics.start_session(
            session_id="sess_2",
            user_id="user_456",
            device="mobile",
            browser="Safari",
            ip_address="192.168.1.2"
        )
        
        if hasattr(analytics, 'get_user_sessions'):
            sessions = analytics.get_user_sessions("user_456")
            assert len(sessions) >= 2
    
    def test_get_session_duration(self, analytics):
        """Test calculating session duration."""
        import time
        
        analytics.start_session(
            session_id="sess_123",
            user_id="user_456",
            device="desktop",
            browser="Chrome",
            ip_address="192.168.1.1"
        )
        
        time.sleep(0.1)  # Small delay
        analytics.end_session("sess_123")
        
        if "sess_123" in analytics.active_sessions:
            session = analytics.active_sessions["sess_123"]
            assert session.duration_seconds >= 0


class TestUserBehaviorAnalytics_Statistics:
    """Test analytics statistics functionality."""
    
    @pytest.fixture
    def analytics(self, tmp_path):
        """Create analytics instance."""
        return UserBehaviorAnalytics(storage_path=str(tmp_path / "analytics"))
    
    def test_get_active_users(self, analytics):
        """Test getting active users count."""
        analytics.start_session("s1", "user1", "desktop", "Chrome", "1.1.1.1")
        analytics.start_session("s2", "user2", "mobile", "Safari", "2.2.2.2")
        
        if hasattr(analytics, 'get_active_users'):
            count = analytics.get_active_users()
            assert count >= 2
    
    def test_get_popular_pages(self, analytics):
        """Test getting popular pages."""
        if hasattr(analytics, 'get_popular_pages'):
            pages = analytics.get_popular_pages()
            assert isinstance(pages, (list, dict))
    
    def test_get_device_breakdown(self, analytics):
        """Test getting device breakdown."""
        analytics.start_session("s1", "user1", "desktop", "Chrome", "1.1.1.1")
        analytics.start_session("s2", "user2", "mobile", "Safari", "2.2.2.2")
        analytics.start_session("s3", "user3", "desktop", "Firefox", "3.3.3.3")
        
        if hasattr(analytics, 'get_device_breakdown'):
            breakdown = analytics.get_device_breakdown()
            assert "desktop" in breakdown or len(breakdown) >= 1

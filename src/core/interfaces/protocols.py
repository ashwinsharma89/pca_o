"""
Protocol definitions for PCA system components.

Protocols define interfaces without implementation, enabling:
- Type checking without concrete dependencies
- Easy mocking for tests
- Clear contracts between components
- Dependency inversion principle
"""

from typing import Protocol, Dict, Any, List, Optional, TypeVar, Generic
import pandas as pd
from datetime import datetime


# ============================================================================
# Knowledge Base Protocols
# ============================================================================

class IRetriever(Protocol):
    """Protocol for RAG retrieval systems."""
    
    def retrieve(
        self,
        query: str,
        top_k: int = 5,
        filters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Retrieve relevant documents for a query.
        
        Args:
            query: Search query
            top_k: Number of results to return
            filters: Optional filters (platform, objective, etc.)
            
        Returns:
            List of retrieved documents with scores
        """
        ...
    
    def add_documents(
        self,
        documents: List[Dict[str, Any]]
    ) -> None:
        """Add documents to the retrieval system."""
        ...


class IBenchmarkEngine(Protocol):
    """Protocol for benchmark data providers."""
    
    def get_benchmarks(
        self,
        platform: str,
        objective: str,
        industry: Optional[str] = None
    ) -> Dict[str, float]:
        """
        Get benchmark metrics for given context.
        
        Args:
            platform: Advertising platform (e.g., 'meta', 'google')
            objective: Campaign objective (e.g., 'conversions', 'awareness')
            industry: Optional industry vertical
            
        Returns:
            Dictionary of metric benchmarks (CTR, CPC, etc.)
        """
        ...
    
    def update_benchmarks(
        self,
        platform: str,
        objective: str,
        metrics: Dict[str, float]
    ) -> None:
        """Update benchmark data."""
        ...


class IKnowledgeBase(Protocol):
    """Protocol for general knowledge base access."""
    
    def query(
        self,
        question: str,
        context: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Query the knowledge base.
        
        Args:
            question: Natural language question
            context: Optional context for the query
            
        Returns:
            Answer from knowledge base
        """
        ...
    
    def get_optimization_strategies(
        self,
        platform: str,
        issue: str
    ) -> List[str]:
        """Get optimization strategies for a specific issue."""
        ...


# ============================================================================
# Agent Protocols
# ============================================================================

class IReasoningAgent(Protocol):
    """Protocol for reasoning agents."""
    
    def analyze(
        self,
        campaign_data: pd.DataFrame,
        channel_insights: Optional[Dict[str, Any]] = None,
        campaign_context: Optional[Any] = None
    ) -> Dict[str, Any]:
        """
        Perform comprehensive analysis.
        
        Args:
            campaign_data: Campaign performance data
            channel_insights: Optional channel-specific insights
            campaign_context: Optional business context
            
        Returns:
            Analysis results with insights and recommendations
        """
        ...
    
    def get_confidence(self) -> float:
        """Get confidence score for last analysis."""
        ...


class IChannelSpecialist(Protocol):
    """Protocol for channel-specific specialist agents."""
    
    def analyze_channel(
        self,
        campaign_data: pd.DataFrame,
        platform: str
    ) -> Dict[str, Any]:
        """
        Perform channel-specific analysis.
        
        Args:
            campaign_data: Campaign data for the channel
            platform: Platform identifier
            
        Returns:
            Channel-specific insights and recommendations
        """
        ...
    
    def get_platform_best_practices(self, platform: str) -> List[str]:
        """Get best practices for the platform."""
        ...


class IVisionAgent(Protocol):
    """Protocol for vision/screenshot analysis agents."""
    
    def analyze_screenshot(
        self,
        image_path: str,
        platform: str
    ) -> Dict[str, Any]:
        """
        Analyze a screenshot from ad platform.
        
        Args:
            image_path: Path to screenshot image
            platform: Platform the screenshot is from
            
        Returns:
            Extracted metrics and insights
        """
        ...
    
    def extract_metrics(
        self,
        image_path: str
    ) -> Dict[str, float]:
        """Extract numerical metrics from image."""
        ...


class IOrchestrator(Protocol):
    """Protocol for multi-agent orchestrators."""
    
    def run(
        self,
        query: str,
        campaign_data: pd.DataFrame,
        platform: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Orchestrate multiple agents to answer query.
        
        Args:
            query: User's natural language query
            campaign_data: Campaign performance data
            platform: Optional platform filter
            
        Returns:
            Comprehensive analysis from multiple agents
        """
        ...
    
    def get_agent_status(self) -> Dict[str, str]:
        """Get status of all managed agents."""
        ...


# ============================================================================
# Service Protocols
# ============================================================================

class IAnalyticsService(Protocol):
    """Protocol for analytics services."""
    
    def generate_insights(
        self,
        data: pd.DataFrame,
        context: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Generate insights from data."""
        ...
    
    def create_report(
        self,
        data: pd.DataFrame,
        report_type: str
    ) -> Dict[str, Any]:
        """Create an analytics report."""
        ...


class ICampaignService(Protocol):
    """Protocol for campaign data services."""
    
    def get_campaign(self, campaign_id: str) -> Optional[Dict[str, Any]]:
        """Get campaign by ID."""
        ...
    
    def list_campaigns(
        self,
        user_id: str,
        filters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """List campaigns for a user."""
        ...
    
    def save_campaign(
        self,
        campaign_data: Dict[str, Any]
    ) -> str:
        """Save campaign data, return campaign ID."""
        ...


class IUserService(Protocol):
    """Protocol for user management services."""
    
    def get_user(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Get user by ID."""
        ...
    
    def authenticate(
        self,
        username: str,
        password: str
    ) -> Optional[str]:
        """Authenticate user, return token if successful."""
        ...
    
    def create_user(
        self,
        username: str,
        email: str,
        password: str
    ) -> str:
        """Create new user, return user ID."""
        ...


# ============================================================================
# Data Access Protocols
# ============================================================================

T = TypeVar('T')


class IRepository(Protocol, Generic[T]):
    """Generic repository protocol for data access."""
    
    def get(self, id: str) -> Optional[T]:
        """Get entity by ID."""
        ...
    
    def list(
        self,
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[T]:
        """List entities with optional filters."""
        ...
    
    def create(self, entity: T) -> T:
        """Create new entity."""
        ...
    
    def update(self, id: str, entity: T) -> T:
        """Update existing entity."""
        ...
    
    def delete(self, id: str) -> bool:
        """Delete entity by ID."""
        ...


class ICache(Protocol):
    """Protocol for caching systems."""
    
    def get(self, key: str) -> Optional[Any]:
        """Get value from cache."""
        ...
    
    def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[int] = None
    ) -> None:
        """Set value in cache with optional TTL."""
        ...
    
    def delete(self, key: str) -> None:
        """Delete key from cache."""
        ...
    
    def clear(self) -> None:
        """Clear all cache entries."""
        ...


class IDatabase(Protocol):
    """Protocol for database operations."""
    
    def execute(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Execute a query and return results."""
        ...
    
    def execute_many(
        self,
        query: str,
        params_list: List[Dict[str, Any]]
    ) -> None:
        """Execute query with multiple parameter sets."""
        ...
    
    def begin_transaction(self) -> None:
        """Begin a database transaction."""
        ...
    
    def commit(self) -> None:
        """Commit current transaction."""
        ...
    
    def rollback(self) -> None:
        """Rollback current transaction."""
        ...


# ============================================================================
# Pattern Detection Protocol
# ============================================================================

class IPatternDetector(Protocol):
    """Protocol for pattern detection in campaign data."""
    
    def detect_all(
        self,
        campaign_data: pd.DataFrame
    ) -> Dict[str, Any]:
        """
        Detect all patterns in campaign data.
        
        Returns:
            Dictionary of detected patterns (trends, anomalies, etc.)
        """
        ...
    
    def detect_trends(
        self,
        data: pd.DataFrame
    ) -> Dict[str, Any]:
        """Detect performance trends."""
        ...
    
    def detect_anomalies(
        self,
        data: pd.DataFrame
    ) -> Dict[str, Any]:
        """Detect anomalies in metrics."""
        ...


# ============================================================================
# Validation Protocol
# ============================================================================

class IValidator(Protocol):
    """Protocol for data validation."""
    
    def validate(self, data: Any) -> bool:
        """Validate data, return True if valid."""
        ...
    
    def get_errors(self) -> List[str]:
        """Get validation errors from last validation."""
        ...

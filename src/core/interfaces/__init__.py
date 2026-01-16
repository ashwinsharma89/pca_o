"""
Interface definitions for the PCA system.

This package contains protocols and abstract base classes that define
contracts between components, enabling dependency injection and loose coupling.
"""

from .protocols import (
    # Knowledge Base Protocols
    IRetriever,
    IBenchmarkEngine,
    IKnowledgeBase,
    
    # Agent Protocols
    IReasoningAgent,
    IChannelSpecialist,
    IVisionAgent,
    IOrchestrator,
    
    # Service Protocols
    IAnalyticsService,
    ICampaignService,
    IUserService,
    
    # Data Access Protocols
    IRepository,
    ICache,
    IDatabase,
)

from .base_classes import (
    BaseAgent,
    BaseRepository,
    BaseService,
)

__all__ = [
    # Protocols
    'IRetriever',
    'IBenchmarkEngine',
    'IKnowledgeBase',
    'IReasoningAgent',
    'IChannelSpecialist',
    'IVisionAgent',
    'IOrchestrator',
    'IAnalyticsService',
    'ICampaignService',
    'IUserService',
    'IRepository',
    'ICache',
    'IDatabase',
    
    # Base Classes
    'BaseAgent',
    'BaseRepository',
    'BaseService',
]

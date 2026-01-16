"""
Event Bus implementation for the PCA system.

Provides publish/subscribe pattern for loose coupling between components.
Supports synchronous and asynchronous event handling.
"""

from typing import Callable, Dict, List, Any, Optional, Set
from collections import defaultdict
import threading
import asyncio
from datetime import datetime
import traceback
from loguru import logger

from src.core.events.event_types import BaseEvent, EventPriority


# ============================================================================
# Event Handler Type
# ============================================================================

EventHandler = Callable[[BaseEvent], None]
AsyncEventHandler = Callable[[BaseEvent], asyncio.Future]


# ============================================================================
# Event Bus
# ============================================================================

class EventBus:
    """
    Central event bus for publish/subscribe pattern.
    
    Features:
    - Subscribe to specific event types
    - Publish events to all subscribers
    - Support for sync and async handlers
    - Thread-safe
    - Event history for debugging
    - Priority-based handling
    
    Example:
        bus = EventBus()
        
        # Subscribe to events
        def on_analysis_complete(event):
            print(f"Analysis done: {event.result}")
        
        bus.subscribe('agent.analysis.completed', on_analysis_complete)
        
        # Publish events
        event = AgentAnalysisCompleted(result={'insights': []})
        bus.publish(event)
    """
    
    def __init__(self, max_history: int = 1000):
        """
        Initialize event bus.
        
        Args:
            max_history: Maximum number of events to keep in history
        """
        self._subscribers: Dict[str, List[EventHandler]] = defaultdict(list)
        self._async_subscribers: Dict[str, List[AsyncEventHandler]] = defaultdict(list)
        self._wildcard_subscribers: List[EventHandler] = []
        self._event_history: List[BaseEvent] = []
        self._max_history = max_history
        self._lock = threading.Lock()
        self._enabled = True
        
        logger.info("Event bus initialized")
    
    def subscribe(
        self,
        event_type: str,
        handler: EventHandler,
        async_handler: bool = False
    ) -> None:
        """
        Subscribe to events of a specific type.
        
        Args:
            event_type: Type of event to subscribe to (e.g., 'agent.analysis.completed')
                       Use '*' to subscribe to all events
            handler: Function to call when event is published
            async_handler: Whether the handler is async
        """
        with self._lock:
            if event_type == '*':
                self._wildcard_subscribers.append(handler)
                logger.debug(f"Subscribed wildcard handler: {handler.__name__}")
            elif async_handler:
                self._async_subscribers[event_type].append(handler)
                logger.debug(f"Subscribed async handler to {event_type}: {handler.__name__}")
            else:
                self._subscribers[event_type].append(handler)
                logger.debug(f"Subscribed handler to {event_type}: {handler.__name__}")
    
    def unsubscribe(
        self,
        event_type: str,
        handler: EventHandler
    ) -> None:
        """
        Unsubscribe from events.
        
        Args:
            event_type: Type of event to unsubscribe from
            handler: Handler to remove
        """
        with self._lock:
            if event_type == '*':
                if handler in self._wildcard_subscribers:
                    self._wildcard_subscribers.remove(handler)
            elif handler in self._subscribers[event_type]:
                self._subscribers[event_type].remove(handler)
            elif handler in self._async_subscribers[event_type]:
                self._async_subscribers[event_type].remove(handler)
            
            logger.debug(f"Unsubscribed handler from {event_type}: {handler.__name__}")
    
    def publish(self, event: BaseEvent) -> None:
        """
        Publish an event to all subscribers.
        
        Args:
            event: Event to publish
        """
        if not self._enabled:
            return
        
        # Add to history
        with self._lock:
            self._event_history.append(event)
            if len(self._event_history) > self._max_history:
                self._event_history.pop(0)
        
        # Get handlers
        event_type = event.event_type
        handlers = self._subscribers.get(event_type, []).copy()
        async_handlers = self._async_subscribers.get(event_type, []).copy()
        wildcard_handlers = self._wildcard_subscribers.copy()
        
        # Log event
        logger.debug(
            f"Publishing event: {event_type}",
            extra={
                'event_id': event.event_id,
                'priority': event.priority.value,
                'handlers_count': len(handlers) + len(async_handlers) + len(wildcard_handlers)
            }
        )
        
        # Call synchronous handlers
        for handler in handlers + wildcard_handlers:
            try:
                handler(event)
            except Exception as e:
                logger.error(
                    f"Error in event handler {handler.__name__}: {e}",
                    extra={
                        'event_type': event_type,
                        'event_id': event.event_id,
                        'error': str(e)
                    }
                )
        
        # Call async handlers
        for handler in async_handlers:
            try:
                asyncio.create_task(handler(event))
            except Exception as e:
                logger.error(
                    f"Error in async event handler {handler.__name__}: {e}",
                    extra={
                        'event_type': event_type,
                        'event_id': event.event_id,
                        'error': str(e)
                    }
                )
    
    async def publish_async(self, event: BaseEvent) -> None:
        """
        Publish an event asynchronously.
        
        Args:
            event: Event to publish
        """
        if not self._enabled:
            return
        
        # Add to history
        with self._lock:
            self._event_history.append(event)
            if len(self._event_history) > self._max_history:
                self._event_history.pop(0)
        
        # Get handlers
        event_type = event.event_type
        handlers = self._subscribers.get(event_type, []).copy()
        async_handlers = self._async_subscribers.get(event_type, []).copy()
        wildcard_handlers = self._wildcard_subscribers.copy()
        
        # Log event
        logger.debug(
            f"Publishing async event: {event_type}",
            extra={
                'event_id': event.event_id,
                'priority': event.priority.value
            }
        )
        
        # Call all handlers
        tasks = []
        
        # Sync handlers in executor
        for handler in handlers + wildcard_handlers:
            try:
                await asyncio.get_event_loop().run_in_executor(None, handler, event)
            except Exception as e:
                logger.error(f"Error in event handler {handler.__name__}: {e}")
        
        # Async handlers
        for handler in async_handlers:
            try:
                tasks.append(handler(event))
            except Exception as e:
                logger.error(f"Error in async event handler {handler.__name__}: {e}")
        
        # Wait for all async handlers
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
    
    def get_history(
        self,
        event_type: Optional[str] = None,
        limit: int = 100
    ) -> List[BaseEvent]:
        """
        Get event history.
        
        Args:
            event_type: Filter by event type (None for all)
            limit: Maximum number of events to return
            
        Returns:
            List of events
        """
        with self._lock:
            if event_type:
                events = [e for e in self._event_history if e.event_type == event_type]
            else:
                events = self._event_history.copy()
            
            return events[-limit:]
    
    def clear_history(self) -> None:
        """Clear event history."""
        with self._lock:
            self._event_history.clear()
            logger.info("Event history cleared")
    
    def get_subscriber_count(self, event_type: Optional[str] = None) -> int:
        """
        Get number of subscribers.
        
        Args:
            event_type: Event type to check (None for all)
            
        Returns:
            Number of subscribers
        """
        with self._lock:
            if event_type:
                return (
                    len(self._subscribers.get(event_type, [])) +
                    len(self._async_subscribers.get(event_type, []))
                )
            else:
                return (
                    sum(len(handlers) for handlers in self._subscribers.values()) +
                    sum(len(handlers) for handlers in self._async_subscribers.values()) +
                    len(self._wildcard_subscribers)
                )
    
    def enable(self) -> None:
        """Enable event publishing."""
        self._enabled = True
        logger.info("Event bus enabled")
    
    def disable(self) -> None:
        """Disable event publishing."""
        self._enabled = False
        logger.info("Event bus disabled")
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get event bus statistics.
        
        Returns:
            Dictionary with statistics
        """
        with self._lock:
            event_type_counts = defaultdict(int)
            for event in self._event_history:
                event_type_counts[event.event_type] += 1
            
            return {
                'enabled': self._enabled,
                'total_subscribers': self.get_subscriber_count(),
                'event_types_subscribed': len(self._subscribers) + len(self._async_subscribers),
                'wildcard_subscribers': len(self._wildcard_subscribers),
                'history_size': len(self._event_history),
                'max_history': self._max_history,
                'event_type_counts': dict(event_type_counts)
            }


# ============================================================================
# Global Event Bus Instance
# ============================================================================

_global_event_bus: Optional[EventBus] = None
_bus_lock = threading.Lock()


def get_event_bus() -> EventBus:
    """
    Get the global event bus instance (singleton).
    
    Returns:
        EventBus instance
    """
    global _global_event_bus
    
    if _global_event_bus is None:
        with _bus_lock:
            if _global_event_bus is None:
                _global_event_bus = EventBus()
    
    return _global_event_bus


def reset_event_bus() -> None:
    """Reset the global event bus (mainly for testing)."""
    global _global_event_bus
    
    with _bus_lock:
        _global_event_bus = None

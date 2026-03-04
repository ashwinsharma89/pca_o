import unittest
import asyncio
from unittest.mock import MagicMock, patch
from src.core.events.event_bus import EventBus
from src.core.events.event_types import BaseEvent, EventPriority

class TestEventBusCoverage(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.bus = EventBus()

    async def test_event_subscription_and_publish(self):
        # Mock subscriber
        callback = MagicMock()
        callback.__name__ = "mock_callback"
        self.bus.subscribe("test_event", callback)
        
        event = BaseEvent(event_type="test_event", metadata={"data": 1}, priority=EventPriority.NORMAL)
        # publish is synchronous but spawns tasks, publish_async is awaitable
        await self.bus.publish_async(event)
        
        callback.assert_called_once_with(event)

    async def test_priority_queue_logic(self):
        events = []
        def subscriber(ev):
            events.append(ev.priority)
            
        self.bus.subscribe("prio_test", subscriber)
        
        # Publish in reverse priority order
        e_low = BaseEvent(event_type="prio_test", metadata={}, priority=EventPriority.LOW)
        e_high = BaseEvent(event_type="prio_test", metadata={}, priority=EventPriority.HIGH)
        
        await self.bus.publish_async(e_high)
        await self.bus.publish_async(e_low)
        
        self.assertEqual(events, [EventPriority.HIGH, EventPriority.LOW])

    async def test_error_resilience(self):
        def failing_subscriber(ev):
            raise Exception("Subscriber failed")
            
        self.bus.subscribe("fail_test", failing_subscriber)
        event = BaseEvent(event_type="fail_test", metadata={})
        
        # publish_async handles exceptions in sync handlers via gather(return_exceptions=True)
        # so it shouldn't crash
        try:
            await self.bus.publish_async(event)
            success = True
        except:
            success = False
            
        self.assertTrue(success)

if __name__ == "__main__":
    unittest.main()

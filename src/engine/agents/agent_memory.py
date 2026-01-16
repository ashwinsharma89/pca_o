"""
Agent Memory System
Persistent context and conversation history between sessions
"""
import os
import json
import hashlib
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field, asdict
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

# Redis for production, file for development
try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False


@dataclass
class MemoryEntry:
    """Single memory entry"""
    id: str
    user_id: str
    session_id: str
    memory_type: str  # conversation, insight, preference, context
    content: Dict[str, Any]
    timestamp: datetime = field(default_factory=datetime.utcnow)
    ttl_hours: int = 168  # 7 days default
    importance: float = 0.5  # 0-1 for memory consolidation


@dataclass
class ConversationContext:
    """Conversation context for an agent session"""
    session_id: str
    user_id: str
    messages: List[Dict[str, str]] = field(default_factory=list)
    current_campaign: Optional[str] = None
    current_platform: Optional[str] = None
    analysis_context: Dict[str, Any] = field(default_factory=dict)
    user_preferences: Dict[str, Any] = field(default_factory=dict)
    last_activity: datetime = field(default_factory=datetime.utcnow)


class AgentMemory:
    """
    Persistent memory system for AI agents
    
    Features:
    - Conversation history
    - User preferences
    - Analysis context
    - Cross-session learning
    
    Usage:
        memory = AgentMemory(user_id="user_123")
        memory.remember("insight", {"finding": "CPC increased 30%"})
        context = memory.get_context()
    """
    
    MEMORY_DIR = Path(os.getenv("AGENT_MEMORY_DIR", "./agent_memory"))
    REDIS_URL = os.getenv("REDIS_URL", "")
    
    def __init__(self, user_id: str, session_id: str = None):
        self.user_id = user_id
        self.session_id = session_id or self._generate_session_id()
        self.context = ConversationContext(
            session_id=self.session_id,
            user_id=user_id
        )
        self._redis = self._init_redis()
        self._load_user_context()
    
    def _generate_session_id(self) -> str:
        return hashlib.sha256(f"{self.user_id}:{datetime.utcnow().isoformat()}".encode()).hexdigest()[:16]
    
    def _init_redis(self):
        if REDIS_AVAILABLE and self.REDIS_URL:
            try:
                return redis.from_url(self.REDIS_URL)
            except Exception as e:
                logger.warning(f"Redis connection failed: {e}")
        return None
    
    def _get_storage_path(self) -> Path:
        user_dir = self.MEMORY_DIR / self.user_id
        user_dir.mkdir(parents=True, exist_ok=True)
        return user_dir
    
    def _load_user_context(self):
        """Load user's persistent context"""
        # Load preferences
        prefs = self.recall("preference", limit=10)
        for pref in prefs:
            self.context.user_preferences.update(pref.content)
        
        # Load recent insights
        insights = self.recall("insight", limit=5)
        self.context.analysis_context["recent_insights"] = [i.content for i in insights]
    
    # =========================================================================
    # Core Memory Operations
    # =========================================================================
    
    def remember(
        self,
        memory_type: str,
        content: Dict[str, Any],
        importance: float = 0.5,
        ttl_hours: int = 168
    ) -> MemoryEntry:
        """
        Store a memory
        
        Args:
            memory_type: conversation, insight, preference, context
            content: Memory content
            importance: 0-1 importance score
            ttl_hours: Time to live in hours
        """
        entry = MemoryEntry(
            id=hashlib.sha256(f"{self.user_id}:{datetime.utcnow().isoformat()}:{memory_type}".encode()).hexdigest()[:16],
            user_id=self.user_id,
            session_id=self.session_id,
            memory_type=memory_type,
            content=content,
            importance=importance,
            ttl_hours=ttl_hours
        )
        
        self._store(entry)
        return entry
    
    def recall(
        self,
        memory_type: str = None,
        limit: int = 10,
        min_importance: float = 0.0
    ) -> List[MemoryEntry]:
        """
        Retrieve memories
        
        Args:
            memory_type: Filter by type (optional)
            limit: Max memories to return
            min_importance: Minimum importance threshold
        """
        memories = self._load_all()
        
        # Filter by type
        if memory_type:
            memories = [m for m in memories if m.memory_type == memory_type]
        
        # Filter by importance
        memories = [m for m in memories if m.importance >= min_importance]
        
        # Filter expired
        now = datetime.utcnow()
        memories = [m for m in memories if m.timestamp + timedelta(hours=m.ttl_hours) > now]
        
        # Sort by timestamp (newest first)
        memories.sort(key=lambda m: m.timestamp, reverse=True)
        
        return memories[:limit]
    
    def forget(self, memory_id: str = None, memory_type: str = None):
        """Remove memories"""
        if self._redis:
            if memory_id:
                self._redis.delete(f"memory:{self.user_id}:{memory_id}")
            elif memory_type:
                keys = self._redis.keys(f"memory:{self.user_id}:*")
                for key in keys:
                    data = self._redis.get(key)
                    if data:
                        entry = json.loads(data)
                        if entry.get("memory_type") == memory_type:
                            self._redis.delete(key)
        else:
            # File-based deletion
            storage_path = self._get_storage_path()
            memories_file = storage_path / "memories.json"
            if memories_file.exists():
                with open(memories_file, 'r') as f:
                    memories = json.load(f)
                
                if memory_id:
                    memories = [m for m in memories if m["id"] != memory_id]
                elif memory_type:
                    memories = [m for m in memories if m["memory_type"] != memory_type]
                
                with open(memories_file, 'w') as f:
                    json.dump(memories, f)
    
    # =========================================================================
    # Conversation Tracking
    # =========================================================================
    
    def add_message(self, role: str, content: str):
        """Add message to conversation history"""
        self.context.messages.append({
            "role": role,
            "content": content,
            "timestamp": datetime.utcnow().isoformat()
        })
        
        # Keep last 20 messages in active context
        if len(self.context.messages) > 20:
            self.context.messages = self.context.messages[-20:]
        
        self.context.last_activity = datetime.utcnow()
    
    def get_conversation_history(self, limit: int = 10) -> List[Dict]:
        """Get recent conversation history"""
        return self.context.messages[-limit:]
    
    def set_campaign_context(self, campaign_name: str, platform: str = None):
        """Set current campaign context"""
        self.context.current_campaign = campaign_name
        if platform:
            self.context.current_platform = platform
    
    def update_preferences(self, preferences: Dict[str, Any]):
        """Update user preferences"""
        self.context.user_preferences.update(preferences)
        self.remember("preference", preferences, importance=0.8, ttl_hours=720)  # 30 days
    
    # =========================================================================
    # Context Retrieval
    # =========================================================================
    
    def get_context(self) -> Dict[str, Any]:
        """Get full context for agent reasoning"""
        return {
            "session_id": self.session_id,
            "user_id": self.user_id,
            "current_campaign": self.context.current_campaign,
            "current_platform": self.context.current_platform,
            "conversation_history": self.get_conversation_history(5),
            "preferences": self.context.user_preferences,
            "recent_insights": self.context.analysis_context.get("recent_insights", []),
            "analysis_context": self.context.analysis_context
        }
    
    def get_summary(self) -> str:
        """Get a text summary of the current context"""
        ctx = self.get_context()
        parts = []
        
        if ctx["current_campaign"]:
            parts.append(f"Currently analyzing: {ctx['current_campaign']}")
        if ctx["current_platform"]:
            parts.append(f"Platform: {ctx['current_platform']}")
        if ctx["preferences"]:
            parts.append(f"User preferences: {', '.join(ctx['preferences'].keys())}")
        if ctx["recent_insights"]:
            parts.append(f"Recent insights: {len(ctx['recent_insights'])} findings")
        
        return " | ".join(parts) if parts else "New session"
    
    # =========================================================================
    # Storage Backend
    # =========================================================================
    
    def _store(self, entry: MemoryEntry):
        """Store memory entry"""
        data = asdict(entry)
        data["timestamp"] = entry.timestamp.isoformat()
        
        if self._redis:
            key = f"memory:{self.user_id}:{entry.id}"
            self._redis.setex(key, entry.ttl_hours * 3600, json.dumps(data))
        else:
            # File-based storage
            storage_path = self._get_storage_path()
            memories_file = storage_path / "memories.json"
            
            memories = []
            if memories_file.exists():
                with open(memories_file, 'r') as f:
                    memories = json.load(f)
            
            memories.append(data)
            
            with open(memories_file, 'w') as f:
                json.dump(memories, f, indent=2)
    
    def _load_all(self) -> List[MemoryEntry]:
        """Load all memories for user"""
        memories = []
        
        if self._redis:
            keys = self._redis.keys(f"memory:{self.user_id}:*")
            for key in keys:
                data = self._redis.get(key)
                if data:
                    entry_data = json.loads(data)
                    entry_data["timestamp"] = datetime.fromisoformat(entry_data["timestamp"])
                    memories.append(MemoryEntry(**entry_data))
        else:
            storage_path = self._get_storage_path()
            memories_file = storage_path / "memories.json"
            
            if memories_file.exists():
                with open(memories_file, 'r') as f:
                    for entry_data in json.load(f):
                        entry_data["timestamp"] = datetime.fromisoformat(entry_data["timestamp"])
                        memories.append(MemoryEntry(**entry_data))
        
        return memories
    
    def save_session(self):
        """Save current session state"""
        self.remember(
            "context",
            {
                "campaign": self.context.current_campaign,
                "platform": self.context.current_platform,
                "messages": self.context.messages[-10:],
                "analysis": self.context.analysis_context
            },
            importance=0.6,
            ttl_hours=24
        )


# Global memory instances
_memories: Dict[str, AgentMemory] = {}

def get_agent_memory(user_id: str, session_id: str = None) -> AgentMemory:
    """Get or create agent memory for a user"""
    key = f"{user_id}:{session_id}" if session_id else user_id
    if key not in _memories:
        _memories[key] = AgentMemory(user_id, session_id)
    return _memories[key]

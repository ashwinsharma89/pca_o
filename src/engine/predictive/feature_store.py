"""
Feature Store for ML Models
Centralized storage and retrieval of ML features
"""
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
import hashlib
import json
import logging
from collections import defaultdict

logger = logging.getLogger(__name__)


@dataclass
class FeatureDefinition:
    """Definition of a feature"""
    name: str
    dtype: str  # int, float, string, list, etc.
    description: str = ""
    entity: str = "user"  # Entity this feature belongs to
    ttl_hours: int = 24  # Time to live
    default: Any = None
    tags: List[str] = field(default_factory=list)


@dataclass
class FeatureValue:
    """Stored feature value"""
    feature_name: str
    entity_id: str
    value: Any
    timestamp: datetime = field(default_factory=datetime.utcnow)


class FeatureStore:
    """
    Feature Store for ML features
    
    Features:
    - Feature registration and discovery
    - Online feature serving
    - Offline feature retrieval
    - Point-in-time correctness
    
    Usage:
        store = FeatureStore()
        store.register_feature("user_spend_30d", "float", entity="user")
        store.set_feature("user_spend_30d", "user_123", 1500.0)
        value = store.get_feature("user_spend_30d", "user_123")
    """
    
    def __init__(self, redis_client=None):
        self.definitions: Dict[str, FeatureDefinition] = {}
        self.online_store: Dict[str, Dict[str, FeatureValue]] = defaultdict(dict)
        self.offline_store: List[FeatureValue] = []  # For historical features
        self.redis = redis_client  # Optional Redis for production
    
    def register_feature(
        self,
        name: str,
        dtype: str,
        entity: str = "user",
        description: str = "",
        ttl_hours: int = 24,
        default: Any = None,
        tags: List[str] = None
    ) -> FeatureDefinition:
        """Register a new feature definition"""
        definition = FeatureDefinition(
            name=name,
            dtype=dtype,
            entity=entity,
            description=description,
            ttl_hours=ttl_hours,
            default=default,
            tags=tags or []
        )
        
        self.definitions[name] = definition
        logger.info(f"Registered feature: {name} (entity={entity}, dtype={dtype})")
        return definition
    
    def set_feature(
        self,
        feature_name: str,
        entity_id: str,
        value: Any,
        timestamp: datetime = None
    ):
        """Set a feature value for an entity"""
        if feature_name not in self.definitions:
            raise ValueError(f"Feature '{feature_name}' not registered")
        
        feature_value = FeatureValue(
            feature_name=feature_name,
            entity_id=entity_id,
            value=value,
            timestamp=timestamp or datetime.utcnow()
        )
        
        # Store in online store
        self.online_store[feature_name][entity_id] = feature_value
        
        # Also store in offline store for historical access
        self.offline_store.append(feature_value)
        
        # Optionally store in Redis
        if self.redis:
            key = f"feature:{feature_name}:{entity_id}"
            ttl = self.definitions[feature_name].ttl_hours * 3600
            self.redis.setex(key, ttl, json.dumps({"value": value, "timestamp": feature_value.timestamp.isoformat()}))
    
    def get_feature(
        self,
        feature_name: str,
        entity_id: str,
        default: Any = None
    ) -> Any:
        """Get current feature value for an entity"""
        if feature_name not in self.definitions:
            return default
        
        # Check Redis first if available
        if self.redis:
            key = f"feature:{feature_name}:{entity_id}"
            cached = self.redis.get(key)
            if cached:
                data = json.loads(cached)
                return data["value"]
        
        # Check online store
        if entity_id in self.online_store[feature_name]:
            feature_value = self.online_store[feature_name][entity_id]
            
            # Check TTL
            definition = self.definitions[feature_name]
            if datetime.utcnow() - feature_value.timestamp < timedelta(hours=definition.ttl_hours):
                return feature_value.value
        
        return default or self.definitions[feature_name].default
    
    def get_features(
        self,
        feature_names: List[str],
        entity_id: str
    ) -> Dict[str, Any]:
        """Get multiple features for an entity"""
        return {name: self.get_feature(name, entity_id) for name in feature_names}
    
    def get_feature_vector(
        self,
        feature_names: List[str],
        entity_id: str
    ) -> List[Any]:
        """Get features as a vector (for model input)"""
        features = self.get_features(feature_names, entity_id)
        return [features.get(name) for name in feature_names]
    
    def get_historical_features(
        self,
        feature_name: str,
        entity_id: str,
        start_time: datetime,
        end_time: datetime
    ) -> List[FeatureValue]:
        """Get historical feature values (point-in-time)"""
        return [
            fv for fv in self.offline_store
            if fv.feature_name == feature_name
            and fv.entity_id == entity_id
            and start_time <= fv.timestamp <= end_time
        ]
    
    def list_features(self, entity: str = None, tag: str = None) -> List[FeatureDefinition]:
        """List registered features"""
        features = list(self.definitions.values())
        
        if entity:
            features = [f for f in features if f.entity == entity]
        if tag:
            features = [f for f in features if tag in f.tags]
        
        return features
    
    def get_feature_stats(self, feature_name: str) -> Dict:
        """Get statistics for a feature"""
        if feature_name not in self.definitions:
            return {"error": "Feature not found"}
        
        values = [fv.value for fv in self.online_store[feature_name].values()
                  if isinstance(fv.value, (int, float))]
        
        if not values:
            return {"count": 0}
        
        import numpy as np
        return {
            "count": len(values),
            "mean": np.mean(values),
            "std": np.std(values),
            "min": np.min(values),
            "max": np.max(values)
        }


# Default feature definitions for PCA Agent
DEFAULT_FEATURES = [
    ("campaign_spend_7d", "float", "campaign", "Total spend in last 7 days"),
    ("campaign_impressions_7d", "int", "campaign", "Total impressions in last 7 days"),
    ("campaign_clicks_7d", "int", "campaign", "Total clicks in last 7 days"),
    ("campaign_ctr", "float", "campaign", "Click-through rate"),
    ("campaign_cpc", "float", "campaign", "Cost per click"),
    ("campaign_roas", "float", "campaign", "Return on ad spend"),
    ("user_total_campaigns", "int", "user", "Total campaigns for user"),
    ("user_avg_spend", "float", "user", "Average campaign spend"),
]


def get_feature_store() -> FeatureStore:
    """Get feature store with default features registered"""
    store = FeatureStore()
    
    for name, dtype, entity, desc in DEFAULT_FEATURES:
        store.register_feature(name, dtype, entity=entity, description=desc)
    
    return store

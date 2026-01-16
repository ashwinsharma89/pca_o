"""
Abstract base classes for PCA system components.

Base classes provide shared implementation and enforce common patterns.
Use these for stateful components that need shared logic.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, TypeVar, Generic
from datetime import datetime
from loguru import logger
import pandas as pd


# ============================================================================
# Base Agent
# ============================================================================

class BaseAgent(ABC):
    """
    Abstract base class for all agents.
    
    Provides common functionality:
    - Initialization logging
    - Error handling
    - Performance tracking
    - Standard lifecycle methods
    """
    
    def __init__(self, name: Optional[str] = None):
        """
        Initialize base agent.
        
        Args:
            name: Optional agent name for logging
        """
        self.name = name or self.__class__.__name__
        self.initialized_at = datetime.utcnow()
        self._execution_count = 0
        self._total_execution_time = 0.0
        
        logger.info(f"Initialized {self.name}")
    
    @abstractmethod
    def analyze(self, *args, **kwargs) -> Dict[str, Any]:
        """
        Main analysis method - must be implemented by subclasses.
        
        Returns:
            Analysis results dictionary
        """
        pass
    
    def get_stats(self) -> Dict[str, Any]:
        """Get agent execution statistics."""
        avg_time = (
            self._total_execution_time / self._execution_count
            if self._execution_count > 0
            else 0.0
        )
        
        return {
            'name': self.name,
            'initialized_at': self.initialized_at.isoformat(),
            'execution_count': self._execution_count,
            'total_execution_time': self._total_execution_time,
            'average_execution_time': avg_time
        }
    
    def _track_execution(self, execution_time: float) -> None:
        """Track execution metrics."""
        self._execution_count += 1
        self._total_execution_time += execution_time
    
    def _handle_error(self, error: Exception, context: Dict[str, Any]) -> None:
        """Standard error handling."""
        logger.error(
            f"{self.name} error: {str(error)}",
            extra={'context': context}
        )
        raise


# ============================================================================
# Base Repository
# ============================================================================

T = TypeVar('T')


class BaseRepository(ABC, Generic[T]):
    """
    Abstract base class for repositories.
    
    Provides common CRUD operations and transaction management.
    """
    
    def __init__(self, session):
        """
        Initialize repository with database session.
        
        Args:
            session: Database session or connection
        """
        self.session = session
        self._model_class = self._get_model_class()
    
    @abstractmethod
    def _get_model_class(self) -> type:
        """Return the model class this repository manages."""
        pass
    
    def get(self, id: str) -> Optional[T]:
        """
        Get entity by ID.
        
        Args:
            id: Entity identifier
            
        Returns:
            Entity if found, None otherwise
        """
        try:
            return self.session.query(self._model_class).filter_by(id=id).first()
        except Exception as e:
            logger.error(f"Error getting {self._model_class.__name__} {id}: {e}")
            return None
    
    def list(
        self,
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[T]:
        """
        List entities with optional filters.
        
        Args:
            filters: Optional filter criteria
            limit: Maximum number of results
            offset: Number of results to skip
            
        Returns:
            List of entities
        """
        try:
            query = self.session.query(self._model_class)
            
            if filters:
                query = query.filter_by(**filters)
            
            return query.limit(limit).offset(offset).all()
        except Exception as e:
            logger.error(f"Error listing {self._model_class.__name__}: {e}")
            return []
    
    def create(self, entity: T) -> T:
        """
        Create new entity.
        
        Args:
            entity: Entity to create
            
        Returns:
            Created entity
        """
        try:
            self.session.add(entity)
            self.session.commit()
            self.session.refresh(entity)
            return entity
        except Exception as e:
            self.session.rollback()
            logger.error(f"Error creating {self._model_class.__name__}: {e}")
            raise
    
    def update(self, id: str, updates: Dict[str, Any]) -> Optional[T]:
        """
        Update existing entity.
        
        Args:
            id: Entity identifier
            updates: Dictionary of fields to update
            
        Returns:
            Updated entity if found, None otherwise
        """
        try:
            entity = self.get(id)
            if not entity:
                return None
            
            for key, value in updates.items():
                if hasattr(entity, key):
                    setattr(entity, key, value)
            
            self.session.commit()
            self.session.refresh(entity)
            return entity
        except Exception as e:
            self.session.rollback()
            logger.error(f"Error updating {self._model_class.__name__} {id}: {e}")
            raise
    
    def delete(self, id: str) -> bool:
        """
        Delete entity by ID.
        
        Args:
            id: Entity identifier
            
        Returns:
            True if deleted, False if not found
        """
        try:
            entity = self.get(id)
            if not entity:
                return False
            
            self.session.delete(entity)
            self.session.commit()
            return True
        except Exception as e:
            self.session.rollback()
            logger.error(f"Error deleting {self._model_class.__name__} {id}: {e}")
            raise
    
    def begin_transaction(self):
        """Begin a new transaction."""
        return self.session.begin()
    
    def commit(self):
        """Commit current transaction."""
        self.session.commit()
    
    def rollback(self):
        """Rollback current transaction."""
        self.session.rollback()


# ============================================================================
# Base Service
# ============================================================================

class BaseService(ABC):
    """
    Abstract base class for services.
    
    Provides common service patterns:
    - Dependency validation
    - Logging setup
    - Error handling
    """
    
    def __init__(self, name: Optional[str] = None):
        """
        Initialize base service.
        
        Args:
            name: Optional service name for logging
        """
        self.name = name or self.__class__.__name__
        self.initialized_at = datetime.utcnow()
        
        # Validate dependencies
        self._validate_dependencies()
        
        logger.info(f"Initialized {self.name}")
    
    @abstractmethod
    def _validate_dependencies(self) -> None:
        """
        Validate that all required dependencies are available.
        
        Raises:
            ValueError: If dependencies are missing or invalid
        """
        pass
    
    def _handle_error(
        self,
        error: Exception,
        operation: str,
        context: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Standard error handling for services.
        
        Args:
            error: The exception that occurred
            operation: Name of the operation that failed
            context: Optional context information
        """
        logger.error(
            f"{self.name} error in {operation}: {str(error)}",
            extra={'context': context or {}}
        )
        raise
    
    def get_status(self) -> Dict[str, Any]:
        """Get service status information."""
        return {
            'name': self.name,
            'status': 'healthy',
            'initialized_at': self.initialized_at.isoformat()
        }


# ============================================================================
# Base Pattern Detector
# ============================================================================

class BasePatternDetector(ABC):
    """
    Abstract base class for pattern detectors.
    
    Provides common pattern detection infrastructure.
    """
    
    def __init__(self):
        """Initialize pattern detector."""
        self.name = self.__class__.__name__
        logger.debug(f"Initialized {self.name}")
    
    @abstractmethod
    def detect_all(self, data: pd.DataFrame) -> Dict[str, Any]:
        """
        Detect all patterns in data.
        
        Args:
            data: Input data for pattern detection
            
        Returns:
            Dictionary of detected patterns
        """
        pass
    
    def _validate_data(self, data: pd.DataFrame, required_columns: List[str]) -> None:
        """
        Validate that data has required columns.
        
        Args:
            data: DataFrame to validate
            required_columns: List of required column names
            
        Raises:
            ValueError: If required columns are missing
        """
        missing = set(required_columns) - set(data.columns)
        if missing:
            raise ValueError(f"Missing required columns: {missing}")
    
    def _safe_calculate(
        self,
        calculation_func,
        data: pd.DataFrame,
        default_value: Any = None
    ) -> Any:
        """
        Safely execute a calculation with error handling.
        
        Args:
            calculation_func: Function to execute
            data: Data to pass to function
            default_value: Value to return on error
            
        Returns:
            Calculation result or default value
        """
        try:
            return calculation_func(data)
        except Exception as e:
            logger.warning(f"Calculation error in {self.name}: {e}")
            return default_value


# ============================================================================
# Base Cache
# ============================================================================

class BaseCache(ABC):
    """
    Abstract base class for cache implementations.
    
    Provides common caching patterns.
    """
    
    def __init__(self, prefix: str = "pca"):
        """
        Initialize cache.
        
        Args:
            prefix: Key prefix for namespacing
        """
        self.prefix = prefix
    
    def _make_key(self, key: str) -> str:
        """Create namespaced cache key."""
        return f"{self.prefix}:{key}"
    
    @abstractmethod
    def get(self, key: str) -> Optional[Any]:
        """Get value from cache."""
        pass
    
    @abstractmethod
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """Set value in cache."""
        pass
    
    @abstractmethod
    def delete(self, key: str) -> None:
        """Delete key from cache."""
        pass
    
    @abstractmethod
    def clear(self) -> None:
        """Clear all cache entries."""
        pass
    
    def get_or_set(
        self,
        key: str,
        factory_func,
        ttl: Optional[int] = None
    ) -> Any:
        """
        Get value from cache or compute and cache it.
        
        Args:
            key: Cache key
            factory_func: Function to compute value if not cached
            ttl: Optional time-to-live in seconds
            
        Returns:
            Cached or computed value
        """
        value = self.get(key)
        if value is not None:
            return value
        
        value = factory_func()
        self.set(key, value, ttl)
        return value

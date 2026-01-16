"""
Model Retraining Pipeline - Automated model retraining
Handles automated model retraining with performance monitoring
"""
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
import pandas as pd
import logging
from src.engine.predictive.model_registry import ModelRegistry

logger = logging.getLogger(__name__)


class ModelRetrainingPipeline:
    """Automated model retraining with monitoring"""
    
    def __init__(
        self,
        model_registry: ModelRegistry = None,
        min_samples: int = 1000,
        performance_threshold: float = 0.7,
        retrain_interval_days: int = 7
    ):
        """
        Initialize retraining pipeline
        
        Args:
            model_registry: Model registry instance
            min_samples: Minimum samples required for training
            performance_threshold: Minimum acceptable performance
            retrain_interval_days: Days between retraining
        """
        self.registry = model_registry or ModelRegistry()
        self.min_samples = min_samples
        self.performance_threshold = performance_threshold
        self.retrain_interval_days = retrain_interval_days
    
    def should_retrain(self, model_name: str) -> tuple[bool, str]:
        """
        Check if model needs retraining
        
        Args:
            model_name: Model name
            
        Returns:
            Tuple of (should_retrain, reason)
        """
        try:
            # Get latest model info
            model_info = self.registry.get_model_info(model_name)
            
            if not model_info:
                return True, "No model exists"
            
            # Check age
            created_ts = model_info.get("creation_timestamp")
            if created_ts:
                created_date = datetime.fromtimestamp(created_ts / 1000)  # MLflow uses milliseconds
                age_days = (datetime.now() - created_date).days
                
                if age_days > self.retrain_interval_days:
                    return True, f"Model is {age_days} days old (threshold: {self.retrain_interval_days})"
            
            # TODO: Add performance degradation check
            # This would require tracking model performance over time
            
            return False, "Model is up to date"
            
        except Exception as e:
            logger.error(f"Error checking retrain status: {e}")
            return True, f"Error checking model: {str(e)}"
    
    def retrain_model(
        self,
        model_name: str,
        training_data: pd.DataFrame,
        model_class: Any,
        params: Dict[str, Any],
        target_column: str = "target"
    ) -> str:
        """
        Retrain model
        
        Args:
            model_name: Model name
            training_data: Training data DataFrame
            model_class: Model class to instantiate
            params: Model parameters
            target_column: Target column name
            
        Returns:
            Run ID of retrained model
        """
        logger.info(f"Starting retraining for {model_name}")
        
        # Validate data
        if len(training_data) < self.min_samples:
            raise ValueError(
                f"Insufficient training data: {len(training_data)} < {self.min_samples}"
            )
        
        if target_column not in training_data.columns:
            raise ValueError(f"Target column '{target_column}' not found in data")
        
        # Prepare data
        X = training_data.drop(target_column, axis=1)
        y = training_data[target_column]
        
        # Train model
        logger.info(f"Training {model_name} with {len(training_data)} samples")
        model = model_class(**params)
        model.fit(X, y)
        
        # Evaluate
        score = model.score(X, y)
        logger.info(f"Model training score: {score:.4f}")
        
        if score < self.performance_threshold:
            logger.warning(
                f"Model performance below threshold: {score:.4f} < {self.performance_threshold}"
            )
        
        # Save new version
        run_id = self.registry.save_model(
            model=model,
            model_name=model_name,
            metrics={
                "accuracy": score,
                "training_samples": len(training_data)
            },
            params=params,
            tags={
                "retrained": "true",
                "retrain_date": datetime.now().isoformat(),
                "data_size": str(len(training_data))
            }
        )
        
        logger.info(f"Model retrained successfully: {model_name} (run_id: {run_id})")
        return run_id
    
    def auto_retrain_if_needed(
        self,
        model_name: str,
        training_data: pd.DataFrame,
        model_class: Any,
        params: Dict[str, Any],
        target_column: str = "target"
    ) -> Optional[str]:
        """
        Automatically retrain model if needed
        
        Args:
            model_name: Model name
            training_data: Training data
            model_class: Model class
            params: Model parameters
            target_column: Target column name
            
        Returns:
            Run ID if retrained, None otherwise
        """
        should_retrain, reason = self.should_retrain(model_name)
        
        if should_retrain:
            logger.info(f"Retraining {model_name}: {reason}")
            return self.retrain_model(
                model_name,
                training_data,
                model_class,
                params,
                target_column
            )
        else:
            logger.info(f"Skipping retraining for {model_name}: {reason}")
            return None

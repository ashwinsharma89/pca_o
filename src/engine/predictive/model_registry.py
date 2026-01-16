"""
ML Model Registry - Local MLflow-based model versioning
Manages model versions, metrics, and metadata locally
"""
import mlflow
import mlflow.sklearn
from typing import Optional, Dict, Any, List
from datetime import datetime
import joblib
import os
import logging

logger = logging.getLogger(__name__)


class ModelRegistry:
    """ML model versioning and registry using local MLflow"""
    
    def __init__(self, tracking_uri: str = None, experiment_name: str = "campaign_prediction"):
        """
        Initialize model registry
        
        Args:
            tracking_uri: MLflow tracking URI (default: ./mlruns)
            experiment_name: Experiment name
        """
        # Use local file storage
        if tracking_uri is None:
            tracking_uri = f"file://{os.path.abspath('./mlruns')}"
        
        mlflow.set_tracking_uri(tracking_uri)
        self.experiment_name = experiment_name
        mlflow.set_experiment(self.experiment_name)
        
        logger.info(f"Model registry initialized: {tracking_uri}")
    
    def save_model(
        self,
        model: Any,
        model_name: str,
        metrics: Dict[str, float],
        params: Dict[str, Any],
        tags: Dict[str, str] = None,
        artifacts: Dict[str, str] = None
    ) -> str:
        """
        Save model with version
        
        Args:
            model: Trained model object
            model_name: Name for the model
            metrics: Model metrics (accuracy, precision, etc.)
            params: Model parameters
            tags: Additional tags
            artifacts: Additional artifacts to log (file paths)
            
        Returns:
            Run ID
        """
        with mlflow.start_run():
            # Log parameters
            mlflow.log_params(params)
            
            # Log metrics
            mlflow.log_metrics(metrics)
            
            # Log tags
            default_tags = {
                "model_name": model_name,
                "created_at": datetime.now().isoformat()
            }
            if tags:
                default_tags.update(tags)
            mlflow.set_tags(default_tags)
            
            # Log additional artifacts
            if artifacts:
                for name, path in artifacts.items():
                    mlflow.log_artifact(path, artifact_path=name)
            
            # Log model
            mlflow.sklearn.log_model(model, model_name)
            
            # Register model
            model_uri = f"runs:/{mlflow.active_run().info.run_id}/{model_name}"
            try:
                mlflow.register_model(model_uri, model_name)
            except Exception as e:
                logger.warning(f"Model registration failed: {e}")
            
            run_id = mlflow.active_run().info.run_id
            logger.info(f"Model saved: {model_name} (run_id: {run_id})")
            return run_id
    
    def load_model(self, model_name: str, version: Optional[int] = None):
        """
        Load model by name and version
        
        Args:
            model_name: Model name
            version: Model version (default: latest)
            
        Returns:
            Loaded model
        """
        try:
            if version:
                model_uri = f"models:/{model_name}/{version}"
            else:
                model_uri = f"models:/{model_name}/latest"
            
            model = mlflow.sklearn.load_model(model_uri)
            logger.info(f"Model loaded: {model_name} (version: {version or 'latest'})")
            return model
        except Exception as e:
            logger.error(f"Model load failed: {e}")
            # Fallback: load from latest run
            return self._load_from_latest_run(model_name)
    
    def _load_from_latest_run(self, model_name: str):
        """Load model from latest run (fallback)"""
        client = mlflow.tracking.MlflowClient()
        experiment = client.get_experiment_by_name(self.experiment_name)
        
        if experiment:
            runs = client.search_runs(
                experiment_ids=[experiment.experiment_id],
                filter_string=f"tags.model_name = '{model_name}'",
                order_by=["start_time DESC"],
                max_results=1
            )
            
            if runs:
                run_id = runs[0].info.run_id
                model_uri = f"runs:/{run_id}/{model_name}"
                return mlflow.sklearn.load_model(model_uri)
        
        raise ValueError(f"No model found: {model_name}")
    
    def get_model_info(self, model_name: str, version: Optional[int] = None) -> Dict:
        """
        Get model metadata
        
        Args:
            model_name: Model name
            version: Model version
            
        Returns:
            Model metadata dictionary
        """
        client = mlflow.tracking.MlflowClient()
        
        try:
            if version:
                model_version = client.get_model_version(model_name, version)
            else:
                versions = client.search_model_versions(f"name='{model_name}'")
                if not versions:
                    raise ValueError(f"No versions found for model: {model_name}")
                model_version = versions[0]
            
            return {
                "name": model_version.name,
                "version": model_version.version,
                "creation_timestamp": model_version.creation_timestamp,
                "last_updated_timestamp": model_version.last_updated_timestamp,
                "current_stage": model_version.current_stage,
                "run_id": model_version.run_id
            }
        except Exception as e:
            logger.error(f"Failed to get model info: {e}")
            return {}
    
    def list_models(self) -> List[Dict]:
        """
        List all registered models
        
        Returns:
            List of model metadata
        """
        client = mlflow.tracking.MlflowClient()
        models = client.search_registered_models()
        
        return [
            {
                "name": model.name,
                "creation_timestamp": model.creation_timestamp,
                "last_updated_timestamp": model.last_updated_timestamp,
                "latest_versions": [
                    {"version": v.version, "stage": v.current_stage}
                    for v in model.latest_versions
                ]
            }
            for model in models
        ]
    
    def delete_model(self, model_name: str, version: Optional[int] = None):
        """
        Delete model version
        
        Args:
            model_name: Model name
            version: Model version (deletes all if None)
        """
        client = mlflow.tracking.MlflowClient()
        
        if version:
            client.delete_model_version(model_name, version)
            logger.info(f"Deleted model version: {model_name} v{version}")
        else:
            client.delete_registered_model(model_name)
            logger.info(f"Deleted model: {model_name}")

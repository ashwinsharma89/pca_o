"""
A/B Testing Framework for ML Models
Enables experimentation with model variants and traffic splitting
"""
import random
import hashlib
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Any, Optional, List
from collections import defaultdict
import logging
import json

logger = logging.getLogger(__name__)


@dataclass
class Experiment:
    """A/B Test experiment configuration"""
    id: str
    name: str
    variants: Dict[str, float]  # variant_name: traffic_percentage
    model_versions: Dict[str, str]  # variant_name: model_version
    start_date: datetime
    end_date: Optional[datetime] = None
    is_active: bool = True
    metadata: Dict = field(default_factory=dict)


@dataclass
class ExperimentResult:
    """Result of an experiment assignment"""
    experiment_id: str
    variant: str
    model_version: str
    user_id: str
    timestamp: datetime = field(default_factory=datetime.utcnow)


class ABTestingFramework:
    """
    A/B Testing for ML models
    
    Usage:
        ab = ABTestingFramework()
        ab.create_experiment("model_test", {"control": 0.5, "treatment": 0.5})
        variant = ab.get_variant("model_test", user_id="user_123")
    """
    
    def __init__(self):
        self.experiments: Dict[str, Experiment] = {}
        self.assignments: Dict[str, Dict[str, str]] = defaultdict(dict)  # user_id -> {exp_id: variant}
        self.metrics: Dict[str, Dict[str, List[float]]] = defaultdict(lambda: defaultdict(list))
    
    def create_experiment(
        self,
        experiment_id: str,
        name: str,
        variants: Dict[str, float],
        model_versions: Dict[str, str] = None
    ) -> Experiment:
        """
        Create a new A/B test experiment
        
        Args:
            experiment_id: Unique experiment identifier
            name: Experiment name
            variants: Dict of variant_name -> traffic_percentage (must sum to 1.0)
            model_versions: Dict of variant_name -> model_version
        """
        if abs(sum(variants.values()) - 1.0) > 0.001:
            raise ValueError("Variant percentages must sum to 1.0")
        
        experiment = Experiment(
            id=experiment_id,
            name=name,
            variants=variants,
            model_versions=model_versions or {v: v for v in variants},
            start_date=datetime.utcnow()
        )
        
        self.experiments[experiment_id] = experiment
        logger.info(f"Created experiment: {experiment_id} with variants {list(variants.keys())}")
        return experiment
    
    def get_variant(self, experiment_id: str, user_id: str) -> Optional[ExperimentResult]:
        """
        Get variant assignment for a user (deterministic based on user_id hash)
        
        Args:
            experiment_id: Experiment to check
            user_id: User identifier
            
        Returns:
            ExperimentResult with assigned variant
        """
        if experiment_id not in self.experiments:
            return None
        
        experiment = self.experiments[experiment_id]
        if not experiment.is_active:
            return None
        
        # Check for existing assignment
        if experiment_id in self.assignments[user_id]:
            variant = self.assignments[user_id][experiment_id]
        else:
            # Deterministic assignment based on user_id hash
            variant = self._assign_variant(user_id, experiment)
            self.assignments[user_id][experiment_id] = variant
        
        return ExperimentResult(
            experiment_id=experiment_id,
            variant=variant,
            model_version=experiment.model_versions.get(variant, variant),
            user_id=user_id
        )
    
    def _assign_variant(self, user_id: str, experiment: Experiment) -> str:
        """Deterministically assign a variant based on user_id hash"""
        hash_input = f"{experiment.id}:{user_id}"
        hash_value = int(hashlib.sha256(hash_input.encode()).hexdigest(), 16)
        bucket = (hash_value % 1000) / 1000
        
        cumulative = 0
        for variant, percentage in experiment.variants.items():
            cumulative += percentage
            if bucket < cumulative:
                return variant
        
        return list(experiment.variants.keys())[-1]
    
    def record_metric(
        self,
        experiment_id: str,
        variant: str,
        metric_name: str,
        value: float
    ):
        """Record a metric for an experiment variant"""
        key = f"{experiment_id}:{variant}"
        self.metrics[key][metric_name].append(value)
    
    def get_results(self, experiment_id: str) -> Dict:
        """Get experiment results with statistics"""
        experiment = self.experiments.get(experiment_id)
        if not experiment:
            return {"error": "Experiment not found"}
        
        results = {
            "experiment_id": experiment_id,
            "name": experiment.name,
            "variants": {}
        }
        
        for variant in experiment.variants:
            key = f"{experiment_id}:{variant}"
            variant_metrics = dict(self.metrics[key])
            
            stats = {}
            for metric, values in variant_metrics.items():
                if values:
                    stats[metric] = {
                        "count": len(values),
                        "mean": sum(values) / len(values),
                        "min": min(values),
                        "max": max(values)
                    }
            
            results["variants"][variant] = {
                "traffic": experiment.variants[variant],
                "model_version": experiment.model_versions.get(variant),
                "metrics": stats
            }
        
        return results
    
    def stop_experiment(self, experiment_id: str, winner: str = None):
        """Stop an experiment and optionally declare a winner"""
        if experiment_id in self.experiments:
            self.experiments[experiment_id].is_active = False
            self.experiments[experiment_id].end_date = datetime.utcnow()
            if winner:
                self.experiments[experiment_id].metadata["winner"] = winner
            logger.info(f"Stopped experiment: {experiment_id}, winner: {winner}")


# Global instance
_ab_framework: Optional[ABTestingFramework] = None

def get_ab_framework() -> ABTestingFramework:
    global _ab_framework
    if _ab_framework is None:
        _ab_framework = ABTestingFramework()
    return _ab_framework

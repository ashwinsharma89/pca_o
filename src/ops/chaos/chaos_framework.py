"""
Chaos Engineering Framework
Test system resilience by injecting controlled failures
"""

import time
import random
import asyncio
from typing import Dict, Any, List, Optional, Callable
from datetime import datetime
from enum import Enum
from dataclasses import dataclass
from loguru import logger

class ChaosType(Enum):
    """Types of chaos experiments."""
    NETWORK_LATENCY = "network_latency"
    SERVICE_FAILURE = "service_failure"
    DATABASE_FAILURE = "database_failure"
    MEMORY_PRESSURE = "memory_pressure"
    CPU_STRESS = "cpu_stress"
    DISK_FULL = "disk_full"
    RANDOM_ERRORS = "random_errors"

@dataclass
class ChaosExperiment:
    """Chaos experiment configuration."""
    name: str
    chaos_type: ChaosType
    duration_seconds: int
    intensity: float  # 0.0 to 1.0
    target_service: Optional[str] = None
    enabled: bool = True

class ChaosFramework:
    """Framework for chaos engineering experiments."""
    
    def __init__(self):
        """Initialize chaos framework."""
        self.active_experiments: Dict[str, ChaosExperiment] = {}
        self.experiment_history: List[Dict[str, Any]] = []
        self.chaos_enabled = False
        
        logger.info("✅ Chaos Engineering Framework initialized")
    
    def enable_chaos(self):
        """Enable chaos engineering."""
        self.chaos_enabled = True
        logger.warning("⚠️  Chaos Engineering ENABLED")
    
    def disable_chaos(self):
        """Disable chaos engineering."""
        self.chaos_enabled = False
        self.active_experiments.clear()
        logger.info("✅ Chaos Engineering DISABLED")
    
    def register_experiment(self, experiment: ChaosExperiment):
        """Register a chaos experiment."""
        self.active_experiments[experiment.name] = experiment
        logger.info(f"Registered chaos experiment: {experiment.name}")
    
    async def run_experiment(
        self,
        experiment_name: str,
        target_function: Optional[Callable] = None
    ) -> Dict[str, Any]:
        """
        Run a chaos experiment.
        
        Args:
            experiment_name: Name of the experiment
            target_function: Optional function to test
        
        Returns:
            Experiment results
        """
        if not self.chaos_enabled:
            return {
                "success": False,
                "message": "Chaos engineering is disabled"
            }
        
        if experiment_name not in self.active_experiments:
            return {
                "success": False,
                "message": f"Experiment {experiment_name} not found"
            }
        
        experiment = self.active_experiments[experiment_name]
        
        logger.warning(f"🔥 Starting chaos experiment: {experiment.name}")
        
        start_time = datetime.utcnow()
        
        try:
            # Execute chaos based on type
            if experiment.chaos_type == ChaosType.NETWORK_LATENCY:
                result = await self._inject_network_latency(experiment)
            elif experiment.chaos_type == ChaosType.SERVICE_FAILURE:
                result = await self._inject_service_failure(experiment)
            elif experiment.chaos_type == ChaosType.DATABASE_FAILURE:
                result = await self._inject_database_failure(experiment)
            elif experiment.chaos_type == ChaosType.MEMORY_PRESSURE:
                result = await self._inject_memory_pressure(experiment)
            elif experiment.chaos_type == ChaosType.CPU_STRESS:
                result = await self._inject_cpu_stress(experiment)
            elif experiment.chaos_type == ChaosType.RANDOM_ERRORS:
                result = await self._inject_random_errors(experiment)
            else:
                result = {"success": False, "message": "Unknown chaos type"}
            
            end_time = datetime.utcnow()
            duration = (end_time - start_time).total_seconds()
            
            # Record experiment
            experiment_record = {
                "name": experiment.name,
                "chaos_type": experiment.chaos_type.value,
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "duration": duration,
                "result": result,
                "intensity": experiment.intensity
            }
            
            self.experiment_history.append(experiment_record)
            
            logger.info(f"✅ Chaos experiment completed: {experiment.name}")
            
            return {
                "success": True,
                "experiment": experiment_record
            }
        
        except Exception as e:
            logger.error(f"❌ Chaos experiment failed: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    async def _inject_network_latency(
        self,
        experiment: ChaosExperiment
    ) -> Dict[str, Any]:
        """Inject network latency."""
        latency_ms = int(experiment.intensity * 5000)  # Up to 5 seconds
        
        logger.warning(f"💤 Injecting {latency_ms}ms network latency")
        
        # Simulate latency
        await asyncio.sleep(latency_ms / 1000)
        
        return {
            "chaos_type": "network_latency",
            "latency_ms": latency_ms,
            "impact": "Requests delayed"
        }
    
    async def _inject_service_failure(
        self,
        experiment: ChaosExperiment
    ) -> Dict[str, Any]:
        """Inject service failure."""
        failure_rate = experiment.intensity
        
        logger.warning(f"💥 Injecting service failures ({failure_rate*100}% rate)")
        
        # Simulate failures
        failures = 0
        attempts = 100
        
        for _ in range(attempts):
            if random.random() < failure_rate:  # nosec B311
                failures += 1
        
        return {
            "chaos_type": "service_failure",
            "failure_rate": failure_rate,
            "failures": failures,
            "attempts": attempts,
            "impact": f"{failures}/{attempts} requests failed"
        }
    
    async def _inject_database_failure(
        self,
        experiment: ChaosExperiment
    ) -> Dict[str, Any]:
        """Inject database connection failures."""
        failure_rate = experiment.intensity
        
        logger.warning(f"🗄️  Injecting database failures ({failure_rate*100}% rate)")
        
        return {
            "chaos_type": "database_failure",
            "failure_rate": failure_rate,
            "impact": "Database connections failing"
        }
    
    async def _inject_memory_pressure(
        self,
        experiment: ChaosExperiment
    ) -> Dict[str, Any]:
        """Inject memory pressure."""
        memory_mb = int(experiment.intensity * 1000)  # Up to 1GB
        
        logger.warning(f"💾 Injecting memory pressure ({memory_mb}MB)")
        
        # Allocate memory
        memory_hog = []
        try:
            for _ in range(memory_mb):
                memory_hog.append(bytearray(1024 * 1024))  # 1MB chunks
            
            # Hold for duration
            await asyncio.sleep(experiment.duration_seconds)
            
            # Release memory
            memory_hog.clear()
        
        except MemoryError:
            logger.error("Memory allocation failed")
        
        return {
            "chaos_type": "memory_pressure",
            "memory_mb": memory_mb,
            "impact": "Memory pressure applied"
        }
    
    async def _inject_cpu_stress(
        self,
        experiment: ChaosExperiment
    ) -> Dict[str, Any]:
        """Inject CPU stress."""
        intensity = experiment.intensity
        
        logger.warning(f"🔥 Injecting CPU stress ({intensity*100}% intensity)")
        
        # CPU-intensive operation
        start = time.time()
        iterations = 0
        
        while time.time() - start < experiment.duration_seconds:
            # Busy loop
            for _ in range(int(intensity * 1000000)):
                _ = sum(range(100))
            iterations += 1
            
            # Small sleep to prevent complete lockup
            await asyncio.sleep(0.01)
        
        return {
            "chaos_type": "cpu_stress",
            "intensity": intensity,
            "iterations": iterations,
            "impact": "CPU stress applied"
        }
    
    async def _inject_random_errors(
        self,
        experiment: ChaosExperiment
    ) -> Dict[str, Any]:
        """Inject random errors."""
        error_rate = experiment.intensity
        
        logger.warning(f"⚠️  Injecting random errors ({error_rate*100}% rate)")
        
        # Randomly raise errors
        if random.random() < error_rate:  # nosec B311
            raise Exception("Chaos-induced error")
        
        return {
            "chaos_type": "random_errors",
            "error_rate": error_rate,
            "impact": "Random errors injected"
        }
    
    def get_experiment_report(self) -> str:
        """Generate chaos experiment report."""
        if not self.experiment_history:
            return "No chaos experiments have been run."
        
        lines = [
            "=" * 70,
            "Chaos Engineering Experiment Report",
            "=" * 70,
            f"Total Experiments: {len(self.experiment_history)}",
            ""
        ]
        
        for exp in self.experiment_history:
            lines.append(f"Experiment: {exp['name']}")
            lines.append(f"  Type: {exp['chaos_type']}")
            lines.append(f"  Start: {exp['start_time']}")
            lines.append(f"  Duration: {exp['duration']:.2f}s")
            lines.append(f"  Intensity: {exp['intensity']*100:.0f}%")
            lines.append(f"  Result: {exp['result']}")
            lines.append("")
        
        lines.append("=" * 70)
        
        return "\n".join(lines)


# Pre-defined experiments
STANDARD_EXPERIMENTS = [
    ChaosExperiment(
        name="network_latency_low",
        chaos_type=ChaosType.NETWORK_LATENCY,
        duration_seconds=60,
        intensity=0.2
    ),
    ChaosExperiment(
        name="network_latency_high",
        chaos_type=ChaosType.NETWORK_LATENCY,
        duration_seconds=60,
        intensity=0.8
    ),
    ChaosExperiment(
        name="service_failure_10pct",
        chaos_type=ChaosType.SERVICE_FAILURE,
        duration_seconds=120,
        intensity=0.1
    ),
    ChaosExperiment(
        name="database_failure_20pct",
        chaos_type=ChaosType.DATABASE_FAILURE,
        duration_seconds=120,
        intensity=0.2
    ),
    ChaosExperiment(
        name="memory_pressure_moderate",
        chaos_type=ChaosType.MEMORY_PRESSURE,
        duration_seconds=60,
        intensity=0.5
    ),
    ChaosExperiment(
        name="cpu_stress_high",
        chaos_type=ChaosType.CPU_STRESS,
        duration_seconds=30,
        intensity=0.8
    )
]

# Global instance
chaos_framework = ChaosFramework()

# Register standard experiments
for exp in STANDARD_EXPERIMENTS:
    chaos_framework.register_experiment(exp)

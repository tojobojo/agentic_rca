"""
Telemetry Module for the Agentic RCA System.
Tracks performance metrics across the RCA workflow.
"""
import time
import logging
from functools import wraps
from typing import Dict, Optional, Callable, Any
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class PerformanceMetrics(BaseModel):
    """Track performance metrics across RCA workflow."""
    discovery_time: float = 0.0
    context_build_time: float = 0.0
    detection_time: float = 0.0
    investigation_time: float = 0.0
    total_time: float = 0.0
    steps_analyzed: int = 0
    anomalies_found: int = 0
    
    def add_phase_time(self, phase: str, duration: float):
        """Add time for a specific phase."""
        if hasattr(self, f"{phase}_time"):
            setattr(self, f"{phase}_time", duration)
        else:
            logger.warning(f"Unknown phase: {phase}")
    
    def report(self) -> str:
        """Generate performance report in Markdown format."""
        avg_per_step = self.context_build_time / max(self.steps_analyzed, 1)
        
        return f"""## Performance Metrics

- **Total Execution Time**: {self.total_time:.2f}s
- **Discovery**: {self.discovery_time:.2f}s
- **Context Building**: {self.context_build_time:.2f}s ({self.steps_analyzed} steps)
- **Anomaly Detection**: {self.detection_time:.2f}s
- **AI Investigation**: {self.investigation_time:.2f}s ({self.anomalies_found} anomalies)

**Average Time per Step**: {avg_per_step:.2f}s
"""


class PhaseTimer:
    """Context manager for timing phases."""
    
    def __init__(self, phase_name: str, metrics: Optional[PerformanceMetrics] = None):
        self.phase_name = phase_name
        self.metrics = metrics
        self.start_time = None
        self.duration = None
    
    def __enter__(self):
        self.start_time = time.time()
        logger.debug(f"Starting phase: {self.phase_name}")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.duration = time.time() - self.start_time
        logger.debug(f"Completed phase: {self.phase_name} in {self.duration:.2f}s")
        
        if self.metrics:
            self.metrics.add_phase_time(self.phase_name, self.duration)
        
        return False  # Don't suppress exceptions


def timed_phase(phase_name: str):
    """Decorator to track execution time of a phase."""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            start = time.time()
            try:
                result = func(*args, **kwargs)
                duration = time.time() - start
                logger.info(f"[Telemetry] {phase_name}: {duration:.2f}s")
                # Store duration as attribute for retrieval
                wrapper.last_duration = duration
                return result
            except Exception as e:
                duration = time.time() - start
                logger.error(f"[Telemetry] {phase_name} failed after {duration:.2f}s: {e}")
                raise
        return wrapper
    return decorator

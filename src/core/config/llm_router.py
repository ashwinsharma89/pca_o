"""
LLM Router - Intelligent Model Selection
Routes tasks to optimal LLM models based on task type
"""
import os
from enum import Enum
from typing import Dict, Any, Tuple, Optional
import logging

from src.core.utils.anthropic_helpers import create_anthropic_client

logger = logging.getLogger(__name__)


class TaskType(Enum):
    """Task types for LLM routing."""
    QUERY_INTERPRETATION = "query_interpretation"
    SQL_GENERATION = "sql_generation"
    INSIGHTS_GENERATION = "insights_generation"
    EVALUATION = "evaluation"
    VISION_ANALYSIS = "vision_analysis"
    WORKFLOW_ORCHESTRATION = "workflow_orchestration"
    CODE_GENERATION = "code_generation"
    DATA_PROCESSING = "data_processing"
    REPORT_GENERATION = "report_generation"


class LLMRouter:
    """Routes tasks to optimal LLM models based on capabilities."""
    
    MODEL_MAPPING = {
        TaskType.QUERY_INTERPRETATION: {
            "provider": "anthropic",
            "model": "claude-3-5-sonnet-20241022",
            "reason": "Multi-step agentic reasoning and tool usage",
            "max_tokens": 4096,
            "temperature": 0.7
        },
        TaskType.SQL_GENERATION: {
            "provider": "openai",
            "model": "gpt-4",  # Will be gpt-5.1-codex when available
            "reason": "Code generation specialist",
            "max_tokens": 2048,
            "temperature": 0.1
        },
        TaskType.INSIGHTS_GENERATION: {
            "provider": "openai",
            "model": "gpt-4",  # Will be gpt-5.1-high-reasoning when available
            "reason": "Strategic analysis and advanced reasoning",
            "max_tokens": 3000,
            "temperature": 0.7
        },
        TaskType.EVALUATION: {
            "provider": "google",
            "model": "gemini-2.0-flash-exp",  # Will be gemini-2.5-pro when available
            "reason": "Massive context window for large-scale analysis",
            "max_tokens": 8192,
            "temperature": 0.5
        },
        TaskType.VISION_ANALYSIS: {
            "provider": "google",
            "model": "gemini-2.0-flash-exp",  # Will be gemini-2.5-pro when available
            "reason": "Multimodal capabilities for image+text",
            "max_tokens": 4096,
            "temperature": 0.3
        },
        TaskType.WORKFLOW_ORCHESTRATION: {
            "provider": "anthropic",
            "model": "claude-3-5-sonnet-20241022",
            "reason": "Agentic reasoning with tool usage",
            "max_tokens": 4096,
            "temperature": 0.5
        },
        TaskType.CODE_GENERATION: {
            "provider": "openai",
            "model": "gpt-4",  # Will be gpt-5.1-codex when available
            "reason": "Code generation and testing specialist",
            "max_tokens": 4096,
            "temperature": 0.1
        },
        TaskType.DATA_PROCESSING: {
            "provider": "openai",
            "model": "gpt-4",  # Will be gpt-5.1-high-reasoning when available
            "reason": "Complex logic and business rules",
            "max_tokens": 3000,
            "temperature": 0.3
        },
        TaskType.REPORT_GENERATION: {
            "provider": "openai",
            "model": "gpt-4",  # Will be gpt-5.1-high-reasoning when available
            "reason": "Natural language generation with reasoning",
            "max_tokens": 4096,
            "temperature": 0.7
        }
    }
    
    @classmethod
    def get_model_config(cls, task_type: TaskType) -> Dict[str, Any]:
        """
        Get optimal model configuration for a task type.
        
        Args:
            task_type: Type of task to perform
            
        Returns:
            Dictionary with model configuration
        """
        config = cls.MODEL_MAPPING.get(task_type)
        if not config:
            raise ValueError(f"Unknown task type: {task_type}")
        
        logger.info(f"Routing {task_type.value} to {config['model']} ({config['reason']})")
        return config
    
    @classmethod
    def get_client(cls, task_type: TaskType) -> Tuple[Any, str, Dict[str, Any]]:
        """
        Get appropriate LLM client for task type.
        
        Args:
            task_type: Type of task to perform
            
        Returns:
            Tuple of (client, model_name, config)
        """
        config = cls.get_model_config(task_type)
        provider = config["provider"]
        model = config["model"]
        
        if provider == "anthropic":
            client = create_anthropic_client(os.getenv('ANTHROPIC_API_KEY'))
            if not client:
                raise RuntimeError("Anthropic client unavailable. Remove ANTHROPIC routing or install supported SDK.")
            return client, model, config
            
        elif provider == "openai":
            from openai import OpenAI
            client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
            return client, model, config
            
        elif provider == "google":
            import google.generativeai as genai
            genai.configure(api_key=os.getenv('GOOGLE_API_KEY'))
            return genai, model, config
        
        raise ValueError(f"Unknown provider: {provider}")
    
    @classmethod
    def call_llm(
        cls,
        task_type: TaskType,
        prompt: str,
        system_prompt: Optional[str] = None,
        **kwargs
    ) -> str:
        """
        Universal LLM call that routes to the appropriate model.
        
        Args:
            task_type: Type of task to perform
            prompt: User prompt
            system_prompt: Optional system prompt
            **kwargs: Additional parameters
            
        Returns:
            Model response as string
        """
        client, model, config = cls.get_client(task_type)
        provider = config["provider"]
        
        # Merge config with kwargs
        params = {
            "max_tokens": config.get("max_tokens", 4096),
            "temperature": config.get("temperature", 0.7)
        }
        params.update(kwargs)
        
        try:
            if provider == "anthropic":
                messages = [{"role": "user", "content": prompt}]
                response = client.messages.create(
                    model=model,
                    messages=messages,
                    system=system_prompt or "",
                    max_tokens=params["max_tokens"],
                    temperature=params["temperature"]
                )
                return response.content[0].text
                
            elif provider == "openai":
                messages = []
                if system_prompt:
                    messages.append({"role": "system", "content": system_prompt})
                messages.append({"role": "user", "content": prompt})
                
                response = client.chat.completions.create(
                    model=model,
                    messages=messages,
                    max_tokens=params["max_tokens"],
                    temperature=params["temperature"]
                )
                return response.choices[0].message.content
                
            elif provider == "google":
                model_instance = client.GenerativeModel(model)
                response = model_instance.generate_content(
                    prompt,
                    generation_config={
                        "max_output_tokens": params["max_tokens"],
                        "temperature": params["temperature"]
                    }
                )
                return response.text
                
        except Exception as e:
            logger.error(f"Error calling {provider} {model}: {e}")
            raise
    
    @classmethod
    def get_cost_estimate(cls, task_type: TaskType, input_tokens: int, output_tokens: int) -> float:
        """
        Estimate cost for a task.
        
        Args:
            task_type: Type of task
            input_tokens: Number of input tokens
            output_tokens: Number of output tokens
            
        Returns:
            Estimated cost in USD
        """
        config = cls.get_model_config(task_type)
        model = config["model"]
        
        # Pricing per 1M tokens (approximate)
        pricing = {
            "claude-3-5-sonnet-20241022": {"input": 3.0, "output": 15.0},
            "gpt-4": {"input": 30.0, "output": 60.0},
            "gpt-5.1-codex": {"input": 10.0, "output": 30.0},  # Estimated
            "gpt-5.1-high-reasoning": {"input": 15.0, "output": 45.0},  # Estimated
            "gemini-2.0-flash-exp": {"input": 0.075, "output": 0.30},
            "gemini-2.5-pro": {"input": 1.25, "output": 5.0}  # Estimated
        }
        
        if model not in pricing:
            logger.warning(f"No pricing data for {model}, using default")
            return 0.0
        
        input_cost = (input_tokens / 1_000_000) * pricing[model]["input"]
        output_cost = (output_tokens / 1_000_000) * pricing[model]["output"]
        
        return input_cost + output_cost


class ModelPerformanceTracker:
    """Track performance metrics for each model."""
    
    def __init__(self):
        self.metrics = {}
    
    def log_call(
        self,
        task_type: TaskType,
        response_time: float,
        input_tokens: int,
        output_tokens: int,
        success: bool
    ):
        """Log a model call for analytics."""
        if task_type not in self.metrics:
            self.metrics[task_type] = {
                "total_calls": 0,
                "successful_calls": 0,
                "total_response_time": 0,
                "total_input_tokens": 0,
                "total_output_tokens": 0,
                "total_cost": 0
            }
        
        m = self.metrics[task_type]
        m["total_calls"] += 1
        if success:
            m["successful_calls"] += 1
        m["total_response_time"] += response_time
        m["total_input_tokens"] += input_tokens
        m["total_output_tokens"] += output_tokens
        
        cost = LLMRouter.get_cost_estimate(task_type, input_tokens, output_tokens)
        m["total_cost"] += cost
    
    def get_summary(self) -> Dict[str, Any]:
        """Get performance summary for all models."""
        summary = {}
        for task_type, m in self.metrics.items():
            if m["total_calls"] > 0:
                summary[task_type.value] = {
                    "total_calls": m["total_calls"],
                    "success_rate": m["successful_calls"] / m["total_calls"],
                    "avg_response_time": m["total_response_time"] / m["total_calls"],
                    "avg_input_tokens": m["total_input_tokens"] / m["total_calls"],
                    "avg_output_tokens": m["total_output_tokens"] / m["total_calls"],
                    "total_cost": m["total_cost"],
                    "avg_cost_per_call": m["total_cost"] / m["total_calls"]
                }
        return summary


# Global performance tracker
performance_tracker = ModelPerformanceTracker()

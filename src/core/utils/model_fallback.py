"""
Model Fallback & Self-Healing Strategy
Automatic model switching upon failure with exponential backoff
"""
import time
import logging
from typing import Callable, Any, List, Optional, Dict
from functools import wraps
from enum import Enum

logger = logging.getLogger(__name__)

class ModelProvider(str, Enum):
    OPENAI_GPT4 = "gpt-4-turbo"
    OPENAI_GPT35 = "gpt-3.5-turbo"
    ANTHROPIC_OPUS = "claude-3-opus"
    ANTHROPIC_SONNET = "claude-3-sonnet"
    LOCAL_MISTRAL = "mistral-7b"

class FallbackStrategy:
    """
    Defines the order of models to try in case of failure
    """
    
    PREMIUM = [
        ModelProvider.OPENAI_GPT4,
        ModelProvider.ANTHROPIC_OPUS,
        ModelProvider.OPENAI_GPT35
    ]
    
    FAST = [
        ModelProvider.OPENAI_GPT35,
        ModelProvider.ANTHROPIC_SONNET,
        ModelProvider.LOCAL_MISTRAL
    ]

class SelfHealingModel:
    """
    Wrapper for LLM calls with automatic fallback and self-healing
    
    Usage:
        @SelfHealingModel.with_fallback(strategy=FallbackStrategy.PREMIUM)
        def generate_response(prompt, model):
            # ... call LLM ...
    """
    
    @staticmethod
    def with_fallback(strategy: List[ModelProvider] = FallbackStrategy.PREMIUM, max_retries_per_model: int = 2):
        def decorator(func: Callable):
            @wraps(func)
            def wrapper(*args, **kwargs):
                original_model = kwargs.get('model')
                last_exception = None
                
                # If original model is not specified or not in strategy, start with first in strategy
                current_strategy = strategy
                
                # Try each model in the strategy
                for model in current_strategy:
                    # Retry logic for each model
                    for attempt in range(max_retries_per_model):
                        try:
                            # Override model in kwargs
                            kwargs['model'] = model.value
                            logger.info(f"Attempting with model: {model.value} (Attempt {attempt + 1})")
                            
                            return func(*args, **kwargs)
                            
                        except Exception as e:
                            last_exception = e
                            logger.warning(f"Model {model.value} failed: {str(e)}")
                            time.sleep(0.5 * (attempt + 1)) # Exponential backoff within model retries
                            
                    logger.warning(f"All retries failed for {model.value}, switching to next fallback...")
                
                # If we get here, all models failed
                logger.error("All fallback models failed.")
                raise last_exception
                
            return wrapper
        return decorator

    @staticmethod
    def mock_llm_call(prompt: str, model: str = "gpt-4-turbo"):
        """Mock LLM call for testing fallbacks"""
        if "fail" in prompt.lower() and model == "gpt-4-turbo":
            raise ValueError("GPT-4 Simulated Failure")
        return f"Response from {model}"

# Global fallback handler
_fallback_handler: Optional[SelfHealingModel] = None

"""
LLM Client Wrapper with Rate Limiting.

Wraps LLM API calls with automatic rate limiting and tracking.
"""

from typing import Optional, Dict, Any
from functools import wraps
import time

from openai import OpenAI
from anthropic import Anthropic
import google.generativeai as genai

from src.core.utils.redis_rate_limiter import get_llm_limiter, check_llm_rate_limit
from src.core.exceptions import RateLimitExceededError
from loguru import logger


def with_llm_rate_limit(provider: str):
    """
    Decorator to add rate limiting to LLM calls.
    
    Args:
        provider: LLM provider name (openai, anthropic, gemini)
        
    Usage:
        @with_llm_rate_limit("openai")
        def call_openai(prompt):
            return client.chat.completions.create(...)
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Get user_id and tier from kwargs or use default
            user_id = kwargs.pop("user_id", "default")
            tier = kwargs.pop("tier", "free")
            
            # Check rate limit
            check_llm_rate_limit(user_id, provider, tier)
            
            # Make LLM call
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                duration = time.time() - start_time
                
                # Track successful call
                _track_llm_call(user_id, provider, result, duration)
                
                return result
                
            except Exception as e:
                duration = time.time() - start_time
                logger.error(f"LLM call failed: {provider} - {e} (took {duration:.2f}s)")
                raise
        
        return wrapper
    return decorator


def _track_llm_call(
    user_id: str,
    provider: str,
    result: Any,
    duration: float
):
    """Track LLM call metrics."""
    try:
        llm_limiter = get_llm_limiter()
        
        # Extract tokens and cost from result
        tokens = 0
        cost = 0.0
        model = "unknown"
        
        if provider == "openai":
            if hasattr(result, "usage"):
                tokens = result.usage.total_tokens
                # Estimate cost (rough approximation)
                cost = tokens * 0.00002  # $0.02 per 1K tokens
            if hasattr(result, "model"):
                model = result.model
        
        elif provider == "anthropic":
            if hasattr(result, "usage"):
                tokens = result.usage.input_tokens + result.usage.output_tokens
                cost = tokens * 0.00003  # $0.03 per 1K tokens
            if hasattr(result, "model"):
                model = result.model
        
        # Track the call
        llm_limiter.track_llm_call(
            user_id=user_id,
            provider=provider,
            model=model,
            tokens=tokens,
            cost=cost
        )
        
        logger.info(
            f"LLM call tracked: {provider}/{model} - "
            f"{tokens} tokens, ${cost:.4f}, {duration:.2f}s"
        )
        
    except Exception as e:
        logger.error(f"Failed to track LLM call: {e}")


class RateLimitedOpenAI:
    """OpenAI client with automatic rate limiting."""
    
    def __init__(self, api_key: str = None, user_id: str = "default", tier: str = "free"):
        """
        Initialize rate-limited OpenAI client.
        
        Args:
            api_key: OpenAI API key
            user_id: User identifier for rate limiting
            tier: User tier (free, pro, enterprise)
        """
        self.client = OpenAI(api_key=api_key)
        self.user_id = user_id
        self.tier = tier
    
    def chat_completions_create(self, **kwargs):
        """
        Create chat completion with rate limiting.
        
        Wraps OpenAI's chat.completions.create with rate limiting.
        """
        @with_llm_rate_limit("openai")
        def _create(**params):
            return self.client.chat.completions.create(**params)
        
        return _create(user_id=self.user_id, tier=self.tier, **kwargs)


class RateLimitedAnthropic:
    """Anthropic client with automatic rate limiting."""
    
    def __init__(self, api_key: str = None, user_id: str = "default", tier: str = "free"):
        """
        Initialize rate-limited Anthropic client.
        
        Args:
            api_key: Anthropic API key
            user_id: User identifier for rate limiting
            tier: User tier (free, pro, enterprise)
        """
        self.client = Anthropic(api_key=api_key)
        self.user_id = user_id
        self.tier = tier
    
    def messages_create(self, **kwargs):
        """
        Create message with rate limiting.
        
        Wraps Anthropic's messages.create with rate limiting.
        """
        @with_llm_rate_limit("anthropic")
        def _create(**params):
            return self.client.messages.create(**params)
        
        return _create(user_id=self.user_id, tier=self.tier, **kwargs)


class RateLimitedGemini:
    """Google Gemini client with automatic rate limiting."""
    
    def __init__(self, api_key: str = None, user_id: str = "default", tier: str = "free"):
        """
        Initialize rate-limited Gemini client.
        
        Args:
            api_key: Google API key
            user_id: User identifier for rate limiting
            tier: User tier (free, pro, enterprise)
        """
        if api_key:
            genai.configure(api_key=api_key)
        self.user_id = user_id
        self.tier = tier
    
    def generate_content(self, model_name: str, prompt: str, **kwargs):
        """
        Generate content with rate limiting.
        
        Wraps Gemini's generate_content with rate limiting.
        """
        @with_llm_rate_limit("gemini")
        def _generate(model_name, prompt, **params):
            model = genai.GenerativeModel(model_name)
            return model.generate_content(prompt, **params)
        
        return _generate(
            model_name,
            prompt,
            user_id=self.user_id,
            tier=self.tier,
            **kwargs
        )


# Factory function
def get_llm_client(
    provider: str,
    api_key: str = None,
    user_id: str = "default",
    tier: str = "free"
):
    """
    Get rate-limited LLM client.
    
    Args:
        provider: LLM provider (openai, anthropic, gemini)
        api_key: API key
        user_id: User identifier
        tier: User tier
        
    Returns:
        Rate-limited LLM client
        
    Example:
        client = get_llm_client("openai", user_id="user123", tier="pro")
        response = client.chat_completions_create(
            model="gpt-4",
            messages=[{"role": "user", "content": "Hello"}]
        )
    """
    provider = provider.lower()
    
    if provider == "openai":
        return RateLimitedOpenAI(api_key, user_id, tier)
    elif provider == "anthropic":
        return RateLimitedAnthropic(api_key, user_id, tier)
    elif provider == "gemini":
        return RateLimitedGemini(api_key, user_id, tier)
    else:
        raise ValueError(f"Unsupported provider: {provider}")

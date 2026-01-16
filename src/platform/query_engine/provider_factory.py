import os
import time
from typing import List, Dict, Optional, Tuple, Any
from abc import ABC, abstractmethod
from loguru import logger
from openai import OpenAI
import google.generativeai as genai

try:
    from langsmith import traceable
    LANGSMITH_AVAILABLE = True
except ImportError:
    LANGSMITH_AVAILABLE = False

if not LANGSMITH_AVAILABLE:
    # Minimal fallback decorator if langsmith is missing
    def traceable(*args, **kwargs):
        def decorator(f):
            return f
        return decorator

class AbstractLLMProvider(ABC):
    """Base class for all LLM providers."""
    
    @abstractmethod
    @traceable(project_name="pca-sql-engine")
    def generate(self, prompt: str, system_prompt: Optional[str] = None, **kwargs) -> str:
        """Generate text from prompt."""
        pass

    @property
    @abstractmethod
    def name(self) -> str:
        """Provider name."""
        pass

class OpenAIProvider(AbstractLLMProvider):
    def __init__(self, api_key: str, model: str = "gpt-4o"):
        self.client = OpenAI(api_key=api_key)
        self._model = model

    def generate(self, prompt: str, system_prompt: Optional[str] = None, **kwargs) -> str:
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})
        
        response = self.client.chat.completions.create(
            model=self._model,
            messages=messages,
            temperature=kwargs.get('temperature', 0),
            max_tokens=kwargs.get('max_tokens', 2000)
        )
        return response.choices[0].message.content

    @property
    def name(self) -> str:
        return f"openai({self._model})"

class GeminiProvider(AbstractLLMProvider):
    def __init__(self, api_key: str, model: str = "gemini-1.5-flash"):
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel(model)
        self._model_name = model

    def generate(self, prompt: str, system_prompt: Optional[str] = None, **kwargs) -> str:
        full_prompt = prompt
        if system_prompt:
            full_prompt = f"{system_prompt}\n\n{prompt}"
            
        response = self.model.generate_content(
            full_prompt,
            generation_config=genai.types.GenerationConfig(
                temperature=kwargs.get('temperature', 0),
                max_output_tokens=kwargs.get('max_tokens', 2000)
            )
        )
        return response.text

    @property
    def name(self) -> str:
        return f"gemini({self._model_name})"

class DeepSeekProvider(AbstractLLMProvider):
    def __init__(self, api_key: str, model: str = "deepseek-chat"):
        self.client = OpenAI(
            api_key=api_key,
            base_url="https://api.deepseek.com"
        )
        self._model = model

    def generate(self, prompt: str, system_prompt: Optional[str] = None, **kwargs) -> str:
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})
        
        response = self.client.chat.completions.create(
            model=self._model,
            messages=messages,
            temperature=kwargs.get('temperature', 0)
        )
        return response.choices[0].message.content

    @property
    def name(self) -> str:
        return f"deepseek({self._model})"

class ProviderFactory:
    """Enterprise-grade provider management."""
    
    def __init__(self):
        self.providers: List[AbstractLLMProvider] = []
        self._initialize_providers()

    def _initialize_providers(self):
        # 1. OpenAI (Priority 1)
        openai_key = os.getenv('OPENAI_API_KEY')
        if openai_key:
            self.providers.append(OpenAIProvider(openai_key))
            logger.info("Registered Tier 1: OpenAI (gpt-4o)")

        # 2. Gemini (Priority 2)
        google_key = os.getenv('GOOGLE_API_KEY')
        if google_key:
            self.providers.append(GeminiProvider(google_key))
            logger.info("Registered Tier 2: Gemini 1.5 Flash")

        # 3. DeepSeek (Priority 3)
        deepseek_key = os.getenv('DEEPSEEK_API_KEY')
        if deepseek_key:
            self.providers.append(DeepSeekProvider(deepseek_key))
            logger.info("Registered Tier 3: DeepSeek Chat")

    def get_provider(self) -> Optional[AbstractLLMProvider]:
        """Get the highest priority available provider."""
        if not self.providers:
            return None
        return self.providers[0]

    @traceable(project_name="pca-sql-engine")
    def generate_with_fallback(self, prompt: str, system_prompt: Optional[str] = None, **kwargs) -> Tuple[Optional[str], Optional[str]]:
        """Attempt generation with automatic fallback logic."""
        for provider in self.providers:
            try:
                logger.info(f"Attempting generation with {provider.name}...")
                response = provider.generate(prompt, system_prompt=system_prompt, **kwargs)
                return response, provider.name
            except Exception as e:
                logger.warning(f"Provider {provider.name} failed: {e}")
                continue
        return None, None

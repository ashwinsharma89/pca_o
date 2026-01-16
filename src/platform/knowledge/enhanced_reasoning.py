"""
Enhanced Reasoning Layer with Knowledge Integration
Combines data analysis with external knowledge from URLs, YouTube, PDFs
"""
from typing import Dict, Any, Optional, List
from openai import OpenAI
import os
from loguru import logger
# from .knowledge_ingestion import KnowledgeIngestion
from .vector_store import HybridRetriever, VectorRetriever, VectorStoreConfig
from src.core.utils.anthropic_helpers import create_anthropic_client


class EnhancedReasoningEngine:
    """
    Enhanced reasoning engine that combines:
    1. Campaign data analysis
    2. External knowledge (URLs, YouTube, PDFs)
    3. LLM reasoning
    """
    
    def __init__(
        self,
        api_key: Optional[str] = None,
        use_anthropic: bool = False,
        vector_store_config: Optional[VectorStoreConfig] = None,
        enable_hybrid: bool = True,
    ):
        """
        Initialize enhanced reasoning engine.
        
        Args:
            api_key: API key for LLM (OpenAI or Anthropic)
            use_anthropic: Whether to use Anthropic Claude
        """
        self.use_anthropic = use_anthropic
        
        if use_anthropic:
            anthropic_key = api_key or os.getenv('ANTHROPIC_API_KEY')
            self.client = create_anthropic_client(anthropic_key)
            if not self.client:
                raise ValueError("Failed to initialize Anthropic client. Check ANTHROPIC_API_KEY or SDK support.")
            self.model = os.getenv('DEFAULT_LLM_MODEL', 'claude-3-5-sonnet-20241022')
        else:
            openai_key = api_key or os.getenv('OPENAI_API_KEY')
            if not openai_key:
                raise ValueError("OPENAI_API_KEY not found")
            self.client = OpenAI(api_key=openai_key)
            self.model = os.getenv('DEFAULT_LLM_MODEL', 'gpt-4')
        
        # Initialize knowledge ingestion
        # self.knowledge = KnowledgeIngestion()
        self.knowledge = None

        # Attempt to load semantic retriever
        self.vector_retriever: Optional[VectorRetriever] = None
        self.hybrid_retriever: Optional[HybridRetriever] = None
        try:
            self.vector_retriever = VectorRetriever(
                config=vector_store_config or VectorStoreConfig()
            )
            logger.info("Vector retriever initialized (FAISS + OpenAI embeddings)")
        except FileNotFoundError:
            logger.warning(
                "Vector store not found. Run auto_ingest_knowledge.py to build it before using semantic retrieval."
            )
        except Exception as exc:
            logger.error(f"Failed to initialize vector retriever: {exc}")
            self.vector_retriever = None

        if enable_hybrid:
            try:
                self.hybrid_retriever = HybridRetriever(
                    config=vector_store_config or VectorStoreConfig()
                )
                logger.info("Hybrid retriever initialized (vector + keyword + rerank)")
            except Exception as exc:
                logger.error(f"Failed to initialize hybrid retriever: {exc}")
                self.hybrid_retriever = None
        
        logger.info(f"Initialized EnhancedReasoningEngine with {self.model}")
    
    def learn_from_url(self, url: str) -> Dict[str, Any]:
        """
        Learn from a URL.
        
        Args:
            url: Web URL to learn from
            
        Returns:
            Ingestion result
        """
        logger.info(f"Learning from URL: {url}")
        return self.knowledge.ingest_from_url(url)
    
    def learn_from_youtube(self, video_url: str) -> Dict[str, Any]:
        """
        Learn from a YouTube video.
        
        Args:
            video_url: YouTube video URL
            
        Returns:
            Ingestion result
        """
        logger.info(f"Learning from YouTube: {video_url}")
        return self.knowledge.ingest_from_youtube(video_url)
    
    def learn_from_pdf(self, pdf_path: str) -> Dict[str, Any]:
        """
        Learn from a PDF file.
        
        Args:
            pdf_path: Path to PDF file
            
        Returns:
            Ingestion result
        """
        logger.info(f"Learning from PDF: {pdf_path}")
        return self.knowledge.ingest_from_pdf(pdf_path)
    
    def analyze_with_knowledge(
        self,
        query: str,
        data_context: Optional[str] = None,
        use_knowledge: bool = True
    ) -> Dict[str, Any]:
        """
        Analyze query using both data and external knowledge.
        
        Args:
            query: User query or analysis request
            data_context: Context from campaign data (optional)
            use_knowledge: Whether to include external knowledge
            
        Returns:
            Analysis result with reasoning
        """
        logger.info(f"Analyzing query with knowledge: {query[:100]}...")
        
        # Build context
        context_parts = []
        
        # Add data context if provided
        if data_context:
            context_parts.append(f"## Campaign Data Context:\n{data_context}")
        
        # Add external knowledge if requested
        external_context = ""
        if use_knowledge:
            external_context = self._get_external_context(query)
            if external_context:
                context_parts.append(f"## External Knowledge:\n{external_context}")
        
        combined_context = "\n\n".join(context_parts)
        
        # Build prompt
        system_prompt = """You are an expert digital marketing analyst with deep knowledge of:
- Campaign performance optimization
- Digital advertising platforms (Google Ads, Meta, LinkedIn, etc.)
- Marketing funnels and conversion optimization
- ROAS and ROI analysis
- Data-driven decision making

You have access to both campaign data and external knowledge sources.
Provide insights that combine data analysis with best practices from the knowledge base.

IMPORTANT RULES:
1. NEVER make up information or hallucinate facts
2. If you're uncertain about something, explicitly state "I'm not certain" or skip it
3. Only use information that can be validated from the provided sources
4. Cite sources when referencing external knowledge (e.g., "According to [source]...")
5. If information is unclear or contradictory, acknowledge the uncertainty
6. Prefer saying "I don't have enough information" over guessing
7. Distinguish between:
   - Facts from sources (cite them)
   - Data-driven insights (reference the data)
   - General best practices (label as such)
   - Your analysis (clearly mark as interpretation)"""

        user_prompt = f"""Query: {query}

{combined_context if combined_context else "No additional context available."}

Please provide a comprehensive analysis that:
1. Answers the query directly
2. References relevant data points with numbers
3. Incorporates ONLY validated information from external knowledge
4. Provides actionable recommendations

CRITICAL INSTRUCTIONS:
- Cite sources explicitly: "According to [source name], ..."
- If information is unclear or uncertain, say so: "The source doesn't provide clear information on..."
- Skip any information you cannot validate
- If sources contradict each other, mention both perspectives
- Never fill gaps with assumptions - acknowledge gaps instead
- Use phrases like:
  ✓ "Based on the data..."
  ✓ "According to [source]..."
  ✓ "The source suggests..."
  ✗ Avoid definitive claims without source attribution

Be specific, honest about limitations, and cite sources."""

        # Call LLM
        try:
            response_text = self._call_llm(system_prompt, user_prompt)
            
            return {
                'success': True,
                'query': query,
                'response': response_text,
                'used_knowledge': bool(use_knowledge and external_context),
                'knowledge_sources': self.knowledge.get_knowledge_summary()
            }
        
        except Exception as e:
            logger.error(f"Error in analysis: {e}")
            return {
                'success': False,
                'query': query,
                'error': str(e)
            }
    
    def get_knowledge_enhanced_insights(
        self,
        campaign_summary: str,
        topic: str = "campaign optimization"
    ) -> Dict[str, Any]:
        """
        Generate insights enhanced with external knowledge.
        
        Args:
            campaign_summary: Summary of campaign performance
            topic: Topic to focus on (e.g., "campaign optimization", "ROAS improvement")
            
        Returns:
            Enhanced insights
        """
        logger.info(f"Generating knowledge-enhanced insights on: {topic}")
        
        # Get relevant knowledge
        external_context = self._get_external_context(topic, max_chunks=5)
        
        system_prompt = """You are a senior digital marketing strategist combining:
1. Real campaign performance data
2. Industry best practices and research
3. Platform-specific optimization techniques

Generate actionable insights that blend data analysis with proven strategies."""

        user_prompt = f"""Campaign Performance Summary:
{campaign_summary}

Topic Focus: {topic}

External Knowledge & Best Practices:
{external_context if external_context else "No external knowledge available - use general best practices."}

Generate 5-7 actionable insights that:
1. Address the current campaign performance
2. Incorporate best practices from external sources
3. Provide specific, measurable recommendations
4. Reference sources when applicable

Format as JSON array with: category, insight, source (data/knowledge/both), recommendation"""

        try:
            response_text = self._call_llm(system_prompt, user_prompt, max_tokens=2000)
            
            return {
                'success': True,
                'topic': topic,
                'insights': response_text,
                'knowledge_used': bool(external_context),
                'sources': self.knowledge.get_knowledge_summary()
            }
        
        except Exception as e:
            logger.error(f"Error generating insights: {e}")
            return {
                'success': False,
                'topic': topic,
                'error': str(e)
            }
    
    def _call_llm(self, system_prompt: str, user_prompt: str, max_tokens: int = 1500) -> str:
        """
        Call LLM with unified interface.
        
        Args:
            system_prompt: System message
            user_prompt: User message
            max_tokens: Maximum tokens to generate
            
        Returns:
            LLM response text
        """
        if self.use_anthropic:
            # Anthropic Claude API
            response = self.client.messages.create(
                model=self.model,
                max_tokens=max_tokens,
                temperature=0.3,
                system=system_prompt,
                messages=[
                    {"role": "user", "content": user_prompt}
                ]
            )
            return response.content[0].text
        else:
            # OpenAI API
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.3,
                max_tokens=max_tokens
            )
            return response.choices[0].message.content
    
    def get_knowledge_status(self) -> Dict[str, Any]:
        """
        Get status of knowledge base.
        
        Returns:
            Knowledge base summary
        """
        return self.knowledge.get_knowledge_summary()
    
    def clear_knowledge(self):
        """Clear all learned knowledge."""
        self.knowledge.clear_knowledge_base()
        logger.info("Knowledge base cleared")

    def _get_external_context(self, query: str, max_chunks: int = 3) -> str:
        """Fetch supporting knowledge via vector retriever or keyword fallback."""

        snippets: List[str] = []

        retriever = self.hybrid_retriever or self.vector_retriever

        if retriever:
            try:
                for result in retriever.search(query, top_k=max_chunks):
                    meta = result.get('metadata', {})
                    title = meta.get('title') or meta.get('url') or 'Unknown Source'
                    url = meta.get('url', 'N/A')
                    snippet = result.get('text', '')
                    score = result.get('score', 0)
                    snippets.append(
                        f"[Source: {title} | {url}]\nScore: {score:.3f}\n{snippet}"
                    )
            except Exception as exc:
                logger.error(f"Hybrid/vector retrieval failed, falling back to keyword search: {exc}")

        if not snippets:
            fallback = self.knowledge.get_context_for_query(query, max_chunks=max_chunks)
            if fallback:
                snippets.append(fallback)

        return "\n\n---\n\n".join(snippets)


# Example usage
if __name__ == "__main__":
    # Initialize
    engine = EnhancedReasoningEngine()
    
    # Learn from various sources
    print("Learning from URL...")
    engine.learn_from_url("https://blog.google/products/ads/google-ads-performance-max/")
    
    print("\nLearning from YouTube...")
    engine.learn_from_youtube("https://www.youtube.com/watch?v=VIDEO_ID")
    
    print("\nLearning from PDF...")
    engine.learn_from_pdf("marketing_guide.pdf")
    
    # Analyze with knowledge
    print("\nAnalyzing query with knowledge...")
    result = engine.analyze_with_knowledge(
        query="How can I improve my ROAS for Performance Max campaigns?",
        data_context="Current ROAS: 3.2x, Spend: $50,000, Platform: Google Ads"
    )
    
    if result['success']:
        print(f"\nResponse:\n{result['response']}")
        print(f"\nUsed knowledge: {result['used_knowledge']}")
    
    # Get knowledge status
    status = engine.get_knowledge_status()
    print(f"\nKnowledge base: {status}")

"""
MCP-Enhanced RAG Integration
Combines static knowledge base with live data from MCP resources
"""

from typing import List, Dict, Any, Optional
import pandas as pd
from loguru import logger
import asyncio

from src.platform.knowledge.enhanced_reasoning import EnhancedReasoningEngine
from src.interface.mcp.client import PCAMCPClient


class MCPEnhancedRAG:
    """RAG system enhanced with live data from MCP resources."""
    
    def __init__(self, rag_engine: Optional[EnhancedReasoningEngine] = None):
        """Initialize MCP-enhanced RAG."""
        self.rag_engine = rag_engine or EnhancedReasoningEngine()
        self.mcp_client = PCAMCPClient()
        self.connected = False
        logger.info("MCP-Enhanced RAG initialized")
    
    async def connect(self):
        """Connect to MCP server."""
        await self.mcp_client.connect()
        self.connected = True
        logger.success("MCP-Enhanced RAG connected")
    
    async def retrieve_context(
        self,
        query: str,
        top_k: int = 5,
        include_live_data: bool = True,
        data_sources: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Retrieve context from multiple sources:
        1. Static knowledge base (vector search)
        2. Live campaign data (MCP resources)
        3. External APIs (via MCP)
        """
        contexts = []
        
        # 1. Static knowledge base (existing RAG)
        try:
            if self.rag_engine.hybrid_retriever:
                static_results = self.rag_engine.hybrid_retriever.search(query, top_k=top_k)
                for result in static_results:
                    contexts.append({
                        "source": "knowledge_base",
                        "type": "static",
                        "content": result.get("text", ""),
                        "metadata": result.get("metadata", {}),
                        "score": result.get("score", 0.0)
                    })
                logger.info(f"Retrieved {len(static_results)} results from knowledge base")
        except Exception as e:
            logger.warning(f"Failed to retrieve from knowledge base: {e}")
        
        # 2. Live data from MCP resources
        if include_live_data and self.connected:
            try:
                live_contexts = await self._retrieve_live_data(query, data_sources)
                contexts.extend(live_contexts)
                logger.info(f"Retrieved {len(live_contexts)} live data contexts")
            except Exception as e:
                logger.warning(f"Failed to retrieve live data: {e}")
        
        # Sort by relevance score
        contexts.sort(key=lambda x: x.get("score", 0.0), reverse=True)
        
        return contexts[:top_k * 2]  # Return more contexts since we have multiple sources
    
    async def _retrieve_live_data(
        self,
        query: str,
        data_sources: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """Retrieve relevant live data from MCP resources."""
        live_contexts = []
        
        # Analyze query to determine what data to fetch
        query_lower = query.lower()
        
        # Check if query is about recent performance
        if any(word in query_lower for word in ["recent", "latest", "current", "today", "this week", "this month"]):
            try:
                # Get recent metrics
                metrics = await self.mcp_client.get_analytics_metrics()
                live_contexts.append({
                    "source": "analytics",
                    "type": "live",
                    "content": f"Current campaign metrics: {metrics}",
                    "metadata": {"timestamp": "now", "type": "metrics"},
                    "score": 0.9  # High relevance for recent data queries
                })
            except Exception as e:
                logger.warning(f"Failed to get analytics metrics: {e}")
        
        # Check if query is about specific platforms
        platforms = ["meta", "facebook", "google", "tiktok", "linkedin", "twitter"]
        mentioned_platforms = [p for p in platforms if p in query_lower]
        
        if mentioned_platforms:
            try:
                # Get platform-specific data
                for platform in mentioned_platforms:
                    # This would query actual campaign data
                    live_contexts.append({
                        "source": f"platform_{platform}",
                        "type": "live",
                        "content": f"Live {platform} campaign data context",
                        "metadata": {"platform": platform},
                        "score": 0.85
                    })
            except Exception as e:
                logger.warning(f"Failed to get platform data: {e}")
        
        # Check if query is about specific metrics
        metrics_keywords = {
            "roas": "return on ad spend",
            "cpa": "cost per acquisition",
            "ctr": "click-through rate",
            "spend": "advertising spend",
            "conversions": "conversion data"
        }
        
        for metric, description in metrics_keywords.items():
            if metric in query_lower:
                try:
                    # Get metric-specific insights
                    live_contexts.append({
                        "source": f"metric_{metric}",
                        "type": "live",
                        "content": f"Current {description} analysis and benchmarks",
                        "metadata": {"metric": metric},
                        "score": 0.8
                    })
                except Exception as e:
                    logger.warning(f"Failed to get {metric} data: {e}")
        
        return live_contexts
    
    async def generate_enhanced_summary(
        self,
        query: str,
        campaign_data: Optional[pd.DataFrame] = None,
        top_k: int = 5
    ) -> Dict[str, Any]:
        """
        Generate summary using both static knowledge and live data.
        
        Args:
            query: User query or summary request
            campaign_data: Current campaign DataFrame
            top_k: Number of context chunks to retrieve
            
        Returns:
            Enhanced summary with sources
        """
        try:
            # Retrieve multi-source context
            contexts = await self.retrieve_context(
                query=query,
                top_k=top_k,
                include_live_data=True
            )
            
            # Separate static and live contexts
            static_contexts = [c for c in contexts if c["type"] == "static"]
            live_contexts = [c for c in contexts if c["type"] == "live"]
            
            # Build enhanced prompt
            prompt_parts = [f"Query: {query}\n"]
            
            if static_contexts:
                prompt_parts.append("\n📚 Knowledge Base Insights:")
                for ctx in static_contexts[:3]:
                    prompt_parts.append(f"- {ctx['content'][:200]}...")
            
            if live_contexts:
                prompt_parts.append("\n📊 Current Performance Data:")
                for ctx in live_contexts[:3]:
                    prompt_parts.append(f"- {ctx['content'][:200]}...")
            
            if campaign_data is not None:
                prompt_parts.append(f"\n📈 Campaign Data: {len(campaign_data)} rows")
                # Add key metrics summary
                if "Spend" in campaign_data.columns:
                    total_spend = campaign_data["Spend"].sum()
                    prompt_parts.append(f"Total Spend: ${total_spend:,.2f}")
                if "ROAS" in campaign_data.columns:
                    avg_roas = campaign_data["ROAS"].mean()
                    prompt_parts.append(f"Average ROAS: {avg_roas:.2f}")
            
            prompt = "\n".join(prompt_parts)
            
            # Generate summary using LLM
            # This would call your existing LLM methods
            summary = {
                "query": query,
                "summary": "Enhanced summary combining knowledge base and live data",
                "sources": {
                    "knowledge_base": len(static_contexts),
                    "live_data": len(live_contexts),
                    "total_contexts": len(contexts)
                },
                "contexts": contexts,
                "prompt": prompt
            }
            
            logger.success("Generated MCP-enhanced summary")
            return summary
            
        except Exception as e:
            logger.error(f"Failed to generate enhanced summary: {e}")
            raise
    
    async def get_contextual_recommendations(
        self,
        campaign_data: pd.DataFrame,
        focus_area: str = "all"
    ) -> List[Dict[str, Any]]:
        """
        Get recommendations using both historical knowledge and current data.
        
        Args:
            campaign_data: Current campaign DataFrame
            focus_area: Area to focus on (budget, targeting, creative, etc.)
            
        Returns:
            List of contextual recommendations
        """
        try:
            # Build query for context retrieval
            query = f"optimization recommendations for {focus_area}"
            
            # Get multi-source context
            contexts = await self.retrieve_context(
                query=query,
                top_k=5,
                include_live_data=True
            )
            
            # Analyze current performance
            recommendations = []
            
            # Example: Low ROAS campaigns
            if "ROAS" in campaign_data.columns and "Spend" in campaign_data.columns:
                low_roas = campaign_data[campaign_data["ROAS"] < 2.0]
                if len(low_roas) > 0:
                    total_low_spend = low_roas["Spend"].sum()
                    recommendations.append({
                        "type": "opportunity",
                        "priority": "high",
                        "title": "Low ROAS Campaigns Detected",
                        "description": f"{len(low_roas)} campaigns with ROAS < 2.0, spending ${total_low_spend:,.0f}",
                        "action": "Review and optimize or pause these campaigns",
                        "context_sources": [c["source"] for c in contexts[:2]]
                    })
            
            # Add more recommendation logic here
            
            logger.info(f"Generated {len(recommendations)} contextual recommendations")
            return recommendations
            
        except Exception as e:
            logger.error(f"Failed to generate recommendations: {e}")
            return []
    
    async def disconnect(self):
        """Disconnect from MCP server."""
        await self.mcp_client.disconnect()
        self.connected = False
        logger.info("MCP-Enhanced RAG disconnected")


# Convenience function
async def create_enhanced_rag() -> MCPEnhancedRAG:
    """Create and connect MCP-enhanced RAG."""
    rag = MCPEnhancedRAG()
    await rag.connect()
    return rag

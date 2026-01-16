"""
MCP (Model Context Protocol) Integration for PCA Agent

Provides:
- Phase 1: Unified data access layer for all sources
- Phase 2: MCP tools for LLM to call directly
- Phase 3: Enhanced RAG with live data integration
- Streamlit helpers: Synchronous wrappers for Streamlit compatibility
"""

from src.interface.mcp.resource_server import PCAResourceServer, get_mcp_server
from src.interface.mcp.client import PCAMCPClient, connect_mcp, read_campaign_data
from src.interface.mcp.tools import PCATools
from src.interface.mcp.rag_integration import MCPEnhancedRAG, create_enhanced_rag

__all__ = [
    "PCAResourceServer",
    "get_mcp_server",
    "PCAMCPClient",
    "connect_mcp",
    "read_campaign_data",
    "PCATools",
    "MCPEnhancedRAG",
    "create_enhanced_rag",
]

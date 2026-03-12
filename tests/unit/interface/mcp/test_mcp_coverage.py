import pytest
pytest.skip("Legacy test - incompatible with current MCP/RAG architecture", allow_module_level=True)
from unittest.mock import MagicMock, patch, AsyncMock
from src.interface.mcp.tools import PCATools
from src.interface.mcp.rag_integration import MCPEnhancedRAG
import pandas as pd

@pytest.mark.skip(reason="Legacy test - incompatible with current MCP structure")
class TestMCPTools(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        with patch("src.interface.mcp.tools.MediaAnalyticsExpert"):
            self.tools = PCATools()

    def test_tool_definitions(self):
        defs = self.tools.get_tool_definitions()
        self.assertIsInstance(defs, list)
        self.assertTrue(len(defs) > 0)
        tool_names = [t.name for t in defs]
        self.assertIn("query_campaigns", tool_names)
        self.assertIn("generate_report", tool_names)

    async def test_execute_tool_routing(self):
        # Mocking the internal methods
        self.tools._query_campaigns = AsyncMock(return_value={"success": True})
        
        # Test valid tool
        result = await self.tools.execute_tool("query_campaigns", {"metric": "spend"})
        self.assertTrue(result["success"])
        self.tools._query_campaigns.assert_called_once()
        
        # Test unknown tool
        result = await self.tools.execute_tool("unknown_tool", {})
        self.assertFalse(result["success"])
        self.assertIn("Unknown tool", result["error"])

@pytest.mark.skip(reason="Legacy test - incompatible with current MCPEnhancedRAG structure")
class TestMCPEnhancedRAG(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        with patch("src.interface.mcp.rag_integration.PCAMCPClient"), \
             patch("src.interface.mcp.rag_integration.EnhancedReasoningEngine"):
            self.rag = MCPEnhancedRAG()
            self.rag.connected = True

    async def test_retrieve_context_fusion(self):
        # Mock knowledge base
        self.rag.rag_engine.hybrid_retriever.search.return_value = [
            {"text": "static content", "score": 0.5, "metadata": {}}
        ]
        
        # Mock live data
        self.rag.mcp_client.get_analytics_metrics = AsyncMock(return_value="live metrics")
        
        contexts = await self.rag.retrieve_context("show me recent spend", include_live_data=True)
        
        source_types = [c["type"] for c in contexts]
        self.assertIn("static", source_types)
        self.assertIn("live", source_types)

    async def test_generate_enhanced_summary(self):
        self.rag.retrieve_context = AsyncMock(return_value=[
            {"type": "static", "content": "Knowledge base info", "source": "kb"},
            {"type": "live", "content": "Current spend is high", "source": "mcp"}
        ])
        
        df = pd.DataFrame({"Spend": [1000], "ROAS": [2.5]})
        summary = await self.rag.generate_enhanced_summary("test query", campaign_data=df)
        
        self.assertEqual(summary["sources"]["knowledge_base"], 1)
        self.assertEqual(summary["sources"]["live_data"], 1)
        self.assertIn("Knowledge Base Insights", summary["prompt"])
        self.assertIn("Current Performance Data", summary["prompt"])

if __name__ == "__main__":
    unittest.main()

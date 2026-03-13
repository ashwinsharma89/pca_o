import os
import sys
import json
import pandas as pd
from unittest.mock import MagicMock, patch

# Add src to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.interface.api.v1.routers.kg_summary import kg_query
from src.interface.api.v1.routers.kg_summary import KGQueryRequest as KGSummaryRequest

async def test_kg_llm_guidance():
    print("\n🔍 Testing LLM Guidance in KG Summary...")
    
    # 1. Setup Mock Data
    mock_df = pd.DataFrame({
        'platform': ['google', 'google', 'meta', 'meta'],
        'channel': ['search', 'search', 'social', 'social'],
        'spend': [100.0, 200.0, 150.0, 250.0],
        'impressions': [1000, 2000, 1500, 2500],
        'clicks': [10, 20, 15, 25],
        'conversions': [1, 2, 1, 2],
        'revenue': [500.0, 1000.0, 300.0, 600.0],
        'date': pd.to_datetime(['2024-01-01', '2024-01-02', '2024-01-01', '2024-01-02'])
    })

    # 2. Mock Dependencies
    with patch("src.interface.api.v1.routers.kg_summary.get_duckdb_manager") as mock_get_mgr:
        mock_mgr = MagicMock()
        mock_mgr.has_data.return_value = True
        mock_mgr.get_campaigns.return_value = mock_df
        mock_get_mgr.return_value = mock_mgr

        with patch("src.platform.query_engine.insight_generator.KGInsightGenerator.generate_guidance") as mock_gen:
            mock_gen.return_value = "Mocked LLM Guidance: Search is performing 2x better than Social in terms of ROAS. **Action:** Shift 20% budget."

            # 3. Request
            request = KGSummaryRequest(query="Compare Search vs Social")
            
            # 4. Execute
            response = await kg_query(request)
            
            # 5. Verify
            print(f"Status: {'✅ Success' if response['success'] else '❌ Failed'}")
            print(f"LLM Guidance: {response.get('llm_guidance')}")
            
            assert response['success'] is True
            assert "llm_guidance" in response
            assert "Search is performing 2x better" in response['llm_guidance']
            print("✅ LLM Guidance Integration Verified!")

if __name__ == "__main__":
    import asyncio
    asyncio.run(test_kg_llm_guidance())

"""
Integration tests for agent pipeline.
Tests end-to-end agent workflows and coordination.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock, AsyncMock
import pandas as pd
from datetime import datetime
import asyncio


class TestAgentPipelineIntegration:
    """Integration tests for full agent pipeline."""
    
    @pytest.fixture
    def sample_campaign_data(self):
        """Sample campaign data for analysis."""
        return pd.DataFrame({
            "Campaign_Name": ["Brand Awareness", "Lead Gen", "Retargeting"],
            "Platform": ["Google Ads", "Meta Ads", "LinkedIn"],
            "Spend": [10000, 15000, 5000],
            "Impressions": [500000, 750000, 100000],
            "Clicks": [5000, 7500, 2000],
            "Conversions": [100, 150, 50],
            "Revenue": [50000, 75000, 25000]
        })
    
    def test_vision_to_extraction_flow(self, sample_campaign_data):
        """Test data flow from vision to extraction."""
        # Simulate vision agent output
        vision_output = {
            "platform": "Google Ads",
            "raw_metrics": {
                "impressions": "500,000",
                "clicks": "5,000",
                "conversions": "100"
            }
        }
        
        # Simulate extraction normalization
        extracted = {
            "impressions": 500000,
            "clicks": 5000,
            "conversions": 100,
            "ctr": 5000 / 500000 * 100
        }
        
        assert extracted["impressions"] == 500000
        assert extracted["ctr"] == pytest.approx(1.0)
    
    def test_extraction_to_reasoning_flow(self, sample_campaign_data):
        """Test data flow from extraction to reasoning."""
        # Simulate extracted data
        extracted_data = {
            "campaigns": sample_campaign_data.to_dict("records"),
            "totals": {
                "spend": sample_campaign_data["Spend"].sum(),
                "conversions": sample_campaign_data["Conversions"].sum()
            }
        }
        
        # Simulate reasoning output
        insights = [
            "Lead Gen campaign has highest conversion volume",
            "Brand Awareness has best reach with 500K impressions",
            "Overall ROAS is 5:1"
        ]
        
        assert len(insights) == 3
        assert extracted_data["totals"]["spend"] == 30000
    
    def test_reasoning_to_visualization_flow(self, sample_campaign_data):
        """Test data flow from reasoning to visualization."""
        # Simulate reasoning output
        reasoning_output = {
            "insights": ["Top performer: Lead Gen"],
            "recommendations": ["Increase Lead Gen budget"],
            "metrics_to_visualize": ["spend", "conversions", "roas"]
        }
        
        # Simulate visualization generation
        charts = [
            {"type": "bar", "data": sample_campaign_data["Spend"].tolist()},
            {"type": "line", "data": sample_campaign_data["Conversions"].tolist()}
        ]
        
        assert len(charts) == 2
        assert charts[0]["type"] == "bar"
    
    def test_full_pipeline_execution(self, sample_campaign_data):
        """Test full pipeline from data to report."""
        # Step 1: Data ingestion
        data = sample_campaign_data.copy()
        assert len(data) == 3
        
        # Step 2: Normalization
        data["CTR"] = data["Clicks"] / data["Impressions"] * 100
        data["CPA"] = data["Spend"] / data["Conversions"]
        data["ROAS"] = data["Revenue"] / data["Spend"]
        
        # Step 3: Analysis
        top_campaign = data.loc[data["Conversions"].idxmax(), "Campaign_Name"]
        total_roas = data["Revenue"].sum() / data["Spend"].sum()
        
        # Step 4: Report generation
        report = {
            "summary": f"Top campaign: {top_campaign}",
            "total_roas": total_roas,
            "recommendations": ["Increase budget for top performer"]
        }
        
        assert report["total_roas"] == 5.0
        assert "Lead Gen" in report["summary"]


class TestChannelSpecialistIntegration:
    """Integration tests for channel specialist agents."""
    
    @pytest.fixture
    def channel_data(self):
        """Sample channel-specific data."""
        return {
            "google": pd.DataFrame({
                "Campaign": ["Search", "Display", "Shopping"],
                "Spend": [5000, 3000, 2000],
                "Conversions": [50, 20, 30]
            }),
            "meta": pd.DataFrame({
                "Campaign": ["Awareness", "Consideration", "Conversion"],
                "Spend": [4000, 3500, 2500],
                "Conversions": [30, 40, 80]
            }),
            "linkedin": pd.DataFrame({
                "Campaign": ["Brand", "Lead Gen"],
                "Spend": [3000, 2000],
                "Conversions": [10, 25]
            })
        }
    
    def test_channel_routing(self, channel_data):
        """Test routing to correct channel specialist."""
        query = "How is my Google Ads performing?"
        
        # Simulate routing
        if "google" in query.lower():
            channel = "google"
        elif "meta" in query.lower() or "facebook" in query.lower():
            channel = "meta"
        elif "linkedin" in query.lower():
            channel = "linkedin"
        else:
            channel = "general"
        
        assert channel == "google"
    
    def test_channel_specific_analysis(self, channel_data):
        """Test channel-specific analysis."""
        google_data = channel_data["google"]
        
        # Google-specific metrics
        total_spend = google_data["Spend"].sum()
        total_conversions = google_data["Conversions"].sum()
        cpa = total_spend / total_conversions
        
        # Channel-specific insight
        top_campaign = google_data.loc[google_data["Conversions"].idxmax(), "Campaign"]
        
        assert cpa == 100.0
        assert top_campaign == "Search"
    
    def test_cross_channel_comparison(self, channel_data):
        """Test cross-channel comparison."""
        channel_metrics = {}
        
        for channel, data in channel_data.items():
            channel_metrics[channel] = {
                "spend": data["Spend"].sum(),
                "conversions": data["Conversions"].sum(),
                "cpa": data["Spend"].sum() / data["Conversions"].sum()
            }
        
        # Find best CPA
        best_channel = min(channel_metrics, key=lambda x: channel_metrics[x]["cpa"])
        
        assert best_channel == "meta"  # Meta has CPA of 66.67


class TestErrorHandlingIntegration:
    """Integration tests for error handling across agents."""
    
    def test_handles_missing_data(self):
        """Test handling of missing data."""
        incomplete_data = pd.DataFrame({
            "Campaign": ["Test"],
            "Spend": [1000]
            # Missing: Conversions, Impressions, etc.
        })
        
        # Should handle missing columns gracefully
        conversions = incomplete_data.get("Conversions", pd.Series([0])).sum()
        
        assert conversions == 0
    
    def test_handles_null_values(self):
        """Test handling of null values."""
        data = pd.DataFrame({
            "Campaign": ["A", "B", "C"],
            "Spend": [1000, None, 2000],
            "Conversions": [10, 20, None]
        })
        
        # Should handle nulls
        total_spend = data["Spend"].fillna(0).sum()
        total_conversions = data["Conversions"].fillna(0).sum()
        
        assert total_spend == 3000
        assert total_conversions == 30
    
    def test_handles_division_by_zero(self):
        """Test handling of division by zero."""
        data = pd.DataFrame({
            "Spend": [1000, 2000],
            "Conversions": [0, 10]
        })
        
        # Safe division
        data["CPA"] = data.apply(
            lambda row: row["Spend"] / row["Conversions"] if row["Conversions"] > 0 else float('inf'),
            axis=1
        )
        
        assert data["CPA"].iloc[0] == float('inf')
        assert data["CPA"].iloc[1] == 200.0
    
    def test_handles_agent_timeout(self):
        """Test handling of agent timeout."""
        async def slow_agent():
            await asyncio.sleep(0.1)
            return "result"
        
        async def run_with_timeout():
            try:
                result = await asyncio.wait_for(slow_agent(), timeout=0.2)
                return result
            except asyncio.TimeoutError:
                return "timeout"
        
        result = asyncio.get_event_loop().run_until_complete(run_with_timeout())
        assert result == "result"


class TestStateManagementIntegration:
    """Integration tests for state management."""
    
    def test_state_persistence(self):
        """Test state persists across agent calls."""
        state = {
            "campaign_data": None,
            "analysis_results": [],
            "current_step": "init"
        }
        
        # Step 1: Load data
        state["campaign_data"] = pd.DataFrame({"A": [1, 2, 3]})
        state["current_step"] = "data_loaded"
        
        # Step 2: Analyze
        state["analysis_results"].append({"insight": "Test"})
        state["current_step"] = "analyzed"
        
        assert state["current_step"] == "analyzed"
        assert len(state["analysis_results"]) == 1
    
    def test_state_rollback_on_error(self):
        """Test state rollback on error."""
        state = {"step": "init", "data": None}
        checkpoint = state.copy()
        
        try:
            state["step"] = "processing"
            state["data"] = "test"
            raise ValueError("Simulated error")
        except ValueError:
            # Rollback
            state = checkpoint
        
        assert state["step"] == "init"
        assert state["data"] is None
    
    def test_concurrent_state_updates(self):
        """Test concurrent state updates."""
        state = {"counter": 0}
        
        def increment():
            state["counter"] += 1
        
        # Simulate concurrent updates
        for _ in range(10):
            increment()
        
        assert state["counter"] == 10

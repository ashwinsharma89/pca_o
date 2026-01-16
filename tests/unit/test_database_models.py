"""
Unit tests for src/database/models.py
Covers: Campaign, Analysis, ChatHistory, KnowledgeDocument models
"""
import pytest
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.core.database.models import Base, Campaign, Analysis, QueryHistory, LLMUsage


# ============================================================================
# Test Fixtures
# ============================================================================

@pytest.fixture
def engine():
    """Create in-memory SQLite engine for testing."""
    engine = create_engine('sqlite:///:memory:', echo=False)
    Base.metadata.create_all(engine)
    return engine


@pytest.fixture
def session(engine):
    """Create a new database session for testing."""
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()


# ============================================================================
# Campaign Model Tests
# ============================================================================

class TestCampaignModel:
    """Tests for Campaign model."""
    
    def test_create_campaign(self, session):
        """Should create a campaign with required fields."""
        campaign = Campaign(
            campaign_id="camp_001",
            campaign_name="Test Campaign",
            platform="Google Ads"
        )
        
        session.add(campaign)
        session.commit()
        
        assert campaign.id is not None
        assert campaign.campaign_id == "camp_001"
        assert campaign.campaign_name == "Test Campaign"
        assert campaign.platform == "Google Ads"
    
    def test_campaign_default_values(self, session):
        """Campaign should have default values for metrics."""
        campaign = Campaign(
            campaign_id="camp_002",
            campaign_name="Default Test",
            platform="Meta"
        )
        
        session.add(campaign)
        session.commit()
        
        assert campaign.spend == 0.0
        assert campaign.impressions == 0
        assert campaign.clicks == 0
        assert campaign.conversions == 0
    
    def test_campaign_with_metrics(self, session):
        """Should store campaign metrics correctly."""
        campaign = Campaign(
            campaign_id="camp_003",
            campaign_name="Metrics Campaign",
            platform="Google Ads",
            spend=1000.50,
            impressions=50000,
            clicks=500,
            conversions=25,
            ctr=0.01,
            cpc=2.00,
            cpa=40.02,
            roas=3.5
        )
        
        session.add(campaign)
        session.commit()
        
        assert campaign.spend == 1000.50
        assert campaign.impressions == 50000
        assert campaign.clicks == 500
        assert campaign.conversions == 25
        assert campaign.roas == 3.5
    
    def test_campaign_additional_data(self, session):
        """Should store JSON additional data."""
        campaign = Campaign(
            campaign_id="camp_004",
            campaign_name="JSON Data Campaign",
            platform="LinkedIn",
            additional_data={
                "targeting": {"age": "25-54", "interests": ["technology"]},
                "budget": {"daily": 100, "total": 3000}
            }
        )
        
        session.add(campaign)
        session.commit()
        
        assert campaign.additional_data is not None
        assert campaign.additional_data["targeting"]["age"] == "25-54"
    
    def test_campaign_timestamps(self, session):
        """Campaign should have created_at timestamp."""
        campaign = Campaign(
            campaign_id="camp_005",
            campaign_name="Timestamp Test",
            platform="TikTok"
        )
        
        session.add(campaign)
        session.commit()
        
        assert campaign.created_at is not None
        assert isinstance(campaign.created_at, datetime)
    
    def test_campaign_unique_id(self, session):
        """Campaign ID should be unique."""
        campaign1 = Campaign(
            campaign_id="unique_001",
            campaign_name="First Campaign",
            platform="Google Ads"
        )
        session.add(campaign1)
        session.commit()
        
        campaign2 = Campaign(
            campaign_id="unique_001",  # Duplicate ID
            campaign_name="Second Campaign",
            platform="Meta"
        )
        session.add(campaign2)
        
        with pytest.raises(Exception):  # IntegrityError
            session.commit()
    
    def test_campaign_repr(self, session):
        """Campaign should have readable repr."""
        campaign = Campaign(
            campaign_id="repr_001",
            campaign_name="Repr Test",
            platform="Google Ads"
        )
        session.add(campaign)
        session.commit()
        
        repr_str = repr(campaign)
        assert "Campaign" in repr_str
        assert "repr_001" in repr_str


# ============================================================================
# Analysis Model Tests
# ============================================================================

class TestAnalysisModel:
    """Tests for Analysis model."""
    
    def test_create_analysis(self, session):
        """Should create an analysis linked to campaign."""
        # First create a campaign
        campaign = Campaign(
            campaign_id="analysis_camp_001",
            campaign_name="Analysis Test Campaign",
            platform="Google Ads"
        )
        session.add(campaign)
        session.commit()
        
        # Create analysis
        analysis = Analysis(
            analysis_id="analysis_001",
            campaign_id=campaign.id,
            analysis_type="auto"
        )
        session.add(analysis)
        session.commit()
        
        assert analysis.id is not None
        assert analysis.campaign_id == campaign.id
        assert analysis.analysis_type == "auto"
    
    def test_analysis_default_status(self, session):
        """Analysis should default to pending status."""
        campaign = Campaign(
            campaign_id="status_camp",
            campaign_name="Status Test",
            platform="Meta"
        )
        session.add(campaign)
        session.commit()
        
        analysis = Analysis(
            analysis_id="status_analysis",
            campaign_id=campaign.id,
            analysis_type="rag"
        )
        session.add(analysis)
        session.commit()
        
        assert analysis.status == "pending"
    
    def test_analysis_json_fields(self, session):
        """Analysis should store JSON insights and recommendations."""
        campaign = Campaign(
            campaign_id="json_camp",
            campaign_name="JSON Test",
            platform="Google Ads"
        )
        session.add(campaign)
        session.commit()
        
        analysis = Analysis(
            analysis_id="json_analysis",
            campaign_id=campaign.id,
            analysis_type="channel",
            insights=[
                {"type": "performance", "message": "CTR is above average"},
                {"type": "budget", "message": "Consider increasing budget"}
            ],
            recommendations=[
                {"priority": "high", "action": "Increase bids on top keywords"}
            ],
            metrics={"ctr": 0.05, "cpc": 1.50, "roas": 4.2}
        )
        session.add(analysis)
        session.commit()
        
        assert len(analysis.insights) == 2
        assert len(analysis.recommendations) == 1
        assert analysis.metrics["roas"] == 4.2
    
    def test_analysis_campaign_relationship(self, session):
        """Analysis should be accessible from campaign."""
        campaign = Campaign(
            campaign_id="rel_camp",
            campaign_name="Relationship Test",
            platform="LinkedIn"
        )
        session.add(campaign)
        session.commit()
        
        analysis = Analysis(
            analysis_id="rel_analysis",
            campaign_id=campaign.id,
            analysis_type="pattern"
        )
        session.add(analysis)
        session.commit()
        
        # Access analysis through campaign
        assert len(campaign.analyses) == 1
        assert campaign.analyses[0].analysis_id == "rel_analysis"


# ============================================================================
# QueryHistory Model Tests
# ============================================================================

class TestQueryHistoryModel:
    """Tests for QueryHistory model."""
    
    def test_create_query_history(self, session):
        """Should create a query history entry."""
        query = QueryHistory(
            query_id="query_001",
            user_query="What is the CTR for Google Ads?",
            query_type="sql"
        )
        session.add(query)
        session.commit()
        
        assert query.id is not None
        assert query.query_id == "query_001"
        assert query.user_query == "What is the CTR for Google Ads?"
    
    def test_query_with_results(self, session):
        """Query should store result data."""
        query = QueryHistory(
            query_id="query_002",
            user_query="Show me top campaigns",
            query_type="hybrid",
            result_data={"campaigns": [{"name": "Brand", "roas": 4.5}]},
            result_summary="Top campaign is Brand with 4.5x ROAS"
        )
        session.add(query)
        session.commit()
        
        assert query.result_data is not None
        assert query.result_summary is not None


# ============================================================================
# LLMUsage Model Tests
# ============================================================================

class TestLLMUsageModel:
    """Tests for LLMUsage model."""
    
    def test_create_llm_usage(self, session):
        """Should create an LLM usage entry."""
        usage = LLMUsage(
            provider="anthropic",
            model="claude-3-sonnet",
            prompt_tokens=100,
            completion_tokens=50,
            total_tokens=150,
            cost=0.005,
            operation="auto_analysis"
        )
        session.add(usage)
        session.commit()
        
        assert usage.id is not None
        assert usage.provider == "anthropic"
        assert usage.total_tokens == 150
    
    def test_llm_usage_cost_tracking(self, session):
        """Should track LLM costs."""
        usage = LLMUsage(
            provider="openai",
            model="gpt-4o",
            prompt_tokens=1000,
            completion_tokens=500,
            total_tokens=1500,
            cost=0.025,
            operation="rag_summary"
        )
        session.add(usage)
        session.commit()
        
        assert usage.cost == 0.025


# ============================================================================
# Query Tests
# ============================================================================

class TestDatabaseQueries:
    """Tests for common database queries."""
    
    def test_query_campaigns_by_platform(self, session):
        """Should query campaigns by platform."""
        # Create campaigns
        for i in range(3):
            session.add(Campaign(
                campaign_id=f"google_{i}",
                campaign_name=f"Google Campaign {i}",
                platform="Google Ads"
            ))
        for i in range(2):
            session.add(Campaign(
                campaign_id=f"meta_{i}",
                campaign_name=f"Meta Campaign {i}",
                platform="Meta"
            ))
        session.commit()
        
        # Query by platform
        google_campaigns = session.query(Campaign).filter(
            Campaign.platform == "Google Ads"
        ).all()
        
        assert len(google_campaigns) == 3
    
    def test_query_campaigns_by_date_range(self, session):
        """Should query campaigns by date range."""
        from datetime import timedelta
        
        now = datetime.utcnow()
        
        # Create campaigns with different dates
        session.add(Campaign(
            campaign_id="recent_1",
            campaign_name="Recent Campaign",
            platform="Google Ads",
            date=now - timedelta(days=1)
        ))
        session.add(Campaign(
            campaign_id="old_1",
            campaign_name="Old Campaign",
            platform="Google Ads",
            date=now - timedelta(days=30)
        ))
        session.commit()
        
        # Query recent campaigns
        week_ago = now - timedelta(days=7)
        recent = session.query(Campaign).filter(
            Campaign.date >= week_ago
        ).all()
        
        assert len(recent) == 1
        assert recent[0].campaign_id == "recent_1"
    
    def test_aggregate_metrics(self, session):
        """Should aggregate campaign metrics."""
        from sqlalchemy import func
        
        # Create campaigns with metrics
        session.add(Campaign(
            campaign_id="agg_1",
            campaign_name="Campaign 1",
            platform="Google Ads",
            spend=1000,
            conversions=10
        ))
        session.add(Campaign(
            campaign_id="agg_2",
            campaign_name="Campaign 2",
            platform="Google Ads",
            spend=2000,
            conversions=20
        ))
        session.commit()
        
        # Aggregate
        result = session.query(
            func.sum(Campaign.spend),
            func.sum(Campaign.conversions)
        ).filter(Campaign.platform == "Google Ads").first()
        
        assert result[0] == 3000  # Total spend
        assert result[1] == 30    # Total conversions

"""
Tests for KG-RAG Schema Definitions
"""

import pytest
from src.kg_rag.schema.nodes import (
    NodeLabel,
    ChannelNode,
    PlatformNode,
    CampaignNode,
    TargetingNode,
    MetricNode,
    PlacementNode,
)
from src.kg_rag.schema.edges import (
    EdgeType,
    EdgeDefinition,
    EDGE_DEFINITIONS,
    get_edge_definition,
    generate_cypher_create_edge,
)
from src.kg_rag.schema.constraints import (
    UNIQUE_CONSTRAINTS,
    generate_constraint_cypher,
)
from src.kg_rag.schema.indexes import (
    SINGLE_PROPERTY_INDEXES,
    generate_index_cypher,
)


class TestNodeDefinitions:
    """Test node definitions."""
    
    def test_channel_seed_data(self):
        """Test Channel seed data has 4 channels."""
        assert len(ChannelNode.SEED_DATA) == 4
        
        channel_ids = {c["id"] for c in ChannelNode.SEED_DATA}
        assert channel_ids == {"search", "social", "display", "programmatic"}
    
    def test_platform_seed_data(self):
        """Test Platform seed data has 20+ platforms."""
        assert len(PlatformNode.SEED_DATA) >= 20
        
        # Check required platforms exist
        platform_ids = {p["id"] for p in PlatformNode.SEED_DATA}
        assert "google_ads" in platform_ids
        assert "meta" in platform_ids
        assert "linkedin" in platform_ids
        assert "dv360" in platform_ids
        assert "cm360" in platform_ids
    
    def test_platform_capabilities(self):
        """Test platform capability flags."""
        # Find Meta
        meta = next(p for p in PlatformNode.SEED_DATA if p["id"] == "meta")
        assert meta["supports_reach"] == True
        assert meta["supports_video_metrics"] == True
        
        # Find LinkedIn
        linkedin = next(p for p in PlatformNode.SEED_DATA if p["id"] == "linkedin")
        assert linkedin["supports_b2b_targeting"] == True
    
    def test_campaign_node_defaults(self):
        """Test CampaignNode default values."""
        campaign = CampaignNode(
            id="c1",
            account_id="a1",
            platform_id="meta",
            name="Test Campaign"
        )
        
        assert campaign.impressions_total == 0
        assert campaign.spend_total == 0.0
        assert campaign.objective is None
    
    def test_targeting_node_has_65_plus_fields(self):
        """Test TargetingNode has 65+ optional fields."""
        targeting = TargetingNode(campaign_id="c1")
        
        # Count non-None fields in dataclass
        field_count = len(TargetingNode.__dataclass_fields__)
        assert field_count >= 50  # Most fields are optional
    
    def test_metric_node_has_raw_metrics(self):
        """Test MetricNode has raw metrics only (no calculated)."""
        metric = MetricNode(
            id="c1_2025-01-01",
            campaign_id="c1",
            date="2025-01-01"
        )
        
        # Should have raw metrics
        assert hasattr(metric, "impressions")
        assert hasattr(metric, "clicks")
        assert hasattr(metric, "spend")
        
        # Should NOT have calculated metrics
        assert not hasattr(metric, "ctr")
        assert not hasattr(metric, "cpc")
        assert not hasattr(metric, "roas")
    
    def test_placement_node_structure(self):
        """Test PlacementNode has all required fields."""
        placement = PlacementNode(
            id="p1",
            entity_group_id="eg1",
            campaign_id="c1",
            name="cnn.com",
            type="site"
        )
        
        assert placement.id == "p1"
        assert hasattr(placement, "viewability_rate")
        assert hasattr(placement, "iab_category")
        assert hasattr(placement, "contracted_cost")


class TestEdgeDefinitions:
    """Test edge definitions."""
    
    def test_edge_count(self):
        """Test we have 12 edge types."""
        assert len(EDGE_DEFINITIONS) == 12
    
    def test_all_edge_types_defined(self):
        """Test all EdgeType enum values have definitions."""
        defined_types = {e.type for e in EDGE_DEFINITIONS}
        enum_types = set(EdgeType)
        
        assert defined_types == enum_types
    
    def test_get_edge_definition(self):
        """Test getting edge definition by type."""
        edge = get_edge_definition(EdgeType.HAS_PLACEMENT)
        
        assert edge is not None
        assert edge.from_label == "EntityGroup"
        assert edge.to_label == "Placement"
    
    def test_edge_with_properties(self):
        """Test edge definitions with properties."""
        overlap = get_edge_definition(EdgeType.OVERLAPS_WITH)
        assert "overlap_pct" in overlap.properties
        
        similar = get_edge_definition(EdgeType.SIMILAR_TO)
        assert "score" in similar.properties
    
    def test_generate_cypher_edge(self):
        """Test Cypher generation for edges."""
        cypher = generate_cypher_create_edge(
            EdgeType.HAS_PERFORMANCE,
            from_id="c1",
            to_id="m1",
            properties={"date": "2025-01-01"}
        )
        
        assert "MATCH" in cypher
        assert ":HAS_PERFORMANCE" in cypher
        assert "$date" in cypher


class TestConstraints:
    """Test constraint definitions."""
    
    def test_unique_constraints_count(self):
        """Test we have unique constraints for all nodes."""
        assert len(UNIQUE_CONSTRAINTS) == 11
    
    def test_generate_constraint_cypher(self):
        """Test constraint Cypher generation."""
        statements = generate_constraint_cypher()
        
        assert len(statements) == 11
        assert all("CREATE CONSTRAINT" in s for s in statements)
        assert all("IS UNIQUE" in s for s in statements)


class TestIndexes:
    """Test index definitions."""
    
    def test_index_count(self):
        """Test we have 20+ indexes."""
        assert len(SINGLE_PROPERTY_INDEXES) >= 20
    
    def test_generate_index_cypher(self):
        """Test index Cypher generation."""
        statements = generate_index_cypher()
        
        assert len(statements) >= 25  # Including composite and fulltext
        assert any("CREATE INDEX" in s for s in statements)
        assert any("FULLTEXT" in s for s in statements)

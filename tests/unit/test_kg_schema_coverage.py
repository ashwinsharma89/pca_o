
import pytest
from datetime import date
from src.kg_rag.schema.nodes import (
    NodeLabel, ChannelNode, PlatformNode, AccountNode, CampaignNode, TargetingNode,
    MetricNode, EntityGroupNode, CreativeNode, KeywordNode, PlacementNode, AudienceNode
)
from src.kg_rag.schema.edges import (
    EdgeType, EdgeDefinition, get_edge_definition, generate_cypher_create_edge
)
from src.kg_rag.schema.indexes import (
    IndexDefinition, generate_index_cypher, generate_drop_index_cypher,
    generate_relationship_index_cypher
)
from src.kg_rag.schema.constraints import (
    generate_constraint_cypher, generate_drop_constraint_cypher,
    generate_node_key_cypher, generate_existence_cypher
)

class TestKGSchemaNodes:
    """Tests for Node Definitions."""

    def test_node_labels_enum(self):
        """Verify NodeLabel enum values."""
        assert NodeLabel.CAMPAIGN == "Campaign"
        assert NodeLabel.PLATFORM == "Platform"

    def test_node_instantiation(self):
        """Instantiate all nodes to verify structure and defaults."""
        
        # Channel
        channel = ChannelNode(id="search", name="Search", description="Desc")
        assert channel.LABEL == NodeLabel.CHANNEL
        
        # Platform
        platform = PlatformNode(id="google", name="Google", channel_id="search", 
                              api_source="API", parent_company="Google")
        assert platform.LABEL == NodeLabel.PLATFORM
        assert platform.supports_keywords is False # Default
        
        # Account
        account = AccountNode(id="acc1", name="Acc 1", platform_id="google")
        assert account.LABEL == NodeLabel.ACCOUNT
        assert account.currency == "USD"
        
        # Campaign
        campaign = CampaignNode(id="camp1", account_id="acc1", platform_id="google", name="Camp 1")
        assert campaign.LABEL == NodeLabel.CAMPAIGN
        assert campaign.impressions_total == 0
        
        # Targeting
        targeting = TargetingNode(campaign_id="camp1")
        assert targeting.LABEL == NodeLabel.TARGETING
        assert targeting.age_range is None
        
        # Metric
        metric = MetricNode(id="m1", campaign_id="camp1", date=date(2023, 1, 1))
        assert metric.LABEL == NodeLabel.METRIC
        assert metric.clicks == 0
        
        # EntityGroup
        group = EntityGroupNode(id="g1", campaign_id="camp1", name="Group 1", entity_type="ad_group")
        assert group.LABEL == NodeLabel.ENTITY_GROUP
        
        # Creative
        creative = CreativeNode(id="c1", entity_group_id="g1", name="Ad 1", creative_type="image")
        assert creative.LABEL == NodeLabel.CREATIVE
        
        # Keyword
        kw = KeywordNode(id="k1", entity_group_id="g1", text="shoes", match_type="exact")
        assert kw.LABEL == NodeLabel.KEYWORD
        
        # Placement
        place = PlacementNode(id="p1", entity_group_id="g1", campaign_id="camp1", name="Site", type="site")
        assert place.LABEL == NodeLabel.PLACEMENT
        
        # Audience
        aud = AudienceNode(id="a1", name="Aud 1", type="lookalike")
        assert aud.LABEL == NodeLabel.AUDIENCE


class TestKGSchemaEdges:
    """Tests for Edge Definitions and Cypher Generation."""

    def test_edge_types(self):
        assert EdgeType.OWNS == "OWNS"

    def test_get_edge_definition(self):
        # Valid
        edge_def = get_edge_definition(EdgeType.OWNS)
        assert isinstance(edge_def, EdgeDefinition)
        assert edge_def.from_label == "Account"
        assert edge_def.to_label == "Campaign"
        
        # Invalid (mocking enum passed in, though logically hard with python enums, 
        # we check the function returns None if loop doesn't match)
        # Using a dummy object to simulate non-matching type
        class FakeEnum:
            type = "FAKE"
        assert get_edge_definition("FAKE") is None

    def test_generate_cypher_create_edge(self):
        # Without properties
        cypher = generate_cypher_create_edge(EdgeType.OWNS, "acc1", "camp1")
        assert "MATCH (a:Account {id: $from_id}), (b:Campaign {id: $to_id})" in cypher
        assert "CREATE (a)-[:OWNS]->(b)" in cypher
        
        # With properties
        cypher_props = generate_cypher_create_edge(
            EdgeType.HAS_PERFORMANCE, "camp1", "met1", {"date": "2023-01-01"}
        )
        assert "CREATE (a)-[:HAS_PERFORMANCE {date: $date}]->(b)" in cypher_props

    def test_generate_cypher_invalid_edge(self):
        with pytest.raises(ValueError):
            generate_cypher_create_edge("INVALID_TYPE", "a", "b")


class TestKGSchemaIndexes:
    """Tests for Index Generation."""
    
    def test_index_definition_name(self):
        idx = IndexDefinition("Label", ["prop1", "prop2"])
        assert idx.get_name() == "idx_label_prop1_prop2"
        
        idx_named = IndexDefinition("Label", ["p"], name="custom_idx")
        assert idx_named.get_name() == "custom_idx"

    def test_generate_index_cypher(self):
        cyphers = generate_index_cypher()
        assert len(cyphers) > 0
        
        # Standard index
        assert any("CREATE INDEX idx_campaign_platform_id IF NOT EXISTS" in c for c in cyphers)
        # Text index
        assert any("CREATE TEXT INDEX idx_campaign_name IF NOT EXISTS" in c for c in cyphers)
        # Fulltext
        assert any("CREATE FULLTEXT INDEX ft_campaign_search IF NOT EXISTS" in c for c in cyphers)

    def test_generate_drop_index_cypher(self):
        drops = generate_drop_index_cypher()
        assert len(drops) > 0
        assert any("DROP INDEX idx_campaign_platform_id IF EXISTS" in drops[0] for drops in [drops]) # Check Logic
        
    def test_generate_relationship_index_cypher(self):
        rels = generate_relationship_index_cypher()
        assert len(rels) > 0
        assert any("CREATE INDEX idx_rel_has_performance_date" in c for c in rels)


class TestKGSchemaConstraints:
    """Tests for Constraint Generation."""

    def test_generate_constraint_cypher(self):
        constraints = generate_constraint_cypher()
        assert len(constraints) > 0
        assert any("CREATE CONSTRAINT constraint_channel_id" in c for c in constraints)
        assert any("REQUIRE n.id IS UNIQUE" in c for c in constraints)

    def test_generate_drop_constraint_cypher(self):
        drops = generate_drop_constraint_cypher()
        assert len(drops) > 0
        assert any("DROP CONSTRAINT constraint_channel_id IF EXISTS" in c for c in drops)

    def test_generate_node_key_cypher(self):
        keys = generate_node_key_cypher()
        assert len(keys) > 0
        assert any("nodekey_metric_campaign_id_date" in c for c in keys)
        assert any("IS NODE KEY" in c for c in keys)

    def test_generate_existence_cypher(self):
        exists = generate_existence_cypher()
        assert len(exists) > 0
        assert any("exists_campaign_name" in c for c in exists)
        assert any("IS NOT NULL" in c for c in exists)

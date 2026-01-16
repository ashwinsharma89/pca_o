"""
Tests for KG-RAG Column Resolver
"""

import pytest
from src.kg_rag.etl.column_resolver import ColumnResolver, get_column_resolver


class TestColumnResolver:
    """Test column resolver functionality."""
    
    @pytest.fixture
    def resolver(self):
        """Get column resolver instance."""
        return get_column_resolver()
    
    def test_resolve_spend_aliases(self, resolver):
        """Test resolving spend column aliases."""
        assert resolver.resolve("Amount Spent") == "spend"
        assert resolver.resolve("amount_spent") == "spend"
        assert resolver.resolve("Cost") == "spend"
        assert resolver.resolve("Media Cost") == "spend"
        assert resolver.resolve("Total Cost") == "spend"
        assert resolver.resolve("Spend") == "spend"
    
    def test_resolve_impressions_aliases(self, resolver):
        """Test resolving impressions column aliases."""
        assert resolver.resolve("Impressions") == "impressions"
        assert resolver.resolve("Impr.") == "impressions"
        assert resolver.resolve("impr") == "impressions"
        assert resolver.resolve("Total Impressions") == "impressions"
    
    def test_resolve_clicks_aliases(self, resolver):
        """Test resolving clicks column aliases."""
        assert resolver.resolve("Clicks") == "clicks"
        assert resolver.resolve("Link Clicks") == "clicks"
        assert resolver.resolve("link_clicks") == "clicks"
        assert resolver.resolve("Swipe Ups") == "clicks"
    
    def test_resolve_conversions_aliases(self, resolver):
        """Test resolving conversions column aliases."""
        assert resolver.resolve("Conversions") == "conversions"
        assert resolver.resolve("Results") == "conversions"
        assert resolver.resolve("Leads") == "conversions"
        assert resolver.resolve("Orders") == "conversions"
    
    def test_resolve_revenue_aliases(self, resolver):
        """Test resolving revenue column aliases."""
        assert resolver.resolve("Revenue") == "revenue"
        assert resolver.resolve("Conv. Value") == "revenue"
        assert resolver.resolve("Sales") == "revenue"
        assert resolver.resolve("Total Sales") == "revenue"
    
    def test_resolve_case_insensitive(self, resolver):
        """Test that resolution is case-insensitive."""
        assert resolver.resolve("SPEND") == "spend"
        assert resolver.resolve("sPeNd") == "spend"
        assert resolver.resolve("IMPRESSIONS") == "impressions"
    
    def test_resolve_unknown_column(self, resolver):
        """Test resolving unknown column returns None."""
        assert resolver.resolve("unknown_column_xyz") is None
        assert resolver.resolve("random_metric") is None
    
    def test_resolve_with_type(self, resolver):
        """Test resolving with type info."""
        result = resolver.resolve_with_type("Amount Spent")
        assert result is not None
        assert result["canonical"] == "spend"
        assert result["type"] == "float"
        assert result["aggregation"] == "sum"
        
        result = resolver.resolve_with_type("Impressions")
        assert result["type"] == "integer"
        assert result["aggregation"] == "sum"
    
    def test_get_all_aliases(self, resolver):
        """Test getting all aliases for a canonical name."""
        aliases = resolver.get_all_aliases("spend")
        assert "cost" in aliases
        assert "Amount Spent" in aliases
        assert len(aliases) > 5
    
    def test_get_canonical_names(self, resolver):
        """Test getting all canonical names."""
        names = resolver.get_canonical_names()
        assert "spend" in names
        assert "impressions" in names
        assert "clicks" in names
        assert "conversions" in names
        assert "revenue" in names
        assert len(names) > 30
    
    def test_resolve_columns_list(self, resolver):
        """Test resolving a list of columns."""
        columns = ["Amount Spent", "Impressions", "unknown", "Link Clicks"]
        resolved = resolver.resolve_columns(columns)
        
        assert resolved["Amount Spent"] == "spend"
        assert resolved["Impressions"] == "impressions"
        assert resolved["Link Clicks"] == "clicks"
        assert "unknown" not in resolved
    
    def test_resolve_dataframe_columns(self, resolver):
        """Test creating rename mapping for DataFrame."""
        columns = ["Amount Spent", "Impr.", "custom_col"]
        rename_map = resolver.resolve_dataframe_columns(columns)
        
        assert rename_map["Amount Spent"] == "spend"
        assert rename_map["Impr."] == "impressions"
        assert rename_map["custom_col"] == "custom_col"  # kept as-is
    
    def test_find_column_in_list(self, resolver):
        """Test finding a column matching a canonical name."""
        columns = ["Amount Spent", "Impressions", "CTR"]
        
        assert resolver.find_column(columns, "spend") == "Amount Spent"
        assert resolver.find_column(columns, "impressions") == "Impressions"
        assert resolver.find_column(columns, "unknown") is None
    
    def test_detect_meta_platform(self, resolver):
        """Test detecting Meta platform from columns."""
        meta_columns = ["Amount Spent", "Link Clicks", "ThruPlay", "Reach"]
        platform = resolver.detect_platform(meta_columns)
        assert platform == "meta"
    
    def test_detect_google_ads_platform(self, resolver):
        """Test detecting Google Ads platform from columns."""
        google_columns = ["Cost", "Impr.", "Avg. CPC", "Quality Score"]
        platform = resolver.detect_platform(google_columns)
        assert platform == "google_ads"
    
    def test_detect_unknown_platform(self, resolver):
        """Test detecting unknown platform returns None."""
        generic_columns = ["id", "name", "value"]
        platform = resolver.detect_platform(generic_columns)
        assert platform is None


class TestColumnResolverSingleton:
    """Test singleton pattern."""
    
    def test_singleton_returns_same_instance(self):
        """Test that get_column_resolver returns the same instance."""
        resolver1 = get_column_resolver()
        resolver2 = get_column_resolver()
        assert resolver1 is resolver2

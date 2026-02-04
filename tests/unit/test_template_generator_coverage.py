
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from src.platform.query_engine.template_generator import (
    TemplateGenerator,
    generate_templates_for_schema,
    load_schema_from_parquet
)

@pytest.fixture
def schema_columns():
    return ["Date", "Campaign Name", "Platform", "Channel", "Spend", "Impressions", "Clicks", "Conversions", "Revenue", "Funnel Stage", "Device Type"]

@pytest.fixture
def generator(schema_columns):
    return TemplateGenerator(schema_columns)

class TestTemplateGenerator:
    def test_init(self, generator, schema_columns):
        assert generator.schema_columns == schema_columns
        assert generator._template_cache == {}

    def test_resolve(self, generator):
        # Basic resolve
        assert generator._resolve("spend") == "Spend"
        # Resolve with spaces/hyphens
        assert generator._resolve("campaign") == '"Campaign Name"'
        assert generator._resolve("funnel") == '"Funnel Stage"'
        # Non-existent
        assert generator._resolve("non_existent") is None

    def test_safe_resolve(self, generator):
        # Successful resolve
        assert generator._safe_resolve("spend") == "Spend"
        # Fallback
        assert generator._safe_resolve("non_existent", "Fallback") == "Fallback"
        # NULL as term
        assert generator._safe_resolve("non_existent") == "NULL AS non_existent"

    def test_generate_funnel_analysis(self, generator):
        template = generator.generate_funnel_analysis()
        assert template is not None
        assert "Marketing Funnel Analysis" in template.name
        assert "funnel_stage" in template.sql

        # Failure case
        bad_generator = TemplateGenerator(["Date"])
        assert bad_generator.generate_funnel_analysis() is None

    def test_generate_roas_analysis(self, generator):
        template = generator.generate_roas_analysis()
        assert template is not None
        assert "ROAS Performance Analysis" in template.name
        assert "roas" in template.sql

        # Failure case
        bad_generator = TemplateGenerator(["Date"])
        assert bad_generator.generate_roas_analysis() is None

    def test_generate_growth_analysis(self, generator):
        template = generator.generate_growth_analysis()
        assert template is not None
        assert "Weekly Growth Analysis" in template.name
        assert "growth_percentage" in template.sql

        # Failure case
        bad_generator = TemplateGenerator(["Date"])
        assert bad_generator.generate_growth_analysis() is None

    def test_generate_monthly_performance(self, generator):
        template = generator.generate_monthly_performance()
        assert template is not None
        assert "Monthly Performance Analysis" in template.name
        assert "month_label" in template.sql

        # Failure case
        bad_generator = TemplateGenerator(["Spend"]) # Missing date
        assert bad_generator.generate_monthly_performance() is None

    def test_generate_top_campaigns(self, generator):
        template = generator.generate_top_campaigns()
        assert template is not None
        assert "Top Performing Campaigns" in template.name
        assert "ORDER BY total_conversions DESC" in template.sql
        assert "platform" in template.sql
        assert "channel" in template.sql

        # Partial columns (Missing platform and channel)
        partial_generator = TemplateGenerator(["Campaign Name", "Spend", "Conversions"])
        template_partial = partial_generator.generate_top_campaigns()
        assert template_partial is not None
        assert "platform" not in template_partial.sql
        assert "channel" not in template_partial.sql

        # Failure case
        bad_generator = TemplateGenerator(["Spend"]) # Missing campaign/conversions
        assert bad_generator.generate_top_campaigns() is None

    def test_generate_channel_comparison(self, generator):
        template = generator.generate_channel_comparison()
        assert template is not None
        assert "Channel Performance Comparison" in template.name

        # Failure case
        bad_generator = TemplateGenerator(["Date"])
        assert bad_generator.generate_channel_comparison() is None

    def test_generate_platform_comparison(self, generator):
        template = generator.generate_platform_comparison()
        assert template is not None
        assert "Platform Performance Comparison" in template.name

        # Failure case
        bad_generator = TemplateGenerator(["Date"])
        assert bad_generator.generate_platform_comparison() is None

    def test_generate_summary(self, generator):
        template = generator.generate_summary()
        assert template is not None
        assert "Overall Summary" in template.name

        # Failure case
        bad_generator = TemplateGenerator(["Date"])
        assert bad_generator.generate_summary() is None

    def test_generate_device_performance(self, generator):
        template = generator.generate_device_performance()
        assert template is not None
        assert "Device Performance Analysis" in template.name

        # Failure case
        bad_generator = TemplateGenerator(["Date"])
        assert bad_generator.generate_device_performance() is None

    def test_generate_all_templates(self, generator):
        templates = generator.generate_all_templates()
        assert len(templates) > 0
        assert "funnel_analysis" in templates
        assert "roas_analysis" in templates

    def test_generate_all_templates_exception(self):
        generator = TemplateGenerator(["Date"])
        with patch.object(generator, 'generate_funnel_analysis', side_effect=Exception("Test error")):
            templates = generator.generate_all_templates()
            assert "funnel_analysis" not in templates

def test_load_schema_from_parquet():
    with patch('pandas.read_parquet') as mock_read:
        mock_read.return_value = pd.DataFrame(columns=["A", "B"])
        assert load_schema_from_parquet("dummy.parquet") == ["A", "B"]

        mock_read.side_effect = Exception("File not found")
        assert load_schema_from_parquet("missing.parquet") == []

def test_generate_templates_for_schema():
    templates = generate_templates_for_schema(["Date", "Spend"])
    assert "summary" in templates

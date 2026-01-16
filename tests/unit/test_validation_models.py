"""
Unit Tests for Request Validation Models

Tests all Pydantic validation models to ensure they correctly
validate and reject invalid inputs.
"""

import pytest
from pydantic import ValidationError
from datetime import date, datetime
from src.interface.api.v1.models import (
    ChatRequest,
    GlobalAnalysisRequest,
    KPIComparisonRequest,
    FileUploadRequest,
    CampaignFilterRequest,
    WebhookPayloadRequest,
    PaginationRequest,
    DateRangeRequest,
)


class TestChatRequest:
    """Test ChatRequest validation"""
    
    def test_valid_request(self):
        """Test valid chat request"""
        req = ChatRequest(
            question="What are the top campaigns?",
            knowledge_mode=False,
            use_rag_context=True
        )
        assert req.question == "What are the top campaigns?"
        assert req.knowledge_mode is False
    
    def test_empty_question_fails(self):
        """Test that empty question is rejected"""
        with pytest.raises(ValidationError) as exc_info:
            ChatRequest(question="")
        
        errors = exc_info.value.errors()
        assert any(e['loc'] == ('question',) for e in errors)
    
    def test_question_too_long_fails(self):
        """Test that overly long questions are rejected"""
        with pytest.raises(ValidationError):
            ChatRequest(question="x" * 1001)
    
    def test_xss_sanitization(self):
        """Test that XSS attempts are sanitized"""
        req = ChatRequest(question="<script>alert('xss')</script>What are sales?")
        # Should be sanitized by bleach
        assert "<script>" not in req.question


class TestFileUploadRequest:
    """Test FileUploadRequest validation"""
    
    def test_valid_csv(self):
        """Test valid CSV file"""
        req = FileUploadRequest(file_extension="csv")
        assert req.file_extension == "csv"
    
    def test_valid_excel(self):
        """Test valid Excel files"""
        req1 = FileUploadRequest(file_extension="xlsx")
        req2 = FileUploadRequest(file_extension="xls")
        assert req1.file_extension == "xlsx"
        assert req2.file_extension == "xls"
    
    def test_invalid_extension_fails(self):
        """Test that invalid extensions are rejected"""
        with pytest.raises(ValidationError) as exc_info:
            FileUploadRequest(file_extension="exe")
        
        errors = exc_info.value.errors()
        assert any(e['type'] == 'string_pattern_mismatch' for e in errors)
    
    def test_file_size_limit(self):
        """Test file size validation"""
        # 100MB = 104857600 bytes
        req = FileUploadRequest(
            file_extension="csv",
            file_size_bytes=104857600
        )
        assert req.file_size_bytes == 104857600
        
        # Over 100MB should fail
        with pytest.raises(ValidationError):
            FileUploadRequest(
                file_extension="csv",
                file_size_bytes=104857601
            )
    
    def test_sheet_name_sanitization(self):
        """Test sheet name is sanitized"""
        req = FileUploadRequest(
            file_extension="xlsx",
            sheet_name="<b>Sheet1</b>"
        )
        assert "<b>" not in req.sheet_name


class TestCampaignFilterRequest:
    """Test CampaignFilterRequest validation"""
    
    def test_valid_filters(self):
        """Test valid filter request"""
        req = CampaignFilterRequest(
            platforms=["Facebook", "Google"],
            channels=["Social", "Search"],
            start_date=date(2024, 1, 1),
            end_date=date(2024, 12, 31)
        )
        assert len(req.platforms) == 2
        assert len(req.channels) == 2
    
    def test_platform_limit(self):
        """Test that platform list is limited to 50"""
        with pytest.raises(ValidationError):
            CampaignFilterRequest(platforms=["Platform"] * 51)
    
    def test_channel_limit(self):
        """Test that channel list is limited to 50"""
        with pytest.raises(ValidationError):
            CampaignFilterRequest(channels=["Channel"] * 51)
    
    def test_date_range_validation(self):
        """Test that end_date must be after start_date"""
        with pytest.raises(ValidationError) as exc_info:
            CampaignFilterRequest(
                start_date=date(2024, 12, 31),
                end_date=date(2024, 1, 1)
            )
        
        errors = exc_info.value.errors()
        assert any('end_date' in str(e) for e in errors)
    
    def test_spend_range_validation(self):
        """Test that max_spend must be greater than min_spend"""
        with pytest.raises(ValidationError):
            CampaignFilterRequest(
                min_spend=1000.0,
                max_spend=500.0
            )
    
    def test_negative_spend_fails(self):
        """Test that negative spend values are rejected"""
        with pytest.raises(ValidationError):
            CampaignFilterRequest(min_spend=-100.0)


class TestWebhookPayloadRequest:
    """Test WebhookPayloadRequest validation"""
    
    def test_valid_payload(self):
        """Test valid webhook payload"""
        req = WebhookPayloadRequest(
            event_type="campaign_created",
            data={"campaign_id": "123", "name": "Test Campaign"}
        )
        assert req.event_type == "campaign_created"
        assert req.data["campaign_id"] == "123"
    
    def test_invalid_event_type_fails(self):
        """Test that invalid event types are rejected"""
        # Uppercase not allowed
        with pytest.raises(ValidationError):
            WebhookPayloadRequest(
                event_type="CAMPAIGN_CREATED",
                data={}
            )
        
        # Spaces not allowed
        with pytest.raises(ValidationError):
            WebhookPayloadRequest(
                event_type="campaign created",
                data={}
            )
    
    def test_event_type_too_long_fails(self):
        """Test that overly long event types are rejected"""
        with pytest.raises(ValidationError):
            WebhookPayloadRequest(
                event_type="x" * 101,
                data={}
            )


class TestPaginationRequest:
    """Test PaginationRequest validation"""
    
    def test_valid_pagination(self):
        """Test valid pagination parameters"""
        req = PaginationRequest(limit=50, offset=100)
        assert req.limit == 50
        assert req.offset == 100
    
    def test_default_values(self):
        """Test default pagination values"""
        req = PaginationRequest()
        assert req.limit == 100
        assert req.offset == 0
    
    def test_limit_too_high_fails(self):
        """Test that limit over 1000 is rejected"""
        with pytest.raises(ValidationError):
            PaginationRequest(limit=1001)
    
    def test_limit_zero_fails(self):
        """Test that limit of 0 is rejected"""
        with pytest.raises(ValidationError):
            PaginationRequest(limit=0)
    
    def test_negative_offset_fails(self):
        """Test that negative offset is rejected"""
        with pytest.raises(ValidationError):
            PaginationRequest(offset=-1)


class TestDateRangeRequest:
    """Test DateRangeRequest validation"""
    
    def test_valid_date_range(self):
        """Test valid date range"""
        req = DateRangeRequest(
            start_date=date(2024, 1, 1),
            end_date=date(2024, 12, 31)
        )
        assert req.start_date == date(2024, 1, 1)
        assert req.end_date == date(2024, 12, 31)
    
    def test_same_dates_allowed(self):
        """Test that same start and end date is allowed"""
        req = DateRangeRequest(
            start_date=date(2024, 6, 15),
            end_date=date(2024, 6, 15)
        )
        assert req.start_date == req.end_date
    
    def test_end_before_start_fails(self):
        """Test that end_date before start_date is rejected"""
        with pytest.raises(ValidationError):
            DateRangeRequest(
                start_date=date(2024, 12, 31),
                end_date=date(2024, 1, 1)
            )


class TestKPIComparisonRequest:
    """Test KPIComparisonRequest validation"""
    
    def test_valid_request(self):
        """Test valid KPI comparison request"""
        req = KPIComparisonRequest(
            kpis=["spend", "conversions", "roas"],
            dimension="platform"
        )
        assert len(req.kpis) == 3
        assert req.dimension == "platform"
    
    def test_kpi_limit(self):
        """Test that KPI list is limited to 20"""
        with pytest.raises(ValidationError):
            KPIComparisonRequest(kpis=["kpi"] * 21)
    
    def test_empty_kpi_list_fails(self):
        """Test that empty KPI list is rejected"""
        with pytest.raises(ValidationError):
            KPIComparisonRequest(kpis=[])
    
    def test_date_format_validation(self):
        """Test that dates must match YYYY-MM-DD format"""
        # Valid format
        req = KPIComparisonRequest(
            kpis=["spend"],
            start_date="2024-01-01",
            end_date="2024-12-31"
        )
        assert req.start_date == "2024-01-01"
        
        # Invalid format should fail
        with pytest.raises(ValidationError):
            KPIComparisonRequest(
                kpis=["spend"],
                start_date="01/01/2024"
            )


class TestGlobalAnalysisRequest:
    """Test GlobalAnalysisRequest validation"""
    
    def test_valid_request(self):
        """Test valid analysis request"""
        req = GlobalAnalysisRequest(
            use_rag_summary=True,
            include_recommendations=True,
            analysis_depth="deep"
        )
        assert req.analysis_depth == "deep"
    
    def test_analysis_depth_validation(self):
        """Test that analysis_depth must be quick/standard/deep"""
        # Valid values
        for depth in ["quick", "standard", "deep"]:
            req = GlobalAnalysisRequest(analysis_depth=depth)
            assert req.analysis_depth == depth
        
        # Invalid value should fail
        with pytest.raises(ValidationError):
            GlobalAnalysisRequest(analysis_depth="invalid")

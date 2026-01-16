"""
Mock response data for ad platform connectors.

Provides realistic mock data based on actual API response schemas
for testing without real advertising accounts.
"""

from datetime import datetime, timedelta
from typing import Dict, List, Any
import random


def _random_date_range(days_back: int = 90) -> tuple:
    """Generate random date range within the past N days."""
    end = datetime.utcnow() - timedelta(days=random.randint(0, 30))
    start = end - timedelta(days=random.randint(30, days_back))
    return start, end


# =============================================================================
# Google Ads Mock Data
# =============================================================================

GOOGLE_ADS_CAMPAIGNS = [
    {
        "id": "ga_camp_001",
        "name": "Brand Awareness - Q4 2024",
        "status": "ENABLED",
        "budget": 15000.00,
        "spend": 12450.75,
        "impressions": 2450000,
        "clicks": 45200,
        "conversions": 1250.5,
        "objective": "BRAND_AWARENESS",
        "bidding_strategy": "TARGET_IMPRESSION_SHARE",
        "ad_network": "SEARCH",
        "start_date": "2024-10-01",
        "end_date": "2024-12-31",
    },
    {
        "id": "ga_camp_002",
        "name": "Lead Generation - Performance Max",
        "status": "ENABLED",
        "budget": 25000.00,
        "spend": 21875.50,
        "impressions": 1850000,
        "clicks": 72500,
        "conversions": 3420.0,
        "objective": "LEAD_GENERATION",
        "bidding_strategy": "MAXIMIZE_CONVERSIONS",
        "ad_network": "PERFORMANCE_MAX",
        "start_date": "2024-09-15",
        "end_date": None,
    },
    {
        "id": "ga_camp_003",
        "name": "Retargeting - Display Network",
        "status": "ENABLED",
        "budget": 8000.00,
        "spend": 6540.25,
        "impressions": 5200000,
        "clicks": 31200,
        "conversions": 890.0,
        "objective": "SALES",
        "bidding_strategy": "TARGET_ROAS",
        "ad_network": "DISPLAY",
        "start_date": "2024-11-01",
        "end_date": None,
    },
    {
        "id": "ga_camp_004",
        "name": "YouTube Video Ads - Product Launch",
        "status": "PAUSED",
        "budget": 12000.00,
        "spend": 11250.00,
        "impressions": 890000,
        "clicks": 15600,
        "conversions": 445.0,
        "objective": "PRODUCT_AND_BRAND_CONSIDERATION",
        "bidding_strategy": "TARGET_CPV",
        "ad_network": "VIDEO",
        "start_date": "2024-08-01",
        "end_date": "2024-10-31",
    },
    {
        "id": "ga_camp_005",
        "name": "Shopping - Holiday Season",
        "status": "ENABLED",
        "budget": 35000.00,
        "spend": 28750.80,
        "impressions": 3200000,
        "clicks": 125000,
        "conversions": 8750.0,
        "objective": "SALES",
        "bidding_strategy": "MAXIMIZE_CONVERSION_VALUE",
        "ad_network": "SHOPPING",
        "start_date": "2024-11-15",
        "end_date": "2024-12-31",
    },
]

GOOGLE_ADS_ACCOUNT = {
    "id": "123-456-7890",
    "name": "Demo Google Ads Account",
    "currency": "USD",
    "timezone": "America/New_York",
    "status": "active",
    "manager_customer_id": "111-222-3333",
}


# =============================================================================
# Meta Ads Mock Data
# =============================================================================

META_ADS_CAMPAIGNS = [
    {
        "id": "meta_camp_001",
        "name": "Conversions - Website Purchases",
        "status": "ACTIVE",
        "budget": 18000.00,
        "spend": 15234.50,
        "impressions": 3450000,
        "clicks": 89500,
        "conversions": 2340.0,
        "objective": "CONVERSIONS",
        "optimization_goal": "OFFSITE_CONVERSIONS",
        "buying_type": "AUCTION",
        "start_date": "2024-10-01",
        "end_date": None,
    },
    {
        "id": "meta_camp_002",
        "name": "Engagement - Social Proof",
        "status": "ACTIVE",
        "budget": 5000.00,
        "spend": 4125.75,
        "impressions": 2100000,
        "clicks": 156000,
        "conversions": 0.0,
        "objective": "ENGAGEMENT",
        "optimization_goal": "POST_ENGAGEMENT",
        "buying_type": "AUCTION",
        "start_date": "2024-09-01",
        "end_date": None,
    },
    {
        "id": "meta_camp_003",
        "name": "Reach - Brand Campaign",
        "status": "ACTIVE",
        "budget": 10000.00,
        "spend": 8456.20,
        "impressions": 8900000,
        "clicks": 42000,
        "conversions": 560.0,
        "objective": "REACH",
        "optimization_goal": "REACH",
        "buying_type": "RESERVATION",
        "start_date": "2024-11-01",
        "end_date": "2024-12-31",
    },
    {
        "id": "meta_camp_004",
        "name": "App Installs - Mobile Gaming",
        "status": "PAUSED",
        "budget": 20000.00,
        "spend": 18750.00,
        "impressions": 4500000,
        "clicks": 225000,
        "conversions": 12500.0,
        "objective": "APP_INSTALLS",
        "optimization_goal": "APP_INSTALLS",
        "buying_type": "AUCTION",
        "start_date": "2024-07-01",
        "end_date": "2024-09-30",
    },
    {
        "id": "meta_camp_005",
        "name": "Catalog Sales - Dynamic Retargeting",
        "status": "ACTIVE",
        "budget": 15000.00,
        "spend": 12890.45,
        "impressions": 2800000,
        "clicks": 98000,
        "conversions": 4560.0,
        "objective": "CATALOG_SALES",
        "optimization_goal": "OFFSITE_CONVERSIONS",
        "buying_type": "AUCTION",
        "start_date": "2024-10-15",
        "end_date": None,
    },
]

META_ADS_ACCOUNT = {
    "id": "act_1234567890",
    "name": "Demo Meta Ads Account",
    "currency": "USD",
    "timezone": "America/Los_Angeles",
    "status": "ACTIVE",
    "business_id": "9876543210",
}


# =============================================================================
# Campaign Manager 360 Mock Data
# =============================================================================

CM360_CAMPAIGNS = [
    {
        "id": "cm_camp_001",
        "name": "Display - Programmatic Guaranteed",
        "status": "ACTIVE",
        "budget": 50000.00,
        "spend": 42350.75,
        "impressions": 12500000,
        "clicks": 87500,
        "conversions": 1875.0,
        "advertiser_id": "adv_001",
        "advertiser_name": "Demo Brand Inc.",
        "billing_invoice_code": "INV-2024-Q4-001",
        "start_date": "2024-10-01",
        "end_date": "2024-12-31",
    },
    {
        "id": "cm_camp_002",
        "name": "Video - CTV Campaign",
        "status": "ACTIVE",
        "budget": 75000.00,
        "spend": 61250.50,
        "impressions": 8900000,
        "clicks": 445000,
        "conversions": 2890.0,
        "advertiser_id": "adv_001",
        "advertiser_name": "Demo Brand Inc.",
        "billing_invoice_code": "INV-2024-Q4-002",
        "start_date": "2024-09-15",
        "end_date": None,
    },
    {
        "id": "cm_camp_003",
        "name": "Audio - Podcast Sponsorship",
        "status": "ACTIVE",
        "budget": 25000.00,
        "spend": 18750.25,
        "impressions": 3200000,
        "clicks": 64000,
        "conversions": 890.0,
        "advertiser_id": "adv_002",
        "advertiser_name": "Secondary Brand LLC",
        "billing_invoice_code": "INV-2024-Q4-003",
        "start_date": "2024-11-01",
        "end_date": "2024-12-31",
    },
    {
        "id": "cm_camp_004",
        "name": "Rich Media - Interactive Ads",
        "status": "COMPLETED",
        "budget": 30000.00,
        "spend": 29875.00,
        "impressions": 6700000,
        "clicks": 201000,
        "conversions": 1560.0,
        "advertiser_id": "adv_001",
        "advertiser_name": "Demo Brand Inc.",
        "billing_invoice_code": "INV-2024-Q3-004",
        "start_date": "2024-07-01",
        "end_date": "2024-09-30",
    },
]

CM360_ACCOUNT = {
    "id": "profile_123456",
    "name": "Demo CM360 Network",
    "currency": "USD",
    "timezone": "America/Chicago",
    "status": "active",
    "network_id": "net_789012",
    "advertisers": ["adv_001", "adv_002"],
}


# =============================================================================
# DV360 Mock Data
# =============================================================================

DV360_CAMPAIGNS = [
    {
        "id": "dv_camp_001",
        "name": "Awareness - Cross-Device Targeting",
        "status": "ENTITY_STATUS_ACTIVE",
        "budget": 80000.00,
        "spend": 68450.75,
        "impressions": 25000000,
        "clicks": 375000,
        "conversions": 4560.0,
        "partner_id": "partner_001",
        "advertiser_id": "dv_adv_001",
        "insertion_order_id": "io_001",
        "campaign_goal_type": "CAMPAIGN_GOAL_TYPE_BRAND_AWARENESS",
        "start_date": "2024-10-01",
        "end_date": "2024-12-31",
    },
    {
        "id": "dv_camp_002",
        "name": "Performance - CPA Optimization",
        "status": "ENTITY_STATUS_ACTIVE",
        "budget": 45000.00,
        "spend": 38750.50,
        "impressions": 8500000,
        "clicks": 255000,
        "conversions": 8920.0,
        "partner_id": "partner_001",
        "advertiser_id": "dv_adv_001",
        "insertion_order_id": "io_002",
        "campaign_goal_type": "CAMPAIGN_GOAL_TYPE_DRIVE_CONVERSIONS",
        "start_date": "2024-09-15",
        "end_date": None,
    },
    {
        "id": "dv_camp_003",
        "name": "YouTube Reserve - Premium Inventory",
        "status": "ENTITY_STATUS_ACTIVE",
        "budget": 100000.00,
        "spend": 85250.25,
        "impressions": 15000000,
        "clicks": 750000,
        "conversions": 3450.0,
        "partner_id": "partner_001",
        "advertiser_id": "dv_adv_002",
        "insertion_order_id": "io_003",
        "campaign_goal_type": "CAMPAIGN_GOAL_TYPE_BRAND_AWARENESS",
        "start_date": "2024-11-01",
        "end_date": "2024-12-31",
    },
    {
        "id": "dv_camp_004",
        "name": "Audio - Streaming Platforms",
        "status": "ENTITY_STATUS_PAUSED",
        "budget": 35000.00,
        "spend": 32100.00,
        "impressions": 9800000,
        "clicks": 196000,
        "conversions": 1250.0,
        "partner_id": "partner_001",
        "advertiser_id": "dv_adv_001",
        "insertion_order_id": "io_004",
        "campaign_goal_type": "CAMPAIGN_GOAL_TYPE_BRAND_AWARENESS",
        "start_date": "2024-08-01",
        "end_date": "2024-10-31",
    },
    {
        "id": "dv_camp_005",
        "name": "Connected TV - Household Targeting",
        "status": "ENTITY_STATUS_ACTIVE",
        "budget": 120000.00,
        "spend": 98750.80,
        "impressions": 6500000,
        "clicks": 130000,
        "conversions": 2890.0,
        "partner_id": "partner_001",
        "advertiser_id": "dv_adv_001",
        "insertion_order_id": "io_005",
        "campaign_goal_type": "CAMPAIGN_GOAL_TYPE_DRIVE_CONVERSIONS",
        "start_date": "2024-10-15",
        "end_date": None,
    },
]

DV360_ACCOUNT = {
    "id": "dv_partner_123456",
    "name": "Demo DV360 Partner",
    "currency": "USD",
    "timezone": "America/New_York",
    "status": "ENTITY_STATUS_ACTIVE",
    "advertisers": [
        {"id": "dv_adv_001", "name": "Primary Advertiser"},
        {"id": "dv_adv_002", "name": "Secondary Advertiser"},
    ],
}


# =============================================================================
# Helper Functions
# =============================================================================

def get_mock_performance_data(
    platform: str,
    start_date: datetime,
    end_date: datetime,
) -> Dict[str, Any]:
    """
    Generate aggregated performance metrics for a date range.
    
    Uses the campaign data to calculate totals.
    """
    campaign_data = {
        "google_ads": GOOGLE_ADS_CAMPAIGNS,
        "meta_ads": META_ADS_CAMPAIGNS,
        "campaign_manager": CM360_CAMPAIGNS,
        "dv360": DV360_CAMPAIGNS,
    }
    
    campaigns = campaign_data.get(platform, [])
    
    # Aggregate metrics
    total_spend = sum(c["spend"] for c in campaigns)
    total_impressions = sum(c["impressions"] for c in campaigns)
    total_clicks = sum(c["clicks"] for c in campaigns)
    total_conversions = sum(c["conversions"] for c in campaigns)
    
    # Estimate revenue (assume avg $50 per conversion)
    total_revenue = total_conversions * random.uniform(45, 55)
    
    return {
        "spend": total_spend,
        "impressions": total_impressions,
        "clicks": total_clicks,
        "conversions": total_conversions,
        "revenue": round(total_revenue, 2),
    }


def get_mock_account(platform: str) -> Dict[str, Any]:
    """Get mock account data for a platform."""
    accounts = {
        "google_ads": GOOGLE_ADS_ACCOUNT,
        "meta_ads": META_ADS_ACCOUNT,
        "campaign_manager": CM360_ACCOUNT,
        "dv360": DV360_ACCOUNT,
    }
    return accounts.get(platform, {})

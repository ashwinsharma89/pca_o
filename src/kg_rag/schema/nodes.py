"""
KG-RAG Node Definitions

Defines all node types for the Knowledge Graph schema.
Each node class includes properties, constraints, and index definitions.
"""

from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
from enum import Enum
from datetime import date


class NodeLabel(str, Enum):
    """Node type labels for Neo4j."""
    CHANNEL = "Channel"
    PLATFORM = "Platform"
    ACCOUNT = "Account"
    CAMPAIGN = "Campaign"
    TARGETING = "Targeting"
    METRIC = "Metric"
    ENTITY_GROUP = "EntityGroup"
    CREATIVE = "Creative"
    KEYWORD = "Keyword"
    PLACEMENT = "Placement"
    AUDIENCE = "Audience"


@dataclass
class ChannelNode:
    """Top-level channel categorization."""
    id: str  # search, social, display, programmatic
    name: str
    description: str
    
    LABEL = NodeLabel.CHANNEL
    
    # Seed data for initialization
    SEED_DATA = [
        {"id": "search", "name": "Search", "description": "Intent-based advertising on search engines"},
        {"id": "social", "name": "Social", "description": "Advertising on social media platforms"},
        {"id": "display", "name": "Display", "description": "Banner/native ads on content networks"},
        {"id": "programmatic", "name": "Programmatic", "description": "Automated buying across ad exchanges"},
    ]


@dataclass
class PlatformNode:
    """Advertising platform."""
    id: str  # google_ads, meta, linkedin, etc.
    name: str
    channel_id: str
    api_source: str
    parent_company: str
    
    # Capabilities
    supports_keywords: bool = False
    supports_placements: bool = False
    supports_video_metrics: bool = False
    supports_reach: bool = False
    supports_revenue: bool = False
    supports_b2b_targeting: bool = False
    
    # Column aliases stored as JSON
    column_aliases: Optional[Dict[str, str]] = None
    
    LABEL = NodeLabel.PLATFORM
    
    # Seed data for all platforms
    SEED_DATA = [
        # Search
        {"id": "google_ads", "name": "Google Ads", "channel_id": "search", "api_source": "Google Ads API", "parent_company": "Google", "supports_keywords": True, "supports_placements": True, "supports_video_metrics": True, "supports_revenue": True},
        {"id": "bing_ads", "name": "Microsoft Advertising", "channel_id": "search", "api_source": "Microsoft Ads API", "parent_company": "Microsoft", "supports_keywords": True, "supports_revenue": True},
        {"id": "apple_search", "name": "Apple Search Ads", "channel_id": "search", "api_source": "Apple Search Ads API", "parent_company": "Apple", "supports_keywords": True},
        {"id": "amazon_sponsored", "name": "Amazon Sponsored Ads", "channel_id": "search", "api_source": "Amazon Ads API", "parent_company": "Amazon", "supports_keywords": True, "supports_revenue": True},
        
        # Social
        {"id": "meta", "name": "Meta Ads", "channel_id": "social", "api_source": "Facebook Marketing API", "parent_company": "Meta", "supports_placements": True, "supports_video_metrics": True, "supports_reach": True, "supports_revenue": True},
        {"id": "instagram", "name": "Instagram Ads", "channel_id": "social", "api_source": "Facebook Marketing API", "parent_company": "Meta", "supports_placements": True, "supports_video_metrics": True, "supports_reach": True},
        {"id": "linkedin", "name": "LinkedIn Ads", "channel_id": "social", "api_source": "LinkedIn Marketing API", "parent_company": "Microsoft", "supports_reach": True, "supports_b2b_targeting": True},
        {"id": "tiktok", "name": "TikTok Ads", "channel_id": "social", "api_source": "TikTok Marketing API", "parent_company": "ByteDance", "supports_video_metrics": True, "supports_reach": True, "supports_revenue": True},
        {"id": "snapchat", "name": "Snapchat Ads", "channel_id": "social", "api_source": "Snapchat Marketing API", "parent_company": "Snap Inc.", "supports_placements": True, "supports_video_metrics": True, "supports_reach": True},
        {"id": "twitter", "name": "X/Twitter Ads", "channel_id": "social", "api_source": "Twitter Ads API", "parent_company": "X Corp", "supports_video_metrics": True, "supports_reach": True},
        {"id": "pinterest", "name": "Pinterest Ads", "channel_id": "social", "api_source": "Pinterest Ads API", "parent_company": "Pinterest", "supports_keywords": True, "supports_revenue": True},
        {"id": "youtube", "name": "YouTube Ads", "channel_id": "social", "api_source": "Google Ads API", "parent_company": "Google", "supports_keywords": True, "supports_placements": True, "supports_video_metrics": True, "supports_revenue": True},
        
        # Display
        {"id": "gdn", "name": "Google Display Network", "channel_id": "display", "api_source": "Google Ads API", "parent_company": "Google", "supports_placements": True, "supports_video_metrics": True},
        {"id": "taboola", "name": "Taboola", "channel_id": "display", "api_source": "Taboola API", "parent_company": "Taboola", "supports_placements": True},
        {"id": "outbrain", "name": "Outbrain", "channel_id": "display", "api_source": "Outbrain API", "parent_company": "Outbrain", "supports_placements": True},
        
        # Programmatic
        {"id": "dv360", "name": "Display & Video 360", "channel_id": "programmatic", "api_source": "DV360 API", "parent_company": "Google", "supports_placements": True, "supports_video_metrics": True, "supports_reach": True, "supports_revenue": True},
        {"id": "cm360", "name": "Campaign Manager 360", "channel_id": "programmatic", "api_source": "DCM API", "parent_company": "Google", "supports_placements": True, "supports_video_metrics": True, "supports_revenue": True},
        {"id": "trade_desk", "name": "The Trade Desk", "channel_id": "programmatic", "api_source": "TTD API", "parent_company": "The Trade Desk", "supports_placements": True, "supports_video_metrics": True, "supports_revenue": True},
        {"id": "amazon_dsp", "name": "Amazon DSP", "channel_id": "programmatic", "api_source": "Amazon Ads API", "parent_company": "Amazon", "supports_placements": True, "supports_video_metrics": True, "supports_revenue": True},
        {"id": "xandr", "name": "Xandr (AppNexus)", "channel_id": "programmatic", "api_source": "Xandr API", "parent_company": "Microsoft", "supports_placements": True, "supports_video_metrics": True},
    ]


@dataclass
class AccountNode:
    """Advertiser account."""
    id: str
    name: str
    platform_id: str
    currency: str = "USD"
    timezone: str = "UTC"
    
    LABEL = NodeLabel.ACCOUNT


@dataclass
class CampaignNode:
    """Campaign entity - core node of the graph."""
    id: str
    account_id: str
    platform_id: str
    name: str
    
    # Settings
    objective: Optional[str] = None
    status: Optional[str] = None
    budget: Optional[float] = None
    budget_type: Optional[str] = None  # daily, lifetime
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    
    # Denormalized totals (for faster queries)
    impressions_total: int = 0
    clicks_total: int = 0
    spend_total: float = 0.0
    conversions_total: float = 0.0
    revenue_total: float = 0.0
    
    LABEL = NodeLabel.CAMPAIGN


@dataclass
class TargetingNode:
    """Campaign targeting configuration - 65+ properties."""
    campaign_id: str
    
    # Demographics
    age_range: Optional[str] = None
    gender: Optional[str] = None
    languages: Optional[List[str]] = None
    income_bracket: Optional[str] = None
    education_level: Optional[str] = None
    parental_status: Optional[str] = None
    marital_status: Optional[str] = None
    homeowner_status: Optional[str] = None
    
    # Geographic
    geo_countries: Optional[List[str]] = None
    geo_regions: Optional[List[str]] = None
    geo_cities: Optional[List[str]] = None
    geo_postal_codes: Optional[List[str]] = None
    geo_dmas: Optional[List[str]] = None
    geo_radius_miles: Optional[float] = None
    
    # Device
    device_types: Optional[List[str]] = None
    operating_systems: Optional[List[str]] = None
    browsers: Optional[List[str]] = None
    carriers: Optional[List[str]] = None
    connection_type: Optional[str] = None
    
    # Audience
    audience_ids: Optional[List[str]] = None
    audience_names: Optional[List[str]] = None
    audience_type: Optional[str] = None
    audience_source: Optional[str] = None
    lookalike_percent: Optional[float] = None
    retargeting_window_days: Optional[int] = None
    
    # Interests & Behaviors
    interests: Optional[List[str]] = None
    affinities: Optional[List[str]] = None
    in_market: Optional[List[str]] = None
    behaviors: Optional[List[str]] = None
    life_events: Optional[List[str]] = None
    
    # Contextual
    topics: Optional[List[str]] = None
    keywords_contextual: Optional[List[str]] = None
    content_categories: Optional[List[str]] = None
    brand_safety_level: Optional[str] = None
    
    # Placement
    placements_included: Optional[List[str]] = None
    placements_excluded: Optional[List[str]] = None
    placement_type: Optional[str] = None
    inventory_type: Optional[str] = None
    viewability_threshold: Optional[float] = None
    
    # B2B (LinkedIn)
    job_titles: Optional[List[str]] = None
    job_functions: Optional[List[str]] = None
    job_seniorities: Optional[List[str]] = None
    companies: Optional[List[str]] = None
    company_industries: Optional[List[str]] = None
    company_sizes: Optional[List[str]] = None
    skills: Optional[List[str]] = None
    
    # Funnel
    funnel_stage: Optional[str] = None
    objective_type: Optional[str] = None
    
    # Delivery
    bid_strategy: Optional[str] = None
    bid_amount: Optional[float] = None
    budget_type: Optional[str] = None
    optimization_goal: Optional[str] = None
    frequency_cap: Optional[int] = None
    
    # Ad Format
    ad_type: Optional[str] = None
    ad_format: Optional[str] = None
    
    # Metadata
    completeness_score: float = 0.0
    available_fields: Optional[List[str]] = None
    
    LABEL = NodeLabel.TARGETING


@dataclass
class MetricNode:
    """Daily performance metrics - raw additive metrics only."""
    id: str  # campaign_id + date
    campaign_id: str
    date: date
    
    # Core (always present)
    impressions: int = 0
    clicks: int = 0
    spend: float = 0.0
    
    # Optional
    conversions: Optional[float] = None
    revenue: Optional[float] = None
    reach: Optional[int] = None
    frequency: Optional[float] = None
    
    # Video (optional)
    video_plays: Optional[int] = None
    video_25: Optional[int] = None
    video_50: Optional[int] = None
    video_75: Optional[int] = None
    video_completes: Optional[int] = None
    
    # Engagement (optional)
    engagements: Optional[int] = None
    likes: Optional[int] = None
    comments: Optional[int] = None
    shares: Optional[int] = None
    
    # Attribution (optional)
    pc_conversions: Optional[float] = None
    pv_conversions: Optional[float] = None
    
    # Custom (escape hatch)
    custom_metrics: Optional[Dict[str, Any]] = None
    
    LABEL = NodeLabel.METRIC


@dataclass
class EntityGroupNode:
    """Ad Group / Ad Set / Line Item."""
    id: str
    campaign_id: str
    name: str
    entity_type: str  # ad_group, ad_set, line_item
    
    daily_budget: Optional[float] = None
    bid_strategy: Optional[str] = None
    status: Optional[str] = None
    
    LABEL = NodeLabel.ENTITY_GROUP


@dataclass
class CreativeNode:
    """Ad / Creative asset."""
    id: str
    entity_group_id: str
    name: str
    creative_type: str  # image, video, carousel, native, text
    
    headline: Optional[str] = None
    description: Optional[str] = None
    landing_url: Optional[str] = None
    image_url: Optional[str] = None
    video_url: Optional[str] = None
    
    # Quality
    ad_strength: Optional[str] = None
    
    LABEL = NodeLabel.CREATIVE


@dataclass
class KeywordNode:
    """Search keyword (Search platforms only)."""
    id: str
    entity_group_id: str
    text: str
    match_type: str  # exact, phrase, broad
    
    quality_score: Optional[int] = None
    bid_amount: Optional[float] = None
    status: Optional[str] = None
    
    # Performance (denormalized)
    impressions: int = 0
    clicks: int = 0
    spend: float = 0.0
    conversions: float = 0.0
    
    LABEL = NodeLabel.KEYWORD


@dataclass
class PlacementNode:
    """Placement / Site / App (Display/Programmatic)."""
    id: str
    entity_group_id: str
    campaign_id: str  # Denormalized for queries
    name: str
    type: str  # site, app, youtube_channel, youtube_video
    
    url: Optional[str] = None
    category: Optional[str] = None
    subcategory: Optional[str] = None
    iab_category: Optional[str] = None
    
    # Position
    position: Optional[str] = None  # above_fold, below_fold, sidebar
    ad_slot: Optional[str] = None
    
    # Quality
    viewability_rate: Optional[float] = None
    brand_safety_score: Optional[float] = None
    
    # Performance (denormalized)
    impressions: int = 0
    clicks: int = 0
    spend: float = 0.0
    conversions: float = 0.0
    
    # CM360/DV360 specific
    site_id: Optional[str] = None
    placement_id: Optional[str] = None
    cost_structure: Optional[str] = None
    contracted_units: Optional[int] = None
    contracted_cost: Optional[float] = None
    
    LABEL = NodeLabel.PLACEMENT


@dataclass
class AudienceNode:
    """Audience segment for overlap analysis."""
    id: str
    name: str
    type: str  # first_party, third_party, lookalike
    size: Optional[int] = None
    source: Optional[str] = None
    
    LABEL = NodeLabel.AUDIENCE

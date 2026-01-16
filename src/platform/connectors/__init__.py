"""
Ad Platform API Connectors.

Unified interface for 12 major ad platforms with mock mode support.
"""

from src.platform.connectors.base_connector import BaseAdConnector, ConnectorStatus
from src.platform.connectors.google_ads_connector import GoogleAdsConnector
from src.platform.connectors.meta_ads_connector import MetaAdsConnector
from src.platform.connectors.campaign_manager_connector import CampaignManagerConnector
from src.platform.connectors.dv360_connector import DV360Connector
from src.platform.connectors.snapchat_ads_connector import SnapchatAdsConnector
from src.platform.connectors.tiktok_ads_connector import TikTokAdsConnector
from src.platform.connectors.twitter_ads_connector import TwitterAdsConnector
from src.platform.connectors.linkedin_ads_connector import LinkedInAdsConnector
from src.platform.connectors.tradedesk_connector import TradeDeskConnector
from src.platform.connectors.amazon_dsp_connector import AmazonDSPConnector
from src.platform.connectors.pinterest_ads_connector import PinterestAdsConnector
from src.platform.connectors.apple_search_ads_connector import AppleSearchAdsConnector
from src.platform.connectors.connector_manager import AdConnectorManager

__all__ = [
    "BaseAdConnector",
    "ConnectorStatus",
    # Phase 1: Core Platforms
    "GoogleAdsConnector",
    "MetaAdsConnector",
    "CampaignManagerConnector",
    "DV360Connector",
    # Phase 2: Additional Platforms
    "SnapchatAdsConnector",
    "TikTokAdsConnector",
    "TwitterAdsConnector",
    "LinkedInAdsConnector",
    "TradeDeskConnector",
    "AmazonDSPConnector",
    "PinterestAdsConnector",
    "AppleSearchAdsConnector",
    # Manager
    "AdConnectorManager",
]

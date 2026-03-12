"""
Campaign Transformer for KG-RAG ETL

Transforms raw campaign data into graph-ready format.
"""

import hashlib
import logging
from datetime import date, datetime
from typing import Any, Optional

logger = logging.getLogger(__name__)


class CampaignTransformer:
    """
    Transform campaign records for KùzuDB ingestion.

    Creates Campaign nodes with proper ID generation and field mapping.
    """

    # Fields to extract for Campaign node
    CAMPAIGN_FIELDS = [
        "campaign_id", "campaign_name", "platform", "account_id",
        "objective", "status", "budget", "budget_type",
        "start_date", "end_date"
    ]

    def __init__(self, default_platform: Optional[str] = None):
        """
        Initialize transformer.

        Args:
            default_platform: Default platform if not in data
        """
        self.default_platform = default_platform

    def transform(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        Transform campaign records for KùzuDB.

        Args:
            records: Raw campaign records (with canonical column names)

        Returns:
            List of transformed Campaign node properties
        """
        transformed = []

        for record in records:
            try:
                campaign = self._transform_record(record)
                if campaign:
                    transformed.append(campaign)
            except Exception as e:
                logger.warning(f"Failed to transform record: {e}")
                continue

        return transformed

    def _transform_record(self, record: dict[str, Any]) -> Optional[dict[str, Any]]:
        """Transform a single campaign record."""
        # Generate ID
        campaign_id = self._get_campaign_id(record)
        if not campaign_id:
            logger.warning("Record missing campaign_id, skipping")
            return None

        # Get platform
        platform = record.get("platform", self.default_platform)
        if not platform:
            logger.warning(f"Record {campaign_id} missing platform")

        # Build node properties
        campaign = {
            "id": campaign_id,
            "platform_id": self._normalize_platform(platform) if platform else None,
            "account_id": record.get("account_id"),
            "name": record.get("campaign_name") or record.get("name") or f"Campaign {campaign_id}",
            "objective": self._normalize_objective(record.get("objective")),
            "status": self._normalize_status(record.get("status")),
            "budget": self._to_float(record.get("budget")),
            "budget_type": record.get("budget_type"),
            "start_date": self._to_date(record.get("start_date")),
            "end_date": self._to_date(record.get("end_date")),

            # Initialize totals (will be updated by metrics)
            "impressions_total": 0,
            "clicks_total": 0,
            "spend_total": 0.0,
            "conversions_total": 0.0,
            "revenue_total": 0.0,
        }

        return campaign

    def _get_campaign_id(self, record: dict[str, Any]) -> Optional[str]:
        """Get or generate campaign ID."""
        # Try common ID fields
        for field in ["campaign_id", "id", "Campaign ID", "campaign"]:
            if field in record and record[field]:
                return str(record[field])

        # Generate from name + platform if available
        name = record.get("campaign_name") or record.get("name")
        platform = record.get("platform")
        if name and platform:
            hash_input = f"{platform}:{name}"
            return hashlib.md5(hash_input.encode(), usedforsecurity=False).hexdigest()[:12]

        return None

    def _normalize_platform(self, platform: str) -> str:
        """Normalize platform name to ID."""
        if not platform:
            return "unknown"

        platform_lower = platform.lower().strip()

        # Common mappings
        mappings = {
            "facebook": "meta",
            "fb": "meta",
            "meta ads": "meta",
            "instagram": "meta",
            "google": "google_ads",
            "google ads": "google_ads",
            "adwords": "google_ads",
            "linkedin": "linkedin",
            "linkedin ads": "linkedin",
            "tiktok": "tiktok",
            "tiktok ads": "tiktok",
            "dv360": "dv360",
            "display video 360": "dv360",
            "display & video 360": "dv360",
            "cm360": "cm360",
            "campaign manager": "cm360",
            "dcm": "cm360",
            "snapchat": "snapchat",
            "snap": "snapchat",
            "twitter": "twitter",
            "x": "twitter",
            "pinterest": "pinterest",
            "youtube": "youtube",
            "bing": "bing_ads",
            "microsoft": "bing_ads",
            "amazon": "amazon_dsp",
            "ttd": "trade_desk",
            "the trade desk": "trade_desk",
        }

        return mappings.get(platform_lower, platform_lower.replace(" ", "_"))

    def _normalize_objective(self, objective: Optional[str]) -> Optional[str]:
        """Normalize campaign objective."""
        if not objective:
            return None

        obj_lower = objective.lower().strip()

        mappings = {
            "conversions": "conversions",
            "conversion": "conversions",
            "sales": "conversions",
            "leads": "leads",
            "lead generation": "leads",
            "lead_generation": "leads",
            "traffic": "traffic",
            "link clicks": "traffic",
            "website traffic": "traffic",
            "awareness": "awareness",
            "brand awareness": "awareness",
            "reach": "awareness",
            "video views": "video_views",
            "video": "video_views",
            "app installs": "app_installs",
            "app_installs": "app_installs",
            "engagement": "engagement",
            "messages": "messages",
            "catalog sales": "catalog_sales",
        }

        return mappings.get(obj_lower, objective)

    def _normalize_status(self, status: Optional[str]) -> Optional[str]:
        """Normalize campaign status."""
        if not status:
            return None

        status_lower = status.lower().strip()

        mappings = {
            "active": "active",
            "enabled": "active",
            "running": "active",
            "paused": "paused",
            "inactive": "paused",
            "completed": "completed",
            "ended": "completed",
            "removed": "removed",
            "deleted": "removed",
            "archived": "archived",
            "draft": "draft",
        }

        return mappings.get(status_lower, status)

    def _to_float(self, value: Any) -> Optional[float]:
        """Convert value to float."""
        if value is None:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def _to_date(self, value: Any) -> Optional[str]:
        """Convert value to date string (ISO format)."""
        if value is None:
            return None

        if isinstance(value, datetime):
            return value.date().isoformat()
        if isinstance(value, date):
            return value.isoformat()
        if isinstance(value, str):
            # Try parsing common formats
            for fmt in ["%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%Y/%m/%d"]:
                try:
                    return datetime.strptime(value, fmt).date().isoformat()
                except ValueError:
                    continue
            return value  # Return as-is if can't parse

        return None

"""
Targeting Transformer for KG-RAG ETL

Transforms campaign targeting data into graph-ready format.
Handles 65+ targeting dimensions across 10 categories.
"""

import logging
from typing import Dict, Any, List, Optional
import json


logger = logging.getLogger(__name__)


class TargetingTransformer:
    """
    Transform targeting configuration for Neo4j ingestion.
    
    Handles dynamic targeting fields based on platform capabilities.
    Calculates completeness score for data quality tracking.
    """
    
    # All supported targeting fields by category
    TARGETING_FIELDS = {
        "demographics": [
            "age_range", "gender", "languages", "income_bracket",
            "education_level", "parental_status", "marital_status", "homeowner_status"
        ],
        "geographic": [
            "geo_countries", "geo_regions", "geo_cities", "geo_postal_codes",
            "geo_dmas", "geo_radius_miles"
        ],
        "device": [
            "device_types", "operating_systems", "browsers", "carriers", "connection_type"
        ],
        "audience": [
            "audience_ids", "audience_names", "audience_type", "audience_source",
            "lookalike_percent", "retargeting_window_days"
        ],
        "interests": [
            "interests", "affinities", "in_market", "behaviors", "life_events"
        ],
        "contextual": [
            "topics", "keywords_contextual", "content_categories",
            "brand_safety_level", "content_exclusions"
        ],
        "placement": [
            "placements_included", "placements_excluded", "placement_type",
            "inventory_type", "viewability_threshold"
        ],
        "b2b": [
            "job_titles", "job_functions", "job_seniorities", "companies",
            "company_industries", "company_sizes", "skills"
        ],
        "funnel": [
            "funnel_stage", "objective_type", "conversion_event"
        ],
        "delivery": [
            "bid_strategy", "bid_amount", "budget_type",
            "optimization_goal", "frequency_cap", "dayparting"
        ],
    }
    
    # Platform-specific field mappings
    PLATFORM_FIELD_ALIASES = {
        "meta": {
            "targeting_age": "age_range",
            "targeting_genders": "gender",
            "geo_locations": "geo_countries",
            "flexible_spec": "interests",
            "custom_audiences": "audience_ids",
        },
        "google_ads": {
            "age_range_type": "age_range",
            "gender_type": "gender",
            "geo_target_constants": "geo_countries",
            "user_lists": "audience_ids",
            "keyword_themes": "topics",
        },
        "linkedin": {
            "facet.urn": "audience_ids",
            "jobTitles": "job_titles",
            "jobFunctions": "job_functions",
            "seniorities": "job_seniorities",
            "industries": "company_industries",
            "companySizes": "company_sizes",
        },
    }
    
    def __init__(self, platform: Optional[str] = None):
        """
        Initialize transformer.
        
        Args:
            platform: Platform ID for field mapping
        """
        self.platform = platform
        self._field_aliases = self.PLATFORM_FIELD_ALIASES.get(platform, {})
    
    def transform(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transform targeting records for Neo4j.
        
        Args:
            records: Campaign records with targeting data
            
        Returns:
            List of transformed Targeting node properties
        """
        transformed = []
        
        for record in records:
            try:
                targeting = self._transform_record(record)
                if targeting:
                    transformed.append(targeting)
            except Exception as e:
                logger.warning(f"Failed to transform targeting: {e}")
                continue
        
        return transformed
    
    def _transform_record(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Transform a single targeting record."""
        campaign_id = record.get("campaign_id") or record.get("id")
        if not campaign_id:
            return None
        
        targeting = {
            "campaign_id": str(campaign_id),
        }
        
        # Track available fields for completeness calculation
        available_fields = []
        
        # Extract targeting fields
        for category, fields in self.TARGETING_FIELDS.items():
            for field in fields:
                # Try canonical name first
                value = record.get(field)
                
                # Try platform-specific aliases
                if value is None and field in self._field_aliases.values():
                    for alias, canonical in self._field_aliases.items():
                        if canonical == field and alias in record:
                            value = record[alias]
                            break
                
                # Try nested targeting object
                if value is None and "targeting" in record:
                    targeting_obj = record["targeting"]
                    if isinstance(targeting_obj, dict):
                        value = targeting_obj.get(field)
                
                # Process value
                if value is not None:
                    processed = self._process_value(field, value)
                    if processed is not None:
                        targeting[field] = processed
                        available_fields.append(field)
        
        # Calculate completeness score
        total_fields = sum(len(f) for f in self.TARGETING_FIELDS.values())
        targeting["completeness_score"] = round(len(available_fields) / total_fields, 2)
        targeting["available_fields"] = available_fields
        
        return targeting
    
    def _process_value(self, field: str, value: Any) -> Any:
        """Process and normalize a targeting value."""
        # Handle JSON strings
        if isinstance(value, str):
            try:
                value = json.loads(value)
            except json.JSONDecodeError:
                pass
        
        # Normalize lists
        list_fields = [
            "languages", "geo_countries", "geo_regions", "geo_cities",
            "geo_postal_codes", "geo_dmas", "device_types", "operating_systems",
            "browsers", "carriers", "audience_ids", "audience_names", "interests",
            "affinities", "in_market", "behaviors", "life_events", "topics",
            "keywords_contextual", "content_categories", "content_exclusions",
            "placements_included", "placements_excluded", "job_titles",
            "job_functions", "job_seniorities", "companies", "company_industries",
            "company_sizes", "skills", "dayparting"
        ]
        
        if field in list_fields:
            if isinstance(value, str):
                # Split comma-separated
                value = [v.strip() for v in value.split(",") if v.strip()]
            elif not isinstance(value, list):
                value = [value]
            return value if value else None
        
        # Normalize numeric fields
        float_fields = [
            "geo_radius_miles", "lookalike_percent", "bid_amount",
            "viewability_threshold", "frequency_cap"
        ]
        
        if field in float_fields:
            try:
                return float(value)
            except (ValueError, TypeError):
                return None
        
        # Normalize int fields
        int_fields = ["retargeting_window_days"]
        if field in int_fields:
            try:
                return int(value)
            except (ValueError, TypeError):
                return None
        
        # Normalize boolean-like fields
        if value in [True, "true", "True", "1", 1]:
            return True
        if value in [False, "false", "False", "0", 0]:
            return False
        
        # Return as-is for strings
        return str(value) if value else None
    
    def extract_from_campaign(self, campaign: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract targeting from a campaign record.
        
        Looks for targeting data in various locations:
        - Top-level targeting fields
        - Nested 'targeting' object
        - Platform-specific format
        
        Args:
            campaign: Campaign record
            
        Returns:
            Targeting record
        """
        # Start with campaign ID
        result = {
            "campaign_id": campaign.get("campaign_id") or campaign.get("id"),
        }
        
        # Copy top-level targeting fields
        for category, fields in self.TARGETING_FIELDS.items():
            for field in fields:
                if field in campaign:
                    result[field] = campaign[field]
        
        # Merge nested targeting object
        if "targeting" in campaign and isinstance(campaign["targeting"], dict):
            for key, value in campaign["targeting"].items():
                if key not in result or result[key] is None:
                    result[key] = value
        
        return self._transform_record(result)

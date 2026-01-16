"""
Data Anonymization Module for LLM Privacy Protection

This module provides anonymization capabilities to protect sensitive data
before sending to external LLM APIs (OpenAI, Gemini, Anthropic).

Features:
- Campaign/Client/Brand name tokenization
- Reversible mapping for UI display
- Configurable anonymization levels
- Audit logging of anonymization events
"""

import hashlib
import re
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum
from loguru import logger
import pandas as pd
import json
from datetime import datetime


class AnonymizationLevel(Enum):
    """Levels of data anonymization."""
    NONE = "none"  # No anonymization
    LIGHT = "light"  # Only campaign/client names
    MODERATE = "moderate"  # Names + geographic data
    STRICT = "strict"  # All identifiable fields + numeric obfuscation


@dataclass
class AnonymizationConfig:
    """Configuration for data anonymization."""
    level: AnonymizationLevel = AnonymizationLevel.MODERATE
    
    # Fields to anonymize at each level
    light_fields: List[str] = field(default_factory=lambda: [
        'Campaign_Name', 'Campaign', 'campaign_name', 'campaign',
        'Client', 'client', 'Client_Name', 'client_name',
        'Brand', 'brand', 'Brand_Name', 'brand_name',
        'Account', 'account', 'Account_Name', 'account_name',
        'Advertiser', 'advertiser'
    ])
    
    moderate_fields: List[str] = field(default_factory=lambda: [
        'Geo', 'geo', 'Geography', 'geography',
        'Country', 'country', 'Region', 'region',
        'City', 'city', 'State', 'state',
        'Creative', 'creative', 'Creative_Name', 'creative_name',
        'Ad_Name', 'ad_name', 'AdGroup', 'ad_group'
    ])
    
    strict_fields: List[str] = field(default_factory=lambda: [
        'Audience', 'audience', 'Audience_Segment', 'audience_segment',
        'Placement', 'placement', 'Publisher', 'publisher',
        'URL', 'url', 'Domain', 'domain'
    ])
    
    # Preserve these fields (never anonymize)
    preserve_fields: List[str] = field(default_factory=lambda: [
        'Platform', 'platform',  # Generic platform names are OK
        'Date', 'date', 'Month', 'Year',
        'Device', 'device', 'Device_Type'
    ])
    
    # Numeric fields that might need obfuscation in strict mode
    sensitive_numeric_fields: List[str] = field(default_factory=lambda: [
        'Budget', 'budget', 'Cost', 'Revenue'
    ])


class DataAnonymizer:
    """
    Handles data anonymization for LLM privacy protection.
    
    Usage:
        anonymizer = DataAnonymizer(level=AnonymizationLevel.MODERATE)
        anon_data, mapping = anonymizer.anonymize_dict(data)
        # Send anon_data to LLM
        # Use mapping to restore original values in UI
    """
    
    def __init__(self, config: Optional[AnonymizationConfig] = None):
        """Initialize anonymizer with configuration."""
        self.config = config or AnonymizationConfig()
        self._mapping: Dict[str, Dict[str, str]] = {}  # field -> {original: token}
        self._reverse_mapping: Dict[str, Dict[str, str]] = {}  # field -> {token: original}
        self._token_counters: Dict[str, int] = {}
        self._session_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        logger.info(f"DataAnonymizer initialized with level: {self.config.level.value}")
    
    def _get_fields_to_anonymize(self) -> List[str]:
        """Get list of fields to anonymize based on level."""
        fields = []
        
        if self.config.level == AnonymizationLevel.NONE:
            return []
        
        if self.config.level in [AnonymizationLevel.LIGHT, AnonymizationLevel.MODERATE, AnonymizationLevel.STRICT]:
            fields.extend(self.config.light_fields)
        
        if self.config.level in [AnonymizationLevel.MODERATE, AnonymizationLevel.STRICT]:
            fields.extend(self.config.moderate_fields)
        
        if self.config.level == AnonymizationLevel.STRICT:
            fields.extend(self.config.strict_fields)
        
        return fields
    
    def _generate_token(self, field_name: str, original_value: str) -> str:
        """Generate a consistent token for a value."""
        # Normalize field name
        field_key = field_name.lower().replace('_', '')
        
        # Check if we already have a token for this value
        if field_key not in self._mapping:
            self._mapping[field_key] = {}
            self._reverse_mapping[field_key] = {}
            self._token_counters[field_key] = 0
        
        if original_value in self._mapping[field_key]:
            return self._mapping[field_key][original_value]
        
        # Generate new token
        self._token_counters[field_key] += 1
        
        # Create readable token based on field type
        if 'campaign' in field_key:
            token = f"Campaign_{self._token_counters[field_key]:03d}"
        elif 'client' in field_key or 'brand' in field_key or 'advertiser' in field_key:
            token = f"Client_{self._token_counters[field_key]:03d}"
        elif 'creative' in field_key or 'ad' in field_key:
            token = f"Creative_{self._token_counters[field_key]:03d}"
        elif 'geo' in field_key or 'country' in field_key or 'region' in field_key or 'city' in field_key:
            token = f"Region_{self._token_counters[field_key]:03d}"
        elif 'audience' in field_key:
            token = f"Audience_{self._token_counters[field_key]:03d}"
        elif 'placement' in field_key or 'publisher' in field_key:
            token = f"Placement_{self._token_counters[field_key]:03d}"
        else:
            token = f"Item_{self._token_counters[field_key]:03d}"
        
        # Store mappings
        self._mapping[field_key][original_value] = token
        self._reverse_mapping[field_key][token] = original_value
        
        return token
    
    def anonymize_value(self, field_name: str, value: Any) -> Any:
        """Anonymize a single value if the field should be anonymized."""
        if value is None or pd.isna(value):
            return value
        
        fields_to_anonymize = self._get_fields_to_anonymize()
        
        # Check if this field should be anonymized
        should_anonymize = any(
            field_name.lower() == f.lower() or field_name.lower().replace('_', '') == f.lower().replace('_', '')
            for f in fields_to_anonymize
        )
        
        # Check if field is in preserve list
        is_preserved = any(
            field_name.lower() == f.lower() or field_name.lower().replace('_', '') == f.lower().replace('_', '')
            for f in self.config.preserve_fields
        )
        
        if is_preserved or not should_anonymize:
            return value
        
        # Convert to string and anonymize
        str_value = str(value)
        return self._generate_token(field_name, str_value)
    
    def anonymize_dataframe(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        """
        Anonymize a pandas DataFrame.
        
        Returns:
            Tuple of (anonymized_df, mapping_info)
        """
        if self.config.level == AnonymizationLevel.NONE:
            return df.copy(), {}
        
        anon_df = df.copy()
        fields_to_anonymize = self._get_fields_to_anonymize()
        anonymized_columns = []
        
        for col in anon_df.columns:
            should_anonymize = any(
                col.lower() == f.lower() or col.lower().replace('_', '') == f.lower().replace('_', '')
                for f in fields_to_anonymize
            )
            
            is_preserved = any(
                col.lower() == f.lower() or col.lower().replace('_', '') == f.lower().replace('_', '')
                for f in self.config.preserve_fields
            )
            
            if should_anonymize and not is_preserved:
                anon_df[col] = anon_df[col].apply(lambda x: self.anonymize_value(col, x))
                anonymized_columns.append(col)
        
        logger.info(f"Anonymized {len(anonymized_columns)} columns: {anonymized_columns}")
        
        return anon_df, self.get_mapping_info()
    
    def anonymize_dict(self, data: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict]:
        """
        Anonymize a dictionary (e.g., metrics dict for LLM).
        
        Returns:
            Tuple of (anonymized_dict, mapping_info)
        """
        if self.config.level == AnonymizationLevel.NONE:
            return data.copy(), {}
        
        def anonymize_recursive(obj: Any, parent_key: str = "") -> Any:
            if isinstance(obj, dict):
                return {k: anonymize_recursive(v, k) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [anonymize_recursive(item, parent_key) for item in obj]
            elif isinstance(obj, str):
                return self.anonymize_value(parent_key, obj)
            else:
                return obj
        
        anon_data = anonymize_recursive(data)
        return anon_data, self.get_mapping_info()
    
    def anonymize_text(self, text: str) -> str:
        """
        Anonymize text by replacing known sensitive values with tokens.
        Useful for anonymizing LLM prompts that contain raw text.
        """
        if self.config.level == AnonymizationLevel.NONE:
            return text
        
        anon_text = text
        
        # Replace all known original values with their tokens
        for field_key, mapping in self._mapping.items():
            for original, token in mapping.items():
                # Case-insensitive replacement
                pattern = re.compile(re.escape(original), re.IGNORECASE)
                anon_text = pattern.sub(token, anon_text)
        
        return anon_text
    
    def deanonymize_text(self, text: str) -> str:
        """
        Restore original values in text from tokens.
        Useful for displaying LLM responses to users.
        """
        if self.config.level == AnonymizationLevel.NONE:
            return text
        
        deanon_text = text
        
        # Replace all tokens with original values
        for field_key, mapping in self._reverse_mapping.items():
            for token, original in mapping.items():
                deanon_text = deanon_text.replace(token, original)
        
        return deanon_text
    
    def deanonymize_dict(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Restore original values in a dictionary."""
        if self.config.level == AnonymizationLevel.NONE:
            return data
        
        def deanonymize_recursive(obj: Any) -> Any:
            if isinstance(obj, dict):
                return {k: deanonymize_recursive(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [deanonymize_recursive(item) for item in obj]
            elif isinstance(obj, str):
                # Check if this is a token
                for field_key, mapping in self._reverse_mapping.items():
                    if obj in mapping:
                        return mapping[obj]
                return obj
            else:
                return obj
        
        return deanonymize_recursive(data)
    
    def get_mapping_info(self) -> Dict[str, Any]:
        """Get information about current mappings."""
        return {
            "session_id": self._session_id,
            "level": self.config.level.value,
            "total_mappings": sum(len(m) for m in self._mapping.values()),
            "fields_anonymized": list(self._mapping.keys()),
            "mapping_counts": {k: len(v) for k, v in self._mapping.items()}
        }
    
    def get_full_mapping(self) -> Dict[str, Dict[str, str]]:
        """Get full mapping for debugging/audit purposes."""
        return {
            "forward": self._mapping.copy(),
            "reverse": self._reverse_mapping.copy()
        }
    
    def clear_mappings(self):
        """Clear all mappings (use when starting new session)."""
        self._mapping.clear()
        self._reverse_mapping.clear()
        self._token_counters.clear()
        self._session_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        logger.info("Anonymization mappings cleared")


# Global anonymizer instance for session persistence
_global_anonymizer: Optional[DataAnonymizer] = None


def get_anonymizer(level: AnonymizationLevel = AnonymizationLevel.MODERATE) -> DataAnonymizer:
    """Get or create the global anonymizer instance."""
    global _global_anonymizer
    
    if _global_anonymizer is None or _global_anonymizer.config.level != level:
        _global_anonymizer = DataAnonymizer(AnonymizationConfig(level=level))
    
    return _global_anonymizer


def anonymize_for_llm(data: Dict[str, Any], level: AnonymizationLevel = AnonymizationLevel.MODERATE) -> Tuple[Dict[str, Any], DataAnonymizer]:
    """
    Convenience function to anonymize data before sending to LLM.
    
    Args:
        data: Dictionary of data to anonymize
        level: Anonymization level
        
    Returns:
        Tuple of (anonymized_data, anonymizer_instance)
    """
    anonymizer = get_anonymizer(level)
    anon_data, _ = anonymizer.anonymize_dict(data)
    return anon_data, anonymizer


def deanonymize_llm_response(text: str) -> str:
    """
    Convenience function to restore original values in LLM response.
    
    Args:
        text: LLM response text with tokens
        
    Returns:
        Text with original values restored
    """
    anonymizer = get_anonymizer()
    return anonymizer.deanonymize_text(text)


# Audit logging for compliance
def log_anonymization_event(event_type: str, details: Dict[str, Any]):
    """Log anonymization events for audit trail."""
    logger.info(f"ANONYMIZATION_AUDIT | {event_type} | {json.dumps(details)}")

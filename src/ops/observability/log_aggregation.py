"""
Log Aggregation System
Ship logs to ELK Stack and Splunk for centralized logging
"""

import json
import socket
import requests
from datetime import datetime
from typing import Dict, Any, Optional
from loguru import logger
import os
from pathlib import Path

class ElasticsearchShipper:
    """Ship logs to Elasticsearch (ELK Stack)."""
    
    def __init__(
        self,
        host: str = None,
        port: int = None,
        index_prefix: str = "pca-agent-logs"
    ):
        """
        Initialize Elasticsearch log shipper.
        
        Args:
            host: Elasticsearch host
            port: Elasticsearch port
            index_prefix: Index name prefix
        """
        self.host = host or os.getenv("ELASTICSEARCH_HOST", "localhost")
        self.port = port or int(os.getenv("ELASTICSEARCH_PORT", "9200"))
        self.index_prefix = index_prefix
        self.es_url = f"http://{self.host}:{self.port}"
        
        # Test connection
        self._test_connection()
    
    def _test_connection(self):
        """Test Elasticsearch connection."""
        try:
            response = requests.get(f"{self.es_url}/_cluster/health", timeout=5)
            if response.status_code == 200:
                logger.info(f"✅ Connected to Elasticsearch at {self.es_url}")
            else:
                logger.warning(f"⚠️  Elasticsearch connection issue: {response.status_code}")
        except Exception as e:
            logger.warning(f"⚠️  Could not connect to Elasticsearch: {e}")
    
    def ship_log(self, log_entry: Dict[str, Any]):
        """
        Ship log entry to Elasticsearch.
        
        Args:
            log_entry: Log entry dictionary
        """
        try:
            # Enrich log entry
            enriched_entry = self._enrich_log(log_entry)
            
            # Create index with date
            index = f"{self.index_prefix}-{datetime.utcnow().strftime('%Y.%m.%d')}"
            
            # Send to Elasticsearch
            response = requests.post(
                f"{self.es_url}/{index}/_doc",
                json=enriched_entry,
                headers={"Content-Type": "application/json"},
                timeout=5
            )
            
            if response.status_code not in [200, 201]:
                logger.debug(f"Failed to ship log to Elasticsearch: {response.status_code}")
        
        except Exception as e:
            # Don't fail the application if log shipping fails
            logger.debug(f"Log shipping error: {e}")
    
    def _enrich_log(self, log_entry: Dict[str, Any]) -> Dict[str, Any]:
        """Enrich log entry with additional metadata."""
        return {
            "@timestamp": datetime.utcnow().isoformat(),
            "service": "pca-agent",
            "environment": os.getenv("ENVIRONMENT", "development"),
            "hostname": socket.gethostname(),
            "version": os.getenv("APP_VERSION", "1.0.0"),
            **log_entry
        }
    
    def create_index_template(self):
        """Create Elasticsearch index template for optimal log storage."""
        template = {
            "index_patterns": [f"{self.index_prefix}-*"],
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 1,
                "index.lifecycle.name": "logs-policy",
                "index.lifecycle.rollover_alias": self.index_prefix
            },
            "mappings": {
                "properties": {
                    "@timestamp": {"type": "date"},
                    "level": {"type": "keyword"},
                    "message": {"type": "text"},
                    "service": {"type": "keyword"},
                    "environment": {"type": "keyword"},
                    "hostname": {"type": "keyword"},
                    "user_id": {"type": "keyword"},
                    "request_id": {"type": "keyword"},
                    "correlation_id": {"type": "keyword"},
                    "duration_ms": {"type": "float"},
                    "status_code": {"type": "integer"},
                    "error_type": {"type": "keyword"},
                    "stack_trace": {"type": "text"},
                    "tags": {"type": "keyword"}
                }
            }
        }
        
        try:
            response = requests.put(
                f"{self.es_url}/_index_template/{self.index_prefix}",
                json=template,
                headers={"Content-Type": "application/json"},
                timeout=10
            )
            
            if response.status_code == 200:
                logger.info(f"✅ Created Elasticsearch index template: {self.index_prefix}")
            else:
                logger.warning(f"⚠️  Failed to create index template: {response.status_code}")
        
        except Exception as e:
            logger.error(f"Error creating index template: {e}")
    
    def create_lifecycle_policy(self):
        """Create index lifecycle policy for log retention."""
        policy = {
            "policy": {
                "phases": {
                    "hot": {
                        "actions": {
                            "rollover": {
                                "max_size": "50GB",
                                "max_age": "7d"
                            }
                        }
                    },
                    "warm": {
                        "min_age": "7d",
                        "actions": {
                            "shrink": {
                                "number_of_shards": 1
                            },
                            "forcemerge": {
                                "max_num_segments": 1
                            }
                        }
                    },
                    "cold": {
                        "min_age": "30d",
                        "actions": {
                            "freeze": {}
                        }
                    },
                    "delete": {
                        "min_age": "90d",
                        "actions": {
                            "delete": {}
                        }
                    }
                }
            }
        }
        
        try:
            response = requests.put(
                f"{self.es_url}/_ilm/policy/logs-policy",
                json=policy,
                headers={"Content-Type": "application/json"},
                timeout=10
            )
            
            if response.status_code == 200:
                logger.info("✅ Created index lifecycle policy")
        
        except Exception as e:
            logger.error(f"Error creating lifecycle policy: {e}")


class SplunkShipper:
    """Ship logs to Splunk via HTTP Event Collector (HEC)."""
    
    def __init__(
        self,
        host: str = None,
        port: int = None,
        token: str = None
    ):
        """
        Initialize Splunk log shipper.
        
        Args:
            host: Splunk host
            port: Splunk HEC port
            token: Splunk HEC token
        """
        self.host = host or os.getenv("SPLUNK_HOST")
        self.port = port or int(os.getenv("SPLUNK_PORT", "8088"))
        self.token = token or os.getenv("SPLUNK_HEC_TOKEN")
        
        if self.host and self.token:
            self.splunk_url = f"https://{self.host}:{self.port}/services/collector"
            self._test_connection()
        else:
            logger.warning("⚠️  Splunk credentials not configured")
    
    def _test_connection(self):
        """Test Splunk HEC connection."""
        try:
            # Get SSL verification setting from environment (default: True for production)
            verify_ssl = os.getenv("SPLUNK_VERIFY_SSL", "true").lower() != "false"
            
            response = requests.get(
                f"https://{self.host}:{self.port}/services/collector/health",
                headers={"Authorization": f"Splunk {self.token}"},
                verify=verify_ssl,
                timeout=5
            )
            
            if response.status_code == 200:
                logger.info(f"✅ Connected to Splunk at {self.host}")
            else:
                logger.warning(f"⚠️  Splunk connection issue: {response.status_code}")
        
        except Exception as e:
            logger.warning(f"⚠️  Could not connect to Splunk: {e}")
    
    def ship_log(self, log_entry: Dict[str, Any]):
        """
        Ship log entry to Splunk.
        
        Args:
            log_entry: Log entry dictionary
        """
        if not self.token:
            return
        
        try:
            # Format for Splunk HEC
            event = {
                "time": datetime.utcnow().timestamp(),
                "host": socket.gethostname(),
                "source": "pca-agent",
                "sourcetype": "_json",
                "event": self._enrich_log(log_entry)
            }
            
            # Get SSL verification setting from environment (default: True for production)
            verify_ssl = os.getenv("SPLUNK_VERIFY_SSL", "true").lower() != "false"
            
            # Send to Splunk
            response = requests.post(
                self.splunk_url,
                json=event,
                headers={
                    "Authorization": f"Splunk {self.token}",
                    "Content-Type": "application/json"
                },
                verify=verify_ssl,
                timeout=5
            )
            
            if response.status_code != 200:
                logger.debug(f"Failed to ship log to Splunk: {response.status_code}")
        
        except Exception as e:
            logger.debug(f"Splunk shipping error: {e}")
    
    def _enrich_log(self, log_entry: Dict[str, Any]) -> Dict[str, Any]:
        """Enrich log entry with additional metadata."""
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "service": "pca-agent",
            "environment": os.getenv("ENVIRONMENT", "development"),
            "hostname": socket.gethostname(),
            **log_entry
        }


class LogAggregator:
    """Unified log aggregation system."""
    
    def __init__(self):
        """Initialize log aggregator with all shippers."""
        self.elasticsearch = ElasticsearchShipper()
        self.splunk = SplunkShipper()
        
        # Create index templates and policies
        self.elasticsearch.create_index_template()
        self.elasticsearch.create_lifecycle_policy()
    
    def ship(self, log_entry: Dict[str, Any]):
        """
        Ship log to all configured destinations.
        
        Args:
            log_entry: Log entry dictionary
        """
        # Ship to Elasticsearch
        self.elasticsearch.ship_log(log_entry)
        
        # Ship to Splunk
        self.splunk.ship_log(log_entry)
    
    def ship_structured_log(
        self,
        level: str,
        message: str,
        **kwargs
    ):
        """
        Ship structured log entry.
        
        Args:
            level: Log level (INFO, WARNING, ERROR, etc.)
            message: Log message
            **kwargs: Additional fields
        """
        log_entry = {
            "level": level,
            "message": message,
            **kwargs
        }
        
        self.ship(log_entry)


# Global instance
log_aggregator = LogAggregator()


def setup_log_aggregation():
    """
    Setup log aggregation for Loguru.
    
    This integrates log aggregation with the existing Loguru logger.
    """
    
    def aggregation_sink(message):
        """Custom sink for log aggregation."""
        record = message.record
        
        # Extract log data
        log_entry = {
            "level": record["level"].name,
            "message": record["message"],
            "timestamp": record["time"].isoformat(),
            "file": record["file"].name,
            "function": record["function"],
            "line": record["line"],
        }
        
        # Add extra fields
        if record.get("extra"):
            log_entry.update(record["extra"])
        
        # Add exception info if present
        if record["exception"]:
            log_entry["error_type"] = record["exception"].type.__name__
            log_entry["error_message"] = str(record["exception"].value)
            log_entry["stack_trace"] = record["exception"].traceback
        
        # Ship to aggregators
        log_aggregator.ship(log_entry)
    
    # Add aggregation sink to logger
    logger.add(
        aggregation_sink,
        level="INFO",
        format="{message}",
        serialize=False
    )
    
    logger.info("✅ Log aggregation initialized")


# Initialize on import
setup_log_aggregation()

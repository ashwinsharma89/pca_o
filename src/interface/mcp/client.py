"""
MCP Client for PCA Agent
Provides easy access to MCP resources and tools
"""

import asyncio
from typing import Optional, Dict, Any, List
import pandas as pd
from loguru import logger
from mcp import ClientSession, StdioServerParameters
from mcp.types import Resource
import json


class PCAMCPClient:
    """Client for accessing PCA Agent resources via MCP."""
    
    def __init__(self):
        """Initialize MCP client."""
        self.client: Optional[ClientSession] = None
        self.connected = False
        logger.info("PCA MCP Client initialized")
    
    async def connect(self, server_params: Optional[StdioServerParameters] = None):
        """Connect to MCP server."""
        try:
            # For now, only support local server
            # Remote MCP connections can be added later
            from src.interface.mcp.resource_server import get_mcp_server
            self.server = get_mcp_server()
            self.connected = True
            logger.success("Connected to local MCP server")
                
        except Exception as e:
            logger.error(f"Failed to connect to MCP server: {e}")
            raise
    
    async def list_resources(self) -> List[Resource]:
        """List all available resources."""
        if not self.connected:
            raise RuntimeError("Not connected to MCP server")
        
        try:
            # Local server
            resources = await self.server.server.list_resources()()
            
            logger.info(f"Found {len(resources)} resources")
            return resources
            
        except Exception as e:
            logger.error(f"Failed to list resources: {e}")
            raise
    
    async def read_resource(self, uri: str) -> Dict[str, Any]:
        """Read data from a resource URI."""
        if not self.connected:
            raise RuntimeError("Not connected to MCP server")
        
        try:
            logger.info(f"Reading resource: {uri}")
            
            # Local server
            content = await self.server.server.read_resource()(uri)
            
            # Parse JSON response
            data = json.loads(content)
            
            logger.success(f"Read resource: {uri}")
            return data
            
        except Exception as e:
            logger.error(f"Failed to read resource {uri}: {e}")
            raise
    
    async def read_as_dataframe(self, uri: str, limit: Optional[int] = None) -> pd.DataFrame:
        """Read resource and return as pandas DataFrame."""
        # Add limit to URI if specified
        if limit and "?" not in uri:
            uri = f"{uri}?limit={limit}"
        elif limit:
            uri = f"{uri}&limit={limit}"
        
        data = await self.read_resource(uri)
        
        # Convert to DataFrame
        if "data" in data:
            df = pd.DataFrame(data["data"])
            logger.info(f"Loaded {len(df)} rows from {uri}")
            return df
        else:
            raise ValueError(f"Resource {uri} does not contain data field")
    
    async def query_database(
        self,
        connection_id: str,
        table: str,
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 1000
    ) -> pd.DataFrame:
        """Query database table with optional filters."""
        uri = f"database://{connection_id}/{table}?limit={limit}"
        
        # Add filters to URI (simplified)
        if filters:
            for key, value in filters.items():
                uri += f"&{key}={value}"
        
        return await self.read_as_dataframe(uri)
    
    async def load_from_cloud(
        self,
        source: str,
        path: str,
        **kwargs
    ) -> pd.DataFrame:
        """Load data from cloud storage (S3, Azure, GCS)."""
        if source.lower() == "s3":
            uri = path if path.startswith("s3://") else f"s3://{path}"
        elif source.lower() in ["azure", "azure_blob"]:
            uri = path if path.startswith("azure://") else f"azure://{path}"
        elif source.lower() in ["gcs", "google_cloud_storage"]:
            uri = path if path.startswith("gcs://") else f"gcs://{path}"
        else:
            raise ValueError(f"Unsupported cloud source: {source}")
        
        return await self.read_as_dataframe(uri)
    
    async def get_analytics_metrics(self) -> Dict[str, Any]:
        """Get aggregated campaign metrics."""
        data = await self.read_resource("analytics://metrics")
        return data
    
    async def get_ai_insights(self) -> Dict[str, Any]:
        """Get AI-generated insights."""
        data = await self.read_resource("analytics://insights")
        return data
    
    async def search_knowledge_base(self, query: str, top_k: int = 5) -> List[Dict[str, Any]]:
        """Search RAG knowledge base."""
        uri = f"vector-store://knowledge-base?query={query}&top_k={top_k}"
        data = await self.read_resource(uri)
        return data.get("results", [])
    
    async def disconnect(self):
        """Disconnect from MCP server."""
        self.connected = False
        logger.info("Disconnected from MCP server")


# Convenience functions
async def connect_mcp() -> PCAMCPClient:
    """Quick connect to MCP server."""
    client = PCAMCPClient()
    await client.connect()
    return client


async def read_campaign_data(
    connection_id: str,
    table: str,
    limit: int = 1000
) -> pd.DataFrame:
    """Quick read campaign data from database."""
    client = await connect_mcp()
    try:
        df = await client.query_database(connection_id, table, limit=limit)
        return df
    finally:
        await client.disconnect()

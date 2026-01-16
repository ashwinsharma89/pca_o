"""
MCP Resource Server for PCA Agent
Provides unified access to all data sources via Model Context Protocol
"""

import asyncio
from typing import Optional, Dict, Any, List
import pandas as pd
from loguru import logger
from mcp.server import Server
from mcp.types import Resource, Tool, TextContent
import json

from src.platform.data.database_connector import DatabaseConnector


class PCAResourceServer:
    """MCP Server exposing campaign data resources and analytics tools."""
    
    def __init__(self):
        """Initialize MCP server with data connectors."""
        self.server = Server("pca-agent")
        self.db_connector = DatabaseConnector()
        self.active_connections: Dict[str, DatabaseConnector] = {}
        
        # Register resources and tools
        self._register_resources()
        self._register_tools()
        
        logger.info("PCA MCP Resource Server initialized")
    
    def _register_resources(self):
        """Register all available data resources."""
        
        @self.server.list_resources()
        async def list_resources() -> List[Resource]:
            """List all available campaign data resources."""
            resources = []
            
            # Database resources
            for conn_id, connector in self.active_connections.items():
                try:
                    tables = connector.get_tables()
                    for table in tables:
                        resources.append(Resource(
                            uri=f"database://{conn_id}/{table}",
                            name=f"{conn_id}.{table}",
                            mimeType="application/json",
                            description=f"Campaign data table: {table}"
                        ))
                except Exception as e:
                    logger.warning(f"Failed to list tables for {conn_id}: {e}")
            
            # Vector store resource
            resources.append(Resource(
                uri="vector-store://knowledge-base",
                name="Knowledge Base",
                mimeType="application/json",
                description="RAG knowledge base with campaign insights"
            ))
            
            # Analytics resources
            resources.append(Resource(
                uri="analytics://metrics",
                name="Campaign Metrics",
                mimeType="application/json",
                description="Aggregated campaign performance metrics"
            ))
            
            resources.append(Resource(
                uri="analytics://insights",
                name="AI Insights",
                mimeType="application/json",
                description="AI-generated campaign insights"
            ))
            
            logger.info(f"Listed {len(resources)} resources")
            return resources
        
        @self.server.read_resource()
        async def read_resource(uri: str) -> str:
            """Read data from a resource URI."""
            logger.info(f"Reading resource: {uri}")
            
            try:
                # Parse URI
                if uri.startswith("database://"):
                    return await self._read_database_resource(uri)
                elif uri.startswith("vector-store://"):
                    return await self._read_vector_store(uri)
                elif uri.startswith("analytics://"):
                    return await self._read_analytics(uri)
                elif uri.startswith("s3://"):
                    return await self._read_s3(uri)
                elif uri.startswith("azure://"):
                    return await self._read_azure(uri)
                elif uri.startswith("gcs://"):
                    return await self._read_gcs(uri)
                else:
                    raise ValueError(f"Unsupported URI scheme: {uri}")
                    
            except Exception as e:
                logger.error(f"Failed to read resource {uri}: {e}")
                raise
    
    async def _read_database_resource(self, uri: str) -> str:
        """Read data from database resource."""
        # Parse: database://connection_id/table_name?limit=1000
        parts = uri.replace("database://", "").split("/")
        conn_id = parts[0]
        table_name = parts[1].split("?")[0] if len(parts) > 1 else None
        
        if conn_id not in self.active_connections:
            raise ValueError(f"No active connection: {conn_id}")
        
        connector = self.active_connections[conn_id]
        
        # Parse query parameters
        limit = 1000  # Default limit
        if "?" in uri:
            params = uri.split("?")[1].split("&")
            for param in params:
                if param.startswith("limit="):
                    limit = int(param.split("=")[1])
        
        # Load data
        df = connector.load_table(table_name, limit=limit)
        
        # Convert to JSON
        result = {
            "table": table_name,
            "rows": len(df),
            "columns": df.columns.tolist(),
            "data": df.to_dict(orient="records")
        }
        
        return json.dumps(result, default=str)
    
    async def _read_vector_store(self, uri: str) -> str:
        """Read from vector store (RAG knowledge base)."""
        # This will be implemented in Phase 3
        return json.dumps({
            "message": "Vector store access - Phase 3 implementation",
            "uri": uri
        })
    
    async def _read_analytics(self, uri: str) -> str:
        """Read analytics data."""
        # This will be implemented with tools in Phase 2
        return json.dumps({
            "message": "Analytics access - Phase 2 implementation",
            "uri": uri
        })
    
    async def _read_s3(self, uri: str) -> str:
        """Read from AWS S3."""
        # Parse S3 URI
        s3_path = uri
        
        # Use database connector's S3 method
        df = self.db_connector.load_from_s3(s3_path)
        
        result = {
            "source": "s3",
            "path": s3_path,
            "rows": len(df),
            "columns": df.columns.tolist(),
            "data": df.to_dict(orient="records")
        }
        
        return json.dumps(result, default=str)
    
    async def _read_azure(self, uri: str) -> str:
        """Read from Azure Blob Storage."""
        # Parse: azure://container/blob
        parts = uri.replace("azure://", "").split("/", 1)
        container = parts[0]
        blob = parts[1] if len(parts) > 1 else ""
        
        df = self.db_connector.load_from_azure_blob(
            container_name=container,
            blob_name=blob
        )
        
        result = {
            "source": "azure",
            "container": container,
            "blob": blob,
            "rows": len(df),
            "columns": df.columns.tolist(),
            "data": df.to_dict(orient="records")
        }
        
        return json.dumps(result, default=str)
    
    async def _read_gcs(self, uri: str) -> str:
        """Read from Google Cloud Storage."""
        # Parse: gcs://bucket/blob
        parts = uri.replace("gcs://", "").split("/", 1)
        bucket = parts[0]
        blob = parts[1] if len(parts) > 1 else ""
        
        df = self.db_connector.load_from_gcs(
            bucket_name=bucket,
            blob_name=blob
        )
        
        result = {
            "source": "gcs",
            "bucket": bucket,
            "blob": blob,
            "rows": len(df),
            "columns": df.columns.tolist(),
            "data": df.to_dict(orient="records")
        }
        
        return json.dumps(result, default=str)
    
    def _register_tools(self):
        """Register MCP tools for LLM to use."""
        from src.interface.mcp.tools import PCATools
        
        self.tools = PCATools()
        
        @self.server.list_tools()
        async def list_tools():
            """List all available tools."""
            return self.tools.get_tool_definitions()
        
        @self.server.call_tool()
        async def call_tool(name: str, arguments: dict):
            """Execute a tool."""
            result = await self.tools.execute_tool(name, arguments)
            return [TextContent(
                type="text",
                text=json.dumps(result, default=str)
            )]
        
        logger.info("Registered MCP tools")
    
    async def connect_database(
        self,
        connection_id: str,
        db_type: str,
        **kwargs
    ) -> Dict[str, Any]:
        """Connect to a database and register it as a resource."""
        try:
            connector = DatabaseConnector()
            connector.connect(db_type=db_type, **kwargs)
            
            self.active_connections[connection_id] = connector
            
            # Get available tables
            tables = connector.get_tables()
            
            logger.success(f"Connected to {db_type} as '{connection_id}' with {len(tables)} tables")
            
            return {
                "success": True,
                "connection_id": connection_id,
                "db_type": db_type,
                "tables": tables
            }
            
        except Exception as e:
            logger.error(f"Failed to connect to {db_type}: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    async def disconnect_database(self, connection_id: str):
        """Disconnect from a database."""
        if connection_id in self.active_connections:
            self.active_connections[connection_id].close()
            del self.active_connections[connection_id]
            logger.info(f"Disconnected: {connection_id}")
    
    def get_server(self) -> Server:
        """Get the MCP server instance."""
        return self.server


# Singleton instance
_server_instance: Optional[PCAResourceServer] = None


def get_mcp_server() -> PCAResourceServer:
    """Get or create the MCP server singleton."""
    global _server_instance
    if _server_instance is None:
        _server_instance = PCAResourceServer()
    return _server_instance

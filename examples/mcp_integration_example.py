"""
Example: Using MCP Integration in PCA Agent

Demonstrates:
1. Connecting to MCP server
2. Loading data from multiple sources
3. Using MCP tools
4. Enhanced RAG with live data
"""

import asyncio
import pandas as pd
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from loguru import logger

# Import MCP components
from src.mcp import (
    get_mcp_server,
    connect_mcp,
    create_enhanced_rag,
    PCATools
)


async def example_1_basic_connection():
    """Example 1: Basic MCP connection and resource listing."""
    print("\n" + "="*60)
    print("Example 1: Basic MCP Connection")
    print("="*60)
    
    # Connect to MCP
    client = await connect_mcp()
    
    # List available resources
    resources = await client.list_resources()
    print(f"\n✅ Connected! Found {len(resources)} resources:")
    
    for resource in resources[:5]:  # Show first 5
        print(f"  - {resource.uri}")
        print(f"    Name: {resource.name}")
        print(f"    Type: {resource.mimeType}")
    
    await client.disconnect()
    print("\n✅ Example 1 complete!")


async def example_2_database_access():
    """Example 2: Accessing database via MCP."""
    print("\n" + "="*60)
    print("Example 2: Database Access via MCP")
    print("="*60)
    
    # Get MCP server
    server = get_mcp_server()
    
    # Connect to SQLite database (example)
    result = await server.connect_database(
        connection_id="example_db",
        db_type="sqlite",
        file_path="data/campaigns.db"  # Your database file
    )
    
    if result['success']:
        print(f"\n✅ Connected to database!")
        print(f"   Tables: {result['tables']}")
        
        # Read data via MCP client
        client = await connect_mcp()
        
        try:
            df = await client.query_database(
                connection_id="example_db",
                table=result['tables'][0],  # First table
                limit=10
            )
            
            print(f"\n📊 Loaded {len(df)} rows:")
            print(df.head())
            
        except Exception as e:
            print(f"⚠️ Could not load data: {e}")
        
        await client.disconnect()
    else:
        print(f"❌ Connection failed: {result['error']}")
    
    print("\n✅ Example 2 complete!")


async def example_3_cloud_storage():
    """Example 3: Loading from cloud storage."""
    print("\n" + "="*60)
    print("Example 3: Cloud Storage Access")
    print("="*60)
    
    client = await connect_mcp()
    
    # Example S3 path (replace with your actual path)
    s3_path = "s3://my-bucket/campaigns/data.csv"
    
    print(f"\n📦 Loading from: {s3_path}")
    print("   (This will fail if path doesn't exist - that's expected)")
    
    try:
        df = await client.load_from_cloud(
            source="s3",
            path=s3_path
        )
        
        print(f"✅ Loaded {len(df)} rows from S3")
        print(df.head())
        
    except Exception as e:
        print(f"⚠️ Expected error (demo): {str(e)[:100]}")
    
    await client.disconnect()
    print("\n✅ Example 3 complete!")


async def example_4_mcp_tools():
    """Example 4: Using MCP tools."""
    print("\n" + "="*60)
    print("Example 4: MCP Tools")
    print("="*60)
    
    # Get tools
    tools = PCATools()
    
    # List available tools
    tool_defs = tools.get_tool_definitions()
    print(f"\n🛠️ Available tools: {len(tool_defs)}")
    
    for tool in tool_defs[:3]:  # Show first 3
        print(f"\n  📌 {tool.name}")
        print(f"     {tool.description}")
    
    # Execute a tool
    print("\n🚀 Executing tool: query_campaigns")
    result = await tools.execute_tool(
        "query_campaigns",
        {
            "metric": "roas",
            "period": "30d",
            "platform": "Meta",
            "limit": 10
        }
    )
    
    print(f"   Result: {result}")
    
    print("\n✅ Example 4 complete!")


async def example_5_enhanced_rag():
    """Example 5: Enhanced RAG with live data."""
    print("\n" + "="*60)
    print("Example 5: Enhanced RAG")
    print("="*60)
    
    # Create enhanced RAG
    rag = await create_enhanced_rag()
    
    # Retrieve context from multiple sources
    print("\n🔍 Retrieving context for: 'optimization strategies'")
    
    contexts = await rag.retrieve_context(
        query="optimization strategies for Meta campaigns",
        top_k=5,
        include_live_data=True
    )
    
    print(f"\n✅ Retrieved {len(contexts)} contexts:")
    
    for i, ctx in enumerate(contexts[:3], 1):
        print(f"\n  {i}. Source: {ctx['source']}")
        print(f"     Type: {ctx['type']}")
        print(f"     Score: {ctx['score']:.2f}")
        print(f"     Content: {ctx['content'][:100]}...")
    
    # Generate enhanced summary
    print("\n📝 Generating enhanced summary...")
    
    # Create sample data
    sample_data = pd.DataFrame({
        'Campaign': ['Campaign A', 'Campaign B', 'Campaign C'],
        'Platform': ['Meta', 'Google', 'TikTok'],
        'Spend': [5000, 8000, 3000],
        'Conversions': [250, 320, 180],
        'ROAS': [2.5, 2.8, 3.2]
    })
    
    summary = await rag.generate_enhanced_summary(
        query="Analyze campaign performance and provide recommendations",
        campaign_data=sample_data,
        top_k=5
    )
    
    print(f"\n✅ Summary generated!")
    print(f"   Sources used:")
    print(f"   - Knowledge base: {summary['sources']['knowledge_base']}")
    print(f"   - Live data: {summary['sources']['live_data']}")
    print(f"   - Total contexts: {summary['sources']['total_contexts']}")
    
    await rag.disconnect()
    print("\n✅ Example 5 complete!")


async def example_6_recommendations():
    """Example 6: Contextual recommendations."""
    print("\n" + "="*60)
    print("Example 6: Contextual Recommendations")
    print("="*60)
    
    # Create enhanced RAG
    rag = await create_enhanced_rag()
    
    # Sample campaign data
    campaign_data = pd.DataFrame({
        'Campaign': ['Low ROAS Campaign', 'High ROAS Campaign', 'Medium Campaign'],
        'Platform': ['Meta', 'Google', 'TikTok'],
        'Spend': [10000, 5000, 7000],
        'Conversions': [100, 150, 140],
        'ROAS': [1.2, 4.5, 2.8]
    })
    
    print("\n📊 Campaign Data:")
    print(campaign_data)
    
    # Get recommendations
    print("\n💡 Getting contextual recommendations...")
    
    recommendations = await rag.get_contextual_recommendations(
        campaign_data=campaign_data,
        focus_area="all"
    )
    
    print(f"\n✅ Generated {len(recommendations)} recommendations:")
    
    for rec in recommendations:
        print(f"\n  🎯 {rec['title']}")
        print(f"     Priority: {rec['priority']}")
        print(f"     {rec['description']}")
        print(f"     Action: {rec['action']}")
    
    await rag.disconnect()
    print("\n✅ Example 6 complete!")


async def run_all_examples():
    """Run all examples."""
    print("\n" + "="*60)
    print("🚀 MCP Integration Examples")
    print("="*60)
    
    try:
        await example_1_basic_connection()
        await example_2_database_access()
        await example_3_cloud_storage()
        await example_4_mcp_tools()
        await example_5_enhanced_rag()
        await example_6_recommendations()
        
        print("\n" + "="*60)
        print("✅ All examples completed successfully!")
        print("="*60)
        
    except Exception as e:
        logger.error(f"Example failed: {e}")
        print(f"\n❌ Error: {e}")


if __name__ == "__main__":
    # Run examples
    asyncio.run(run_all_examples())

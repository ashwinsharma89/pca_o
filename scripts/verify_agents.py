
import sys
import os
import logging
import asyncio

# Add project root to path
sys.path.append(os.getcwd())

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def verify_imports():
    logger.info("Verifying Agent Imports...")
    
    try:
        logger.info("1. Importing VisionAgent...")
        from src.engine.agents import VisionAgent
        logger.info("   -> VisionAgent imported successfully (or None if optional deps missing)")
    except ImportError as e:
        logger.error(f"   -> Failed to import VisionAgent: {e}")
    except Exception as e:
        logger.error(f"   -> Error importing VisionAgent: {e}")

    try:
        logger.info("2. Importing ExtractionAgent...")
        from src.engine.agents import ExtractionAgent
        logger.info("   -> ExtractionAgent imported successfully")
    except Exception as e:
        logger.error(f"   -> Failed to import ExtractionAgent: {e}")

    try:
        logger.info("3. Importing ReasoningAgent...")
        from src.engine.agents import ReasoningAgent
        logger.info("   -> ReasoningAgent imported successfully")
    except Exception as e:
        logger.error(f"   -> Failed to import ReasoningAgent: {e}")

    try:
        logger.info("4. Importing VisualizationAgent...")
        from src.engine.agents import VisualizationAgent
        logger.info("   -> VisualizationAgent imported successfully")
    except Exception as e:
        logger.error(f"   -> Failed to import VisualizationAgent: {e}")

    try:
        logger.info("5. Importing EnhancedReasoningAgent...")
        from src.engine.agents import EnhancedReasoningAgent
        logger.info("   -> EnhancedReasoningAgent imported successfully")
    except Exception as e:
        logger.error(f"   -> Failed to import EnhancedReasoningAgent: {e}")

    try:
        logger.info("6. Importing B2BSpecialistAgent...")
        from src.engine.agents import B2BSpecialistAgent
        logger.info("   -> B2BSpecialistAgent imported successfully")
    except Exception as e:
        logger.error(f"   -> Failed to import B2BSpecialistAgent: {e}")

    try:
        logger.info("7. Importing MediaAnalyticsExpert (from analytics)...")
        from src.engine.analytics.auto_insights import MediaAnalyticsExpert
        logger.info("   -> MediaAnalyticsExpert imported successfully")
    except Exception as e:
        logger.error(f"   -> Failed to import MediaAnalyticsExpert: {e}")
        
    try:
        logger.info("8. Importing PCAWorkflow (Orchestration)...")
        from src.engine.orchestration.workflow import PCAWorkflow
        logger.info("   -> PCAWorkflow imported successfully")
    except Exception as e:
        logger.error(f"   -> Failed to import PCAWorkflow: {e}")

    logger.info("Verification Complete.")

if __name__ == "__main__":
    asyncio.run(verify_imports())

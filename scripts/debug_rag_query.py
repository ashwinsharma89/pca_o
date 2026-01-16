
import sys
import os
import asyncio
from dotenv import load_dotenv

sys.path.append(os.getcwd())
load_dotenv()

from src.kg_rag.query.query_router import QueryRouter
from src.kg_rag.query.result_formatter import ResultFormatter

async def main():
    print("Initializing QueryRouter...")
    try:
        router = QueryRouter()
        
        query_text = "Meta ads performance"
        print(f"Routing query: {query_text}")
        
        context = {"limit": 20}
        
        result = router.route(query_text, context)
        print("Raw Result:", result)
        
        formatter = ResultFormatter()
        formatted = formatter.format(result.get("results", []))
        print("Formatted Result:", formatted.to_dict())
        
    except Exception as e:
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())

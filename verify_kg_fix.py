import sys
from pathlib import Path

# Add project root to python path
project_root = "/Users/ashwin/Desktop/pca_agent copy"
sys.path.append(project_root)

from dotenv import load_dotenv
load_dotenv(dotenv_path=Path(project_root) / ".env")

from src.kg_rag.query.query_router import QueryRouter

def test_fix():
    router = QueryRouter()
    
    # Case 1: Performance-first (Default)
    query1 = "Top 5 campaigns for social channel"
    print(f"\n--- Testing Query 1: {query1} ---")
    result1 = router.route(query1)
    print(f"Intent: {result1.get('intent')}")
    print(f"Routing: {result1.get('routing')}")
    print(f"Cypher:\n{result1.get('cypher')}")
    
    if "ORDER BY roas DESC, cpa ASC, ctr DESC" in result1.get('cypher', ''):
        print("✅ SUCCESS: Performance-first ordering found.")
    else:
        print("❌ FAILURE: Incorrect ordering for default 'top'.")
        
    # Case 2: Explicit 'by spend'
    query2 = "Top 5 campaigns by spend for social channel"
    print(f"\n--- Testing Query 2: {query2} ---")
    result2 = router.route(query2)
    print(f"Intent: {result2.get('intent')}")
    print(f"Routing: {result2.get('routing')}")
    print(f"Cypher:\n{result2.get('cypher')}")
    
    # Case 3: Top without numbers + Channel Normalization
    query3 = "top social campaigns by roas"
    print(f"\n--- Testing Query 3: {query3} ---")
    result3 = router.route(query3)
    print(f"Intent: {result3.get('intent')}")
    print(f"Routing: {result3.get('routing')}")
    print(f"Cypher:\n{result3.get('cypher')}")
    
    if result3.get('routing') == 'template':
        if "toLower(m.channel) = toLower($channel)" in result3.get('cypher', ''):
             print("✅ SUCCESS: Routed to template with channel filter.")
        else:
             print("❌ FAILURE: Routed to template but filter missing.")
             
        results = result3.get('results', [])
        if results and any(r.get('roas', 0) > 0 for r in results):
            print(f"✅ SUCCESS: Non-zero ROAS found in results.")
            for r in results[:2]: print(f"  - {r}")
        else:
            print("❌ FAILURE: ROAS is still zero or results empty.")
    else:
        print("❌ FAILURE: Did not route to template.")
    # Case 4: Year-over-Year (YoY) Seasonal
    query4 = "compare July performance for each year"
    print(f"\n--- Testing Query 4: {query4} ---")
    result4 = router.route(query4)
    print(f"Intent: {result4.get('intent')}")
    print(f"Routing: {result4.get('routing')}")
    print(f"Cypher:\n{result4.get('cypher')}")
    
    if result4.get('routing') == 'template' and "month_num" in result4.get('cypher', ''):
        results = result4.get('results', [])
        print(f"✅ SUCCESS: Routed to seasonal template. Results: {len(results)}")
        for r in results:
            print(f"  - Year {r.get('year')} {r.get('period')}: Spend {r.get('spend')}")
    else:
        print("❌ FAILURE: Incorrect routing or Cypher for YoY seasonal.")

if __name__ == "__main__":
    test_fix()

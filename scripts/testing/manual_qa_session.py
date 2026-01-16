"""
Interactive Manual Q&A Session
Test the system with your own questions
"""
import pandas as pd
import os
from dotenv import load_dotenv
from src.query_engine import NaturalLanguageQueryEngine
from loguru import logger
import sys

# Load environment
load_dotenv()

# Configure logger
logger.remove()
logger.add(sys.stdout, format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>")

def print_banner():
    """Print welcome banner."""
    print("\n" + "="*80)
    print("🤖 PCA AGENT - INTERACTIVE Q&A SESSION")
    print("="*80)
    print("\n✨ Ask strategic questions about your campaign data!")
    print("\n📚 Example Questions:")
    print("  • What's the underlying story behind our performance?")
    print("  • Identify top 20% of campaigns driving 80% of results")
    print("  • How should we reallocate budget to maximize conversions?")
    print("  • Which campaigns show declining trends?")
    print("  • What are the top 3 optimization opportunities?")
    print("\n💡 Tips:")
    print("  • Type 'examples' to see more question templates")
    print("  • Type 'quit' or 'exit' to end session")
    print("  • Type 'clear' to clear screen")
    print("="*80 + "\n")

def show_examples():
    """Show example questions by category."""
    examples = {
        "📊 Basic Analysis": [
            "What is the total spend by channel?",
            "Which campaigns have the highest ROAS?",
            "Show me top 10 campaigns by conversions",
        ],
        "📈 Temporal Comparisons": [
            "Compare last week vs previous week performance",
            "Show me the trend for CPA over the last 2 months",
            "How did our CTR change month-over-month?",
        ],
        "🎯 Strategic Insights": [
            "What's the underlying story behind our performance?",
            "Why did our CPA increase? Conduct a root cause analysis",
            "What hidden patterns exist in top-performing campaigns?",
            "What are the key drivers of campaign success?",
        ],
        "💡 Recommendations": [
            "How should we reallocate budget to maximize conversions?",
            "Create a 30-day optimization roadmap",
            "Recommend which campaigns to scale or pause",
            "What specific actions should we take to improve performance?",
        ],
        "🔍 Advanced Analysis": [
            "Identify performance anomalies using statistical outliers",
            "Calculate performance volatility for each campaign",
            "Identify top 20% of campaigns driving 80% of results",
            "Which campaigns show declining trends?",
        ],
    }
    
    print("\n" + "="*80)
    print("📚 EXAMPLE QUESTIONS BY CATEGORY")
    print("="*80 + "\n")
    
    for category, questions in examples.items():
        print(f"{category}:")
        for q in questions:
            print(f"  • {q}")
        print()

def format_results(result):
    """Format query results for display."""
    if not result['success']:
        logger.error(f"❌ Error: {result['error']}")
        return
    
    # Answer
    print("\n" + "="*80)
    print("📝 ANSWER")
    print("="*80)
    print(result['answer'])
    
    # SQL Query
    print("\n" + "-"*80)
    print("🔧 Generated SQL Query")
    print("-"*80)
    print(result['sql_query'])
    
    # Results table
    if len(result['results']) > 0:
        print("\n" + "-"*80)
        print(f"📊 Results ({len(result['results'])} rows)")
        print("-"*80)
        
        # Show first 10 rows
        display_df = result['results'].head(10)
        print(display_df.to_string(index=False))
        
        if len(result['results']) > 10:
            print(f"\n... and {len(result['results']) - 10} more rows")
    else:
        print("\n⚠️ No results returned (this might be due to date filtering on historical data)")
    
    print("\n" + "="*80 + "\n")

def main():
    """Run interactive Q&A session."""
    print_banner()
    
    # Check API key
    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key or api_key == 'your_openai_api_key_here':
        logger.error("❌ ERROR: OpenAI API key not found!")
        logger.info("Please set OPENAI_API_KEY in .env file")
        return
    
    # Load data
    logger.info("Loading campaign data...")
    data_path = "data/sitevisit_fixed.csv"
    
    if not os.path.exists(data_path):
        logger.error(f"❌ Data file not found: {data_path}")
        logger.info("Please run fix_and_test.py first to create the fixed data file")
        return
    
    df = pd.read_csv(data_path)
    logger.success(f"✓ Loaded {len(df)} rows with {len(df.columns)} columns")
    
    # Initialize Q&A engine
    logger.info("🤖 Initializing Q&A Engine...")
    engine = NaturalLanguageQueryEngine(api_key)
    engine.load_data(df)
    logger.success("✓ Engine ready! You can now ask questions.\n")
    
    # Interactive loop
    question_count = 0
    
    while True:
        try:
            # Get question
            question = input("❓ Your question (or 'examples'/'quit'): ").strip()
            
            if not question:
                continue
            
            # Handle commands
            if question.lower() in ['quit', 'exit', 'q']:
                logger.info("👋 Goodbye!")
                break
            
            if question.lower() == 'examples':
                show_examples()
                continue
            
            if question.lower() == 'clear':
                os.system('cls' if os.name == 'nt' else 'clear')
                print_banner()
                continue
            
            # Process question
            question_count += 1
            logger.info(f"\n[Question #{question_count}] Processing...")
            
            result = engine.ask(question)
            format_results(result)
            
        except KeyboardInterrupt:
            logger.info("\n\n👋 Session interrupted. Goodbye!")
            break
        except Exception as e:
            logger.error(f"❌ Error: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    # Summary
    if question_count > 0:
        logger.info(f"\n📊 Session Summary: {question_count} questions asked")

if __name__ == "__main__":
    main()

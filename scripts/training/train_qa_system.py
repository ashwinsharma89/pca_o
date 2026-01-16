"""
Training and Testing Script for Q&A System
Tests the natural language to SQL conversion with 20 sample questions
"""
import json
import pandas as pd
from src.query_engine import NaturalLanguageQueryEngine
from loguru import logger
import os
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()

def load_training_questions():
    """Load training questions from JSON file."""
    with open('data/training_questions.json', 'r') as f:
        data = json.load(f)
    return data['training_questions']

def load_sample_data():
    """Load sample campaign data."""
    return pd.read_csv('data/sample_multi_campaign_database.csv')

def test_question(engine, question_data, test_number):
    """
    Test a single question.
    
    Args:
        engine: NaturalLanguageQueryEngine instance
        question_data: Dictionary with question details
        test_number: Test number for display
    """
    print(f"\n{'='*80}")
    print(f"TEST #{test_number}: {question_data['category']}")
    print(f"{'='*80}")
    print(f"Question: {question_data['question']}")
    print(f"Difficulty: {question_data['difficulty']}")
    print(f"\nExpected SQL Pattern:")
    print(f"  {question_data['expected_sql'][:100]}...")
    
    try:
        # Generate SQL from natural language
        result = engine.ask(question_data['question'])
        
        print(f"\n✅ Generated SQL:")
        print(f"  {result['sql_query']}")
        
        print(f"\n📊 Results:")
        print(result['results'].to_string(index=False))
        
        print(f"\n⏱️  Execution Time: {result['execution_time']:.3f}s")
        
        return {
            'test_number': test_number,
            'question_id': question_data['id'],
            'category': question_data['category'],
            'question': question_data['question'],
            'status': 'PASS',
            'generated_sql': result['sql_query'],
            'execution_time': result['execution_time'],
            'row_count': len(result['results']),
            'error': None
        }
        
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        logger.error(f"Error in test #{test_number}: {e}")
        
        return {
            'test_number': test_number,
            'question_id': question_data['id'],
            'category': question_data['category'],
            'question': question_data['question'],
            'status': 'FAIL',
            'generated_sql': None,
            'execution_time': None,
            'row_count': None,
            'error': str(e)
        }

def run_training_tests():
    """Run all training tests."""
    print("\n" + "="*80)
    print("🚀 PCA AGENT Q&A SYSTEM - TRAINING & TESTING")
    print("="*80)
    
    # Check API key
    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key or api_key == 'your_openai_api_key_here':
        print("\n❌ ERROR: OpenAI API key not found!")
        print("Please set OPENAI_API_KEY in .env file")
        return
    
    # Load data
    print("\n📂 Loading sample data...")
    df = load_sample_data()
    print(f"✅ Loaded {len(df)} rows with {len(df.columns)} columns")
    
    # Load questions
    print("\n📋 Loading training questions...")
    questions = load_training_questions()
    print(f"✅ Loaded {len(questions)} test questions")
    
    # Initialize engine
    print("\n🤖 Initializing Q&A Engine...")
    engine = NaturalLanguageQueryEngine(api_key)
    engine.load_data(df)
    print("✅ Engine ready!")
    
    # Run tests
    print("\n" + "="*80)
    print("RUNNING TESTS")
    print("="*80)
    
    results = []
    for i, question in enumerate(questions, 1):
        result = test_question(engine, question, i)
        results.append(result)
    
    # Summary
    print("\n" + "="*80)
    print("📊 TEST SUMMARY")
    print("="*80)
    
    passed = sum(1 for r in results if r['status'] == 'PASS')
    failed = sum(1 for r in results if r['status'] == 'FAIL')
    total = len(results)
    pass_rate = (passed / total) * 100
    
    print(f"\nTotal Tests: {total}")
    print(f"✅ Passed: {passed}")
    print(f"❌ Failed: {failed}")
    print(f"📈 Pass Rate: {pass_rate:.1f}%")
    
    if failed > 0:
        print(f"\n❌ Failed Tests:")
        for r in results:
            if r['status'] == 'FAIL':
                print(f"  - Test #{r['test_number']}: {r['question']}")
                print(f"    Error: {r['error']}")
    
    # Category breakdown
    print(f"\n📊 Results by Category:")
    categories = {}
    for r in results:
        cat = r['category']
        if cat not in categories:
            categories[cat] = {'pass': 0, 'fail': 0}
        if r['status'] == 'PASS':
            categories[cat]['pass'] += 1
        else:
            categories[cat]['fail'] += 1
    
    for cat, stats in sorted(categories.items()):
        total_cat = stats['pass'] + stats['fail']
        rate = (stats['pass'] / total_cat) * 100
        print(f"  {cat}: {stats['pass']}/{total_cat} ({rate:.0f}%)")
    
    # Save results
    results_df = pd.DataFrame(results)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = f'data/test_results_{timestamp}.csv'
    results_df.to_csv(output_file, index=False)
    print(f"\n💾 Results saved to: {output_file}")
    
    print("\n" + "="*80)
    print("✅ TRAINING COMPLETE!")
    print("="*80)
    
    return results

def run_interactive_mode():
    """Run interactive Q&A mode for manual testing."""
    print("\n" + "="*80)
    print("🎯 INTERACTIVE Q&A MODE")
    print("="*80)
    print("\nType your questions or 'quit' to exit")
    
    # Check API key
    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key or api_key == 'your_openai_api_key_here':
        print("\n❌ ERROR: OpenAI API key not found!")
        return
    
    # Load data
    df = load_sample_data()
    engine = NaturalLanguageQueryEngine(api_key)
    engine.load_data(df)
    print("✅ Engine ready!\n")
    
    while True:
        question = input("\n❓ Your question: ").strip()
        
        if question.lower() in ['quit', 'exit', 'q']:
            print("\n👋 Goodbye!")
            break
        
        if not question:
            continue
        
        try:
            result = engine.ask(question)
            print(f"\n📝 Generated SQL:")
            print(f"  {result['sql_query']}")
            print(f"\n📊 Results:")
            print(result['results'].to_string(index=False))
            print(f"\n⏱️  Time: {result['execution_time']:.3f}s")
        except Exception as e:
            print(f"\n❌ Error: {str(e)}")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == 'interactive':
        run_interactive_mode()
    else:
        run_training_tests()

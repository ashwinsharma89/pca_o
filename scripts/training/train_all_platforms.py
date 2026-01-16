"""
Comprehensive Training Script for All Platform Datasets
Tests Q&A system across Meta Ads, Google Ads, Snapchat, CM360, DV360, LinkedIn, Trade Desk
"""
import pandas as pd
import json
import os
import time
from datetime import datetime
from dotenv import load_dotenv
from src.query_engine.nl_to_sql import NaturalLanguageQueryEngine

# Load environment variables
load_dotenv()

# Platform configurations
PLATFORMS = {
    'Meta Ads': {
        'dataset': 'data/meta_ads_dataset.csv',
        'questions': 'data/meta_ads_training_questions.json',
        'table_name': 'meta_campaigns'
    },
    'Google Ads': {
        'dataset': 'data/google_ads_dataset.csv',
        'questions': 'data/google_ads_training_questions.json',
        'table_name': 'google_campaigns'
    },
    'LinkedIn Ads': {
        'dataset': 'data/linkedin_ads_dataset.csv',
        'questions': 'data/linkedin_ads_training_questions.json',
        'table_name': 'linkedin_campaigns'
    },
    'Snapchat Ads': {
        'dataset': 'data/snapchat_ads_dataset.csv',
        'questions': 'data/snapchat_ads_training_questions.json',
        'table_name': 'snapchat_campaigns'
    },
    'CM360': {
        'dataset': 'data/cm360_dataset.csv',
        'questions': 'data/cm360_training_questions.json',
        'table_name': 'cm360_campaigns'
    },
    'DV360': {
        'dataset': 'data/dv360_dataset.csv',
        'questions': 'data/dv360_training_questions.json',
        'table_name': 'dv360_campaigns'
    },
    'The Trade Desk': {
        'dataset': 'data/tradedesk_dataset.csv',
        'questions': 'data/tradedesk_training_questions.json',
        'table_name': 'tradedesk_campaigns'
    }
}

def load_questions(file_path):
    """Load training questions from JSON file"""
    if not os.path.exists(file_path):
        print(f"⚠️  Questions file not found: {file_path}")
        return []
    
    with open(file_path, 'r') as f:
        return json.load(f)

def test_platform(platform_name, config, engine):
    """Test Q&A system for a specific platform"""
    print(f"\n{'='*80}")
    print(f"🎯 Testing: {platform_name}")
    print(f"{'='*80}\n")
    
    # Load dataset
    print(f"📊 Loading dataset: {config['dataset']}")
    df = pd.read_csv(config['dataset'])
    print(f"   ✅ Loaded {len(df):,} rows, {len(df.columns)} columns")
    
    # Load into query engine
    engine.load_data(df, table_name=config['table_name'])
    
    # Load questions
    questions = load_questions(config['questions'])
    if not questions:
        print(f"   ⚠️  No questions found, skipping...")
        return None
    
    print(f"   ✅ Loaded {len(questions)} training questions\n")
    
    # Test questions
    results = []
    categories = {}
    
    for i, q in enumerate(questions, 1):
        question_text = q['question']
        category = q.get('category', 'unknown')
        difficulty = q.get('difficulty', 'unknown')
        
        print(f"[{i}/{len(questions)}] Testing: {question_text}")
        
        try:
            start_time = time.time()
            result = engine.ask(question_text)
            execution_time = time.time() - start_time
            
            success = result.get('success', False)
            status = "✅ PASS" if success else "❌ FAIL"
            
            print(f"   {status} | {execution_time:.2f}s | Category: {category}")
            
            if not success:
                print(f"   Error: {result.get('error', 'Unknown error')}")
            
            # Track results
            results.append({
                'question_id': q['id'],
                'question': question_text,
                'category': category,
                'difficulty': difficulty,
                'success': success,
                'execution_time': execution_time,
                'sql_generated': result.get('sql_query', ''),
                'error': result.get('error', '')
            })
            
            # Track by category
            if category not in categories:
                categories[category] = {'total': 0, 'passed': 0}
            categories[category]['total'] += 1
            if success:
                categories[category]['passed'] += 1
                
        except Exception as e:
            print(f"   ❌ EXCEPTION: {str(e)}")
            results.append({
                'question_id': q['id'],
                'question': question_text,
                'category': category,
                'difficulty': difficulty,
                'success': False,
                'execution_time': 0,
                'sql_generated': '',
                'error': str(e)
            })
    
    # Calculate statistics
    total_questions = len(results)
    passed = sum(1 for r in results if r['success'])
    failed = total_questions - passed
    pass_rate = (passed / total_questions * 100) if total_questions > 0 else 0
    avg_time = sum(r['execution_time'] for r in results) / total_questions if total_questions > 0 else 0
    
    # Print summary
    print(f"\n{'='*80}")
    print(f"📊 {platform_name} - Test Summary")
    print(f"{'='*80}")
    print(f"Total Questions: {total_questions}")
    print(f"✅ Passed: {passed} ({pass_rate:.1f}%)")
    print(f"❌ Failed: {failed} ({100-pass_rate:.1f}%)")
    print(f"⏱️  Avg Execution Time: {avg_time:.2f}s")
    
    # Category breakdown
    print(f"\n📂 Performance by Category:")
    for cat, stats in sorted(categories.items()):
        cat_pass_rate = (stats['passed'] / stats['total'] * 100) if stats['total'] > 0 else 0
        print(f"   {cat:30s}: {stats['passed']}/{stats['total']} ({cat_pass_rate:.1f}%)")
    
    # Save results
    results_df = pd.DataFrame(results)
    output_file = f"training_results_{platform_name.replace(' ', '_').lower()}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    results_df.to_csv(output_file, index=False)
    print(f"\n💾 Results saved to: {output_file}")
    
    return {
        'platform': platform_name,
        'total': total_questions,
        'passed': passed,
        'failed': failed,
        'pass_rate': pass_rate,
        'avg_time': avg_time,
        'categories': categories,
        'results_file': output_file
    }

def main():
    """Main training function"""
    print("="*80)
    print("🚀 MULTI-PLATFORM Q&A TRAINING SYSTEM")
    print("="*80)
    print(f"Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Platforms to test: {len(PLATFORMS)}")
    print("="*80)
    
    # Initialize query engine
    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        print("❌ ERROR: OPENAI_API_KEY not found in environment variables")
        print("Please set your API key in the .env file")
        return
    
    engine = NaturalLanguageQueryEngine(api_key)
    
    # Test each platform
    all_results = []
    for platform_name, config in PLATFORMS.items():
        result = test_platform(platform_name, config, engine)
        if result:
            all_results.append(result)
    
    # Overall summary
    print(f"\n{'='*80}")
    print("🎉 OVERALL TRAINING SUMMARY")
    print(f"{'='*80}")
    
    total_questions = sum(r['total'] for r in all_results)
    total_passed = sum(r['passed'] for r in all_results)
    overall_pass_rate = (total_passed / total_questions * 100) if total_questions > 0 else 0
    
    print(f"\n📊 Aggregate Statistics:")
    print(f"   Total Platforms Tested: {len(all_results)}")
    print(f"   Total Questions: {total_questions}")
    print(f"   Total Passed: {total_passed} ({overall_pass_rate:.1f}%)")
    print(f"   Total Failed: {total_questions - total_passed}")
    
    print(f"\n📈 Platform Breakdown:")
    for result in all_results:
        print(f"   {result['platform']:20s}: {result['passed']}/{result['total']} ({result['pass_rate']:.1f}%) | Avg: {result['avg_time']:.2f}s")
    
    print(f"\n{'='*80}")
    print(f"End Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)

if __name__ == "__main__":
    main()

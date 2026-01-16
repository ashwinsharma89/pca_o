"""
Fix Metric Calculations in Training Questions
Replaces AVG() of calculated metrics with proper recalculation from base metrics
"""
import json
import re
from pathlib import Path

# Metric correction patterns
CORRECTIONS = {
    # CTR corrections
    r'AVG\(CTR\)': '(SUM(Clicks) * 100.0 / SUM(Impressions))',
    r'AVG\(ctr\)': '(SUM(Clicks) * 100.0 / SUM(Impressions))',
    
    # CPC corrections
    r'AVG\(CPC\)': '(SUM(Spend) / SUM(Clicks))',
    r'AVG\(Avg_CPC\)': '(SUM(Cost) / SUM(Clicks))',
    r'AVG\(cpc\)': '(SUM(Spend) / SUM(Clicks))',
    
    # CPM corrections
    r'AVG\(CPM\)': '(SUM(Spend) / SUM(Impressions) * 1000)',
    r'AVG\(cpm\)': '(SUM(Spend) / SUM(Impressions) * 1000)',
    r'AVG\(eCPM\)': '(SUM(Spend) / SUM(Impressions) * 1000)',
    
    # ROAS corrections
    r'AVG\(ROAS\)': '(SUM(Revenue) / SUM(Spend))',
    r'AVG\(roas\)': '(SUM(Revenue) / SUM(Spend))',
    
    # Conversion Rate corrections
    r'AVG\(Conv_Rate\)': '(SUM(Conversions) * 100.0 / SUM(Clicks))',
    r'AVG\(Conversion_Rate\)': '(SUM(Conversions) * 100.0 / SUM(Clicks))',
    r'AVG\(conv_rate\)': '(SUM(Conversions) * 100.0 / SUM(Clicks))',
    
    # CPA corrections
    r'AVG\(Cost_Per_Conv\)': '(SUM(Cost) / SUM(Conversions))',
    r'AVG\(cost_per_conv\)': '(SUM(Cost) / SUM(Conversions))',
    r'AVG\(CPA\)': '(SUM(Spend) / SUM(Conversions))',
    
    # AOV corrections
    r'AVG\(AOV\)': '(SUM(Revenue) / SUM(Conversions))',
    r'AVG\(aov\)': '(SUM(Revenue) / SUM(Conversions))',
    
    # Engagement Rate corrections
    r'AVG\(Engagement_Rate\)': '(SUM(Engagement) * 100.0 / SUM(Impressions))',
    r'AVG\(engagement_rate\)': '(SUM(Engagement) * 100.0 / SUM(Impressions))',
    
    # Video View Rate corrections
    r'AVG\(Video_View_Rate\)': '(SUM(Video_Views) * 100.0 / SUM(Impressions))',
    
    # Swipe Rate corrections (Snapchat)
    r'AVG\(Swipe_Up_Rate\)': '(SUM(Swipes) * 100.0 / SUM(Impressions))',
    r'AVG\(swipe_rate\)': '(SUM(Swipes) * 100.0 / SUM(Impressions))',
    
    # Frequency corrections
    r'AVG\(Frequency\)': '(SUM(Impressions) * 1.0 / SUM(Reach))',
    r'AVG\(frequency\)': '(SUM(Impressions) * 1.0 / SUM(Reach))',
    
    # Cost per Lead (LinkedIn)
    r'AVG\(Cost_Per_Lead\)': '(SUM(Spend) / SUM(Leads))',
    
    # Lead Form Completion Rate
    r'AVG\(Lead_Form_Completion_Rate\)': '(SUM(Leads) * 100.0 / SUM(Lead_Form_Opens))',
    
    # Impression Share (keep as AVG - it's already a percentage)
    # Quality Score (keep as AVG - it's a discrete rating)
}

def fix_sql_query(sql):
    """Apply all corrections to a SQL query"""
    original = sql
    for pattern, replacement in CORRECTIONS.items():
        sql = re.sub(pattern, replacement, sql, flags=re.IGNORECASE)
    
    return sql, sql != original

def process_training_file(file_path):
    """Process a training questions JSON file"""
    print(f"\n📄 Processing: {file_path.name}")
    
    with open(file_path, 'r') as f:
        questions = json.load(f)
    
    fixed_count = 0
    for question in questions:
        if 'expected_sql' in question:
            fixed_sql, was_fixed = fix_sql_query(question['expected_sql'])
            if was_fixed:
                print(f"   ✅ Fixed Q{question['id']}: {question['question'][:60]}...")
                print(f"      Before: {question['expected_sql'][:80]}...")
                print(f"      After:  {fixed_sql[:80]}...")
                question['expected_sql'] = fixed_sql
                fixed_count += 1
    
    if fixed_count > 0:
        # Save updated file
        with open(file_path, 'w') as f:
            json.dump(questions, f, indent=2)
        print(f"   💾 Saved {fixed_count} corrections to {file_path.name}")
    else:
        print(f"   ℹ️  No corrections needed")
    
    return fixed_count

def main():
    """Main function to process all training files"""
    print("="*80)
    print("🔧 FIXING METRIC CALCULATIONS IN TRAINING QUESTIONS")
    print("="*80)
    print("\nSearching for training question files...")
    
    data_dir = Path('data')
    training_files = list(data_dir.glob('*_training_questions.json'))
    
    if not training_files:
        print("❌ No training question files found in data/ directory")
        return
    
    print(f"Found {len(training_files)} training files:\n")
    for f in training_files:
        print(f"   - {f.name}")
    
    total_fixed = 0
    for file_path in training_files:
        fixed = process_training_file(file_path)
        total_fixed += fixed
    
    print("\n" + "="*80)
    print("📊 SUMMARY")
    print("="*80)
    print(f"Files processed: {len(training_files)}")
    print(f"Total corrections: {total_fixed}")
    print("\n✅ All training questions have been updated with correct metric calculations!")
    print("\nKey changes:")
    print("   - AVG(CTR) → (SUM(Clicks) * 100.0 / SUM(Impressions))")
    print("   - AVG(CPC) → (SUM(Spend) / SUM(Clicks))")
    print("   - AVG(ROAS) → (SUM(Revenue) / SUM(Spend))")
    print("   - AVG(Conv_Rate) → (SUM(Conversions) * 100.0 / SUM(Clicks))")
    print("   - And more...")
    print("\n📚 See METRIC_CALCULATION_GUIDE.md for detailed explanations")

if __name__ == "__main__":
    main()

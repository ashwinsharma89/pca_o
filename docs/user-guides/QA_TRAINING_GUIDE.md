# 🎓 Q&A System Training Guide

## Overview

This guide explains how to train and test the Natural Language to SQL Q&A system with 20 diverse questions covering all aspects of campaign analytics.

---

## 📋 Training Dataset

**Location**: `data/training_questions.json`

### Question Categories (20 Total)

1. **Basic Aggregation** - Simple SUM, AVG, COUNT queries
2. **Platform Comparison** - Compare performance across platforms
3. **Campaign Performance** - Analyze specific campaigns
4. **Time-based Analysis** - Month-over-month, date ranges
5. **Cost Metrics** - CPC, CPA, spend analysis
6. **Efficiency Analysis** - Find best/worst performers
7. **Multi-metric Comparison** - Multiple KPIs at once
8. **Specific Campaign** - Query individual campaigns
9. **CTR Analysis** - Click-through rate metrics
10. **Date Range** - Filter by specific dates
11. **Reach & Frequency** - Audience metrics
12. **ROI Comparison** - ROAS comparisons
13. **Budget Analysis** - Spend distribution
14. **Seasonal Performance** - Monthly/quarterly trends
15. **Campaign Count** - Count unique entities
16. **Platform Distribution** - Distribution analysis
17. **Performance Threshold** - Filter by thresholds
18. **Cost Efficiency** - Efficiency with conditions
19. **Top Performers** - Top N queries
20. **Quarterly Analysis** - Q4 performance

### Difficulty Levels

- **Easy (12 questions)**: Basic aggregations, simple filters
- **Medium (8 questions)**: Date operations, complex filters, percentages
- **Hard (0 questions)**: Reserved for future advanced queries

---

## 🚀 Running the Training Tests

### Method 1: Automated Testing (All 20 Questions)

```bash
python scripts/training/train_qa_system.py
```

**What it does:**
1. ✅ Loads sample campaign data
2. ✅ Loads 20 training questions
3. ✅ Initializes Q&A engine
4. ✅ Tests each question
5. ✅ Generates SQL queries
6. ✅ Executes queries
7. ✅ Shows results
8. ✅ Calculates pass rate
9. ✅ Saves results to CSV

**Output:**
```
🚀 PCA AGENT Q&A SYSTEM - TRAINING & TESTING
================================================

📂 Loading sample data...
✅ Loaded 30 rows with 13 columns

📋 Loading training questions...
✅ Loaded 20 test questions

🤖 Initializing Q&A Engine...
✅ Engine ready!

RUNNING TESTS
================================================

TEST #1: Basic Aggregation
Question: What is the total spend across all campaigns?
✅ Generated SQL: SELECT SUM(Spend) as Total_Spend FROM campaigns
📊 Results: $641,000
⏱️  Execution Time: 0.123s

[... continues for all 20 tests ...]

📊 TEST SUMMARY
================================================
Total Tests: 20
✅ Passed: 18
❌ Failed: 2
📈 Pass Rate: 90.0%
```

### Method 2: Interactive Mode (Manual Testing)

```bash
python scripts/training/train_qa_system.py interactive
```

**What it does:**
- Opens interactive prompt
- Type any question
- Get instant SQL + results
- Test custom queries
- Type 'quit' to exit

**Example:**
```
🎯 INTERACTIVE Q&A MODE
Type your questions or 'quit' to exit

❓ Your question: Which campaign had the best ROAS?

📝 Generated SQL:
  SELECT Campaign_Name, MAX(ROAS) as Best_ROAS 
  FROM campaigns GROUP BY Campaign_Name 
  ORDER BY Best_ROAS DESC LIMIT 1

📊 Results:
  Campaign_Name        Best_ROAS
  Cyber_Monday_2024    5.5

⏱️  Time: 0.145s
```

---

## 📊 Sample Questions

### Easy Questions

1. **"What is the total spend across all campaigns?"**
   - Tests: Basic SUM aggregation
   - Expected: Single number result

2. **"Which platform generated the highest total conversions?"**
   - Tests: GROUP BY, ORDER BY, LIMIT
   - Expected: Platform name with count

3. **"Show me the top 3 campaigns by ROAS"**
   - Tests: TOP N queries
   - Expected: 3 campaigns with ROAS

4. **"What is the average CPC by platform?"**
   - Tests: AVG with GROUP BY
   - Expected: Platform-wise averages

5. **"Which campaign has the lowest CPA?"**
   - Tests: MIN with GROUP BY
   - Expected: Campaign with lowest CPA

### Medium Questions

6. **"Compare performance of last 2 months"**
   - Tests: Date operations, DATE_TRUNC
   - Expected: Monthly comparison

7. **"What percentage of total spend was on LinkedIn Ads?"**
   - Tests: CASE WHEN, percentage calculation
   - Expected: Percentage value

8. **"Which month had the highest total conversions?"**
   - Tests: Date extraction, aggregation
   - Expected: Month with count

9. **"Show campaigns with ROAS greater than 4.5"**
   - Tests: WHERE clause with threshold
   - Expected: Filtered campaign list

10. **"What was the total spend in Q4 2024?"**
    - Tests: Date range filtering
    - Expected: Q4 spend total

---

## 📈 Expected Results

### Pass Rate Goals

- **Target**: 85%+ pass rate
- **Good**: 75-85% pass rate
- **Needs Work**: <75% pass rate

### Common Issues

1. **Date Type Mismatches**
   - Fixed: Auto-convert Date columns to datetime
   - SQL: Use CAST(Date AS DATE) when needed

2. **Column Name Variations**
   - Fixed: Case-sensitive matching
   - SQL: Use exact column names from schema

3. **Aggregation Errors**
   - Fixed: Proper GROUP BY clauses
   - SQL: Include all non-aggregated columns

---

## 🔧 Training Process

### Step 1: Initial Run
```bash
python scripts/training/train_qa_system.py
```
- Run all 20 tests
- Note pass rate
- Identify failing questions

### Step 2: Analyze Failures
- Check generated SQL
- Compare with expected SQL
- Identify pattern issues

### Step 3: Improve Prompts
Edit `src/query_engine/nl_to_sql.py`:
- Update system prompt
- Add more examples
- Clarify instructions

### Step 4: Re-test
```bash
python scripts/training/train_qa_system.py
```
- Run tests again
- Check improvement
- Iterate until 85%+ pass rate

### Step 5: Interactive Testing
```bash
python scripts/training/train_qa_system.py interactive
```
- Test edge cases
- Try variations
- Validate improvements

---

## 📁 Output Files

### Test Results CSV
**Location**: `data/test_results_YYYYMMDD_HHMMSS.csv`

**Columns:**
- `test_number`: Test ID
- `question_id`: Question ID from training set
- `category`: Question category
- `question`: Natural language question
- `status`: PASS or FAIL
- `generated_sql`: SQL query generated
- `execution_time`: Query execution time
- `row_count`: Number of result rows
- `error`: Error message if failed

**Example:**
```csv
test_number,question_id,category,question,status,generated_sql,execution_time,row_count,error
1,1,Basic Aggregation,"What is the total spend?",PASS,"SELECT SUM(Spend)...",0.123,1,
2,2,Platform Comparison,"Which platform...",PASS,"SELECT Platform...",0.145,1,
3,3,Time-based Analysis,"Compare last 2 months",FAIL,,0.0,0,Date type mismatch
```

---

## 🎯 Success Metrics

### Quantitative Metrics

1. **Pass Rate**: % of questions answered correctly
2. **Execution Time**: Average query execution time
3. **SQL Quality**: Correctness of generated SQL
4. **Result Accuracy**: Correctness of query results

### Qualitative Metrics

1. **SQL Readability**: Is the SQL clean and understandable?
2. **Query Efficiency**: Are queries optimized?
3. **Error Handling**: Are errors handled gracefully?
4. **User Experience**: Is the system easy to use?

---

## 🔄 Continuous Improvement

### Add New Questions

1. Edit `data/training_questions.json`
2. Add new question object:
```json
{
  "id": 21,
  "category": "New Category",
  "question": "Your question here",
  "expected_sql": "Expected SQL pattern",
  "difficulty": "Easy|Medium|Hard"
}
```
3. Update metadata counts
4. Re-run training

### Improve Prompts

1. Open `src/query_engine/nl_to_sql.py`
2. Edit `generate_sql()` method
3. Update system prompt
4. Add more context
5. Test improvements

### Fine-tune Model

For advanced users:
1. Collect successful query pairs
2. Create fine-tuning dataset
3. Fine-tune GPT-4 on your data
4. Update model in code
5. Test improvements

---

## 📚 Question Templates

### Aggregation Template
```
"What is the [SUM|AVG|COUNT|MIN|MAX] of [metric] [by platform|by campaign]?"
```

### Comparison Template
```
"Which [platform|campaign] has the [highest|lowest] [metric]?"
```

### Time-based Template
```
"Show [metric] for [last N months|Q4|specific date range]"
```

### Threshold Template
```
"Show [campaigns|platforms] with [metric] [greater than|less than] [value]"
```

### Top N Template
```
"Show top [N] [campaigns|platforms] by [metric]"
```

---

## 🎓 Best Practices

### Writing Good Questions

1. ✅ **Be Specific**: "Show Google Ads spend" not "Show spend"
2. ✅ **Use Metrics**: Reference actual column names
3. ✅ **Clear Intent**: Make the goal obvious
4. ✅ **Avoid Ambiguity**: "Last 2 months" not "recent"
5. ✅ **Test Variations**: Try different phrasings

### Testing Strategy

1. **Start Simple**: Test basic queries first
2. **Add Complexity**: Gradually increase difficulty
3. **Cover Categories**: Test all question types
4. **Edge Cases**: Test unusual scenarios
5. **Real Questions**: Use actual user questions

---

## 🚀 Quick Start

### 1. Run Full Training
```bash
python scripts/training/train_qa_system.py
```

### 2. Check Results
- Look at pass rate
- Review failed tests
- Check output CSV

### 3. Try Interactive Mode
```bash
python scripts/training/train_qa_system.py interactive
```

### 4. Test Your Own Questions
- Type any question
- See SQL generated
- Verify results

### 5. Iterate and Improve
- Update prompts
- Add more questions
- Re-test

---

## 📞 Troubleshooting

### Issue: Low Pass Rate (<70%)

**Solutions:**
1. Check API key is valid
2. Verify data loaded correctly
3. Review generated SQL
4. Update prompt with more examples
5. Add schema context

### Issue: Date Queries Failing

**Solutions:**
1. Ensure Date column is datetime type
2. Use CAST(Date AS DATE) in queries
3. Update prompt with date handling instructions
4. Test date conversion in load_data()

### Issue: Wrong Results

**Solutions:**
1. Verify expected SQL is correct
2. Check data quality
3. Test query manually in DuckDB
4. Compare with actual results
5. Update expected SQL pattern

---

## ✅ Success Checklist

- [ ] API key configured
- [ ] Sample data loaded (30 rows)
- [ ] 20 training questions loaded
- [ ] Training script runs successfully
- [ ] Pass rate ≥ 85%
- [ ] Interactive mode works
- [ ] Results saved to CSV
- [ ] Failed tests analyzed
- [ ] Prompts improved if needed
- [ ] Custom questions tested

---

## 🎉 Next Steps

After successful training:

1. **Deploy to Production**: Use in Streamlit app
2. **Add More Questions**: Expand training set
3. **Fine-tune Model**: Create custom model
4. **Monitor Performance**: Track real usage
5. **Collect Feedback**: Learn from users
6. **Iterate**: Continuously improve

---

**Happy Training! 🚀**

Your Q&A system is now ready to handle complex campaign analytics questions with high accuracy!

# 🎓 Multi-Platform Q&A Training Guide

## Overview

Comprehensive training system for testing Natural Language to SQL capabilities across **7 advertising platforms** with **90+ training questions** covering various analytics scenarios.

---

## 📊 Training Coverage

### **Platforms & Questions**

| Platform | Dataset Rows | Training Questions | Status |
|----------|--------------|-------------------|--------|
| **Meta Ads** | 2,000 | 30 | ✅ Ready |
| **Google Ads** | 2,000 | 30 | ✅ Ready |
| **LinkedIn Ads** | 2,000 | 30 | ✅ Ready |
| **Snapchat Ads** | 2,000 | - | 🔄 Pending |
| **CM360** | 2,000 | - | 🔄 Pending |
| **DV360** | 2,000 | - | 🔄 Pending |
| **The Trade Desk** | 2,000 | - | 🔄 Pending |
| **TOTAL** | **14,000** | **90+** | - |

---

## 🎯 Question Categories

### **1. Basic Aggregation** (Easy)
Simple sum, count, average calculations
- "What is the total spend across all campaigns?"
- "Show total impressions by campaign"
- "What is the average CTR?"

### **2. Performance Ranking** (Easy-Medium)
Top/bottom performers, best/worst metrics
- "Which campaign has the highest ROAS?"
- "Show top 5 keywords by conversion value"
- "Which ad set has the best conversion rate?"

### **3. Dimensional Analysis** (Easy-Medium)
Group by dimensions, breakdown by attributes
- "Show average CTR by placement"
- "What is the cost per conversion by age group?"
- "Display total impressions by device"

### **4. Comparative Analysis** (Medium)
Compare two or more segments
- "Compare Mobile vs Desktop performance"
- "Show male vs female engagement rates"
- "Compare Search vs Shopping network by ROAS"

### **5. Performance Filtering** (Easy-Medium)
Filter based on thresholds
- "Which campaigns have ROAS greater than 5?"
- "Show keywords with Quality Score > 8"
- "Display campaigns with CTR above 2.5%"

### **6. Time Series Analysis** (Medium-Hard)
Trends, growth, temporal patterns
- "Show daily spend trend for last 30 days"
- "Display week-over-week revenue growth"
- "What is the monthly conversion trend?"

### **7. Efficiency Metrics** (Medium)
Cost efficiency, performance ratios
- "What is the cost per thousand impressions?"
- "Show cost per lead by job function"
- "Calculate link click rate by placement"

### **8. Demographic Analysis** (Medium)
Age, gender, job function, seniority analysis
- "Which age group has the highest conversion rate?"
- "Show performance by job function"
- "Compare Director vs VP level by ROAS"

### **9. Engagement Analysis** (Medium)
Social engagement, interactions
- "Show total post reactions by campaign"
- "What is the engagement rate by placement?"
- "Display comments and shares by ad format"

### **10. Video Analysis** (Medium)
Video-specific metrics
- "What is the video completion rate by device?"
- "Show video view rate by placement"
- "Compare video performance across placements"

---

## 🚀 Quick Start

### **1. Run Training for All Platforms**

```bash
cd c:\Users\asharm08\OneDrive - dentsu\Desktop\windsurf\PCA_Agent
python train_all_platforms.py
```

This will:
- Load each platform's dataset
- Test all training questions
- Generate performance reports
- Save results to CSV files

### **2. Run Training for Single Platform**

```bash
python train_qa_system.py
```

Then select the platform and dataset when prompted.

### **3. Interactive Testing**

```python
from src.query_engine import NaturalLanguageQueryEngine
import pandas as pd
import os

# Initialize
api_key = os.getenv('OPENAI_API_KEY')
engine = NaturalLanguageQueryEngine(api_key)

# Load dataset
df = pd.read_csv('data/meta_ads_dataset.csv')
engine.load_data(df, table_name='campaigns')

# Ask questions
result = engine.ask("What is the total spend by campaign?")
print(result['answer'])
```

---

## 📈 Expected Results

### **Success Criteria**

| Metric | Target | Good | Excellent |
|--------|--------|------|-----------|
| **Pass Rate** | >70% | >80% | >90% |
| **Avg Execution Time** | <3s | <2s | <1s |
| **Easy Questions** | >90% | >95% | 100% |
| **Medium Questions** | >70% | >80% | >90% |
| **Hard Questions** | >50% | >60% | >70% |

### **Common Issues**

1. **Date Parsing Errors**
   - Solution: Ensure date columns are cast properly
   - Use: `CAST(Date AS DATE)` or `TRY_CAST(Date AS DATE)`

2. **Aggregation Errors**
   - Solution: Check GROUP BY clauses
   - Verify column names match schema

3. **Division by Zero**
   - Solution: Add null checks
   - Use: `CASE WHEN denominator > 0 THEN ... END`

4. **Column Name Mismatches**
   - Solution: Verify exact column names
   - Check case sensitivity

---

## 📊 Sample Training Questions

### **Meta Ads (30 Questions)**

**Easy:**
1. What is the total spend across all campaigns?
2. Which campaign has the highest ROAS?
3. Show average CTR by placement
4. What is the total revenue from Instagram placements?
5. Which ad set has the best conversion rate?

**Medium:**
6. Compare engagement rate between Mobile and Desktop
7. Show daily spend trend for the last 30 days
8. What is the average video completion rate for Stories vs Reels?
9. Which age group has the highest conversion rate?
10. What is the cost per conversion by objective?

**Hard:**
11. Show month-over-month revenue growth
12. Calculate incremental ROAS for retargeting campaigns
13. What is the 7-day moving average for CTR?

### **Google Ads (30 Questions)**

**Easy:**
1. What is the total cost across all campaigns?
2. Which keywords have Quality Score greater than 8?
3. Show average CTR by match type
4. Which campaign has the highest conversion rate?

**Medium:**
5. Compare Search vs Shopping network performance by ROAS
6. What is the average impression share by device?
7. Which campaigns have search lost IS due to rank > 15%?
8. What is the cost per conversion by age group?

**Hard:**
9. Show month-over-month clicks trend
10. Calculate incremental conversion value by match type

### **LinkedIn Ads (30 Questions)**

**Easy:**
1. What is the total spend across all campaigns?
2. Which job function has the highest conversion rate?
3. Show average CPC by seniority level
4. Which campaign group generates the most leads?

**Medium:**
5. Compare performance across industries by ROAS
6. What is the lead form completion rate by ad format?
7. What is the cost per lead by job function?
8. Which seniority level has the highest engagement rate?

**Hard:**
9. Show month-over-month revenue growth
10. Calculate lead generation efficiency by company size

---

## 🔧 Troubleshooting

### **Issue: API Key Not Found**
```bash
# Set in .env file
OPENAI_API_KEY=your_key_here
```

### **Issue: Module Not Found**
```bash
pip install -r requirements.txt
```

### **Issue: Date Parsing Errors**
- Check date format in CSV (YYYY-MM-DD)
- Verify date column name matches schema
- Use explicit date casting in SQL

### **Issue: Low Pass Rate**
- Review failed questions in results CSV
- Check SQL query generation
- Verify data types in dataset
- Test with simpler questions first

---

## 📁 Output Files

### **Training Results**
Each platform generates a CSV file:
- `training_results_meta_ads_YYYYMMDD_HHMMSS.csv`
- `training_results_google_ads_YYYYMMDD_HHMMSS.csv`
- `training_results_linkedin_ads_YYYYMMDD_HHMMSS.csv`

### **Columns in Results**
- `question_id`: Unique question identifier
- `question`: Natural language question
- `category`: Question category
- `difficulty`: Easy/Medium/Hard
- `success`: True/False
- `execution_time`: Time in seconds
- `sql_generated`: Generated SQL query
- `error`: Error message if failed

---

## 🎯 Next Steps

### **Phase 1: Current (3 Platforms)** ✅
- [x] Meta Ads (30 questions)
- [x] Google Ads (30 questions)
- [x] LinkedIn Ads (30 questions)
- [x] Training script created
- [x] 2,000 rows per platform

### **Phase 2: Remaining Platforms** 🔄
- [ ] Create Snapchat Ads questions (30)
- [ ] Create CM360 questions (30)
- [ ] Create DV360 questions (30)
- [ ] Create Trade Desk questions (30)

### **Phase 3: Advanced Training** 🔮
- [ ] Cross-platform comparison questions
- [ ] Multi-table join questions
- [ ] Complex aggregation questions
- [ ] Attribution modeling questions

### **Phase 4: Production** 🚀
- [ ] Deploy to Streamlit
- [ ] Create user documentation
- [ ] Set up monitoring
- [ ] Implement feedback loop

---

## 📚 Resources

### **Documentation**
- `QA_TRAINING_GUIDE.md` - Original Q&A training guide
- `PLATFORM_DATASETS_GUIDE.md` - Platform-specific dataset guide
- `LARGE_DATASETS_SUMMARY.md` - Dataset generation summary

### **Scripts**
- `train_all_platforms.py` - Multi-platform training
- `train_qa_system.py` - Single platform training
- `generate_2000_row_datasets.py` - Dataset generation

### **Data Files**
- `data/*_dataset.csv` - Platform datasets (2,000 rows each)
- `data/*_training_questions.json` - Training questions

---

## ✅ Success Metrics

Track these metrics for each platform:

1. **Overall Pass Rate**: % of questions answered correctly
2. **Category Performance**: Pass rate by question category
3. **Difficulty Performance**: Pass rate by difficulty level
4. **Execution Speed**: Average time per question
5. **Error Types**: Common failure patterns

---

**Ready to start training!** 🚀

Run `python train_all_platforms.py` to begin testing across all platforms.

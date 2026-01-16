# 💬 Natural Language Query Engine Guide

## Overview

PCA Agent now includes a **powerful Natural Language SQL Query Engine** that lets you ask questions about your campaign data in plain English!

### Key Features

✅ **Natural Language Queries**: Ask questions in plain English  
✅ **AI-Powered SQL Generation**: Automatically converts questions to SQL  
✅ **Direct SQL Mode**: Write SQL queries directly for advanced users  
✅ **Instant Results**: Get answers with data tables  
✅ **Query History**: Track your previous questions  
✅ **Download Results**: Export query results as CSV  

---

## How It Works

### Architecture

```
Your Question (English)
        ↓
GPT-4 (LLM) → Converts to SQL
        ↓
DuckDB → Executes query on your data
        ↓
Results + Natural Language Answer
```

### Technology Stack

- **DuckDB**: Fast in-memory SQL database
- **OpenAI GPT-4**: Natural language to SQL conversion
- **Pandas**: Data manipulation
- **Streamlit**: Interactive UI

---

## Getting Started

### Step 1: Navigate to Q&A Tab

1. Open PCA Agent Streamlit dashboard
2. Click on **"💬 Ask Questions (SQL)"** tab
3. You'll see the query interface

### Step 2: Upload Your Data

1. Click **"Upload CSV file to query"**
2. Select your campaign data CSV
3. System loads data into DuckDB
4. Query engine initializes automatically

### Step 3: Ask Questions

**Two modes available:**

#### Mode 1: Natural Language (AI-Powered)
- Type questions in plain English
- AI converts to SQL automatically
- Get instant answers

#### Mode 2: Direct SQL
- Write SQL queries directly
- Full control over queries
- Perfect for advanced users

---

## Example Questions

### Campaign Performance

```
"Which campaign had the highest ROAS?"
"Show me the top 5 campaigns by conversions"
"Which campaign had the lowest CPA?"
"List all campaigns with ROAS greater than 4.0"
```

### Platform Analysis

```
"Which platform performs best on average?"
"Compare spend between google_ads and meta_ads"
"What is the CTR for each platform?"
"Show total conversions by platform"
```

### Financial Queries

```
"What is the total spend across all campaigns?"
"Calculate average CPA by campaign"
"Show campaigns sorted by spend descending"
"Which campaigns have spend over $50,000?"
```

### Time-Based Queries

```
"Which campaigns ran in November 2024?"
"Show monthly spend trends"
"What was the total spend in Q4 2024?"
"Compare performance by month"
```

### Aggregation Queries

```
"What is the average ROAS by platform?"
"Calculate total impressions by campaign"
"Show sum of conversions grouped by platform"
"What's the median CPA across all campaigns?"
```

---

## SQL Query Examples

### Basic Queries

**Get all campaigns:**
```sql
SELECT * FROM campaigns
```

**Top campaigns by ROAS:**
```sql
SELECT Campaign_Name, Platform, ROAS, Spend, Conversions
FROM campaigns
ORDER BY ROAS DESC
LIMIT 10
```

**Filter by platform:**
```sql
SELECT * FROM campaigns
WHERE Platform = 'google_ads'
```

### Aggregation Queries

**Total spend by platform:**
```sql
SELECT Platform, 
       SUM(Spend) as Total_Spend,
       SUM(Conversions) as Total_Conversions,
       AVG(ROAS) as Avg_ROAS
FROM campaigns
GROUP BY Platform
ORDER BY Total_Spend DESC
```

**Campaign performance summary:**
```sql
SELECT Campaign_Name,
       COUNT(DISTINCT Platform) as Platforms,
       SUM(Spend) as Total_Spend,
       SUM(Conversions) as Total_Conversions,
       AVG(ROAS) as Avg_ROAS
FROM campaigns
GROUP BY Campaign_Name
ORDER BY Total_Spend DESC
```

### Advanced Queries

**Performance categories:**
```sql
SELECT Campaign_Name,
       Platform,
       ROAS,
       CASE 
           WHEN ROAS >= 4.0 THEN 'Excellent'
           WHEN ROAS >= 3.0 THEN 'Good'
           WHEN ROAS >= 2.0 THEN 'Average'
           ELSE 'Needs Improvement'
       END as Performance_Category
FROM campaigns
ORDER BY ROAS DESC
```

**Monthly trends:**
```sql
SELECT strftime(Date, '%Y-%m') as Month,
       SUM(Spend) as Total_Spend,
       SUM(Conversions) as Total_Conversions,
       AVG(ROAS) as Avg_ROAS
FROM campaigns
GROUP BY Month
ORDER BY Month
```

**Top and bottom performers:**
```sql
(SELECT 'Top 5' as Category, Campaign_Name, Platform, ROAS
 FROM campaigns
 ORDER BY ROAS DESC
 LIMIT 5)
UNION ALL
(SELECT 'Bottom 5' as Category, Campaign_Name, Platform, ROAS
 FROM campaigns
 ORDER BY ROAS ASC
 LIMIT 5)
```

---

## Features

### 1. Natural Language Mode

**How it works:**
1. Type your question in plain English
2. Click "🔍 Get Answer"
3. AI generates SQL query
4. Query executes on your data
5. Get natural language answer + data table

**Example:**
```
Question: "Which campaign had the best ROAS?"

Generated SQL:
SELECT Campaign_Name, Platform, ROAS, Spend, Conversions
FROM campaigns
ORDER BY ROAS DESC
LIMIT 1

Answer: "Cyber Monday 2024 on Google Ads had the best ROAS 
at 5.5x, with $92,000 spend generating 2,100 conversions."
```

### 2. Direct SQL Mode

**How it works:**
1. Write SQL query in text area
2. Click "▶️ Execute Query"
3. Results displayed instantly
4. Download as CSV if needed

**Benefits:**
- Full control over queries
- Complex joins and subqueries
- Advanced SQL functions
- No AI interpretation needed

### 3. Suggested Questions

Click any suggested question to auto-fill and execute:
- Pre-built common queries
- Learn by example
- Quick insights
- No typing needed

### 4. Query History

- Last 5 queries saved
- Review previous questions
- See SQL that was generated
- Reuse successful queries

### 5. Download Results

- Export any query results
- CSV format
- Use in Excel, Google Sheets
- Share with team

---

## Tips & Best Practices

### For Natural Language Queries

1. **Be Specific**: "Show top 5 campaigns by ROAS" vs "Show campaigns"
2. **Use Column Names**: Mention specific metrics (ROAS, CPA, Spend)
3. **Specify Sorting**: "sorted by", "highest", "lowest"
4. **Set Limits**: "top 10", "bottom 5"
5. **Use Comparisons**: "greater than", "less than", "between"

### For SQL Queries

1. **Table Name**: Always use `campaigns` as table name
2. **Column Names**: Case-sensitive, use exact names
3. **Date Functions**: Use DuckDB date functions
4. **Aggregations**: Always use GROUP BY with aggregate functions
5. **Test First**: Start simple, then add complexity

### Performance Tips

1. **Limit Results**: Use LIMIT for large datasets
2. **Index Columns**: Filter on indexed columns (Platform, Campaign_Name)
3. **Avoid SELECT ***: Specify only needed columns
4. **Use WHERE**: Filter before aggregating

---

## Common Use Cases

### 1. Campaign Review Meeting

**Questions to ask:**
- "Show all campaigns sorted by ROAS"
- "Which campaigns exceeded $50K spend?"
- "What's the average CPA by platform?"
- "Compare Q3 vs Q4 performance"

### 2. Budget Planning

**Questions to ask:**
- "What was total spend by month?"
- "Which platforms have best ROI?"
- "Show campaigns with CPA under $50"
- "Calculate spend efficiency by campaign"

### 3. Platform Evaluation

**Questions to ask:**
- "Compare all platforms by ROAS"
- "Which platform has lowest CPA?"
- "Show conversion rates by platform"
- "Total impressions by platform"

### 4. Performance Optimization

**Questions to ask:**
- "Which campaigns underperformed?"
- "Show campaigns with ROAS below 3.0"
- "Find high-spend, low-conversion campaigns"
- "Identify best performing creatives"

---

## Troubleshooting

### Issue: "Query engine not initialized"

**Solution**: Ensure OpenAI API key is set in `.env` file:
```
OPENAI_API_KEY=sk-your-key-here
```

### Issue: "SQL Error"

**Solutions**:
- Check column names (case-sensitive)
- Verify table name is `campaigns`
- Use valid SQL syntax
- Check for typos

### Issue: "No results found"

**Solutions**:
- Verify data is loaded
- Check filter conditions
- Try broader query
- Review WHERE clause

### Issue: "AI generated wrong SQL"

**Solutions**:
- Rephrase question more clearly
- Use Direct SQL mode instead
- Check suggested questions for examples
- Specify column names explicitly

---

## Advanced Features

### Custom Functions

DuckDB supports many SQL functions:

**String Functions:**
```sql
SELECT UPPER(Campaign_Name), LOWER(Platform)
FROM campaigns
```

**Date Functions:**
```sql
SELECT strftime(Date, '%Y-%m') as Month,
       strftime(Date, '%A') as Day_of_Week
FROM campaigns
```

**Math Functions:**
```sql
SELECT Campaign_Name,
       ROUND(ROAS, 2) as ROAS_Rounded,
       CEIL(CPA) as CPA_Ceiling
FROM campaigns
```

### Window Functions

```sql
SELECT Campaign_Name,
       ROAS,
       RANK() OVER (ORDER BY ROAS DESC) as ROAS_Rank,
       AVG(ROAS) OVER () as Avg_ROAS
FROM campaigns
```

### Subqueries

```sql
SELECT * FROM campaigns
WHERE ROAS > (SELECT AVG(ROAS) FROM campaigns)
ORDER BY ROAS DESC
```

---

## API Integration (Future)

Coming soon: REST API endpoints for programmatic queries

```python
# Future API usage
import requests

response = requests.post("http://localhost:8000/api/query", json={
    "question": "Which campaign had the best ROAS?",
    "mode": "natural_language"
})

results = response.json()
print(results['answer'])
print(results['data'])
```

---

## Comparison: Natural Language vs Direct SQL

| Feature | Natural Language | Direct SQL |
|---------|-----------------|------------|
| **Ease of Use** | ⭐⭐⭐⭐⭐ Very Easy | ⭐⭐⭐ Moderate |
| **Speed** | ⭐⭐⭐ (AI processing) | ⭐⭐⭐⭐⭐ Instant |
| **Flexibility** | ⭐⭐⭐ Good | ⭐⭐⭐⭐⭐ Full control |
| **Learning Curve** | ⭐⭐⭐⭐⭐ None | ⭐⭐ SQL knowledge needed |
| **Complex Queries** | ⭐⭐⭐ Limited | ⭐⭐⭐⭐⭐ Unlimited |
| **Best For** | Quick insights | Advanced analysis |

---

## Summary

The Natural Language Query Engine makes campaign data analysis accessible to everyone:

✅ **No SQL knowledge required** (Natural Language mode)  
✅ **Instant answers** to business questions  
✅ **Full SQL power** for advanced users  
✅ **Export results** for further analysis  
✅ **Query history** for tracking insights  

**Start asking questions about your campaign data today!**

Upload your CSV → Ask questions → Get insights → Make better decisions! 🚀

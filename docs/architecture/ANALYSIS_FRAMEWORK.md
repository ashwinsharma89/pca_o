# 📊 Post-Campaign Analysis Framework

## 🎯 **Scenario-Based Analysis Selection Guide**

Understanding **which analysis to apply in which scenario** is critical for actionable insights.

---

## 1️⃣ **Performance Analysis**

### **When to Use**:
- ✅ Campaign just ended - need overall health check
- ✅ Mid-campaign optimization review
- ✅ Stakeholder reporting (executives, clients)
- ✅ Budget allocation decisions

### **Key Metrics**:

#### **Reach & Frequency**
```sql
-- Unique reach and average frequency
SELECT 
    Campaign_Name,
    SUM(Reach) as total_reach,
    (SUM(Impressions) * 1.0 / SUM(Reach)) as avg_frequency
FROM campaigns
GROUP BY Campaign_Name
```

**Scenarios**:
- 📌 **High frequency, low reach**: Oversaturation - expand targeting
- 📌 **Low frequency, high reach**: Awareness campaign - good for brand building
- 📌 **Frequency > 5**: Risk of ad fatigue - creative refresh needed

#### **Engagement Metrics**
```sql
-- CTR, video completion, engagement rate
SELECT 
    Placement,
    (SUM(Clicks) * 100.0 / SUM(Impressions)) as ctr,
    (SUM(Video_Views) * 100.0 / SUM(Impressions)) as video_view_rate,
    (SUM(Engagement) * 100.0 / SUM(Impressions)) as engagement_rate
FROM campaigns
GROUP BY Placement
```

**Scenarios**:
- 📌 **Low CTR (<1%)**: Creative or targeting issue
- 📌 **High views, low completion**: Video too long or not engaging
- 📌 **High engagement, low conversion**: Awareness working, funnel broken

#### **Conversion Tracking**
```sql
-- Conversion funnel analysis
SELECT 
    Campaign_Name,
    SUM(Impressions) as impressions,
    SUM(Clicks) as clicks,
    SUM(Conversions) as conversions,
    (SUM(Clicks) * 100.0 / SUM(Impressions)) as ctr,
    (SUM(Conversions) * 100.0 / SUM(Clicks)) as conv_rate
FROM campaigns
GROUP BY Campaign_Name
```

**Scenarios**:
- 📌 **High CTR, low conv rate**: Landing page issue
- 📌 **Low CTR, high conv rate**: Good targeting, poor creative
- 📌 **Both low**: Fundamental campaign issue

#### **Cost Efficiency**
```sql
-- Cost metrics comparison
SELECT 
    Campaign_Name,
    (SUM(Spend) / SUM(Clicks)) as cpc,
    (SUM(Spend) / SUM(Impressions) * 1000) as cpm,
    (SUM(Spend) / SUM(Conversions)) as cpa,
    (SUM(Revenue) / SUM(Spend)) as roas
FROM campaigns
GROUP BY Campaign_Name
```

**Scenarios**:
- 📌 **CPA > target**: Need optimization or pause
- 📌 **ROAS < 1**: Losing money - immediate action
- 📌 **CPM increasing**: Auction pressure or ad fatigue

---

## 2️⃣ **Statistical Testing**

### **When to Use**:
- ✅ A/B test results need validation
- ✅ Proving campaign incrementality
- ✅ Budget justification to CFO
- ✅ Understanding true impact vs correlation

### **A/B Testing Results**

**Scenario**: Testing 2 ad creatives
```sql
-- Compare Creative A vs Creative B
SELECT 
    Creative_Name,
    SUM(Impressions) as impressions,
    SUM(Clicks) as clicks,
    (SUM(Clicks) * 100.0 / SUM(Impressions)) as ctr,
    SUM(Conversions) as conversions,
    (SUM(Conversions) * 100.0 / SUM(Clicks)) as conv_rate
FROM campaigns
WHERE Creative_Name IN ('Creative_A', 'Creative_B')
GROUP BY Creative_Name
```

**Statistical Tests Needed**:
- **Chi-square test**: Is CTR difference significant?
- **T-test**: Is conversion rate difference significant?
- **Sample size**: Do we have enough data?

**Decision Framework**:
- 📌 **p-value < 0.05**: Statistically significant - scale winner
- 📌 **p-value > 0.05**: Not significant - need more data
- 📌 **Small sample**: Don't make decisions yet

### **Lift Studies**

**Scenario**: Proving campaign drove incremental sales
```sql
-- Compare exposed vs control group
SELECT 
    'Exposed' as group_type,
    COUNT(DISTINCT user_id) as users,
    SUM(conversions) as conversions,
    (SUM(conversions) * 1.0 / COUNT(DISTINCT user_id)) as conv_per_user
FROM exposed_users
UNION ALL
SELECT 
    'Control' as group_type,
    COUNT(DISTINCT user_id) as users,
    SUM(conversions) as conversions,
    (SUM(conversions) * 1.0 / COUNT(DISTINCT user_id)) as conv_per_user
FROM control_users
```

**Lift Calculation**:
```
Lift = (Exposed Conv Rate - Control Conv Rate) / Control Conv Rate * 100
```

**Scenarios**:
- 📌 **Positive lift**: Campaign is incremental - continue
- 📌 **No lift**: Campaign not driving incremental value
- 📌 **Negative lift**: Something wrong - investigate

### **Attribution Modeling**

**Scenario**: Understanding channel contribution
```sql
-- Multi-touch attribution
SELECT 
    Channel,
    SUM(first_touch_conversions) as first_touch,
    SUM(last_touch_conversions) as last_touch,
    SUM(linear_attribution_conversions) as linear,
    SUM(time_decay_conversions) as time_decay
FROM attribution_data
GROUP BY Channel
```

**When to Use**:
- 📌 **First-touch**: Understanding awareness drivers
- 📌 **Last-touch**: Understanding conversion drivers
- 📌 **Linear**: Equal credit to all touchpoints
- 📌 **Time-decay**: Recent touchpoints get more credit

---

## 3️⃣ **Audience Analysis**

### **When to Use**:
- ✅ Planning next campaign targeting
- ✅ Understanding who responds best
- ✅ Personalizing creative strategy
- ✅ Budget allocation by segment

### **Demographic Performance**

**Scenario**: Which age/gender segments perform best?
```sql
-- Performance by demographics
SELECT 
    Age,
    Gender,
    SUM(Impressions) as impressions,
    (SUM(Clicks) * 100.0 / SUM(Impressions)) as ctr,
    (SUM(Conversions) * 100.0 / SUM(Clicks)) as conv_rate,
    (SUM(Revenue) / SUM(Spend)) as roas
FROM campaigns
GROUP BY Age, Gender
ORDER BY roas DESC
```

**Decision Framework**:
- 📌 **High ROAS segment**: Increase budget allocation
- 📌 **Low ROAS segment**: Reduce or pause
- 📌 **High CTR, low conv**: Different creative needed

### **Geographic Analysis**

**Scenario**: Regional performance variations
```sql
-- Performance by location
SELECT 
    Location,
    SUM(Spend) as spend,
    SUM(Revenue) as revenue,
    (SUM(Revenue) / SUM(Spend)) as roas,
    (SUM(Spend) / SUM(Conversions)) as cpa
FROM campaigns
GROUP BY Location
ORDER BY roas DESC
```

**Scenarios**:
- 📌 **High-performing regions**: Increase geo-targeting
- 📌 **Low-performing regions**: Investigate or exit
- 📌 **Seasonal patterns**: Adjust by region

### **Device & Platform Breakdown**

**Scenario**: Cross-device behavior
```sql
-- Device performance
SELECT 
    Device,
    SUM(Impressions) as impressions,
    (SUM(Clicks) * 100.0 / SUM(Impressions)) as ctr,
    (SUM(Spend) / SUM(Clicks)) as cpc,
    (SUM(Revenue) / SUM(Spend)) as roas
FROM campaigns
GROUP BY Device
```

**Decision Framework**:
- 📌 **Mobile high CTR, low conv**: Mobile landing page issue
- 📌 **Desktop high ROAS**: Allocate more desktop budget
- 📌 **CTV high engagement**: Consider video-first strategy

---

## 4️⃣ **Temporal Analysis**

### **When to Use**:
- ✅ Optimizing ad scheduling
- ✅ Understanding user behavior patterns
- ✅ Budget pacing optimization
- ✅ Identifying best times to advertise

### **Dayparting**

**Scenario**: When do ads perform best?
```sql
-- Performance by hour of day
SELECT 
    EXTRACT(HOUR FROM timestamp) as hour_of_day,
    EXTRACT(DOW FROM timestamp) as day_of_week,
    SUM(Impressions) as impressions,
    (SUM(Clicks) * 100.0 / SUM(Impressions)) as ctr,
    (SUM(Conversions) * 100.0 / SUM(Clicks)) as conv_rate
FROM campaigns
GROUP BY hour_of_day, day_of_week
ORDER BY conv_rate DESC
```

**Decision Framework**:
- 📌 **High conv rate hours**: Increase bids during these times
- 📌 **Low conv rate hours**: Reduce bids or pause
- 📌 **Weekend vs weekday**: Adjust strategy accordingly

### **Trend Analysis**

**Scenario**: Performance trajectory over time
```sql
-- Daily performance trend
SELECT 
    Date,
    SUM(Spend) as daily_spend,
    SUM(Revenue) as daily_revenue,
    (SUM(Revenue) / SUM(Spend)) as daily_roas,
    AVG((SUM(Revenue) / SUM(Spend))) OVER (
        ORDER BY Date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as rolling_7day_roas
FROM campaigns
GROUP BY Date
ORDER BY Date
```

**Scenarios**:
- 📌 **Declining ROAS**: Ad fatigue - refresh creative
- 📌 **Improving ROAS**: Learning phase working - scale up
- 📌 **Volatile ROAS**: Inconsistent delivery - investigate

### **Pacing Analysis**

**Scenario**: Budget utilization tracking
```sql
-- Budget pacing
SELECT 
    Campaign_Name,
    SUM(Spend) as spent_to_date,
    MAX(budget) as total_budget,
    (SUM(Spend) * 100.0 / MAX(budget)) as pct_spent,
    DATEDIFF(MAX(end_date), CURRENT_DATE) as days_remaining
FROM campaigns
GROUP BY Campaign_Name
```

**Decision Framework**:
- 📌 **Underpacing (<80% at 80% time)**: Increase bids/budget
- 📌 **Overpacing (>80% at 50% time)**: Reduce bids/budget
- 📌 **On pace**: Continue monitoring

---

## 5️⃣ **Comparative Analysis**

### **When to Use**:
- ✅ Benchmarking against standards
- ✅ Channel mix optimization
- ✅ Creative testing and optimization
- ✅ Competitive analysis

### **Benchmark Comparison**

**Scenario**: How do we compare to industry standards?
```sql
-- Campaign vs benchmark
SELECT 
    Campaign_Name,
    (SUM(Clicks) * 100.0 / SUM(Impressions)) as actual_ctr,
    2.5 as industry_benchmark_ctr,
    (((SUM(Clicks) * 100.0 / SUM(Impressions)) - 2.5) / 2.5 * 100) as pct_vs_benchmark
FROM campaigns
GROUP BY Campaign_Name
```

**Benchmarks by Industry**:
- 📌 **E-commerce**: CTR 1.5-2.5%, Conv Rate 2-3%
- 📌 **B2B**: CTR 2-3%, Conv Rate 5-10%
- 📌 **Finance**: CTR 0.5-1%, Conv Rate 5-10%

### **Channel Comparison**

**Scenario**: Which channels drive best results?
```sql
-- Cross-channel performance
SELECT 
    Channel,
    SUM(Spend) as spend,
    SUM(Revenue) as revenue,
    (SUM(Revenue) / SUM(Spend)) as roas,
    (SUM(Spend) / SUM(Conversions)) as cpa,
    SUM(Conversions) as conversions
FROM campaigns
GROUP BY Channel
ORDER BY roas DESC
```

**Decision Framework**:
- 📌 **High ROAS channel**: Increase budget allocation
- 📌 **Low ROAS channel**: Optimize or reduce
- 📌 **High volume, low ROAS**: Good for awareness, not conversion

### **Creative Performance**

**Scenario**: Which assets drive best results?
```sql
-- Creative performance comparison
SELECT 
    Creative_Name,
    Creative_Type,
    SUM(Impressions) as impressions,
    (SUM(Clicks) * 100.0 / SUM(Impressions)) as ctr,
    (SUM(Conversions) * 100.0 / SUM(Clicks)) as conv_rate,
    (SUM(Revenue) / SUM(Spend)) as roas
FROM campaigns
GROUP BY Creative_Name, Creative_Type
ORDER BY roas DESC
```

**Decision Framework**:
- 📌 **Top 20% creatives**: Scale these
- 📌 **Bottom 20% creatives**: Pause these
- 📌 **Middle 60%**: Test variations

---

## 6️⃣ **Business Impact**

### **When to Use**:
- ✅ C-suite reporting
- ✅ Budget justification
- ✅ Long-term strategy planning
- ✅ Brand health tracking

### **Brand Lift Studies**

**Scenario**: Did campaign improve brand metrics?
```sql
-- Brand metrics comparison (pre vs post campaign)
SELECT 
    metric_type,
    pre_campaign_score,
    post_campaign_score,
    (post_campaign_score - pre_campaign_score) as absolute_lift,
    ((post_campaign_score - pre_campaign_score) / pre_campaign_score * 100) as pct_lift
FROM brand_lift_study
WHERE metric_type IN ('Awareness', 'Consideration', 'Preference', 'Intent')
```

**Interpretation**:
- 📌 **Awareness lift >5%**: Strong brand building
- 📌 **Consideration lift >3%**: Moving down funnel
- 📌 **Intent lift >2%**: Close to conversion

### **Sales Impact Analysis**

**Scenario**: Campaign impact on sales
```sql
-- Sales correlation with media spend
SELECT 
    DATE_TRUNC('week', date) as week,
    SUM(media_spend) as weekly_spend,
    SUM(sales) as weekly_sales,
    LAG(SUM(sales), 1) OVER (ORDER BY DATE_TRUNC('week', date)) as prev_week_sales,
    (SUM(sales) - LAG(SUM(sales), 1) OVER (ORDER BY DATE_TRUNC('week', date))) as sales_change
FROM campaign_sales_data
GROUP BY week
ORDER BY week
```

**Analysis**:
- 📌 **Correlation coefficient >0.7**: Strong relationship
- 📌 **Lag analysis**: How long does impact take?
- 📌 **Diminishing returns**: When does more spend not help?

### **Customer Acquisition Analysis**

**Scenario**: Quality and cost of acquired customers
```sql
-- Customer acquisition metrics
SELECT 
    Campaign_Name,
    COUNT(DISTINCT customer_id) as new_customers,
    (SUM(Spend) / COUNT(DISTINCT customer_id)) as cac,
    AVG(customer_lifetime_value) as avg_ltv,
    (AVG(customer_lifetime_value) / (SUM(Spend) / COUNT(DISTINCT customer_id))) as ltv_cac_ratio
FROM customer_acquisition
GROUP BY Campaign_Name
```

**Decision Framework**:
- 📌 **LTV:CAC > 3:1**: Healthy acquisition
- 📌 **LTV:CAC < 1:1**: Losing money on customers
- 📌 **CAC increasing**: Need optimization

---

## 🎯 **Decision Matrix: Which Analysis When?**

| Scenario | Primary Analysis | Secondary Analysis | Key Metrics |
|----------|-----------------|-------------------|-------------|
| **Campaign just ended** | Performance Analysis | Comparative Analysis | ROAS, CPA, CTR |
| **Mid-campaign check** | Temporal Analysis | Performance Analysis | Pacing, Daily ROAS |
| **Budget allocation** | Audience Analysis | Channel Comparison | ROAS by segment |
| **Creative refresh** | Creative Performance | A/B Testing | CTR, Engagement |
| **Proving ROI to CFO** | Statistical Testing | Business Impact | Lift, Incrementality |
| **Planning next campaign** | Audience Analysis | Benchmark Comparison | Best segments |
| **Ad fatigue suspected** | Temporal Analysis | Engagement Metrics | Frequency, CTR trend |
| **Landing page issue** | Conversion Tracking | Device Analysis | Conv rate by device |
| **Brand campaign** | Brand Lift Studies | Reach & Frequency | Awareness lift |
| **Performance campaign** | Cost Efficiency | Conversion Tracking | CPA, ROAS |

---

## 📚 **Analysis Workflow**

### **Step 1: Define Objective**
- What decision needs to be made?
- Who is the audience for this analysis?
- What's the timeline?

### **Step 2: Select Analysis Type**
- Use decision matrix above
- Consider data availability
- Align with business goals

### **Step 3: Run Analysis**
- Execute appropriate SQL queries
- Apply statistical tests if needed
- Validate data quality

### **Step 4: Interpret Results**
- Compare to benchmarks
- Identify patterns and anomalies
- Consider external factors

### **Step 5: Generate Insights**
- What's working and why?
- What's not working and why?
- What should we do differently?

### **Step 6: Make Recommendations**
- Specific, actionable recommendations
- Prioritized by impact
- Include expected outcomes

---

## 🎓 **Key Principles**

1. **Context Matters**: Same metric means different things in different scenarios
2. **Multiple Lenses**: Use 2-3 analysis types for complete picture
3. **Statistical Rigor**: Don't confuse correlation with causation
4. **Actionability**: Every analysis should lead to a decision
5. **Continuous Learning**: Track what works and iterate

---

**This framework ensures you apply the RIGHT analysis in the RIGHT scenario for ACTIONABLE insights.**

# Causal Analysis Knowledge Base for Marketing

## What is Causal Impact Analysis?

### Definition
Causal impact analysis is a statistical method that determines the true effect of a marketing intervention by comparing actual performance against a counterfactual (what would have happened without the intervention).

### Core Concept
**Mental Model:** Instead of asking "What happened?", causal analysis asks "What happened BECAUSE of our action?"

### Key Difference from Correlation
- **Correlation:** Two metrics move together
- **Causation:** One metric directly causes change in another
- **Example:** Ice cream sales and drowning deaths are correlated (both increase in summer), but ice cream doesn't cause drowning

---

## Why Causal Analysis Matters for Marketing

### 1. **Attribution Accuracy**
- Traditional attribution often credits the last touchpoint
- Causal analysis reveals true incremental impact
- Answers: "Did this campaign actually drive sales, or would they have happened anyway?"

### 2. **Budget Optimization**
- Identify which channels truly drive results
- Eliminate wasteful spend on non-causal activities
- Reallocate budget to high-impact initiatives

### 3. **Strategic Decision Making**
- Test hypotheses with statistical rigor
- Understand cause-and-effect relationships
- Make data-driven decisions with confidence

### 4. **Incrementality Measurement**
- Measure lift above baseline
- Quantify true business impact
- Separate organic growth from campaign-driven growth

---

## Types of Causal Analysis in Marketing

### 1. **A/B Testing (Randomized Controlled Trials)**
**What it is:** Randomly assign users to treatment vs control groups

**When to use:**
- Testing website changes
- Email campaign variations
- Ad creative testing

**Pros:**
- Gold standard for causality
- Clear cause-effect relationship
- Statistical rigor

**Cons:**
- Requires large sample sizes
- Not always feasible (can't randomize TV markets)
- May take time to reach significance

**Example:**
```
Treatment Group: See new ad creative → 5% conversion rate
Control Group: See old ad creative → 3% conversion rate
Causal Impact: +2% conversion rate (67% lift)
```

### 2. **Difference-in-Differences (DiD)**
**What it is:** Compare changes in treatment group vs control group over time

**When to use:**
- Geographic experiments (test in one region, not another)
- Time-based interventions
- When randomization isn't possible

**Formula:**
```
Causal Effect = (Treatment_After - Treatment_Before) - (Control_After - Control_Before)
```

**Example:**
```
Market A (Treatment): Ran TV campaign
- Before: 100 sales/day
- After: 150 sales/day
- Change: +50

Market B (Control): No TV campaign
- Before: 100 sales/day
- After: 120 sales/day
- Change: +20

Causal Impact = 50 - 20 = 30 sales/day attributed to TV campaign
```

### 3. **Synthetic Control Method**
**What it is:** Create a synthetic control group by weighting similar units

**When to use:**
- Only one treatment unit (e.g., one market)
- No natural control group exists
- Long time series data available

**How it works:**
1. Find similar markets/regions
2. Weight them to match pre-intervention trends
3. Compare treatment to synthetic control post-intervention

**Example:**
```
Launched campaign in California (no similar state)
Synthetic California = 0.4×Texas + 0.3×Florida + 0.3×New York
Compare actual California vs Synthetic California post-launch
```

### 4. **Regression Discontinuity Design (RDD)**
**What it is:** Exploit sharp cutoffs in treatment assignment

**When to use:**
- Treatment assigned based on threshold
- Clear before/after cutoff point

**Example:**
```
Loyalty program: Customers with >$1000 spend get VIP status
Compare customers just above vs just below $1000 threshold
Causal impact of VIP status on retention
```

### 5. **Interrupted Time Series Analysis**
**What it is:** Analyze time series before and after intervention

**When to use:**
- Single unit (one company, one market)
- Long historical data
- Clear intervention point

**Example:**
```
Daily sales for 2 years → Launch new product → Daily sales continue
Model pre-intervention trend, project forward
Difference = Causal impact of new product
```

### 6. **Propensity Score Matching**
**What it is:** Match treatment and control units based on similar characteristics

**When to use:**
- Observational data (no randomization)
- Many confounding variables
- Need to create comparable groups

**How it works:**
1. Calculate probability of receiving treatment (propensity score)
2. Match treated units to control units with similar scores
3. Compare outcomes between matched pairs

**Example:**
```
Email campaign sent to engaged users (not random)
Match engaged users who got email vs didn't get email
Based on: demographics, past behavior, engagement score
Compare conversion rates between matched groups
```

---

## Causal Impact Analysis Framework

### Step 1: Define the Question
**Bad Question:** "Did our campaign work?"
**Good Question:** "How many incremental conversions did our campaign drive beyond what would have happened organically?"

### Step 2: Identify Treatment and Control
- **Treatment:** Units that received the intervention
- **Control:** Units that didn't (or synthetic control)
- **Key:** Control must be comparable to treatment

### Step 3: Choose Analysis Method
Based on:
- Data availability
- Ability to randomize
- Number of treatment units
- Time series length

### Step 4: Check Assumptions
**Parallel Trends:** Treatment and control would have moved together without intervention
**No Spillover:** Treatment doesn't affect control group
**Stable Unit Treatment:** One unit's treatment doesn't affect another

### Step 5: Run Analysis
- Estimate counterfactual (what would have happened)
- Calculate difference (actual - counterfactual)
- Test statistical significance
- Quantify uncertainty (confidence intervals)

### Step 6: Interpret Results
**Report:**
- Absolute impact (e.g., +500 conversions)
- Relative impact (e.g., +25% lift)
- Statistical significance (p-value)
- Confidence interval (e.g., 95% CI: [+400, +600])

---

## Common Pitfalls and How to Avoid Them

### 1. **Confounding Variables**
**Problem:** Other factors drive the change, not your intervention

**Example:**
```
Launched campaign in December → Sales increased
But: December is holiday season (confounding variable)
```

**Solution:**
- Use control group experiencing same seasonality
- Include time controls in regression
- Match on confounding variables

### 2. **Selection Bias**
**Problem:** Treatment group differs systematically from control

**Example:**
```
Sent email to "engaged" users → High conversion rate
But: Engaged users convert more anyway
```

**Solution:**
- Randomize treatment assignment
- Use propensity score matching
- Regression adjustment for differences

### 3. **Reverse Causality**
**Problem:** Effect causes the treatment, not vice versa

**Example:**
```
High-value customers get VIP treatment → High retention
But: High retention customers were already valuable
```

**Solution:**
- Use instrumental variables
- Exploit natural experiments
- Regression discontinuity design

### 4. **Measurement Error**
**Problem:** Inaccurate data leads to wrong conclusions

**Solution:**
- Validate data sources
- Use multiple metrics
- Check for data quality issues

### 5. **Insufficient Power**
**Problem:** Sample size too small to detect true effect

**Solution:**
- Calculate required sample size beforehand
- Run longer experiments
- Use more sensitive metrics

---

## Causal Analysis for Specific Marketing Channels

### **Paid Search (Google Ads)**
**Question:** Do branded search ads drive incremental clicks?

**Method:** Geo-holdout experiment
- Turn off branded ads in some markets
- Keep running in others
- Compare branded traffic between markets

**Causal Insight:**
```
Markets with ads: 10,000 branded clicks
Markets without ads: 9,500 branded clicks (500 went to organic)
Incremental clicks from ads: 500 (5%)
Conclusion: 95% of branded clicks would happen anyway (organic)
```

### **Display Advertising**
**Question:** Does display advertising drive conversions?

**Method:** PSA (Public Service Announcement) control
- Show PSAs to control group instead of ads
- Compare conversion rates

**Causal Insight:**
```
Treatment (saw ads): 2.5% conversion rate
Control (saw PSAs): 2.3% conversion rate
Incremental lift: +0.2% (8.7% relative lift)
```

### **TV Advertising**
**Question:** What's the incremental impact of TV spend?

**Method:** Difference-in-Differences with matched markets
- Heavy-up TV in some DMAs
- Match to similar DMAs without heavy-up
- Compare sales changes

**Causal Insight:**
```
Treatment DMAs: +15% sales
Control DMAs: +5% sales (organic growth)
Causal impact: +10% incremental sales from TV
```

### **Email Marketing**
**Question:** Do promotional emails drive incremental revenue?

**Method:** Randomized holdout
- Randomly withhold email from 10% of list
- Compare revenue between groups

**Causal Insight:**
```
Email group: $50 average revenue
Holdout group: $45 average revenue
Incremental revenue: $5 per email sent
ROI calculation: ($5 × list size) - email cost
```

### **Social Media Advertising**
**Question:** Does Facebook advertising drive app installs?

**Method:** Conversion lift study (Facebook's tool)
- Facebook creates matched control group
- Shows PSAs to control
- Measures lift in installs

**Causal Insight:**
```
Treatment: 1,000 installs
Control: 850 installs
Incremental installs: 150 (15% lift)
Cost per incremental install: Total spend / 150
```

---

## Advanced Causal Techniques

### 1. **Marketing Mix Modeling (MMM)**
**What it is:** Regression-based approach to measure impact of all marketing channels

**Equation:**
```
Sales = β₀ + β₁(TV) + β₂(Digital) + β₃(Print) + β₄(Seasonality) + ε
```

**Pros:**
- Measures all channels simultaneously
- Accounts for interactions
- Historical data only (no experiments needed)

**Cons:**
- Correlation-based (not pure causation)
- Requires long time series
- Assumes linear relationships

### 2. **Bayesian Structural Time Series (BSTS)**
**What it is:** Google's CausalImpact package approach

**How it works:**
1. Model pre-intervention time series
2. Forecast what would have happened (counterfactual)
3. Compare actual vs forecast
4. Quantify uncertainty with Bayesian inference

**Use case:**
```
Launched new campaign on Day 100
Model Days 1-99, forecast Days 100-200
Actual sales vs forecasted sales = Causal impact
```

### 3. **Instrumental Variables (IV)**
**What it is:** Use a variable that affects treatment but not outcome directly

**Example:**
```
Question: Does email marketing drive sales?
Problem: We email engaged customers (selection bias)
Instrument: Random email delivery failures (technical issues)
Logic: Delivery failure affects who gets email, but doesn't directly affect sales
```

### 4. **Regression Discontinuity (RD)**
**What it is:** Exploit sharp cutoffs in treatment

**Example:**
```
Loyalty program: Spend >$500 → Gold status
Compare customers just above vs just below $500
Assumption: Customers at $499 vs $501 are similar
Difference in behavior = Causal impact of Gold status
```

---

## Metrics for Causal Analysis

### **Absolute Impact**
```
Incremental Conversions = Treatment_Conversions - Counterfactual_Conversions
Incremental Revenue = Treatment_Revenue - Counterfactual_Revenue
```

### **Relative Impact (Lift)**
```
Lift % = (Treatment - Control) / Control × 100
ROAS Lift = (Treatment_ROAS - Control_ROAS) / Control_ROAS × 100
```

### **Statistical Significance**
```
p-value < 0.05 → Statistically significant
Confidence Interval: [Lower Bound, Upper Bound]
Example: 95% CI: [+100, +300] conversions
```

### **Effect Size**
```
Cohen's d = (Mean_Treatment - Mean_Control) / Pooled_SD
Small effect: d = 0.2
Medium effect: d = 0.5
Large effect: d = 0.8
```

---

## Causal Analysis Checklist

### Before Analysis
- [ ] Clear hypothesis defined
- [ ] Treatment and control groups identified
- [ ] Sufficient sample size calculated
- [ ] Pre-intervention data collected
- [ ] Randomization plan (if applicable)
- [ ] Confounding variables identified

### During Analysis
- [ ] Parallel trends assumption checked
- [ ] No spillover between groups verified
- [ ] Data quality validated
- [ ] Statistical tests run
- [ ] Sensitivity analysis performed
- [ ] Robustness checks completed

### After Analysis
- [ ] Absolute impact quantified
- [ ] Relative lift calculated
- [ ] Statistical significance tested
- [ ] Confidence intervals reported
- [ ] Business implications documented
- [ ] Recommendations provided

---

## Integration with Marketing KPIs

### **ROAS (Return on Ad Spend)**
**Traditional:** Revenue / Spend
**Causal:** Incremental Revenue / Spend

**Why it matters:**
```
Traditional ROAS: $5 (includes organic conversions)
Causal ROAS: $2 (only incremental conversions)
Decision: Campaign may not be profitable when accounting for incrementality
```

### **CPA (Cost Per Acquisition)**
**Traditional:** Spend / Conversions
**Causal:** Spend / Incremental Conversions

**Why it matters:**
```
Traditional CPA: $50
Causal CPA: $125 (only 40% of conversions were incremental)
Decision: Need to optimize or reduce spend
```

### **Customer Lifetime Value (CLV)**
**Causal Question:** Does this campaign drive higher CLV customers?

**Analysis:**
```
Treatment group CLV: $500
Control group CLV: $450
Incremental CLV: $50 per customer
Decision: Worth higher CPA if incremental CLV is positive
```

---

## Tools and Technologies

### **Statistical Packages**
- **R:** CausalImpact, Matching, MatchIt, did
- **Python:** CausalML, DoWhy, EconML, CausalImpact
- **Stata:** Difference-in-differences, RDD packages

### **Platform-Specific Tools**
- **Google Ads:** Geo experiments, conversion lift studies
- **Facebook:** Conversion lift studies, brand lift studies
- **Google Analytics:** Experiments framework

### **Commercial Solutions**
- **Measured:** Marketing incrementality platform
- **Keen Decision Systems:** MMM and attribution
- **Neustar:** Marketing mix modeling
- **Analytic Partners:** ROI Genome

---

## Best Practices

### 1. **Start with Clear Hypotheses**
- Define what you expect to happen
- Specify magnitude of expected effect
- Document assumptions

### 2. **Design for Causality from the Start**
- Build in control groups
- Randomize when possible
- Collect pre-intervention data

### 3. **Use Multiple Methods**
- Triangulate findings across methods
- Check robustness
- Build confidence in results

### 4. **Communicate Uncertainty**
- Report confidence intervals
- Discuss limitations
- Acknowledge assumptions

### 5. **Iterate and Learn**
- Use findings to improve future campaigns
- Build institutional knowledge
- Document learnings

---

## Real-World Examples

### **Example 1: E-commerce Email Campaign**
**Scenario:** Online retailer wants to measure email impact

**Design:**
- Randomly withhold email from 10% of subscribers
- Send promotional email to 90%
- Measure 7-day revenue

**Results:**
```
Email group (90%): $45 average revenue per user
Holdout group (10%): $42 average revenue per user
Incremental revenue: $3 per email
Total list: 100,000 subscribers
Total incremental revenue: $3 × 90,000 = $270,000
Email cost: $5,000
Incremental ROI: ($270,000 - $5,000) / $5,000 = 5,300%
```

**Decision:** Email highly profitable, continue and potentially increase frequency

### **Example 2: TV Advertising for CPG Brand**
**Scenario:** Consumer packaged goods brand tests TV effectiveness

**Design:**
- Select 10 matched pairs of DMAs
- Heavy-up TV in one DMA per pair
- Measure retail sales via scanner data

**Results:**
```
Treatment DMAs: +12% sales vs prior year
Control DMAs: +3% sales vs prior year (category growth)
Causal impact: +9% incremental sales from TV
Annual sales in treatment DMAs: $50M
Incremental sales: $4.5M
TV spend: $2M
Incremental ROAS: $4.5M / $2M = 2.25
```

**Decision:** TV profitable, expand to more markets

### **Example 3: Paid Search Brand Bidding**
**Scenario:** SaaS company questions value of branded search ads

**Design:**
- Pause branded search in 50% of markets (randomly selected)
- Continue in other 50%
- Measure total branded traffic (paid + organic)

**Results:**
```
Markets with ads: 10,000 branded clicks (8,000 paid + 2,000 organic)
Markets without ads: 9,000 branded clicks (all organic)
Incremental clicks from ads: 1,000 (10%)
Branded ad spend: $5,000
Cost per incremental click: $5
```

**Decision:** 90% of branded clicks would happen organically, reduce branded bidding

---

## Glossary

**Counterfactual:** What would have happened without the intervention

**Treatment Effect:** Difference between actual outcome and counterfactual

**Incrementality:** Additional outcomes caused by marketing activity

**Parallel Trends:** Assumption that treatment and control would move together without intervention

**Spillover:** When treatment affects control group (violates assumptions)

**Synthetic Control:** Weighted combination of control units to match treatment unit

**Propensity Score:** Probability of receiving treatment given characteristics

**Instrumental Variable:** Variable affecting treatment but not outcome directly

**Regression Discontinuity:** Causal inference at threshold cutoffs

**Difference-in-Differences:** Compare changes in treatment vs control over time

---

## References and Further Reading

1. **Causal Inference: The Mixtape** by Scott Cunningham
2. **Mostly Harmless Econometrics** by Angrist & Pischke
3. **The Book of Why** by Judea Pearl
4. **Trustworthy Online Controlled Experiments** by Kohavi et al.
5. **Marketing Analytics: Data-Driven Techniques** by Winston

## Key Takeaways

1. **Correlation ≠ Causation:** Always question whether observed relationships are causal
2. **Incrementality is Key:** Focus on what you caused, not what happened
3. **Design Matters:** Build experiments with causality in mind from the start
4. **Multiple Methods:** Use different approaches to validate findings
5. **Communicate Clearly:** Report both absolute and relative impacts with uncertainty
6. **Iterate:** Use causal insights to continuously improve marketing effectiveness

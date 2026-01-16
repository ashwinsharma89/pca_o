# Keyword Match Types Guide

## Overview
Match types control how closely a user's search query must match your keyword for your ad to be eligible to show. Choosing the right match type is crucial for balancing reach and relevance.

## Match Type Hierarchy

```
Broad Match (Widest Reach)
    ↓
Phrase Match (Moderate Reach)
    ↓
Exact Match (Narrowest Reach)
```

## Match Type Details

### 1. Broad Match
**Symbol**: No symbol (default)
**Example Keyword**: `running shoes`

**Triggers Ads For**:
- Synonyms: athletic footwear, jogging sneakers
- Related searches: buy trainers, sports shoes
- Variations: shoe for running, running shoe sale
- Misspellings: runing shoes

**Pros**:
- Maximum reach
- Discovers new search terms
- Less keyword management
- Good for discovery phase

**Cons**:
- Least control over relevance
- Can trigger irrelevant searches
- Higher wasted spend risk
- Lower conversion rates typically

**Best For**:
- Brand awareness campaigns
- Discovery/testing phase
- Large budgets
- Broad product categories

**Performance Benchmarks**:
- CTR: 1.5-2.5%
- Conversion Rate: 2-4%
- CPC: Lower (more competition)

### 2. Phrase Match
**Symbol**: `"keyword"`
**Example Keyword**: `"running shoes"`

**Triggers Ads For**:
- Exact phrase: running shoes
- Close variations: running shoe, shoes for running
- With additional words: best running shoes, running shoes for women
- Reordered (sometimes): shoes running

**Does NOT Trigger For**:
- Synonyms: jogging sneakers
- Related but different: athletic footwear

**Pros**:
- Balance of reach and control
- More relevant than broad
- Captures intent variations
- Manageable search term volume

**Cons**:
- More limited reach than broad
- Requires more keyword variations
- Can still trigger some irrelevant searches

**Best For**:
- Most campaigns (default choice)
- Balancing performance and scale
- Specific product/service categories
- Mid-funnel targeting

**Performance Benchmarks**:
- CTR: 2.5-4%
- Conversion Rate: 4-7%
- CPC: Moderate

### 3. Exact Match
**Symbol**: `[keyword]`
**Example Keyword**: `[running shoes]`

**Triggers Ads For**:
- Exact keyword: running shoes
- Close variations: running shoe, shoes running
- Misspellings: runing shoes
- Singular/plural: running shoe
- Abbreviations: running shoes NYC

**Does NOT Trigger For**:
- Additional words: best running shoes
- Synonyms: jogging sneakers
- Related searches: athletic footwear

**Pros**:
- Maximum control
- Highest relevance
- Best conversion rates
- Lowest wasted spend

**Cons**:
- Limited reach
- Requires extensive keyword lists
- May miss valuable traffic
- Higher management overhead

**Best For**:
- High-intent keywords
- Brand terms
- Limited budgets
- Conversion-focused campaigns
- Competitive keywords

**Performance Benchmarks**:
- CTR: 4-8%
- Conversion Rate: 7-12%
- CPC: Higher (less competition, higher relevance)

## Match Type Strategy by Campaign Goal

### Brand Awareness
**Recommended**: Broad Match (70%) + Phrase Match (30%)
**Why**: Maximize reach, discover new audiences
**Budget**: Higher required

### Consideration
**Recommended**: Phrase Match (60%) + Broad Match (30%) + Exact Match (10%)
**Why**: Balance reach and relevance
**Budget**: Moderate

### Conversion
**Recommended**: Exact Match (50%) + Phrase Match (40%) + Broad Match (10%)
**Why**: Maximize efficiency and ROI
**Budget**: Can work with lower budgets

## Match Type Migration Strategy

### Phase 1: Discovery (Weeks 1-4)
```
Broad Match: 60%
Phrase Match: 30%
Exact Match: 10%

Goal: Discover high-performing search terms
```

### Phase 2: Optimization (Weeks 5-8)
```
Broad Match: 40%
Phrase Match: 40%
Exact Match: 20%

Goal: Shift budget to proven performers
Action: Add top broad match search terms as phrase/exact
```

### Phase 3: Efficiency (Week 9+)
```
Broad Match: 20%
Phrase Match: 40%
Exact Match: 40%

Goal: Maximize ROI
Action: Focus on high-converting exact match keywords
```

## Negative Keywords by Match Type

### Negative Broad Match
**Symbol**: `-keyword`
**Blocks**: All variations and related searches
**Use**: Rarely (too restrictive)

### Negative Phrase Match
**Symbol**: `-"keyword"`
**Blocks**: Searches containing the exact phrase
**Use**: Most common (balanced blocking)

### Negative Exact Match
**Symbol**: `-[keyword]`
**Blocks**: Only the exact keyword
**Use**: Surgical blocking of specific terms

## Advanced Strategies

### 1. Tiered Bidding Strategy
```
Exact Match: Bid 100% (baseline)
Phrase Match: Bid 70-80%
Broad Match: Bid 50-60%

Rationale: Pay more for higher intent/relevance
```

### 2. Search Term Mining
**Process**:
1. Run broad match campaigns
2. Review search term reports weekly
3. Add high-performers as phrase/exact
4. Add irrelevant terms as negatives
5. Pause low-performing broad keywords

**Frequency**: Weekly for first month, then bi-weekly

### 3. SKAG (Single Keyword Ad Groups)
**Structure**:
```
Ad Group: Running Shoes
- [running shoes] (Exact)
- "running shoes" (Phrase)
- running shoes (Broad Match Modified - deprecated)

Benefits: Maximum control, tailored ad copy
Drawbacks: High management overhead
```

### 4. Match Type Segmentation
**Setup**:
- Campaign 1: Exact Match Only (High Priority)
- Campaign 2: Phrase Match Only (Medium Priority)
- Campaign 3: Broad Match Only (Low Priority)

**Benefits**:
- Clear performance attribution
- Independent budget control
- Easier optimization

## Common Mistakes

### 1. Broad Match Without Negatives
**Problem**: Wasted spend on irrelevant searches
**Solution**: Build comprehensive negative keyword list (500+ terms)

### 2. Only Using Exact Match
**Problem**: Limited reach, missed opportunities
**Solution**: Add phrase match for top performers

### 3. Not Mining Search Terms
**Problem**: Missing optimization opportunities
**Solution**: Weekly search term review and action

### 4. Ignoring Match Type Performance
**Problem**: Inefficient budget allocation
**Solution**: Segment reporting by match type

### 5. Broad Match with Low Budgets
**Problem**: Budget exhausted on low-quality traffic
**Solution**: Start with phrase/exact, expand cautiously

## Match Type Performance Analysis

### Key Metrics by Match Type

| Metric | Exact | Phrase | Broad |
|--------|-------|--------|-------|
| Avg CTR | 5-8% | 3-5% | 1.5-3% |
| Avg CVR | 8-12% | 5-8% | 2-5% |
| Avg CPC | High | Medium | Low |
| Avg CPA | Low | Medium | High |
| Quality Score | 8-10 | 6-8 | 4-7 |

### ROI by Match Type
```
Exact Match: 4-6x ROAS
Phrase Match: 3-5x ROAS
Broad Match: 2-4x ROAS

(Industry averages, varies by vertical)
```

## Match Type Recommendations by Industry

### E-commerce
- **Primary**: Phrase Match (50%)
- **Secondary**: Exact Match (30%), Broad Match (20%)
- **Why**: Balance scale and efficiency

### B2B/SaaS
- **Primary**: Exact Match (60%)
- **Secondary**: Phrase Match (35%), Broad Match (5%)
- **Why**: High-value conversions, need precision

### Local Services
- **Primary**: Phrase Match (45%)
- **Secondary**: Exact Match (40%), Broad Match (15%)
- **Why**: Local intent + service variations

### Lead Generation
- **Primary**: Exact Match (55%)
- **Secondary**: Phrase Match (40%), Broad Match (5%)
- **Why**: Quality over quantity

## Testing Framework

### A/B Test Setup
```
Test: Phrase Match vs. Exact Match
Duration: 4 weeks minimum
Budget Split: 50/50
Success Metric: CPA or ROAS

Hypothesis: Phrase match will deliver 30% more conversions
at acceptable CPA increase (<20%)
```

### Statistical Significance
- Minimum 100 conversions per variation
- 95% confidence level
- Account for seasonality

## Future of Match Types

### Google's Evolution
- Broad Match Modifier deprecated (2021)
- Phrase match expanded to include BMM behavior
- Increased reliance on AI/machine learning
- Smart Bidding integration

### Best Practices Going Forward
1. Embrace phrase match as new default
2. Use Smart Bidding with broader match types
3. Maintain strong negative keyword lists
4. Monitor search term reports religiously
5. Test broad match with automated bidding

## Resources
- Google Ads Match Types Guide
- Search Term Report Analysis
- Negative Keyword Best Practices

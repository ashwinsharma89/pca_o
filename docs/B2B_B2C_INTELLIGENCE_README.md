# B2B vs B2C Intelligence System

## Overview
The B2B/B2C Intelligence System provides context-aware campaign analysis that differentiates between business models, applying appropriate benchmarks, metrics, and recommendations based on whether campaigns target businesses (B2B), consumers (B2C), or both (B2B2C).

---

## 🎯 Key Features

### 1. Business Model Detection
- Automatic inference from campaign data
- Manual specification via CampaignContext
- Support for hybrid B2B2C models

### 2. Context-Aware Benchmarks
- **B2B Benchmarks**: Higher CPCs, lower CTRs, focus on lead quality
- **B2C Benchmarks**: Lower CPCs, higher CTRs, focus on conversion volume
- Platform-specific benchmarks (LinkedIn, Google, Meta)

### 3. Specialized Analysis
- **B2B**: Lead quality, pipeline impact, sales cycle alignment, audience seniority
- **B2C**: Purchase behavior, CAC efficiency, LTV analysis, conversion funnel
- **B2B2C**: Combined analysis with hybrid recommendations

### 4. Industry-Specific Insights
- Vertical-specific benchmarks and best practices
- Sales cycle considerations
- Target audience level analysis

---

## 📊 Data Models

### CampaignContext

```python
from src.models.campaign import CampaignContext, BusinessModel, TargetAudienceLevel

# B2B Example
b2b_context = CampaignContext(
    business_model=BusinessModel.B2B,
    industry_vertical="SaaS",
    sales_cycle_length=60,  # days
    average_deal_size=25000,  # dollars
    target_audience_level=TargetAudienceLevel.VP_DIRECTOR,
    customer_lifetime_value=75000,
    target_cac=5000,
    geographic_focus=["North America", "Europe"],
    competitive_intensity="high",
    brand_maturity="growth"
)

# B2C Example
b2c_context = CampaignContext(
    business_model=BusinessModel.B2C,
    industry_vertical="E-commerce",
    average_order_value=85,
    purchase_frequency="monthly",
    customer_lifetime_value=425,
    target_cac=35,
    geographic_focus=["United States"],
    seasonality_factor="high"
)
```

### Business Model Types

```python
class BusinessModel(str, Enum):
    B2B = "B2B"          # Business to Business
    B2C = "B2C"          # Business to Consumer
    B2B2C = "B2B2C"      # Hybrid model
```

### Target Audience Levels (B2B)

```python
class TargetAudienceLevel(str, Enum):
    C_SUITE = "C-suite"                      # CEO, CFO, CTO
    VP_DIRECTOR = "VP/Director"              # VP, Director level
    MANAGER = "Manager"                      # Manager level
    INDIVIDUAL_CONTRIBUTOR = "Individual Contributor"
    MIXED = "Mixed"                          # Multiple levels
    CONSUMER = "Consumer"                    # B2C audience
```

---

## 🔧 Usage

### Basic Usage

```python
from src.agents.b2b_specialist_agent import B2BSpecialistAgent
from src.models.campaign import CampaignContext, BusinessModel
from src.analytics import MediaAnalyticsExpert

# 1. Run base analysis
expert = MediaAnalyticsExpert()
base_analysis = expert.analyze_all(campaign_data)

# 2. Define business context
context = CampaignContext(
    business_model=BusinessModel.B2B,
    industry_vertical="SaaS",
    sales_cycle_length=60,
    average_deal_size=25000
)

# 3. Enhance with B2B specialist
specialist = B2BSpecialistAgent()
enhanced_analysis = specialist.enhance_analysis(
    base_insights=base_analysis,
    campaign_context=context,
    campaign_data=campaign_data
)

# 4. Access B2B-specific insights
b2b_insights = enhanced_analysis['business_model_analysis']
print(b2b_insights['lead_quality_analysis'])
print(b2b_insights['pipeline_contribution'])
```

### With RAG Integration

```python
from src.mcp.rag_integration import RAGIntegration

# Initialize with RAG for enhanced insights
rag = RAGIntegration()
specialist = B2BSpecialistAgent(rag_retriever=rag)

# Analysis will include context from knowledge base
enhanced_analysis = specialist.enhance_analysis(
    base_insights=base_analysis,
    campaign_context=context,
    campaign_data=campaign_data
)
```

---

## 📈 B2B-Specific Analysis

### 1. Lead Quality Analysis

**Metrics Analyzed**:
- Total leads generated
- Estimated MQL (Marketing Qualified Lead) rate
- Estimated SQL (Sales Qualified Lead) rate
- Cost per lead
- Cost per SQL

**Benchmarks**:
- MQL rate: 25% (industry average)
- SQL rate: 30% of MQLs
- Cost per SQL vs target CAC

**Output Example**:
```python
{
    'metric': 'Lead Quality',
    'total_leads': 435,
    'estimated_mqls': 109,
    'estimated_sqls': 33,
    'cost_per_lead': '$103.45',
    'cost_per_sql': '$1,363.64',
    'status': 'good',
    'findings': [
        '✅ Cost per SQL ($1,363.64) is within target CAC ($5,000.00)'
    ]
}
```

### 2. Pipeline Contribution

**Metrics Analyzed**:
- Estimated pipeline value
- Estimated revenue impact
- Number of opportunities
- Number of closed deals
- ROI calculation

**Assumptions**:
- SQL to Opportunity: 40%
- Opportunity to Close: 25%
- Uses average_deal_size from context

**Output Example**:
```python
{
    'metric': 'Pipeline Impact',
    'estimated_pipeline_value': '$330,000',
    'estimated_revenue': '$82,500',
    'estimated_opportunities': 13,
    'estimated_closed_deals': 3,
    'roi': '83.3%',
    'findings': [
        '💰 Estimated pipeline contribution: $330,000',
        '💵 Estimated revenue impact: $82,500',
        '📈 Estimated ROI: 83.3%'
    ]
}
```

### 3. Sales Cycle Alignment

**Analysis**:
- Short cycle (< 30 days): Direct response focus
- Medium cycle (30-90 days): Balance awareness and conversion
- Long cycle (> 90 days): Nurture and thought leadership

**Output Example**:
```python
{
    'metric': 'Sales Cycle Alignment',
    'sales_cycle_days': 60,
    'cycle_type': 'Medium',
    'findings': [
        '📅 Medium sales cycle (60 days) - balance awareness and conversion'
    ],
    'recommendation': 'Mix of educational content and conversion-focused campaigns'
}
```

### 4. Audience Seniority Analysis

**By Target Level**:

**C-Suite**:
- Expected CPC: High ($15-30)
- Expected CVR: Low (1-3%)
- Focus: Strategic value, ROI, executive content
- Channels: LinkedIn, industry publications, executive events

**VP/Director**:
- Expected CPC: Medium-High ($8-15)
- Expected CVR: Medium (3-5%)
- Focus: Balance strategic and tactical
- Channels: LinkedIn, search, industry content

**Manager**:
- Expected CPC: Medium ($5-10)
- Expected CVR: Medium-High (5-8%)
- Focus: Practical solutions, efficiency
- Channels: Search, LinkedIn, how-to content

**Individual Contributor**:
- Expected CPC: Low-Medium ($3-8)
- Expected CVR: High (8-12%)
- Focus: Ease of use, features
- Channels: Broad targeting, product content

---

## 📊 B2C-Specific Analysis

### 1. Purchase Behavior Analysis

**Metrics Analyzed**:
- Purchase frequency
- Average order value
- Repeat purchase patterns

**Output Example**:
```python
{
    'metric': 'Purchase Behavior',
    'purchase_frequency': 'monthly',
    'average_order_value': '$85.00',
    'findings': [
        '🔄 Purchase frequency: monthly',
        '💰 Average order value: $85.00'
    ],
    'recommendation': 'Balance acquisition and retention'
}
```

### 2. CAC Efficiency Analysis

**Metrics Analyzed**:
- Actual CAC vs target CAC
- CAC efficiency percentage
- Optimization opportunities

**Output Example**:
```python
{
    'metric': 'CAC Efficiency',
    'actual_cac': '$32.50',
    'cac_efficiency': '107.7%',
    'status': 'excellent',
    'findings': [
        '✅ CAC ($32.50) is within target ($35.00)'
    ]
}
```

### 3. Lifetime Value Analysis

**Metrics Analyzed**:
- LTV:CAC ratio
- Payback period
- Long-term profitability

**Benchmarks**:
- Excellent: LTV:CAC ≥ 3:1
- Good: LTV:CAC ≥ 2:1
- Poor: LTV:CAC < 2:1

**Output Example**:
```python
{
    'metric': 'Lifetime Value',
    'ltv': '$425.00',
    'ltv_cac_ratio': '13.08:1',
    'status': 'excellent',
    'findings': [
        '✅ Excellent LTV:CAC ratio (13.08:1)'
    ]
}
```

### 4. Conversion Funnel Analysis

**Metrics Analyzed**:
- Click-through rate
- Conversion rate
- Funnel bottlenecks

**Output Example**:
```python
{
    'metric': 'Conversion Funnel',
    'ctr': '1.50%',
    'conversion_rate': '3.00%',
    'bottleneck': None,
    'findings': [
        '✅ Healthy funnel performance'
    ]
}
```

---

## 🔀 B2B2C Hybrid Analysis

For hybrid business models, the system runs both B2B and B2C analyses and combines recommendations.

**Output Structure**:
```python
{
    'business_model': 'B2B2C',
    'b2b_analysis': {
        # Full B2B analysis
    },
    'b2c_analysis': {
        # Full B2C analysis
    },
    'hybrid_recommendations': [
        # Combined recommendations from both models
    ]
}
```

---

## 📊 Benchmarks

### B2B Benchmarks

#### LinkedIn
```python
{
    'ctr': {'excellent': 0.008, 'good': 0.005, 'poor': 0.003},
    'cpc': {'excellent': 5, 'acceptable': 8, 'high': 12},
    'lead_quality_rate': {'excellent': 0.4, 'good': 0.25, 'poor': 0.15},
    'mql_to_sql_rate': {'excellent': 0.35, 'good': 0.25, 'poor': 0.15}
}
```

#### Google Search (B2B)
```python
{
    'ctr': {'excellent': 0.05, 'good': 0.03, 'poor': 0.02},
    'cpc': {'excellent': 4, 'acceptable': 7, 'high': 15},
    'conversion_rate': {'excellent': 0.08, 'good': 0.05, 'poor': 0.03}
}
```

#### General B2B
```python
{
    'ltv_cac_ratio': {'excellent': 3.0, 'good': 2.0, 'poor': 1.0},
    'cac_payback_months': {'excellent': 12, 'acceptable': 18, 'poor': 24}
}
```

### B2C Benchmarks

#### Meta
```python
{
    'ctr': {'excellent': 0.015, 'good': 0.009, 'poor': 0.005},
    'cpc': {'excellent': 0.50, 'acceptable': 1.00, 'high': 2.00},
    'conversion_rate': {'excellent': 0.05, 'good': 0.025, 'poor': 0.01}
}
```

#### Google Search (B2C)
```python
{
    'ctr': {'excellent': 0.05, 'good': 0.035, 'poor': 0.02},
    'cpc': {'excellent': 1.00, 'acceptable': 2.50, 'high': 5.00},
    'conversion_rate': {'excellent': 0.06, 'good': 0.04, 'poor': 0.02}
}
```

#### General B2C
```python
{
    'ltv_cac_ratio': {'excellent': 3.0, 'good': 2.0, 'poor': 1.0},
    'repeat_purchase_rate': {'excellent': 0.30, 'good': 0.20, 'poor': 0.10}
}
```

---

## 🎯 Recommendations

### B2B Recommendations

**High Priority**:
- Lead quality optimization
- Sales cycle alignment
- Audience seniority targeting
- Pipeline contribution maximization

**Medium Priority**:
- Account-based marketing tactics
- Content strategy alignment
- Multi-touch attribution
- Sales enablement

**Low Priority**:
- Brand awareness expansion
- Thought leadership content
- Industry event participation

### B2C Recommendations

**High Priority**:
- CAC efficiency optimization
- LTV:CAC ratio improvement
- Conversion funnel optimization
- Retention strategies

**Medium Priority**:
- Creative testing and refresh
- Audience expansion
- Seasonal planning
- Cross-sell/upsell tactics

**Low Priority**:
- Brand awareness campaigns
- Social proof collection
- Influencer partnerships

---

## 🔄 Integration Points

### With MediaAnalyticsExpert

```python
# Enhance existing analysis
expert = MediaAnalyticsExpert()
base_analysis = expert.analyze_all(df)

specialist = B2BSpecialistAgent()
enhanced = specialist.enhance_analysis(base_analysis, context, df)
```

### With Channel Specialists

```python
from src.agents.channel_specialists import ChannelRouter

# Channel analysis first
router = ChannelRouter()
channel_analysis = router.route_and_analyze(df)

# Then enhance with B2B/B2C context
specialist = B2BSpecialistAgent()
final_analysis = specialist.enhance_analysis(
    channel_analysis, 
    context, 
    df
)
```

### With Streamlit App

```python
# In streamlit_app.py
if campaign_context:
    specialist = B2BSpecialistAgent()
    enhanced_analysis = specialist.enhance_analysis(
        st.session_state.analysis_data,
        campaign_context,
        st.session_state.df
    )
    
    # Display B2B/B2C specific insights
    st.markdown("## 🎯 Business Model Analysis")
    display_business_model_insights(enhanced_analysis)
```

---

## 📝 Examples

See `examples/b2b_specialist_integration.py` for complete examples:

1. **B2B SaaS Campaign** - Lead quality, pipeline impact, sales cycle
2. **B2C E-commerce Campaign** - CAC efficiency, LTV, conversion funnel
3. **B2B2C Marketplace** - Hybrid analysis with combined insights
4. **Benchmark Comparison** - Side-by-side B2B vs B2C benchmarks

Run examples:
```bash
python examples/b2b_specialist_integration.py
```

---

## 🧪 Testing

### Unit Tests
```bash
pytest tests/test_b2b_specialist.py
```

### Integration Tests
```bash
pytest tests/integration/test_b2b_integration.py
```

---

## 🚀 Future Enhancements

### Planned Features
1. **Industry-Specific Benchmarks** - Vertical-specific metrics
2. **Predictive Lead Scoring** - ML-based lead quality prediction
3. **Sales Cycle Optimization** - Automated nurture recommendations
4. **Account-Based Analytics** - Deep account engagement tracking
5. **Multi-Touch Attribution** - B2B-specific attribution models
6. **Pipeline Forecasting** - Revenue impact predictions
7. **Competitive Intelligence** - B2B competitive analysis
8. **Intent Data Integration** - Third-party intent signals

---

## 📚 Resources

### Documentation
- `src/models/campaign.py` - Data models
- `src/agents/b2b_specialist_agent.py` - Agent implementation
- `examples/b2b_specialist_integration.py` - Usage examples

### Knowledge Base
- `knowledge_sources/b2b/` - B2B-specific best practices
- `knowledge_sources/search/b2b_search_strategies.md` - B2B search tactics

### Related Systems
- Channel Specialists - Platform-specific analysis
- RAG Integration - Knowledge base retrieval
- MediaAnalyticsExpert - Base analysis engine

---

## ✅ Summary

**What Was Built**:
- ✅ CampaignContext model with B2B/B2C fields
- ✅ B2BSpecialistAgent with dual analysis modes
- ✅ B2B-specific metrics (lead quality, pipeline, sales cycle)
- ✅ B2C-specific metrics (CAC, LTV, funnel)
- ✅ Hybrid B2B2C support
- ✅ Context-aware benchmarks
- ✅ Specialized recommendations
- ✅ Complete examples and documentation

**Impact**:
- 🎯 Context-aware analysis based on business model
- 📊 Appropriate benchmarks for B2B vs B2C
- 💡 Relevant recommendations by business type
- 🔄 Support for hybrid models
- 📈 Better ROI measurement and forecasting

---

**🎉 B2B/B2C INTELLIGENCE SYSTEM: COMPLETE AND PRODUCTION-READY! 🎉**

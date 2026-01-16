# Channel-Specific Intelligence Layer

## Overview
The Channel-Specific Intelligence Layer provides specialized analysis for different advertising channels (Search, Social, Programmatic). Each channel has unique metrics, best practices, and optimization strategies that generic analysis cannot capture.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      Campaign Data                           │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│                   Channel Router                             │
│  • Auto-detects channel type                                 │
│  • Routes to appropriate specialist                          │
│  • Manages multi-channel analysis                            │
└─────────────┬───────────────┬───────────────┬───────────────┘
              │               │               │
              ▼               ▼               ▼
    ┌─────────────┐  ┌─────────────┐  ┌─────────────┐
    │   Search    │  │   Social    │  │Programmatic │
    │  Specialist │  │  Specialist │  │  Specialist │
    └─────────────┘  └─────────────┘  └─────────────┘
              │               │               │
              ▼               ▼               ▼
    ┌─────────────────────────────────────────────────┐
    │         RAG Knowledge Base (Optional)            │
    │  • Search best practices                         │
    │  • Social creative strategies                    │
    │  • Programmatic quality standards                │
    └─────────────────────────────────────────────────┘
```

## Channel Specialists

### 1. Search Channel Agent
**Platforms**: Google Ads, Bing Ads, SA360, DV360 Search

**Specialized Analysis**:
- **Quality Score Analysis**: Identifies low QS keywords, provides improvement recommendations
- **Auction Insights**: CPC trends, competitive positioning
- **Keyword Performance**: High/low performers, spend concentration
- **Impression Share Gaps**: Budget vs. rank limitations, opportunity sizing
- **Match Type Efficiency**: Performance by match type, optimization recommendations
- **Search Term Analysis**: Negative keyword opportunities

**Key Metrics**:
- Quality Score (target: 7+)
- Impression Share (target: 70%+)
- CTR (benchmark: 3.5%)
- CPC efficiency
- Keyword-level performance

**Knowledge Base**:
- `knowledge_sources/search/google_ads_quality_score.md`
- `knowledge_sources/search/impression_share_optimization.md`
- `knowledge_sources/search/keyword_match_types.md`

### 2. Social Channel Agent
**Platforms**: Meta (Facebook/Instagram), LinkedIn, TikTok, Snapchat, Twitter/X

**Specialized Analysis**:
- **Creative Fatigue Detection**: CTR decline patterns, refresh recommendations
- **Audience Saturation**: Frequency analysis, expansion strategies
- **Engagement Metrics**: Likes, comments, shares analysis
- **Algorithm Performance**: CPM trends, delivery consistency
- **Creative Performance**: Asset-level analysis, variation testing
- **Audience Insights**: Segment performance, targeting optimization

**Key Metrics**:
- Frequency (optimal: 2-3)
- Creative fatigue indicators
- Engagement rate (platform-specific benchmarks)
- CPM efficiency
- Creative lifespan

**Knowledge Base**:
- `knowledge_sources/social/creative_fatigue_detection.md`
- `knowledge_sources/social/frequency_optimization.md`
- `knowledge_sources/social/meta_algorithm_2024.md`

### 3. Programmatic Agent
**Platforms**: DV360, CM360, The Trade Desk, Amazon DSP

**Specialized Analysis**:
- **Viewability Analysis**: MRC compliance, placement optimization
- **Brand Safety**: Content verification, risk assessment
- **Placement Performance**: Site-level analysis, blocklist recommendations
- **Supply Path Optimization**: Exchange efficiency, cost reduction
- **Fraud Detection**: Invalid traffic (IVT) analysis
- **Video Performance**: Completion rates, quartile analysis
- **Inventory Quality**: Overall quality scoring

**Key Metrics**:
- Viewability rate (target: 70%+)
- Brand safety score (target: 95%+)
- IVT rate (threshold: <2%)
- CPM efficiency
- Supply path cost

**Knowledge Base**:
- `knowledge_sources/programmatic/viewability_standards.md`
- `knowledge_sources/programmatic/brand_safety_guidelines.md`
- `knowledge_sources/programmatic/supply_path_optimization.md`

## Usage

### Basic Usage

```python
from src.agents.channel_specialists import ChannelRouter
import pandas as pd

# Initialize router
router = ChannelRouter()

# Load campaign data
campaign_data = pd.read_csv('campaign_data.csv')

# Auto-detect channel and analyze
results = router.route_and_analyze(campaign_data)

print(f"Channel: {results['channel_type']}")
print(f"Health: {results['overall_health']}")
print(f"Recommendations: {results['recommendations']}")
```

### Multi-Channel Analysis

```python
# Analyze multiple channels
campaigns_by_channel = {
    'search': search_campaign_data,
    'social': social_campaign_data,
    'programmatic': programmatic_campaign_data
}

results = router.analyze_multi_channel(campaigns_by_channel)

# Get cross-channel insights
print(results['cross_channel_insights'])
```

### Direct Specialist Usage

```python
from src.agents.channel_specialists import SearchChannelAgent

# Use specific specialist directly
search_agent = SearchChannelAgent()
analysis = search_agent.analyze(search_data)

# Access specific analysis areas
print(analysis['quality_score_analysis'])
print(analysis['impression_share_gaps'])
```

### With RAG Integration

```python
from src.mcp.rag_integration import RAGIntegration

# Initialize with RAG for enhanced insights
rag = RAGIntegration()
router = ChannelRouter(rag_retriever=rag)

# Analysis will include context from knowledge base
results = router.route_and_analyze(campaign_data)
```

## Platform Detection

The system automatically detects the advertising platform and channel type using:

1. **Explicit Platform Column**: Checks for 'Platform' or 'Source' columns
2. **Column Name Patterns**: Identifies platform-specific metrics
3. **Metric Inference**: Analyzes available metrics to determine channel type

**Detection Examples**:
- Quality Score column → Google Ads (Search)
- Frequency column → Social platform
- Viewability column → Programmatic platform

## Benchmarks

### Industry Benchmarks by Channel

#### Search
- CTR: 3.5%
- Quality Score: 7.0
- Impression Share: 70%
- Conversion Rate: 4%

#### Social (Meta)
- CTR: 0.9%
- Frequency: 2.5
- Engagement Rate: 6%
- CPM: $7.19

#### Programmatic
- Viewability: 70%
- Brand Safety: 95%
- IVT Rate: <2%
- CPM: $2.80

### Custom Benchmarks

```python
from src.agents.channel_specialists.search_agent import SearchBenchmarks

# Override for your industry
SearchBenchmarks.BENCHMARKS['ctr'] = 0.045  # 4.5%
SearchBenchmarks.BENCHMARKS['quality_score'] = 8.0
```

## Health Scoring

Each specialist calculates an overall health score:

- **Excellent**: 90%+ metrics above benchmark
- **Good**: 70-90% metrics above benchmark
- **Average/Needs Attention**: 50-70% metrics above benchmark
- **Poor/Critical**: <50% metrics above benchmark

## Recommendations

Each specialist generates prioritized recommendations:

```python
{
    'priority': 'high',  # high, medium, low
    'area': 'quality_score',
    'issue': 'Low Quality Scores detected',
    'recommendation': 'Improve ad copy relevance and landing page experience',
    'expected_impact': 'high'  # high, medium, low
}
```

## Knowledge Base Structure

```
knowledge_sources/
├── search/
│   ├── google_ads_quality_score.md
│   ├── impression_share_optimization.md
│   ├── keyword_match_types.md
│   └── search_attribution_models.md
├── social/
│   ├── creative_fatigue_detection.md
│   ├── frequency_optimization.md
│   ├── meta_algorithm_2024.md
│   ├── linkedin_b2b_targeting.md
│   └── tiktok_creative_best_practices.md
└── programmatic/
    ├── viewability_standards.md
    ├── brand_safety_guidelines.md
    ├── supply_path_optimization.md
    └── fraud_prevention.md
```

## Integration with Existing System

### With MediaAnalyticsExpert

```python
from src.analytics.auto_insights import MediaAnalyticsExpert
from src.agents.channel_specialists import ChannelRouter

# Standard analysis
expert = MediaAnalyticsExpert(df)
standard_analysis = expert.analyze_all(df)

# Enhanced with channel specialists
router = ChannelRouter()
channel_analysis = router.route_and_analyze(df)

# Combine insights
combined_insights = {
    'standard_analysis': standard_analysis,
    'channel_specific': channel_analysis
}
```

### With Streamlit App

```python
import streamlit as st
from src.agents.channel_specialists import ChannelRouter

# In your Streamlit app
if st.button("Run Channel-Specific Analysis"):
    router = ChannelRouter()
    results = router.route_and_analyze(st.session_state.df)
    
    st.subheader(f"{results['channel_type'].capitalize()} Channel Analysis")
    st.metric("Overall Health", results['overall_health'])
    
    # Display recommendations
    for rec in results['recommendations']:
        st.warning(f"**{rec['priority'].upper()}**: {rec['recommendation']}")
```

## Extending the System

### Adding a New Specialist

1. **Create Specialist Class**:
```python
from .base_specialist import BaseChannelSpecialist

class VideoChannelAgent(BaseChannelSpecialist):
    def analyze(self, campaign_data):
        # Implement video-specific analysis
        pass
    
    def get_benchmarks(self):
        return {'vcr': 0.70, 'cpcv': 0.15}
```

2. **Register in Router**:
```python
# In channel_router.py
self.specialists['video'] = VideoChannelAgent(rag_retriever)
```

3. **Add Platform Mapping**:
```python
PLATFORM_MAPPING = {
    'youtube': 'video',
    'youtube ads': 'video'
}
```

### Adding Custom Analysis

```python
class CustomSearchAgent(SearchChannelAgent):
    def analyze(self, campaign_data):
        # Get base analysis
        analysis = super().analyze(campaign_data)
        
        # Add custom analysis
        analysis['custom_metric'] = self._custom_analysis(campaign_data)
        
        return analysis
    
    def _custom_analysis(self, data):
        # Your custom logic
        pass
```

## Performance Considerations

- **Caching**: Results are not cached by default. Implement caching if analyzing same data repeatedly.
- **Parallel Processing**: Multi-channel analysis runs sequentially. Consider parallelization for large datasets.
- **RAG Queries**: RAG retrieval adds latency. Use only when enhanced insights are needed.

## Testing

```python
# Run example integrations
python examples/channel_specialist_integration.py

# Unit tests
pytest tests/test_channel_specialists.py
```

## Future Enhancements

### Planned Features
1. **Video Channel Specialist**: YouTube, video-specific metrics
2. **Retail Media Specialist**: Amazon Ads, Walmart Connect
3. **Audio Channel Specialist**: Spotify, podcast advertising
4. **Cross-Channel Attribution**: Multi-touch attribution modeling
5. **Predictive Analytics**: Forecast channel performance
6. **Automated Optimization**: Auto-apply recommendations

### RAG Enhancements
- Real-time knowledge base updates
- Platform-specific case studies
- Competitive intelligence integration
- Industry-specific benchmarks

## Support & Documentation

- **Examples**: `examples/channel_specialist_integration.py`
- **Knowledge Base**: `knowledge_sources/[channel]/`
- **API Reference**: See docstrings in each specialist class
- **Issues**: Report bugs or request features via GitHub issues

## License
Same as parent project (PCA Agent)

## Contributors
- Ashwin Sharma (Initial Implementation)

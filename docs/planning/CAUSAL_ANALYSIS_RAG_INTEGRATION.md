# Causal Analysis with RAG Integration

## Overview

The PCA Agent now features a **comprehensive Causal Analysis system** enhanced with **RAG (Retrieval-Augmented Generation)** capabilities through an integrated knowledge base. This system combines mathematical precision with domain expertise to provide actionable, context-aware insights.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    User Interface (Streamlit)                │
│  ┌──────────────┬──────────────┬─────────────────────────┐ │
│  │ 🎯 Causal    │ 🔍 Driver    │ 🔬 Comprehensive        │ │
│  │   Analysis   │   Analysis   │   Diagnostics           │ │
│  └──────────────┴──────────────┴─────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│              Causal Analysis Engine + RAG                    │
│                                                              │
│  ┌──────────────────────┬────────────────────────────────┐ │
│  │ Formula-Based        │ Knowledge Base (RAG)           │ │
│  │ Decomposition        │ • Method recommendations       │ │
│  │ • ROAS (6 components)│ • Best practices               │ │
│  │ • CPA (4 components) │ • Interpretation guidance      │ │
│  │ • CTR, CVR, CPC, CPM │ • Pitfall warnings             │ │
│  └──────────────────────┴────────────────────────────────┘ │
│                                                              │
│  ┌──────────────────────┬────────────────────────────────┐ │
│  │ Attribution Analysis │ ML-Based Impact                │ │
│  │ • Platform           │ • XGBoost                      │ │
│  │ • Channel            │ • SHAP values                  │ │
│  └──────────────────────┴────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

---

## Key Components

### 1. **Causal Analysis Engine** (`src/analytics/causal_analysis.py`)

**Core Capabilities:**
- Formula-based KPI decomposition
- Attribution analysis by platform/channel
- ML-based causal impact (XGBoost + SHAP)
- Statistical confidence scoring
- Automated insight generation

**Supported Metrics:**
- **ROAS** (6-component decomposition)
- **CPA** (4-component decomposition)
- **CTR** (2-component decomposition)
- **CVR** (2-component decomposition)
- **CPC** (2-component decomposition)
- **CPM** (2-component decomposition)
- **Revenue** (2-component decomposition)
- **Spend** (platform-based decomposition)

### 2. **Knowledge Base** (`knowledge_base/causal_analysis_knowledge.md`)

**Content Domains:**
- **Causal Methods:** A/B testing, DiD, Synthetic Control, RDD, PSM
- **Marketing Metrics:** ROAS, CPA, CTR, CVR with causal interpretations
- **Common Pitfalls:** Confounding, selection bias, reverse causality
- **Best Practices:** Hypothesis formation, experimental design, validation
- **Real-World Examples:** E-commerce, TV advertising, paid search
- **Channel-Specific Guidance:** Search, display, social, email, TV

**Knowledge Sections:**
1. What is Causal Impact Analysis?
2. Why Causal Analysis Matters for Marketing
3. Types of Causal Analysis (6 methods)
4. Causal Analysis Framework (6 steps)
5. Common Pitfalls and Solutions
6. Channel-Specific Analysis
7. Advanced Techniques (MMM, BSTS, IV, RD)
8. Metrics and Interpretation
9. Tools and Technologies
10. Real-World Examples

### 3. **RAG Integration Module** (`src/knowledge/causal_kb_rag.py`)

**RAG Capabilities:**

#### **Method Recommendation**
```python
kb.get_method_recommendation({
    "has_randomization": False,
    "has_control_group": True,
    "num_time_periods": 30,
    "sample_size": 5000
})
# Returns: Difference-in-Differences recommended
```

#### **Interpretation Guidance**
```python
kb.get_interpretation_guidance(
    metric="CPA",
    change=10.0,
    change_pct=25.0,
    primary_driver="CPC"
)
# Returns: Context-aware interpretation with insights
```

#### **Actionable Recommendations**
```python
kb.get_recommendations({
    "primary_driver": "Cost Per Click (CPC)",
    "metric": "CPA",
    "platform_issues": ["Google"]
})
# Returns: Prioritized, actionable recommendations
```

#### **Pitfall Warnings**
```python
kb.get_pitfall_warnings({
    "seasonal_period": True,
    "randomized": False,
    "sample_size": 500
})
# Returns: Relevant pitfalls to watch for
```

---

## How RAG Enhances Causal Analysis

### **Before RAG (Pure Mathematical)**
```
Analysis Result:
- CPA increased by $10.00 (+25%)
- Primary driver: CPC (+$4.20, 42% contribution)
- Secondary drivers: CVR (+$1.90), CTR (+$1.10)
```

### **After RAG (Context-Aware)**
```
Analysis Result:
- CPA increased by $10.00 (+25%)
- Primary driver: CPC (+$4.20, 42% contribution)
- Secondary drivers: CVR (+$1.90), CTR (+$1.10)

💡 KB-Enhanced Insights:
- **Traditional CPA:** Spend / Conversions
- **Causal CPA:** Spend / Incremental Conversions
- **Key Insight:** Causal CPA reflects true cost of acquiring incremental customers
- ⚠️ **Watch out:** Traditional CPA underestimates by counting non-incremental conversions

🎯 KB-Enhanced Recommendations:
1. Reduce CPC through bid optimization and improved Quality Score
2. Review keyword targeting to eliminate low-performing terms
3. Test ad copy variations to improve relevance and CTR
4. Consider automated bidding strategies to optimize costs

⚠️ Pitfall Warning:
- Selection Bias: Treatment group differs systematically from control
- Solution: Randomize treatment assignment, use propensity score matching
```

---

## Integration Flow

### **Step 1: User Runs Analysis**
```python
# User selects metric and parameters in UI
metric = "CPA"
lookback_days = 30
include_ml = True
include_attribution = True
```

### **Step 2: Causal Engine Analyzes**
```python
# CausalAnalysisEngine performs decomposition
result = causal_engine.analyze(
    df=campaign_data,
    metric="CPA",
    lookback_days=30,
    method=DecompositionMethod.HYBRID,
    include_ml=True,
    include_attribution=True
)
```

### **Step 3: RAG Enhancement**
```python
# Knowledge base enhances result automatically
if KB_AVAILABLE:
    result = engine._enhance_with_knowledge_base(result, {
        "num_periods": 30,
        "sample_size": 1000,
        "has_control_group": False,
        "method": "hybrid"
    })
```

### **Step 4: Enhanced Result Returned**
```python
# Result now includes:
# - Original mathematical decomposition
# - KB-enhanced insights
# - Context-aware recommendations
# - Pitfall warnings
# - Best practice guidance
```

---

## Knowledge Base Structure

### **Causal Methods Knowledge**
```python
{
    "ab_testing": {
        "name": "A/B Testing (RCT)",
        "when_to_use": ["Testing website changes", "Large sample size"],
        "pros": ["Gold standard", "Clear causality"],
        "cons": ["Requires large samples", "Not always feasible"],
        "requirements": {
            "min_sample_size": 1000,
            "randomization": True
        }
    },
    "difference_in_differences": {...},
    "synthetic_control": {...},
    ...
}
```

### **Metric Guidance**
```python
{
    "ROAS": {
        "traditional": "Revenue / Spend",
        "causal": "Incremental Revenue / Spend",
        "components": ["Conversion Volume", "AOV", "Spend Efficiency", ...],
        "interpretation": "Causal ROAS shows true incremental return",
        "common_pitfall": "Traditional ROAS overestimates..."
    },
    ...
}
```

### **Recommendation Templates**
```python
{
    "high_cpc": [
        "Reduce CPC through bid optimization",
        "Review keyword targeting",
        "Test ad copy variations",
        ...
    ],
    "low_cvr": [...],
    "platform_negative": [...]
}
```

---

## Usage Examples

### **Example 1: Basic Causal Analysis with RAG**

```python
from src.analytics.causal_analysis import CausalAnalysisEngine

# Initialize engine
engine = CausalAnalysisEngine()

# Run analysis (RAG enhancement automatic)
result = engine.analyze(
    df=campaign_data,
    metric="ROAS",
    lookback_days=30
)

# Access enhanced insights
print(result.insights)
# Output includes both mathematical and KB-enhanced insights

print(result.recommendations)
# Output includes context-aware, actionable recommendations
```

### **Example 2: Method Recommendation**

```python
from src.knowledge.causal_kb_rag import get_knowledge_base

kb = get_knowledge_base()

# Get method recommendation
recommendation = kb.get_method_recommendation({
    "has_randomization": False,
    "has_control_group": True,
    "num_time_periods": 45,
    "sample_size": 3000
})

print(recommendation["primary_recommendation"]["method"])
# Output: "Difference-in-Differences (DiD)"

print(recommendation["primary_recommendation"]["reasons"])
# Output: ["Control group available", "Sufficient time periods", ...]
```

### **Example 3: Interpretation Guidance**

```python
kb = get_knowledge_base()

# Get interpretation for CPA increase
guidance = kb.get_interpretation_guidance(
    metric="CPA",
    change=15.0,
    change_pct=30.0,
    primary_driver="Cost Per Click (CPC)"
)

print(guidance["insights"])
# Output:
# - **Traditional CPA:** Spend / Conversions
# - **Causal CPA:** Spend / Incremental Conversions
# - **Key Insight:** Causal CPA reflects true cost...
# - 🎯 **Primary Driver:** CPC is the main cause...
```

### **Example 4: Comprehensive Diagnostics with RAG**

```python
from src.analytics.performance_diagnostics import PerformanceDiagnostics

diagnostics = PerformanceDiagnostics()

# Run comprehensive analysis (includes RAG enhancement)
results = diagnostics.comprehensive_diagnostics(
    df=campaign_data,
    metric="CPA",
    lookback_days=30
)

# Access cross-validated insights
print(results["combined_insights"])
# Output:
# - 🎯 Causal Analysis: CPC is primary cause (42%)
# - 🔍 Driver Analysis: CPC is top ML driver (0.234)
# - ✅ Validated: Both analyses agree on: CPC, CVR
# - 💡 Top Recommendation: Reduce CPC through bid optimization
```

---

## Benefits of RAG Integration

### **1. Context-Aware Insights**
- Mathematical decomposition + domain knowledge
- Interpretations tailored to marketing context
- Industry best practices automatically applied

### **2. Actionable Recommendations**
- Specific, prioritized actions
- Based on proven marketing strategies
- Adapted to your specific findings

### **3. Pitfall Prevention**
- Automatic warnings for common mistakes
- Context-specific cautions
- Solutions provided upfront

### **4. Method Guidance**
- Recommends best causal method for your data
- Explains pros/cons of each approach
- Validates assumptions

### **5. Educational Value**
- Teaches causal inference concepts
- Explains traditional vs causal metrics
- Builds analytical capability

---

## Knowledge Base Sources

The knowledge base is curated from industry-leading resources:

1. **Scale Marketing** - Causal Impact Analysis fundamentals
2. **Swydo** - Causal analysis for marketing
3. **Stella Hey Stella** - Advanced marketing leader tools
4. **Seer Interactive** - Practical causal impact analysis
5. **Exploratory.io** - Introduction to causal impact
6. **Lifesight** - Measuring causal impact in marketing
7. **Brian Curry Research** - Practical guide to causal inference
8. **BBC Studios** - Geo-holdouts and causal inference

---

## Configuration

### **Enable/Disable RAG**
```python
# RAG is automatically enabled if knowledge base is available
# To disable, set KB_AVAILABLE = False in causal_analysis.py

# Or check availability:
from src.analytics.causal_analysis import KB_AVAILABLE
if KB_AVAILABLE:
    print("RAG enhancement active")
```

### **Customize Knowledge Base**
```python
# Edit knowledge_base/causal_analysis_knowledge.md
# Or extend CausalKnowledgeBase class in src/knowledge/causal_kb_rag.py

class CustomKnowledgeBase(CausalKnowledgeBase):
    def _get_custom_recommendations(self):
        # Add your custom recommendations
        pass
```

---

## Testing

### **Test RAG Integration**
```python
from src.knowledge.causal_kb_rag import get_knowledge_base

kb = get_knowledge_base()

# Test method recommendation
rec = kb.get_method_recommendation({"has_randomization": True})
assert rec["primary_recommendation"]["method_key"] == "ab_testing"

# Test interpretation
guidance = kb.get_interpretation_guidance("ROAS", 5.0, 25.0)
assert len(guidance["insights"]) > 0

# Test recommendations
recs = kb.get_recommendations({"primary_driver": "CPC"})
assert "CPC" in recs[0] or "bid" in recs[0].lower()
```

---

## Future Enhancements

### **Planned Features:**
1. **Vector Database Integration** - Semantic search over knowledge base
2. **LLM-Powered Insights** - GPT-4 generated explanations
3. **Custom Knowledge Upload** - User-provided domain knowledge
4. **Interactive Q&A** - Ask questions about causal analysis
5. **Case Study Library** - Real-world examples database
6. **A/B Test Designer** - Guided experiment design
7. **Power Calculator** - Sample size recommendations
8. **Assumption Checker** - Automated validation of causal assumptions

---

## Troubleshooting

### **Knowledge Base Not Loading**
```python
# Check path
from src.knowledge.causal_kb_rag import CausalKnowledgeBase
kb = CausalKnowledgeBase()
print(kb.kb_path)  # Verify path is correct

# Check file exists
import os
print(os.path.exists(kb.kb_path))
```

### **RAG Enhancement Not Applied**
```python
# Check if KB is available
from src.analytics.causal_analysis import KB_AVAILABLE
print(f"KB Available: {KB_AVAILABLE}")

# Check logs
import logging
logging.basicConfig(level=logging.INFO)
# Look for "Enhanced causal result with knowledge base"
```

---

## Summary

The **Causal Analysis with RAG Integration** provides:

✅ **Mathematical Precision** - Formula-based decomposition  
✅ **Domain Expertise** - Industry best practices  
✅ **Context Awareness** - Tailored interpretations  
✅ **Actionable Insights** - Specific recommendations  
✅ **Pitfall Prevention** - Automatic warnings  
✅ **Method Guidance** - Best approach for your data  
✅ **Educational Value** - Learn while analyzing  

This system transforms raw data into **actionable, validated, context-aware insights** that drive better marketing decisions! 🚀

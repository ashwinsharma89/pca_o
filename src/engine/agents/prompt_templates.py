"""
Prompt Template System for AI Agents

Provides versioned, maintainable prompt templates with:
- Template versioning for rollback
- Variable substitution
- Prompt validation
- Usage tracking
"""

from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import json
import hashlib
from pathlib import Path
from loguru import logger


class PromptCategory(Enum):
    """Categories of prompts for organization."""
    ANALYSIS = "analysis"
    VISUALIZATION = "visualization"
    REASONING = "reasoning"
    REPORT = "report"
    EXTRACTION = "extraction"
    KNOWLEDGE = "knowledge"
    VALIDATION = "validation"


@dataclass
class PromptTemplate:
    """A versioned prompt template."""
    name: str
    template: str
    version: str
    category: PromptCategory
    description: str = ""
    variables: List[str] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.utcnow)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def hash(self) -> str:
        """Generate unique hash for this template version."""
        content = f"{self.name}:{self.version}:{self.template}"
        return hashlib.sha256(content.encode()).hexdigest()[:12]
    
    def render(self, **kwargs) -> str:
        """Render template with provided variables."""
        rendered = self.template
        for var in self.variables:
            placeholder = f"{{{var}}}"
            if var in kwargs:
                rendered = rendered.replace(placeholder, str(kwargs[var]))
            elif placeholder in rendered:
                logger.warning(f"Missing variable '{var}' in prompt '{self.name}'")
        return rendered
    
    def validate(self, **kwargs) -> List[str]:
        """Validate that all required variables are provided."""
        missing = []
        for var in self.variables:
            if var not in kwargs:
                missing.append(var)
        return missing


class PromptRegistry:
    """
    Central registry for all prompt templates.
    
    Usage:
        registry = PromptRegistry()
        
        # Register a template
        registry.register(PromptTemplate(
            name="campaign_analysis",
            template="Analyze the following campaign data: {data}",
            version="1.0.0",
            category=PromptCategory.ANALYSIS,
            variables=["data"]
        ))
        
        # Get and render
        prompt = registry.get("campaign_analysis")
        rendered = prompt.render(data="...")
    """
    
    def __init__(self):
        self._templates: Dict[str, Dict[str, PromptTemplate]] = {}  # name -> version -> template
        self._active_versions: Dict[str, str] = {}  # name -> active version
        self._usage_stats: Dict[str, int] = {}
        self._initialize_default_templates()
    
    def _initialize_default_templates(self):
        """Register default prompt templates."""
        
        # ========================
        # ANALYSIS PROMPTS
        # ========================
        
        self.register(PromptTemplate(
            name="campaign_performance_analysis",
            template="""You are an expert digital marketing analyst. Analyze the following campaign performance data and provide actionable insights.

## Campaign Data
{campaign_data}

## Analysis Requirements
1. Identify top and bottom performing campaigns
2. Calculate key metrics (CTR, CPC, ROAS, CVR)
3. Highlight anomalies or concerning trends
4. Provide optimization recommendations

## Response Format
Provide your analysis in a structured format with:
- Executive Summary (2-3 sentences)
- Key Metrics Table
- Top 3 Insights
- Top 3 Recommendations
- Risk Factors (if any)""",
            version="1.0.0",
            category=PromptCategory.ANALYSIS,
            description="Comprehensive campaign performance analysis",
            variables=["campaign_data"]
        ))
        
        self.register(PromptTemplate(
            name="channel_comparison",
            template="""Compare marketing channel performance based on the following data:

## Channel Data
{channel_data}

## Comparison Criteria
- Cost efficiency (CPC, CPM)
- Engagement (CTR, bounce rate)
- Conversion performance (CVR, ROAS)
- Audience quality

Provide a ranked comparison with specific recommendations for budget allocation.""",
            version="1.0.0",
            category=PromptCategory.ANALYSIS,
            description="Multi-channel performance comparison",
            variables=["channel_data"]
        ))
        
        # ========================
        # VISUALIZATION PROMPTS
        # ========================
        
        self.register(PromptTemplate(
            name="chart_recommendation",
            template="""Based on the following data characteristics, recommend the best visualization:

## Data Description
{data_description}

## Available Chart Types
- bar: Categorical comparisons
- line: Time series trends
- pie: Part-to-whole relationships
- scatter: Correlations
- heatmap: Multi-dimensional patterns
- funnel: Conversion flows
- treemap: Hierarchical data

## Response Format
{{
    "recommended_chart": "<chart_type>",
    "confidence": <0.0-1.0>,
    "reasoning": "<explanation>",
    "alternatives": ["<alt1>", "<alt2>"]
}}""",
            version="1.0.0",
            category=PromptCategory.VISUALIZATION,
            description="Intelligent chart type recommendation",
            variables=["data_description"]
        ))
        
        # ========================
        # REASONING PROMPTS
        # ========================
        
        self.register(PromptTemplate(
            name="sql_generation",
            template="""Convert the following natural language query to SQL for the campaigns table.

## User Query
{user_query}

## Table Schema
{table_schema}

## Rules
1. Use only columns that exist in the schema
2. Use appropriate aggregations for metrics
3. Handle date filtering properly
4. Limit results to 1000 rows maximum
5. Use parameterized queries for safety

## Response Format
Return ONLY the SQL query, no explanation.""",
            version="1.0.0",
            category=PromptCategory.REASONING,
            description="Natural language to SQL conversion",
            variables=["user_query", "table_schema"]
        ))
        
        self.register(PromptTemplate(
            name="insight_generation",
            template="""Generate actionable marketing insights from the following analysis results:

## Analysis Results
{analysis_results}

## Context
{context}

## Requirements
- Focus on business impact
- Provide specific, actionable recommendations
- Quantify potential improvements when possible
- Consider seasonality and market trends

Generate 3-5 high-impact insights.""",
            version="1.0.0",
            category=PromptCategory.REASONING,
            description="Generate actionable insights from data",
            variables=["analysis_results", "context"]
        ))
        
        # ========================
        # REPORT PROMPTS
        # ========================
        
        self.register(PromptTemplate(
            name="executive_summary",
            template="""Create an executive summary for the following marketing report:

## Report Data
{report_data}

## Time Period
{time_period}

## Requirements
- Maximum 200 words
- Lead with the most important finding
- Include key metrics with % changes
- End with strategic recommendation

Write in a professional, concise tone suitable for C-suite executives.""",
            version="1.0.0",
            category=PromptCategory.REPORT,
            description="Generate executive summary for reports",
            variables=["report_data", "time_period"]
        ))

        self.register(PromptTemplate(
            name="rag_executive_summary",
            template="""You are a marketing performance analyst at Dentsu conducting post-campaign analysis for clients. Analyze the campaign data and generate a concise 5-page strategic review focused on ROI impact, channel performance, and actionable recommendations.

## INPUT CONTEXT
- Campaign Period: {context}
- Client: Dentsu Client
- Campaign Objective: {objective}
- Total Budget: (Derived from Data)
- Channels Used: (Derived from Data)

{metrics}

---

## OUTPUT STRUCTURE (MAXIMUM 5 PAGES)

### PAGE 1: EXECUTIVE SUMMARY

**Campaign Performance Snapshot:**
- Total Spend: [AMOUNT]
- Total Results: [LEADS/CONVERSIONS/REACH]
- Overall ROI/ROAS: [METRIC]
- vs. Target: [% TO GOAL]

**Key Outcomes:**
✅ **Top 3 Wins:**
1. [Win] - [Metric/Impact]
2. [Win] - [Metric/Impact]
3. [Win] - [Metric/Impact]

⚠️ **Top 3 Challenges:**
1. [Issue] - [Impact]
2. [Issue] - [Impact]
3. [Issue] - [Impact]

**Critical Recommendations:**
1. [Action] → Expected Impact: [QUANTIFIED]
2. [Action] → Expected Impact: [QUANTIFIED]
3. [Action] → Expected Impact: [QUANTIFIED]

---

### PAGE 2: CHANNEL & PLATFORM PERFORMANCE

**Channel Performance Matrix:**

| Channel | Spend | Spend % | Results | CPL/CPA | ROI | Status | Action |
|---------|-------|---------|---------|---------|-----|--------|--------|
| LinkedIn | $X | X% | X | $X | X | 🟢 | Scale +20% |
| Google | $X | X% | X | $X | X | 🟡 | Optimize targeting |
| Meta | $X | X% | X | $X | X | 🔴 | Reduce budget |
| Display | $X | X% | X | $X | X | 🟡 | Test new creative |

**Channel Insights:**
- **Best Performer:** [Channel] - [Why it worked] - [Scale recommendation]
- **Biggest Opportunity:** [Channel] - [What to fix] - [Expected improvement]
- **Underperformer:** [Channel] - [Root cause] - [Action: optimize or pause]

**Budget Reallocation:**
- Move $[X] from [Channel] to [Channel]
- Expected ROI improvement: [%]

---

### PAGE 3: FUNNEL & AUDIENCE PERFORMANCE

**Funnel Analysis:**
```
IMPRESSIONS (X) → CLICKS (X) → LEADS (X) → CONVERSIONS (X)
     100%            X%           X%            X%
                   [vs benchmark]
```

**Funnel Insights:**
- **Biggest Drop-off:** [Stage] - [X% loss] - **Fix:** [Specific action]
- **Best Performing Stage:** [Stage] - [Benchmark comparison]
- **Conversion Rate:** [X%] vs. Target [Y%] - [Gap analysis]

**Audience Performance:**

**Top 3 Segments (Scale These):**
1. [Segment] - Conv Rate: [X%] - CPL: $[X]
2. [Segment] - Conv Rate: [X%] - CPL: $[X]
3. [Segment] - Conv Rate: [X%] - CPL: $[X]

**Bottom 3 Segments (Exclude These):**
1. [Segment] - Conv Rate: [X%] - CPL: $[X]
2. [Segment] - Conv Rate: [X%] - CPL: $[X]
3. [Segment] - Conv Rate: [X%] - CPL: $[X]

**Targeting Recommendation:** [Specific audience expansion/exclusion strategy]

---

### PAGE 4: CREATIVE & TACTICAL INSIGHTS

**Creative Performance:**

**What Worked:**
- **Ad Format:** [Format] outperformed by [X%]
- **Messaging:** [Theme/Angle] drove [X%] higher CTR
- **Visual:** [Style/Element] increased engagement by [X%]
- **CTA:** "[Text]" converted [X%] better

**What Didn't Work:**
- [Creative element] - [Performance issue]
- [Creative element] - [Performance issue]

**Creative Recommendations:**
1. [Specific change] → Expected lift: [%]
2. [Specific change] → Expected lift: [%]

**Tactical Optimization:**
- **Best Day/Time:** [Finding] - [Action]
- **Device Performance:** [Mobile/Desktop] - [X%] better - [Bid adjustment]
- **Placement:** [Top performing placement] - [Budget allocation]
- **Bid Strategy:** [What worked] - [Recommendation]

**A/B Test Results:**
- Test: [What was tested]
- Winner: [Variation X] by [X%]
- Learning: [Insight]
- Next Test: [Recommendation]

---

### PAGE 5: STRATEGIC RECOMMENDATIONS & NEXT STEPS

**Budget Optimization:**

| Action | From | To | Amount | Expected Impact |
|--------|------|-----|--------|-----------------|
| Shift | [Channel/Campaign] | [Channel/Campaign] | $[X] | +[X] leads, +[Y%] ROI |
| Scale | [Top performer] | - | +$[X] | +[X] leads |
| Pause | [Underperformer] | - | -$[X] | Reallocate to better ROI |

**Total Expected Improvement:** +[X%] ROI, +[X] additional conversions

**Campaign Action Plan:**

**Immediate Actions (Week 1-2):**
1. ✓ [Action] - Owner: [Name] - Impact: [Metric]
2. ✓ [Action] - Owner: [Name] - Impact: [Metric]
3. ✓ [Action] - Owner: [Name] - Impact: [Metric]

**Short-term Optimizations (Month 1):**
1. [Action] - Expected: [Result]
2. [Action] - Expected: [Result]

**Strategic Initiatives (Next Campaign):**
1. [Initiative] - Test budget: $[X] - Success metric: [KPI]
2. [Initiative] - Test budget: $[X] - Success metric: [KPI]

**Next Campaign Recommendations:**
- **Budget:** $[X] (based on [rationale])
- **Channel Mix:** [X% / Y% / Z%] across [channels]
- **Primary Audience:** [Segment focus]
- **Creative Strategy:** [Approach]
- **Success Metrics:** [KPI] = [Target]

**Key Learnings for Future Campaigns:**
1. [Learning from this campaign]
2. [Learning from this campaign]
3. [Learning from this campaign]

---

## OUTPUT FORMAT

Return a valid JSON object matching this structure exactly:
{{
    "overview": "A 3-4 sentence comprehensive overview of the portfolio health (Executive Summary Page 1).",
    "key_takeaways": [
        "**Page 1: Executive Summary**\\n[Content for Page 1]",
        "**Page 2: Channel Performance**\\n[Content for Page 2]",
        "**Page 3: Funnel & Audience**\\n[Content for Page 3]",
        "**Page 4: Creative & Tactics**\\n[Content for Page 4]",
        "**Page 5: Strategic Recommendations**\\n[Content for Page 5]"
    ],
    "rag_metadata": {{
        "sources_used": ["Campaign Data", "Dentsu Strategy Base"],
        "confidence_score": 0.99
    }}
}}

IMPORTANT: Ensure the JSON is valid. Escape newlines in the strings as \\n.""",
            version="2.0.0",
            category=PromptCategory.REPORT,
            description="Dentsu Strategic Review Template (5-Page Format)",
            variables=["objective", "context", "metrics", "status", "recommendations"]
        ))
        
        # ========================
        # VALIDATION PROMPTS
        # ========================
        
        self.register(PromptTemplate(
            name="data_quality_check",
            template="""Validate the quality of the following campaign data:

## Data Sample
{data_sample}

## Validation Checks
1. Missing values
2. Outliers (values outside normal ranges)
3. Inconsistent formats
4. Logical errors (e.g., clicks > impressions)
5. Date range issues

## Response Format
{{
    "quality_score": <0-100>,
    "issues": [
        {{"type": "<issue_type>", "column": "<column>", "severity": "high|medium|low", "description": "<desc>"}}
    ],
    "recommendations": ["<rec1>", "<rec2>"]
}}""",
            version="1.0.0",
            category=PromptCategory.VALIDATION,
            description="Data quality validation",
            variables=["data_sample"]
        ))
    
    def register(self, template: PromptTemplate, set_active: bool = True) -> None:
        """Register a prompt template."""
        if template.name not in self._templates:
            self._templates[template.name] = {}
        
        self._templates[template.name][template.version] = template
        
        if set_active:
            self._active_versions[template.name] = template.version
        
        logger.debug(f"Registered prompt: {template.name} v{template.version}")
    
    def get(self, name: str, version: Optional[str] = None) -> Optional[PromptTemplate]:
        """Get a prompt template by name (optionally specific version)."""
        if name not in self._templates:
            logger.warning(f"Prompt template '{name}' not found")
            return None
        
        target_version = version or self._active_versions.get(name)
        if not target_version:
            logger.warning(f"No active version for prompt '{name}'")
            return None
        
        template = self._templates[name].get(target_version)
        if template:
            self._usage_stats[name] = self._usage_stats.get(name, 0) + 1
        
        return template
    
    def render(self, name: str, version: Optional[str] = None, **kwargs) -> Optional[str]:
        """Get and render a prompt template."""
        template = self.get(name, version)
        if not template:
            return None
        
        missing = template.validate(**kwargs)
        if missing:
            logger.warning(f"Missing variables for '{name}': {missing}")
        
        return template.render(**kwargs)
    
    def set_active_version(self, name: str, version: str) -> bool:
        """Set the active version for a template."""
        if name not in self._templates:
            return False
        if version not in self._templates[name]:
            return False
        
        old_version = self._active_versions.get(name)
        self._active_versions[name] = version
        logger.info(f"Prompt '{name}': active version changed {old_version} -> {version}")
        return True
    
    def get_versions(self, name: str) -> List[str]:
        """Get all versions of a template."""
        if name not in self._templates:
            return []
        return list(self._templates[name].keys())
    
    def list_templates(self, category: Optional[PromptCategory] = None) -> List[Dict[str, Any]]:
        """List all registered templates."""
        templates = []
        for name, versions in self._templates.items():
            active_version = self._active_versions.get(name)
            if active_version and active_version in versions:
                template = versions[active_version]
                if category is None or template.category == category:
                    templates.append({
                        "name": name,
                        "version": active_version,
                        "category": template.category.value,
                        "description": template.description,
                        "variables": template.variables,
                        "usage_count": self._usage_stats.get(name, 0)
                    })
        return templates
    
    def export_templates(self, path: Path) -> None:
        """Export all templates to JSON file."""
        export_data = {
            "exported_at": datetime.utcnow().isoformat(),
            "templates": []
        }
        
        for name, versions in self._templates.items():
            for version, template in versions.items():
                export_data["templates"].append({
                    "name": template.name,
                    "version": template.version,
                    "category": template.category.value,
                    "description": template.description,
                    "template": template.template,
                    "variables": template.variables,
                    "is_active": self._active_versions.get(name) == version
                })
        
        path.write_text(json.dumps(export_data, indent=2))
        logger.info(f"Exported {len(export_data['templates'])} templates to {path}")
    
    def import_templates(self, path: Path) -> int:
        """Import templates from JSON file."""
        data = json.loads(path.read_text())
        count = 0
        
        for t in data.get("templates", []):
            template = PromptTemplate(
                name=t["name"],
                template=t["template"],
                version=t["version"],
                category=PromptCategory(t["category"]),
                description=t.get("description", ""),
                variables=t.get("variables", [])
            )
            self.register(template, set_active=t.get("is_active", False))
            count += 1
        
        logger.info(f"Imported {count} templates from {path}")
        return count


# Global registry instance
prompt_registry = PromptRegistry()


def get_prompt(name: str, **kwargs) -> str:
    """Convenience function to get and render a prompt."""
    result = prompt_registry.render(name, **kwargs)
    if result is None:
        raise ValueError(f"Prompt template '{name}' not found or missing variables")
    return result


def list_prompts(category: Optional[str] = None) -> List[Dict[str, Any]]:
    """List available prompt templates."""
    cat = PromptCategory(category) if category else None
    return prompt_registry.list_templates(cat)

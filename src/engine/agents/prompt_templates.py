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
            template="""You are a world-class Media Analyst. Write a highly structured, data-driven executive summary for these campaigns. Use a professional, authoritative tone.

OBJECTIVE: {objective}
CONTEXT: {context}

PERFORMANCE DATA:
{metrics}

KEY METRICS STATUS:
{status}

RECOMMENDATIONS GENERATED:
{recommendations}

INSTRUCTIONS:
1. Use markdown headers for structure. Specifically use these headers: **Performance Overview:**, **What Is Working:**, **What Is Not Working:**, **Root Cause Analysis:**, **Priority Actions:**, and **Strategic Recommendations:**.
2. Quantify everything. Use percentages and dollar amounts from the data.
3. Don't be generic. Explain the "So What?" for every data point.
4. Output must be valid JSON matching the format below.

OUTPUT FORMAT:
Return a JSON object with:
{{
    "overview": "A 3-4 sentence comprehensive overview of the portfolio health.",
    "key_takeaways": [
        "**Performance Overview:** [Detailed analysis of core metrics vs goals]",
        "**What Is Working:** [Top performing segments and positive trends]",
        "**What Is Not Working:** [Underperforming areas and wasted spend]",
        "**Root Cause Analysis:** [Explain the 'Why' behind the performance patterns]",
        "**Priority Actions:** [Immediate, high-impact changes to make]",
        "**Strategic Recommendations:** [Longer-term strategic advice based on knowledge base context]"
    ],
    "rag_metadata": {{
        "sources_used": ["Campaign Data", "Knowledge Base"],
        "confidence_score": 0.98
    }}
}}""",
            version="1.1.0",
            category=PromptCategory.REPORT,
            description="Enhanced RAG-enhanced executive summary with detailed structured sections",
            variables=["objective", "context", "metrics", "status", "recommendations"]
        ))
        
        self.register(PromptTemplate(
            name="standard_analysis_narrative",
            template="""Analyze the following campaign performance data as a senior media buyer. 

DATA:
{metrics}

STATUS:
{status}

RECOMMENDATIONS:
{recommendations}

Provide a structured analysis focusing on critical issues and opportunities.
Use markdown headers: **Performance Overview:**, **Key Strengths:**, **Priority Actions:**.
Be concise but highly actionable.""",
            version="1.0.0",
            category=PromptCategory.ANALYSIS,
            description="Standard automated analysis narrative",
            variables=["metrics", "status", "recommendations"]
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

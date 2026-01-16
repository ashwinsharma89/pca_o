"""
Agentic Reasoning Agent for campaign analysis and insights generation.
"""
import json
from typing import List, Dict, Any, Optional
from collections import defaultdict

from openai import AsyncOpenAI
from loguru import logger

from src.platform.models.campaign import (
    Campaign,
    ChannelPerformance,
    CrossChannelInsight,
    Achievement,
    CampaignObjective
)
from src.platform.models.platform import PlatformType, MetricType, NormalizedMetric, PLATFORM_CONFIGS
from src.core.config.settings import settings
from src.core.utils.anthropic_helpers import create_async_anthropic_client

# Import prompt template system for versioned, manageable prompts
try:
    from .prompt_templates import prompt_registry, get_prompt
    PROMPT_TEMPLATES_AVAILABLE = True
except ImportError:
    PROMPT_TEMPLATES_AVAILABLE = False
    logger.warning("Prompt templates not available, using inline prompts")


class ReasoningAgent:
    """Agent for generating insights, detecting achievements, and providing recommendations."""
    
    def __init__(self, model: str = None, provider: str = "openai"):
        """
        Initialize Reasoning Agent.
        
        Args:
            model: LLM model to use
            provider: Model provider ('openai' or 'anthropic')
        """
        self.model = model or settings.default_llm_model
        self.provider = provider
        
        if provider == "openai":
            self.client = AsyncOpenAI(api_key=settings.openai_api_key)
        elif provider == "anthropic":
            self.client = create_async_anthropic_client(settings.anthropic_api_key)
            if not self.client:
                raise ValueError("Failed to initialize Anthropic async client. Check ANTHROPIC_API_KEY or SDK support.")
        else:
            raise ValueError(f"Unsupported provider: {provider}")
        
        logger.info(f"Initialized ReasoningAgent with {provider}/{self.model}")
    
    async def analyze_campaign(self, campaign: Campaign) -> Campaign:
        """
        Perform comprehensive campaign analysis.
        
        Args:
            campaign: Campaign with normalized metrics
            
        Returns:
            Campaign with insights, achievements, and recommendations
        """
        logger.info(f"Analyzing campaign {campaign.campaign_id}")
        
        # Analyze individual channels
        channel_performances = await self._analyze_channels(campaign)
        
        # Cross-channel analysis
        cross_channel_insights = await self._cross_channel_analysis(campaign, channel_performances)
        
        # Detect achievements
        achievements = await self._detect_achievements(campaign, channel_performances)
        
        # Generate recommendations
        recommendations = await self._generate_recommendations(
            campaign,
            channel_performances,
            cross_channel_insights
        )
        
        # Store results
        campaign.insights = {
            "channel_performances": [cp.model_dump() for cp in channel_performances],
            "cross_channel_insights": [cci.model_dump() for cci in cross_channel_insights]
        }
        campaign.achievements = [a.model_dump() for a in achievements]
        campaign.recommendations = recommendations
        
        logger.info(f"Analysis complete: {len(channel_performances)} channels, "
                   f"{len(achievements)} achievements, {len(recommendations)} recommendations")
        
        return campaign
    
    async def _analyze_channels(self, campaign: Campaign) -> List[ChannelPerformance]:
        """Analyze performance of individual channels."""
        channel_performances = []
        
        # Group metrics by platform
        by_platform = defaultdict(list)
        for metric in campaign.normalized_metrics:
            by_platform[metric.platform].append(metric)
        
        for platform, metrics in by_platform.items():
            performance = await self._analyze_single_channel(
                platform,
                metrics,
                campaign.objectives
            )
            channel_performances.append(performance)
        
        # Rank channels by efficiency
        channel_performances = self._rank_channels(channel_performances)
        
        return channel_performances
    
    async def _analyze_single_channel(
        self,
        platform: PlatformType,
        metrics: List[NormalizedMetric],
        objectives: List[CampaignObjective]
    ) -> ChannelPerformance:
        """Analyze a single channel's performance."""
        platform_config = PLATFORM_CONFIGS[platform]
        
        # Extract key metrics
        metrics_dict = {m.metric_type: m.value for m in metrics}
        
        performance = ChannelPerformance(
            platform=platform,
            platform_name=platform_config.display_name,
            total_impressions=metrics_dict.get(MetricType.IMPRESSIONS),
            total_clicks=metrics_dict.get(MetricType.CLICKS),
            total_conversions=metrics_dict.get(MetricType.CONVERSIONS),
            total_spend=metrics_dict.get(MetricType.SPEND),
            ctr=metrics_dict.get(MetricType.CTR),
            cpc=metrics_dict.get(MetricType.CPC),
            cpa=metrics_dict.get(MetricType.CPA),
            roas=metrics_dict.get(MetricType.ROAS),
            conversion_rate=metrics_dict.get(MetricType.CONVERSION_RATE)
        )
        
        # Calculate performance score
        performance.performance_score = self._calculate_performance_score(
            performance,
            objectives
        )
        
        # Generate insights using LLM
        insights = await self._generate_channel_insights(performance, metrics_dict, objectives)
        performance.strengths = insights.get("strengths", [])
        performance.weaknesses = insights.get("weaknesses", [])
        performance.opportunities = insights.get("opportunities", [])
        performance.top_creative = insights.get("top_creative")
        performance.top_audience = insights.get("top_audience")
        
        return performance
    
    def _calculate_performance_score(
        self,
        performance: ChannelPerformance,
        objectives: List[CampaignObjective]
    ) -> float:
        """Calculate overall performance score (0-100)."""
        score = 0.0
        weights = []
        
        # Score based on objectives
        if CampaignObjective.AWARENESS in objectives:
            if performance.total_impressions:
                # Normalize impressions (assume 1M is good)
                score += min(performance.total_impressions / 1_000_000 * 30, 30)
                weights.append(30)
        
        if CampaignObjective.CONSIDERATION in objectives or CampaignObjective.TRAFFIC in objectives:
            if performance.ctr:
                # CTR > 2% is good
                score += min(performance.ctr / 2.0 * 30, 30)
                weights.append(30)
        
        if CampaignObjective.CONVERSION in objectives:
            if performance.roas:
                # ROAS > 3 is good
                score += min(performance.roas / 3.0 * 40, 40)
                weights.append(40)
            elif performance.cpa and performance.total_spend:
                # Lower CPA is better (inverse score)
                avg_cpa = performance.cpa
                if avg_cpa > 0:
                    score += min(100 / avg_cpa * 20, 40)
                    weights.append(40)
        
        # Normalize score
        if weights:
            max_score = sum(weights)
            score = (score / max_score) * 100
        
        return round(score, 1)
    
    async def _generate_channel_insights(
        self,
        performance: ChannelPerformance,
        metrics: Dict[MetricType, float],
        objectives: List[CampaignObjective]
    ) -> Dict[str, Any]:
        """Generate insights for a channel using LLM."""
        prompt = f"""
        Analyze the performance of {performance.platform_name} for a campaign with objectives: {[o.value for o in objectives]}.
        
        Metrics:
        {json.dumps({k.value: v for k, v in metrics.items()}, indent=2)}
        
        Performance Score: {performance.performance_score}/100
        
        Provide analysis in JSON format:
        {{
            "strengths": ["List 2-3 key strengths"],
            "weaknesses": ["List 2-3 areas for improvement"],
            "opportunities": ["List 2-3 optimization opportunities"],
            "top_creative": "Best performing creative type (if inferable)",
            "top_audience": "Best performing audience segment (if inferable)"
        }}
        
        Be specific and actionable. Return ONLY valid JSON.
        """
        
        response = await self._call_llm(prompt)
        
        try:
            return json.loads(response)
        except json.JSONDecodeError:
            logger.error(f"Failed to parse channel insights JSON")
            return {
                "strengths": [],
                "weaknesses": [],
                "opportunities": []
            }
    
    def _rank_channels(self, performances: List[ChannelPerformance]) -> List[ChannelPerformance]:
        """Rank channels by efficiency."""
        # Sort by performance score
        sorted_performances = sorted(
            performances,
            key=lambda p: p.performance_score or 0,
            reverse=True
        )
        
        # Assign ranks
        for i, perf in enumerate(sorted_performances, 1):
            perf.efficiency_rank = i
        
        return sorted_performances
    
    async def _cross_channel_analysis(
        self,
        campaign: Campaign,
        channel_performances: List[ChannelPerformance]
    ) -> List[CrossChannelInsight]:
        """Perform cross-channel analysis."""
        insights = []
        
        # Prepare data for LLM
        channel_data = []
        for cp in channel_performances:
            channel_data.append({
                "platform": cp.platform.value,
                "score": cp.performance_score,
                "spend": cp.total_spend,
                "conversions": cp.total_conversions,
                "roas": cp.roas,
                "cpa": cp.cpa
            })
        
        prompt = f"""
        Analyze cross-channel performance for a campaign with objectives: {[o.value for o in campaign.objectives]}.
        
        Channel Performance:
        {json.dumps(channel_data, indent=2)}
        
        Identify:
        1. Channel synergies (how channels work together)
        2. Attribution patterns (which channels drive conversions)
        3. Budget allocation insights
        4. Audience overlap opportunities
        
        Return JSON array of insights:
        [
            {{
                "insight_type": "synergy|attribution|budget|audience",
                "title": "Brief title",
                "description": "Detailed description",
                "affected_platforms": ["platform1", "platform2"],
                "impact_score": 7.5
            }}
        ]
        
        Return ONLY valid JSON array with 3-5 insights.
        """
        
        response = await self._call_llm(prompt)
        
        try:
            insights_data = json.loads(response)
            for insight_data in insights_data:
                insights.append(CrossChannelInsight(
                    insight_type=insight_data["insight_type"],
                    title=insight_data["title"],
                    description=insight_data["description"],
                    affected_platforms=[PlatformType(p) for p in insight_data["affected_platforms"]],
                    impact_score=insight_data["impact_score"]
                ))
        except (json.JSONDecodeError, ValueError, KeyError) as e:
            logger.error(f"Failed to parse cross-channel insights: {e}")
        
        return insights
    
    async def _detect_achievements(
        self,
        campaign: Campaign,
        channel_performances: List[ChannelPerformance]
    ) -> List[Achievement]:
        """Detect and highlight campaign achievements."""
        achievements = []
        
        # Prepare data
        campaign_data = {
            "objectives": [o.value for o in campaign.objectives],
            "channels": [
                {
                    "platform": cp.platform.value,
                    "score": cp.performance_score,
                    "spend": cp.total_spend,
                    "conversions": cp.total_conversions,
                    "roas": cp.roas,
                    "ctr": cp.ctr
                }
                for cp in channel_performances
            ]
        }
        
        prompt = f"""
        Identify the TOP 5 achievements for this campaign.
        
        Campaign Data:
        {json.dumps(campaign_data, indent=2)}
        
        Look for:
        - Exceptional performance metrics (high ROAS, CTR, etc.)
        - Goal attainment (meeting/exceeding objectives)
        - Efficiency wins (low CPA, high conversion rate)
        - Channel standouts (best performing platform)
        - Audience insights (strong demographic performance)
        
        Return JSON array:
        [
            {{
                "achievement_type": "performance|efficiency|goal_attainment|channel_win",
                "title": "Brief achievement title",
                "description": "Detailed description with numbers",
                "metric_value": 4.2,
                "metric_name": "ROAS",
                "platform": "meta_ads",
                "impact_level": "high|medium|low"
            }}
        ]
        
        Return ONLY valid JSON array with exactly 5 achievements.
        """
        
        response = await self._call_llm(prompt)
        
        try:
            achievements_data = json.loads(response)
            for ach_data in achievements_data:
                achievements.append(Achievement(
                    achievement_type=ach_data["achievement_type"],
                    title=ach_data["title"],
                    description=ach_data["description"],
                    metric_value=ach_data.get("metric_value"),
                    metric_name=ach_data.get("metric_name"),
                    platform=PlatformType(ach_data["platform"]) if ach_data.get("platform") else None,
                    impact_level=ach_data["impact_level"]
                ))
        except (json.JSONDecodeError, ValueError, KeyError) as e:
            logger.error(f"Failed to parse achievements: {e}")
        
        return achievements
    
    async def _generate_recommendations(
        self,
        campaign: Campaign,
        channel_performances: List[ChannelPerformance],
        cross_channel_insights: List[CrossChannelInsight]
    ) -> List[str]:
        """Generate strategic recommendations."""
        # Prepare context
        context = {
            "objectives": [o.value for o in campaign.objectives],
            "channels": [
                {
                    "platform": cp.platform.value,
                    "score": cp.performance_score,
                    "strengths": cp.strengths,
                    "weaknesses": cp.weaknesses,
                    "opportunities": cp.opportunities
                }
                for cp in channel_performances
            ],
            "insights": [
                {
                    "type": cci.insight_type,
                    "title": cci.title
                }
                for cci in cross_channel_insights
            ]
        }
        
        prompt = f"""
        Generate 5-7 strategic recommendations for optimizing this campaign.
        
        Context:
        {json.dumps(context, indent=2)}
        
        Recommendations should be:
        - Specific and actionable
        - Data-driven
        - Prioritized by impact
        - Cover: budget allocation, creative optimization, audience targeting, channel strategy
        
        Return JSON array of strings:
        ["Recommendation 1", "Recommendation 2", ...]
        
        Return ONLY valid JSON array.
        """
        
        response = await self._call_llm(prompt)
        
        try:
            return json.loads(response)
        except json.JSONDecodeError:
            logger.error("Failed to parse recommendations")
            return []
    
    async def _call_llm(self, prompt: str) -> str:
        """Call the LLM API."""
        if self.provider == "openai":
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": "You are an expert digital marketing analyst specializing in campaign performance analysis. Provide data-driven insights and actionable recommendations."
                    },
                    {"role": "user", "content": prompt}
                ],
                temperature=settings.temperature,
                max_tokens=settings.max_tokens
            )
            return response.choices[0].message.content
        
        elif self.provider == "anthropic":
            response = await self.client.messages.create(
                model=self.model,
                max_tokens=settings.max_tokens,
                temperature=settings.temperature,
                system="You are an expert digital marketing analyst specializing in campaign performance analysis. Provide data-driven insights and actionable recommendations.",
                messages=[
                    {"role": "user", "content": prompt}
                ]
            )
            return response.content[0].text

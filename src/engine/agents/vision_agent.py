"""
Vision Agent for extracting data from dashboard snapshots using VLMs.
"""
import base64
import json
from typing import Dict, List, Optional, Any
from pathlib import Path
import asyncio

from openai import AsyncOpenAI
from loguru import logger

from src.platform.models.platform import (
    PlatformType,
    PlatformSnapshot,
    ExtractedMetric,
    ExtractedChart,
    ExtractedTable,
    ChartType,
    MetricType,
    PLATFORM_CONFIGS
)
from src.core.config.settings import settings
from src.core.utils.anthropic_helpers import create_async_anthropic_client


class VisionAgent:
    """Agent for extracting data from dashboard screenshots using Vision Language Models."""
    
    def __init__(
        self,
        model: str = None,
        provider: str = "openai"
    ):
        """
        Initialize Vision Agent.
        
        Args:
            model: VLM model to use (default from settings)
            provider: Model provider ('openai' or 'anthropic')
        """
        self.model = model or settings.default_vlm_model
        self.provider = provider
        
        if provider == "openai":
            self.client = AsyncOpenAI(api_key=settings.openai_api_key)
        elif provider == "anthropic":
            self.client = create_async_anthropic_client(settings.anthropic_api_key)
            if not self.client:
                raise ValueError("Failed to initialize Anthropic async client. Check ANTHROPIC_API_KEY or SDK support.")
        else:
            raise ValueError(f"Unsupported provider: {provider}")
        
        logger.info(f"Initialized VisionAgent with {provider}/{self.model}")
    
    async def analyze_snapshot(
        self,
        snapshot: PlatformSnapshot
    ) -> PlatformSnapshot:
        """
        Analyze a dashboard snapshot and extract all data.
        
        Args:
            snapshot: Platform snapshot to analyze
            
        Returns:
            Updated snapshot with extracted data
        """
        logger.info(f"Analyzing snapshot {snapshot.snapshot_id} from {snapshot.platform}")
        
        try:
            # Load and encode image
            image_data = self._load_image(snapshot.file_path)
            
            # Detect platform if not specified
            if not snapshot.detected_platform:
                snapshot.detected_platform = await self._detect_platform(image_data)
                logger.info(f"Detected platform: {snapshot.detected_platform}")
            
            # Extract metrics
            metrics = await self._extract_metrics(image_data, snapshot.platform)
            snapshot.extracted_metrics = metrics
            logger.info(f"Extracted {len(metrics)} metrics")
            
            # Extract charts
            charts = await self._extract_charts(image_data, snapshot.platform)
            snapshot.extracted_charts = charts
            logger.info(f"Extracted {len(charts)} charts")
            
            # Extract tables
            tables = await self._extract_tables(image_data, snapshot.platform)
            snapshot.extracted_tables = tables
            logger.info(f"Extracted {len(tables)} tables")
            
            # Extract metadata
            metadata = await self._extract_metadata(image_data)
            snapshot.date_range = metadata.get("date_range")
            snapshot.campaign_name = metadata.get("campaign_name")
            
            snapshot.processing_status = "completed"
            snapshot.vision_model_used = f"{self.provider}/{self.model}"
            
        except Exception as e:
            logger.error(f"Error analyzing snapshot {snapshot.snapshot_id}: {e}")
            snapshot.processing_status = "failed"
            snapshot.processing_error = str(e)
        
        return snapshot
    
    async def _detect_platform(self, image_data: str) -> Optional[PlatformType]:
        """Detect the advertising platform from visual branding."""
        prompt = """
        Analyze this dashboard screenshot and identify which advertising platform it's from.
        
        Look for:
        - Logo and branding
        - UI design patterns
        - Color schemes
        - Text and labels
        
        Supported platforms:
        - google_ads: Google Ads (blue/white, Google branding)
        - cm360: Campaign Manager 360 (Google Marketing Platform)
        - dv360: Display & Video 360 (Google Marketing Platform)
        - meta_ads: Meta Ads Manager (Facebook/Instagram, blue branding)
        - snapchat_ads: Snapchat Ads Manager (yellow branding, ghost logo)
        - linkedin_ads: LinkedIn Campaign Manager (blue/white, LinkedIn branding)
        
        Respond with ONLY the platform identifier (e.g., "google_ads") or "unknown" if uncertain.
        """
        
        response = await self._call_vision_model(image_data, prompt)
        platform_str = response.strip().lower()
        
        try:
            return PlatformType(platform_str)
        except ValueError:
            logger.warning(f"Could not detect platform from response: {platform_str}")
            return None
    
    async def _extract_metrics(
        self,
        image_data: str,
        platform: PlatformType
    ) -> List[ExtractedMetric]:
        """Extract numerical metrics from the dashboard."""
        platform_config = PLATFORM_CONFIGS.get(platform)
        expected_metrics = [m.value for m in platform_config.expected_metrics] if platform_config else []
        
        prompt = f"""
        Extract ALL numerical metrics from this {platform.value} dashboard screenshot.
        
        Expected metrics for {platform.value}:
        {', '.join(expected_metrics)}
        
        For each metric, identify:
        1. Metric name (exact text from dashboard)
        2. Numeric value
        3. Unit (%, $, etc.) if present
        4. Currency (USD, EUR, etc.) if it's a monetary value
        
        Return a JSON array of metrics in this format:
        [
            {{
                "metric_name": "Impressions",
                "value": 1250000,
                "unit": null,
                "currency": null,
                "confidence": 0.95
            }},
            {{
                "metric_name": "Total Spend",
                "value": 45000.50,
                "unit": "$",
                "currency": "USD",
                "confidence": 0.98
            }}
        ]
        
        IMPORTANT:
        - Extract ALL visible metrics, not just the expected ones
        - Convert abbreviated numbers (1.2M → 1200000, 45K → 45000)
        - Remove currency symbols and commas from values
        - Set confidence based on clarity (0.0-1.0)
        - Return ONLY valid JSON, no additional text
        """
        
        response = await self._call_vision_model(image_data, prompt)
        
        try:
            metrics_data = json.loads(response)
            metrics = []
            
            for m in metrics_data:
                # Try to map to standard metric type
                metric_type = self._map_metric_type(m["metric_name"], platform)
                
                metrics.append(ExtractedMetric(
                    metric_name=m["metric_name"],
                    metric_type=metric_type,
                    value=float(m["value"]),
                    unit=m.get("unit"),
                    currency=m.get("currency"),
                    confidence=m.get("confidence", 1.0)
                ))
            
            return metrics
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse metrics JSON: {e}")
            logger.debug(f"Response was: {response}")
            return []
    
    async def _extract_charts(
        self,
        image_data: str,
        platform: PlatformType
    ) -> List[ExtractedChart]:
        """Extract charts and their data points from the dashboard."""
        prompt = """
        Identify and extract ALL charts/graphs from this dashboard screenshot.
        
        For each chart, extract:
        1. Chart type (line_chart, bar_chart, pie_chart, area_chart, etc.)
        2. Chart title
        3. Data points (x and y values)
        4. Axis labels
        5. Legend items
        
        Return a JSON array in this format:
        [
            {
                "chart_type": "line_chart",
                "title": "Impressions Over Time",
                "data_points": [
                    {"x": "Jan 1", "y": 125000},
                    {"x": "Jan 2", "y": 130000}
                ],
                "x_axis_label": "Date",
                "y_axis_label": "Impressions",
                "legend": ["Campaign A", "Campaign B"],
                "confidence": 0.9
            }
        ]
        
        IMPORTANT:
        - Extract data points as accurately as possible by reading the chart
        - For bar charts, include all bars
        - For pie charts, include all slices with percentages
        - For line charts, sample key points (don't need every pixel)
        - Return ONLY valid JSON
        """
        
        response = await self._call_vision_model(image_data, prompt)
        
        try:
            charts_data = json.loads(response)
            charts = []
            
            for c in charts_data:
                charts.append(ExtractedChart(
                    chart_type=ChartType(c["chart_type"]),
                    title=c.get("title"),
                    data_points=c.get("data_points", []),
                    x_axis_label=c.get("x_axis_label"),
                    y_axis_label=c.get("y_axis_label"),
                    legend=c.get("legend"),
                    confidence=c.get("confidence", 1.0)
                ))
            
            return charts
            
        except (json.JSONDecodeError, ValueError) as e:
            logger.error(f"Failed to parse charts JSON: {e}")
            return []
    
    async def _extract_tables(
        self,
        image_data: str,
        platform: PlatformType
    ) -> List[ExtractedTable]:
        """Extract tables from the dashboard."""
        prompt = """
        Extract ALL tables from this dashboard screenshot.
        
        For each table, extract:
        1. Column headers
        2. All rows of data
        3. Table title (if present)
        
        Return a JSON array in this format:
        [
            {
                "title": "Campaign Performance",
                "headers": ["Campaign Name", "Impressions", "Clicks", "CTR"],
                "rows": [
                    ["Campaign A", "1,250,000", "25,000", "2.0%"],
                    ["Campaign B", "980,000", "18,500", "1.9%"]
                ],
                "confidence": 0.95
            }
        ]
        
        IMPORTANT:
        - Preserve exact text from cells
        - Include ALL rows visible in the table
        - Return ONLY valid JSON
        """
        
        response = await self._call_vision_model(image_data, prompt)
        
        try:
            tables_data = json.loads(response)
            tables = []
            
            for t in tables_data:
                tables.append(ExtractedTable(
                    title=t.get("title"),
                    headers=t["headers"],
                    rows=t["rows"],
                    confidence=t.get("confidence", 1.0)
                ))
            
            return tables
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse tables JSON: {e}")
            return []
    
    async def _extract_metadata(self, image_data: str) -> Dict[str, Any]:
        """Extract metadata like date range and campaign name."""
        prompt = """
        Extract metadata from this dashboard screenshot:
        
        1. Date range (e.g., "Jan 1 - Jan 31, 2024")
        2. Campaign name (if visible)
        
        Return JSON:
        {
            "date_range": "Jan 1 - Jan 31, 2024",
            "campaign_name": "Q4 Holiday Campaign"
        }
        
        Return ONLY valid JSON. Use null for missing values.
        """
        
        response = await self._call_vision_model(image_data, prompt)
        
        try:
            return json.loads(response)
        except json.JSONDecodeError:
            return {}
    
    async def _call_vision_model(self, image_data: str, prompt: str) -> str:
        """Call the vision model API."""
        if self.provider == "openai":
            return await self._call_openai(image_data, prompt)
        elif self.provider == "anthropic":
            return await self._call_anthropic(image_data, prompt)
    
    async def _call_openai(self, image_data: str, prompt: str) -> str:
        """Call OpenAI GPT-4V."""
        response = await self.client.chat.completions.create(
            model=self.model,
            messages=[
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt},
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:image/jpeg;base64,{image_data}"
                            }
                        }
                    ]
                }
            ],
            max_tokens=settings.max_tokens,
            temperature=settings.temperature
        )
        
        return response.choices[0].message.content
    
    async def _call_anthropic(self, image_data: str, prompt: str) -> str:
        """Call Anthropic Claude with vision."""
        response = await self.client.messages.create(
            model=self.model,
            max_tokens=settings.max_tokens,
            temperature=settings.temperature,
            messages=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image",
                            "source": {
                                "type": "base64",
                                "media_type": "image/jpeg",
                                "data": image_data
                            }
                        },
                        {
                            "type": "text",
                            "text": prompt
                        }
                    ]
                }
            ]
        )
        
        return response.content[0].text
    
    def _load_image(self, file_path: str) -> str:
        """Load and encode image to base64."""
        with open(file_path, "rb") as f:
            return base64.b64encode(f.read()).decode("utf-8")
    
    def _map_metric_type(self, metric_name: str, platform: PlatformType) -> Optional[MetricType]:
        """Map platform-specific metric name to standard MetricType."""
        platform_config = PLATFORM_CONFIGS.get(platform)
        if not platform_config:
            return None
        
        # Check platform-specific mappings
        metric_name_lower = metric_name.lower()
        for key, metric_type in platform_config.metric_mappings.items():
            if key.lower() in metric_name_lower:
                return metric_type
        
        # Try direct match with MetricType enum
        try:
            return MetricType(metric_name_lower.replace(" ", "_"))
        except ValueError:
            return None

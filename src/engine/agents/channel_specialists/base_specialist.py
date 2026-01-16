"""
Base Channel Specialist Agent
Abstract base class for channel-specific analysis agents
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
import pandas as pd
from loguru import logger


class BaseChannelSpecialist(ABC):
    """Base class for channel-specific analysis agents"""
    
    def __init__(self, rag_retriever=None):
        """
        Initialize channel specialist
        
        Args:
            rag_retriever: RAG retriever for knowledge base access
        """
        self.rag = rag_retriever
        self.channel_type = self.__class__.__name__.replace('Agent', '').replace('Channel', '')
        logger.info(f"Initialized {self.channel_type} Channel Specialist")
    
    @abstractmethod
    def analyze(self, campaign_data: pd.DataFrame) -> Dict[str, Any]:
        """
        Perform channel-specific analysis
        
        Args:
            campaign_data: Campaign performance data
            
        Returns:
            Dictionary with channel-specific insights
        """
        pass
    
    @abstractmethod
    def get_benchmarks(self) -> Dict[str, float]:
        """
        Get channel-specific performance benchmarks
        
        Returns:
            Dictionary of benchmark metrics
        """
        pass
    
    def retrieve_knowledge(self, query: str, filters: Optional[Dict[str, Any]] = None) -> str:
        """
        Retrieve relevant knowledge from RAG system
        
        Args:
            query: Search query
            filters: Optional filters for retrieval
            
        Returns:
            Retrieved context string
        """
        if not self.rag:
            logger.warning(f"No RAG retriever available for {self.channel_type}")
            return ""
        
        try:
            # Add channel-specific filter
            if filters is None:
                filters = {}
            filters['channel'] = self.channel_type.lower()
            
            context = self.rag.retrieve(query=query, filters=filters)
            return context
        except Exception as e:
            logger.error(f"Error retrieving knowledge: {e}")
            return ""
    
    def _calculate_metric_health(self, value: float, benchmark: float, 
                                  higher_is_better: bool = True) -> str:
        """
        Calculate health status of a metric vs benchmark
        
        Args:
            value: Actual metric value
            benchmark: Benchmark value
            higher_is_better: Whether higher values are better
            
        Returns:
            Health status: 'excellent', 'good', 'average', 'poor'
        """
        if higher_is_better:
            ratio = value / benchmark if benchmark > 0 else 0
        else:
            ratio = benchmark / value if value > 0 else 0
        
        if ratio >= 1.2:
            return 'excellent'
        elif ratio >= 1.0:
            return 'good'
        elif ratio >= 0.8:
            return 'average'
        else:
            return 'poor'
    
    def _generate_recommendations(self, insights: Dict[str, Any], 
                                   context: str = "") -> List[Dict[str, Any]]:
        """
        Generate actionable recommendations based on insights
        
        Args:
            insights: Analysis insights
            context: Retrieved knowledge context
            
        Returns:
            List of recommendation dictionaries
        """
        recommendations = []
        
        # Subclasses should override this method
        # This is a basic implementation
        for key, value in insights.items():
            if isinstance(value, dict) and 'status' in value:
                if value['status'] in ['poor', 'needs_improvement']:
                    recommendations.append({
                        'area': key,
                        'priority': 'high',
                        'issue': value.get('issue', f"Low performance in {key}"),
                        'recommendation': value.get('recommendation', f"Optimize {key}"),
                        'expected_impact': value.get('impact', 'medium')
                    })
        
        return recommendations
    
    def detect_platform(self, campaign_data: pd.DataFrame) -> str:
        """
        Detect the advertising platform from campaign data
        
        Args:
            campaign_data: Campaign performance data
            
        Returns:
            Platform name
        """
        # Check for platform indicators in column names or data
        columns = [col.lower() for col in campaign_data.columns]
        
        # Platform detection logic
        if any('google' in col or 'adwords' in col for col in columns):
            return 'Google Ads'
        elif any('meta' in col or 'facebook' in col or 'instagram' in col for col in columns):
            return 'Meta'
        elif any('linkedin' in col for col in columns):
            return 'LinkedIn'
        elif any('dv360' in col or 'dbm' in col for col in columns):
            return 'DV360'
        elif any('cm360' in col for col in columns):
            return 'CM360'
        elif any('snapchat' in col for col in columns):
            return 'Snapchat'
        elif any('tiktok' in col for col in columns):
            return 'TikTok'
        else:
            return 'Unknown'

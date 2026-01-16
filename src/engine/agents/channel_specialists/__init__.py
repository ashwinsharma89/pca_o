"""
Channel-Specific Intelligence Layer
Specialized agents for different advertising channels
"""

from .base_specialist import BaseChannelSpecialist
from .search_agent import SearchChannelAgent
from .social_agent import SocialChannelAgent
from .programmatic_agent import ProgrammaticAgent
from .channel_router import ChannelRouter

__all__ = [
    'BaseChannelSpecialist',
    'SearchChannelAgent',
    'SocialChannelAgent',
    'ProgrammaticAgent',
    'ChannelRouter'
]

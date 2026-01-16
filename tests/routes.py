"""
Centralized API Route Definitions for Testing.
This ensures all tests use the correct, updated 7-layer architecture paths.
"""

class APIV1:
    PREFIX = "/api/v1"
    
    # Auth
    AUTH = f"{PREFIX}/auth"
    REGISTER = f"{AUTH}/register"
    LOGIN = f"{AUTH}/login"
    ME = f"{AUTH}/me"
    
    # Campaigns Root
    CAMPAIGNS = f"{PREFIX}/campaigns"
    
    # Analytics / Visualizations (in analytics.py)
    METRICS = f"{CAMPAIGNS}/metrics"
    VISUALIZATIONS = f"{CAMPAIGNS}/visualizations"
    DASHBOARD_STATS = f"{CAMPAIGNS}/dashboard-stats"
    CHART_DATA = f"{CAMPAIGNS}/chart-data"
    FILTERS = f"{CAMPAIGNS}/filters"
    
    # Analysis (in analysis.py)
    ANALYZE_GLOBAL = f"{CAMPAIGNS}/analyze/global"
    FUNNEL_STATS = f"{CAMPAIGNS}/funnel-stats"
    AUDIENCE_STATS = f"{CAMPAIGNS}/audience-stats"
    
    # Chat / Intelligence (in chat.py)
    CHAT = f"{CAMPAIGNS}/chat"
    SUGGESTED_QUESTIONS = f"{CAMPAIGNS}/suggested-questions"
    
    # Ingestion (in ingestion.py)
    UPLOAD = f"{CAMPAIGNS}/upload"
    
    # Reports
    REPORTS = f"{CAMPAIGNS}/reports"

from enum import Enum

class Columns(str, Enum):
    """
    Central source of truth for Column Names.
    Using this prevents typo-based bugs (e.g. 'Spend' vs 'spend').
    """
    # Dimensions
    DATE = "date"
    PLATFORM = "platform"
    CHANNEL = "channel"
    CAMPAIGN = "campaign"
    CAMPAIGN_NAME = "campaign_name"
    AD_GROUP = "ad_group"
    CREATIVE = "creative"
    OBJECTIVE = "objective"
    FUNNEL = "funnel"
    
    # Metrics
    SPEND = "spend"
    IMPRESSIONS = "impressions"
    CLICKS = "clicks"
    CONVERSIONS = "conversions"
    REVENUE = "revenue"
    
    # Metadata
    YEAR = "year"
    MONTH = "month"
    FILE_HASH = "file_hash"
    JOB_ID = "job_id"
    RUN_ID = "run_id" # For testing/tracking

    @classmethod
    def metrics(cls):
        return [cls.SPEND, cls.IMPRESSIONS, cls.CLICKS, cls.CONVERSIONS, cls.REVENUE]

    @classmethod
    def dimensions(cls):
        return [cls.PLATFORM, cls.CHANNEL, cls.CAMPAIGN, cls.FUNNEL, cls.OBJECTIVE]

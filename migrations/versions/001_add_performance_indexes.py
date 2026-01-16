"""
Database migration to add performance indexes.

Adds composite indexes for common query patterns.
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers
revision = '001_add_performance_indexes'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    """Add performance indexes."""
    
    # Campaign table indexes
    op.create_index(
        'idx_campaign_date_platform',
        'campaigns',
        ['date', 'platform']
    )
    
    op.create_index(
        'idx_campaign_date_status',
        'campaigns',
        ['date', 'status']
    )
    
    op.create_index(
        'idx_campaign_platform_objective',
        'campaigns',
        ['platform', 'objective']
    )
    
    op.create_index(
        'idx_campaign_platform_status',
        'campaigns',
        ['platform', 'status']
    )
    
    op.create_index(
        'idx_campaign_start_end_date',
        'campaigns',
        ['start_date', 'end_date']
    )
    
    op.create_index(
        'idx_campaign_date_spend',
        'campaigns',
        ['date', 'spend']
    )
    
    op.create_index(
        'idx_campaign_platform_spend',
        'campaigns',
        ['platform', 'spend']
    )
    
    op.create_index(
        'idx_campaign_platform_channel_date',
        'campaigns',
        ['platform', 'channel', 'date']
    )
    
    op.create_index(
        'idx_campaign_funnel_objective',
        'campaigns',
        ['funnel_stage', 'objective']
    )
    
    # Analysis table indexes
    op.create_index(
        'idx_analysis_campaign_type',
        'analyses',
        ['campaign_id', 'analysis_type']
    )
    
    op.create_index(
        'idx_analysis_created_status',
        'analyses',
        ['created_at', 'status']
    )


def downgrade():
    """Remove performance indexes."""
    
    # Campaign table indexes
    op.drop_index('idx_campaign_date_platform', table_name='campaigns')
    op.drop_index('idx_campaign_date_status', table_name='campaigns')
    op.drop_index('idx_campaign_platform_objective', table_name='campaigns')
    op.drop_index('idx_campaign_platform_status', table_name='campaigns')
    op.drop_index('idx_campaign_start_end_date', table_name='campaigns')
    op.drop_index('idx_campaign_date_spend', table_name='campaigns')
    op.drop_index('idx_campaign_platform_spend', table_name='campaigns')
    op.drop_index('idx_campaign_platform_channel_date', table_name='campaigns')
    op.drop_index('idx_campaign_funnel_objective', table_name='campaigns')
    
    # Analysis table indexes
    op.drop_index('idx_analysis_campaign_type', table_name='analyses')
    op.drop_index('idx_analysis_created_status', table_name='analyses')

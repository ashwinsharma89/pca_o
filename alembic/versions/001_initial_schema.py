"""Initial schema

Revision ID: 001
Revises: 
Create Date: 2024-12-01 18:15:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '001'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create campaigns table
    op.create_table(
        'campaigns',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('campaign_id', sa.String(length=255), nullable=False),
        sa.Column('campaign_name', sa.String(length=500), nullable=False),
        sa.Column('platform', sa.String(length=100), nullable=False),
        sa.Column('channel', sa.String(length=100), nullable=True),
        sa.Column('spend', sa.Float(), nullable=True, default=0.0),
        sa.Column('impressions', sa.Integer(), nullable=True, default=0),
        sa.Column('clicks', sa.Integer(), nullable=True, default=0),
        sa.Column('conversions', sa.Integer(), nullable=True, default=0),
        sa.Column('ctr', sa.Float(), nullable=True, default=0.0),
        sa.Column('cpc', sa.Float(), nullable=True, default=0.0),
        sa.Column('cpa', sa.Float(), nullable=True, default=0.0),
        sa.Column('roas', sa.Float(), nullable=True),
        sa.Column('date', sa.DateTime(), nullable=True),
        sa.Column('funnel_stage', sa.String(length=50), nullable=True),
        sa.Column('audience', sa.String(length=255), nullable=True),
        sa.Column('creative_type', sa.String(length=100), nullable=True),
        sa.Column('placement', sa.String(length=255), nullable=True),
        sa.Column('additional_data', postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('idx_campaign_platform_date', 'campaigns', ['platform', 'date'])
    op.create_index('idx_campaign_channel_date', 'campaigns', ['channel', 'date'])
    op.create_index('idx_campaign_funnel_date', 'campaigns', ['funnel_stage', 'date'])
    op.create_index(op.f('ix_campaigns_campaign_id'), 'campaigns', ['campaign_id'], unique=True)

    # Create analyses table
    op.create_table(
        'analyses',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('analysis_id', sa.String(length=255), nullable=False),
        sa.Column('campaign_id', sa.Integer(), nullable=False),
        sa.Column('analysis_type', sa.String(length=50), nullable=False),
        sa.Column('insights', postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column('recommendations', postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column('metrics', postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column('executive_summary', postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column('status', sa.String(length=50), nullable=True, default='pending'),
        sa.Column('error_message', sa.Text(), nullable=True),
        sa.Column('execution_time', sa.Float(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('completed_at', sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(['campaign_id'], ['campaigns.id'], ),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('idx_analysis_type_created', 'analyses', ['analysis_type', 'created_at'])
    op.create_index('idx_analysis_status', 'analyses', ['status'])
    op.create_index(op.f('ix_analyses_analysis_id'), 'analyses', ['analysis_id'], unique=True)

    # Create query_history table
    op.create_table(
        'query_history',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('query_id', sa.String(length=255), nullable=False),
        sa.Column('user_query', sa.Text(), nullable=False),
        sa.Column('sql_query', sa.Text(), nullable=True),
        sa.Column('query_type', sa.String(length=50), nullable=True),
        sa.Column('result_data', postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column('result_summary', sa.Text(), nullable=True),
        sa.Column('execution_time', sa.Float(), nullable=True),
        sa.Column('tokens_used', sa.Integer(), nullable=True),
        sa.Column('status', sa.String(length=50), nullable=True, default='success'),
        sa.Column('error_message', sa.Text(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('idx_query_created', 'query_history', ['created_at'])
    op.create_index('idx_query_status', 'query_history', ['status'])
    op.create_index(op.f('ix_query_history_query_id'), 'query_history', ['query_id'], unique=True)

    # Create llm_usage table
    op.create_table(
        'llm_usage',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('provider', sa.String(length=50), nullable=False),
        sa.Column('model', sa.String(length=100), nullable=False),
        sa.Column('prompt_tokens', sa.Integer(), nullable=True, default=0),
        sa.Column('completion_tokens', sa.Integer(), nullable=True, default=0),
        sa.Column('total_tokens', sa.Integer(), nullable=True, default=0),
        sa.Column('cost', sa.Float(), nullable=True, default=0.0),
        sa.Column('operation', sa.String(length=100), nullable=True),
        sa.Column('request_id', sa.String(length=255), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('idx_llm_provider_created', 'llm_usage', ['provider', 'created_at'])
    op.create_index('idx_llm_operation_created', 'llm_usage', ['operation', 'created_at'])
    op.create_index(op.f('ix_llm_usage_request_id'), 'llm_usage', ['request_id'])

    # Create campaign_contexts table
    op.create_table(
        'campaign_contexts',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('campaign_id', sa.Integer(), nullable=False),
        sa.Column('business_model', sa.String(length=50), nullable=True),
        sa.Column('industry', sa.String(length=100), nullable=True),
        sa.Column('target_audience', sa.Text(), nullable=True),
        sa.Column('goals', postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column('benchmarks', postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column('historical_metrics', postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(['campaign_id'], ['campaigns.id'], ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('campaign_id')
    )


def downgrade() -> None:
    op.drop_table('campaign_contexts')
    op.drop_table('llm_usage')
    op.drop_table('query_history')
    op.drop_table('analyses')
    op.drop_table('campaigns')

# Database Setup Guide

## Overview

The PCA Agent now uses **PostgreSQL** for persistent storage with **SQLAlchemy ORM** and **dependency injection** for clean architecture.

## Features

✅ **PostgreSQL Persistence**: Replace in-memory storage with production-ready database  
✅ **Connection Pooling**: Efficient database connection management  
✅ **Repository Pattern**: Clean separation of data access logic  
✅ **Dependency Injection**: Centralized dependency management using `dependency-injector`  
✅ **SQLite Fallback**: Easy local development without PostgreSQL  

## Quick Start

### Option 1: SQLite (Development)

For quick local development without PostgreSQL:

```bash
# Set in .env
USE_SQLITE=true

# Initialize database
python scripts/init_database.py
```

### Option 2: PostgreSQL (Production)

#### 1. Install PostgreSQL

**Windows:**
```bash
# Download from https://www.postgresql.org/download/windows/
# Or use chocolatey:
choco install postgresql
```

**Mac:**
```bash
brew install postgresql
brew services start postgresql
```

**Linux:**
```bash
sudo apt-get install postgresql postgresql-contrib
sudo systemctl start postgresql
```

#### 2. Create Database

```bash
# Connect to PostgreSQL
psql -U postgres

# Create database
CREATE DATABASE pca_agent;

# Create user (optional)
CREATE USER pca_user WITH PASSWORD 'your_password';
GRANT ALL PRIVILEGES ON DATABASE pca_agent TO pca_user;

# Exit
\q
```

#### 3. Configure Environment

Update `.env` file:

```env
# PostgreSQL Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=pca_agent
DB_USER=postgres
DB_PASSWORD=your_password_here
USE_SQLITE=false
```

#### 4. Initialize Database

```bash
# Install dependencies
pip install -r requirements.txt

# Initialize database (creates all tables)
python scripts/init_database.py
```

## Database Schema

### Tables

1. **campaigns**: Campaign data with metrics
2. **analyses**: Analysis results (auto, RAG, channel, pattern)
3. **query_history**: Q&A query history
4. **llm_usage**: LLM API usage tracking
5. **campaign_contexts**: Campaign business context

### Indexes

Optimized indexes for common queries:
- Platform + Date
- Channel + Date
- Funnel Stage + Date
- Analysis Type + Created Date

## Architecture

### Dependency Injection

```python
from src.di import init_container, get_container

# Initialize container
container = init_container()

# Get services
campaign_service = container.services.campaign_service()
analytics_expert = container.services.analytics_expert()
```

### Repository Pattern

```python
from src.database import get_db_manager
from src.database.repositories import CampaignRepository

# Get database session
db_manager = get_db_manager()
with db_manager.get_session() as session:
    # Use repository
    campaign_repo = CampaignRepository(session)
    campaigns = campaign_repo.get_all(limit=100)
```

### Service Layer

```python
from src.services import CampaignService
from src.database.repositories import (
    CampaignRepository,
    AnalysisRepository,
    CampaignContextRepository
)

# Create service with repositories
campaign_service = CampaignService(
    campaign_repo=CampaignRepository(session),
    analysis_repo=AnalysisRepository(session),
    context_repo=CampaignContextRepository(session)
)

# Import campaigns from DataFrame
result = campaign_service.import_from_dataframe(df)

# Save analysis results
analysis_id = campaign_service.save_analysis(
    campaign_id="camp_123",
    analysis_type="auto",
    results=analysis_results,
    execution_time=45.2
)
```

## Migration from In-Memory Storage

### Before (In-Memory)

```python
# Old approach
campaigns_db = {}
campaigns_db[campaign_id] = campaign_data
```

### After (PostgreSQL)

```python
# New approach with dependency injection
from src.di import get_container

container = get_container()
campaign_service = container.services.campaign_service()

# Import from DataFrame
campaign_service.import_from_dataframe(df)

# Get campaigns
campaigns = campaign_service.get_campaigns(
    filters={'platform': 'Google'},
    limit=100
)
```

## Connection Pooling

Configured for production workloads:

- **Pool Size**: 5 connections
- **Max Overflow**: 10 additional connections
- **Pool Timeout**: 30 seconds
- **Pool Recycle**: 1 hour (prevents stale connections)

## Performance Considerations

### Bulk Operations

```python
# Efficient bulk insert
campaigns_data = [...]  # List of campaign dicts
campaign_repo.create_bulk(campaigns_data)
```

### Pagination

```python
# Paginated queries
campaigns = campaign_repo.get_all(limit=100, offset=200)
```

### Filtered Queries

```python
# Optimized filtered queries
campaigns = campaign_repo.search({
    'platform': 'Google',
    'start_date': datetime(2024, 1, 1),
    'end_date': datetime(2024, 12, 31),
    'min_spend': 1000
}, limit=100)
```

## Monitoring

### Health Check

```python
from src.database import get_db_manager

db_manager = get_db_manager()
is_healthy = db_manager.health_check()
```

### LLM Usage Tracking

```python
from src.database.repositories import LLMUsageRepository

llm_repo = LLMUsageRepository(session)

# Track usage
llm_repo.create({
    'provider': 'openai',
    'model': 'gpt-4',
    'prompt_tokens': 1000,
    'completion_tokens': 500,
    'total_tokens': 1500,
    'cost': 0.045,
    'operation': 'auto_analysis'
})

# Get usage stats
total_usage = llm_repo.get_total_usage()
provider_usage = llm_repo.get_usage_by_provider('openai')
```

## Troubleshooting

### Connection Issues

```bash
# Test PostgreSQL connection
psql -U postgres -d pca_agent -c "SELECT 1"

# Check if PostgreSQL is running
# Windows
sc query postgresql

# Mac/Linux
brew services list  # Mac
systemctl status postgresql  # Linux
```

### Reset Database

```bash
# Drop and recreate
psql -U postgres -c "DROP DATABASE pca_agent"
psql -U postgres -c "CREATE DATABASE pca_agent"

# Re-initialize
python scripts/init_database.py
```

### View Logs

```python
# Enable SQL logging
import logging
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
```

## Best Practices

1. **Use Context Managers**: Always use `with db_manager.get_session()` for automatic cleanup
2. **Commit Explicitly**: Call `repo.commit()` after write operations
3. **Handle Errors**: Wrap database operations in try-except blocks
4. **Use Repositories**: Don't query models directly, use repository methods
5. **Leverage Indexes**: Design queries to use existing indexes

## Next Steps

- [ ] Add Alembic migrations for schema changes
- [ ] Implement database backup strategy
- [ ] Add read replicas for scaling
- [ ] Implement caching layer (Redis)
- [ ] Add database monitoring (pg_stat_statements)

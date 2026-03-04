# Neo4j to KùzuDB Migration - Key Changes Reference

## Configuration Changes

### Before (Neo4j)
```python
# settings.py
neo4j_uri: str = "neo4j+s://your-instance.databases.neo4j.io"
neo4j_user: str = "neo4j"
neo4j_password: str = "password"
```

### After (KùzuDB)
```python
# settings.py
kuzu_db_path: str = Field(
    default="./kuzu_db",
    description="Path to the KùzuDB database directory"
)
```

---

## Database Connection Changes

### Before (Neo4j)
```python
# connection.py
from neo4j import GraphDatabase

driver = GraphDatabase.driver(uri, auth=(user, password))
session = driver.session()
result = session.run(query)
```

### After (KùzuDB)
```python
# client/connection.py
import kuzu

db = kuzu.Database(db_path)
conn = kuzu.Connection(db)
result = conn.execute(query, params)
```

---

## Schema Definition Changes

### Before (Neo4j)
```cypher
CREATE CONSTRAINT Campaign_id ON (c:Campaign) ASSERT c.id IS UNIQUE
CREATE INDEX idx_campaign_status FOR (c:Campaign) ON c.status
```

### After (KùzuDB)
```cypher
CREATE NODE TABLE IF NOT EXISTS Campaign(
    id STRING,
    name STRING,
    status STRING,
    ...
    PRIMARY KEY(id)
)
```

---

## Loader Changes

### Before (Neo4j)
```python
# neo4j_loader.py (old)
class Neo4jLoader:
    def load_campaigns(self, campaigns):
        query = "UNWIND $batch AS row MERGE (c:Campaign {id: row.id}) SET ..."
        self.session.run(query, batch=campaigns)
```

### After (KùzuDB)
```python
# neo4j_loader.py (new)
class KuzuLoader:
    def load_campaigns(self, campaigns):
        query = "UNWIND $batch AS row MERGE (c:Campaign {id: row.id}) SET ..."
        self._conn.execute_batch(query, campaigns)
```

---

## Import Changes

### Before (Neo4j)
```python
from neo4j import GraphDatabase
from src.kg_rag.etl.loaders.neo4j_loader import Neo4jLoader
from src.kg_rag.client.connection import Neo4jConnection
```

### After (KùzuDB)
```python
import kuzu
from src.kg_rag.etl.loaders.neo4j_loader import KuzuLoader as Neo4jLoader
from src.kg_rag.client.connection import KuzuConnection as Neo4jConnection
```

Backward compatible aliases ensure existing code still works!

---

## Query Generation Changes

### Before (Neo4j)
```python
CYPHER_SYSTEM_PROMPT = """You are a Neo4j Cypher query expert.
Generate queries for a Neo4j database..."""
```

### After (KùzuDB)
```python
CYPHER_SYSTEM_PROMPT = """You are a Cypher query expert for KùzuDB.
Generate queries for a KùzuDB embedded graph database..."""
```

---

## Health Check Changes

### Before (Neo4j)
```json
{
  "status": "healthy",
  "neo4j_connected": true,
  "neo4j_uri": "neo4j+s://..."
}
```

### After (KùzuDB)
```json
{
  "status": "healthy",
  "graph_db_connected": true,
  "db_path": "./kuzu_db"
}
```

---

## ETL Pipeline Changes

### Before (Neo4j)
```python
from src.kg_rag.etl.loaders.neo4j_loader import Neo4jLoader

loader = Neo4jLoader(connection_uri, user, password)
loader.load_campaigns(campaigns)
```

### After (KùzuDB)
```python
from src.kg_rag.etl.loaders.neo4j_loader import KuzuLoader

loader = KuzuLoader()  # Uses config path automatically
loader.load_campaigns(campaigns)
```

---

## Context Builder Changes

### Before (Neo4j)
```python
# _get_node_info()
result = self._conn.execute_query(
    "CALL db.labels() YIELD label RETURN label"
)
```

### After (KùzuDB)
```python
# _get_node_info()
tables = self._conn.show_tables()
node_tables = {t: {"name": t} for t in tables if t in NODE_TYPES}
```

---

## Dependencies Changes

### Before (Neo4j)
```txt
neo4j>=5.0.0
```

### After (KùzuDB)
```txt
kuzu>=0.4.0
```

---

## Environment Configuration Changes

### Before (Neo4j)
```env
KG_RAG_NEO4J_URI=neo4j+s://instance.databases.neo4j.io
KG_RAG_NEO4J_USER=neo4j
KG_RAG_NEO4J_PASSWORD=password
```

### After (KùzuDB)
```env
KG_RAG_KUZU_DB_PATH=./kuzu_db
```

---

## New Files Added

### 1. src/kg_rag/client/connection.py
**Purpose:** Wraps KùzuDB connection
**Key Classes:**
- `KuzuConnection` - Main connection wrapper
- `get_neo4j_connection()` - Backward-compatible singleton
- `Neo4jConnection` - Type alias for compatibility

### 2. debug_kuzu.py
**Purpose:** Interactive debugging utility
**Commands:**
- `show_schema` - List tables
- `show_nodes` - Node counts
- `show_rels` - Relationship counts
- `sample` - Sample data
- `exit` - Exit debugger

### 3. scripts/kg_rag/init_kuzu.py
**Purpose:** Schema initialization
**Actions:**
1. Creates all node tables from KUZU_NODE_DDL
2. Creates all relationship tables from KUZU_REL_DDL
3. Verifies creation
4. Reports table names

---

## Schema DDL Files Added

### nodes.py - KUZU_NODE_DDL
- Channel
- Platform
- Account
- Campaign
- Targeting
- Metric
- EntityGroup
- Creative
- Keyword
- Placement
- Audience

### edges.py - KUZU_REL_DDL
- CATEGORIZES(Channel → Platform)
- HOSTS(Platform → Account)
- OWNS(Account → Campaign)
- BELONGS_TO(Campaign → Platform)
- HAS_TARGETING(Campaign → Targeting)
- HAS_PERFORMANCE(Campaign → Metric)
- CONTAINS(Campaign → EntityGroup)
- HAS_CREATIVE(EntityGroup → Creative)
- HAS_KEYWORD(EntityGroup → Keyword)
- HAS_PLACEMENT(EntityGroup → Placement)
- OVERLAPS_WITH(Audience → Audience)
- SIMILAR_TO(Creative → Creative)

---

## Backward Compatibility Maintained

All these still work without changes:
```python
# Old Neo4j imports - still work!
from src.kg_rag.client.connection import get_neo4j_connection, Neo4jConnection
from src.kg_rag.etl.loaders.neo4j_loader import Neo4jLoader

# They now use KùzuDB under the hood
conn = get_neo4j_connection()  # Returns KuzuConnection
loader = Neo4jLoader()          # Creates KuzuLoader
```

---

## Files Modified Summary

| File | Type | Change |
|------|------|--------|
| settings.py | Config | Added kuzu_db_path |
| nodes.py | Schema | Added KUZU_NODE_DDL |
| edges.py | Schema | Added KUZU_REL_DDL |
| constraints.py | Schema | Replaced (doc-only) |
| indexes.py | Schema | Replaced (doc-only) |
| neo4j_loader.py | ETL | Replaced with KuzuLoader |
| ingestion.py | ETL | Updated import |
| cypher_generator.py | Query | Updated prompt |
| query_router.py | Query | Updated imports/prompt |
| context_builder.py | Context | Updated introspection |
| router.py | API | Updated health check |
| requirements.txt | Deps | Replaced neo4j with kuzu |
| .env | Config | Replaced NEO4J_* with KUZU_* |

---

## New Files Summary

| File | Type | Purpose |
|------|------|---------|
| connection.py | NEW | KùzuDB connection wrapper |
| debug_kuzu.py | NEW | Debug/testing utility |
| init_kuzu.py | NEW | Schema initialization |
| MIGRATION_*.md | NEW | Documentation |
| VERIFY_MIGRATION.sh | NEW | Verification script |

---

## Performance Implications

| Aspect | Neo4j | KùzuDB |
|--------|-------|--------|
| Network Latency | Yes (server) | No (embedded) |
| Connection Overhead | Per-request | In-process |
| Query Parsing | Server-side | In-process |
| Transaction Cost | Server tx | In-process tx |
| Deployment | Docker/Server | File-based |
| Scalability | Horizontal | Vertical |

For the PCA project's known use case (campaign analytics), KùzuDB's embedded
nature provides sufficient performance with zero network overhead.

---

## Quick Reference: What Changed and Why

1. **Connection Method** - No server needed (embedded)
2. **Authentication** - Not needed (local file)
3. **Schema Definition** - PRIMARY KEY in table creation (vs separate constraints)
4. **Indexes** - Only PRIMARY KEY indexes (vs full index support)
5. **Deployment** - Single file-based database (vs server)
6. **Dependencies** - kuzu package (vs neo4j package)
7. **Code Impact** - Minimal (aliases maintain compatibility)

---

**Next Steps:**
1. Install: `pip install -r requirements.txt`
2. Initialize: `python scripts/kg_rag/init_kuzu.py`
3. Verify: `python debug_kuzu.py`
4. Deploy and test

**Questions?** See:
- MIGRATION_COMPLETE.md - Full status
- MIGRATION_NOTES.md - Detailed guide
- FILES_MIGRATION_MANIFEST.txt - Complete file listing

# Neo4j to KùzuDB Migration

This project has been migrated from Neo4j to KùzuDB, an embedded graph database.

## Key Changes

### 1. Database Connection
- **Old:** Neo4j driver with server URI (neo4j+s://, neo4j+ssc://)
- **New:** KùzuDB embedded connection with local database path
- No authentication required (embedded database)
- No network I/O (embedded database)

### 2. Configuration
- **Old:** `KG_RAG_NEO4J_URI`, `KG_RAG_NEO4J_USER`, `KG_RAG_NEO4J_PASSWORD`
- **New:** `KG_RAG_KUZU_DB_PATH` (default: ./kuzu_db)

### 3. Dependencies
- **Removed:** neo4j (Python Neo4j driver)
- **Added:** kuzu (Python KùzuDB driver)

### 4. Cypher Compatibility
- Most Cypher queries work unchanged
- Some Neo4j-specific functions may need adaptation:
  - `date()` vs parameter-based dates
  - Collection operations remain similar
  - `now()` and date functions are supported
  - Aggregations work as expected

### 5. Schema Initialization
New initialization script:
```bash
python scripts/kg_rag/init_kuzu.py
```

This creates all node and relationship tables based on `KUZU_NODE_DDL` and `KUZU_REL_DDL`.

### 6. Files Changed

#### Configuration
- `src/kg_rag/config/settings.py` - Updated for KùzuDB path (no credentials)

#### Schema
- `src/kg_rag/schema/nodes.py` - Added KUZU_NODE_DDL
- `src/kg_rag/schema/edges.py` - Added KUZU_REL_DDL
- `src/kg_rag/schema/constraints.py` - Replaced (constraints via PRIMARY KEY)
- `src/kg_rag/schema/indexes.py` - Replaced (secondary indexes not supported via DDL)

#### Database Connection
- `src/kg_rag/client/connection.py` - New KuzuConnection class

#### ETL/Loaders
- `src/kg_rag/etl/loaders/neo4j_loader.py` - Renamed to KuzuLoader (Neo4jLoader alias for compatibility)
- `src/kg_rag/etl/ingestion.py` - Updated import to use KuzuLoader

#### Query & Context
- `src/kg_rag/query/cypher_generator.py` - Updated LLM prompt
- `src/kg_rag/query/query_router.py` - Updated imports and prompts
- `src/kg_rag/context/context_builder.py` - Updated to use schema introspection

#### API
- `src/kg_rag/api/router.py` - Updated health check endpoint

#### Utilities
- `debug_kuzu.py` - New debug utility
- `scripts/kg_rag/init_kuzu.py` - New schema initialization script

#### Configuration Files
- `requirements.txt` - Replaced neo4j with kuzu
- `.env` - Replaced NEO4J_* with KUZU_DB_PATH
- `docker/docker-compose.neo4j.yml` - To be removed (no Docker needed for embedded DB)

## Migration Checklist

- [ ] Install KùzuDB: `pip install -r requirements.txt`
- [ ] Initialize schema: `python scripts/kg_rag/init_kuzu.py`
- [ ] Run debug utility: `python debug_kuzu.py`
- [ ] Test ETL pipeline
- [ ] Update integration tests
- [ ] Update deployment docs
- [ ] Remove docker-compose.neo4j.yml

## Performance Notes

KùzuDB is optimized for:
- Embedded graph workloads (no network latency)
- In-process query execution
- Moderate dataset sizes (millions of nodes)

For the KG-RAG dataset with millions of campaign/metric records, KùzuDB provides
sufficient performance with the benefit of simplified deployment (no Docker/server).

## Rollback

If needed, switch back to Neo4j by:
1. Reverting `requirements.txt` (add neo4j back)
2. Reverting `connection.py` to Neo4j driver
3. Reverting loader and query classes to use Neo4j Cypher driver

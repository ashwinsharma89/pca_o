#!/bin/bash

# Migration Verification Script
# Checks that all migration files are in place and properly configured

BASE_DIR="/sessions/inspiring-wonderful-ramanujan/mnt/pca_agent_copy"
ERRORS=0
WARNINGS=0

echo "Neo4j to KùzuDB Migration Verification"
echo "========================================"
echo ""

# Check configuration files
echo "Checking configuration files..."
check_file() {
    if [ -f "$1" ]; then
        echo "  OK: $1"
    else
        echo "  ERROR: Missing $1"
        ERRORS=$((ERRORS + 1))
    fi
}

check_file "$BASE_DIR/src/kg_rag/config/settings.py"
check_file "$BASE_DIR/src/kg_rag/schema/nodes.py"
check_file "$BASE_DIR/src/kg_rag/schema/edges.py"
check_file "$BASE_DIR/src/kg_rag/schema/constraints.py"
check_file "$BASE_DIR/src/kg_rag/schema/indexes.py"
check_file "$BASE_DIR/src/kg_rag/client/connection.py"
check_file "$BASE_DIR/src/kg_rag/etl/loaders/neo4j_loader.py"
check_file "$BASE_DIR/src/kg_rag/etl/ingestion.py"
check_file "$BASE_DIR/src/kg_rag/query/cypher_generator.py"
check_file "$BASE_DIR/src/kg_rag/query/query_router.py"
check_file "$BASE_DIR/src/kg_rag/context/context_builder.py"
check_file "$BASE_DIR/src/kg_rag/api/router.py"
check_file "$BASE_DIR/debug_kuzu.py"
check_file "$BASE_DIR/scripts/kg_rag/init_kuzu.py"
check_file "$BASE_DIR/requirements.txt"
check_file "$BASE_DIR/.env"
echo ""

# Check content verification
echo "Checking file content..."

check_content() {
    if grep -q "$2" "$1" 2>/dev/null; then
        echo "  OK: Found '$2' in $1"
    else
        echo "  ERROR: Missing '$2' in $1"
        ERRORS=$((ERRORS + 1))
    fi
}

check_content "$BASE_DIR/src/kg_rag/config/settings.py" "kuzu_db_path"
check_content "$BASE_DIR/src/kg_rag/schema/nodes.py" "KUZU_NODE_DDL"
check_content "$BASE_DIR/src/kg_rag/schema/edges.py" "KUZU_REL_DDL"
check_content "$BASE_DIR/src/kg_rag/client/connection.py" "class KuzuConnection"
check_content "$BASE_DIR/src/kg_rag/etl/loaders/neo4j_loader.py" "class KuzuLoader"
check_content "$BASE_DIR/src/kg_rag/etl/ingestion.py" "KuzuLoader as Neo4jLoader"
check_content "$BASE_DIR/requirements.txt" "kuzu>=0.4.0"
check_content "$BASE_DIR/.env" "KG_RAG_KUZU_DB_PATH"
echo ""

# Check for removed Neo4j references in new code
echo "Checking for Neo4j references (should be absent from new code)..."

check_no_neo4j_refs() {
    if ! grep -q "neo4j_uri\|neo4j_user\|neo4j_password" "$1" 2>/dev/null; then
        echo "  OK: No old Neo4j config in $1"
    else
        echo "  WARNING: Old Neo4j config found in $1"
        WARNINGS=$((WARNINGS + 1))
    fi
}

check_no_neo4j_refs "$BASE_DIR/src/kg_rag/config/settings.py"
check_no_neo4j_refs "$BASE_DIR/.env"
echo ""

# Summary
echo "========================================"
echo "Verification Summary"
echo "========================================"
echo "Errors: $ERRORS"
echo "Warnings: $WARNINGS"
echo ""

if [ $ERRORS -eq 0 ]; then
    echo "SUCCESS: All migration files are in place!"
    exit 0
else
    echo "FAILURE: $ERRORS error(s) found. Please review."
    exit 1
fi

"""
Multi-Table Manager Module
Manages multiple tables and their relationships for complex queries
"""
from typing import Dict, Any, List, Optional, Tuple
import pandas as pd
from loguru import logger
import re


class TableRelationship:
    """Represents a relationship between two tables."""
    
    def __init__(
        self,
        table1: str,
        table2: str,
        join_type: str,
        on_column: str,
        table1_column: Optional[str] = None,
        table2_column: Optional[str] = None,
        auto_detected: bool = False
    ):
        self.table1 = table1
        self.table2 = table2
        self.join_type = join_type.upper()
        self.on_column = on_column
        self.table1_column = table1_column or on_column
        self.table2_column = table2_column or on_column
        self.auto_detected = auto_detected
        self.validated = not auto_detected  # Manual relationships are pre-validated
    
    def to_sql(self, alias1: str = None, alias2: str = None) -> str:
        """Generate SQL JOIN clause."""
        t1 = alias1 or self.table1
        t2 = alias2 or self.table2
        return f"{self.join_type} JOIN {self.table2} {t2} ON {t1}.{self.table1_column} = {t2}.{self.table2_column}"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'table1': self.table1,
            'table2': self.table2,
            'join_type': self.join_type,
            'on_column': self.on_column,
            'table1_column': self.table1_column,
            'table2_column': self.table2_column,
            'auto_detected': self.auto_detected,
            'validated': self.validated
        }


class MultiTableManager:
    """Manages multiple tables and their relationships."""
    
    def __init__(self, conn):
        """
        Initialize the multi-table manager.
        
        Args:
            conn: Database connection (DuckDB)
        """
        self.conn = conn
        self.tables: Dict[str, Dict[str, Any]] = {}
        self.relationships: List[TableRelationship] = []
        self.primary_keys: Dict[str, str] = {}
    
    def register_table(
        self,
        name: str,
        df: pd.DataFrame,
        primary_key: Optional[str] = None,
        description: Optional[str] = None,
        skip_db_registration: bool = False
    ):
        """
        Register a table with the manager.
        
        Args:
            name: Table name
            df: DataFrame to register
            primary_key: Primary key column name
            description: Optional table description
            skip_db_registration: If True, only stores metadata without registering with DuckDB
        """
        # Register with DuckDB if not skipping
        if not skip_db_registration:
            self.conn.register(name, df)
        
        # Store metadata
        self.tables[name] = {
            'name': name,
            'columns': df.columns.tolist(),
            'dtypes': df.dtypes.to_dict(),
            'row_count': len(df),
            'sample_data': df.head(3).to_dict('records'),
            'description': description or f"{name} table"
        }
        
        if primary_key:
            self.primary_keys[name] = primary_key
        
        db_status = " (metadata only)" if skip_db_registration else ""
        logger.info(f"Registered table '{name}'{db_status} with {len(df)} rows and {len(df.columns)} columns")
    
    def auto_detect_relationships(self) -> List[TableRelationship]:
        """
        Auto-detect relationships between tables based on column names.
        
        Returns:
            List of detected relationships (unvalidated)
        """
        detected = []
        table_names = list(self.tables.keys())
        
        # Common foreign key patterns
        fk_patterns = [
            (r'(\w+)_id$', r'\1'),  # product_id -> product
            (r'(\w+)id$', r'\1'),   # productid -> product
            (r'(\w+)_key$', r'\1'), # customer_key -> customer
        ]
        
        for i, table1 in enumerate(table_names):
            for table2 in table_names[i+1:]:
                # Get columns for both tables
                cols1 = set(self.tables[table1]['columns'])
                cols2 = set(self.tables[table2]['columns'])
                
                # Check for exact column name matches
                common_cols = cols1.intersection(cols2)
                for col in common_cols:
                    # Skip generic columns
                    if col.lower() in ['id', 'date', 'created_at', 'updated_at']:
                        continue
                    
                    # Likely a join key
                    rel = TableRelationship(
                        table1=table1,
                        table2=table2,
                        join_type='LEFT',  # Default to LEFT JOIN
                        on_column=col,
                        auto_detected=True
                    )
                    detected.append(rel)
                    logger.info(f"Auto-detected relationship: {table1}.{col} = {table2}.{col}")
                
                # Check for foreign key patterns
                for col1 in cols1:
                    for pattern, replacement in fk_patterns:
                        match = re.match(pattern, col1, re.IGNORECASE)
                        if match:
                            referenced_table = match.group(1).lower()
                            # Check if referenced table exists
                            if referenced_table in [t.lower() for t in table_names]:
                                # Find the actual table name (case-sensitive)
                                actual_table = next(t for t in table_names if t.lower() == referenced_table)
                                # Assume primary key is 'id' or table_name + '_id'
                                pk_candidates = ['id', f'{actual_table}_id', f'{actual_table}id']
                                for pk in pk_candidates:
                                    if pk in cols2 or pk.lower() in [c.lower() for c in cols2]:
                                        rel = TableRelationship(
                                            table1=table1,
                                            table2=actual_table,
                                            join_type='LEFT',
                                            on_column=col1,
                                            table1_column=col1,
                                            table2_column=pk,
                                            auto_detected=True
                                        )
                                        detected.append(rel)
                                        logger.info(f"Auto-detected FK: {table1}.{col1} -> {actual_table}.{pk}")
                                        break
        
        return detected
    
    def define_relationship(
        self,
        table1: str,
        table2: str,
        join_type: str = 'LEFT',
        on_column: Optional[str] = None,
        table1_column: Optional[str] = None,
        table2_column: Optional[str] = None
    ) -> TableRelationship:
        """
        Manually define a relationship between tables.
        
        Args:
            table1: First table name
            table2: Second table name
            join_type: JOIN type (INNER, LEFT, RIGHT, FULL)
            on_column: Column name if same in both tables
            table1_column: Column name in table1
            table2_column: Column name in table2
            
        Returns:
            TableRelationship object
        """
        if table1 not in self.tables:
            raise ValueError(f"Table '{table1}' not registered")
        if table2 not in self.tables:
            raise ValueError(f"Table '{table2}' not registered")
        
        rel = TableRelationship(
            table1=table1,
            table2=table2,
            join_type=join_type,
            on_column=on_column or table1_column,
            table1_column=table1_column,
            table2_column=table2_column,
            auto_detected=False
        )
        
        self.relationships.append(rel)
        logger.info(f"Defined relationship: {table1} {join_type} JOIN {table2}")
        
        return rel
    
    def validate_relationship(self, relationship: TableRelationship) -> Tuple[bool, str]:
        """
        Validate a relationship by checking data integrity.
        
        Args:
            relationship: TableRelationship to validate
            
        Returns:
            Tuple of (is_valid, message)
        """
        try:
            # Check if columns exist
            t1_cols = self.tables[relationship.table1]['columns']
            t2_cols = self.tables[relationship.table2]['columns']
            
            if relationship.table1_column not in t1_cols:
                return False, f"Column '{relationship.table1_column}' not found in table '{relationship.table1}'"
            
            if relationship.table2_column not in t2_cols:
                return False, f"Column '{relationship.table2_column}' not found in table '{relationship.table2}'"
            
            # Test the join
            test_query = f"""
                SELECT COUNT(*) as count
                FROM {relationship.table1} t1
                {relationship.join_type} JOIN {relationship.table2} t2
                ON t1.{relationship.table1_column} = t2.{relationship.table2_column}
            """  # nosec B608
            
            result = self.conn.execute(test_query).fetchdf()
            count = result['count'].iloc[0]
            
            # Check for orphaned records (for INNER JOIN)
            if relationship.join_type == 'INNER':
                t1_count = self.tables[relationship.table1]['row_count']
                if count < t1_count * 0.9:  # More than 10% data loss
                    return False, f"INNER JOIN would lose {t1_count - count} rows ({((t1_count - count)/t1_count)*100:.1f}%). Consider LEFT JOIN."
            
            relationship.validated = True
            return True, f"Valid relationship. Join produces {count} rows."
            
        except Exception as e:
            return False, f"Validation error: {str(e)}"
    
    def get_schema_with_relationships(self) -> str:
        """
        Get enhanced schema description including relationships.
        
        Returns:
            Formatted schema description with relationships
        """
        lines = ["# Database Schema\n"]
        
        # List all tables
        for table_name, table_info in self.tables.items():
            lines.append(f"## Table: {table_name}")
            if table_name in self.primary_keys:
                lines.append(f"Primary Key: {self.primary_keys[table_name]}")
            lines.append(f"Rows: {table_info['row_count']}")
            lines.append("Columns:")
            for col in table_info['columns']:
                dtype = table_info['dtypes'].get(col)
                lines.append(f"  - {col} ({dtype})")
            lines.append("")
        
        # List relationships
        if self.relationships:
            lines.append("## Table Relationships\n")
            for rel in self.relationships:
                status = "✓ Validated" if rel.validated else "⚠ Auto-detected (unvalidated)"
                lines.append(
                    f"- {rel.table1}.{rel.table1_column} {rel.join_type} JOIN "
                    f"{rel.table2}.{rel.table2_column} [{status}]"
                )
        
        return "\n".join(lines)
    
    def get_join_hints_for_llm(self) -> str:
        """
        Generate join hints for LLM prompt.
        
        Returns:
            Formatted join hints
        """
        if not self.relationships:
            return "No table relationships defined. Single table queries only."
        
        hints = ["## Available Table Joins:\n"]
        
        for rel in self.relationships:
            example = f"""
Example: To join {rel.table1} and {rel.table2}:
SELECT c.*, p.*
FROM {rel.table1} c
{rel.to_sql('c', 'p')}
"""  # nosec B608
            hints.append(example)
        
        hints.append("\n## Multi-Table Query Patterns:")
        hints.append("- Use table aliases (c, p, etc.) for clarity")
        hints.append("- Qualify all column names with table alias (c.campaign_name)")
        hints.append("- Use LEFT JOIN to preserve all records from main table")
        hints.append("- Use INNER JOIN only when you need matching records from both tables")
        
        return "\n".join(hints)
    
    def suggest_joins_for_question(self, question: str) -> List[TableRelationship]:
        """
        Suggest relevant joins based on question content.
        
        Args:
            question: Natural language question
            
        Returns:
            List of relevant relationships
        """
        relevant = []
        question_lower = question.lower()
        
        for rel in self.relationships:
            # Check if question mentions either table
            if rel.table1.lower() in question_lower or rel.table2.lower() in question_lower:
                relevant.append(rel)
        
        return relevant
    
    def get_all_relationships(self, validated_only: bool = False) -> List[Dict[str, Any]]:
        """
        Get all relationships as dictionaries.
        
        Args:
            validated_only: If True, return only validated relationships
            
        Returns:
            List of relationship dictionaries
        """
        rels = self.relationships
        if validated_only:
            rels = [r for r in rels if r.validated]
        
        return [r.to_dict() for r in rels]


def create_multi_table_manager(conn) -> MultiTableManager:
    """
    Factory function to create a MultiTableManager instance.
    
    Args:
        conn: Database connection
        
    Returns:
        MultiTableManager instance
    """
    return MultiTableManager(conn)

import re
import sqlparse
from loguru import logger
from typing import List, Dict, Set, Tuple, Optional, Any
from .column_resolver import ColumnResolver

class SQLGuardrails:
    """
    Enterprise-grade SQL validation and auto-fixer.
    Prevents common BinderErrors by ensuring column/alias consistency.
    """
    
    def __init__(self, column_resolver: ColumnResolver):
        self.column_resolver = column_resolver
        self.sql_keywords = {
            'SELECT', 'FROM', 'WHERE', 'GROUP', 'BY', 'ORDER', 'LIMIT', 'JOIN', 'ON', 'AS', 'AND', 'OR',
            'IN', 'IS', 'NULL', 'NOT', 'SUM', 'AVG', 'MIN', 'MAX', 'COUNT', 'ROUND', 'CASE', 'WHEN',
            'THEN', 'ELSE', 'END', 'WITH', 'DISTINCT', 'OVER', 'PARTITION', 'LAG', 'LEAD', 'RANK',
            'INTERVAL', 'DAY', 'WEEK', 'MONTH', 'YEAR', 'DESC', 'ASC', 'NULLS', 'FIRST', 'LAST', 'EXTRACT'
        }
        self.pii_blacklist = {
            r'email', r'phone', r'address', r'ssn', r'password', r'credit_card', 
            r'birth', r'dob', r'social_security', r'customer_name'
        }

    def check_security(self, sql_query: str) -> Tuple[bool, Optional[str]]:
        """
        Performs security and complexity analysis on the query.
        Returns (is_safe, error_message).
        """
        # 1. PII Check
        for pattern in self.pii_blacklist:
            if re.search(pattern, sql_query, re.IGNORECASE):
                return False, f"Query rejected: Potential PII access detected ('{pattern}')"

        # 2. Complexity Analysis
        # Count JOINS
        joins = len(re.findall(r'\bJOIN\b', sql_query, re.IGNORECASE))
        if joins > 4:
            return False, f"Query rejected: Too many JOINs ({joins} > limit 4)"

        # Count CTEs
        ctes = len(re.findall(r'\bAS\s*\(', sql_query, re.IGNORECASE))
        if ctes > 6:
            return False, f"Query rejected: Too many CTEs ({ctes} > limit 6)"

        # Block destructive commands (DML/DDL) even though connection might be RO
        destructive = ['DROP', 'TRUNCATE', 'DELETE', 'UPDATE', 'INSERT', 'ALTER', 'CREATE']
        for cmd in destructive:
            if re.search(r'\b' + cmd + r'\b', sql_query, re.IGNORECASE):
                return False, f"Query rejected: Forbidden command '{cmd}'"

        return True, None

    def validate_and_fix(self, sql_query: str, schema_info: Dict[str, Any]) -> Tuple[str, List[str]]:
        """
        Validates the SQL query and attempts to fix common issues.
        Returns the (fixed_sql, list_of_fixes).
        """
        fixes_applied = []
        columns = schema_info.get('columns', [])
        
        # 1. Protect against zero-length identifiers (Parser Error)
        if '""' in sql_query:
            sql_query = sql_query.replace('""', '"empty_identifier"')
            fixes_applied.append("Replaced zero-length identifier")

        # 2. Extract CTEs and their projected columns
        cte_info = self._extract_cte_projections(sql_query)
        cte_names = set(cte_info.keys())
        
        # 3. Detect aliases defined in the query (including CTE names)
        defined_aliases = self._detect_defined_aliases(sql_query)
        defined_aliases.update(cte_names)
        
        # Add protected functions/keywords and common marketing aliases
        protected_refs = {
            'NULLIF', 'COALESCE', 'GREATEST', 'LEAST', 'DATE_TRUNC', 'DATE_DIFF',
            'ROAS', 'CPA', 'CTR', 'CPC', 'CPM', 'ROI', 'max_date', 'min_date'
        }
        defined_aliases.update(protected_refs)
        
        # 4. Resolve potential column references
        # We look for words that aren't keywords, aren't in quotes, and aren't in schema
        potential_refs = re.findall(r'\b[a-zA-Z_]\w*\b', sql_query)
        
        # Filter out keywords and already correct columns
        potential_refs = [r for r in potential_refs if r.upper() not in self.sql_keywords]
        
        # Process each potential reference
        # Note: This regex-based approach is a heuristic; it needs to be careful not to 
        # break aliases in SELECT p.Something
        
        unique_refs = set(potential_refs)
        for ref_name in unique_refs:
            # Skip if it's already a valid column or a defined alias
            if ref_name in columns or ref_name in defined_aliases:
                continue
            
            # Skip if it's a projection from a CTE
            if any(ref_name in proj for proj in cte_info.values()):
                continue
                
            # Skip very short identifiers (aliases)
            if len(ref_name) <= 2:
                continue
                
            # Try to resolve
            resolved_col = self.column_resolver.resolve(ref_name)
            if resolved_col and resolved_col != ref_name:
                # CONTEXT CHECK: Don't resolve if it follows INTERVAL or is a known unit
                if ref_name.upper() in ['DAY', 'WEEK', 'MONTH', 'YEAR', 'HOUR', 'MINUTE', 'SECOND']:
                    # Look for preceding INTERVAL
                    pattern_check = r'INTERVAL\s+\d+\s+' + re.escape(ref_name)
                    if re.search(pattern_check, sql_query, re.IGNORECASE):
                        continue

                # Replace with quoted version
                pattern = r'(?<![".\w])' + re.escape(ref_name) + r'(?![".\w])'
                sql_query = re.sub(pattern, f'"{resolved_col}"', sql_query)
                fixes_applied.append(f"Resolved: '{ref_name}' → '{resolved_col}'")

        # 5. Case Correction (Ensure exact case for schema columns)
        for col in columns:
            if col in sql_query:
                continue # Already exact match
                
            # Look for case-insensitive match not in quotes
            # Pattern: \bcolumn_name\b ignoring case
            pattern = r'(?<!["])' + re.escape(col) + r'(?!["])'
            count = len(re.findall(pattern, sql_query, re.IGNORECASE))
            if count > 0:
                sql_query = re.sub(pattern, f'"{col}"', sql_query, flags=re.IGNORECASE)
                fixes_applied.append(f"Case-corrected: '{col}' ({count}x)")

        return sql_query, fixes_applied

    def _extract_cte_projections(self, sql_query: str) -> Dict[str, Set[str]]:
        """
        Extract CTE names and the columns they project using a robust 
        parentheses-aware parser.
        """
        ctes = {}
        
        # 1. Normalize query and find the initial WITH
        normalized = sql_query.strip()
        with_match = re.search(r'\bWITH\b', normalized, re.IGNORECASE)
        if not with_match:
            return ctes
            
        # Start scanning after WITH
        pos = with_match.end()
        query_len = len(normalized)
        
        while pos < query_len:
            # Look for [name] AS (
            match = re.search(r'([a-zA-Z_]\w*)\s+AS\s*\(', normalized[pos:], re.IGNORECASE)
            if not match:
                break
                
            cte_name = match.group(1)
            # Find the balanced closing parenthesis
            start_paren = pos + match.end() - 1
            end_paren = self._find_balanced_parenthesis(normalized, start_paren)
            
            if end_paren == -1:
                break
                
            # Extract content and find projections
            content = normalized[start_paren+1:end_paren]
            projected = set()
            as_matches = re.findall(r'\bAS\s+["\']?([a-zA-Z_]\w*)["\']?', content, re.IGNORECASE)
            for m in as_matches:
                projected.add(m)
            
            ctes[cte_name] = projected
            
            # Move position to after the CTE block
            pos = end_paren + 1
            # Look for a comma to continue, or stop if it's the main SELECT
            after_match = re.match(r'\s*,', normalized[pos:], re.IGNORECASE)
            if after_match:
                pos += after_match.end()
            else:
                break
                
        return ctes

    def _find_balanced_parenthesis(self, text: str, start_index: int) -> int:
        """Find the index of the balanced closing parenthesis for the one at start_index."""
        if text[start_index] != '(':
            return -1
            
        depth = 0
        for i in range(start_index, len(text)):
            if text[i] == '(':
                depth += 1
            elif text[i] == ')':
                depth -= 1
                if depth == 0:
                    return i
        return -1

    def _detect_defined_aliases(self, sql_query: str) -> Set[str]:
        """Detect all table and column aliases defined in the query."""
        aliases = set()
        
        # Column aliases: SELECT col AS alias OR SELECT col alias
        # Look for [identifier] [alias] in SELECT clauses (simplified heuristic)
        # Matches: SUM(x) AS "alias", SUM(x) alias, col "alias", col alias
        # We look for words following a comma or SELECT, preceded by optional AS
        col_aliases = re.findall(r'(?:SELECT|,)\s+.*?\s+(?:AS\s+)?["\']?([a-zA-Z_]\w*)["\']?(?=\s*(?:,|$|FROM|GROUP|ORDER|LIMIT))', sql_query, re.IGNORECASE | re.DOTALL)
        for a in col_aliases:
            if a.upper() not in self.sql_keywords:
                aliases.add(a)
        
        # Second pass for standard AS aliases which are reliable
        as_aliases = re.findall(r'\bAS\s+["\']?([a-zA-Z_]\w*)["\']?', sql_query, re.IGNORECASE)
        for a in as_aliases:
            aliases.add(a)
            
        # Table aliases: FROM table AS alias or FROM table alias
        # This is trickier. Look for FROM/JOIN [table] [alias]
        from_join_pattern = r'\b(?:FROM|JOIN)\s+(?:\"[^\"]+\"|[a-zA-Z_]\w*)\s+(?:AS\s+)?(?:\"([^\"]+)\"|([a-zA-Z_]\w*))'
        table_aliases = re.findall(from_join_pattern, sql_query, re.IGNORECASE)
        for match in table_aliases:
            alias = match[0] or match[1]
            if alias and alias.upper() not in self.sql_keywords:
                aliases.add(alias)
                
        return aliases

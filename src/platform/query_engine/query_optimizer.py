"""
Query Optimizer Module
Analyzes query plans using EXPLAIN and suggests optimizations
"""
from typing import Dict, Any, List, Optional, Tuple
from loguru import logger
import re
import time


class QueryPlan:
    """Represents a query execution plan."""
    
    def __init__(self, raw_plan: str, execution_time: float = 0.0):
        self.raw_plan = raw_plan
        self.execution_time = execution_time
        self.has_seq_scan = 'SEQ_SCAN' in raw_plan.upper() or 'SEQUENTIAL SCAN' in raw_plan.upper()
        self.has_index_scan = 'INDEX' in raw_plan.upper()
        self.estimated_rows = self._extract_estimated_rows()
        self.cost = self._extract_cost()
    
    def _extract_estimated_rows(self) -> int:
        """Extract estimated row count from plan."""
        match = re.search(r'estimated.*?(\d+)\s+rows', self.raw_plan, re.IGNORECASE)
        if match:
            return int(match.group(1))
        return 0
    
    def _extract_cost(self) -> float:
        """Extract query cost from plan."""
        match = re.search(r'cost[:\s]+(\d+\.?\d*)', self.raw_plan, re.IGNORECASE)
        if match:
            return float(match.group(1))
        return 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert plan to dictionary."""
        return {
            'raw_plan': self.raw_plan,
            'execution_time': self.execution_time,
            'has_seq_scan': self.has_seq_scan,
            'has_index_scan': self.has_index_scan,
            'estimated_rows': self.estimated_rows,
            'cost': self.cost
        }


class QueryOptimizer:
    """Analyzes and optimizes SQL queries using EXPLAIN."""
    
    def __init__(self, conn):
        """
        Initialize the query optimizer.
        
        Args:
            conn: Database connection (DuckDB)
        """
        self.conn = conn
        self.optimization_history: List[Dict[str, Any]] = []
    
    def analyze_query(self, sql_query: str) -> QueryPlan:
        """
        Analyze query execution plan using EXPLAIN.
        
        Args:
            sql_query: SQL query to analyze
            
        Returns:
            QueryPlan object with analysis results
        """
        try:
            # Get execution plan
            explain_query = f"EXPLAIN {sql_query}"
            plan_result = self.conn.execute(explain_query).fetchdf()
            
            # DuckDB returns plan as DataFrame with columns
            if not plan_result.empty:
                # Combine all plan rows into single string
                plan_text = '\n'.join(plan_result.iloc[:, 0].astype(str).tolist())
            else:
                plan_text = "No plan available"
            
            # Measure actual execution time
            start_time = time.time()
            self.conn.execute(sql_query).fetchdf()
            execution_time = time.time() - start_time
            
            plan = QueryPlan(plan_text, execution_time)
            logger.info(f"Query plan analyzed: {plan.cost:.2f} cost, {execution_time:.3f}s execution")
            
            return plan
            
        except Exception as e:
            logger.error(f"Error analyzing query plan: {e}")
            return QueryPlan(f"Error: {str(e)}", 0.0)
    
    def suggest_optimizations(self, plan: QueryPlan, sql_query: str) -> List[str]:
        """
        Suggest query optimizations based on execution plan.
        
        Args:
            plan: QueryPlan from analyze_query
            sql_query: Original SQL query
            
        Returns:
            List of optimization suggestions
        """
        suggestions = []
        
        # Check for sequential scans
        if plan.has_seq_scan and plan.estimated_rows > 1000:
            suggestions.append(
                "⚠️ Sequential scan detected on large table. Consider:\n"
                "  - Adding WHERE clause to filter rows early\n"
                "  - Creating an index on frequently filtered columns\n"
                "  - Using LIMIT to reduce result set"
            )
        
        # Check for missing indexes
        if not plan.has_index_scan and 'WHERE' in sql_query.upper():
            where_match = re.search(r'WHERE\s+(\w+)', sql_query, re.IGNORECASE)
            if where_match:
                column = where_match.group(1)
                suggestions.append(
                    f"💡 Consider creating an index on '{column}' column for faster filtering"
                )
        
        # Check for SELECT *
        if re.search(r'SELECT\s+\*', sql_query, re.IGNORECASE):
            suggestions.append(
                "📊 Avoid SELECT * in production. Specify only needed columns to reduce data transfer"
            )
        
        # Check for complex aggregations without CTEs
        if sql_query.count('SUM(') + sql_query.count('AVG(') + sql_query.count('COUNT(') > 5:
            if 'WITH' not in sql_query.upper():
                suggestions.append(
                    "🔧 Complex aggregations detected. Consider using CTEs (WITH clause) for better readability"
                )
        
        # Check execution time
        if plan.execution_time > 1.0:
            suggestions.append(
                f"⏱️ Slow query ({plan.execution_time:.2f}s). Consider:\n"
                "  - Adding indexes on join/filter columns\n"
                "  - Reducing result set with LIMIT\n"
                "  - Breaking into smaller queries with CTEs"
            )
        elif plan.execution_time > 0.5:
            suggestions.append(
                f"⚡ Moderate execution time ({plan.execution_time:.2f}s). Monitor for performance degradation"
            )
        
        # Check for Cartesian products (missing JOIN conditions)
        if 'FROM' in sql_query.upper() and sql_query.count(',') > 0:
            if 'WHERE' not in sql_query.upper() and 'JOIN' not in sql_query.upper():
                suggestions.append(
                    "❌ Possible Cartesian product detected. Ensure proper JOIN conditions"
                )
        
        # Check for subqueries that could be CTEs
        subquery_count = sql_query.count('SELECT') - 1  # Subtract main SELECT
        if subquery_count > 2 and 'WITH' not in sql_query.upper():
            suggestions.append(
                "🔄 Multiple subqueries detected. Refactor using CTEs for better performance and readability"
            )
        
        if not suggestions:
            suggestions.append("✅ Query looks well-optimized!")
        
        return suggestions
    
    def optimize_query(self, sql_query: str) -> Tuple[str, List[str]]:
        """
        Attempt to automatically optimize a query.
        
        Args:
            sql_query: Original SQL query
            
        Returns:
            Tuple of (optimized_query, list_of_changes_made)
        """
        optimized = sql_query
        changes = []
        
        # Replace SELECT * with specific columns (if we can detect them)
        # This is conservative - only do it for simple queries
        if re.search(r'SELECT\s+\*\s+FROM\s+(\w+)', optimized, re.IGNORECASE):
            # For now, we'll just log this as a suggestion
            # Actual column replacement would require schema knowledge
            changes.append("Suggested: Replace SELECT * with specific columns")
        
        # Add LIMIT if missing and no aggregation
        if 'LIMIT' not in optimized.upper() and 'GROUP BY' not in optimized.upper():
            if 'ORDER BY' in optimized.upper():
                optimized = optimized.rstrip(';') + ' LIMIT 1000;'
                changes.append("Added LIMIT 1000 to prevent large result sets")
            else:
                optimized = optimized.rstrip(';') + ' ORDER BY 1 LIMIT 1000;'
                changes.append("Added ORDER BY and LIMIT to prevent large result sets")
        
        # Convert subqueries to CTEs (basic pattern)
        # This is a simple heuristic - real optimization would be more sophisticated
        if optimized.count('SELECT') > 2 and 'WITH' not in optimized.upper():
            changes.append("Suggested: Consider converting subqueries to CTEs")
        
        return optimized, changes
    
    def get_query_stats(self, sql_query: str) -> Dict[str, Any]:
        """
        Get comprehensive query statistics.
        
        Args:
            sql_query: SQL query to analyze
            
        Returns:
            Dictionary with query statistics
        """
        plan = self.analyze_query(sql_query)
        suggestions = self.suggest_optimizations(plan, sql_query)
        
        stats = {
            'execution_time': plan.execution_time,
            'estimated_rows': plan.estimated_rows,
            'cost': plan.cost,
            'has_seq_scan': plan.has_seq_scan,
            'has_index_scan': plan.has_index_scan,
            'optimization_suggestions': suggestions,
            'plan': plan.raw_plan
        }
        
        # Store in history
        self.optimization_history.append({
            'timestamp': time.time(),
            'query': sql_query,
            'stats': stats
        })
        
        return stats
    
    def get_optimization_report(self) -> str:
        """
        Generate a summary report of all optimizations.
        
        Returns:
            Formatted optimization report
        """
        if not self.optimization_history:
            return "No queries analyzed yet."
        
        report = ["# Query Optimization Report\n"]
        report.append(f"Total queries analyzed: {len(self.optimization_history)}\n")
        
        # Calculate averages
        avg_time = sum(h['stats']['execution_time'] for h in self.optimization_history) / len(self.optimization_history)
        avg_cost = sum(h['stats']['cost'] for h in self.optimization_history) / len(self.optimization_history)
        
        report.append(f"Average execution time: {avg_time:.3f}s")
        report.append(f"Average query cost: {avg_cost:.2f}\n")
        
        # Find slowest queries
        sorted_by_time = sorted(self.optimization_history, key=lambda x: x['stats']['execution_time'], reverse=True)
        report.append("## Slowest Queries:")
        for i, entry in enumerate(sorted_by_time[:3], 1):
            report.append(f"\n{i}. Time: {entry['stats']['execution_time']:.3f}s")
            report.append(f"   Query: {entry['query'][:100]}...")
        
        return '\n'.join(report)


def create_optimizer(conn) -> QueryOptimizer:
    """
    Factory function to create a QueryOptimizer instance.
    
    Args:
        conn: Database connection
        
    Returns:
        QueryOptimizer instance
    """
    return QueryOptimizer(conn)

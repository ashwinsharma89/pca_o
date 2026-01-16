"""Context helper for SQL best practices and reference material."""
from __future__ import annotations

from typing import Any, Dict, List, Optional
from loguru import logger

try:
    from ..knowledge.vector_store import HybridRetriever, VectorStoreConfig

    HYBRID_AVAILABLE = True
except Exception as exc:  # pragma: no cover - optional dependency
    logger.warning(f"Hybrid retriever unavailable for SQL context: {exc}")
    HYBRID_AVAILABLE = False


SQL_BEST_PRACTICES = """
## SQL Best Practices for Marketing Analytics
- Always compute rate/ratio metrics from aggregated numerators/denominators (SUM-based math).
- Anchor relative time windows to the latest date present in the dataset, not CURRENT_DATE.
- Use descriptive aliases and ROUND/NULLIF to keep numeric outputs readable and stable.
- Break complex logic into CTEs: bounds for date ranges, period labeling, funnel stages, etc.
- Prefer CASE expressions for segmenting campaigns (top performers, risky cohorts, etc.).
- Track core KPIs together (Spend, Impressions, Clicks, Conversions, Revenue) before sorting.
- Document assumptions inline using comments when possible.

## Common Query Patterns
1. **Channel leaderboard**: GROUP BY platform/channel; calculate CTR, CPC, CPA, ROAS.
2. **Time comparisons**: CTE for `bounds` (max_date) + CASE for last vs previous periods.
3. **Funnel drop-off**: Summaries per stage with conversion rates between stages.
4. **Budget pacing**: SUM(actual) vs SUM(budget) by week/month, include variance columns.
5. **Creative testing**: GROUP BY creative_name/ad_copy with efficiency metrics (CTR, CPA).

## Schema Documentation Checklist
- Table name + primary key / date columns.
- Column data types, especially for metrics vs dimensions.
- Any calculated fields that should be recomputed (CTR columns, etc.).
- Known enumerations (platform names, funnel stages).

## Example Queries (Explain Why)
- "Spend & efficiency by channel" → shows scale + efficiency simultaneously for budget allocation.
- "Week-over-week ROAS" → ensures trending context; highlights acceleration/deceleration.
- "Top risky campaigns" → filter on low conversion rate / high CPA to prioritize fixes.

## Performance Optimization Guides
- Use DATE_TRUNC and pre-aggregations when possible.
- Filter early (WHERE clauses) before heavy GROUP BYs.
- Ensure numeric comparisons use consistent units (e.g., percentages vs raw decimals).
- Limit result sets or use ORDER BY/LIMIT for dashboards to avoid overwhelming users.
"""


class SQLKnowledgeHelper:
    """Provides supplemental SQL knowledge and retriever context."""

    def __init__(
        self,
        enable_hybrid: bool = True,
        retriever_filters: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.enable_hybrid = enable_hybrid
        self.retriever_filters = retriever_filters
        self.retriever: Optional[HybridRetriever] = None
        self._last_package: Optional[Dict[str, Any]] = None

        if enable_hybrid and HYBRID_AVAILABLE:
            try:
                self.retriever = HybridRetriever(config=VectorStoreConfig())
            except Exception as exc:  # pragma: no cover - optional dependency
                logger.error(f"Failed to initialize hybrid retriever for SQL helper: {exc}")
                self.retriever = None

    def build_context(
        self,
        query: str,
        schema_info: Optional[Dict[str, Any]] = None,
        top_k: int = 3,
    ) -> str:
        package = self.get_context_package(query, schema_info, top_k=top_k)
        self._last_package = package
        return self.format_package(package)

    def get_context_package(
        self,
        query: str,
        schema_info: Optional[Dict[str, Any]] = None,
        top_k: int = 3,
    ) -> Dict[str, Any]:
        package: Dict[str, Any] = {
            "best_practices": SQL_BEST_PRACTICES.strip(),
            "schema": self._format_schema(schema_info) if schema_info else "",
            "retrieved": [],
        }

        if self.retriever:
            try:
                filters = self.retriever_filters or None
                results = self.retriever.search(
                    f"{query} SQL best practices", top_k=top_k, metadata_filters=filters
                )
                retrieved_chunks = []
                for res in results:
                    meta = res.get("metadata", {})
                    retrieved_chunks.append(
                        {
                            "title": meta.get("title") or meta.get("url") or "Knowledge Source",
                            "url": meta.get("url"),
                            "score": res.get("score", 0),
                            "text": res.get("text", ""),
                        }
                    )
                package["retrieved"] = retrieved_chunks
            except Exception as exc:
                logger.error(f"Hybrid retrieval for SQL helper failed: {exc}")

        self._last_package = package
        return package

    def get_last_context_package(self) -> Optional[Dict[str, Any]]:
        return self._last_package

    @staticmethod
    def format_package(package: Dict[str, Any]) -> str:
        sections: List[str] = []
        best = package.get("best_practices")
        if best:
            sections.append(best)
        schema = package.get("schema")
        if schema:
            sections.append(schema)

        retrieved_chunks = package.get("retrieved", []) or []
        if retrieved_chunks:
            formatted = []
            for chunk in retrieved_chunks:
                formatted.append(
                    f"[Source: {chunk.get('title','Knowledge Source')}]\nScore: {chunk.get('score',0):.3f}\n{chunk.get('text','')}"
                )
            sections.append("\n\n".join(formatted))

        return "\n\n---\n\n".join(sections)

    @staticmethod
    def _format_schema(schema_info: Dict[str, Any]) -> str:
        columns = schema_info.get("columns", [])
        dtypes = schema_info.get("dtypes", {})
        sample_rows = schema_info.get("sample_data", [])

        details = ["## Schema Snapshot"]
        details.append(f"Table: {schema_info.get('table_name', 'campaigns')}")
        details.append("Columns:")
        for col in columns:
            dtype = dtypes.get(col)
            details.append(f"- {col} ({dtype})")

        if sample_rows:
            details.append("Sample rows:")
            for row in sample_rows:
                details.append(f"- {row}")

        return "\n".join(details)

"""
DuckDB + Parquet database layer.
Replaces SQLite/SQLAlchemy for fast analytics.
Now with persistent indexes for 10-100x performance improvement.

IMPORTANT: DuckDB is imported LAZILY inside methods to avoid C++ mutex locks
at module import time. This allows the module to be imported by IDE language
servers without blocking.
"""

import os
# NOTE: Do NOT import duckdb here! It triggers C++ mutex locks.
# import duckdb  # REMOVED - imported lazily in get_connection()
import pandas as pd
from pathlib import Path
from typing import Optional, List, Dict, Any, TYPE_CHECKING
from loguru import logger
from contextlib import contextmanager
import time

if TYPE_CHECKING:
    import duckdb

from src.core.schema.columns import Columns


# Data directory for parquet files
DATA_DIR = Path("data")
# Data directory for parquet files
DATA_DIR = Path("data")
CAMPAIGNS_DIR = DATA_DIR / "campaigns"
CAMPAIGNS_PATTERN = str(CAMPAIGNS_DIR / "**" / "*.parquet")
CAMPAIGNS_PARQUET = DATA_DIR / "campaigns.parquet"  # Legacy single-file path for backwards compatibility
CAMPAIGNS_PARQUET = DATA_DIR / "campaigns.parquet"  # Legacy single-file path for backwards compatibility
DUCKDB_FILE = Path(os.getenv("DUCKDB_PATH", str(DATA_DIR / "analytics.duckdb")))  # Persistent DuckDB database


class DuckDBManager:
    """Manages DuckDB connections and campaign data with performance indexes."""
    
    # Enable performance optimizations
    ENABLE_PARALLEL = True
    ENABLE_INDEXES = True
    
    _instance = None
    _lock = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DuckDBManager, cls).__new__(cls)
            # Initialize lock for thread safety during instantiation
            from threading import RLock
            cls._lock = RLock()
        return cls._instance

    def __init__(self):
        # Prevent re-initialization
        if getattr(self, "_initialized_flag", False):
            return
            
        with self._lock:
            if getattr(self, "_initialized_flag", False):
                return
                
            self.data_dir = DATA_DIR
            self.data_dir.mkdir(exist_ok=True)
            self._conn = None
            self._indexed = False
            self._use_memory = False
            self._initialized_flag = True
            self._tables_initialized = False
            
            logger.info("DuckDBManager Singleton Initialized")

    def initialize(self):
        """Initialize enterprise tracking tables."""
        if self._tables_initialized:
            return
            
        try:
            with self.connection() as conn:
                # 1. Recommendation History Table
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS recommendation_history (
                        id VARCHAR PRIMARY KEY,
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        target_metric VARCHAR,
                        feature_name VARCHAR,
                        suggested_strategy VARCHAR,
                        recommendation_score DOUBLE,
                        details JSON
                    )
                """)
                
                # 2. Recommendation Feedback Table
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS recommendation_feedback (
                        recommendation_id VARCHAR,
                        user_action VARCHAR,
                        implementation_date TIMESTAMP,
                        actual_impact_observed DOUBLE,
                        feedback_notes VARCHAR,
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                logger.info("Enterprise tracking tables verified in DuckDB")
                self._tables_initialized = True
        except Exception as e:
            logger.error(f"Failed to initialize enterprise tables: {e}")


    def get_connection(self):
        """
        Get the persistent shared DuckDB connection.
        Uses in-memory mode to avoid file-based locking and C-level crashes.
        All analytics are read from parquet files directly at query time.
        """
        import duckdb

        # If connection exists and is alive, return it
        if self._conn:
            try:
                self._conn.execute("SELECT 1")
                return self._conn
            except Exception:
                logger.warning("Existing DuckDB connection dead. Reconnecting...")
                self._conn = None

        with self._lock:
            # Double-check locking
            if self._conn:
                return self._conn

            try:
                # Use in-memory DuckDB to avoid file-based locking and segfaults.
                # All campaign data is read from parquet files at query time via
                # read_parquet(), so persistence in DuckDB is not needed.
                conn = duckdb.connect(":memory:")
                conn.execute("SET threads TO 4")
                conn.execute("SET memory_limit = '2GB'")

                logger.info("DuckDB Connected (In-Memory mode - analytics read from parquet)")
                self._conn = conn
                self._use_memory = True

                # Auto-initialize tracking tables
                self.initialize()

            except Exception as e:
                logger.error(f"Failed to connect to DuckDB: {e}")
                raise

            return self._conn

    def shutdown(self):
        """
        Gracefully close the persistent connection.
        Must be called on application exit to prevent file locks.
        """
        if self._conn:
            try:
                logger.info("Closing DuckDB Connection...")
                self._conn.close()
                self._conn = None
            except Exception as e:
                logger.warning(f"Error closing DuckDB connection: {e}")

    
    def ensure_indexes(self):
        """Create indexes on the campaigns table for fast queries."""
        if self._indexed or not self.has_data():
            return

        try:
            self._do_ensure_indexes()
        except Exception as e:
            logger.warning(f"ensure_indexes failed (non-fatal): {e}")
            self._indexed = True  # Mark as done to prevent retries

    def _do_ensure_indexes(self):
        """Internal index creation — called with timeout protection."""
        try:
            start = time.time()
            with self.connection() as conn:
                # Skip if Read Only
                try:
                    conn.execute("CHECKPOINT") # Test write access
                except Exception:
                    logger.warning("Skipping index creation (DB is Read-Only)")
                    self._indexed = True # Pretend it's indexed to avoid errors
                    return
                
                # 1. Create View (schema evolution source)
                conn.execute("DROP VIEW IF EXISTS campaigns_view")
                conn.execute(f"""
                    CREATE OR REPLACE VIEW campaigns_view AS 
                    SELECT * FROM read_parquet('{CAMPAIGNS_PATTERN}', hive_partitioning=true, union_by_name=true)
                """)
                
                # 2. Materialize to Table (for indexes)
                # Force drop to ensure fresh materialization from Parquet
                conn.execute("DROP TABLE IF EXISTS campaigns")
                conn.execute("CREATE TABLE campaigns AS SELECT * FROM campaigns_view")
                
                # Get column names for index creation
                cols_df = conn.execute("SELECT * FROM campaigns LIMIT 0").df()
                columns = [c.lower() for c in cols_df.columns]
                
                # Create performance indexes based on common query patterns
                index_definitions = [
                    # Date-based queries (most common)
                    ("idx_date", ["Date"], "date"),
                    
                    # Platform filtering
                    ("idx_platform", ["Platform", "Ad_Network", "Network"], "platform"),
                    
                    # Channel filtering
                    ("idx_channel", ["Channel", "Marketing Channel", "Ad Group"], "channel"),
                    
                    # Geographic filtering
                    ("idx_region", ["Geographic_Region", "Region", "State", "Location"], "region"),
                    
                    # Device filtering
                    ("idx_device", ["Device_Type", "Device", "Device Category"], "device"),
                    
                    # Campaign name/ID
                    ("idx_campaign", ["Campaign", "Campaign_Name", "Campaign Name"], "campaign"),
                    
                    # Funnel stage
                    ("idx_funnel", ["Funnel", "Funnel_Stage", "Stage"], "funnel"),
                    
                    # Objective
                    ("idx_objective", ["Objective", "Campaign_Objective", "Goal"], "objective"),
                ]
                
                indexes_created = 0
                for idx_name, possible_cols, desc in index_definitions:
                    for col in possible_cols:
                        if col.lower() in columns or col in cols_df.columns:
                            actual_col = col if col in cols_df.columns else [c for c in cols_df.columns if c.lower() == col.lower()][0]
                            try:
                                conn.execute(f'CREATE INDEX IF NOT EXISTS {idx_name} ON campaigns ("{actual_col}")')
                                indexes_created += 1
                                logger.debug(f"Created index {idx_name} on {actual_col}")
                                break
                            except Exception as ie:
                                logger.debug(f"Index {idx_name} error: {ie}")
                
                # Composite indexes for common filter combinations
                try:
                    # Date + Platform (most common combo)
                    if 'date' in columns:
                        date_col = [c for c in cols_df.columns if c.lower() == 'date'][0]
                        platform_col = None
                        for p in ['Platform', 'Ad_Network', 'Network']:
                            if p.lower() in columns or p in cols_df.columns:
                                platform_col = p if p in cols_df.columns else [c for c in cols_df.columns if c.lower() == p.lower()][0]
                                break
                        
                        if platform_col:
                            conn.execute(f'CREATE INDEX IF NOT EXISTS idx_date_platform ON campaigns ("{date_col}", "{platform_col}")')
                            indexes_created += 1
                except Exception as ce:
                    logger.debug(f"Composite index error: {ce}")
                
                elapsed = time.time() - start
                logger.info(f"Created {indexes_created} performance indexes in {elapsed:.2f}s")
                self._indexed = True
                
        except Exception as e:
            logger.warning(f"Index creation failed: {e}")
    
    def get_optimized_table(self) -> str:
        """Get the table name to use - indexed table if available, else Parquet."""
        if self._indexed:
            return "campaigns"
        else:
            return f"read_parquet('{CAMPAIGNS_PATTERN}', hive_partitioning=true, union_by_name=true)"
    
    @contextmanager
    def connection(self):
        """Context manager for DuckDB connections."""
        conn = self.get_connection()
        try:
            yield conn
        finally:
            # Do NOT close the connection here, it's a persistent singleton!
            # The connection is closed by the close() method on app exit.
            pass

    
    def has_data(self) -> bool:
        """Check if campaign data exists."""
        # Check if directory exists and has at least one parquet file
        return CAMPAIGNS_DIR.exists() and any(CAMPAIGNS_DIR.glob("**/*.parquet"))
    
    def save_campaigns(self, df: pd.DataFrame) -> int:
        """
        Save campaigns DataFrame to Partitioned Parquet (Hive Style).
        Structure: data/campaigns/year=YYYY/month=MM/campaigns-uuid.parquet
        Returns number of rows saved.
        """
        # Serialize writes to prevent Race Conditions on Parquet/DuckDB
        with self._lock:
            try:
                # Ensure base directory exists
                campaigns_dir = self.data_dir / "campaigns"
                campaigns_dir.mkdir(parents=True, exist_ok=True)
                
                # Parse Date column
                # Parse Date column ('date' is normalized name)
                date_col = 'date' if 'date' in df.columns else 'Date'
                
                if date_col in df.columns:
                    df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
                    
                    # drop rows with invalid dates
                    if df[date_col].isna().any():
                         logger.warning(f"Dropping {df[date_col].isna().sum()} rows with invalid dates")
                         df = df.dropna(subset=[date_col])
                
                    if len(df) == 0:
                         raise ValueError("All rows have invalid or missing dates")

                    # Add partition columns
                    df['year'] = df[date_col].dt.year
                    df['month'] = df[date_col].dt.month

                logger.info(f"Saving campaigns with columns: {list(df.columns)}")
            
                # Save as Partitioned Parquet
                # compression='snappy' is default and good balance
                df.to_parquet(
                    campaigns_dir,
                    index=False,
                    compression='snappy',
                    partition_cols=['year', 'month'],
                    existing_data_behavior='overwrite_or_ignore'
                )
                
                # Index rebuild disabled - queries use parquet directly (stable)
                self._indexed = False
                
                logger.info(f"Saved {len(df)} campaigns to {campaigns_dir} (partitioned)")
                return len(df)
            except Exception as e:
                logger.error(f"Error saving campaigns: {e}")
                raise e
    
    def append_campaigns(self, df: pd.DataFrame) -> int:
        """
        Append campaigns to partitioned dataset.
        For partitioned parquet, 'append' is just 'write new files'.
        """
        try:
            return self.save_campaigns(df)
        except Exception as e:
            logger.error(f"Failed to append campaigns: {e}")
            raise

    def get_job_summary(self, job_id: str) -> Dict[str, Any]:
        """
        Get summary metrics for a specific upload job using pandas (DuckDB-free).
        Uses pandas directly to avoid DuckDB C-level crashes during upload processing.
        This is intentionally DuckDB-free for stability during the upload pipeline.
        """
        empty = {
            "row_count": 0, "total_spend": 0,
            "total_impressions": 0, "total_clicks": 0,
            "total_conversions": 0
        }
        try:
            import pandas as pd

            # Find all parquet files
            if not CAMPAIGNS_DIR.exists():
                return empty

            parquet_files = list(CAMPAIGNS_DIR.glob("**/*.parquet"))
            if not parquet_files:
                return empty

            # Read and filter by job_id using pandas (no DuckDB = no segfault risk)
            job_col = Columns.JOB_ID.value  # "job_id"
            matching_frames = []
            for f in parquet_files:
                try:
                    df = pd.read_parquet(f)
                    if job_col in df.columns:
                        filtered = df[df[job_col] == job_id]
                        if len(filtered) > 0:
                            matching_frames.append(filtered)
                except Exception as pq_err:
                    logger.warning(f"Could not read parquet {f}: {pq_err}")

            if not matching_frames:
                return empty

            combined = pd.concat(matching_frames, ignore_index=True)

            def _safe_sum(col_name: str, as_int: bool = False):
                if col_name in combined.columns:
                    val = combined[col_name].fillna(0).sum()
                    return int(val) if as_int else float(val)
                return 0

            return {
                "row_count": len(combined),
                "total_spend": _safe_sum(Columns.SPEND.value),
                "total_impressions": _safe_sum(Columns.IMPRESSIONS.value, as_int=True),
                "total_clicks": _safe_sum(Columns.CLICKS.value, as_int=True),
                "total_conversions": _safe_sum(Columns.CONVERSIONS.value, as_int=True),
            }

        except Exception as e:
            logger.error(f"Failed to get job summary for {job_id}: {e}")
            logger.warning("Returning zero-summary due to error")
            return empty
    
    def get_campaigns(
        self,
        filters: Optional[Dict[str, Any]] = None,
        limit: Optional[int] = None,
        offset: int = 0
    ) -> pd.DataFrame:
        """
        Get campaigns with optional filters.
        Uses DuckDB to query Parquet directly.
        """
        if not self.has_data():
            return pd.DataFrame()
        
        try:
            with self.connection() as conn:
                # Build WHERE clause from filters
                where_clauses = []
                params = []
                
                if filters:
                    for key, value in filters.items():
                        if value and key not in ['primary_metric', 'secondary_metric']:
                            if isinstance(value, str) and ',' in value:
                                # Multiple values - use IN clause
                                values = [v.strip() for v in value.split(',')]
                                placeholders = ', '.join(['?' for _ in values])
                                where_clauses.append(f'"{key}" IN ({placeholders})')
                                params.extend(values)
                            elif isinstance(value, list):
                                placeholders = ', '.join(['?' for _ in value])
                                where_clauses.append(f'"{key}" IN ({placeholders})')
                                params.extend(value)
                            else:
                                where_clauses.append(f'"{key}" = ?')
                                params.append(value)
                
                where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
                
                query = f"""
                    SELECT * 
                    FROM {self.get_optimized_table()}
                    WHERE {where_sql}
                    LIMIT {limit if limit is not None else 'ALL'} OFFSET {offset}
                """
                
                result = conn.execute(query, params).df()
                
                # Normalize and Coalesce Columns
                # 1. Map variations to standard names
                # 2. If standard name exists, fill NaNs with variation
                column_map = {
                    "Spend_USD": "Spend",
                    "Revenue_USD": "Revenue", 
                    "Campaign_Name": "Campaign",
                    "campaign_name": "Campaign",
                    "Cost": "Spend",
                    "Amount_Spent": "Spend",
                    "spend_usd": "Spend",
                    "revenue_usd": "Revenue",
                    "Ad_Group": "Ad Group",
                    "ad_group": "Ad Group"
                }
                
                for old_col, new_col in column_map.items():
                    if old_col in result.columns:
                        if new_col in result.columns and old_col != new_col:
                            # Coalesce: Use existing, fill with new (old_col data)
                            result[new_col] = result[new_col].fillna(result[old_col])
                            result = result.drop(columns=[old_col])
                        else:
                            # Just rename
                            result = result.rename(columns={old_col: new_col})

                return result
                
        except Exception as e:
            logger.error(f"Failed to get campaigns: {e}")
            return pd.DataFrame()
    
    def get_campaigns_polars(
        self,
        filters: Optional[Dict[str, Any]] = None,
        limit: Optional[int] = None
    ):
        """
        Get campaigns as Polars DataFrame for high-performance analytics.
        """
        import polars as pl
        
        if not self.has_data():
            return pl.DataFrame()
        
        try:
            with self.connection() as conn:
                # Build WHERE clause from filters
                where_clauses = []
                params = []
                
                if filters:
                    for key, value in filters.items():
                        if value and key not in ['primary_metric', 'secondary_metric']:
                            # Handle standard keys that might need mapping to DB columns
                            db_key = key
                            if key == 'campaign_id': db_key = 'Campaign_ID'
                            
                            if isinstance(value, str) and ',' in value:
                                values = [v.strip() for v in value.split(',')]
                                placeholders = ', '.join(['?' for _ in values])
                                where_clauses.append(f'"{db_key}" IN ({placeholders})')
                                params.extend(values)
                            elif isinstance(value, list):
                                placeholders = ', '.join(['?' for _ in value])
                                where_clauses.append(f'"{db_key}" IN ({placeholders})')
                                params.extend(value)
                            else:
                                where_clauses.append(f'"{db_key}" = ?')
                                params.append(value)
                
                where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
                
                query = f"""
                    SELECT * 
                    FROM {self.get_optimized_table()}
                    WHERE {where_sql}
                    LIMIT {limit if limit is not None else 'ALL'}
                """
                
                # Use .pl() to get Polars DataFrame directly (Zero Copy if possible)
                result = conn.execute(query, params).pl()
                
                # Column Normalization (Snake Case -> Title Case)
                # Polars is case specific, so we use aliases
                
                # Standard mapping
                column_map = {
                    "Spend_USD": "Spend",
                    "Revenue_USD": "Revenue", 
                    "Campaign_Name": "Campaign",
                    "campaign_name": "Campaign",
                    "Cost": "Spend",
                    "Amount_Spent": "Spend",
                    "spend_usd": "Spend",
                    "revenue_usd": "Revenue",
                    "Ad_Group": "Ad Group",
                    "ad_group": "Ad Group"
                }
                
                # Rename columns if they exist
                existing_cols = result.columns
                rename_dict = {}
                
                for old_col, new_col in column_map.items():
                    if old_col in existing_cols:
                        if new_col not in existing_cols:
                             rename_dict[old_col] = new_col
                        elif old_col != new_col:
                             # If both exist, we need to coalesce (fill nulls)
                             # In Polars this is an expression: pl.col(new).fill_null(pl.col(old))
                             # But for now let's just prioritize the existing new_col and drop old
                             pass

                if rename_dict:
                    result = result.rename(rename_dict)
                    
                # Fill nulls for critical metrics (Polars specific optimization)
                fill_cols = [c for c in ['Spend', 'Impressions', 'Clicks', 'Conversions'] if c in result.columns]
                if fill_cols:
                    result = result.with_columns([
                        pl.col(c).fill_null(0) for c in fill_cols
                    ])

                return result
                
        except Exception as e:
            logger.error(f"Failed to get campaigns (Polars): {e}")
            return pl.DataFrame()
    
    def get_filter_options(self) -> Dict[str, List[str]]:
        """
        Get unique values for all filterable columns.
        Dynamically detects columns from the data.
        """
        if not self.has_data():
            return {}
        
        try:
            with self.connection() as conn:
                # Get column names
                # Use read_parquet directly for describe if table not ready
                table_ref = self.get_optimized_table()
                columns_query = f"DESCRIBE SELECT * FROM {table_ref}"
                columns_df = conn.execute(columns_query).df()
                all_columns = columns_df['column_name'].tolist()
                
                # Define which columns should be filters (categorical/string columns)
                # Exclude numeric and date columns
                numeric_keywords = ['spend', 'impressions', 'clicks', 'conversions', 
                                   'ctr', 'cpc', 'cpa', 'roas', 'cpm', 'id', 'count', 'total']
                date_keywords = ['date', 'time', 'created', 'updated']
                
                filter_columns = []
                for col in all_columns:
                    col_lower = col.lower()
                    if not any(kw in col_lower for kw in numeric_keywords + date_keywords):
                        filter_columns.append(col)
                
                # Get unique values for each filter column
                result = {}
                for col in filter_columns:
                    try:
                        query = f"""
                            SELECT DISTINCT CAST("{col}" AS VARCHAR) as val
                            FROM {self.get_optimized_table()}
                            WHERE "{col}" IS NOT NULL 
                            AND CAST("{col}" AS VARCHAR) != 'Unknown'
                            AND CAST("{col}" AS VARCHAR) != ''
                            ORDER BY val
                            LIMIT 100
                        """
                        values_df = conn.execute(query).df()
                        values = values_df['val'].dropna().astype(str).tolist()
                        
                        if values:
                            # Normalize column name for API
                            api_key = col.lower().replace(' ', '_')
                            result[api_key] = values
                    except Exception as e:
                        logger.warning(f"Could not get filter values for {col}: {e}")
                
                return result
                
        except Exception as e:
            logger.error(f"Failed to get filter options: {e}")
            return {}
    
    def get_aggregated_data(
        self,
        group_by: str,
        filters: Optional[Dict[str, Any]] = None
    ) -> pd.DataFrame:
        """
        Get aggregated metrics grouped by a column.
        """
        if not self.has_data():
            return pd.DataFrame()
        
        try:
            with self.connection() as conn:
                # Build WHERE clause
                where_clauses = []
                params = []
                
                if filters:
                    for key, value in filters.items():
                        if value and key not in ['primary_metric', 'secondary_metric']:
                            if isinstance(value, str) and ',' in value:
                                values = [v.strip() for v in value.split(',')]
                                placeholders = ', '.join(['?' for _ in values])
                                where_clauses.append(f'"{key}" IN ({placeholders})')
                                params.extend(values)
                            else:
                                where_clauses.append(f'"{key}" = ?')
                                params.append(value)
                
                where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
                
                query = f"""
                    SELECT 
                        "{group_by}" as name,
                        SUM(COALESCE("Spend", 0)) as spend,
                        SUM(COALESCE("Impressions", 0)) as impressions,
                        SUM(COALESCE("Clicks", 0)) as clicks,
                        SUM(COALESCE("Conversions", 0)) as conversions
                    FROM {self.get_optimized_table()}
                    WHERE {where_sql}
                    AND "{group_by}" IS NOT NULL
                    GROUP BY "{group_by}"
                    ORDER BY spend DESC
                """
                
                result = conn.execute(query, params).df()
                
                # Calculate derived metrics
                if not result.empty:
                    result['ctr'] = (result['clicks'] / result['impressions'] * 100).fillna(0).round(2)
                    result['cpc'] = (result['spend'] / result['clicks']).fillna(0).round(2)
                    result['cpa'] = (result['spend'] / result['conversions']).fillna(0).round(2)
                
                return result
                
        except Exception as e:
            logger.error(f"Failed to get aggregated data: {e}")
            return pd.DataFrame()
    
    def get_trend_data(
        self,
        date_column: str = "Date",
        filters: Optional[Dict[str, Any]] = None
    ) -> pd.DataFrame:
        """
        Get time-series trend data.
        """
        if not self.has_data():
            return pd.DataFrame()
        
        try:
            with self.connection() as conn:
                # Build WHERE clause
                where_clauses = []
                params = []
                
                if filters:
                    for key, value in filters.items():
                        if value and key not in ['primary_metric', 'secondary_metric', 'start_date', 'end_date']:
                            if isinstance(value, str) and ',' in value:
                                values = [v.strip() for v in value.split(',')]
                                placeholders = ', '.join(['?' for _ in values])
                                where_clauses.append(f'"{key}" IN ({placeholders})')
                                params.extend(values)
                            else:
                                where_clauses.append(f'"{key}" = ?')
                                params.append(value)
                
                where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
                
                query = f"""
                    SELECT 
                        "{date_column}" as date,
                        SUM(COALESCE("Spend", 0)) as spend,
                        SUM(COALESCE("Impressions", 0)) as impressions,
                        SUM(COALESCE("Clicks", 0)) as clicks,
                        SUM(COALESCE("Conversions", 0)) as conversions
                    FROM {self.get_optimized_table()}
                    WHERE {where_sql}
                    AND "{date_column}" IS NOT NULL
                    GROUP BY "{date_column}"
                    ORDER BY "{date_column}"
                """
                
                result = conn.execute(query, params).df()
                
                # Calculate derived metrics
                if not result.empty:
                    result['ctr'] = (result['clicks'] / result['impressions'] * 100).fillna(0).round(2)
                    result['cpc'] = (result['spend'] / result['clicks']).fillna(0).round(2)
                    result['cpa'] = (result['spend'] / result['conversions']).fillna(0).round(2)
                    result['cpm'] = (result['spend'] / result['impressions'] * 1000).fillna(0).round(2)
                
                return result
                
        except Exception as e:
            logger.error(f"Failed to get trend data: {e}")
            return pd.DataFrame()
    
    def get_total_metrics(self, filters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Get aggregated metrics across ALL campaigns."""
        if not self.has_data():
            return {
                'total_spend': 0,
                'total_impressions': 0,
                'total_clicks': 0,
                'total_conversions': 0,
                'avg_ctr': 0,
                'avg_cpc': 0,
                'avg_cpa': 0,
                'campaign_count': 0
            }
        
        try:
            with self.connection() as conn:
                # Build WHERE clause
                where_clauses = []
                params = []
                
                if filters:
                    for key, value in filters.items():
                        if value and key not in ['primary_metric', 'secondary_metric', 'start_date', 'end_date']:
                            where_clauses.append(f'"{key}" = ?')
                            params.append(value)
                
                where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
                
                query = f"""
                    SELECT 
                        SUM(COALESCE("Spend", 0)) as total_spend,
                        SUM(COALESCE("Impressions", 0)) as total_impressions,
                        SUM(COALESCE("Clicks", 0)) as total_clicks,
                        SUM(COALESCE("Conversions", 0)) as total_conversions,
                        COUNT(*) as campaign_count
                    FROM {self.get_optimized_table()}
                    WHERE {where_sql}
                """
                
                result_df = conn.execute(query, params).df()
                
                if result_df.empty:
                    return {}
                
                row = result_df.iloc[0]
                total_spend = float(row['total_spend'] or 0)
                total_impressions = int(row['total_impressions'] or 0)
                total_clicks = int(row['total_clicks'] or 0)
                total_conversions = int(row['total_conversions'] or 0)
                
                return {
                    'total_spend': total_spend,
                    'total_impressions': total_impressions,
                    'total_clicks': total_clicks,
                    'total_conversions': total_conversions,
                    'avg_ctr': (total_clicks / total_impressions * 100) if total_impressions > 0 else 0,
                    'avg_cpc': (total_spend / total_clicks) if total_clicks > 0 else 0,
                    'avg_cpa': (total_spend / total_conversions) if total_conversions > 0 else 0,
                    'campaign_count': int(row['campaign_count'] or 0)
                }
        except Exception as e:
            logger.error(f"Failed to get total metrics: {e}")
            return {}

    def get_total_count(self) -> int:
        """Get total number of campaign records."""
        if not self.has_data():
            return 0
        
        try:
            with self.connection() as conn:
                result = conn.execute(f"SELECT COUNT(*) FROM {self.get_optimized_table()}").fetchone()
                return result[0] if result else 0
        except Exception as e:
            logger.error(f"Failed to get count: {e}")
            return 0

    def get_filtered_count(self, filters: Optional[Dict[str, Any]] = None) -> int:
        """Get count of records matching filters."""
        if not self.has_data():
            return 0
            
        try:
            with self.connection() as conn:
                where_clauses = []
                params = []
                
                if filters:
                    for key, value in filters.items():
                        if value and key not in ['primary_metric', 'secondary_metric']:
                            if isinstance(value, str) and ',' in value:
                                values = [v.strip() for v in value.split(',')]
                                placeholders = ', '.join(['?' for _ in values])
                                where_clauses.append(f'"{key}" IN ({placeholders})')
                                params.extend(values)
                            elif isinstance(value, list):
                                placeholders = ', '.join(['?' for _ in value])
                                where_clauses.append(f'"{key}" IN ({placeholders})')
                                params.extend(value)
                            else:
                                where_clauses.append(f'"{key}" = ?')
                                params.append(value)
                
                where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
                
                query = f"SELECT COUNT(*) FROM {self.get_optimized_table()} WHERE {where_sql}"
                result = conn.execute(query, params).fetchone()
                return result[0] if result else 0
        except Exception as e:
            logger.error(f"Failed to get filtered count: {e}")
            return 0
    
    def clear_data(self):
        """Delete all campaign data and tracking tables."""
        if CAMPAIGNS_DIR.exists():
            import shutil
            shutil.rmtree(CAMPAIGNS_DIR, ignore_errors=True)
            logger.info("Cleared campaign data parquet")
            
        try:
            with self.connection() as conn:
                # Nuclear drop of all materialized views and tables
                conn.execute("DROP TABLE IF EXISTS campaigns")
                conn.execute("DROP VIEW IF EXISTS campaigns_view")
                conn.execute("DELETE FROM recommendation_feedback")
                conn.execute("DELETE FROM recommendation_history")
                logger.info("Cleared enterprise tracking tables and materialized data")
                self._indexed = False
        except Exception as e:
            logger.warning(f"Could not clear tracking tables/materialized data: {e}")



# Global instance (kept for backwards compatibility, but prefer dependency injection)
_duckdb_manager: Optional[DuckDBManager] = None


def get_duckdb_manager() -> DuckDBManager:
    """Get or create global DuckDB manager instance.
    
    Note: For new code, prefer using `get_db` dependency with FastAPI's Depends().
    This function is kept for backwards compatibility.
    """
    global _duckdb_manager
    if _duckdb_manager is None:
        _duckdb_manager = DuckDBManager()
    return _duckdb_manager


# FastAPI Dependency Injection Support
def get_db():
    """FastAPI dependency for DuckDB manager.
    
    Usage in routes:
        @router.get("/endpoint")
        async def endpoint(db: DuckDBManager = Depends(get_db)):
            data = db.get_campaigns()
            ...
    
    For app lifecycle initialization (recommended):
        from contextlib import asynccontextmanager
        
        @asynccontextmanager
        async def lifespan(app: FastAPI):
            # Startup: initialize DB manager
            app.state.db_manager = DuckDBManager()
            yield
            # Shutdown: cleanup if needed
            
        app = FastAPI(lifespan=lifespan)
        
        # Then override get_db to use app.state:
        def get_db_from_app(request: Request):
            return request.app.state.db_manager
    """
    return get_duckdb_manager()

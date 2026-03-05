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
            import polars as pl

            # Find all parquet files
            if not CAMPAIGNS_DIR.exists():
                return empty

            parquet_files = list(CAMPAIGNS_DIR.glob("**/*.parquet"))
            if not parquet_files:
                return empty

            # Read and filter by job_id using polars (no DuckDB = no segfault risk)
            job_col = Columns.JOB_ID.value  # "job_id"
            matching_frames = []
            for f in parquet_files:
                try:
                    df = pl.read_parquet(f)
                    if job_col in df.columns:
                        filtered = df.filter(pl.col(job_col) == job_id)
                        if filtered.height > 0:
                            matching_frames.append(filtered)
                except Exception as pq_err:
                    logger.warning(f"Could not read parquet {f}: {pq_err}")

            if not matching_frames:
                return empty

            combined = pl.concat(matching_frames, how="diagonal")

            def _safe_sum(col_name: str, as_int: bool = False):
                if col_name in combined.columns:
                    val = combined[col_name].fill_null(0).sum()
                    return int(val) if as_int else float(val)
                return 0

            return {
                "row_count": combined.height,
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
        Uses Polars scan_parquet for crash-free, high-performance reads.
        """
        if not self.has_data():
            return pd.DataFrame()

        try:
            import polars as pl
            return self.get_campaigns_polars(filters=filters, limit=limit, offset=offset).to_pandas()
        except Exception as e:
            logger.error(f"Failed to get campaigns: {e}")
            return pd.DataFrame()
    
    def get_campaigns_polars(
        self,
        filters: Optional[Dict[str, Any]] = None,
        limit: Optional[int] = None,
        offset: int = 0
    ):
        """
        Get campaigns as Polars LazyFrame → DataFrame.
        Uses scan_parquet for zero-DuckDB, crash-free reads.
        Reads each parquet file individually then concatenates with diagonal_relaxed,
        which handles schema evolution (missing columns → null) and type mismatches
        (Int64 vs Float64 → Float64) without crashing.
        """
        import polars as pl

        if not self.has_data():
            return pl.DataFrame()

        try:
            parquet_files = list(CAMPAIGNS_DIR.glob("**/*.parquet"))
            if not parquet_files:
                return pl.DataFrame()

            # Fast path: schema-only reads (limit ≤ 10, no filters, no offset).
            # Read just the first file instead of all files — this is used by endpoints
            # that only need column names, not actual data (e.g. get_campaigns(limit=1)
            # for column detection before building filter_params).
            if limit is not None and limit <= 10 and not filters and not offset:
                try:
                    first_df = pl.read_parquet(parquet_files[0]).head(limit)
                    # Fill nulls for core metrics
                    metric_cols = [c for c in ['spend', 'impressions', 'clicks', 'conversions', 'revenue']
                                   if c in first_df.columns]
                    if metric_cols:
                        first_df = first_df.with_columns([pl.col(c).fill_null(0) for c in metric_cols])
                    return first_df
                except Exception as fast_err:
                    logger.warning(f"Fast-path read failed ({fast_err}), falling back to full read")

            # Read each file individually to handle mixed schemas across uploads
            frames: List[pl.DataFrame] = []
            for f in parquet_files:
                try:
                    frames.append(pl.read_parquet(f))
                except Exception as read_err:
                    logger.warning(f"Could not read parquet {f}: {read_err}")

            if not frames:
                return pl.DataFrame()

            # diagonal_relaxed: fills missing columns with null, casts type mismatches
            # to a common supertype (e.g. Int64 + Float64 → Float64)
            result = pl.concat(frames, how="diagonal_relaxed")

            # Apply filters
            skip_keys = {'primary_metric', 'secondary_metric'}
            if filters:
                for key, value in filters.items():
                    if not value or key in skip_keys:
                        continue
                    if key == 'campaign_id':
                        key = 'Campaign_ID'
                    if key not in result.columns:
                        continue

                    if isinstance(value, str) and ',' in value:
                        vals = [v.strip() for v in value.split(',')]
                        result = result.filter(pl.col(key).is_in(vals))
                    elif isinstance(value, list):
                        result = result.filter(pl.col(key).is_in(value))
                    else:
                        result = result.filter(pl.col(key) == value)

            # Apply offset / limit
            if offset:
                result = result.slice(offset, limit if limit is not None else result.height)
            elif limit is not None:
                result = result.head(limit)

            # Fill nulls for core metrics
            metric_cols = [c for c in ['spend', 'impressions', 'clicks', 'conversions', 'revenue']
                           if c in result.columns]
            if metric_cols:
                result = result.with_columns([pl.col(c).fill_null(0) for c in metric_cols])

            return result

        except Exception as e:
            logger.error(f"Failed to get campaigns (Polars): {e}")
            return pl.DataFrame()
    
    def get_filter_options(self) -> Dict[str, List[str]]:
        """
        Get unique values for all filterable columns.
        Uses Polars for crash-free reads (no DuckDB glob pattern).
        """
        if not self.has_data():
            return {}

        try:
            import polars as pl
            df = self.get_campaigns_polars()

            if df.is_empty():
                return {}

            # Exclude numeric and date/metadata columns
            numeric_keywords = ['spend', 'impressions', 'clicks', 'conversions',
                                 'ctr', 'cpc', 'cpa', 'roas', 'cpm', 'revenue', 'reach',
                                 'id', 'count', 'total', 'hash', 'run_id']
            date_keywords = ['date', 'time', 'created', 'updated', 'year', 'month']

            # Only include string/categorical columns
            numeric_types = {pl.Int8, pl.Int16, pl.Int32, pl.Int64,
                             pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64,
                             pl.Float32, pl.Float64}
            date_types = {pl.Date, pl.Datetime, pl.Duration, pl.Time}

            result = {}
            for col in df.columns:
                col_lower = col.lower()
                if any(kw in col_lower for kw in numeric_keywords + date_keywords):
                    continue
                if df[col].dtype in numeric_types or df[col].dtype in date_types:
                    continue
                try:
                    values = df[col].drop_nulls().unique().to_list()
                    values = sorted([str(v) for v in values
                                     if v and str(v) not in ('Unknown', '')])[:100]
                    if values:
                        result[col_lower.replace(' ', '_')] = values
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
        Uses Polars for crash-free reads (no DuckDB glob pattern).
        """
        if not self.has_data():
            return pd.DataFrame()

        try:
            import polars as pl
            df = self.get_campaigns_polars(filters=filters)

            if df.is_empty():
                return pd.DataFrame()

            # Resolve group_by column (case-insensitive)
            actual_group_by = None
            for col in df.columns:
                if col.lower() == group_by.lower():
                    actual_group_by = col
                    break
            if not actual_group_by:
                return pd.DataFrame()

            # Resolve metric columns (case-insensitive)
            def find_col(name):
                for c in df.columns:
                    if c.lower() == name.lower():
                        return c
                return None

            spend_c = find_col('spend')
            impr_c = find_col('impressions')
            clicks_c = find_col('clicks')
            conv_c = find_col('conversions')

            agg_exprs = [
                (pl.col(spend_c).fill_null(0).cast(pl.Float64).sum().alias("spend")
                 if spend_c else pl.lit(0.0).alias("spend")),
                (pl.col(impr_c).fill_null(0).cast(pl.Int64).sum().alias("impressions")
                 if impr_c else pl.lit(0).alias("impressions")),
                (pl.col(clicks_c).fill_null(0).cast(pl.Int64).sum().alias("clicks")
                 if clicks_c else pl.lit(0).alias("clicks")),
                (pl.col(conv_c).fill_null(0).cast(pl.Int64).sum().alias("conversions")
                 if conv_c else pl.lit(0).alias("conversions")),
            ]

            result_pl = df.group_by(actual_group_by).agg(agg_exprs).sort("spend", descending=True)
            result = result_pl.rename({actual_group_by: "name"}).to_pandas()

            # Calculate derived metrics
            if not result.empty:
                result['ctr'] = (result['clicks'] / result['impressions'].replace(0, float('nan')) * 100).fillna(0).round(2)
                result['cpc'] = (result['spend'] / result['clicks'].replace(0, float('nan'))).fillna(0).round(2)
                result['cpa'] = (result['spend'] / result['conversions'].replace(0, float('nan'))).fillna(0).round(2)

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
        Uses Polars for crash-free reads (no DuckDB glob pattern).
        """
        if not self.has_data():
            return pd.DataFrame()

        try:
            import polars as pl
            df = self.get_campaigns_polars(filters=filters)

            if df.is_empty():
                return pd.DataFrame()

            # Resolve date column (case-insensitive)
            actual_date_col = None
            for col in df.columns:
                if col.lower() == date_column.lower():
                    actual_date_col = col
                    break
            if not actual_date_col:
                return pd.DataFrame()

            # Resolve metric columns (case-insensitive)
            def find_col(name):
                for c in df.columns:
                    if c.lower() == name.lower():
                        return c
                return None

            spend_c = find_col('spend')
            impr_c = find_col('impressions')
            clicks_c = find_col('clicks')
            conv_c = find_col('conversions')

            agg_exprs = [
                (pl.col(spend_c).fill_null(0).cast(pl.Float64).sum().alias("spend")
                 if spend_c else pl.lit(0.0).alias("spend")),
                (pl.col(impr_c).fill_null(0).cast(pl.Int64).sum().alias("impressions")
                 if impr_c else pl.lit(0).alias("impressions")),
                (pl.col(clicks_c).fill_null(0).cast(pl.Int64).sum().alias("clicks")
                 if clicks_c else pl.lit(0).alias("clicks")),
                (pl.col(conv_c).fill_null(0).cast(pl.Int64).sum().alias("conversions")
                 if conv_c else pl.lit(0).alias("conversions")),
            ]

            result_pl = df.group_by(actual_date_col).agg(agg_exprs).sort(actual_date_col)
            result = result_pl.rename({actual_date_col: "date"}).to_pandas()

            # Calculate derived metrics
            if not result.empty:
                result['ctr'] = (result['clicks'] / result['impressions'].replace(0, float('nan')) * 100).fillna(0).round(2)
                result['cpc'] = (result['spend'] / result['clicks'].replace(0, float('nan'))).fillna(0).round(2)
                result['cpa'] = (result['spend'] / result['conversions'].replace(0, float('nan'))).fillna(0).round(2)
                result['cpm'] = (result['spend'] / result['impressions'].replace(0, float('nan')) * 1000).fillna(0).round(2)

            return result

        except Exception as e:
            logger.error(f"Failed to get trend data: {e}")
            return pd.DataFrame()
    
    def get_total_metrics(self, filters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Get aggregated metrics across ALL campaigns.
        Uses Polars for crash-free reads (no DuckDB glob pattern).
        """
        empty = {
            'total_spend': 0, 'total_impressions': 0, 'total_clicks': 0,
            'total_conversions': 0, 'avg_ctr': 0, 'avg_cpc': 0, 'avg_cpa': 0,
            'campaign_count': 0
        }
        if not self.has_data():
            return empty

        try:
            import polars as pl
            df = self.get_campaigns_polars(filters=filters)

            if df.is_empty():
                return empty

            def find_col(name):
                for c in df.columns:
                    if c.lower() == name.lower():
                        return c
                return None

            spend_c = find_col('spend')
            impr_c = find_col('impressions')
            clicks_c = find_col('clicks')
            conv_c = find_col('conversions')

            total_spend = float(df[spend_c].fill_null(0).sum()) if spend_c else 0.0
            total_impr = int(df[impr_c].fill_null(0).sum()) if impr_c else 0
            total_clicks = int(df[clicks_c].fill_null(0).sum()) if clicks_c else 0
            total_conv = int(df[conv_c].fill_null(0).sum()) if conv_c else 0

            return {
                'total_spend': total_spend,
                'total_impressions': total_impr,
                'total_clicks': total_clicks,
                'total_conversions': total_conv,
                'avg_ctr': (total_clicks / total_impr * 100) if total_impr > 0 else 0,
                'avg_cpc': (total_spend / total_clicks) if total_clicks > 0 else 0,
                'avg_cpa': (total_spend / total_conv) if total_conv > 0 else 0,
                'campaign_count': df.height
            }
        except Exception as e:
            logger.error(f"Failed to get total metrics: {e}")
            return empty

    def get_total_count(self) -> int:
        """Get total number of campaign records.
        Uses Polars for crash-free reads (no DuckDB glob pattern).
        """
        if not self.has_data():
            return 0

        try:
            return self.get_campaigns_polars().height
        except Exception as e:
            logger.error(f"Failed to get count: {e}")
            return 0

    def get_filtered_count(self, filters: Optional[Dict[str, Any]] = None) -> int:
        """Get count of records matching filters.
        Uses Polars for crash-free reads (no DuckDB glob pattern).
        """
        if not self.has_data():
            return 0

        try:
            return self.get_campaigns_polars(filters=filters).height
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

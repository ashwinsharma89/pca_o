"""
Robust Data Loading Utility with Comprehensive Error Handling
Handles CSV files, file uploads, and various data formats
"""
import pandas as pd
import os
from typing import Optional, Union, Tuple
from pathlib import Path
from loguru import logger
import io
import re


class DataLoadError(Exception):
    """Custom exception for data loading errors."""
    pass


class DataLoader:
    """Centralized data loader with comprehensive error handling."""
    
    # Supported file extensions
    SUPPORTED_EXTENSIONS = ['.csv', '.xlsx', '.xls', '.json', '.parquet']
    
    # Maximum file size (200MB)
    import os
    MAX_UPLOAD_SIZE_MB = int(os.getenv("MAX_UPLOAD_SIZE_MB", "200"))
    MAX_FILE_SIZE = MAX_UPLOAD_SIZE_MB * 1024 * 1024
    # Minimum required rows
    MIN_ROWS = 1
    
    def __init__(self):
        """Initialize the data loader."""
        logger.info("Initialized DataLoader")
    
    @staticmethod
    def load_csv(
        file_path: Union[str, Path, io.BytesIO],
        validate: bool = True,
        fix_column_names: bool = True,
        encoding: str = 'utf-8'
    ) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        """
        Load CSV file with comprehensive error handling.
        
        Args:
            file_path: Path to CSV file or file-like object
            validate: Whether to validate the loaded data
            fix_column_names: Whether to fix column names (spaces to underscores)
            encoding: File encoding (default: utf-8)
            
        Returns:
            Tuple of (DataFrame, error_message)
            If successful: (df, None)
            If failed: (None, error_message)
        """
        try:
            # Check if file exists (for file paths)
            if isinstance(file_path, (str, Path)):
                file_path = Path(file_path)
                
                if not file_path.exists():
                    error_msg = f"File not found: {file_path}"
                    logger.error(error_msg)
                    return None, error_msg
                
                # Check file size
                file_size = file_path.stat().st_size
                if file_size == 0:
                    error_msg = f"File is empty: {file_path}"
                    logger.error(error_msg)
                    return None, error_msg
                
                if file_size > DataLoader.MAX_FILE_SIZE:
                    error_msg = f"File too large ({file_size / 1024 / 1024:.1f}MB). Maximum: {DataLoader.MAX_FILE_SIZE / 1024 / 1024}MB"
                    logger.error(error_msg)
                    return None, error_msg
                
                # Check extension
                if file_path.suffix.lower() not in DataLoader.SUPPORTED_EXTENSIONS:
                    error_msg = f"Unsupported file type: {file_path.suffix}. Supported: {', '.join(DataLoader.SUPPORTED_EXTENSIONS)}"
                    logger.error(error_msg)
                    return None, error_msg

                # Check magic numbers (content-based validation)
                with open(file_path, 'rb') as f:
                    first_bytes = f.read(8)
                    if not DataLoader._validate_file_content(first_bytes, file_path.suffix.lower()):
                        error_msg = f"File content does not match extension: {file_path.suffix}"
                        logger.error(error_msg)
                        return None, error_msg
            
            # Try to read CSV with different encodings
            encodings_to_try = [encoding, 'utf-8', 'latin-1', 'iso-8859-1', 'cp1252']
            df = None
            last_error = None
            
            for enc in encodings_to_try:
                try:
                    df = pd.read_csv(file_path, encoding=enc)
                    logger.info(f"Successfully loaded CSV with encoding: {enc}")
                    break
                except UnicodeDecodeError as e:
                    last_error = e
                    continue
                except Exception as e:
                    last_error = e
                    break
            
            if df is None:
                error_msg = f"Failed to read CSV: {str(last_error)}"
                logger.error(error_msg)
                return None, error_msg
            
            # Validate data
            if validate:
                validation_error = DataLoader._validate_dataframe(df)
                if validation_error:
                    logger.error(validation_error)
                    return None, validation_error
            
            # Fix column names
            if fix_column_names:
                df = DataLoader._fix_column_names(df)
            
            # Log success
            logger.success(f"✓ Loaded {len(df):,} rows with {len(df.columns)} columns")
            
            return df, None
        
        except pd.errors.EmptyDataError:
            error_msg = "CSV file is empty or has no data"
            logger.error(error_msg)
            return None, error_msg
        
        except pd.errors.ParserError as e:
            error_msg = f"CSV parsing error: {str(e)}"
            logger.error(error_msg)
            return None, error_msg
        
        except MemoryError:
            error_msg = "Not enough memory to load this file. Try a smaller file."
            logger.error(error_msg)
            return None, error_msg
        
        except Exception as e:
            error_msg = f"Unexpected error loading CSV: {str(e)}"
            logger.error(error_msg)
            return None, error_msg
    
    @staticmethod
    def _validate_dataframe(df: pd.DataFrame) -> Optional[str]:
        """
        Validate DataFrame for common issues.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Error message if validation fails, None if successful
        """
        # Check if empty
        if df.empty:
            return "DataFrame is empty (0 rows)"
        
        # Check minimum rows
        if len(df) < DataLoader.MIN_ROWS:
            return f"DataFrame has too few rows ({len(df)}). Minimum: {DataLoader.MIN_ROWS}"
        
        # Check if all columns are unnamed
        unnamed_cols = [col for col in df.columns if 'Unnamed' in str(col)]
        if len(unnamed_cols) == len(df.columns):
            return "All columns are unnamed. Check if CSV has headers."
        
        # Check for duplicate column names
        duplicate_cols = df.columns[df.columns.duplicated()].tolist()
        if duplicate_cols:
            logger.warning(f"Found duplicate column names: {duplicate_cols}")
            # Don't fail, just warn
        
        # Check if all values are NaN
        if df.isna().all().all():
            return "All values in DataFrame are NaN/missing"
        
        return None

    @staticmethod
    def _validate_file_content(first_bytes: bytes, extension: str) -> bool:
        """
        Validate file content using magic numbers (first few bytes).
        
        Args:
            first_bytes: First 8 bytes of the file
            extension: File extension (including dot)
            
        Returns:
            True if valid, False otherwise
        """
        if not first_bytes:
            return False

        # Magic numbers for supported formats
        # Parquet: PAR1
        if extension == '.parquet':
            return first_bytes.startswith(b'PAR1')
        
        # Excel: [50, 4B, 03, 04] (ZIP-based for XLSX) or [D0, CF, 11, E0] (Old XLS)
        if extension == '.xlsx':
            return first_bytes.startswith(b'PK\x03\x04')
        if extension == '.xls':
            return first_bytes.startswith(b'\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1')
        
        # JSON: [7B] ({) or [5B] ([)
        if extension == '.json':
            return first_bytes.startswith(b'{') or first_bytes.startswith(b'[')
        
        # CSV: Harder to validate via magic number, check for common characters
        # But we can at least ensure it's not a known binary format
        if extension == '.csv':
            # Ensure it's not a ZIP, Executable, etc.
            binary_signatures = [
                b'PK\x03\x04',    # ZIP/XLSX
                b'\x7fELF',      # ELF
                b'MZ',           # PE
                b'\x89PNG',      # PNG
                b'\xff\xd8\xff', # JPEG
            ]
            for sig in binary_signatures:
                if first_bytes.startswith(sig):
                    return False
            return True
            
        return True
    
    @staticmethod
    def _fix_column_names(df: pd.DataFrame) -> pd.DataFrame:
        """
        Fix column names by replacing spaces with underscores.
        
        Args:
            df: DataFrame with potentially problematic column names
            
        Returns:
            DataFrame with fixed column names
        """
        # Replace spaces with underscores
        df.columns = df.columns.str.replace(' ', '_')
        
        # Remove special characters except underscore
        df.columns = df.columns.str.replace(r'[^a-zA-Z0-9_]', '', regex=True)
        
        # Ensure columns don't start with numbers
        df.columns = ['col_' + col if col[0].isdigit() else col for col in df.columns]
        
        # Handle duplicate columns by adding suffix
        cols = pd.Series(df.columns)
        for dup in cols[cols.duplicated()].unique():
            cols[cols[cols == dup].index.values.tolist()] = [dup + '_' + str(i) if i != 0 else dup for i in range(sum(cols == dup))]
        df.columns = cols
        
        logger.info(f"Fixed column names: {df.columns.tolist()[:5]}...")
        
        return df
    
    @staticmethod
    def load_from_streamlit_upload(
        uploaded_file,
        validate: bool = True,
        fix_column_names: bool = True
    ) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        """
        Load data from Streamlit file uploader.
        
        Args:
            uploaded_file: Streamlit UploadedFile object
            validate: Whether to validate the loaded data
            fix_column_names: Whether to fix column names
            
        Returns:
            Tuple of (DataFrame, error_message)
        """
        if uploaded_file is None:
            return None, "No file uploaded"
        
        try:
            # Check file size
            file_size = uploaded_file.size
            if file_size > DataLoader.MAX_FILE_SIZE:
                error_msg = f"File too large ({file_size / 1024 / 1024:.1f}MB). Maximum: {DataLoader.MAX_FILE_SIZE / 1024 / 1024}MB"
                logger.error(error_msg)
                return None, error_msg
            
            # Get file extension
            file_ext = Path(uploaded_file.name).suffix.lower()
            
            if file_ext == '.csv':
                return DataLoader.load_csv(uploaded_file, validate, fix_column_names)
            elif file_ext in ['.xlsx', '.xls']:
                return DataLoader.load_excel(uploaded_file, validate, fix_column_names)
            elif file_ext == '.json':
                return DataLoader.load_json(uploaded_file, validate, fix_column_names)
            elif file_ext == '.parquet':
                return DataLoader.load_parquet(uploaded_file, validate, fix_column_names)
            else:
                error_msg = f"Unsupported file type: {file_ext}"
                logger.error(error_msg)
                return None, error_msg
        
        except Exception as e:
            error_msg = f"Error loading file: {str(e)}"
            logger.error(error_msg)
            return None, error_msg
    
    @staticmethod
    def load_excel(
        file_path: Union[str, Path, io.BytesIO],
        validate: bool = True,
        fix_column_names: bool = True,
        sheet_name: Union[str, int] = 0
    ) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        """Load Excel file with error handling."""
        try:
            df = pd.read_excel(file_path, sheet_name=sheet_name)
            
            if validate:
                validation_error = DataLoader._validate_dataframe(df)
                if validation_error:
                    return None, validation_error
            
            if fix_column_names:
                df = DataLoader._fix_column_names(df)
            
            logger.success(f"✓ Loaded {len(df):,} rows from Excel")
            return df, None
        
        except Exception as e:
            error_msg = f"Error loading Excel file: {str(e)}"
            logger.error(error_msg)
            return None, error_msg
    
    @staticmethod
    def load_json(
        file_path: Union[str, Path, io.BytesIO],
        validate: bool = True,
        fix_column_names: bool = True
    ) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        """Load JSON file with error handling."""
        try:
            df = pd.read_json(file_path)
            
            if validate:
                validation_error = DataLoader._validate_dataframe(df)
                if validation_error:
                    return None, validation_error
            
            if fix_column_names:
                df = DataLoader._fix_column_names(df)
            
            logger.success(f"✓ Loaded {len(df):,} rows from JSON")
            return df, None
        
        except Exception as e:
            error_msg = f"Error loading JSON file: {str(e)}"
            logger.error(error_msg)
            return None, error_msg
    
    @staticmethod
    def load_parquet(
        file_path: Union[str, Path, io.BytesIO],
        validate: bool = True,
        fix_column_names: bool = True
    ) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        """Load Parquet file with error handling."""
        try:
            df = pd.read_parquet(file_path)
            
            if validate:
                validation_error = DataLoader._validate_dataframe(df)
                if validation_error:
                    return None, validation_error
            
            if fix_column_names:
                df = DataLoader._fix_column_names(df)
            
            logger.success(f"✓ Loaded {len(df):,} rows from Parquet")
            return df, None
        
        except Exception as e:
            error_msg = f"Error loading Parquet file: {str(e)}"
            logger.error(error_msg)
            return None, error_msg


# Convenience functions for backward compatibility
def fetch_data(file_path: Union[str, Path], **kwargs) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """
    Fetch data from file with error handling.
    
    Args:
        file_path: Path to data file
        **kwargs: Additional arguments for DataLoader
        
    Returns:
        Tuple of (DataFrame, error_message)
    """
    loader = DataLoader()
    
    file_ext = Path(file_path).suffix.lower()
    
    if file_ext == '.csv':
        return loader.load_csv(file_path, **kwargs)
    elif file_ext in ['.xlsx', '.xls']:
        return loader.load_excel(file_path, **kwargs)
    elif file_ext == '.json':
        return loader.load_json(file_path, **kwargs)
    elif file_ext == '.parquet':
        return loader.load_parquet(file_path, **kwargs)
    else:
        return None, f"Unsupported file type: {file_ext}"


def safe_load_csv(file_path: Union[str, Path], **kwargs) -> Optional[pd.DataFrame]:
    """
    Safely load CSV and return DataFrame or None.
    
    Args:
        file_path: Path to CSV file
        **kwargs: Additional arguments for DataLoader
        
    Returns:
        DataFrame if successful, None if failed
    """
    df, error = DataLoader.load_csv(file_path, **kwargs)
    if error:
        logger.error(f"Failed to load CSV: {error}")
    return df


def normalize_campaign_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize campaign dataframe column names to standard format.
    Maps common variations to: Campaign_Name, Platform, Spend, Conversions, Revenue, etc.
    """
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    def _norm(name: str) -> str:
        return re.sub(r"[^a-z0-9]+", "_", str(name).strip().lower())

    column_mapping = {
        # Campaign identifiers
        "campaign_name": "Campaign_Name",
        "campaign": "Campaign_Name",
        "campaignid": "Campaign_Name",
        "campaign_id": "Campaign_Name",
        "campaign_name_full": "Campaign_Name",
        
        # Platform/Channel
        "platform": "Platform",
        "channel": "Platform",
        "publisher": "Platform",
        "network": "Platform",
        "source": "Platform",
        
        # Spend/Cost
        "spend": "Spend",
        "total_spend": "Spend",
        "total_spent": "Spend",
        "media_spend": "Spend",
        "ad_spend": "Spend",
        "cost": "Spend",
        "costs": "Spend",
        "amount_spent": "Spend",
        
        # Conversions
        "conversions": "Conversions",
        "conv": "Conversions",
        "site_visit": "Conversions",
        "site_visits": "Conversions",
        "conversion": "Conversions",
        
        # Revenue
        "revenue": "Revenue",
        "conversion_value": "Revenue",
        "total_revenue": "Revenue",
        
        # Impressions
        "impressions": "Impressions",
        "impr": "Impressions",
        "impression": "Impressions",
        
        # Clicks
        "clicks": "Clicks",
        "click": "Clicks",
        
        # Date
        "date": "Date",
        "day": "Date",
        "report_date": "Date",
        
        # Placement
        "placement": "Placement",
        "ad_placement": "Placement",
    }

    # Convert columns to list to avoid unhashable type errors
    existing = set(str(c) for c in df.columns)
    rename_map = {}
    for col in df.columns:
        key = _norm(col)
        target = column_mapping.get(key)
        if target and target not in existing:
            rename_map[str(col)] = target

    if rename_map:
        df = df.rename(columns=rename_map)

    # Convert Spend to numeric
    if "Spend" in df.columns:
        spend_series = df["Spend"]
        if not pd.api.types.is_numeric_dtype(spend_series):
            spend_series = spend_series.astype(str).str.replace(r"[^\d\.\-]", "", regex=True)
            df["Spend"] = pd.to_numeric(spend_series, errors="coerce").fillna(0)
    
    # Convert Conversions to numeric
    if "Conversions" in df.columns:
        conv_series = df["Conversions"]
        if not pd.api.types.is_numeric_dtype(conv_series):
            conv_series = conv_series.astype(str).str.replace(r"[^\d\.\-]", "", regex=True)
            df["Conversions"] = pd.to_numeric(conv_series, errors="coerce").fillna(0)
    
    # Convert Revenue to numeric
    if "Revenue" in df.columns:
        rev_series = df["Revenue"]
        if not pd.api.types.is_numeric_dtype(rev_series):
            rev_series = rev_series.astype(str).str.replace(r"[^\d\.\-]", "", regex=True)
            df["Revenue"] = pd.to_numeric(rev_series, errors="coerce").fillna(0)
    
    # Ensure 'Type' is string to satisfy PyArrow (fixes ArrowInvalid error)
    if "Type" in df.columns:
        df["Type"] = df["Type"].astype(str)
        
    return df

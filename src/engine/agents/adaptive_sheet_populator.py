"""
Adaptive Sheet Populator for Pacing Reports - STABLE VERSION

Memory-efficient, crash-resistant data population for Excel sheets up to 100+ MB.
Handles large files with chunked processing and robust error handling.
"""

import pandas as pd
from typing import Dict, Any, Optional, List, Tuple
from loguru import logger
import gc


class AdaptiveSheetPopulator:
    """
    Intelligently populate Excel sheets with data, adapting to any template structure.
    
    STABILITY FEATURES:
    - Chunked data processing (1000 rows at a time)
    - Memory-efficient operations
    - Comprehensive error handling
    - Progress logging for large datasets
    - Graceful degradation on errors
    """
    
    # Column mapping patterns (case-insensitive) - EXPANDED for robust matching
    # CRITICAL: Order matters! More specific patterns MUST come before generic ones
    # e.g., 'monthly_spend' must come before 'month' and 'spend'
    COLUMN_PATTERNS = {
        # SPECIFIC COMPOUND PATTERNS FIRST (before their generic counterparts)
        'monthly_spend': ['monthly spend'],      # BEFORE 'month' and 'spend'
        'daily_spend': ['daily spend'],          # BEFORE 'day' and 'spend'
        'day_of_week': ['day of week', 'weekday', 'día de la semana', 'wochentag'],  # BEFORE 'day'
        'year_month': ['year-month', 'yearmonth', 'ym'],  # BEFORE 'year' and 'month'
        
        # Budget Pacing Dashboard specific - BEFORE generic spend
        'variance_pct': ['variance %', 'var %', 'variance pct', '% variance', 'variance%'],
        'variance': ['variance', 'var', 'diff', 'difference'],
        'budget': ['budget', 'planned', 'target', 'goal', 'allocated'],
        'status': ['status', 'pacing status'],
        'alert': ['alert', 'flag', 'indicator', 'warning'],
        
        # Date/Time columns - AFTER specific compound patterns
        'date': ['date', 'day', 'fecha', 'datum', 'period'],
        'year': ['year', 'año', 'jahr'],
        'month': ['month', 'mes', 'monat'],  # AFTER 'monthly_spend' and 'year_month'
        'week': ['week', 'semana', 'woche', 'year-week'],
        
        # Dimension columns
        'platform': ['platform', 'channel', 'source', 'medio', 'kanal'],
        'campaign': ['campaign', 'campaña', 'kampagne', 'ad group', 'adgroup'],
        
        # Metric columns
        'impressions': ['impression', 'impress', 'impresion', 'views', 'reach'],
        'clicks': ['click', 'clic', 'visits'],
        'conversions': ['conversion', 'conv', 'convert', 'leads', 'actions', 'goals'],
        
        # Spend - AFTER specific variants (monthly_spend, daily_spend, budget)
        'spend': ['spend', 'cost', 'gasto', 'kosten', 'total spend', 'ad spend', 'media spend',
                  'spend_usd', 'cost_usd', 'amount'],
        
        # Revenue columns
        'revenue': ['revenue', 'ingreso', 'einnahmen', 'sales', 'income', 'value',
                   'revenue_usd', 'total revenue', 'gross revenue'],
        
        # Calculated metrics
        'ctr': ['ctr', 'click through', 'click-through', 'click rate'],
        'cpc': ['cpc', 'cost per click', 'avg cpc', 'average cpc'],
        'cpm': ['cpm', 'cost per mille', 'cost per thousand', 'cost per 1000'],
        'cpa': ['cpa', 'cost per acquisition', 'cost per action', 'cost per conversion'],
        'roas': ['roas', 'return on ad spend', 'return on spend'],
        'cvr': ['cvr', 'conversion rate', 'conv rate', 'cr'],
        
        # Profit/Margin columns (for Pivot Analysis)
        'profit': ['profit', 'gross profit', 'net profit', 'margin'],
        'margin_pct': ['margin %', 'margin pct', 'profit margin', 'gpm', 'gross margin']
    }
    
    # Configuration for stability and performance
    CHUNK_SIZE = 5000  # Process data in larger chunks for speed (from 1000)
    MAX_ROWS = 1000000  # Safety limit: 1 million rows
    
    def __init__(self):
        self.header_row = None
        self.data_start_row = None
        self.column_mapping = {}
        self.tables_found = []  # Track all tables found in sheet
    
    def detect_all_tables(self, sheet, max_rows_to_check: int = 200) -> List[Dict]:
        """
        INTELLIGENT MULTI-TABLE DETECTION
        
        Scans sheet for ALL data tables, not just the first one.
        A table is identified by a row with 3+ matching header patterns,
        followed by data rows.
        
        Returns: List of table definitions with header_row, data_start_row, columns
        """
        tables = []
        last_table_end = 0
        
        logger.info(f"🔍 Scanning '{sheet.title}' for data tables...")
        
        try:
            for row in range(1, min(max_rows_to_check + 1, sheet.max_row + 1)):
                # Skip if we're still in a previous table's data
                if row <= last_table_end:
                    continue
                    
                matches = 0
                non_empty = 0
                matched_headers = []
                
                for col in range(1, min(sheet.max_column + 1, 30)):
                    try:
                        cell_value = sheet.cell(row, col).value
                        if cell_value:
                            non_empty += 1
                            cell_str = str(cell_value).lower().strip()
                            
                            for field, patterns in self.COLUMN_PATTERNS.items():
                                if any(pattern in cell_str for pattern in patterns):
                                    matches += 1
                                    matched_headers.append((col, field, cell_str))
                                    break
                    except Exception:
                        continue
                
                # If we found a header row (3+ matches and >30% match rate)
                if matches >= 3 and non_empty > 0 and (matches / non_empty) >= 0.3:
                    # Estimate table end by finding empty rows
                    data_end = row + 1
                    consecutive_empty = 0
                    for check_row in range(row + 1, min(row + 10000, sheet.max_row + 1)):
                        row_empty = True
                        for col in range(1, min(5, sheet.max_column + 1)):
                            if sheet.cell(check_row, col).value:
                                row_empty = False
                                break
                        if row_empty:
                            consecutive_empty += 1
                            if consecutive_empty >= 3:
                                data_end = check_row - consecutive_empty
                                break
                        else:
                            consecutive_empty = 0
                            data_end = check_row
                    
                    table_info = {
                        'header_row': row,
                        'data_start_row': row + 1,
                        'data_end_row': data_end,
                        'columns': matched_headers,
                        'column_count': non_empty,
                        'match_count': matches
                    }
                    tables.append(table_info)
                    last_table_end = data_end
                    logger.info(f"   📊 Found table at row {row}: {matches} columns matched, ~{data_end - row} data rows")
            
            # Also check for Excel named tables
            if hasattr(sheet, 'tables'):
                for table_name, table_range in sheet.tables.items():
                    logger.info(f"   📋 Found Excel table: '{table_name}' ({table_range})")
            
            logger.info(f"   ✓ Total tables found in '{sheet.title}': {len(tables)}")
            self.tables_found = tables
            return tables
            
        except Exception as e:
            logger.error(f"Error detecting tables: {e}")
            return []
    
    def populate_all_tables(
        self,
        sheet,
        data: pd.DataFrame,
        clear_existing: bool = False
    ) -> Dict[str, Any]:
        """
        Populate ALL tables found in a sheet.
        
        Returns comprehensive QA report for each table.
        """
        # First, detect all tables
        tables = self.detect_all_tables(sheet)
        
        if not tables:
            # Fallback to single-table mode
            logger.warning(f"No tables detected, using single-table mode")
            return self.populate_sheet(sheet, data, clear_existing)
        
        results = {
            "success": True,
            "tables_populated": 0,
            "tables_failed": 0,
            "total_rows_written": 0,
            "table_details": []
        }
        
        for table in tables:
            try:
                # Set the detected header row for this table
                self.header_row = table['header_row']
                self.data_start_row = table['data_start_row']
                
                # Map columns for this specific table, passing data columns for dynamic discovery
                self.column_mapping = self.map_columns(sheet, table['header_row'], data_columns=data.columns)
                
                if not self.column_mapping:
                    results['table_details'].append({
                        "header_row": table['header_row'],
                        "success": False,
                        "error": "No matching columns"
                    })
                    results['tables_failed'] += 1
                    continue
                
                # Write data to this table region
                rows_written, write_details = self._write_data_chunked(sheet, data)
                
                results['tables_populated'] += 1
                results['total_rows_written'] += rows_written
                results['table_details'].append({
                    "header_row": table['header_row'],
                    "success": True,
                    "rows_written": rows_written,
                    "columns_mapped": len(self.column_mapping),
                    "qa_details": write_details
                })
                
            except Exception as e:
                logger.error(f"Error populating table at row {table['header_row']}: {e}")
                results['tables_failed'] += 1
                results['table_details'].append({
                    "header_row": table['header_row'],
                    "success": False,
                    "error": str(e)
                })
        
        results['success'] = results['tables_failed'] == 0
        return results
    
    def detect_header_row(self, sheet, max_rows_to_check: int = 50) -> Optional[int]:
        """
        Detect which row contains the headers.
        
        Returns the 1-indexed row number, or None if not found.
        """
        try:
            for row in range(1, min(max_rows_to_check + 1, sheet.max_row + 1)):
                matches = 0
                non_empty = 0
                
                # Check first 30 columns only to avoid memory issues
                for col in range(1, min(sheet.max_column + 1, 30)):
                    try:
                        cell_value = sheet.cell(row, col).value
                        if cell_value:
                            non_empty += 1
                            cell_str = str(cell_value).lower().strip()
                            
                            # Check if this cell matches any of our patterns
                            for patterns in self.COLUMN_PATTERNS.values():
                                if any(pattern in cell_str for pattern in patterns):
                                    matches += 1
                                    break
                    except Exception as e:
                        logger.warning(f"Error reading cell ({row}, {col}): {e}")
                        continue
                
                # If we found at least 3 matching headers
                if matches >= 3 and non_empty > 0 and (matches / non_empty) >= 0.3:
                    logger.info(f"Detected header row at row {row} ({matches}/{non_empty} matches)")
                    return row
            
            logger.warning("Could not detect header row, defaulting to row 1")
            return 1
            
        except Exception as e:
            logger.error(f"Error detecting header row: {e}")
            return 1
    
    def map_columns(self, sheet, header_row: int, data_columns: List[str] = None) -> Dict[str, int]:
        """
        Map our data columns to the template's columns.
        
        Args:
            sheet: openpyxl worksheet
            header_row: row index for headers
            data_columns: Optional list of columns from the DataFrame for dynamic matching
            
        Returns dict mapping field names to column numbers.
        """
        mapping = {}
        # Ensure data_columns is a list-like of strings
        if data_columns is not None:
            data_cols_lower = {str(c).lower().strip(): c for c in data_columns}
        else:
            data_cols_lower = {}
        
        try:
            for col in range(1, min(sheet.max_column + 1, 100)):
                try:
                    header = sheet.cell(header_row, col).value
                    if not header:
                        continue
                    
                    header_str = str(header).lower().strip()
                    
                    # 1. Try to match against hardcoded COLUMN_PATTERNS (best for metrics/standard dims)
                    matched_field = None
                    for field_name, patterns in self.COLUMN_PATTERNS.items():
                        if any(pattern in header_str for pattern in patterns):
                            matched_field = field_name
                            break
                    
                    if matched_field:
                        mapping[matched_field] = col
                        logger.debug(f"Mapped '{matched_field}' → column {col} ('{header}')")
                        continue
                        
                    # 2. Try to match against data columns directly (for custom dimensions)
                    if header_str in data_cols_lower:
                        field_name = data_cols_lower[header_str]
                        mapping[field_name] = col
                        logger.debug(f"Mapped dynamic field '{field_name}' → column {col} ('{header}')")
                        
                except Exception as e:
                    logger.warning(f"Error reading header column {col}: {e}")
                    continue
            
            logger.info(f"Column mapping complete: {len(mapping)} columns mapped")
            return mapping
            
        except Exception as e:
            logger.error(f"Error mapping columns: {e}")
            return {}
    
    def populate_sheet(
        self,
        sheet,
        data: pd.DataFrame,
        clear_existing: bool = False
    ) -> Dict[str, Any]:
        """Intelligently populate a sheet with data."""
        qa_details = {
            "sheet_name": sheet.title,
            "template_headers": [],
            "data_columns": list(data.columns),
            "mapped_columns": {},
            "unmapped_template_columns": [],
            "unused_data_columns": [],
            "suggestions": []
        }
        
        try:
            self.header_row = self.detect_header_row(sheet)
            self.data_start_row = self.header_row + 1
            
            for col in range(1, min(sheet.max_column + 1, 100)):
                header = sheet.cell(self.header_row, col).value
                if header:
                    qa_details["template_headers"].append(str(header))
            
            # Map columns using data columns for dynamic discovery
            self.column_mapping = self.map_columns(sheet, self.header_row, data.columns)
            
            if not self.column_mapping:
                return {"success": False, "error": "No matching columns found", "qa_details": qa_details}
            
            if len(data) > self.MAX_ROWS:
                data = data.head(self.MAX_ROWS)
            
            if clear_existing and len(data) < 10000:
                self._clear_data_rows_safe(sheet)
            
            rows_written, write_details = self._write_data_chunked(sheet, data)
            
            qa_details.update(write_details)
            return {
                "success": True,
                "rows_written": rows_written,
                "header_row": self.header_row,
                "data_start_row": self.data_start_row,
                "columns_mapped": len(self.column_mapping),
                "qa_details": qa_details
            }
        except Exception as e:
            logger.exception(f"Failed to populate sheet: {e}")
            return {"success": False, "error": str(e), "qa_details": qa_details}
    
    def _clear_data_rows_safe(self, sheet):
        """Safely clear data rows."""
        try:
            target_cols = list(self.column_mapping.values())
            if not target_cols: return
            max_row_to_clear = min(sheet.max_row, self.data_start_row + 10000)
            for row in range(self.data_start_row, max_row_to_clear + 1):
                for col in target_cols:
                    sheet.cell(row, col).value = None
        except: pass
    
    def _write_data_chunked(self, sheet, data: pd.DataFrame) -> tuple:
        """Write data with dynamic column discovery."""
        rows_written = 0
        current_row = self.data_start_row
        total_rows = len(data)
        
        write_details = {
            "mapped_columns": {},
            "unmapped_template_columns": [],
            "unused_data_columns": [],
            "suggestions": []
        }
        
        # MAPPING TABLE: field_name -> possible DataFrame column names
        # We start with the core metrics and common names
        mapping_table = {
            'year_month': ['Year-Month'],
            'date': ['Date', 'Period'],
            'year': ['Year'],
            'month': ['Month'],
            'week': ['Week'],
            'day_of_week': ['Day of Week', 'Weekday'],
            'week_end': ['Week_End'],
            'platform': ['Platform', 'Channel'],
            'campaign': ['Campaign'],
            'impressions': ['Impressions'],
            'clicks': ['Clicks'],
            'conversions': ['Conversions'],
            'monthly_spend': ['Monthly Spend', 'Spend_USD'],
            'daily_spend': ['Daily Spend', 'Spend_USD'],
            'budget': ['Budget'],
            'spend': ['Spend_USD', 'Spend', 'Cost'],
            'revenue': ['Revenue_USD', 'Revenue'],
            'ctr': ['CTR'],
            'cpc': ['CPC'],
            'cpm': ['CPM'],
            'cpa': ['CPA'],
            'cvr': ['CVR'],
            'roas': ['ROAS'],
            'variance': ['Variance'],
            'variance_pct': ['Variance %', 'Variance_Pct'],
            'status': ['Status'],
            'alert': ['Alert'],
            'profit': ['Profit'],
            'margin_pct': ['Margin %', 'Margin_Pct']
        }
        
        active_mappings = []
        data_columns = set(data.columns)
        used_data_columns = set()
        
        # Build active mappings
        for field, col_idx in self.column_mapping.items():
            matched_col = None
            
            # 1. Try mapping_table
            if field in mapping_table:
                for possible in mapping_table[field]:
                    if possible in data_columns:
                        matched_col = possible
                        break
            
            # 2. Try direct match (for custom dimensions)
            if not matched_col and field in data_columns:
                matched_col = field
            
            # 3. Fallback: case-insensitive direct match
            if not matched_col:
                field_lower = field.lower()
                for dc in data_columns:
                    if dc.lower() == field_lower:
                        matched_col = dc
                        break
            
            if matched_col:
                active_mappings.append((col_idx, matched_col))
                write_details["mapped_columns"][field] = matched_col
                used_data_columns.add(matched_col)
            else:
                write_details["unmapped_template_columns"].append(field)
        
        write_details["unused_data_columns"] = list(data_columns - used_data_columns)
        
        # Performance optimizations
        active_mappings_sorted = sorted(active_mappings, key=lambda x: x[0])
        column_data = {dc: data[dc].values for _, dc in active_mappings_sorted}
        
        # Chunked write
        for chunk_start in range(0, total_rows, self.CHUNK_SIZE):
            chunk_end = min(chunk_start + self.CHUNK_SIZE, total_rows)
            for row_offset in range(chunk_end - chunk_start):
                row_idx = chunk_start + row_offset
                for col_idx, dc in active_mappings_sorted:
                    val = column_data[dc][row_idx]
                    if val is not None and (not isinstance(val, float) or val == val): # Not NaN
                        sheet.cell(row=current_row, column=col_idx, value=val)
                current_row += 1
                rows_written += 1
                
        return rows_written, write_details

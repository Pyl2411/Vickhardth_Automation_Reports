# app.py - Streamlit Version for Render.com Deployment
import streamlit as st
import pyodbc
import pandas as pd
import os
import logging
from datetime import datetime, timedelta
import sys
from openpyxl import Workbook, load_workbook
from openpyxl.styles import PatternFill, Font, Alignment, Border, Side
from openpyxl.utils import get_column_letter, column_index_from_string
import traceback
import shutil
from typing import Dict, List, Optional, Any, Tuple
import re
import tempfile
import base64
from io import BytesIO
import json

# Setup logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('table_exporter.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# ============================================================================
# DATABASE MANAGER WITH SSL FIX
# ============================================================================

class DatabaseManager:
    """Manages database connections and queries"""
    
    def __init__(self):
        self.connection = None
        self.connected = False
        self.server = None
        self.database = None
    
    def connect(self, server: str, database: str, 
                username: str = None, password: str = None,
                use_windows_auth: bool = True,
                encrypt: bool = True,
                trust_server_cert: bool = True) -> Tuple[bool, str]:
        """Connect to SQL Server database"""
        try:
            logger.info(f"Attempting to connect to {server}.{database}")
            
            # Build connection string with SSL fix
            if use_windows_auth:
                conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;"
            else:
                conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password};"
            
            # Add SSL/encryption options
            if encrypt:
                conn_str += "Encrypt=yes;"
            else:
                conn_str += "Encrypt=no;"
            
            # THIS IS THE FIX FOR SSL CERTIFICATE ERROR
            if trust_server_cert:
                conn_str += "TrustServerCertificate=yes;"
            
            logger.debug(f"Connection string: {conn_str}")
            
            # Attempt connection
            self.connection = pyodbc.connect(conn_str, timeout=30)
            self.connected = True
            self.server = server
            self.database = database
            
            logger.info(f"[OK] Connected to {server}.{database}")
            return True, "Connection successful"
            
        except pyodbc.Error as e:
            error_msg = f"Database connection error: {str(e)}"
            logger.error(error_msg)
            self.connected = False
            return False, error_msg
        except Exception as e:
            error_msg = f"Unexpected error during connection: {str(e)}"
            logger.error(error_msg)
            self.connected = False
            return False, error_msg
    
    def disconnect(self):
        """Disconnect from database"""
        try:
            if self.connection:
                self.connection.close()
                logger.info(f"[DISCONNECT] Disconnected from {self.server}.{self.database}")
        except Exception as e:
            logger.error(f"Error disconnecting: {e}")
        finally:
            self.connection = None
            self.connected = False
            self.server = None
            self.database = None
    
    def get_tables(self) -> List[str]:
        """Get list of tables in the database"""
        try:
            cursor = self.connection.cursor()
            
            # Query to get user tables (excluding system tables)
            query = """
            SELECT TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_NAME
            """
            
            cursor.execute(query)
            tables = [row[0] for row in cursor.fetchall()]
            cursor.close()
            
            logger.info(f"Retrieved {len(tables)} tables")
            return tables
            
        except Exception as e:
            logger.error(f"Error getting tables: {e}")
            raise
    
    def get_table_columns(self, table_name: str) -> List[str]:
        """Get column names for a specific table"""
        try:
            cursor = self.connection.cursor()
            
            # Query to get column names
            query = f"""
            SELECT COLUMN_NAME 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION
            """
            
            cursor.execute(query, (table_name,))
            columns = [row[0] for row in cursor.fetchall()]
            cursor.close()
            
            logger.info(f"Retrieved {len(columns)} columns for table {table_name}")
            return columns
            
        except Exception as e:
            logger.error(f"Error getting columns for {table_name}: {e}")
            raise
    
    def get_batches_from_table(self, table_name: str) -> List[str]:
        """Get distinct batch names from a table"""
        try:
            cursor = self.connection.cursor()
            
            # Try to find batch column
            columns = self.get_table_columns(table_name)
            batch_column = None
            
            # Look for common batch column names
            batch_keywords = ['BATCH', 'BATCH_NAME', 'BATCH_NUMBER', 'BATCH_NO', 'BATCHID']
            for col in columns:
                if any(keyword in col.upper() for keyword in batch_keywords):
                    batch_column = col
                    break
            
            if not batch_column:
                logger.warning(f"No batch column found in table {table_name}")
                return []
            
            # Get distinct batches
            query = f"SELECT DISTINCT [{batch_column}] FROM [{table_name}] WHERE [{batch_column}] IS NOT NULL ORDER BY [{batch_column}]"
            cursor.execute(query)
            batches = [row[0] for row in cursor.fetchall()]
            cursor.close()
            
            logger.info(f"Retrieved {len(batches)} batches from {table_name}")
            return batches
            
        except Exception as e:
            logger.error(f"Error getting batches from {table_name}: {e}")
            return []
    
    def get_time_columns(self, table_name: str) -> List[str]:
        """Get time-related columns from a table"""
        try:
            columns = self.get_table_columns(table_name)
            time_columns = []
            
            # Look for time-related columns
            time_keywords = ['TIME', 'DATE', 'TIMESTAMP', 'DATETIME', 'START', 'STOP', 'END']
            for col in columns:
                if any(keyword in col.upper() for keyword in time_keywords):
                    time_columns.append(col)
            
            logger.info(f"Found {len(time_columns)} time columns in {table_name}")
            return time_columns
            
        except Exception as e:
            logger.error(f"Error getting time columns from {table_name}: {e}")
            return []
    
    def fetch_filtered_data(self, table_name: str, batch_name: str = None, 
                          start_time: datetime = None, end_time: datetime = None,
                          limit: int = None) -> Dict:
        """Fetch data from a table with filters for batch and time range - VALUES ONLY, NO COLUMN NAMES"""
        try:
            logger.info(f"[FETCH] Fetching filtered data from table: {table_name}")
            
            # Get display name
            display_name = self.get_display_name(table_name)
            
            # Build WHERE clause for filters
            where_clauses = []
            params = []
            
            # We need to get columns temporarily to build WHERE clause
            temp_columns = self.get_table_columns(table_name)
            
            # Add batch filter if specified
            if batch_name:
                # Find batch column
                batch_column = None
                batch_keywords = ['BATCH', 'BATCH_NAME', 'BATCH_NUMBER', 'BATCH_NO', 'BATCHID']
                for col in temp_columns:
                    if any(keyword in col.upper() for keyword in batch_keywords):
                        batch_column = col
                        break
                
                if batch_column:
                    where_clauses.append(f"[{batch_column}] = ?")
                    params.append(batch_name)
                    logger.info(f"Filtering by batch: {batch_name} in column {batch_column}")
                else:
                    logger.warning(f"No batch column found for filtering")
            
            # Add time range filter if specified
            if start_time or end_time:
                # Find time column
                time_column = None
                time_keywords = ['TIME', 'TIMESTAMP', 'DATETIME', 'DATE_TIME', 'CREATED_AT']
                for col in temp_columns:
                    if any(keyword in col.upper() for keyword in time_keywords):
                        time_column = col
                        break
                
                if time_column:
                    if start_time:
                        where_clauses.append(f"[{time_column}] >= ?")
                        params.append(start_time)
                    if end_time:
                        where_clauses.append(f"[{time_column}] <= ?")
                        params.append(end_time)
                    logger.info(f"Filtering by time range in column {time_column}")
                else:
                    logger.warning(f"No time column found for filtering")
            
            # Build query - SELECT * to get all data
            query = f"SELECT TOP ({limit if limit and limit > 0 else 1000}) * FROM [{table_name}]"
            
            if where_clauses:
                query += " WHERE " + " AND ".join(where_clauses)
            
            # Add order by time if available
            time_columns = self.get_time_columns(table_name)
            if time_columns:
                query += f" ORDER BY [{time_columns[0]}]"
            
            logger.debug(f"Executing query: {query}")
            logger.debug(f"Parameters: {params}")
            
            # Execute query
            cursor = self.connection.cursor()
            cursor.execute(query, params)
            
            # Fetch all rows - JUST RAW DATA VALUES, NO COLUMN NAMES
            rows = cursor.fetchall()
            row_count = len(rows)
            
            # Convert to list of lists - PURE VALUES ONLY, NO COLUMN NAMES
            data = []
            for row in rows:
                row_list = []
                for value in row:
                    # Handle None values
                    if value is None:
                        value = ""
                    # Handle datetime objects
                    elif isinstance(value, datetime):
                        value = value.strftime('%Y-%m-%d %H:%M:%S')
                    # Convert everything to string (but keep empty strings as "")
                    row_list.append(str(value) if value is not None else "")
                data.append(row_list)
            
            cursor.close()
            
            # Log sample data (just values)
            if data:
                logger.debug(f"Sample filtered row from {table_name}: First 3 values - {data[0][:3]}")
            
            logger.info(f"[OK] Fetched {row_count} filtered rows from {table_name} (VALUES ONLY)")
            
            return {
                'success': True,
                'display_name': display_name,
                'table_name': table_name,
                'data': data,  # ONLY VALUES, no column names at all
                'row_count': row_count,
                'filters_applied': {
                    'batch': batch_name,
                    'start_time': start_time,
                    'end_time': end_time
                }
            }
            
        except Exception as e:
            error_msg = f"Error fetching filtered data from {table_name}: {str(e)}"
            logger.error(f"[ERROR] {error_msg}")
            logger.error(traceback.format_exc())
            return {
                'success': False,
                'error': error_msg,
                'display_name': self.get_display_name(table_name),
                'table_name': table_name,
                'data': [],
                'row_count': 0
            }
    
    def get_display_name(self, table_name: str) -> str:
        """Convert table name to display name"""
        # Remove underscores and capitalize
        display_name = table_name.replace('_', ' ').title()
        
        # Common replacements
        replacements = {
            'Tbl': '',
            'Table': '',
            'Vw': 'View: ',
            'Vw_': 'View: ',
            'View': 'View: '
        }
        
        for old, new in replacements.items():
            if display_name.startswith(old):
                display_name = display_name.replace(old, new, 1).strip()
        
        return display_name if display_name else table_name

# ============================================================================
# EXCEL EXPORTER WITH SHEET SELECTION SUPPORT
# ============================================================================

class ExcelTableExporter:
    """Handles exporting tables to Excel with position mapping and merged cell support"""
    
    @staticmethod
    def export_tables_to_template(tables_data: Dict, template_path: str, 
                                table_configs: Dict[str, Dict],
                                output_path: str,
                                merge_rules: List[str] = None) -> bool:
        """
        Export data into an existing template using position mappings.
        Template structure is kept AS IS, only values are filled in.
        IMPORTANT: Data fetched from DB contains VALUES ONLY, NO COLUMN NAMES
        """
        try:
            logger.info("="*60)
            logger.info("[START] STARTING TEMPLATE EXPORT")
            logger.info("="*60)
            logger.info(f"Template: {template_path}")
            logger.info(f"Output: {output_path}")
            logger.info(f"Tables to export: {list(tables_data.keys())}")
            
            # Check if we have data
            total_rows = sum(t.get('row_count', 0) for t in tables_data.values() if t.get('success', False))
            logger.info(f"Total rows to export: {total_rows}")
            
            if total_rows == 0:
                logger.warning("[WARNING] No data found to export!")
            
            # Make a copy of the template
            logger.info("[COPY] Copying template...")
            shutil.copy2(template_path, output_path)
            
            # Load the copied template
            logger.info("[LOAD] Loading template workbook...")
            wb = load_workbook(output_path)
            logger.info(f"[SHEETS] Workbook sheets: {wb.sheetnames}")
            
            # Apply user merge rules first (optional)
            if merge_rules:
                logger.info(f"Applying {len(merge_rules)} merge rules")
                for rule in merge_rules:
                    try:
                        if "!" in rule:
                            sheet_name, cell_range = rule.split("!", 1)
                            sheet_name = sheet_name.strip()
                            cell_range = cell_range.strip()
                            if sheet_name in wb.sheetnames:
                                wb[sheet_name].merge_cells(cell_range)
                                logger.info(f"[OK] Merged {sheet_name}!{cell_range}")
                    except Exception as e:
                        logger.warning(f"Failed to apply merge rule '{rule}': {e}")
            
            # Process each table
            for table_name, table_data in tables_data.items():
                logger.info("-"*40)
                logger.info(f"[PROCESS] Processing table: {table_name}")
                
                if not table_data.get('success', False):
                    logger.warning(f"[ERROR] Table {table_name} has error: {table_data.get('error')}")
                    continue
                
                if table_name not in table_configs:
                    logger.warning(f"[WARNING] No configuration found for table: {table_name}")
                    continue
                
                table_config = table_configs[table_name]
                logger.info(f"Table type: {'TABULAR' if table_config.get('start_row', 0) > 0 else 'HEADER'}")
                logger.info(f"Data: {table_data.get('row_count', 0)} rows (VALUES ONLY, NO COLUMN NAMES)")
                
                # Write individual column mappings (for header tables)
                if table_config.get('column_mappings'):
                    column_mappings = table_config['column_mappings']
                    logger.info(f"Processing {len(column_mappings)} column mappings")
                    
                    # IMPORTANT: For header tables, we need to get DB columns to know value positions
                    # We'll fetch columns from DB just for header tables
                    try:
                        # Get actual DB columns to know which position each mapped column is in
                        # But for simplicity, we'll assume the first row contains all values in order
                        
                        if table_data['data'] and len(table_data['data']) > 0:
                            first_row = table_data['data'][0]  # First data row (VALUES ONLY)
                            
                            for column_name, cell_mapping in column_mappings.items():
                                logger.debug(f"Mapping column: {column_name}")
                                
                                # Find which position this column is in the data
                                # The column_mappings should be in the same order as DB columns
                                # So we can use the index of column_name in column_mappings keys
                                column_index = list(column_mappings.keys()).index(column_name)
                                
                                # Get value from first row
                                value = ""
                                if column_index < len(first_row):
                                    value = first_row[column_index]
                                
                                logger.debug(f"Value for {column_name}: {value}")
                                
                                # Determine which sheets to write to
                                sheets_to_write = []
                                if cell_mapping.get('apply_to_all_sheets', False) or table_config.get('apply_to_all_sheets', False):
                                    # Write to all sheets
                                    sheets_to_write = wb.sheetnames
                                elif cell_mapping.get('selected_sheets'):
                                    # Write to selected sheets
                                    sheets_to_write = [s for s in cell_mapping['selected_sheets'] if s in wb.sheetnames]
                                elif table_config.get('selected_sheets'):
                                    # Write to table's selected sheets
                                    sheets_to_write = [s for s in table_config['selected_sheets'] if s in wb.sheetnames]
                                else:
                                    # Write to specific sheet only
                                    if cell_mapping.get('template_sheet') in wb.sheetnames:
                                        sheets_to_write = [cell_mapping['template_sheet']]
                                
                                # Write to each sheet
                                for sheet_name in sheets_to_write:
                                    success = ExcelTableExporter.write_to_cell_safe(
                                        wb, 
                                        sheet_name, 
                                        cell_mapping['template_cell'], 
                                        value
                                    )
                                    
                                    if success:
                                        logger.debug(f"[OK] Wrote '{value}' to {sheet_name}!{cell_mapping['template_cell']}")
                                    else:
                                        logger.warning(f"[ERROR] Could not write to {sheet_name}!{cell_mapping['template_cell']}")
                        else:
                            logger.warning(f"No data found for header table {table_name}")
                            
                    except Exception as e:
                        logger.warning(f"Error processing column mappings for {table_name}: {e}")
                
                # Write tabular data if start position is configured (for BACKGROUND/BATCH data)
                start_row = table_config.get('start_row', 0)
                start_col = table_config.get('start_col', '')
                
                if start_row > 0 and start_col:
                    logger.info(f"Writing tabular data starting at {start_col}{start_row}")
                    
                    if not table_data['data']:
                        logger.warning(f"[WARNING] No data found for table {table_name}")
                        continue
                    
                    # Determine which sheets to write to
                    sheets_to_write = []
                    if table_config.get('apply_to_all_sheets', False):
                        sheets_to_write = wb.sheetnames
                    elif table_config.get('selected_sheets'):
                        sheets_to_write = [s for s in table_config['selected_sheets'] if s in wb.sheetnames]
                    else:
                        sheet_name = table_config.get('sheet_name', '')
                        if sheet_name in wb.sheetnames:
                            sheets_to_write = [sheet_name]
                    
                    for sheet_name in sheets_to_write:
                        ws = wb[sheet_name]
                        start_col_idx = column_index_from_string(start_col)
                        
                        # Find first safe row
                        safe_row = ExcelTableExporter.find_safe_row_for_table(ws, start_row)
                        logger.info(f"Writing to sheet '{sheet_name}' starting at row {safe_row}")
                        
                        # NO HEADERS - Template already has headers
                        # Just write data starting from the specified position
                        
                        # Write data (PURE VALUES ONLY - no column names)
                        logger.info(f"Writing {len(table_data['data'])} data rows (VALUES ONLY)")
                        data_rows = table_data['data']  # This is a list of lists - PURE VALUES ONLY
                        for row_idx, row_data in enumerate(data_rows, start=0):  # Start from 0 to write at start row
                            for col_idx, value in enumerate(row_data, start=0):
                                cell_col = start_col_idx + col_idx
                                cell_row = safe_row + row_idx  # Start writing at the start row
                                cell_ref = f"{get_column_letter(cell_col)}{cell_row}"
                                
                                ExcelTableExporter.write_to_cell_safe(
                                    wb, sheet_name, cell_ref, value
                                )
                            
                            # Log progress every 10 rows
                            if row_idx % 10 == 0 and row_idx > 0:
                                logger.debug(f"  Processed {row_idx} rows...")
            
            # Save workbook
            logger.info("[SAVE] Saving workbook...")
            wb.save(output_path)
            logger.info("="*60)
            logger.info("[OK] TEMPLATE EXPORT COMPLETED SUCCESSFULLY")
            logger.info("="*60)
            return True
            
        except Exception as e:
            logger.error("="*60)
            logger.error("[ERROR] TEMPLATE EXPORT FAILED")
            logger.error("="*60)
            logger.error(f"Error: {str(e)}")
            logger.error(traceback.format_exc())
            raise
    
    @staticmethod
    def write_to_cell_safe(wb, sheet_name: str, cell_ref: str, value: Any) -> bool:
        """
        Safely write to a cell, handling merged cells.
        Returns True if successful, False if cell is in a merged range.
        """
        try:
            if sheet_name not in wb.sheetnames:
                logger.warning(f"Sheet '{sheet_name}' not found in workbook")
                return False
            
            ws = wb[sheet_name]
            
            # Parse cell reference
            col_letter = ''.join([c for c in cell_ref if c.isalpha()])
            row_num = int(''.join([c for c in cell_ref if c.isdigit()]))
            
            # Validate cell reference
            if not col_letter or not row_num:
                logger.warning(f"Invalid cell reference: {cell_ref}")
                return False
            
            col_num = column_index_from_string(col_letter)
            
            # Check if cell is part of a merged range
            for merged_range in ws.merged_cells.ranges:
                if (merged_range.min_row <= row_num <= merged_range.max_row and
                    merged_range.min_col <= col_num <= merged_range.max_col):
                    # Cell is in a merged range
                    # Try to write to the top-left cell of the merged range
                    top_left_cell = f"{get_column_letter(merged_range.min_col)}{merged_range.min_row}"
                    try:
                        ws[top_left_cell] = value
                        # Center alignment for merged cells
                        ws[top_left_cell].alignment = Alignment(horizontal='center', vertical='center')
                        logger.debug(f"[WRITE] Wrote to merged cell {top_left_cell} (original: {cell_ref})")
                        return True
                    except Exception as e:
                        logger.warning(f"Failed to write to merged cell {top_left_cell}: {e}")
                        return False
            
            # Cell is not merged, write normally
            ws[cell_ref] = value
            logger.debug(f"[WRITE] Wrote to cell {cell_ref}")
            return True
            
        except Exception as e:
            logger.error(f"[ERROR] Error writing to cell {sheet_name}!{cell_ref}: {e}")
            return False
    
    @staticmethod
    def find_safe_row_for_table(ws, start_row: int) -> int:
        """Find a safe row to start table data (not in merged cells)"""
        current_row = start_row
        
        # Check if start row is safe (not in merged cells in first 5 columns)
        for col in range(1, 6):  # Check first 5 columns
            cell_ref = f"{get_column_letter(col)}{current_row}"
            for merged_range in ws.merged_cells.ranges:
                if cell_ref in merged_range:
                    # Row is in merged range, try next row
                    current_row += 1
                    logger.debug(f"Row {current_row-1} is in merged range, trying row {current_row}")
                    return ExcelTableExporter.find_safe_row_for_table(ws, current_row)
        
        return current_row

# ============================================================================
# STREAMLIT APPLICATION
# ============================================================================

def main():
    st.set_page_config(
        page_title="Excel Table Exporter",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    st.title("📊 Excel Table Exporter with Database Connection")
    st.markdown("Export SQL Server tables to Excel templates with position mapping")
    
    # Initialize session state
    if 'db' not in st.session_state:
        st.session_state.db = DatabaseManager()
    if 'selected_tables' not in st.session_state:
        st.session_state.selected_tables = []
    if 'table_configs' not in st.session_state:
        st.session_state.table_configs = {}
    if 'filters' not in st.session_state:
        st.session_state.filters = {}
    if 'template_path' not in st.session_state:
        st.session_state.template_path = None
    if 'template_sheets' not in st.session_state:
        st.session_state.template_sheets = []
    if 'tables_list' not in st.session_state:
        st.session_state.tables_list = []
    if 'current_step' not in st.session_state:
        st.session_state.current_step = 1
    
    # Sidebar for navigation
    with st.sidebar:
        st.header("Navigation")
        
        # Step indicators
        steps = [
            ("🔗", "Connection", 1),
            ("📋", "Table Selection", 2),
            ("📍", "Position Mapping", 3),
            ("⚙️", "Filters", 4),
            ("📤", "Export", 5)
        ]
        
        for icon, name, step_num in steps:
            if step_num == st.session_state.current_step:
                st.markdown(f"**{icon} {name}**")
            else:
                if st.button(f"{icon} {name}", key=f"nav_{step_num}", use_container_width=True):
                    st.session_state.current_step = step_num
                    st.rerun()
        
        st.divider()
        
        # Connection status
        if st.session_state.db.connected:
            st.success("✅ Connected")
            if st.button("🔌 Disconnect"):
                st.session_state.db.disconnect()
                st.session_state.tables_list = []
                st.session_state.selected_tables = []
                st.success("Disconnected")
                st.rerun()
        else:
            st.warning("⚠️ Not Connected")
        
        # Selected tables count
        if st.session_state.selected_tables:
            st.info(f"📋 {len(st.session_state.selected_tables)} tables selected")
    
    # Main content based on current step
    if st.session_state.current_step == 1:
        show_connection_tab()
    elif st.session_state.current_step == 2:
        show_table_selection_tab()
    elif st.session_state.current_step == 3:
        show_position_mapping_tab()
    elif st.session_state.current_step == 4:
        show_filters_tab()
    elif st.session_state.current_step == 5:
        show_export_tab()

def show_connection_tab():
    """Show connection tab"""
    st.header("Step 1: Database Connection")
    
    col1, col2 = st.columns(2)
    with col1:
        server = st.text_input("Server", value="MAHESHWAGH\\WINCC")
        database = st.text_input("Database", value="VPI1")
    
    with col2:
        auth_type = st.radio("Authentication", ["Windows Authentication", "SQL Server Authentication"])
        
        if auth_type == "SQL Server Authentication":
            username = st.text_input("Username")
            password = st.text_input("Password", type="password")
        else:
            username = None
            password = None
    
    # SSL/Encryption options
    st.subheader("SSL/Encryption Settings")
    col1, col2 = st.columns(2)
    with col1:
        encrypt = st.checkbox("Encrypt connection", value=True)
    with col2:
        trust_cert = st.checkbox("Trust Server Certificate", value=True, 
                                help="Check this to fix SSL certificate errors")
    
    # Connection buttons
    col1, col2, col3 = st.columns(3)
    with col1:
        if st.button("🔗 Connect & Next", type="primary", use_container_width=True):
            with st.spinner("Connecting to database..."):
                use_windows_auth = auth_type == "Windows Authentication"
                success, message = st.session_state.db.connect(
                    server=server,
                    database=database,
                    username=username,
                    password=password,
                    use_windows_auth=use_windows_auth,
                    encrypt=encrypt,
                    trust_server_cert=trust_cert
                )
                
                if success:
                    st.success("✅ Connected successfully!")
                    # Load tables
                    st.session_state.tables_list = st.session_state.db.get_tables()
                    # Move to next step
                    st.session_state.current_step = 2
                    st.rerun()
                else:
                    st.error(f"❌ Connection failed: {message}")
    
    with col2:
        if st.button("🔗 Connect", use_container_width=True):
            with st.spinner("Connecting to database..."):
                use_windows_auth = auth_type == "Windows Authentication"
                success, message = st.session_state.db.connect(
                    server=server,
                    database=database,
                    username=username,
                    password=password,
                    use_windows_auth=use_windows_auth,
                    encrypt=encrypt,
                    trust_server_cert=trust_cert
                )
                
                if success:
                    st.success("✅ Connected successfully!")
                    # Load tables
                    st.session_state.tables_list = st.session_state.db.get_tables()
                    st.rerun()
                else:
                    st.error(f"❌ Connection failed: {message}")
    
    with col3:
        if st.button("🔄 Test Connection", use_container_width=True):
            with st.spinner("Testing connection..."):
                use_windows_auth = auth_type == "Windows Authentication"
                success, message = st.session_state.db.connect(
                    server=server,
                    database=database,
                    username=username,
                    password=password,
                    use_windows_auth=use_windows_auth,
                    encrypt=encrypt,
                    trust_server_cert=trust_cert
                )
                
                if success:
                    st.success("✅ Connection test successful!")
                    st.session_state.db.disconnect()
                else:
                    st.error(f"❌ Connection test failed: {message}")
    
    # Show connection status
    if st.session_state.db.connected:
        st.info(f"✅ Connected to {st.session_state.db.server}.{st.session_state.db.database}")
    else:
        st.warning("⚠️ Not connected to database")

def show_table_selection_tab():
    """Show table selection tab"""
    st.header("Step 2: Table Selection")
    
    if not st.session_state.db.connected:
        st.warning("Please connect to database first")
        if st.button("← Go to Connection"):
            st.session_state.current_step = 1
            st.rerun()
        return
    
    col1, col2 = st.columns([3, 1])
    
    with col1:
        st.subheader(f"Available Tables ({len(st.session_state.tables_list)})")
    
    with col2:
        if st.button("🔄 Refresh", use_container_width=True):
            st.session_state.tables_list = st.session_state.db.get_tables()
            st.rerun()
    
    # Table selection
    if st.session_state.tables_list:
        # Select all/none buttons
        col1, col2 = st.columns(2)
        with col1:
            if st.button("✅ Select All", use_container_width=True):
                st.session_state.selected_tables = st.session_state.tables_list.copy()
                st.rerun()
        with col2:
            if st.button("❌ Clear All", use_container_width=True):
                st.session_state.selected_tables = []
                st.rerun()
        
        # Multi-select for tables
        selected = st.multiselect(
            "Select tables to export:",
            st.session_state.tables_list,
            default=st.session_state.selected_tables,
            placeholder="Choose tables..."
        )
        
        st.session_state.selected_tables = selected
        
        st.info(f"Selected {len(selected)} table(s)")
        
        # Show selected tables
        if selected:
            st.write("Selected tables:")
            for i, table in enumerate(selected, 1):
                st.write(f"{i}. {table}")
        
        # Navigation buttons
        col1, col2, col3 = st.columns(3)
        with col1:
            if st.button("← Previous: Connection", use_container_width=True):
                st.session_state.current_step = 1
                st.rerun()
        
        with col3:
            if st.button("Next: Position Mapping →", type="primary", use_container_width=True):
                if not st.session_state.selected_tables:
                    st.warning("Please select at least one table")
                else:
                    st.session_state.current_step = 3
                    st.rerun()
    else:
        st.info("No tables found in database")
        
        # Navigation button
        if st.button("← Previous: Connection"):
            st.session_state.current_step = 1
            st.rerun()

def show_position_mapping_tab():
    """Show position mapping tab"""
    st.header("Step 3: Position Mapping")
    
    if not st.session_state.selected_tables:
        st.warning("Please select tables first")
        if st.button("← Go to Table Selection"):
            st.session_state.current_step = 2
            st.rerun()
        return
    
    # Template upload
    st.subheader("Upload Excel Template")
    uploaded_template = st.file_uploader(
        "Choose an Excel template file", 
        type=['xlsx', 'xls']
    )
    
    if uploaded_template is not None:
        # Save uploaded template
        temp_dir = tempfile.gettempdir()
        template_path = os.path.join(temp_dir, uploaded_template.name)
        
        with open(template_path, "wb") as f:
            f.write(uploaded_template.getbuffer())
        
        st.session_state.template_path = template_path
        
        # Get sheet names
        try:
            wb = load_workbook(template_path, read_only=True)
            st.session_state.template_sheets = wb.sheetnames
            st.success(f"✅ Template loaded with {len(st.session_state.template_sheets)} sheets")
            
            # Show sheet names
            with st.expander("View Sheets"):
                for sheet in st.session_state.template_sheets:
                    st.write(f"• {sheet}")
        except Exception as e:
            st.error(f"Error reading template: {e}")
    elif st.session_state.template_path:
        st.info(f"Template loaded: {os.path.basename(st.session_state.template_path)}")
    
    # Merge rules
    st.subheader("Merge Cell Rules (Optional)")
    merge_rules_text = st.text_area(
        "Enter merge ranges (one per line): SheetName!StartCell:EndCell",
        height=100,
        help="Example: Sheet1!B4:D4  (merges B4, C4, D4)\nExample: Sheet1!A1:C1  (merges A1, B1, C1)"
    )
    
    # Position configuration for each table
    st.subheader("Configure Position Mappings")
    
    if not st.session_state.template_path:
        st.warning("Please upload a template first")
    else:
        # Store configurations temporarily
        temp_configs = {}
        
        for table_name in st.session_state.selected_tables:
            with st.expander(f"Configure {table_name}", expanded=False):
                # Determine if this is a simple table or needs column mapping
                is_simple = any(keyword in table_name.upper() for keyword in 
                               ['BACKGROUND', 'BATCH', 'DATA'])
                
                if is_simple:
                    st.write("**Simple Table (BACKGROUND/BATCH/DATA)**")
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        sheet = st.selectbox(
                            f"Select sheet for {table_name}",
                            st.session_state.template_sheets,
                            key=f"sheet_{table_name}"
                        )
                    
                    with col2:
                        start_cell = st.text_input(
                            f"Start cell for {table_name}",
                            value="A1",
                            key=f"cell_{table_name}",
                            help="Enter cell reference like B4, C10, D20"
                        )
                    
                    # Apply to all sheets option
                    apply_all = st.checkbox(
                        f"Apply to all sheets",
                        value=False,
                        key=f"apply_all_{table_name}"
                    )
                    
                    selected_sheets = []
                    if not apply_all:
                        selected_sheets = st.multiselect(
                            f"Select specific sheets for {table_name}",
                            st.session_state.template_sheets,
                            default=[sheet],
                            key=f"selected_sheets_{table_name}"
                        )
                    else:
                        selected_sheets = st.session_state.template_sheets
                    
                    # Validate and save configuration
                    if st.button(f"Save configuration for {table_name}", key=f"save_simple_{table_name}"):
                        if not re.match(r'^[A-Z]+\d+$', start_cell.upper()):
                            st.error("Invalid cell format. Use like B4, C10, D20")
                        else:
                            # Parse cell
                            col_letter = ''.join([c for c in start_cell.upper() if c.isalpha()])
                            row_num = int(''.join([c for c in start_cell.upper() if c.isdigit()])
                            
                            temp_configs[table_name] = {
                                'table_name': table_name,
                                'display_name': st.session_state.db.get_display_name(table_name),
                                'start_row': row_num,
                                'start_col': col_letter,
                                'sheet_name': sheet,
                                'column_mappings': {},
                                'apply_to_all_sheets': apply_all,
                                'selected_sheets': selected_sheets
                            }
                            st.success(f"Configuration saved for {table_name}")
                
                else:
                    st.write("**Header/Static Data Table**")
                    st.info("For header tables, map individual columns to specific cells")
                    
                    # Get columns for this table
                    try:
                        columns = st.session_state.db.get_table_columns(table_name)
                        
                        st.write(f"Columns in {table_name}:")
                        column_mappings = {}
                        
                        for col in columns:
                            col1, col2, col3 = st.columns([2, 2, 1])
                            
                            with col1:
                                sheet = st.selectbox(
                                    f"Sheet for {col}",
                                    st.session_state.template_sheets,
                                    key=f"sheet_{table_name}_{col}"
                                )
                            
                            with col2:
                                cell = st.text_input(
                                    f"Cell for {col}",
                                    value="",
                                    key=f"cell_{table_name}_{col}",
                                    placeholder="e.g., B4, C4"
                                )
                            
                            with col3:
                                apply_type = st.selectbox(
                                    f"Apply to",
                                    ["This Sheet", "All Sheets"],
                                    key=f"apply_{table_name}_{col}"
                                )
                            
                            if cell:
                                column_mappings[col] = {
                                    'table_name': table_name,
                                    'column_name': col,
                                    'template_sheet': sheet,
                                    'template_cell': cell.upper(),
                                    'apply_to_all_sheets': apply_type == "All Sheets",
                                    'selected_sheets': [sheet] if apply_type == "This Sheet" else st.session_state.template_sheets
                                }
                        
                        # Save button for column mappings
                        if st.button(f"Save column mappings for {table_name}", key=f"save_header_{table_name}"):
                            valid = True
                            
                            for col, mapping in column_mappings.items():
                                cell_val = mapping['template_cell']
                                if not re.match(r'^[A-Z]+\d+$', cell_val):
                                    st.error(f"Invalid cell format for {col}: {cell_val}")
                                    valid = False
                                    break
                            
                            if valid:
                                temp_configs[table_name] = {
                                    'table_name': table_name,
                                    'display_name': st.session_state.db.get_display_name(table_name),
                                    'start_row': 0,
                                    'start_col': '',
                                    'sheet_name': '',
                                    'column_mappings': column_mappings,
                                    'apply_to_all_sheets': False,
                                    'selected_sheets': []
                                }
                                st.success(f"Column mappings saved for {table_name}")
                    
                    except Exception as e:
                        st.error(f"Error getting columns: {e}")
        
        # Save all configurations
        if temp_configs and st.button("💾 Save All Configurations", type="primary"):
            st.session_state.table_configs.update(temp_configs)
            st.success(f"Saved configurations for {len(temp_configs)} tables")
    
    # Show current configurations
    st.subheader("Current Configurations")
    if st.session_state.table_configs:
        for table_name, config in st.session_state.table_configs.items():
            with st.expander(f"{config['display_name']}", expanded=False):
                if config['start_row'] > 0:
                    st.write(f"**Type:** Data Table")
                    st.write(f"**Start Position:** {config['start_col']}{config['start_row']}")
                    if config.get('selected_sheets'):
                        sheets = config['selected_sheets']
                        if len(sheets) > 3:
                            st.write(f"**Sheets:** {', '.join(sheets[:3])} (+{len(sheets)-3} more)")
                        else:
                            st.write(f"**Sheets:** {', '.join(sheets)}")
                else:
                    st.write(f"**Type:** Header Table")
                    st.write(f"**Column Mappings:** {len(config['column_mappings'])}")
                    for col_name, mapping in config['column_mappings'].items():
                        st.write(f"  • {col_name} → {mapping['template_cell']}")
    else:
        st.info("No configurations saved yet")
    
    # Navigation buttons
    col1, col2, col3 = st.columns(3)
    with col1:
        if st.button("← Previous: Table Selection", use_container_width=True):
            st.session_state.current_step = 2
            st.rerun()
    
    with col3:
        if st.button("Next: Filters →", type="primary", use_container_width=True):
            if not st.session_state.table_configs:
                st.warning("Please configure position mappings first")
            else:
                st.session_state.current_step = 4
                st.rerun()

def show_filters_tab():
    """Show filters tab"""
    st.header("Step 4: Data Filters")
    
    if not st.session_state.selected_tables:
        st.warning("Please select tables first")
        if st.button("← Go to Table Selection"):
            st.session_state.current_step = 2
            st.rerun()
        return
    
    for table_name in st.session_state.selected_tables:
        with st.expander(f"Filters for {table_name}", expanded=False):
            # Get batches for this table
            batches = st.session_state.db.get_batches_from_table(table_name)
            
            if batches:
                batch = st.selectbox(
                    f"Select batch for {table_name}",
                    batches,
                    key=f"batch_{table_name}"
                )
            else:
                st.info("No batch column found in this table")
                batch = None
            
            # Time range
            enable_time = st.checkbox(f"Enable time filtering for {table_name}", 
                                    key=f"enable_time_{table_name}")
            
            if enable_time:
                col1, col2 = st.columns(2)
                with col1:
                    start_date = st.date_input(
                        f"Start date for {table_name}",
                        value=datetime.now() - timedelta(days=1),
                        key=f"start_date_{table_name}"
                    )
                    start_time = st.time_input(
                        f"Start time for {table_name}",
                        value=datetime.now().time(),
                        key=f"start_time_{table_name}"
                    )
                
                with col2:
                    end_date = st.date_input(
                        f"End date for {table_name}",
                        value=datetime.now(),
                        key=f"end_date_{table_name}"
                    )
                    end_time = st.time_input(
                        f"End time for {table_name}",
                        value=datetime.now().time(),
                        key=f"end_time_{table_name}"
                    )
                
                start_datetime = datetime.combine(start_date, start_time)
                end_datetime = datetime.combine(end_date, end_time)
            else:
                start_datetime = None
                end_datetime = None
            
            # Save filters
            if st.button(f"Save filters for {table_name}", key=f"save_filters_{table_name}"):
                st.session_state.filters[table_name] = {
                    'batch': batch,
                    'start_time': start_datetime,
                    'end_time': end_datetime
                }
                st.success(f"Filters saved for {table_name}")
    
    # Show current filters
    st.subheader("Current Filters")
    if st.session_state.filters:
        for table_name, filters in st.session_state.filters.items():
            st.write(f"**{table_name}:**")
            if filters['batch']:
                st.write(f"  • Batch: {filters['batch']}")
            if filters['start_time'] and filters['end_time']:
                st.write(f"  • Time: {filters['start_time']} to {filters['end_time']}")
    else:
        st.info("No filters configured")
    
    # Navigation buttons
    col1, col2, col3 = st.columns(3)
    with col1:
        if st.button("← Previous: Position Mapping", use_container_width=True):
            st.session_state.current_step = 3
            st.rerun()
    
    with col3:
        if st.button("Next: Export →", type="primary", use_container_width=True):
            st.session_state.current_step = 5
            st.rerun()

def show_export_tab():
    """Show export tab"""
    st.header("Step 5: Export Data")
    
    if not st.session_state.selected_tables:
        st.warning("Please select tables first")
        if st.button("← Go to Table Selection"):
            st.session_state.current_step = 2
            st.rerun()
        return
    
    if not st.session_state.template_path:
        st.warning("Please upload a template first")
        if st.button("← Go to Position Mapping"):
            st.session_state.current_step = 3
            st.rerun()
        return
    
    # Export options
    st.subheader("Export Settings")
    
    col1, col2 = st.columns(2)
    with col1:
        row_limit = st.number_input(
            "Row limit (0 = all)",
            min_value=0,
            value=0,
            help="Limit the number of rows fetched from each table"
        )
    
    with col2:
        default_filename = f"TemplateExport_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        filename = st.text_input(
            "Output filename",
            value=default_filename
        )
    
    # Get merge rules from previous step
    merge_rules_text = ""  # This would come from a session state
    
    # Export button
    if st.button("🚀 Export to Template", type="primary", use_container_width=True):
        with st.spinner("Exporting data..."):
            try:
                # Prepare merge rules
                merge_rules = [line.strip() for line in merge_rules_text.splitlines() if line.strip()]
                
                # Fetch data
                tables_data = {}
                for table_name in st.session_state.selected_tables:
                    filters = st.session_state.filters.get(table_name, {})
                    
                    data = st.session_state.db.fetch_filtered_data(
                        table_name=table_name,
                        batch_name=filters.get('batch'),
                        start_time=filters.get('start_time'),
                        end_time=filters.get('end_time'),
                        limit=row_limit if row_limit > 0 else None
                    )
                    
                    tables_data[table_name] = data
                
                # Create temporary output file
                temp_dir = tempfile.gettempdir()
                output_path = os.path.join(temp_dir, filename)
                
                # Export to template
                exporter = ExcelTableExporter()
                success = exporter.export_tables_to_template(
                    tables_data=tables_data,
                    template_path=st.session_state.template_path,
                    table_configs=st.session_state.table_configs,
                    output_path=output_path,
                    merge_rules=merge_rules
                )
                
                if success:
                    st.success("✅ Export completed successfully!")
                    
                    # Provide download link
                    with open(output_path, "rb") as f:
                        bytes_data = f.read()
                        b64 = base64.b64encode(bytes_data).decode()
                        href = f'<a href="data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{b64}" download="{filename}">📥 Download Excel File</a>'
                        st.markdown(href, unsafe_allow_html=True)
                    
                    # Show summary
                    st.subheader("Export Summary")
                    total_rows = sum(t.get('row_count', 0) for t in tables_data.values() if t.get('success', False))
                    st.write(f"• Tables exported: {len([t for t in tables_data.values() if t.get('success', False)])}")
                    st.write(f"• Total rows: {total_rows}")
                    st.write(f"• Output file: {filename}")
                
            except Exception as e:
                st.error(f"❌ Export failed: {str(e)}")
                st.exception(e)
    
    # Navigation buttons
    col1, col2 = st.columns(2)
    with col1:
        if st.button("← Previous: Filters", use_container_width=True):
            st.session_state.current_step = 4
            st.rerun()

# ============================================================================
# RUN THE APP
# ============================================================================

if __name__ == "__main__":
    main()
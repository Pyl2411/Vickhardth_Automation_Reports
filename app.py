"""
Excel Table Exporter - Complete Streamlit Application
Version: 4.0.0 - Production Ready
Description: Export SQL Server tables to Excel templates with position mapping
With advanced filtering and multi-sheet support
"""

import streamlit as st
import pandas as pd
import os
import sys
import logging
from datetime import datetime, timedelta
from openpyxl import load_workbook, Workbook
from openpyxl.styles import Alignment
from openpyxl.utils import get_column_letter, column_index_from_string
import traceback
import shutil
import re
import tempfile
import base64
from io import BytesIO
from typing import Dict, List, Optional, Any, Tuple
import json
from dataclasses import dataclass, field
import urllib.parse
import warnings

# Suppress warnings for cleaner output
warnings.filterwarnings('ignore')

# Database imports - using SQLAlchemy for compatibility
try:
    from sqlalchemy import create_engine, text, inspect
    from sqlalchemy.exc import SQLAlchemyError
    SQLALCHEMY_AVAILABLE = True
except ImportError as e:
    SQLALCHEMY_AVAILABLE = False
    st.error(f"SQLAlchemy not available: {e}. Please install it.")

# Try to import pyodbc with fallback
try:
    import pyodbc
    PYODBC_AVAILABLE = True
except ImportError as e:
    PYODBC_AVAILABLE = False
    st.warning(f"pyodbc not available: {e}. Using SQLAlchemy with connection strings.")

# ============================================================================
# LOGGING SETUP (Streamlit Cloud Compatible)
# ============================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# ============================================================================
# DATA CLASSES
# ============================================================================

@dataclass
class CellMapping:
    """Mapping information for a single cell"""
    table_name: str
    column_name: str
    template_sheet: str
    template_cell: str  # e.g., "A1", "B3", "C5"
    apply_to_all_sheets: bool = False
    selected_sheets: List[str] = field(default_factory=list)
    
@dataclass
class TableConfig:
    """Configuration for a data table"""
    table_name: str
    display_name: str
    start_row: int
    start_col: str
    sheet_name: str
    column_mappings: Dict[str, CellMapping] = field(default_factory=dict)
    apply_to_all_sheets: bool = False
    selected_sheets: List[str] = field(default_factory=list)

# ============================================================================
# DATABASE MANAGER (pyodbc with fallback)
# ============================================================================

class DatabaseManager:
    """Manages database connections using pyodbc with Windows Authentication"""
    
    def __init__(self):
        self.engine = None
        self.connected = False
        self.server = None
        self.database = None
        self.driver = None
        self.table_cache = {}
        self.column_cache = {}
    
    def connect(self, server: str, database: str) -> Tuple[bool, str]:
        """Connect to SQL Server with optimized pyodbc connection"""
        if not SQLALCHEMY_AVAILABLE:
            return False, "SQLAlchemy not available"
        
        try:
            logger.info(f"Connecting to {server}.{database}")
            
            # Clear caches
            self.table_cache.clear()
            self.column_cache.clear()
            
            # Try different connection methods in order
            connection_methods = []
            
            # Method 1: ODBC Driver 17 for SQL Server (most reliable)
            connection_methods.append({
                'name': 'ODBC Driver 17 for SQL Server',
                'string': f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;TrustServerCertificate=yes;",
                'requires_pyodbc': True
            })
            
            # Method 2: SQL Server Native Client 11.0
            connection_methods.append({
                'name': 'SQL Server Native Client 11.0',
                'string': f"DRIVER={{SQL Server Native Client 11.0}};SERVER={server};DATABASE={database};Trusted_Connection=yes;",
                'requires_pyodbc': True
            })
            
            # Method 3: SQL Server (older)
            connection_methods.append({
                'name': 'SQL Server',
                'string': f"DRIVER={{SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;",
                'requires_pyodbc': True
            })
            
            # Method 4: pymssql fallback (if pyodbc fails)
            if not PYODBC_AVAILABLE:
                connection_methods.append({
                    'name': 'pymssql',
                    'string': f"mssql+pymssql://{server}/{database}?charset=utf8",
                    'requires_pyodbc': False
                })
            
            # Try each method
            for method in connection_methods:
                try:
                    logger.info(f"Trying: {method['name']}")
                    
                    if method['requires_pyodbc'] and not PYODBC_AVAILABLE:
                        continue
                    
                    if method['requires_pyodbc']:
                        # Use pyodbc connection string
                        params = urllib.parse.quote_plus(method['string'])
                        engine_string = f"mssql+pyodbc:///?odbc_connect={params}"
                    else:
                        # Use direct SQLAlchemy URL
                        engine_string = method['string']
                    
                    # Create engine with optimized settings
                    self.engine = create_engine(
                        engine_string,
                        pool_pre_ping=True,
                        pool_recycle=3600,
                        echo=False,
                        future=True
                    )
                    
                    # Test connection with timeout
                    with self.engine.connect() as conn:
                        result = conn.execute(text("SELECT 1 AS test"))
                        test_val = result.fetchone()[0]
                        if test_val != 1:
                            raise Exception("Test query failed")
                    
                    self.connected = True
                    self.server = server
                    self.database = database
                    self.driver = method['name']
                    
                    logger.info(f"✅ Connected using {method['name']}")
                    return True, f"Connected using {method['name']}"
                    
                except Exception as e:
                    logger.warning(f"Method {method['name']} failed: {str(e)}")
                    continue
            
            return False, "All connection attempts failed. Please check:\n1. SQL Server is running\n2. Windows Authentication is enabled\n3. ODBC drivers are installed"
            
        except Exception as e:
            error_msg = f"Connection error: {str(e)}"
            logger.error(f"{error_msg}\n{traceback.format_exc()}")
            return False, error_msg
    
    def disconnect(self):
        """Disconnect from database"""
        try:
            if self.engine:
                self.engine.dispose()
            self.table_cache.clear()
            self.column_cache.clear()
            logger.info("Disconnected")
        except Exception as e:
            logger.error(f"Error disconnecting: {e}")
        finally:
            self.engine = None
            self.connected = False
            self.server = None
            self.database = None
            self.driver = None
    
    def get_tables(self) -> List[str]:
        """Get list of tables with caching"""
        cache_key = f"{self.database}_tables"
        
        if cache_key in self.table_cache:
            return self.table_cache[cache_key]
        
        try:
            query = """
            SELECT TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_NAME
            """
            
            with self.engine.connect() as conn:
                result = conn.execute(text(query))
                tables = [row[0] for row in result.fetchall()]
            
            logger.info(f"Found {len(tables)} tables")
            self.table_cache[cache_key] = tables
            return tables
            
        except Exception as e:
            logger.error(f"Error getting tables: {e}")
            return []
    
    def get_table_columns(self, table_name: str) -> List[str]:
        """Get column names for a table"""
        cache_key = f"{table_name}_columns"
        
        if cache_key in self.column_cache:
            return self.column_cache[cache_key]
        
        try:
            query = """
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = :table_name
            ORDER BY ORDINAL_POSITION
            """
            
            with self.engine.connect() as conn:
                result = conn.execute(text(query), {"table_name": table_name})
                columns = [row[0] for row in result.fetchall()]
            
            logger.info(f"Found {len(columns)} columns in {table_name}")
            self.column_cache[cache_key] = columns
            return columns
            
        except Exception as e:
            logger.error(f"Error getting columns: {e}")
            return []
    
    def get_batches_from_table(self, table_name: str) -> List[str]:
        """Get distinct batch names from a table"""
        try:
            columns = self.get_table_columns(table_name)
            batch_column = None
            
            # Look for batch column
            batch_keywords = ['BATCH', 'BATCH_NAME', 'BATCH_NUMBER', 'BATCH_NO', 'BATCHID', 'LOT', 'LOT_NO']
            for col in columns:
                col_upper = col.upper()
                if any(keyword in col_upper for keyword in batch_keywords):
                    batch_column = col
                    break
            
            if not batch_column:
                return []
            
            query = f"SELECT DISTINCT [{batch_column}] FROM [{table_name}] WHERE [{batch_column}] IS NOT NULL ORDER BY [{batch_column}]"
            
            with self.engine.connect() as conn:
                result = conn.execute(text(query))
                batches = [str(row[0]) for row in result.fetchall()]
            
            return batches
            
        except Exception as e:
            logger.error(f"Error getting batches: {e}")
            return []
    
    def fetch_filtered_data(self, table_name: str, batch_name: str = None, 
                          start_time: datetime = None, end_time: datetime = None,
                          limit: int = None) -> Dict:
        """Fetch data from a table with filters"""
        try:
            logger.info(f"Fetching data from {table_name}")
            
            display_name = self.get_display_name(table_name)
            columns = self.get_table_columns(table_name)
            
            if not columns:
                return {
                    'success': False,
                    'error': f"No columns found in {table_name}",
                    'display_name': display_name,
                    'table_name': table_name,
                    'data': [],
                    'row_count': 0
                }
            
            # Build WHERE clause
            where_clauses = []
            params = {}
            
            # Batch filter
            if batch_name:
                batch_column = self._find_batch_column(columns)
                if batch_column:
                    where_clauses.append(f"[{batch_column}] = :batch_name")
                    params["batch_name"] = batch_name
            
            # Time filter
            if start_time or end_time:
                time_column = self._find_time_column(columns)
                if time_column:
                    if start_time:
                        where_clauses.append(f"[{time_column}] >= :start_time")
                        params["start_time"] = start_time
                    if end_time:
                        where_clauses.append(f"[{time_column}] <= :end_time")
                        params["end_time"] = end_time
            
            # Build query
            if limit and limit > 0:
                query = f"SELECT TOP ({limit}) * FROM [{table_name}]"
            else:
                query = f"SELECT * FROM [{table_name}]"
            
            if where_clauses:
                query += " WHERE " + " AND ".join(where_clauses)
            
            # Execute query
            with self.engine.connect() as conn:
                df = pd.read_sql_query(text(query), conn, params=params)
            
            # Convert to list of lists
            data = []
            for _, row in df.iterrows():
                row_list = []
                for value in row:
                    if pd.isna(value):
                        row_list.append("")
                    elif isinstance(value, datetime):
                        row_list.append(value.strftime('%Y-%m-%d %H:%M:%S'))
                    elif isinstance(value, timedelta):
                        total_seconds = int(value.total_seconds())
                        hours, remainder = divmod(total_seconds, 3600)
                        minutes, seconds = divmod(remainder, 60)
                        row_list.append(f"{hours:02d}:{minutes:02d}:{seconds:02d}")
                    elif isinstance(value, (int, float)):
                        row_list.append(value)
                    else:
                        row_list.append(str(value).strip() if value else "")
                data.append(row_list)
            
            row_count = len(data)
            logger.info(f"Fetched {row_count} rows from {table_name}")
            
            return {
                'success': True,
                'display_name': display_name,
                'table_name': table_name,
                'data': data,
                'row_count': row_count,
                'filters_applied': {
                    'batch': batch_name,
                    'start_time': start_time,
                    'end_time': end_time
                },
                'column_names': columns
            }
            
        except Exception as e:
            error_msg = f"Error fetching data: {str(e)}"
            logger.error(f"{error_msg}\n{traceback.format_exc()}")
            return {
                'success': False,
                'error': error_msg,
                'display_name': self.get_display_name(table_name),
                'table_name': table_name,
                'data': [],
                'row_count': 0
            }
    
    def _find_batch_column(self, columns: List[str]) -> Optional[str]:
        """Find batch column"""
        batch_keywords = ['BATCH', 'BATCH_NAME', 'BATCH_NUMBER', 'BATCH_NO', 'BATCHID', 'LOT', 'LOT_NO']
        for col in columns:
            col_upper = col.upper()
            if any(keyword in col_upper for keyword in batch_keywords):
                return col
        return None
    
    def _find_time_column(self, columns: List[str]) -> Optional[str]:
        """Find time column"""
        time_keywords = ['TIME', 'DATE', 'TIMESTAMP', 'DATETIME', 'CREATED']
        for col in columns:
            col_upper = col.upper()
            if any(keyword in col_upper for keyword in time_keywords):
                return col
        return None
    
    def get_display_name(self, table_name: str) -> str:
        """Convert table name to display name"""
        name = table_name.replace('_', ' ').title()
        
        prefixes = ['Tbl', 'Table', 'Vw', 'View', 'Dim', 'Fact']
        for prefix in prefixes:
            if name.startswith(prefix + ' '):
                name = name[len(prefix):].strip()
        
        return name if name else table_name

# ============================================================================
# EXCEL EXPORTER
# ============================================================================

class ExcelTableExporter:
    """Handles exporting tables to Excel"""
    
    @staticmethod
    def export_tables_to_new_excel(tables_data: Dict, output_path: str) -> bool:
        """Export to new Excel file"""
        try:
            wb = Workbook()
            
            # Remove default sheet
            if wb.sheetnames:
                wb.remove(wb.active)
            
            for table_name, table_data in tables_data.items():
                if table_data.get('success', False):
                    sheet_name = ExcelTableExporter.get_valid_sheet_name(table_data['display_name'])
                    ws = wb.create_sheet(title=sheet_name)
                    
                    data = table_data['data']
                    for row_idx, row_data in enumerate(data, 1):
                        for col_idx, value in enumerate(row_data, 1):
                            ws.cell(row=row_idx, column=col_idx, value=value)
            
            wb.save(output_path)
            logger.info(f"Created Excel: {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error creating Excel: {e}")
            return False
    
    @staticmethod
    def export_tables_to_template(tables_data: Dict, template_path: str, 
                                table_configs: Dict[str, TableConfig],
                                output_path: str,
                                merge_rules: List[str] = None) -> bool:
        """Export data into an existing template"""
        try:
            logger.info("Starting template export")
            
            total_rows = sum(t.get('row_count', 0) for t in tables_data.values() if t.get('success', False))
            if total_rows == 0:
                logger.warning("No data to export")
                return False
            
            # Copy template
            shutil.copy2(template_path, output_path)
            
            # Load workbook
            wb = load_workbook(output_path)
            
            # Apply merge rules
            if merge_rules:
                for rule in merge_rules:
                    try:
                        if "!" in rule:
                            sheet_name, cell_range = rule.split("!", 1)
                            sheet_name = sheet_name.strip()
                            cell_range = cell_range.strip()
                            if sheet_name in wb.sheetnames:
                                ws = wb[sheet_name]
                                if cell_range not in ws.merged_cells.ranges:
                                    ws.merge_cells(cell_range)
                    except Exception as e:
                        logger.warning(f"Failed to apply merge rule: {e}")
            
            # Process each table
            for table_name, table_data in tables_data.items():
                if not table_data.get('success', False) or table_name not in table_configs:
                    continue
                
                table_config = table_configs[table_name]
                data = table_data['data']
                
                # Handle header tables
                if table_config.column_mappings and data:
                    first_row = data[0] if data else []
                    column_names = table_data.get('column_names', [])
                    
                    for column_name, cell_mapping in table_config.column_mappings.items():
                        column_index = -1
                        if column_names:
                            try:
                                column_index = column_names.index(column_name)
                            except ValueError:
                                pass
                        
                        value = ""
                        if 0 <= column_index < len(first_row):
                            value = first_row[column_index]
                        
                        # Determine sheets to write to
                        sheets_to_write = ExcelTableExporter._get_sheets_to_write(
                            wb, cell_mapping, table_config
                        )
                        
                        for sheet_name in sheets_to_write:
                            ExcelTableExporter.write_to_cell_safe(
                                wb, sheet_name, cell_mapping.template_cell, value
                            )
                
                # Write tabular data
                if table_config.start_row > 0 and table_config.start_col and data:
                    sheets_to_write = ExcelTableExporter._get_sheets_to_write(wb, None, table_config)
                    
                    for sheet_name in sheets_to_write:
                        ws = wb[sheet_name]
                        start_col_idx = column_index_from_string(table_config.start_col)
                        
                        # Find safe row
                        safe_row = ExcelTableExporter.find_safe_row_for_table(ws, table_config.start_row)
                        
                        # Write data
                        for row_idx, row_data in enumerate(data, 0):
                            for col_idx, value in enumerate(row_data, 0):
                                cell_col = start_col_idx + col_idx
                                cell_row = safe_row + row_idx
                                cell_ref = f"{get_column_letter(cell_col)}{cell_row}"
                                ExcelTableExporter.write_to_cell_safe(wb, sheet_name, cell_ref, value)
            
            # Save workbook
            wb.save(output_path)
            logger.info("Template export completed")
            return True
            
        except Exception as e:
            logger.error(f"Template export failed: {e}\n{traceback.format_exc()}")
            return False
    
    @staticmethod
    def _get_sheets_to_write(wb, cell_mapping: Optional[CellMapping], 
                           table_config: TableConfig) -> List[str]:
        """Determine which sheets to write data to"""
        if cell_mapping and cell_mapping.apply_to_all_sheets:
            return wb.sheetnames
        elif cell_mapping and cell_mapping.selected_sheets:
            return [s for s in cell_mapping.selected_sheets if s in wb.sheetnames]
        elif table_config.apply_to_all_sheets:
            return wb.sheetnames
        elif table_config.selected_sheets:
            return [s for s in table_config.selected_sheets if s in wb.sheetnames]
        elif cell_mapping and cell_mapping.template_sheet in wb.sheetnames:
            return [cell_mapping.template_sheet]
        elif table_config.sheet_name in wb.sheetnames:
            return [table_config.sheet_name]
        else:
            return []
    
    @staticmethod
    def write_to_cell_safe(wb, sheet_name: str, cell_ref: str, value: Any) -> bool:
        """Safely write to a cell"""
        try:
            if sheet_name not in wb.sheetnames:
                return False
            
            ws = wb[sheet_name]
            match = re.match(r'^([A-Z]+)(\d+)$', cell_ref.upper())
            if not match:
                return False
            
            col_letter = match.group(1)
            row_num = int(match.group(2))
            col_num = column_index_from_string(col_letter)
            
            # Check for merged cells
            for merged_range in ws.merged_cells.ranges:
                if (merged_range.min_row <= row_num <= merged_range.max_row and
                    merged_range.min_col <= col_num <= merged_range.max_col):
                    top_left_cell = f"{get_column_letter(merged_range.min_col)}{merged_range.min_row}"
                    ws[top_left_cell] = value
                    ws[top_left_cell].alignment = Alignment(horizontal='center', vertical='center')
                    return True
            
            # Write to regular cell
            ws[cell_ref] = value
            return True
            
        except Exception:
            return False
    
    @staticmethod
    def find_safe_row_for_table(ws, start_row: int) -> int:
        """Find safe row for table data"""
        current_row = start_row
        
        for col in range(1, 11):
            cell_ref = f"{get_column_letter(col)}{current_row}"
            for merged_range in ws.merged_cells.ranges:
                if cell_ref in merged_range:
                    current_row += 1
                    return ExcelTableExporter.find_safe_row_for_table(ws, current_row)
        
        return current_row
    
    @staticmethod
    def get_valid_sheet_name(name: str) -> str:
        """Get valid Excel sheet name"""
        invalid_chars = ['\\', '/', '*', '?', ':', '[', ']']
        for char in invalid_chars:
            name = name.replace(char, '_')
        
        name = ' '.join(name.split())
        
        if len(name) > 31:
            name = name[:28] + "..."
        
        return name[:31]

# ============================================================================
# STREAMLIT APPLICATION
# ============================================================================

def init_session_state():
    """Initialize session state"""
    if 'db' not in st.session_state:
        st.session_state.db = DatabaseManager()
    if 'step' not in st.session_state:
        st.session_state.step = 1
    if 'tables_list' not in st.session_state:
        st.session_state.tables_list = []
    if 'selected_tables' not in st.session_state:
        st.session_state.selected_tables = []
    if 'template_path' not in st.session_state:
        st.session_state.template_path = None
    if 'template_sheets' not in st.session_state:
        st.session_state.template_sheets = []
    if 'table_configs' not in st.session_state:
        st.session_state.table_configs = {}
    if 'filters' not in st.session_state:
        st.session_state.filters = {}
    if 'merge_rules' not in st.session_state:
        st.session_state.merge_rules = []
    if 'row_limit' not in st.session_state:
        st.session_state.row_limit = 1000
    if 'configuring_positions' not in st.session_state:
        st.session_state.configuring_positions = False

def main():
    # Page configuration
    st.set_page_config(
        page_title="Excel Table Exporter Pro",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Custom CSS
    st.markdown("""
    <style>
    .main-header {
        font-size: 2.5rem;
        color: #1E3A8A;
        margin-bottom: 1rem;
    }
    .step-box {
        background-color: #f8f9fa;
        padding: 1.5rem;
        border-radius: 10px;
        border-left: 5px solid #3B82F6;
        margin-bottom: 1.5rem;
    }
    .stButton > button {
        width: 100%;
    }
    .warning-box {
        background-color: #fff3cd;
        padding: 1rem;
        border-radius: 5px;
        border-left: 4px solid #ffc107;
        margin: 10px 0;
    }
    </style>
    """, unsafe_allow_html=True)
    
    # Title
    st.markdown('<h1 class="main-header">📊 Excel Table Exporter Pro</h1>', unsafe_allow_html=True)
    st.markdown("Export SQL Server tables to Excel with pyodbc")
    
    # Initialize session state
    init_session_state()
    
    # Sidebar
    with st.sidebar:
        st.image("https://img.icons8.com/color/96/000000/microsoft-excel-2019.png", width=80)
        
        st.markdown("### Navigation")
        
        steps = [
            ("🔗", "Connection", 1),
            ("📋", "Table Selection", 2),
            ("📍", "Position Mapping", 3),
            ("⚙️", "Filters", 4),
            ("📤", "Export", 5)
        ]
        
        for icon, name, step_num in steps:
            if step_num == st.session_state.step:
                st.markdown(f"**{icon} {name}**")
            else:
                if st.button(f"{icon} {name}", key=f"nav_{step_num}"):
                    st.session_state.step = step_num
                    st.rerun()
        
        st.divider()
        
        # Connection status
        if st.session_state.db.connected:
            st.success("✅ Connected")
            if st.button("🔌 Disconnect"):
                st.session_state.db.disconnect()
                st.session_state.tables_list = []
                st.session_state.selected_tables = []
                st.session_state.filters = {}
                st.session_state.step = 1
                st.rerun()
        else:
            st.warning("⚠️ Not Connected")
        
        # Selected tables
        if st.session_state.selected_tables:
            st.info(f"📋 {len(st.session_state.selected_tables)} tables selected")
    
    # Main content
    if st.session_state.step == 1:
        show_connection_tab()
    elif st.session_state.step == 2:
        show_table_selection_tab()
    elif st.session_state.step == 3:
        show_position_mapping_tab()
    elif st.session_state.step == 4:
        show_filters_tab()
    elif st.session_state.step == 5:
        show_export_tab()

def show_connection_tab():
    """Connection tab"""
    st.markdown("## Step 1: Database Connection")
    
    with st.container():
        st.markdown('<div class="step-box">', unsafe_allow_html=True)
        
        col1, col2 = st.columns(2)
        with col1:
            server = st.text_input("Server", "MAHESHWAGH\\WINCC", key="conn_server")
        
        with col2:
            database = st.text_input("Database", "VPI1", key="conn_database")
        
        # Test and Connect button
        if st.button("🔗 Connect & Continue", type="primary"):
            if not server or not database:
                st.error("Please enter server and database name")
                return
            
            with st.spinner("Connecting..."):
                success, message = st.session_state.db.connect(
                    server=server,
                    database=database
                )
                
                if success:
                    st.success("✅ Connected successfully!")
                    st.session_state.tables_list = st.session_state.db.get_tables()
                    st.session_state.step = 2
                    st.rerun()
                else:
                    st.error(f"❌ {message}")
        
        # Connection help
        with st.expander("🔧 Connection Help"):
            st.markdown("""
            **Server Examples:**
            - `SERVER\\INSTANCE` (e.g., MAHESHWAGH\\WINCC)
            - `localhost\\SQLEXPRESS`
            - `192.168.1.100`
            
            **Requirements:**
            1. SQL Server must be running
            2. Windows Authentication must be enabled
            3. ODBC Driver 17 for SQL Server should be installed
            
            **Troubleshooting:**
            - Check if SQL Server is running
            - Verify Windows Authentication is enabled
            - Ensure firewall allows connections
            """)
        
        st.markdown('</div>', unsafe_allow_html=True)

def show_table_selection_tab():
    """Table selection tab"""
    st.markdown("## Step 2: Table Selection")
    
    if not st.session_state.db.connected:
        st.warning("Please connect to database first")
        if st.button("← Go to Connection"):
            st.session_state.step = 1
            st.rerun()
        return
    
    with st.container():
        st.markdown('<div class="step-box">', unsafe_allow_html=True)
        
        # Search
        search = st.text_input("🔍 Search tables", placeholder="Type to filter...", key="table_search")
        
        # Select all / clear
        col1, col2 = st.columns(2)
        with col1:
            if st.button("Select All", use_container_width=True):
                st.session_state.selected_tables = st.session_state.tables_list.copy()
                st.rerun()
        with col2:
            if st.button("Clear All", use_container_width=True):
                st.session_state.selected_tables = []
                st.rerun()
        
        # Filter tables
        filtered = st.session_state.tables_list
        if search:
            filtered = [t for t in filtered if search.lower() in t.lower()]
        
        # Multi-select
        if filtered:
            selected = st.multiselect(
                "Select tables to export:",
                filtered,
                default=st.session_state.selected_tables,
                key="table_multiselect"
            )
            
            st.session_state.selected_tables = selected
            
            if selected:
                st.success(f"✅ Selected {len(selected)} table(s)")
        else:
            st.warning("No tables found")
        
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Navigation
    col1, col2 = st.columns(2)
    with col1:
        if st.button("← Previous: Connection", use_container_width=True):
            st.session_state.step = 1
            st.rerun()
    with col2:
        if st.button("Next: Position Mapping →", type="primary", use_container_width=True):
            if not st.session_state.selected_tables:
                st.warning("Please select at least one table")
            else:
                st.session_state.step = 3
                st.rerun()

def show_position_mapping_tab():
    """Position mapping tab"""
    st.markdown("## Step 3: Position Mapping")
    
    if not st.session_state.selected_tables:
        st.warning("Please select tables first")
        if st.button("← Go to Table Selection"):
            st.session_state.step = 2
            st.rerun()
        return
    
    with st.container():
        st.markdown('<div class="step-box">', unsafe_allow_html=True)
        
        # Template upload
        st.markdown("### 📄 Template Selection")
        uploaded = st.file_uploader("Upload Excel template", type=['xlsx', 'xls'], key="template_upload")
        
        if uploaded:
            try:
                temp_dir = tempfile.gettempdir()
                template_path = os.path.join(temp_dir, uploaded.name)
                
                with open(template_path, "wb") as f:
                    f.write(uploaded.getbuffer())
                
                st.session_state.template_path = template_path
                
                # Get sheet names
                try:
                    wb = load_workbook(template_path, read_only=True)
                    st.session_state.template_sheets = wb.sheetnames
                    st.success(f"✅ Template loaded with {len(st.session_state.template_sheets)} sheet(s)")
                except Exception as e:
                    st.error(f"Error reading template: {e}")
            except Exception as e:
                st.error(f"Error saving template: {e}")
        
        # Merge rules
        st.markdown("### 🔗 Merge Cell Rules (Optional)")
        merge_rules_text = st.text_area(
            "Enter merge ranges (one per line): SheetName!StartCell:EndCell",
            height=100,
            placeholder="Example:\nSheet1!B4:D4",
            key="merge_rules_text"
        )
        
        # Configure positions
        if st.session_state.template_path:
            if st.button("⚙️ Configure Position Mappings", type="primary", use_container_width=True):
                if merge_rules_text:
                    st.session_state.merge_rules = [line.strip() for line in merge_rules_text.splitlines() if line.strip()]
                st.session_state.configuring_positions = True
                st.rerun()
        else:
            st.info("Upload a template to configure mappings")
        
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Show configuration if enabled
    if st.session_state.configuring_positions:
        show_position_configuration()
    
    # Navigation
    col1, col2 = st.columns(2)
    with col1:
        if st.button("← Previous: Table Selection", use_container_width=True):
            st.session_state.step = 2
            st.rerun()
    with col2:
        if st.button("Next: Filters →", type="primary", use_container_width=True):
            st.session_state.step = 4
            st.rerun()

def show_position_configuration():
    """Show position configuration"""
    st.markdown("---")
    st.markdown("### ⚙️ Position Configuration")
    
    # Simple configuration for all tables
    for table_name in st.session_state.selected_tables:
        with st.expander(f"Configure: {table_name}", expanded=False):
            display_name = st.session_state.db.get_display_name(table_name)
            
            col1, col2 = st.columns(2)
            with col1:
                sheet = st.selectbox(
                    "Target Sheet",
                    st.session_state.template_sheets,
                    key=f"sheet_{table_name}"
                )
            
            with col2:
                start_cell = st.text_input(
                    "Start cell",
                    value="A2",
                    key=f"cell_{table_name}",
                    help="e.g., B4, C10"
                )
            
            # Simple apply to option
            apply_option = st.selectbox(
                "Apply to",
                ["This Sheet Only", "All Sheets"],
                key=f"apply_{table_name}"
            )
            
            if st.button(f"Save for {table_name}", key=f"save_{table_name}"):
                if re.match(r'^[A-Z]+\d+$', start_cell.upper()):
                    col_letter = ''.join([c for c in start_cell.upper() if c.isalpha()])
                    row_num = int(''.join([c for c in start_cell if c.isdigit()]))
                    
                    apply_to_all = (apply_option == "All Sheets")
                    target_sheets = st.session_state.template_sheets if apply_to_all else [sheet]
                    
                    st.session_state.table_configs[table_name] = TableConfig(
                        table_name=table_name,
                        display_name=display_name,
                        start_row=row_num,
                        start_col=col_letter,
                        sheet_name=sheet,
                        apply_to_all_sheets=apply_to_all,
                        selected_sheets=target_sheets
                    )
                    
                    st.success(f"✅ Saved mapping for {table_name}")
                else:
                    st.error("Invalid cell format")
    
    # Done button
    if st.button("✅ Done Configuring", type="primary", use_container_width=True):
        st.session_state.configuring_positions = False
        st.rerun()

def show_filters_tab():
    """Filters tab"""
    st.markdown("## Step 4: Data Filters")
    
    with st.container():
        st.markdown('<div class="step-box">', unsafe_allow_html=True)
        
        # Global row limit
        row_limit = st.number_input(
            "Row limit per table (0 = all rows)",
            min_value=0,
            max_value=100000,
            value=1000,
            key="global_row_limit"
        )
        st.session_state.row_limit = row_limit
        
        # Table-specific filters
        for table_name in st.session_state.selected_tables:
            with st.expander(f"Filters for {table_name}", expanded=False):
                current_filters = st.session_state.filters.get(table_name, {})
                
                # Batch filter
                batches = st.session_state.db.get_batches_from_table(table_name)
                if batches:
                    selected_batch = st.selectbox(
                        "Select Batch",
                        ["All Batches"] + batches,
                        key=f"batch_{table_name}"
                    )
                else:
                    selected_batch = "All Batches"
                
                # Time filter
                enable_time = st.checkbox("Enable time filter", key=f"enable_time_{table_name}")
                
                if enable_time:
                    col1, col2 = st.columns(2)
                    with col1:
                        start_date = st.date_input("Start Date", key=f"start_date_{table_name}")
                        start_time = st.time_input("Start Time", key=f"start_time_{table_name}")
                    
                    with col2:
                        end_date = st.date_input("End Date", key=f"end_date_{table_name}")
                        end_time = st.time_input("End Time", key=f"end_time_{table_name}")
                
                if st.button(f"Save filters", key=f"save_filters_{table_name}"):
                    filters = {}
                    
                    if selected_batch != "All Batches":
                        filters['batch'] = selected_batch
                    
                    if enable_time:
                        filters['start_time'] = datetime.combine(start_date, start_time)
                        filters['end_time'] = datetime.combine(end_date, end_time)
                    
                    st.session_state.filters[table_name] = filters
                    st.success(f"✅ Filters saved for {table_name}")
        
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Navigation
    col1, col2 = st.columns(2)
    with col1:
        if st.button("← Previous: Position Mapping", use_container_width=True):
            st.session_state.step = 3
            st.rerun()
    with col2:
        if st.button("Next: Export →", type="primary", use_container_width=True):
            st.session_state.step = 5
            st.rerun()

def show_export_tab():
    """Export tab"""
    st.markdown("## Step 5: Export")
    
    with st.container():
        st.markdown('<div class="step-box">', unsafe_allow_html=True)
        
        # Export summary
        st.info("### 📊 Export Summary")
        st.write(f"**Tables:** {len(st.session_state.selected_tables)}")
        st.write(f"**Template:** {'Loaded' if st.session_state.template_path else 'New Excel'}")
        st.write(f"**Mappings:** {len(st.session_state.table_configs)} configured")
        st.write(f"**Row Limit:** {st.session_state.row_limit if st.session_state.row_limit > 0 else 'All'}")
        
        # Export mode
        if st.session_state.template_path:
            export_mode = st.radio(
                "Export mode:",
                ["Use Template", "New Excel File"],
                key="export_mode"
            )
        else:
            export_mode = "New Excel File"
        
        # Output filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_filename = st.text_input(
            "Output filename",
            value=f"export_{timestamp}.xlsx",
            key="output_filename"
        )
        
        # Export button
        if st.button("🚀 Export to Excel", type="primary", use_container_width=True):
            if not output_filename.endswith('.xlsx'):
                output_filename += '.xlsx'
            
            temp_dir = tempfile.gettempdir()
            output_path = os.path.join(temp_dir, output_filename)
            
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            # Export process
            try:
                status_text.info("Fetching data...")
                progress_bar.progress(30)
                
                all_data = {}
                row_limit = st.session_state.row_limit if st.session_state.row_limit > 0 else None
                
                # Fetch data for each table
                for i, table_name in enumerate(st.session_state.selected_tables):
                    filters = st.session_state.filters.get(table_name, {})
                    
                    result = st.session_state.db.fetch_filtered_data(
                        table_name=table_name,
                        batch_name=filters.get('batch'),
                        start_time=filters.get('start_time'),
                        end_time=filters.get('end_time'),
                        limit=row_limit
                    )
                    
                    all_data[table_name] = result
                    progress_bar.progress(30 + int(40 * (i + 1) / len(st.session_state.selected_tables)))
                
                status_text.info("Exporting to Excel...")
                progress_bar.progress(80)
                
                # Export data
                success = False
                if export_mode == "Use Template" and st.session_state.template_path:
                    success = ExcelTableExporter.export_tables_to_template(
                        tables_data=all_data,
                        template_path=st.session_state.template_path,
                        table_configs=st.session_state.table_configs,
                        output_path=output_path,
                        merge_rules=st.session_state.merge_rules
                    )
                else:
                    success = ExcelTableExporter.export_tables_to_new_excel(
                        tables_data=all_data,
                        output_path=output_path
                    )
                
                progress_bar.progress(100)
                
                if success:
                    status_text.success("✅ Export completed!")
                    
                    # Provide download
                    try:
                        with open(output_path, 'rb') as f:
                            excel_data = f.read()
                        
                        b64 = base64.b64encode(excel_data).decode()
                        download_link = f'''
                        <a href="data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{b64}" 
                           download="{output_filename}" 
                           style="background-color:#4CAF50;color:white;padding:10px 20px;text-decoration:none;border-radius:5px;display:inline-block;">
                           📥 Download {output_filename}
                        </a>
                        '''
                        st.markdown(download_link, unsafe_allow_html=True)
                        
                        # Cleanup
                        try:
                            os.remove(output_path)
                        except:
                            pass
                            
                    except Exception as e:
                        st.error(f"Error creating download: {e}")
                else:
                    status_text.error("❌ Export failed")
                    
            except Exception as e:
                status_text.error(f"❌ Error: {str(e)}")
                logger.error(f"Export error: {e}\n{traceback.format_exc()}")
        
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Navigation
    col1, col2 = st.columns(2)
    with col1:
        if st.button("← Previous: Filters", use_container_width=True):
            st.session_state.step = 4
            st.rerun()
    with col2:
        if st.button("🔄 Start Over", use_container_width=True):
            # Reset state
            st.session_state.filters = {}
            st.session_state.table_configs = {}
            st.session_state.template_path = None
            st.session_state.template_sheets = []
            st.session_state.merge_rules = []
            st.session_state.step = 2
            st.rerun()

# ============================================================================
# RUN APPLICATION
# ============================================================================

if __name__ == "__main__":
    main()
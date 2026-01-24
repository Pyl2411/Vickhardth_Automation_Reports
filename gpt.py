import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext, filedialog
import pyodbc
import pandas as pd
import json
import os
import logging
from datetime import datetime, timedelta
import threading
import sys
from tkcalendar import DateEntry
from openpyxl import Workbook, load_workbook
from openpyxl.styles import PatternFill, Font, Alignment, Border, Side
from openpyxl.utils import get_column_letter
from openpyxl.drawing.image import Image as XLImage
from PIL import Image, ImageTk
import io
import re
import traceback
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from collections import OrderedDict

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('table_exporter.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# ============================================================================
# DATA CLASSES
# ============================================================================

@dataclass
class TablePosition:
    """Stores position information for a table"""
    table_name: str
    sheet_name: str
    start_row: int = 16
    start_col: str = "A"
    header_positions: Dict[str, str] = None
    
    def __post_init__(self):
        if self.header_positions is None:
            self.header_positions = {}
    
    def get_start_col_num(self) -> int:
        """Convert column letter to number"""
        col = self.start_col.upper()
        result = 0
        for char in col:
            result = result * 26 + (ord(char) - ord('A') + 1)
        return result

# ============================================================================
# DATABASE MANAGER
# ============================================================================

class DatabaseManager:
    """Handles database connections and queries"""
    
    def __init__(self):
        self.connection = None
        self.cursor = None
        self.connected = False
        self.server_info = {}
        
    def connect(self, server: str, database: str, use_windows_auth: bool = True, 
                username: Optional[str] = None, password: Optional[str] = None) -> tuple:
        """Connect to SQL Server"""
        try:
            if use_windows_auth:
                conn_str = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;'
            else:
                conn_str = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password};'
            
            self.connection = pyodbc.connect(conn_str, timeout=30)
            self.cursor = self.connection.cursor()
            self.connected = True
            
            # Get server info
            self.cursor.execute("SELECT @@VERSION")
            version_info = self.cursor.fetchone()[0]
            self.server_info = {
                'version': version_info,
                'server': server,
                'database': database
            }
            
            return True, "Connected successfully"
            
        except pyodbc.OperationalError as e:
            return False, f"Connection timeout or server not reachable: {str(e)}"
        except pyodbc.Error as e:
            error_msg = str(e)
            if "Login failed" in error_msg:
                return False, "Login failed. Check username/password."
            elif "Cannot open database" in error_msg:
                return False, f"Database '{database}' not found or access denied."
            else:
                return False, f"Connection failed: {error_msg}"
        except Exception as e:
            return False, f"Unexpected error: {str(e)}"
    
    def disconnect(self):
        """Disconnect from database"""
        try:
            if self.cursor:
                self.cursor.close()
            if self.connection:
                self.connection.close()
        except:
            pass
        finally:
            self.connected = False
    
    def get_tables(self) -> List[str]:
        """Get list of all tables with row counts"""
        try:
            query = """
            SELECT 
                s.name as schema_name,
                t.name as table_name,
                p.rows as row_count
            FROM sys.tables t
            INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
            INNER JOIN sys.partitions p ON t.object_id = p.object_id
            WHERE p.index_id IN (0, 1)
            GROUP BY s.name, t.name, p.rows
            ORDER BY s.name, t.name
            """
            self.cursor.execute(query)
            tables = self.cursor.fetchall()
            return [f"{row[0]}.{row[1]} ({row[2]:,} rows)" for row in tables]
        except Exception as e:
            logger.error(f"Error getting tables: {e}")
            return []
    
    def get_table_columns(self, table_name: str) -> List[str]:
        """Get all columns for a table with data types"""
        try:
            # Remove row count info if present
            if '(' in table_name:
                table_name = table_name.split('(')[0].strip()
            
            # Remove schema if present
            if '.' in table_name:
                schema, table = table_name.split('.')
                query = """
                SELECT 
                    c.COLUMN_NAME,
                    c.DATA_TYPE,
                    c.CHARACTER_MAXIMUM_LENGTH,
                    c.IS_NULLABLE
                FROM INFORMATION_SCHEMA.COLUMNS c
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                ORDER BY ORDINAL_POSITION
                """
                self.cursor.execute(query, (schema, table))
            else:
                query = """
                SELECT 
                    c.COLUMN_NAME,
                    c.DATA_TYPE,
                    c.CHARACTER_MAXIMUM_LENGTH,
                    c.IS_NULLABLE
                FROM INFORMATION_SCHEMA.COLUMNS c
                WHERE TABLE_NAME = ?
                ORDER BY ORDINAL_POSITION
                """
                self.cursor.execute(query, (table_name,))
            
            columns = self.cursor.fetchall()
            formatted_columns = []
            for col in columns:
                col_name = col[0]
                data_type = col[1]
                length = col[2]
                nullable = col[3]
                
                type_info = data_type
                if length and length > 0:
                    type_info += f"({length})"
                
                formatted_columns.append(f"{col_name} ({type_info}, {nullable})")
            
            return formatted_columns
        except Exception as e:
            logger.error(f"Error getting columns: {e}")
            return []
    
    def get_raw_column_names(self, table_name: str) -> List[str]:
        """Get only column names without formatting"""
        try:
            # Remove row count info if present
            if '(' in table_name:
                table_name = table_name.split('(')[0].strip()
            
            if '.' in table_name:
                schema, table = table_name.split('.')
                query = """
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                ORDER BY ORDINAL_POSITION
                """
                self.cursor.execute(query, (schema, table))
            else:
                query = """
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = ?
                ORDER BY ORDINAL_POSITION
                """
                self.cursor.execute(query, (table_name,))
            
            columns = self.cursor.fetchall()
            return [col[0] for col in columns]
        except Exception as e:
            logger.error(f"Error getting raw columns: {e}")
            return []
    
    def fetch_table_data(self, table_name: str, limit: Optional[int] = None) -> Dict:
        """Fetch all data from a table"""
        try:
            # Clean table name
            if '(' in table_name:
                table_name = table_name.split('(')[0].strip()
            
            if '.' in table_name:
                schema, table = table_name.split('.')
                table_ref = f"[{schema}].[{table}]"
            else:
                table_ref = f"[{table_name}]"
            
            # Build query with limit
            if limit and limit > 0:
                query = f"SELECT TOP ({limit}) * FROM {table_ref}"
            else:
                query = f"SELECT * FROM {table_ref}"
            
            self.cursor.execute(query)
            columns = [column[0] for column in self.cursor.description]
            rows = self.cursor.fetchall()
            
            # Get row count for info
            count_query = f"SELECT COUNT(*) FROM {table_ref}"
            self.cursor.execute(count_query)
            total_count = self.cursor.fetchone()[0]
            
            # Convert to list of dictionaries
            data_list = []
            for row in rows:
                row_dict = {}
                for i, col in enumerate(columns):
                    value = row[i]
                    if isinstance(value, datetime):
                        value = value.strftime('%Y-%m-%d %H:%M:%S')
                    elif isinstance(value, bytes):
                        value = str(value)
                    row_dict[col] = value
                data_list.append(row_dict)
            
            return {
                'success': True,
                'table_name': table_name,
                'display_name': self.get_display_name(table_name),
                'data': data_list,
                'columns': columns,
                'row_count': len(rows),
                'total_count': total_count
            }
            
        except Exception as e:
            logger.error(f"Error fetching table {table_name}: {e}")
            return {
                'success': False,
                'error': str(e),
                'table_name': table_name,
                'display_name': self.get_display_name(table_name),
                'data': [],
                'columns': [],
                'row_count': 0,
                'total_count': 0
            }
    
    def get_display_name(self, table_name: str) -> str:
        """Get display name from full table name"""
        if '(' in table_name:
            table_name = table_name.split('(')[0].strip()
        if '.' in table_name:
            return table_name.split('.')[-1]
        return table_name

# ============================================================================
# TABLE POSITION MAPPING DIALOG (FIXED GUI)
# ============================================================================

class TablePositionDialog:
    """Dialog for mapping tables to Excel positions - FIXED VERSION"""
    
    def __init__(self, parent, tables: List[str]):
        self.parent = parent
        self.tables = tables
        self.table_positions = {}  # {table_name: TablePosition object}
        self.result = None
        
        # Default header positions
        self.default_headers = OrderedDict([
            ("BATCH_NAME", "A1"),
            ("BATCH_NUMBER", "A2"),
            ("JOB_NO", "A3"),
            ("OPERATOR_NAME", "A4"),
            ("DATE", "H1"),
            ("TIME", "H2"),
            ("SHIFT", "H3"),
            ("STATION", "H4"),
            ("MACHINE_NO", "H5"),
            ("PRODUCT_CODE", "A6"),
            ("LOT_NO", "A7"),
            ("QUANTITY", "A8"),
            ("STATUS", "H6"),
            ("REMARKS", "H7"),
            ("INSPECTED_BY", "H8"),
            ("APPROVED_BY", "H9")
        ])
        
        self.create_dialog()
        self.dialog.protocol("WM_DELETE_WINDOW", self.cancel)
    
    def create_dialog(self):
        """Create the table position mapping dialog with proper layout"""
        self.dialog = tk.Toplevel(self.parent)
        self.dialog.title("Configure Table Positions for Excel")
        self.dialog.geometry("1300x800")
        self.dialog.transient(self.parent)
        self.dialog.grab_set()
        
        # Center dialog
        self.dialog.update_idletasks()
        x = (self.dialog.winfo_screenwidth() - self.dialog.winfo_width()) // 2
        y = (self.dialog.winfo_screenheight() - self.dialog.winfo_height()) // 2
        self.dialog.geometry(f"+{x}+{y}")
        
        # Main container with scrollbar
        main_container = ttk.Frame(self.dialog)
        main_container.pack(fill='both', expand=True, padx=5, pady=5)
        
        # Create canvas with scrollbar
        canvas = tk.Canvas(main_container)
        scrollbar = ttk.Scrollbar(main_container, orient="vertical", command=canvas.yview)
        scrollable_frame = ttk.Frame(canvas)
        
        scrollable_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )
        
        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)
        
        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
        
        # Title
        title_frame = ttk.Frame(scrollable_frame, padding="10")
        title_frame.pack(fill='x', pady=(0, 10))
        
        ttk.Label(title_frame, text="📊 Configure Table Positions for Excel Export", 
                 font=('Segoe UI', 16, 'bold')).pack()
        
        ttk.Label(title_frame, 
                 text="Set starting position and header mappings for each table. Each table will be on a separate sheet.",
                 font=('Segoe UI', 10)).pack(pady=5)
        
        # Instructions
        instr_frame = ttk.LabelFrame(scrollable_frame, text="📋 Instructions", padding="15")
        instr_frame.pack(fill='x', pady=(0, 20), padx=10)
        
        instructions = [
            "1. For each table, set where the data should START in Excel",
            "2. Configure HEADER positions for common fields (optional)",
            "3. Data will start from the specified row and column",
            "4. Each table will be placed on a SEPARATE sheet",
            "5. Use 'Copy Settings' to apply same settings to all tables"
        ]
        
        for i, instr in enumerate(instructions):
            ttk.Label(instr_frame, text=f"• {instr}", font=('Segoe UI', 9)).pack(anchor='w', pady=2)
        
        # Table configuration area
        self.table_frames = {}
        for table in self.tables:
            table_frame = self.create_table_config_frame(scrollable_frame, table)
            self.table_frames[table] = table_frame
        
        # Global controls
        control_frame = ttk.Frame(scrollable_frame, padding="20")
        control_frame.pack(fill='x', pady=20)
        
        # Global settings
        global_frame = ttk.LabelFrame(control_frame, text="⚙️ Global Settings (Apply to All Tables)", padding="15")
        global_frame.pack(fill='x', pady=(0, 20))
        
        # Start position
        start_frame = ttk.Frame(global_frame)
        start_frame.pack(fill='x', pady=10)
        
        ttk.Label(start_frame, text="Start Row:", font=('Segoe UI', 10, 'bold')).pack(side=tk.LEFT, padx=(0, 10))
        self.global_start_row = tk.IntVar(value=16)
        ttk.Spinbox(start_frame, from_=1, to=1000, textvariable=self.global_start_row, width=10).pack(side=tk.LEFT, padx=(0, 20))
        
        ttk.Label(start_frame, text="Start Column:", font=('Segoe UI', 10, 'bold')).pack(side=tk.LEFT, padx=(0, 10))
        self.global_start_col = tk.StringVar(value="A")
        ttk.Entry(start_frame, textvariable=self.global_start_col, width=10).pack(side=tk.LEFT)
        
        ttk.Button(global_frame, text="Apply Start Position to All Tables", 
                  command=self.apply_global_start_position).pack(pady=10)
        
        # Header templates
        header_frame = ttk.LabelFrame(global_frame, text="Header Templates", padding="10")
        header_frame.pack(fill='x', pady=(10, 0))
        
        templates_frame = ttk.Frame(header_frame)
        templates_frame.pack(fill='x', pady=5)
        
        ttk.Button(templates_frame, text="Use Default Headers", 
                  command=lambda: self.apply_header_template("default"), width=20).pack(side=tk.LEFT, padx=5)
        ttk.Button(templates_frame, text="Clear All Headers", 
                  command=lambda: self.apply_header_template("clear"), width=20).pack(side=tk.LEFT, padx=5)
        
        # Action buttons
        action_frame = ttk.Frame(control_frame)
        action_frame.pack(fill='x')
        
        ttk.Button(action_frame, text="✅ APPLY ALL MAPPINGS", 
                  command=self.apply_mappings, style='Accent.TButton', width=25).pack(side=tk.RIGHT, padx=5)
        ttk.Button(action_frame, text="❌ CANCEL", 
                  command=self.cancel, width=15).pack(side=tk.RIGHT, padx=5)
        ttk.Button(action_frame, text="📋 COPY FROM FIRST TABLE", 
                  command=self.copy_from_first_table, width=20).pack(side=tk.LEFT, padx=5)
        
        # Bind mouse wheel for scrolling
        canvas.bind_all("<MouseWheel>", lambda e: canvas.yview_scroll(int(-1*(e.delta/120)), "units"))
    
    def create_table_config_frame(self, parent, table_name: str) -> ttk.Frame:
        """Create configuration frame for a single table"""
        frame = ttk.LabelFrame(parent, text=f"📄 {self.get_display_name(table_name)}", padding="15")
        frame.pack(fill='x', pady=10, padx=10)
        
        # Table info
        info_frame = ttk.Frame(frame)
        info_frame.pack(fill='x', pady=(0, 15))
        
        ttk.Label(info_frame, text=f"Full Name: {table_name}", font=('Segoe UI', 9)).pack(anchor='w')
        
        # Position settings in a grid
        grid_frame = ttk.Frame(frame)
        grid_frame.pack(fill='x', pady=(0, 20))
        
        # Start Row
        ttk.Label(grid_frame, text="Start Row:", font=('Segoe UI', 10, 'bold')).grid(row=0, column=0, sticky=tk.W, padx=(0, 10), pady=5)
        start_row_var = tk.IntVar(value=16)
        ttk.Spinbox(grid_frame, from_=1, to=1000, textvariable=start_row_var, width=10).grid(row=0, column=1, sticky=tk.W, pady=5)
        ttk.Label(grid_frame, text="(Data begins at this row)").grid(row=0, column=2, sticky=tk.W, padx=(10, 0), pady=5)
        
        # Start Column
        ttk.Label(grid_frame, text="Start Column:", font=('Segoe UI', 10, 'bold')).grid(row=0, column=3, sticky=tk.W, padx=(20, 10), pady=5)
        start_col_var = tk.StringVar(value="A")
        ttk.Entry(grid_frame, textvariable=start_col_var, width=10).grid(row=0, column=4, sticky=tk.W, pady=5)
        ttk.Label(grid_frame, text="(Data begins at this column)").grid(row=0, column=5, sticky=tk.W, padx=(10, 0), pady=5)
        
        # Header positions
        header_label = ttk.Label(frame, text="Header Positions (Fixed Cells):", font=('Segoe UI', 10, 'bold'))
        header_label.pack(anchor='w', pady=(0, 10))
        
        # Create header grid
        header_frame = ttk.Frame(frame)
        header_frame.pack(fill='x')
        
        # Create header entries in 4 columns
        header_vars = {}
        for i, (header_name, default_pos) in enumerate(self.default_headers.items()):
            row = i // 4
            col = (i % 4) * 3
            
            # Header name
            ttk.Label(header_frame, text=f"{header_name}:").grid(row=row, column=col, sticky=tk.W, padx=(0, 5), pady=2)
            
            # Position entry
            pos_var = tk.StringVar(value=default_pos)
            ttk.Entry(header_frame, textvariable=pos_var, width=8).grid(row=row, column=col+1, sticky=tk.W, padx=(0, 10), pady=2)
            
            header_vars[header_name] = pos_var
        
        # Store variables as attributes
        frame.start_row_var = start_row_var
        frame.start_col_var = start_col_var
        frame.header_vars = header_vars
        
        return frame
    
    def get_display_name(self, table_name: str) -> str:
        """Get short display name for table"""
        if '(' in table_name:
            table_name = table_name.split('(')[0].strip()
        if '.' in table_name:
            return table_name.split('.')[-1]
        return table_name[:30]
    
    def apply_global_start_position(self):
        """Apply global start position to all tables"""
        start_row = self.global_start_row.get()
        start_col = self.global_start_col.get().upper()
        
        for table, frame in self.table_frames.items():
            frame.start_row_var.set(start_row)
            frame.start_col_var.set(start_col)
        
        messagebox.showinfo("Applied", f"Start position set to {start_col}{start_row} for all tables")
    
    def apply_header_template(self, template_type: str):
        """Apply header template to all tables"""
        if template_type == "default":
            for table, frame in self.table_frames.items():
                for header_name, pos_var in frame.header_vars.items():
                    default_pos = self.default_headers.get(header_name, "")
                    pos_var.set(default_pos)
            messagebox.showinfo("Applied", "Default headers applied to all tables")
        elif template_type == "clear":
            for table, frame in self.table_frames.items():
                for pos_var in frame.header_vars.values():
                    pos_var.set("")
            messagebox.showinfo("Cleared", "All headers cleared")
    
    def copy_from_first_table(self):
        """Copy settings from first table to all others"""
        if not self.table_frames:
            return
        
        first_table = list(self.table_frames.keys())[0]
        first_frame = self.table_frames[first_table]
        
        start_row = first_frame.start_row_var.get()
        start_col = first_frame.start_col_var.get()
        header_values = {name: var.get() for name, var in first_frame.header_vars.items()}
        
        for table, frame in self.table_frames.items():
            if table != first_table:
                frame.start_row_var.set(start_row)
                frame.start_col_var.set(start_col)
                for header_name, pos_var in frame.header_vars.items():
                    if header_name in header_values:
                        pos_var.set(header_values[header_name])
        
        messagebox.showinfo("Copied", f"Settings copied from '{self.get_display_name(first_table)}' to all other tables")
    
    def apply_mappings(self):
        """Apply all table position mappings"""
        try:
            self.table_positions = {}
            
            for table, frame in self.table_frames.items():
                # Get start position
                start_row = frame.start_row_var.get()
                start_col = frame.start_col_var.get().upper()
                
                # Validate start column
                if not re.match(r'^[A-Z]+$', start_col):
                    messagebox.showerror("Invalid Column", 
                                       f"Invalid column '{start_col}' for table '{self.get_display_name(table)}'. Use letters only (A, B, C, etc.)")
                    return
                
                # Get header positions
                header_positions = {}
                for header_name, pos_var in frame.header_vars.items():
                    pos = pos_var.get().strip()
                    if pos:  # Only include non-empty positions
                        header_positions[header_name] = pos.upper()
                
                # Create TablePosition object
                table_pos = TablePosition(
                    table_name=table,
                    sheet_name=self.get_display_name(table),
                    start_row=start_row,
                    start_col=start_col,
                    header_positions=header_positions
                )
                
                self.table_positions[table] = table_pos
            
            self.result = self.table_positions
            self.dialog.destroy()
            
        except Exception as e:
            messagebox.showerror("Error", f"Error applying mappings: {str(e)}")
    
    def cancel(self):
        """Cancel the mapping"""
        self.result = None
        self.dialog.destroy()
    
    def get_positions(self) -> Optional[Dict]:
        """Get the mapping result"""
        return self.result

# ============================================================================
# EXCEL EXPORTER (FIXED LOGO ISSUES)
# ============================================================================

class ExcelTableExporter:
    """Handles exporting multiple tables to Excel - FIXED VERSION"""
    
    @staticmethod
    def export_tables_to_excel(tables_data: Dict, table_positions: Dict[str, TablePosition], 
                               logo_path: Optional[str] = None,
                               output_path: str = None) -> Any:
        """Export multiple tables to Excel with custom positions - FIXED LOGO ISSUES"""
        try:
            wb = Workbook()
            
            # Remove default sheet
            if wb.sheetnames and 'Sheet' in wb.sheetnames[0]:
                ws_default = wb.active
                wb.remove(ws_default)
            
            # Create a sheet for each table
            for table_name, table_data in tables_data.items():
                if table_data.get('success', False) and table_name in table_positions:
                    position_info = table_positions[table_name]
                    
                    # Create sheet
                    sheet_name = ExcelTableExporter.get_valid_sheet_name(position_info.sheet_name)
                    ws = wb.create_sheet(title=sheet_name)
                    
                    # Add content to sheet
                    ExcelTableExporter.add_table_to_sheet(ws, table_data, position_info)
            
            # Add summary sheet
            ws_summary = wb.create_sheet(title="Summary")
            ExcelTableExporter.add_summary_sheet(ws_summary, tables_data, table_positions)
            
            # Add logo AFTER all content is added (to avoid repair issues)
            if logo_path and os.path.exists(logo_path):
                try:
                    # Load and resize logo
                    img = Image.open(logo_path)
                    # Resize to reasonable dimensions
                    max_size = (200, 80)  # width, height
                    img.thumbnail(max_size, Image.Resampling.LANCZOS)
                    
                    # Save to temporary file
                    temp_logo = "temp_logo.png"
                    img.save(temp_logo)
                    
                    # Add to Excel
                    excel_img = XLImage(temp_logo)
                    
                    for ws in wb.worksheets:
                        ws.add_image(excel_img, 'A1')
                    
                    # Clean up temp file
                    try:
                        os.remove(temp_logo)
                    except:
                        pass
                        
                except Exception as e:
                    logger.warning(f"Could not add logo: {e}")
                    # Continue without logo - don't crash
            
            # Save workbook
            if output_path:
                # Ensure directory exists
                os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
                wb.save(output_path)
                return True
            else:
                # Save to bytes
                excel_buffer = io.BytesIO()
                wb.save(excel_buffer)
                excel_buffer.seek(0)
                return excel_buffer
            
        except Exception as e:
            logger.error(f"Excel export error: {e}")
            logger.error(traceback.format_exc())
            raise Exception(f"Excel export error: {str(e)}")
    
    @staticmethod
    def get_valid_sheet_name(name: str) -> str:
        """Get valid Excel sheet name"""
        # Remove invalid characters
        invalid_chars = ['\\', '/', '*', '?', ':', '[', ']']
        for char in invalid_chars:
            name = name.replace(char, '_')
        
        # Truncate if too long
        if len(name) > 31:
            name = name[:28] + "..."
        
        # Ensure not empty
        if not name.strip():
            name = "Sheet"
        
        return name[:31]  # Excel sheet name limit
    
    @staticmethod
    def add_table_to_sheet(ws, table_data: Dict, position_info: TablePosition):
        """Add a table to Excel sheet at specified position WITH HEADER DATA"""
        try:
            # 1. Add header positions first (FIXED: Now actually writes data)
            header_positions = position_info.header_positions
            
            # Get first row of data for header values
            header_data = {}
            if table_data['data']:
                first_row = table_data['data'][0]
                # Try to map common header names to data columns
                for header_name in header_positions.keys():
                    # Try exact match
                    if header_name in first_row:
                        header_data[header_name] = first_row[header_name]
                    # Try case-insensitive match
                    else:
                        header_name_lower = header_name.lower()
                        for col_name, value in first_row.items():
                            if col_name.lower() == header_name_lower:
                                header_data[header_name] = value
                                break
                        else:
                            # Try partial match
                            for col_name, value in first_row.items():
                                if header_name_lower in col_name.lower() or col_name.lower() in header_name_lower:
                                    header_data[header_name] = value
                                    break
            
            # Write headers to their positions
            for header_name, cell_ref in header_positions.items():
                try:
                    # Get value from data or use header name
                    value = header_data.get(header_name, header_name)
                    ws[cell_ref] = str(value) if value is not None else header_name
                    
                    # Style the header
                    ws[cell_ref].font = Font(bold=True, size=11)
                    ws[cell_ref].fill = PatternFill(start_color="E6F3FF", end_color="E6F3FF", fill_type="solid")
                    
                except Exception as e:
                    logger.warning(f"Could not write header {header_name} to {cell_ref}: {e}")
            
            # 2. Add table title
            title_row = position_info.start_row - 3
            title_cell = f"{position_info.start_col}{title_row}"
            ws[title_cell] = f"Data from {table_data['display_name']}"
            ws[title_cell].font = Font(size=14, bold=True, color="366092")
            ws.merge_cells(f"{title_cell}:{get_column_letter(position_info.get_start_col_num() + 5)}{title_row}")
            ws[title_cell].alignment = Alignment(horizontal='center')
            
            # 3. Add column headers
            start_row = position_info.start_row
            start_col_num = position_info.get_start_col_num()
            columns = table_data['columns']
            
            for col_idx, col_name in enumerate(columns):
                cell = ws.cell(row=start_row, column=start_col_num + col_idx, value=col_name)
                cell.font = Font(bold=True, size=10)
                cell.fill = PatternFill(start_color="D9E1F2", end_color="D9E1F2", fill_type="solid")
                cell.alignment = Alignment(horizontal='center', vertical='center')
                cell.border = Border(
                    left=Side(style='thin', color='000000'),
                    right=Side(style='thin', color='000000'),
                    top=Side(style='thin', color='000000'),
                    bottom=Side(style='thin', color='000000')
                )
            
            # 4. Add data rows
            data = table_data['data']
            for row_idx, row_data in enumerate(data):
                excel_row = start_row + 1 + row_idx
                for col_idx, col_name in enumerate(columns):
                    value = row_data.get(col_name, '')
                    cell = ws.cell(row=excel_row, column=start_col_num + col_idx, value=value)
                    
                    # Add alternating row colors
                    if row_idx % 2 == 0:
                        cell.fill = PatternFill(start_color="F8F9FA", end_color="F8F9FA", fill_type="solid")
                    else:
                        cell.fill = PatternFill(start_color="FFFFFF", end_color="FFFFFF", fill_type="solid")
                    
                    # Add borders
                    cell.border = Border(
                        left=Side(style='thin', color='CCCCCC'),
                        right=Side(style='thin', color='CCCCCC'),
                        top=Side(style='thin', color='CCCCCC'),
                        bottom=Side(style='thin', color='CCCCCC')
                    )
            
            # 5. Auto-size columns
            ExcelTableExporter.auto_size_columns(ws, start_col_num, len(columns), start_row, len(data))
            
            # 6. Freeze header row
            ws.freeze_panes = ws.cell(row=start_row + 1, column=start_col_num)
            
            # 7. Add sheet info
            info_row = start_row + len(data) + 3
            ws.cell(row=info_row, column=start_col_num, value="Generated:").font = Font(bold=True)
            ws.cell(row=info_row, column=start_col_num + 1, value=datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            
            ws.cell(row=info_row + 1, column=start_col_num, value="Total Rows:").font = Font(bold=True)
            ws.cell(row=info_row + 1, column=start_col_num + 1, value=len(data))
            
            ws.cell(row=info_row + 2, column=start_col_num, value="Total Columns:").font = Font(bold=True)
            ws.cell(row=info_row + 2, column=start_col_num + 1, value=len(columns))
            
        except Exception as e:
            logger.error(f"Error adding table to sheet: {e}")
            raise
    
    @staticmethod
    def auto_size_columns(ws, start_col: int, num_cols: int, start_row: int, num_rows: int):
        """Auto-size columns with limits"""
        for col_offset in range(num_cols):
            col_idx = start_col + col_offset
            max_length = 0
            
            # Check header
            header_cell = ws.cell(row=start_row, column=col_idx)
            if header_cell.value:
                max_length = len(str(header_cell.value))
            
            # Check data (limit to first 100 rows for performance)
            check_rows = min(100, num_rows)
            for row_offset in range(check_rows):
                cell = ws.cell(row=start_row + 1 + row_offset, column=col_idx)
                if cell.value:
                    max_length = max(max_length, len(str(cell.value)))
            
            # Set width with limits
            adjusted_width = min(max_length + 2, 50)  # Max width 50
            adjusted_width = max(adjusted_width, 8)   # Min width 8
            
            ws.column_dimensions[get_column_letter(col_idx)].width = adjusted_width
    
    @staticmethod
    def add_summary_sheet(ws, tables_data: Dict, table_positions: Dict[str, TablePosition]):
        """Add summary sheet with export details"""
        ws.title = "Export Summary"
        
        # Title
        ws['A1'] = "MULTI-TABLE EXPORT SUMMARY"
        ws['A1'].font = Font(size=18, bold=True, color="366092")
        ws.merge_cells('A1:E1')
        ws['A1'].alignment = Alignment(horizontal='center')
        
        # Metadata
        ws['A3'] = "Export Date:"
        ws['B3'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        ws['A3'].font = Font(bold=True)
        
        ws['A4'] = "Total Tables:"
        ws['B4'] = len([t for t in tables_data.values() if t.get('success', False)])
        ws['A4'].font = Font(bold=True)
        
        # Table details header
        ws['A6'] = "TABLE DETAILS"
        ws['A6'].font = Font(size=14, bold=True, color="366092")
        ws.merge_cells('A6:E6')
        
        # Column headers
        headers = ["Table Name", "Sheet Name", "Start Position", "Rows Exported", "Total Rows", "Status"]
        for col, header in enumerate(headers, start=1):
            cell = ws.cell(row=8, column=col, value=header)
            cell.font = Font(bold=True)
            cell.fill = PatternFill(start_color="D9E1F2", end_color="D9E1F2", fill_type="solid")
            cell.alignment = Alignment(horizontal='center')
            cell.border = Border(
                left=Side(style='thin'),
                right=Side(style='thin'),
                top=Side(style='thin'),
                bottom=Side(style='thin')
            )
        
        # Table data
        row = 9
        success_count = 0
        for table_name, table_data in tables_data.items():
            if table_name in table_positions:
                pos_info = table_positions[table_name]
                
                ws.cell(row=row, column=1, value=table_name)
                ws.cell(row=row, column=2, value=pos_info.sheet_name)
                ws.cell(row=row, column=3, value=f"{pos_info.start_col}{pos_info.start_row}")
                
                if table_data.get('success', False):
                    ws.cell(row=row, column=4, value=table_data['row_count'])
                    ws.cell(row=row, column=5, value=table_data.get('total_count', table_data['row_count']))
                    ws.cell(row=row, column=6, value="✅ SUCCESS")
                    success_count += 1
                else:
                    ws.cell(row=row, column=4, value=0)
                    ws.cell(row=row, column=5, value=0)
                    ws.cell(row=row, column=6, value=f"❌ {table_data.get('error', 'Failed')}")
                
                row += 1
        
        # Summary stats
        summary_row = row + 2
        ws.cell(row=summary_row, column=1, value="EXPORT SUMMARY").font = Font(size=12, bold=True)
        ws.merge_cells(f"A{summary_row}:E{summary_row}")
        
        ws.cell(row=summary_row + 1, column=1, value="Successful Tables:").font = Font(bold=True)
        ws.cell(row=summary_row + 1, column=2, value=success_count)
        
        ws.cell(row=summary_row + 2, column=1, value="Failed Tables:").font = Font(bold=True)
        ws.cell(row=summary_row + 2, column=2, value=len(tables_data) - success_count)
        
        # Auto-size columns
        for col in range(1, 7):
            ws.column_dimensions[get_column_letter(col)].width = 20

# ============================================================================
# MAIN APPLICATION (FIXED GUI)
# ============================================================================

class MultiTableExporterApp:
    """Main Application - FIXED VERSION"""
    
    def __init__(self, root):
        self.root = root
        self.root.title("Multi-Table Excel Exporter - Professional Edition")
        self.root.geometry("1400x900")
        
        # Set minimum size
        self.root.minsize(1200, 700)
        
        # Database connection
        self.db = DatabaseManager()
        self.exporter = ExcelTableExporter()
        
        # Variables
        self.server_var = tk.StringVar(value="MAHESHWAGH\\WINCC")
        self.database_var = tk.StringVar(value="VPI1")
        
        # Table selection
        self.selected_tables = []
        self.table_checkboxes = {}
        
        # Logo
        self.logo_path = None
        
        # Data storage
        self.tables_data = {}
        self.table_positions = {}
        
        # Setup UI
        self.setup_ui()
        
        # Load saved settings
        self.load_settings()
    
    def setup_ui(self):
        """Setup the user interface - FIXED LAYOUT"""
        # Configure ttk styles
        self.setup_styles()
        
        # Create main container
        main_container = ttk.Frame(self.root)
        main_container.pack(fill='both', expand=True, padx=5, pady=5)
        
        # Create notebook for tabs
        self.notebook = ttk.Notebook(main_container)
        self.notebook.pack(fill='both', expand=True)
        
        # Create tabs
        self.setup_connection_tab()
        self.setup_table_selection_tab()
        self.setup_position_mapping_tab()
        self.setup_export_tab()
        
        # Status bar
        self.status_bar = ttk.Label(self.root, text="🚀 Ready to connect to database", 
                                   relief=tk.SUNKEN, anchor=tk.W)
        self.status_bar.pack(side=tk.BOTTOM, fill=tk.X)
        
        # Bind tab change event
        self.notebook.bind("<<NotebookTabChanged>>", self.on_tab_changed)
    
    def setup_styles(self):
        """Configure ttk styles"""
        style = ttk.Style()
        try:
            style.theme_use('clam')
        except:
            pass
        
        # Configure colors
        style.configure('Title.TLabel', font=('Segoe UI', 16, 'bold'), foreground='#2C3E50')
        style.configure('Header.TLabel', font=('Segoe UI', 11, 'bold'), foreground='#34495E')
        style.configure('Subheader.TLabel', font=('Segoe UI', 10), foreground='#7F8C8D')
        
        # Configure buttons
        style.configure('Accent.TButton', font=('Segoe UI', 10, 'bold'), 
                       padding=8, background='#3498DB', foreground='white')
        style.map('Accent.TButton',
                 background=[('active', '#2980B9')])
        
        style.configure('Success.TButton', font=('Segoe UI', 10, 'bold'),
                       padding=8, background='#27AE60', foreground='white')
        
        style.configure('Warning.TButton', font=('Segoe UI', 10, 'bold'),
                       padding=8, background='#F39C12', foreground='white')
        
        # Configure frames
        style.configure('Card.TFrame', background='white', relief='solid', borderwidth=1)
    
    def setup_connection_tab(self):
        """Setup Connection Tab - FIXED LAYOUT"""
        conn_tab = ttk.Frame(self.notebook)
        self.notebook.add(conn_tab, text="🔌 Connection")
        
        # Main frame with padding
        main_frame = ttk.Frame(conn_tab, padding="30")
        main_frame.pack(fill='both', expand=True)
        
        # Title
        title_frame = ttk.Frame(main_frame)
        title_frame.pack(fill='x', pady=(0, 30))
        
        ttk.Label(title_frame, text="Database Connection", style='Title.TLabel').pack()
        ttk.Label(title_frame, text="Connect to your SQL Server database to access tables", 
                 style='Subheader.TLabel').pack()
        
        # Connection card
        card = ttk.LabelFrame(main_frame, text="Connection Settings", padding="25")
        card.pack(fill='both', expand=True)
        
        # Server
        server_frame = ttk.Frame(card)
        server_frame.pack(fill='x', pady=(0, 15))
        
        ttk.Label(server_frame, text="Server:", font=('Segoe UI', 11, 'bold'), 
                 width=15, anchor='w').pack(side=tk.LEFT)
        server_entry = ttk.Entry(server_frame, textvariable=self.server_var, font=('Segoe UI', 10))
        server_entry.pack(side=tk.LEFT, fill='x', expand=True, padx=(10, 0))
        
        # Database
        db_frame = ttk.Frame(card)
        db_frame.pack(fill='x', pady=(0, 20))
        
        ttk.Label(db_frame, text="Database:", font=('Segoe UI', 11, 'bold'), 
                 width=15, anchor='w').pack(side=tk.LEFT)
        db_entry = ttk.Entry(db_frame, textvariable=self.database_var, font=('Segoe UI', 10))
        db_entry.pack(side=tk.LEFT, fill='x', expand=True, padx=(10, 0))
        
        # Connection buttons
        btn_frame = ttk.Frame(card)
        btn_frame.pack(fill='x', pady=(0, 30))
        
        self.connect_btn = ttk.Button(btn_frame, text="🔗 Connect to Database", 
                                     command=self.connect_db, style='Accent.TButton', width=20)
        self.connect_btn.pack(side=tk.LEFT, padx=(0, 10))
        
        ttk.Button(btn_frame, text="🧪 Test Connection", 
                  command=self.test_connection, width=15).pack(side=tk.LEFT, padx=10)
        
        ttk.Button(btn_frame, text="🔄 Refresh", 
                  command=self.refresh_tables, width=15).pack(side=tk.LEFT, padx=10)
        
        # Status display
        status_frame = ttk.LabelFrame(card, text="Connection Status", padding="15")
        status_frame.pack(fill='x', pady=(0, 20))
        
        self.status_label = ttk.Label(status_frame, text="🔴 Not connected", 
                                     font=('Segoe UI', 10), foreground='red')
        self.status_label.pack(anchor='w')
        
        self.server_info_label = ttk.Label(status_frame, text="", font=('Segoe UI', 9))
        self.server_info_label.pack(anchor='w')
        
        # Quick guide
        guide_frame = ttk.LabelFrame(card, text="Quick Start Guide", padding="15")
        guide_frame.pack(fill='x')
        
        steps = [
            "1. Enter your SQL Server details above",
            "2. Click 'Connect to Database'",
            "3. Go to 'Table Selection' tab to choose tables",
            "4. Map tables to Excel positions in next tab",
            "5. Export all tables to Excel with one click"
        ]
        
        for step in steps:
            ttk.Label(guide_frame, text=step, font=('Segoe UI', 9)).pack(anchor='w', pady=2)
    
    def setup_table_selection_tab(self):
        """Setup Table Selection Tab - FIXED LAYOUT"""
        selection_tab = ttk.Frame(self.notebook)
        self.notebook.add(selection_tab, text="📋 Table Selection")
        
        # Main frame
        main_frame = ttk.Frame(selection_tab, padding="20")
        main_frame.pack(fill='both', expand=True)
        
        # Title
        title_frame = ttk.Frame(main_frame)
        title_frame.pack(fill='x', pady=(0, 20))
        
        ttk.Label(title_frame, text="Select Tables for Export", style='Title.TLabel').pack()
        ttk.Label(title_frame, text="Choose which database tables to export to Excel", 
                 style='Subheader.TLabel').pack()
        
        # Control panel
        control_frame = ttk.LabelFrame(main_frame, text="Selection Controls", padding="15")
        control_frame.pack(fill='x', pady=(0, 20))
        
        btn_frame = ttk.Frame(control_frame)
        btn_frame.pack()
        
        buttons = [
            ("Select All", self.select_all_tables, 'Accent.TButton'),
            ("Clear All", self.clear_all_tables, None),
            ("Invert Selection", self.invert_selection, None),
            ("Refresh List", self.refresh_tables, None)
        ]
        
        for text, command, style_name in buttons:
            btn = ttk.Button(btn_frame, text=text, command=command, width=15)
            if style_name:
                btn.configure(style=style_name)
            btn.pack(side=tk.LEFT, padx=5)
        
        # Selected count
        self.selected_count_label = ttk.Label(control_frame, 
                                             text="0 tables selected",
                                             font=('Segoe UI', 10, 'bold'),
                                             foreground='#27AE60')
        self.selected_count_label.pack(pady=(10, 0))
        
        # Table selection frame with scrollbar
        table_container = ttk.Frame(main_frame)
        table_container.pack(fill='both', expand=True)
        
        # Create canvas with scrollbar
        canvas = tk.Canvas(table_container, highlightthickness=0)
        scrollbar = ttk.Scrollbar(table_container, orient="vertical", command=canvas.yview)
        self.checkbox_container = ttk.Frame(canvas)
        
        canvas.pack(side=tk.LEFT, fill='both', expand=True)
        scrollbar.pack(side=tk.RIGHT, fill='y')
        
        canvas.configure(yscrollcommand=scrollbar.set)
        canvas.create_window((0, 0), window=self.checkbox_container, anchor="nw")
        
        # Configure scrolling
        self.checkbox_container.bind("<Configure>", 
            lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        
        # Bind mouse wheel
        canvas.bind_all("<MouseWheel>", lambda e: canvas.yview_scroll(int(-1*(e.delta/120)), "units"))
    
    def setup_position_mapping_tab(self):
        """Setup Position Mapping Tab - FIXED LAYOUT"""
        mapping_tab = ttk.Frame(self.notebook)
        self.notebook.add(mapping_tab, text="🗺️ Position Mapping")
        
        # Main frame
        main_frame = ttk.Frame(mapping_tab, padding="20")
        main_frame.pack(fill='both', expand=True)
        
        # Title
        title_frame = ttk.Frame(main_frame)
        title_frame.pack(fill='x', pady=(0, 20))
        
        ttk.Label(title_frame, text="Configure Excel Positions", style='Title.TLabel').pack()
        ttk.Label(title_frame, text="Set where each table should appear in Excel", 
                 style='Subheader.TLabel').pack()
        
        # Instructions
        instr_frame = ttk.LabelFrame(main_frame, text="📚 Instructions", padding="15")
        instr_frame.pack(fill='x', pady=(0, 20))
        
        instructions = [
            "• Each table will be placed on a SEPARATE sheet",
            "• Set START ROW where data begins (e.g., 16 for row 16)",
            "• Set START COLUMN where data begins (e.g., A for column A)",
            "• Configure HEADER positions for common fields (optional)",
            "• Use 'Configure Table Positions' button below to begin"
        ]
        
        for instr in instructions:
            ttk.Label(instr_frame, text=instr, font=('Segoe UI', 9)).pack(anchor='w', pady=2)
        
        # Configuration button
        btn_frame = ttk.Frame(main_frame)
        btn_frame.pack(pady=10)
        
        self.map_btn = ttk.Button(btn_frame, text="⚙️ Configure Table Positions", 
                                 command=self.configure_positions, 
                                 style='Accent.TButton', width=25, state='disabled')
        self.map_btn.pack()
        
        # Current mappings display
        mapping_frame = ttk.LabelFrame(main_frame, text="Current Position Mappings", padding="15")
        mapping_frame.pack(fill='both', expand=True, pady=(20, 0))
        
        self.mapping_text = scrolledtext.ScrolledText(mapping_frame, height=15, wrap=tk.WORD,
                                                     font=('Consolas', 9))
        self.mapping_text.pack(fill='both', expand=True)
        
        # Initial message
        self.mapping_text.insert(1.0, 
            "No table positions configured yet.\n\n"
            "1. First, select tables in the 'Table Selection' tab\n"
            "2. Then click 'Configure Table Positions' button above\n"
            "3. Set starting positions and header mappings for each table\n"
            "4. Apply the mappings to continue")
        self.mapping_text.config(state='disabled')
    
    def setup_export_tab(self):
        """Setup Export Tab - FIXED LAYOUT"""
        export_tab = ttk.Frame(self.notebook)
        self.notebook.add(export_tab, text="💾 Export")
        
        # Main frame
        main_frame = ttk.Frame(export_tab, padding="20")
        main_frame.pack(fill='both', expand=True)
        
        # Title
        title_frame = ttk.Frame(main_frame)
        title_frame.pack(fill='x', pady=(0, 20))
        
        ttk.Label(title_frame, text="Export to Excel", style='Title.TLabel').pack()
        ttk.Label(title_frame, text="Export all selected tables to Excel with configured positions", 
                 style='Subheader.TLabel').pack()
        
        # Export settings card
        settings_card = ttk.LabelFrame(main_frame, text="Export Settings", padding="25")
        settings_card.pack(fill='both', expand=True)
        
        # Logo section
        logo_frame = ttk.Frame(settings_card)
        logo_frame.pack(fill='x', pady=(0, 20))
        
        ttk.Label(logo_frame, text="Company Logo:", font=('Segoe UI', 11, 'bold'), 
                 width=15, anchor='w').pack(side=tk.LEFT)
        
        logo_btn_frame = ttk.Frame(logo_frame)
        logo_btn_frame.pack(side=tk.LEFT, padx=(10, 0))
        
        ttk.Button(logo_btn_frame, text="🖼️ Upload Logo", 
                  command=self.upload_logo, width=15).pack(side=tk.LEFT, padx=(0, 10))
        ttk.Button(logo_btn_frame, text="🗑️ Remove", 
                  command=self.remove_logo, width=10).pack(side=tk.LEFT)
        
        self.logo_label = ttk.Label(logo_frame, text="No logo selected", 
                                   font=('Segoe UI', 9), foreground='gray')
        self.logo_label.pack(side=tk.LEFT, padx=(20, 0))
        
        # Export options
        options_frame = ttk.Frame(settings_card)
        options_frame.pack(fill='x', pady=(0, 30))
        
        # Row limit
        limit_frame = ttk.Frame(options_frame)
        limit_frame.pack(anchor='w', pady=(0, 10))
        
        ttk.Label(limit_frame, text="Rows per table:", font=('Segoe UI', 10)).pack(side=tk.LEFT, padx=(0, 10))
        self.row_limit_var = tk.StringVar(value="1000")
        ttk.Entry(limit_frame, textvariable=self.row_limit_var, width=10).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Label(limit_frame, text="(0 = all rows)").pack(side=tk.LEFT)
        
        # Checkboxes
        check_frame = ttk.Frame(options_frame)
        check_frame.pack(anchor='w')
        
        self.include_summary_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(check_frame, text="Include Summary Sheet", 
                       variable=self.include_summary_var).pack(side=tk.LEFT, padx=(0, 20))
        
        self.auto_size_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(check_frame, text="Auto-size Columns", 
                       variable=self.auto_size_var).pack(side=tk.LEFT)
        
        # Export buttons
        export_btn_frame = ttk.Frame(settings_card)
        export_btn_frame.pack(fill='x', pady=(0, 20))
        
        self.export_btn = ttk.Button(export_btn_frame, text="🚀 Export All Tables to Excel", 
                                    command=self.export_to_excel, 
                                    style='Success.TButton', width=25, state='disabled')
        self.export_btn.pack(pady=(0, 10))
        
        ttk.Button(export_btn_frame, text="👁️ Export Preview (10 rows per table)", 
                  command=self.export_preview, width=25).pack()
        
        # Export log
        log_frame = ttk.LabelFrame(settings_card, text="Export Log", padding="15")
        log_frame.pack(fill='both', expand=True)
        
        self.log_text = scrolledtext.ScrolledText(log_frame, height=10, wrap=tk.WORD,
                                                 font=('Consolas', 9))
        self.log_text.pack(fill='both', expand=True)
        
        # Configure log tags
        self.log_text.tag_configure('success', foreground='#27AE60', font=('Consolas', 9, 'bold'))
        self.log_text.tag_configure('error', foreground='#E74C3C', font=('Consolas', 9, 'bold'))
        self.log_text.tag_configure('info', foreground='#3498DB', font=('Consolas', 9))
        self.log_text.tag_configure('warning', foreground='#F39C12', font=('Consolas', 9))
        
        # Initial log message
        self.log_message("📋 Ready for export. Configure table positions first.", 'info')
    
    def on_tab_changed(self, event):
        """Handle tab change event"""
        current_tab = self.notebook.index(self.notebook.select())
        tab_names = ["Connection", "Table Selection", "Position Mapping", "Export"]
        
        if current_tab < len(tab_names):
            self.status_bar.config(text=f"📌 Current tab: {tab_names[current_tab]}")
    
    # ============================================================================
    # DATABASE METHODS
    # ============================================================================
    
    def connect_db(self):
        """Connect to database"""
        def connect():
            self.status_bar.config(text="🔗 Connecting to database...")
            self.status_label.config(text="🟡 Connecting...", foreground='orange')
            
            try:
                success, message = self.db.connect(
                    server=self.server_var.get(),
                    database=self.database_var.get(),
                    use_windows_auth=True
                )
                
                if success:
                    self.status_label.config(text="🟢 Connected", foreground='green')
                    self.server_info_label.config(
                        text=f"Connected to: {self.db.server_info.get('server', 'Unknown')}")
                    self.status_bar.config(text="✅ Connected successfully")
                    self.log_message("✅ Database connected successfully", 'success')
                    self.refresh_tables()
                else:
                    self.status_label.config(text="🔴 Connection Failed", foreground='red')
                    self.status_bar.config(text=f"❌ Connection failed: {message}")
                    self.log_message(f"❌ Connection failed: {message}", 'error')
                    messagebox.showerror("Connection Error", message)
                    
            except Exception as e:
                self.status_label.config(text="🔴 Connection Error", foreground='red')
                self.status_bar.config(text=f"❌ Error: {str(e)}")
                self.log_message(f"❌ Connection error: {str(e)}", 'error')
                messagebox.showerror("Connection Error", f"Error during connection:\n{str(e)}")
        
        threading.Thread(target=connect, daemon=True).start()
    
    def disconnect_db(self):
        """Disconnect from database"""
        try:
            self.db.disconnect()
            self.status_label.config(text="🔴 Disconnected", foreground='red')
            self.server_info_label.config(text="")
            self.status_bar.config(text="🔌 Disconnected")
            self.log_message("🔌 Disconnected from database", 'info')
            
            # Clear tables
            self.clear_table_checkboxes()
            self.selected_tables.clear()
            self.selected_count_label.config(text="0 tables selected")
            
        except Exception as e:
            self.log_message(f"❌ Error during disconnect: {str(e)}", 'error')
    
    def test_connection(self):
        """Test database connection"""
        def test():
            self.status_bar.config(text="🧪 Testing connection...")
            
            try:
                success, message = self.db.connect(
                    server=self.server_var.get(),
                    database=self.database_var.get(),
                    use_windows_auth=True
                )
                
                if success:
                    self.status_bar.config(text="✅ Connection test successful")
                    self.db.disconnect()
                    messagebox.showinfo("Connection Test", "✅ Connection successful!")
                else:
                    self.status_bar.config(text="❌ Connection test failed")
                    messagebox.showerror("Connection Test", f"❌ Connection failed:\n{message}")
                    
            except Exception as e:
                self.status_bar.config(text=f"❌ Error: {str(e)}")
                messagebox.showerror("Connection Test", f"❌ Error during connection test:\n{str(e)}")
        
        threading.Thread(target=test, daemon=True).start()
    
    def refresh_tables(self):
        """Refresh list of tables"""
        if not self.db.connected:
            messagebox.showwarning("Not Connected", "Please connect to database first")
            return
        
        def refresh():
            self.status_bar.config(text="📊 Loading tables...")
            
            try:
                tables = self.db.get_tables()
                self.create_table_checkboxes(tables)
                
                self.status_bar.config(text=f"✅ Loaded {len(tables)} tables")
                self.log_message(f"✅ Loaded {len(tables)} tables", 'success')
                
            except Exception as e:
                self.status_bar.config(text=f"❌ Error loading tables: {str(e)}")
                self.log_message(f"❌ Error loading tables: {str(e)}", 'error')
        
        threading.Thread(target=refresh, daemon=True).start()
    
    def create_table_checkboxes(self, tables: List[str]):
        """Create checkboxes for table selection"""
        # Clear existing checkboxes
        self.clear_table_checkboxes()
        self.table_checkboxes.clear()
        
        # Create new checkboxes in 3 columns for better layout
        for i, table in enumerate(tables):
            var = tk.BooleanVar(value=False)
            self.table_checkboxes[table] = var
            
            # Create checkbox with better styling
            cb_frame = ttk.Frame(self.checkbox_container)
            cb_frame.grid(row=i//3, column=i%3, sticky=tk.W, padx=15, pady=5)
            
            cb = ttk.Checkbutton(cb_frame, text=table, variable=var,
                                command=self.update_selected_count)
            cb.pack(anchor='w')
            
            # Add tooltip for full name
            if len(table) > 40:
                cb_frame.tooltip_text = table
        
        # Update count
        self.update_selected_count()
    
    def clear_table_checkboxes(self):
        """Clear all table checkboxes"""
        for widget in self.checkbox_container.winfo_children():
            widget.destroy()
    
    def select_all_tables(self):
        """Select all tables"""
        for var in self.table_checkboxes.values():
            var.set(True)
        self.update_selected_count()
        self.log_message("✅ Selected all tables", 'info')
    
    def clear_all_tables(self):
        """Clear all table selections"""
        for var in self.table_checkboxes.values():
            var.set(False)
        self.update_selected_count()
        self.log_message("🗑️ Cleared all table selections", 'info')
    
    def invert_selection(self):
        """Invert table selection"""
        for var in self.table_checkboxes.values():
            var.set(not var.get())
        self.update_selected_count()
        self.log_message("🔄 Inverted table selection", 'info')
    
    def update_selected_count(self):
        """Update selected tables count"""
        self.selected_tables = [table for table, var in self.table_checkboxes.items() if var.get()]
        count = len(self.selected_tables)
        self.selected_count_label.config(text=f"{count} table{'s' if count != 1 else ''} selected")
        
        # Enable/disable buttons based on selection
        if count > 0:
            self.map_btn.config(state='normal')
            self.export_btn.config(state='normal')
        else:
            self.map_btn.config(state='disabled')
            self.export_btn.config(state='disabled')
    
    # ============================================================================
    # POSITION MAPPING METHODS
    # ============================================================================
    
    def configure_positions(self):
        """Configure table positions"""
        if not self.selected_tables:
            messagebox.showwarning("No Selection", "Please select tables first")
            return
        
        # Open position mapping dialog
        dialog = TablePositionDialog(self.root, self.selected_tables)
        
        # Wait for dialog to close
        self.root.wait_window(dialog.dialog)
        
        # Get mapping result
        positions = dialog.get_positions()
        if positions:
            self.table_positions = positions
            self.update_mapping_display()
            self.export_btn.config(state='normal')
            self.log_message("✅ Table positions configured", 'success')
            self.log_message(f"📊 Configured {len(positions)} tables", 'info')
            
            # Show summary
            messagebox.showinfo("Positions Applied", 
                              f"✅ Table positions configured successfully!\n\n"
                              f"📋 Tables configured: {len(positions)}\n"
                              f"📄 Each table will be on a separate sheet\n"
                              f"📍 Data starts at specified positions")
    
    def update_mapping_display(self):
        """Update position mapping display"""
        self.mapping_text.config(state='normal')
        self.mapping_text.delete(1.0, tk.END)
        
        if not self.table_positions:
            self.mapping_text.insert(1.0, 
                "No table positions configured.\n\n"
                "1. First, select tables in the 'Table Selection' tab\n"
                "2. Then click 'Configure Table Positions' button above\n"
                "3. Set starting positions and header mappings for each table\n"
                "4. Apply the mappings to continue")
        else:
            # Add mapping information
            self.mapping_text.insert(tk.END, "✅ TABLE POSITION MAPPINGS CONFIGURED\n")
            self.mapping_text.insert(tk.END, "="*50 + "\n\n")
            
            for table_name, table_pos in self.table_positions.items():
                display_name = self.db.get_display_name(table_name)
                
                self.mapping_text.insert(tk.END, f"📄 {display_name}\n", 'header')
                self.mapping_text.insert(tk.END, f"   📍 Start: {table_pos.start_col}{table_pos.start_row}\n")
                self.mapping_text.insert(tk.END, f"   📋 Sheet: {table_pos.sheet_name}\n")
                
                # Add header positions if any
                if table_pos.header_positions:
                    self.mapping_text.insert(tk.END, "   🏷️ Headers:\n")
                    for header, pos in table_pos.header_positions.items():
                        self.mapping_text.insert(tk.END, f"     • {header}: {pos}\n")
                
                self.mapping_text.insert(tk.END, "\n")
        
        self.mapping_text.config(state='disabled')
    
    # ============================================================================
    # EXPORT METHODS
    # ============================================================================
    
    def upload_logo(self):
        """Upload company logo"""
        filetypes = [("Image files", "*.png *.jpg *.jpeg"), ("All files", "*.*")]
        filename = filedialog.askopenfilename(
            title="Select Company Logo", 
            filetypes=filetypes
        )
        
        if filename:
            try:
                # Validate image
                img = Image.open(filename)
                img.verify()  # Verify it's a valid image
                
                self.logo_path = filename
                self.logo_label.config(
                    text=os.path.basename(filename), 
                    foreground='green'
                )
                self.log_message(f"✅ Logo uploaded: {os.path.basename(filename)}", 'success')
                
            except Exception as e:
                messagebox.showerror("Invalid Image", 
                                   f"Failed to load image:\n{str(e)}\n\n"
                                   "Please select a valid PNG or JPG file.")
    
    def remove_logo(self):
        """Remove uploaded logo"""
        self.logo_path = None
        self.logo_label.config(text="No logo selected", foreground='gray')
        self.log_message("🗑️ Logo removed", 'info')
    
    def fetch_table_data(self):
        """Fetch data for all selected tables"""
        if not self.selected_tables:
            messagebox.showwarning("No Selection", "Please select tables first")
            return False
        
        # Get row limit
        try:
            row_limit = int(self.row_limit_var.get())
            if row_limit < 0:
                row_limit = 0
        except:
            row_limit = 1000
        
        def fetch_all():
            self.status_bar.config(text="📥 Fetching table data...")
            self.log_message("📥 Fetching data for selected tables...", 'info')
            
            self.tables_data.clear()
            success_count = 0
            total_rows = 0
            
            for table in self.selected_tables:
                self.status_bar.config(text=f"📥 Fetching {self.db.get_display_name(table)}...")
                
                try:
                    data = self.db.fetch_table_data(table, limit=row_limit)
                    self.tables_data[table] = data
                    
                    if data['success']:
                        success_count += 1
                        total_rows += data['row_count']
                        self.log_message(f"✅ {self.db.get_display_name(table)}: {data['row_count']:,} rows", 'success')
                    else:
                        self.log_message(f"❌ {self.db.get_display_name(table)}: {data.get('error', 'Unknown error')}", 'error')
                        
                except Exception as e:
                    self.tables_data[table] = {'success': False, 'error': str(e)}
                    self.log_message(f"❌ {self.db.get_display_name(table)}: {str(e)}", 'error')
            
            self.status_bar.config(text=f"✅ Fetched {success_count} of {len(self.selected_tables)} tables ({total_rows:,} total rows)")
            
            if success_count > 0:
                self.log_message(f"✅ Successfully fetched {success_count} tables ({total_rows:,} total rows)", 'success')
                return True
            else:
                self.log_message("❌ Failed to fetch any tables", 'error')
                return False
        
        # Run in thread
        thread = threading.Thread(target=fetch_all, daemon=True)
        thread.start()
        thread.join(timeout=30)  # Wait up to 30 seconds
        
        return success_count > 0
    
    def export_to_excel(self):
        """Export selected tables to Excel"""
        if not self.selected_tables:
            messagebox.showwarning("No Selection", "Please select tables first")
            return
        
        if not self.table_positions:
            messagebox.showwarning("No Positions", "Please configure table positions first")
            return
        
        # Ask for confirmation
        response = messagebox.askyesno("Confirm Export", 
                                      f"Export {len(self.selected_tables)} tables to Excel?\n\n"
                                      f"This will:\n"
                                      f"• Create a new Excel file\n"
                                      f"• Put each table on a separate sheet\n"
                                      f"• Use configured positions and headers\n"
                                      f"• Add company logo (if provided)")
        
        if not response:
            return
        
        # Generate filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"MultiTable_Export_{timestamp}.xlsx"
        
        file_path = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")],
            initialfile=filename,
            title="Save Excel File"
        )
        
        if not file_path:
            return
        
        # Fetch data first
        self.log_message("🔄 Starting data fetch...", 'info')
        if not self.fetch_table_data():
            messagebox.showerror("Export Failed", "Could not fetch data from database")
            return
        
        # Wait a moment for UI to update
        self.root.after(1000, lambda: self.do_export(file_path))
    
    def do_export(self, file_path: str):
        """Perform the export after data is fetched"""
        def export():
            self.status_bar.config(text="📤 Exporting to Excel...")
            self.log_message("📤 Creating Excel file...", 'info')
            
            try:
                # Export tables to Excel
                success = self.exporter.export_tables_to_excel(
                    tables_data=self.tables_data,
                    table_positions=self.table_positions,
                    logo_path=self.logo_path,
                    output_path=file_path
                )
                
                if success:
                    file_size = os.path.getsize(file_path) / 1024  # Size in KB
                    
                    self.status_bar.config(text=f"✅ Excel file created: {os.path.basename(file_path)}")
                    self.log_message(f"✅ Excel file created successfully!", 'success')
                    self.log_message(f"📁 File: {file_path}", 'info')
                    self.log_message(f"📊 Size: {file_size:.1f} KB", 'info')
                    
                    # Count successful exports
                    success_count = sum(1 for data in self.tables_data.values() 
                                      if data.get('success', False))
                    total_rows = sum(data.get('row_count', 0) 
                                   for data in self.tables_data.values() 
                                   if data.get('success', False))
                    
                    self.log_message(f"📋 Tables exported: {success_count}", 'info')
                    self.log_message(f"📈 Total rows: {total_rows:,}", 'info')
                    
                    # Show success dialog
                    self.root.after(0, lambda: self.show_export_success(
                        file_path, success_count, total_rows, file_size))
                    
                else:
                    self.status_bar.config(text="❌ Export failed")
                    self.log_message("❌ Failed to create Excel file", 'error')
                    
            except Exception as e:
                error_msg = str(e)
                self.status_bar.config(text=f"❌ Export error: {error_msg}")
                self.log_message(f"❌ Export error: {error_msg}", 'error')
                logger.error(f"Export error: {traceback.format_exc()}")
                
                self.root.after(0, lambda: messagebox.showerror("Export Error", 
                    f"Failed to create Excel file:\n{error_msg}"))
        
        threading.Thread(target=export, daemon=True).start()
    
    def export_preview(self):
        """Export preview with limited rows"""
        original_limit = self.row_limit_var.get()
        self.row_limit_var.set("10")
        self.export_to_excel()
        self.row_limit_var.set(original_limit)
    
    def show_export_success(self, file_path: str, success_count: int, total_rows: int, file_size: float):
        """Show export success dialog"""
        file_name = os.path.basename(file_path)
        file_dir = os.path.dirname(file_path)
        
        dialog = tk.Toplevel(self.root)
        dialog.title("✅ Export Successful")
        dialog.geometry("500x400")
        dialog.transient(self.root)
        dialog.grab_set()
        
        # Center dialog
        dialog.update_idletasks()
        x = (dialog.winfo_screenwidth() - dialog.winfo_width()) // 2
        y = (dialog.winfo_screenheight() - dialog.winfo_height()) // 2
        dialog.geometry(f"+{x}+{y}")
        
        # Success message
        ttk.Label(dialog, text="✅", font=('Arial', 32)).pack(pady=(20, 10))
        ttk.Label(dialog, text="Excel File Created Successfully!", 
                 font=('Segoe UI', 14, 'bold')).pack()
        
        # Stats
        stats_frame = ttk.Frame(dialog, padding="20")
        stats_frame.pack(fill='x')
        
        stats = [
            ("File:", file_name),
            ("Location:", file_dir),
            ("Size:", f"{file_size:.1f} KB"),
            ("Tables Exported:", f"{success_count}"),
            ("Total Rows:", f"{total_rows:,}"),
            ("Generated:", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        ]
        
        for label, value in stats:
            row_frame = ttk.Frame(stats_frame)
            row_frame.pack(fill='x', pady=2)
            
            ttk.Label(row_frame, text=label, font=('Segoe UI', 9, 'bold'), 
                     width=15, anchor='w').pack(side=tk.LEFT)
            ttk.Label(row_frame, text=value, font=('Segoe UI', 9)).pack(side=tk.LEFT)
        
        # Buttons
        btn_frame = ttk.Frame(dialog, padding="20")
        btn_frame.pack(fill='x')
        
        ttk.Button(btn_frame, text="📂 Open File", 
                  command=lambda: [os.startfile(file_path), dialog.destroy()],
                  style='Accent.TButton', width=15).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="📂 Open Folder", 
                  command=lambda: [os.startfile(file_dir), dialog.destroy()],
                  width=15).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="OK", 
                  command=dialog.destroy, width=10).pack(side=tk.RIGHT, padx=5)
    
    def log_message(self, message: str, message_type: str = 'info'):
        """Add message to log"""
        timestamp = datetime.now().strftime('%H:%M:%S')
        self.log_text.insert(tk.END, f"[{timestamp}] {message}\n")
        
        # Apply tag to the last line
        start_index = self.log_text.index(f"end-{len(message.split(chr(10)))+1}c")
        end_index = self.log_text.index("end-1c")
        self.log_text.tag_add(message_type, start_index, end_index)
        
        self.log_text.see(tk.END)
    
    def load_settings(self):
        """Load application settings"""
        try:
            if os.path.exists('table_exporter_settings.json'):
                with open('table_exporter_settings.json', 'r') as f:
                    settings = json.load(f)
                
                self.server_var.set(settings.get('server', 'MAHESHWAGH\\WINCC'))
                self.database_var.set(settings.get('database', 'VPI1'))
                self.row_limit_var.set(settings.get('row_limit', '1000'))
                
                logo_path = settings.get('logo_path')
                if logo_path and os.path.exists(logo_path):
                    self.logo_path = logo_path
                    self.logo_label.config(
                        text=os.path.basename(logo_path), 
                        foreground='green'
                    )
                
                self.log_message("✅ Settings loaded", 'info')
                
        except Exception as e:
            self.log_message(f"Note: Could not load settings: {str(e)}", 'warning')
    
    def save_settings(self):
        """Save application settings"""
        settings = {
            'server': self.server_var.get(),
            'database': self.database_var.get(),
            'row_limit': self.row_limit_var.get(),
            'logo_path': self.logo_path,
            'last_save': datetime.now().isoformat()
        }
        
        try:
            with open('table_exporter_settings.json', 'w') as f:
                json.dump(settings, f, indent=2)
            
            self.log_message("💾 Settings saved", 'info')
            
        except Exception as e:
            self.log_message(f"❌ Failed to save settings: {str(e)}", 'error')
    
    def on_closing(self):
        """Handle window closing"""
        if messagebox.askokcancel("Quit", "Do you want to quit the application?"):
            self.save_settings()
            if self.db.connected:
                self.db.disconnect()
            self.root.destroy()

# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

def main():
    """Main function to run the application"""
    # Set DPI awareness on Windows
    if sys.platform == 'win32':
        try:
            from ctypes import windll
            windll.shcore.SetProcessDpiAwareness(1)
        except:
            pass
    
    root = tk.Tk()
    
    # Set window title and icon
    root.title("Multi-Table Excel Exporter Pro")
    
    # Create application
    app = MultiTableExporterApp(root)
    
    # Center window on screen
    root.update_idletasks()
    width = root.winfo_width()
    height = root.winfo_height()
    x = (root.winfo_screenwidth() // 2) - (width // 2)
    y = (root.winfo_screenheight() // 2) - (height // 2)
    root.geometry(f'{width}x{height}+{x}+{y}')
    
    # Handle window close
    root.protocol("WM_DELETE_WINDOW", app.on_closing)
    
    # Start main loop
    root.mainloop()

if __name__ == "__main__":
    main()
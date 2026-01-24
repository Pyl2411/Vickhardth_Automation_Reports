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
from openpyxl.styles import PatternFill, Font, Alignment, Border, Side, NamedStyle
from openpyxl.utils import get_column_letter
from openpyxl.drawing.image import Image as XLImage
from PIL import Image, ImageTk
import io

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_fetcher.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class DatabaseManager:
    """Handles database connections and queries"""
    
    def __init__(self):
        self.connection = None
        self.cursor = None
        self.connected = False
        
    def connect(self, server, database, use_windows_auth=True, username=None, password=None):
        """Connect to SQL Server"""
        try:
            if use_windows_auth:
                conn_str = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;'
            else:
                conn_str = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password};'
            
            self.connection = pyodbc.connect(conn_str)
            self.cursor = self.connection.cursor()
            self.connected = True
            return True, "Connected successfully"
            
        except pyodbc.Error as e:
            return False, f"Connection failed: {str(e)}"
    
    def disconnect(self):
        """Disconnect from database"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        self.connected = False
    
    def get_tables(self):
        """Get list of all tables"""
        try:
            query = """
            SELECT 
                TABLE_SCHEMA,
                TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_SCHEMA, TABLE_NAME
            """
            self.cursor.execute(query)
            tables = self.cursor.fetchall()
            return [f"{row[0]}.{row[1]}" for row in tables]
        except Exception as e:
            return []
    
    def get_table_columns(self, table_name):
        """Get all columns for a table"""
        try:
            # Remove schema if present
            if '.' in table_name:
                schema, table = table_name.split('.')
                query = f"""
                SELECT COLUMN_NAME, DATA_TYPE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                ORDER BY ORDINAL_POSITION
                """
                self.cursor.execute(query, (schema, table))
            else:
                query = f"""
                SELECT COLUMN_NAME, DATA_TYPE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = ?
                ORDER BY ORDINAL_POSITION
                """
                self.cursor.execute(query, (table_name,))
            
            columns = self.cursor.fetchall()
            return [col[0] for col in columns]
        except Exception as e:
            return []
    
    def fetch_data(self, table_name, date_column=None, start_date=None, end_date=None, 
                   selected_columns=None, limit=None):
        """Fetch data with filters"""
        try:
            # Remove schema if present
            if '.' in table_name:
                schema, table = table_name.split('.')
                table_ref = f"[{schema}].[{table}]"
            else:
                table_ref = f"[{table_name}]"
            
            # Build SELECT clause
            if selected_columns and len(selected_columns) > 0:
                columns_str = ", ".join([f"[{col}]" for col in selected_columns])
                select_clause = f"SELECT {columns_str}"
            else:
                select_clause = "SELECT *"
            
            # Build WHERE clause
            where_conditions = []
            params = []
            
            if date_column and start_date:
                where_conditions.append(f"[{date_column}] >= ?")
                params.append(start_date)
            
            if date_column and end_date:
                where_conditions.append(f"[{date_column}] <= ?")
                params.append(f"{end_date} 23:59:59")
            
            where_clause = ""
            if where_conditions:
                where_clause = "WHERE " + " AND ".join(where_conditions)
            
            # Build LIMIT clause
            limit_clause = ""
            if limit and limit > 0:
                limit_clause = f"TOP {limit}"
                select_clause = select_clause.replace("SELECT", f"SELECT {limit_clause}")
            
            # Build full query
            query = f"{select_clause} FROM {table_ref} {where_clause}"
            
            # Execute query
            self.cursor.execute(query, params)
            
            # Get results
            columns = [column[0] for column in self.cursor.description]
            rows = self.cursor.fetchall()
            
            return {
                'success': True,
                'data': rows,
                'columns': columns,
                'row_count': len(rows),
                'query': query
            }
            
        except pyodbc.Error as e:
            return {
                'success': False,
                'error': str(e),
                'data': [],
                'columns': []
            }

class VPIExcelExporter:
    """Handles exporting data to VPI Job Card Excel format"""
    
    @staticmethod
    def create_vpi_excel_template(data, table_name, selected_columns=None, logo_path=None):
        """Create VPI Job Card Excel with proper format"""
        try:
            # Create a new workbook
            wb = Workbook()
            ws = wb.active
            ws.title = "VPI Job Card"
            
            # Define styles
            title_font = Font(name='Arial', size=14, bold=True)
            header_font = Font(name='Arial', size=10, bold=True)
            normal_font = Font(name='Arial', size=10)
            
            # Header section - Based on your template
            ws.merge_cells('A1:Q1')
            ws['A1'] = "LS Orleans"
            ws['A1'].font = title_font
            ws['A1'].alignment = Alignment(horizontal='center')
            
            ws.merge_cells('A2:Q2')
            ws['A2'] = "QUALITE"
            ws['A2'].font = header_font
            ws['A2'].alignment = Alignment(horizontal='center')
            
            ws.merge_cells('A3:Q3')
            ws['A3'] = "VACCUM IMPREGNATION AND PRESSURE MV/HV/VHV CYCLE"
            ws['A3'].font = header_font
            ws['A3'].alignment = Alignment(horizontal='center')
            
            ws.merge_cells('A4:Q4')
            ws['A4'] = "CONTIFCTL/002 095 Bangalore rev J (Suivant Sco0123)"
            ws['A4'].font = normal_font
            ws['A4'].alignment = Alignment(horizontal='center')
            
            # Batch info rows
            ws['A6'] = "BATCH NUMBER"
            ws['H6'] = "PROCESS START TIME"
            
            ws['A7'] = "JOB NO."
            ws['H7'] = "PROCESS STOP TIME"
            
            ws['A8'] = "OPERATOR NAME"
            ws['H8'] = "PROCESS TOTAL TIME"
            
            ws['A9'] = "STATOR NOMINAL VOLTAGE"
            ws['H9'] = "STATOR LENGTH"
            
            ws['A10'] = "JOB 1 SERIAL NO."
            ws['H10'] = "JOB 3 SERIAL NO."
            
            ws['A11'] = "JOB 2 SERIAL NO."
            ws['H11'] = "JOB 4 SERIAL NO."
            
            # Main table headers (starting at row 13)
            headers = [
                "SR NO.", "PROCESS DESCRIPTION", "TIME", "SETPOINT", "UNIT",
                "PROCESS TANK VACUUM / PRESSURE\nmBar", 
                "RESIN TANK VACUUM / PRESSURE\nmBar",
                "JOB NO 1 SERIAL NO CAPACITANCE\nC1 (nF)",
                "JOB NO 2 SERIAL NO CAPACITANCE\nC2 (nF)",
                "JOB NO 3 SERIAL NO CAPACITANCE\nC3 (nF)",
                "JOB NO 4 SERIAL NO CAPACITANCE\nC4 (nF)",
                "RESIN TEMP.\n⁰C", "RESIN LTR",
                "JOB 1 SERIAL NO TEMP.⁰C",
                "JOB 2 SERIAL NO TEMP.⁰C",
                "JOB 3 SERIAL NO TEMP.⁰C",
                "JOB 4 SERIAL NO TEMP.⁰C"
            ]
            
            for col, header in enumerate(headers, start=1):
                cell = ws.cell(row=13, column=col, value=header)
                cell.font = Font(name='Arial', size=9, bold=True)
                cell.alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
                cell.fill = PatternFill(start_color="D9E1F2", end_color="D9E1F2", fill_type="solid")
                cell.border = Border(
                    left=Side(style='thin'),
                    right=Side(style='thin'),
                    top=Side(style='thin'),
                    bottom=Side(style='thin')
                )
            
            # Process steps (based on your template)
            process_steps = [
                (1, "BEFORE PUMPING OF VACUUM"),
                (2, "DRY VACUUM (0.4 mbar to 0.6 mbar)"),
                (3, "DRY VACUUM HOLD                                                               1 HOUR"),
                (None, "2 HOUR"),
                (None, "3 HOUR"),
                (4, "READY TO RESIN TRANSFER"),
                (5, "RESIN LEVEL VISUAL CHECK"),
                (6, "WET VACUUM/EVACUATION"),
                (7, "WET VACUUM/EVACUATION HOLD"),
                (8, "PRESSURISATION PHASE 1 - 3 bars"),
                (9, "BEGIN TIME"),
                (None, "1 HOUR LATER"),
                (None, "2 HOUR LATER"),
                (None, "3 HOUR LATER"),
                (10, "PRESSURISATION PHASE 2 - 6 bars"),
                (11, "BEGIN TIME"),
                (None, "1 HOUR LATER"),
                (None, "1 1/2 HOUR LATER"),
                (None, "2 HOUR LATER"),
                (None, "2 1/2 HOUR LATER"),
                (None, "3 HOUR LATER"),
                (None, "3 1/2 HOUR LATER"),
                (12, "CAPACITANCE CRITERIA"),
                (None, "Criteria 1 - (Capacitance at 120 Hour @ 6bar - Capacitance at 90 Mins @ 6bar )  ≤ 0.2nF"),
                (None, "Criteria 2 - (Final Value/Initial Value) > 2 (MV) & 3.2 TIMES (HV)"),
                (13, "IF ABOVECAPACITANCE CRITERIA NOT ACHIEVED"),
                (None, "PRESSURISATION PHASE 3 1- 4 bars"),
                (None, "1 HOUR LATER"),
                (None, "2 HOUR LATER"),
                (None, "3 HOUR LATER"),
                (None, "4 HOUR LATER"),
                (14, "CAPACITANCE CRITERIA"),
                (None, "Criteria 1 - (Capacitance at 120 Hour @ 6bar - Capacitance at 90 Mins @ 6bar )  ≤ 0.2nF"),
                (None, "Criteria 2 - (Final Value/Initial Value) > 2 (MV) & 3.2 TIMES (HV)"),
                (15, "DE PRESSURISATION"),
                (16, "RESIN RETURN"),
                (17, "PROCESS COMPLETE"),
                (18, "OVEN START TIME")
            ]
            
            # Add process steps
            start_row = 14
            for i, (sr_no, description) in enumerate(process_steps):
                row_num = start_row + i
                if sr_no:
                    ws.cell(row=row_num, column=1, value=sr_no)
                ws.cell(row=row_num, column=2, value=description)
                
                # Add borders to all cells in this row
                for col in range(1, 18):  # Columns A to Q
                    cell = ws.cell(row=row_num, column=col)
                    cell.border = Border(
                        left=Side(style='thin'),
                        right=Side(style='thin'),
                        top=Side(style='thin'),
                        bottom=Side(style='thin')
                    )
            
            # Observations and signature
            obs_row = start_row + len(process_steps) + 1
            ws.merge_cells(f'A{obs_row}:Q{obs_row}')
            ws.cell(row=obs_row, column=1, value="OBSERVATIONS:")
            ws.cell(row=obs_row, column=1).font = Font(bold=True)
            
            sig_row = obs_row + 1
            ws.merge_cells(f'A{sig_row}:C{sig_row}')
            ws.cell(row=sig_row, column=1, value="DATE")
            ws.cell(row=sig_row, column=1).font = Font(bold=True)
            
            ws.merge_cells(f'E{sig_row}:G{sig_row}')
            ws.cell(row=sig_row, column=5, value="NAME")
            ws.cell(row=sig_row, column=5).font = Font(bold=True)
            
            ws.merge_cells(f'N{sig_row}:Q{sig_row}')
            ws.cell(row=sig_row, column=14, value="VISA")
            ws.cell(row=sig_row, column=14).font = Font(bold=True)
            
            # Set column widths
            column_widths = {
                'A': 8, 'B': 50, 'C': 8, 'D': 10, 'E': 8,
                'F': 15, 'G': 15, 'H': 15, 'I': 15, 'J': 15,
                'K': 15, 'L': 10, 'M': 10, 'N': 15, 'O': 15,
                'P': 15, 'Q': 15
            }
            
            for col_letter, width in column_widths.items():
                ws.column_dimensions[col_letter].width = width
            
            # Set row heights
            ws.row_dimensions[13].height = 40  # Header row
            
            # Add logo if provided
            if logo_path and os.path.exists(logo_path):
                try:
                    img = XLImage(logo_path)
                    # Resize logo
                    img.height = 80
                    img.width = 200
                    # Add to top left
                    ws.add_image(img, 'A1')
                    # Shift all rows down
                    ws.insert_rows(1, 4)
                    ws.row_dimensions[1].height = 60
                except Exception as e:
                    print(f"Could not add logo: {e}")
            
            # Create a second sheet for LT VPI
            ws2 = wb.create_sheet(title="LT VPI")
            VPIExcelExporter.create_lt_vpi_sheet(ws2, logo_path)
            
            # Save to bytes
            excel_buffer = io.BytesIO()
            wb.save(excel_buffer)
            excel_buffer.seek(0)
            
            return excel_buffer
            
        except Exception as e:
            raise Exception(f"Excel creation error: {str(e)}")
    
    @staticmethod
    def create_lt_vpi_sheet(ws, logo_path=None):
        """Create LT VPI sheet"""
        # Header section
        ws.merge_cells('A1:Q1')
        ws['A1'] = "LS Orleans"
        ws['A1'].font = Font(name='Arial', size=14, bold=True)
        ws['A1'].alignment = Alignment(horizontal='center')
        
        ws.merge_cells('A2:Q2')
        ws['A2'] = "QUALITE"
        ws['A2'].font = Font(name='Arial', size=10, bold=True)
        ws['A2'].alignment = Alignment(horizontal='center')
        
        ws.merge_cells('A3:Q3')
        ws['A3'] = "VACCUM PRESSURE IMPREGNATION ROUND WIRE LV STATOR"
        ws['A3'].font = Font(name='Arial', size=10, bold=True)
        ws['A3'].alignment = Alignment(horizontal='center')
        
        ws.merge_cells('A4:Q4')
        ws['A4'] = "CONT/FCTL/002 036 rev H"
        ws['A4'].font = Font(name='Arial', size=10)
        ws['A4'].alignment = Alignment(horizontal='center')
        
        # Batch info
        ws['A6'] = "BATCH NAME"
        ws['H6'] = "PROCESS START TIME"
        
        ws['A7'] = "JOB NO."
        ws['H7'] = "PROCESS STOP TIME"
        
        ws['A8'] = "OPERATOR NAME"
        ws['H8'] = "PROCESS TOTAL TIME"
        
        ws['A9'] = "RESIN TYPE"
        ws['H9'] = "STATOR VOLTAGE"
        
        ws['A10'] = "JOB 1 SERIAL NO."
        ws['H10'] = "JOB 3 SERIAL NO."
        
        ws['A11'] = "JOB 2 SERIAL NO."
        ws['H11'] = "JOB 4 SERIAL NO."
        
        # Main table headers
        headers = [
            "SR NO.", "PROCESS DESCRIPTION", "TIME", "SETPOINT", "UNIT",
            "PROCESS TANK VACUUM / PRESSURE\nmBar", 
            "RESIN TANK VACUUM / PRESSURE\nmBar",
            "JOB NO 1 SERIAL NO CAPACITANCE\nC1 (nF)",
            "JOB NO 2 SERIAL NO CAPACITANCE\nC2 (nF)",
            "JOB NO 3 SERIAL NO CAPACITANCE\nC3 (nF)",
            "JOB NO 4 SERIAL NO CAPACITANCE\nC4 (nF)",
            "RESIN TEMP.\n⁰C", "RESIN LTR",
            "JOB 1 SERIAL NO TEMP.⁰C",
            "JOB 2 SERIAL NO TEMP.⁰C",
            "JOB 3 SERIAL NO TEMP.⁰C",
            "JOB 4 SERIAL NO TEMP.⁰C"
        ]
        
        for col, header in enumerate(headers, start=1):
            cell = ws.cell(row=13, column=col, value=header)
            cell.font = Font(name='Arial', size=9, bold=True)
            cell.alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
            cell.fill = PatternFill(start_color="D9E1F2", end_color="D9E1F2", fill_type="solid")
            cell.border = Border(
                left=Side(style='thin'),
                right=Side(style='thin'),
                top=Side(style='thin'),
                bottom=Side(style='thin')
            )
        
        # Process steps for LT VPI
        process_steps = [
            (1, "STATOR PLACED IN TANK"),
            (2, "VACUUM CREATION <5mbar"),
            (3, "VACUUM HOLDING TIME"),
            (4, "READY TO RESIN TRANSFER"),
            (5, "RESIN TRANSFER"),
            (6, "WET VACUUM/EVACUATION"),
            (7, "WET VACUUM/EVACUATION HOLD"),
            (8, "PRESSURISATION"),
            (9, "PRESSURISATION HOLD TIME"),
            (10, "DE PRESSURISATION"),
            (11, "RESIN RETURN"),
            (12, "PROCESS COMPLETE"),
            (13, "DRAINING & CLEANING"),
            (14, "CURING START TIME at 180 degree"),
            (15, "CURING TEMPERATURE REACH 150 DEGREE TIME IN THE SLOT")
        ]
        
        # Add process steps
        start_row = 14
        for i, (sr_no, description) in enumerate(process_steps):
            row_num = start_row + i
            ws.cell(row=row_num, column=1, value=sr_no)
            ws.cell(row=row_num, column=2, value=description)
            
            # Add borders
            for col in range(1, 18):
                cell = ws.cell(row=row_num, column=col)
                cell.border = Border(
                    left=Side(style='thin'),
                    right=Side(style='thin'),
                    top=Side(style='thin'),
                    bottom=Side(style='thin')
                )
        
        # Observations and signature
        obs_row = start_row + len(process_steps) + 1
        ws.merge_cells(f'A{obs_row}:Q{obs_row}')
        ws.cell(row=obs_row, column=1, value="OBSERVATIONS:")
        ws.cell(row=obs_row, column=1).font = Font(bold=True)
        
        sig_row = obs_row + 1
        ws.merge_cells(f'A{sig_row}:C{sig_row}')
        ws.cell(row=sig_row, column=1, value="DATE")
        ws.cell(row=sig_row, column=1).font = Font(bold=True)
        
        ws.merge_cells(f'E{sig_row}:G{sig_row}')
        ws.cell(row=sig_row, column=5, value="NAME")
        ws.cell(row=sig_row, column=5).font = Font(bold=True)
        
        ws.merge_cells(f'K{sig_row}:M{sig_row}')
        ws.cell(row=sig_row, column=11, value="VISA")
        ws.cell(row=sig_row, column=11).font = Font(bold=True)
        
        # Set column widths
        column_widths = {
            'A': 8, 'B': 50, 'C': 8, 'D': 10, 'E': 8,
            'F': 15, 'G': 15, 'H': 15, 'I': 15, 'J': 15,
            'K': 15, 'L': 10, 'M': 10, 'N': 15, 'O': 15,
            'P': 15, 'Q': 15
        }
        
        for col_letter, width in column_widths.items():
            ws.column_dimensions[col_letter].width = width
        
        ws.row_dimensions[13].height = 40
        
        # Add logo if provided
        if logo_path and os.path.exists(logo_path):
            try:
                img = XLImage(logo_path)
                img.height = 80
                img.width = 200
                ws.add_image(img, 'A1')
                ws.insert_rows(1, 4)
                ws.row_dimensions[1].height = 60
            except:
                pass

class DataFetcherApp:
    """Main Application with User-Friendly UI"""
    
    def __init__(self, root):
        self.root = root
        self.root.title("SCADA Data Fetcher - VPI Job Card Generator")
        self.root.geometry("1500x900")
        
        # Database connection
        self.db = DatabaseManager()
        self.exporter = VPIExcelExporter()
        
        # Variables
        self.server_var = tk.StringVar(value="MAHESHWAGH\\WINCC")
        self.database_var = tk.StringVar(value="VPI1")
        self.selected_table_var = tk.StringVar()
        self.selected_date_column_var = tk.StringVar()
        
        # Date variables
        self.start_date_var = tk.StringVar()
        self.end_date_var = tk.StringVar()
        
        # Column selection
        self.available_columns = []
        self.selected_columns = []  # List of selected columns
        self.column_checkboxes = {}  # Store checkbutton variables
        
        # Logo
        self.logo_path = None
        self.logo_image = None
        self.logo_preview_label = None
        
        # Data storage
        self.current_data = None
        
        # Authentication
        self.auth_type = tk.StringVar(value="windows")
        self.username_var = tk.StringVar()
        self.password_var = tk.StringVar()
        
        # Setup UI
        self.setup_ui()
        
        # Set default dates
        self.set_default_dates()
    
    def setup_ui(self):
        """Setup the user interface"""
        # Create a style for better looking widgets
        style = ttk.Style()
        style.theme_use('clam')
        
        style.configure('Title.TLabel', font=('Segoe UI', 16, 'bold'))
        style.configure('Header.TLabel', font=('Segoe UI', 11, 'bold'))
        style.configure('Accent.TButton', font=('Segoe UI', 10, 'bold'), padding=10)
        
        # Main container with notebook for tabs
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(fill='both', expand=True, padx=5, pady=5)
        
        # Create tabs
        self.setup_connection_tab()
        self.setup_data_tab()
        self.setup_export_tab()
        
        # Status bar
        self.status_bar = ttk.Label(self.root, text="Ready", relief=tk.SUNKEN, anchor=tk.W)
        self.status_bar.pack(side=tk.BOTTOM, fill=tk.X)
    
    def setup_connection_tab(self):
        """Setup Connection Tab"""
        conn_tab = ttk.Frame(self.notebook)
        self.notebook.add(conn_tab, text="🔌 Connection")
        
        # Main frame with padding
        main_frame = ttk.Frame(conn_tab, padding="20")
        main_frame.pack(fill='both', expand=True)
        
        # Title
        ttk.Label(main_frame, text="Database Connection", style='Title.TLabel').grid(
            row=0, column=0, columnspan=3, pady=(0, 20))
        
        # Connection settings frame
        settings_frame = ttk.LabelFrame(main_frame, text="Connection Settings", padding="15")
        settings_frame.grid(row=1, column=0, columnspan=3, sticky='ew', pady=(0, 20))
        settings_frame.columnconfigure(1, weight=1)
        
        # Server
        ttk.Label(settings_frame, text="Server:", font=('Segoe UI', 10, 'bold')).grid(
            row=0, column=0, sticky=tk.W, pady=8)
        server_entry = ttk.Entry(settings_frame, textvariable=self.server_var, width=40)
        server_entry.grid(row=0, column=1, pady=8, padx=(10, 20), sticky='ew')
        
        # Database
        ttk.Label(settings_frame, text="Database:", font=('Segoe UI', 10, 'bold')).grid(
            row=0, column=2, sticky=tk.W, pady=8)
        db_entry = ttk.Entry(settings_frame, textvariable=self.database_var, width=20)
        db_entry.grid(row=0, column=3, pady=8, sticky='ew')
        
        # Authentication type
        auth_frame = ttk.Frame(settings_frame)
        auth_frame.grid(row=1, column=0, columnspan=4, pady=10, sticky='w')
        
        ttk.Label(auth_frame, text="Authentication:", font=('Segoe UI', 10, 'bold')).pack(side=tk.LEFT)
        ttk.Radiobutton(auth_frame, text="Windows", variable=self.auth_type, 
                       value="windows", command=self.toggle_auth_fields).pack(side=tk.LEFT, padx=(10, 5))
        ttk.Radiobutton(auth_frame, text="SQL Server", variable=self.auth_type, 
                       value="sql", command=self.toggle_auth_fields).pack(side=tk.LEFT, padx=5)
        
        # SQL Auth fields (initially hidden)
        self.sql_auth_frame = ttk.Frame(settings_frame)
        self.sql_auth_frame.grid(row=2, column=0, columnspan=4, pady=10, sticky='w')
        
        ttk.Label(self.sql_auth_frame, text="Username:").grid(row=0, column=0, padx=(0, 5))
        ttk.Entry(self.sql_auth_frame, textvariable=self.username_var, width=20).grid(row=0, column=1, padx=(0, 20))
        
        ttk.Label(self.sql_auth_frame, text="Password:").grid(row=0, column=2, padx=(0, 5))
        ttk.Entry(self.sql_auth_frame, textvariable=self.password_var, width=20, show="*").grid(row=0, column=3)
        
        # Connection buttons frame
        btn_frame = ttk.Frame(main_frame)
        btn_frame.grid(row=2, column=0, columnspan=3, pady=20)
        
        self.connect_btn = ttk.Button(btn_frame, text="✅ Connect to Database", 
                                     command=self.connect_db, width=20, style='Accent.TButton')
        self.connect_btn.pack(side=tk.LEFT, padx=10)
        
        self.disconnect_btn = ttk.Button(btn_frame, text="❌ Disconnect", 
                                        command=self.disconnect_db, width=15, state='disabled')
        self.disconnect_btn.pack(side=tk.LEFT, padx=10)
        
        self.test_btn = ttk.Button(btn_frame, text="🔍 Test Connection", 
                                  command=self.test_connection, width=15)
        self.test_btn.pack(side=tk.LEFT, padx=10)
        
        # Status frame
        status_frame = ttk.LabelFrame(main_frame, text="Connection Status", padding="10")
        status_frame.grid(row=3, column=0, columnspan=3, sticky='ew', pady=(0, 10))
        
        self.status_label = ttk.Label(status_frame, text="Not connected", foreground="red")
        self.status_label.pack(anchor='w')
        
        # Available tables frame
        tables_frame = ttk.LabelFrame(main_frame, text="Available Tables", padding="10")
        tables_frame.grid(row=4, column=0, columnspan=3, sticky='nsew', pady=(0, 10))
        main_frame.rowconfigure(4, weight=1)
        main_frame.columnconfigure(0, weight=1)
        
        # Table list with scrollbar
        table_container = ttk.Frame(tables_frame)
        table_container.pack(fill='both', expand=True)
        
        self.table_listbox = tk.Listbox(table_container, height=15, selectmode='single')
        self.table_listbox.pack(side=tk.LEFT, fill='both', expand=True)
        
        scrollbar = ttk.Scrollbar(table_container, command=self.table_listbox.yview)
        scrollbar.pack(side=tk.RIGHT, fill='y')
        self.table_listbox.config(yscrollcommand=scrollbar.set)
        
        # Table buttons
        table_btn_frame = ttk.Frame(tables_frame)
        table_btn_frame.pack(fill='x', pady=(10, 0))
        
        ttk.Button(table_btn_frame, text="🔄 Refresh Tables", 
                  command=self.refresh_tables).pack(side=tk.LEFT, padx=5)
        ttk.Button(table_btn_frame, text="📋 Select Table", 
                  command=self.select_table_from_list).pack(side=tk.LEFT, padx=5)
        
        # Initially hide SQL auth fields
        self.toggle_auth_fields()
    
    def setup_data_tab(self):
        """Setup Data Fetching Tab"""
        data_tab = ttk.Frame(self.notebook)
        self.notebook.add(data_tab, text="📊 Data Fetching")
        
        # Main frame with padding
        main_frame = ttk.Frame(data_tab, padding="15")
        main_frame.pack(fill='both', expand=True)
        
        # Title
        ttk.Label(main_frame, text="Data Fetching Options", style='Title.TLabel').grid(
            row=0, column=0, columnspan=3, pady=(0, 15))
        
        # Table selection frame
        table_frame = ttk.LabelFrame(main_frame, text="Table Selection", padding="10")
        table_frame.grid(row=1, column=0, columnspan=3, sticky='ew', pady=(0, 15))
        
        ttk.Label(table_frame, text="Selected Table:", font=('Segoe UI', 10, 'bold')).grid(
            row=0, column=0, sticky=tk.W, pady=5)
        
        self.selected_table_label = ttk.Label(table_frame, text="No table selected", foreground="blue")
        self.selected_table_label.grid(row=0, column=1, sticky=tk.W, pady=5, padx=(10, 0))
        
        # Date filter frame
        date_frame = ttk.LabelFrame(main_frame, text="Date Filter (Optional)", padding="10")
        date_frame.grid(row=2, column=0, columnspan=3, sticky='ew', pady=(0, 15))
        
        # Date column selection
        ttk.Label(date_frame, text="Date Column:").grid(row=0, column=0, sticky=tk.W, pady=5)
        self.date_column_combo = ttk.Combobox(date_frame, textvariable=self.selected_date_column_var, 
                                             state='readonly', width=25)
        self.date_column_combo.grid(row=0, column=1, pady=5, padx=(5, 20))
        
        # Date range
        ttk.Label(date_frame, text="Start Date:").grid(row=0, column=2, sticky=tk.W, pady=5)
        self.start_date_entry = DateEntry(date_frame, width=12, background='darkblue',
                                         foreground='white', borderwidth=2,
                                         date_pattern='yyyy-mm-dd')
        self.start_date_entry.grid(row=0, column=3, pady=5, padx=(5, 20))
        
        ttk.Label(date_frame, text="End Date:").grid(row=0, column=4, sticky=tk.W, pady=5)
        self.end_date_entry = DateEntry(date_frame, width=12, background='darkblue',
                                       foreground='white', borderwidth=2,
                                       date_pattern='yyyy-mm-dd')
        self.end_date_entry.grid(row=0, column=5, pady=5)
        
        # Column selection frame
        col_frame = ttk.LabelFrame(main_frame, text="Column Selection", padding="10")
        col_frame.grid(row=3, column=0, columnspan=2, sticky='nsew', pady=(0, 15), padx=(0, 10))
        main_frame.rowconfigure(3, weight=1)
        main_frame.columnconfigure(0, weight=1)
        
        # Column selection header with buttons
        col_header = ttk.Frame(col_frame)
        col_header.pack(fill='x', pady=(0, 10))
        
        ttk.Label(col_header, text="Select columns to fetch:", font=('Segoe UI', 10, 'bold')).pack(side=tk.LEFT)
        
        btn_frame = ttk.Frame(col_header)
        btn_frame.pack(side=tk.RIGHT)
        
        ttk.Button(btn_frame, text="Select All", 
                  command=self.select_all_columns, width=10).pack(side=tk.LEFT, padx=2)
        ttk.Button(btn_frame, text="Clear All", 
                  command=self.clear_all_columns, width=10).pack(side=tk.LEFT, padx=2)
        
        # Scrollable frame for checkboxes
        col_canvas = tk.Canvas(col_frame, highlightthickness=0)
        col_scrollbar = ttk.Scrollbar(col_frame, orient="vertical", command=col_canvas.yview)
        self.col_checkbox_frame = ttk.Frame(col_canvas)
        
        col_canvas.pack(side=tk.LEFT, fill='both', expand=True)
        col_scrollbar.pack(side=tk.RIGHT, fill='y')
        
        col_canvas.configure(yscrollcommand=col_scrollbar.set)
        col_canvas.create_window((0, 0), window=self.col_checkbox_frame, anchor="nw")
        
        # Configure scrolling
        self.col_checkbox_frame.bind("<Configure>", 
            lambda e: col_canvas.configure(scrollregion=col_canvas.bbox("all")))
        
        # Fetch options frame
        fetch_frame = ttk.LabelFrame(main_frame, text="Fetch Options", padding="10")
        fetch_frame.grid(row=3, column=2, sticky='nsew', pady=(0, 15))
        
        # Row limit
        ttk.Label(fetch_frame, text="Row Limit:", font=('Segoe UI', 10, 'bold')).grid(
            row=0, column=0, sticky=tk.W, pady=10)
        
        self.fetch_limit_var = tk.StringVar(value="1000")
        limit_frame = ttk.Frame(fetch_frame)
        limit_frame.grid(row=0, column=1, pady=10, sticky='w')
        
        ttk.Entry(limit_frame, textvariable=self.fetch_limit_var, width=10).pack(side=tk.LEFT)
        ttk.Label(limit_frame, text=" rows (0 = all)").pack(side=tk.LEFT, padx=(5, 0))
        
        # Fetch buttons
        ttk.Label(fetch_frame, text="Fetch Actions:", font=('Segoe UI', 10, 'bold')).grid(
            row=1, column=0, sticky=tk.W, pady=(20, 10))
        
        self.fetch_btn = ttk.Button(fetch_frame, text="🚀 FETCH DATA NOW", 
                                   command=self.fetch_data, width=25, style='Accent.TButton',
                                   state='disabled')
        self.fetch_btn.grid(row=2, column=0, columnspan=2, pady=10)
        
        self.fetch_all_btn = ttk.Button(fetch_frame, text="📊 Fetch All Data", 
                                       command=self.fetch_all_data, width=25, state='disabled')
        self.fetch_all_btn.grid(row=3, column=0, columnspan=2, pady=5)
        
        # Progress display
        self.progress_label = ttk.Label(fetch_frame, text="Select a table and columns first")
        self.progress_label.grid(row=4, column=0, columnspan=2, pady=(20, 0))
        
        # Data preview frame
        preview_frame = ttk.LabelFrame(main_frame, text="Data Preview", padding="10")
        preview_frame.grid(row=4, column=0, columnspan=3, sticky='nsew', pady=(0, 10))
        main_frame.rowconfigure(4, weight=1)
        
        # Treeview for data display
        tree_frame = ttk.Frame(preview_frame)
        tree_frame.pack(fill='both', expand=True)
        
        self.data_tree = ttk.Treeview(tree_frame, show='headings')
        self.data_tree.pack(side=tk.LEFT, fill='both', expand=True)
        
        # Scrollbars
        v_scrollbar = ttk.Scrollbar(tree_frame, orient=tk.VERTICAL, command=self.data_tree.yview)
        v_scrollbar.pack(side=tk.RIGHT, fill='y')
        self.data_tree.configure(yscrollcommand=v_scrollbar.set)
        
        h_scrollbar = ttk.Scrollbar(preview_frame, orient=tk.HORIZONTAL, command=self.data_tree.xview)
        h_scrollbar.pack(side=tk.BOTTOM, fill='x')
        self.data_tree.configure(xscrollcommand=h_scrollbar.set)
    
    def setup_export_tab(self):
        """Setup Export Tab"""
        export_tab = ttk.Frame(self.notebook)
        self.notebook.add(export_tab, text="💾 Export to VPI Job Card")
        
        # Main frame with padding
        main_frame = ttk.Frame(export_tab, padding="20")
        main_frame.pack(fill='both', expand=True)
        
        # Title
        ttk.Label(main_frame, text="VPI Job Card Export", style='Title.TLabel').pack(pady=(0, 20))
        
        # Export options frame
        options_frame = ttk.LabelFrame(main_frame, text="Export Settings", padding="20")
        options_frame.pack(fill='both', expand=True, pady=(0, 20))
        
        # Logo upload section
        logo_frame = ttk.Frame(options_frame)
        logo_frame.pack(fill='x', pady=(0, 20))
        
        ttk.Label(logo_frame, text="Company Logo:", font=('Segoe UI', 10, 'bold')).pack(anchor='w', pady=(0, 5))
        
        logo_btn_frame = ttk.Frame(logo_frame)
        logo_btn_frame.pack(fill='x', pady=(0, 10))
        
        ttk.Button(logo_btn_frame, text="🖼️ Upload Logo", 
                  command=self.upload_logo, width=15).pack(side=tk.LEFT, padx=(0, 10))
        
        self.logo_status_label = ttk.Label(logo_btn_frame, text="No logo selected", foreground="gray")
        self.logo_status_label.pack(side=tk.LEFT)
        
        # Logo preview
        self.logo_preview_frame = ttk.LabelFrame(logo_frame, text="Logo Preview", padding="10")
        self.logo_preview_frame.pack(fill='x', pady=(0, 10))
        
        self.logo_preview_label = ttk.Label(self.logo_preview_frame, text="No logo")
        self.logo_preview_label.pack()
        
        # Quick export section
        quick_frame = ttk.Frame(options_frame)
        quick_frame.pack(fill='x', pady=(0, 20))
        
        ttk.Label(quick_frame, text="VPI Job Card Export:", font=('Segoe UI', 12, 'bold')).pack(anchor='w', pady=(0, 10))
        ttk.Label(quick_frame, text="Export data to VPI Job Card Excel format with logo").pack(anchor='w')
        
        self.export_btn = ttk.Button(quick_frame, text="📥 Export to VPI Job Card", 
                                    command=self.export_to_vpi_format, width=30, style='Accent.TButton',
                                    state='disabled')
        self.export_btn.pack(pady=10)
        
        # Template preview note
        note_frame = ttk.Frame(options_frame)
        note_frame.pack(fill='x', pady=(0, 20))
        
        ttk.Label(note_frame, text="⚠️ Template Format:", font=('Segoe UI', 10, 'bold')).pack(anchor='w')
        ttk.Label(note_frame, text="Exported file will contain:").pack(anchor='w')
        ttk.Label(note_frame, text="• HT VPI Sheet (High Tension Vacuum Pressure Impregnation)").pack(anchor='w')
        ttk.Label(note_frame, text="• LT VPI Sheet (Low Tension Vacuum Pressure Impregnation)").pack(anchor='w')
        ttk.Label(note_frame, text="• Your company logo at the top").pack(anchor='w')
        ttk.Label(note_frame, text="• Professional formatting with borders and colors").pack(anchor='w')
        
        # Selected columns display
        col_display_frame = ttk.LabelFrame(options_frame, text="Selected Columns for Export", padding="10")
        col_display_frame.pack(fill='both', expand=True, pady=(0, 20))
        
        self.selected_col_display = scrolledtext.ScrolledText(col_display_frame, height=10, wrap=tk.WORD)
        self.selected_col_display.pack(fill='both', expand=True)
        
        # Log frame
        log_frame = ttk.LabelFrame(main_frame, text="Export Log", padding="10")
        log_frame.pack(fill='both', expand=True)
        
        self.log_text = scrolledtext.ScrolledText(log_frame, height=8, wrap=tk.WORD)
        self.log_text.pack(fill='both', expand=True)
        
        # Configure log tags
        self.log_text.tag_configure('success', foreground='green')
        self.log_text.tag_configure('error', foreground='red')
        self.log_text.tag_configure('info', foreground='blue')
        self.log_text.tag_configure('warning', foreground='orange')
    
    def toggle_auth_fields(self):
        """Toggle SQL authentication fields"""
        if self.auth_type.get() == "windows":
            self.sql_auth_frame.grid_remove()
        else:
            self.sql_auth_frame.grid()
    
    def set_default_dates(self):
        """Set default date values"""
        today = datetime.now()
        week_ago = today - timedelta(days=7)
        
        self.start_date_entry.set_date(week_ago)
        self.end_date_entry.set_date(today)
    
    def upload_logo(self):
        """Upload company logo"""
        filetypes = [("Image files", "*.png *.jpg *.jpeg *.bmp"), ("All files", "*.*")]
        filename = filedialog.askopenfilename(title="Select Company Logo", filetypes=filetypes)
        
        if filename:
            try:
                self.logo_path = filename
                # Load and resize image for preview
                img = Image.open(filename)
                img.thumbnail((150, 150), Image.Resampling.LANCZOS)
                photo = ImageTk.PhotoImage(img)
                
                self.logo_preview_label.config(image=photo, text="")
                self.logo_preview_label.image = photo  # Keep reference
                self.logo_status_label.config(text=os.path.basename(filename))
                
                self.log_message(f"✅ Logo uploaded: {os.path.basename(filename)}", 'success')
            except Exception as e:
                self.log_message(f"❌ Failed to load logo: {str(e)}", 'error')
    
    def log_message(self, message, message_type='info'):
        """Add message to log"""
        timestamp = datetime.now().strftime('%H:%M:%S')
        self.log_text.insert(tk.END, f"[{timestamp}] {message}\n")
        self.log_text.tag_add(message_type, "end-2l", "end-1l")
        self.log_text.see(tk.END)
    
    def test_connection(self):
        """Test database connection"""
        def test():
            self.status_bar.config(text="Testing connection...")
            self.status_label.config(text="Testing...", foreground="orange")
            
            if self.auth_type.get() == "windows":
                success, message = self.db.connect(
                    server=self.server_var.get(),
                    database=self.database_var.get(),
                    use_windows_auth=True
                )
            else:
                success, message = self.db.connect(
                    server=self.server_var.get(),
                    database=self.database_var.get(),
                    use_windows_auth=False,
                    username=self.username_var.get(),
                    password=self.password_var.get()
                )
            
            if success:
                self.status_label.config(text="Test: Connection OK", foreground="green")
                self.status_bar.config(text="Connection test successful")
                self.db.disconnect()  # Close test connection
                messagebox.showinfo("Connection Test", "✅ Connection successful!")
            else:
                self.status_label.config(text="Test: Failed", foreground="red")
                self.status_bar.config(text=f"Connection test failed: {message}")
                messagebox.showerror("Connection Test", f"❌ Connection failed:\n{message}")
        
        threading.Thread(target=test, daemon=True).start()
    
    def connect_db(self):
        """Connect to database"""
        def connect():
            self.status_bar.config(text="Connecting...")
            self.status_label.config(text="Connecting...", foreground="orange")
            self.connect_btn.config(state='disabled')
            
            if self.auth_type.get() == "windows":
                success, message = self.db.connect(
                    server=self.server_var.get(),
                    database=self.database_var.get(),
                    use_windows_auth=True
                )
            else:
                success, message = self.db.connect(
                    server=self.server_var.get(),
                    database=self.database_var.get(),
                    use_windows_auth=False,
                    username=self.username_var.get(),
                    password=self.password_var.get()
                )
            
            if success:
                self.status_label.config(text="Connected", foreground="green")
                self.status_bar.config(text="Connected successfully")
                self.connect_btn.config(state='disabled')
                self.disconnect_btn.config(state='normal')
                self.log_message("Database connected successfully", 'success')
                self.refresh_tables()
            else:
                self.status_label.config(text="Connection Failed", foreground="red")
                self.status_bar.config(text=f"Connection failed: {message}")
                self.connect_btn.config(state='normal')
                self.log_message(f"Connection failed: {message}", 'error')
                messagebox.showerror("Connection Error", f"Failed to connect:\n{message}")
        
        threading.Thread(target=connect, daemon=True).start()
    
    def disconnect_db(self):
        """Disconnect from database"""
        self.db.disconnect()
        self.status_label.config(text="Disconnected", foreground="red")
        self.status_bar.config(text="Disconnected")
        self.connect_btn.config(state='normal')
        self.disconnect_btn.config(state='disabled')
        self.fetch_btn.config(state='disabled')
        self.fetch_all_btn.config(state='disabled')
        self.export_btn.config(state='disabled')
        self.log_message("Disconnected from database", 'info')
        
        # Clear UI
        self.table_listbox.delete(0, tk.END)
        self.selected_table_label.config(text="No table selected")
        self.date_column_combo.set('')
        self.date_column_combo['values'] = []
        self.clear_column_checkboxes()
        self.clear_data_tree()
        self.selected_col_display.delete(1.0, tk.END)
    
    def refresh_tables(self):
        """Refresh list of tables"""
        if not self.db.connected:
            self.log_message("Not connected to database", 'error')
            return
        
        def refresh():
            self.status_bar.config(text="Loading tables...")
            
            tables = self.db.get_tables()
            self.table_listbox.delete(0, tk.END)
            
            for table in tables:
                self.table_listbox.insert(tk.END, table)
            
            self.status_bar.config(text=f"Loaded {len(tables)} tables")
            self.log_message(f"Loaded {len(tables)} tables", 'success')
            
            if tables:
                # Auto-select first table
                self.table_listbox.selection_set(0)
                self.select_table_from_list()
        
        threading.Thread(target=refresh, daemon=True).start()
    
    def select_table_from_list(self):
        """Select table from listbox"""
        selection = self.table_listbox.curselection()
        if not selection:
            messagebox.showwarning("Selection", "Please select a table from the list")
            return
        
        idx = selection[0]
        table_name = self.table_listbox.get(idx)
        self.selected_table_var.set(table_name)
        self.selected_table_label.config(text=table_name)
        
        # Load columns for this table
        self.load_table_columns(table_name)
    
    def load_table_columns(self, table_name):
        """Load columns for selected table"""
        def load():
            self.status_bar.config(text=f"Loading columns for {table_name}...")
            
            columns = self.db.get_table_columns(table_name)
            self.available_columns = columns
            
            # Update date column combo
            self.date_column_combo['values'] = columns
            
            # Try to auto-select a date column
            date_columns = [col for col in columns 
                          if any(x in col.lower() for x in ['date', 'time', 'timestamp'])]
            if date_columns:
                self.selected_date_column_var.set(date_columns[0])
            elif columns:
                self.selected_date_column_var.set(columns[0])
            
            # Create column checkboxes
            self.create_column_checkboxes(columns)
            
            # Enable fetch buttons
            self.fetch_btn.config(state='normal')
            self.fetch_all_btn.config(state='normal')
            
            self.status_bar.config(text=f"Loaded {len(columns)} columns for {table_name}")
            self.log_message(f"Loaded {len(columns)} columns from {table_name}", 'success')
        
        threading.Thread(target=load, daemon=True).start()
    
    def create_column_checkboxes(self, columns):
        """Create checkboxes for column selection"""
        # Clear existing checkboxes
        self.clear_column_checkboxes()
        self.column_checkboxes.clear()
        self.selected_columns = []
        
        # Create new checkboxes
        for i, column in enumerate(columns):
            var = tk.BooleanVar(value=True)  # Default to selected
            self.column_checkboxes[column] = var
            
            cb = ttk.Checkbutton(self.col_checkbox_frame, text=column, variable=var,
                                command=self.update_selected_columns)
            cb.grid(row=i, column=0, sticky=tk.W, padx=5, pady=2)
        
        # Update selected columns display
        self.update_selected_columns()
    
    def clear_column_checkboxes(self):
        """Clear all column checkboxes"""
        for widget in self.col_checkbox_frame.winfo_children():
            widget.destroy()
    
    def update_selected_columns(self):
        """Update the selected columns list and display"""
        self.selected_columns = [col for col, var in self.column_checkboxes.items() if var.get()]
        display_text = "\n".join(self.selected_columns) if self.selected_columns else "No columns selected"
        self.selected_col_display.delete(1.0, tk.END)
        self.selected_col_display.insert(1.0, display_text)
    
    def select_all_columns(self):
        """Select all columns"""
        for var in self.column_checkboxes.values():
            var.set(True)
        self.update_selected_columns()
        self.log_message("Selected all columns", 'info')
    
    def clear_all_columns(self):
        """Clear all column selections"""
        for var in self.column_checkboxes.values():
            var.set(False)
        self.update_selected_columns()
        self.log_message("Cleared all column selections", 'info')
    
    def fetch_data(self):
        """Fetch data with current settings"""
        if not self.db.connected:
            self.log_message("Not connected to database", 'error')
            return
        
        table_name = self.selected_table_var.get()
        if not table_name:
            self.log_message("Please select a table first", 'error')
            return
        
        if not self.selected_columns:
            self.log_message("Please select at least one column", 'warning')
            return
        
        def fetch():
            self.status_bar.config(text="Fetching data...")
            self.progress_label.config(text="Fetching data...")
            self.fetch_btn.config(state='disabled')
            
            # Get parameters
            date_column = self.selected_date_column_var.get()
            start_date = self.start_date_entry.get_date().strftime('%Y-%m-%d')
            end_date = self.end_date_entry.get_date().strftime('%Y-%m-%d')
            
            # Get limit
            try:
                limit = int(self.fetch_limit_var.get())
            except:
                limit = 1000
            
            # Fetch data
            result = self.db.fetch_data(
                table_name=table_name,
                date_column=date_column if date_column else None,
                start_date=start_date if date_column else None,
                end_date=end_date if date_column else None,
                selected_columns=self.selected_columns,
                limit=limit
            )
            
            if result['success']:
                self.current_data = result
                self.display_data(result)
                
                # Update UI
                row_count = result['row_count']
                self.progress_label.config(text=f"✅ Fetched {row_count} rows successfully")
                self.status_bar.config(text=f"Fetched {row_count} rows")
                
                # Enable export button
                self.export_btn.config(state='normal')
                
                filter_info = ""
                if date_column:
                    filter_info = f" with date filter ({start_date} to {end_date})"
                
                self.log_message(f"✅ Fetched {row_count} rows{filter_info}", 'success')
            else:
                self.progress_label.config(text="❌ Fetch failed")
                self.status_bar.config(text=f"Error: {result['error']}")
                self.log_message(f"❌ Fetch failed: {result['error']}", 'error')
                messagebox.showerror("Fetch Error", f"Failed to fetch data:\n{result['error']}")
            
            self.fetch_btn.config(state='normal')
        
        threading.Thread(target=fetch, daemon=True).start()
    
    def fetch_all_data(self):
        """Fetch all data without limit"""
        if not self.db.connected:
            self.log_message("Not connected to database", 'error')
            return
        
        table_name = self.selected_table_var.get()
        if not table_name:
            self.log_message("Please select a table first", 'error')
            return
        
        if not self.selected_columns:
            self.log_message("Please select at least one column", 'warning')
            return
        
        # Set limit to 0 for "all data"
        self.fetch_limit_var.set("0")
        self.fetch_data()
    
    def display_data(self, data):
        """Display data in treeview"""
        # Clear existing data
        for item in self.data_tree.get_children():
            self.data_tree.delete(item)
        
        # Clear existing columns
        self.data_tree['columns'] = []
        
        # Setup new columns
        columns = data['columns']
        self.data_tree['columns'] = columns
        
        for col in columns:
            self.data_tree.heading(col, text=col, anchor=tk.W)
            self.data_tree.column(col, width=100, minwidth=50, anchor=tk.W)
        
        # Insert data (limit to 500 rows for performance)
        display_rows = min(500, len(data['data']))
        for row in data['data'][:display_rows]:
            values = [str(val)[:100] if val is not None else "" for val in row]  # Truncate long values
            self.data_tree.insert('', 'end', values=values)
        
        # Auto-size columns
        self.auto_size_columns()
        
        if len(data['data']) > display_rows:
            self.log_message(f"Displaying first {display_rows} of {len(data['data'])} rows", 'info')
    
    def auto_size_columns(self):
        """Auto-size treeview columns"""
        for col in self.data_tree['columns']:
            max_len = len(col)
            for item in self.data_tree.get_children():
                value = self.data_tree.set(item, col)
                if value and len(value) > max_len:
                    max_len = len(value)
            # Set width with some padding
            self.data_tree.column(col, width=min(max_len * 8, 300))
    
    def clear_data_tree(self):
        """Clear the data treeview"""
        for item in self.data_tree.get_children():
            self.data_tree.delete(item)
    
    def export_to_vpi_format(self):
        """Export data to VPI Job Card Excel format"""
        if not self.current_data or not self.current_data['data']:
            self.log_message("No data to export", 'error')
            return
        
        table_name = self.selected_table_var.get()
        if not table_name:
            table_name = "data"
        
        # Generate filename
        file_name = table_name.split('.')[-1] if '.' in table_name else table_name
        date_str = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        file_path = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")],
            initialfile=f"VPI_JobCard_{file_name}_{date_str}.xlsx"
        )
        
        if not file_path:
            return
        
        def export():
            self.status_bar.config(text="Creating VPI Job Card...")
            self.log_message("Creating VPI Job Card Excel...", 'info')
            
            try:
                # Create VPI Excel template
                excel_buffer = self.exporter.create_vpi_excel_template(
                    data=self.current_data,
                    table_name=table_name,
                    selected_columns=self.selected_columns,
                    logo_path=self.logo_path
                )
                
                # Save to file
                with open(file_path, 'wb') as f:
                    f.write(excel_buffer.getvalue())
                
                self.status_bar.config(text=f"VPI Job Card created: {os.path.basename(file_path)}")
                self.log_message(f"✅ VPI Job Card created successfully!", 'success')
                self.log_message(f"📁 File: {file_path}", 'info')
                self.log_message(f"📊 Sheets: HT VPI, LT VPI", 'info')
                
                # Ask to open file
                if messagebox.askyesno("Export Successful", 
                                      f"✅ VPI Job Card created successfully!\n\n"
                                      f"File: {os.path.basename(file_path)}\n"
                                      f"Location: {os.path.dirname(file_path)}\n\n"
                                      f"Open file now?"):
                    os.startfile(file_path)
                    
            except Exception as e:
                self.status_bar.config(text=f"Export error: {str(e)}")
                self.log_message(f"❌ Export error: {str(e)}", 'error')
                messagebox.showerror("Export Error", f"Failed to create VPI Job Card:\n{str(e)}")
        
        threading.Thread(target=export, daemon=True).start()

def main():
    """Main entry point"""
    try:
        # Check dependencies
        import pyodbc
        import pandas
        from openpyxl import Workbook
        from PIL import Image
        
        # Create main window
        root = tk.Tk()
        
        # Set window icon and position
        root.title("SCADA Data Fetcher - VPI Job Card Generator")
        root.geometry("1500x900")
        
        # Center window on screen
        root.update_idletasks()
        width = root.winfo_width()
        height = root.winfo_height()
        x = (root.winfo_screenwidth() // 2) - (width // 2)
        y = (root.winfo_screenheight() // 2) - (height // 2)
        root.geometry(f'{width}x{height}+{x}+{y}')
        
        app = DataFetcherApp(root)
        
        # Start application
        root.mainloop()
        
    except ImportError as e:
        print(f"Missing dependency: {e}")
        print("\nInstall required packages:")
        print("pip install pyodbc pandas openpyxl tkcalendar pillow")
        
        install = input("\nInstall now? (y/n): ")
        if install.lower() == 'y':
            import subprocess
            subprocess.check_call([sys.executable, "-m", "pip", "install",
                                 "pyodbc", "pandas", "openpyxl", "tkcalendar", "pillow"])
            print("\nPackages installed successfully!")
            print("Please restart the application.")
        else:
            print("Please install packages manually.")

if __name__ == "__main__":
    main()
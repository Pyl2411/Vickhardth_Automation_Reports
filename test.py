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
from openpyxl import Workbook
from openpyxl.styles import PatternFill, Font, Alignment, Border, Side
from openpyxl.utils import get_column_letter

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

class DataFetcherApp:
    """Main Application with User-Friendly UI"""
    
    def __init__(self, root):
        self.root = root
        self.root.title("SQL Data Fetcher - VPI1 Database")
        self.root.geometry("1400x850")
        
        # Database connection
        self.db = DatabaseManager()
        
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
        style.configure('Title.TLabel', font=('Arial', 14, 'bold'))
        style.configure('Header.TLabel', font=('Arial', 10, 'bold'))
        style.configure('Accent.TButton', font=('Arial', 10, 'bold'))
        
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
        ttk.Label(settings_frame, text="Server:", font=('Arial', 10, 'bold')).grid(
            row=0, column=0, sticky=tk.W, pady=8)
        server_entry = ttk.Entry(settings_frame, textvariable=self.server_var, width=40)
        server_entry.grid(row=0, column=1, pady=8, padx=(10, 20), sticky='ew')
        
        # Database
        ttk.Label(settings_frame, text="Database:", font=('Arial', 10, 'bold')).grid(
            row=0, column=2, sticky=tk.W, pady=8)
        db_entry = ttk.Entry(settings_frame, textvariable=self.database_var, width=20)
        db_entry.grid(row=0, column=3, pady=8, sticky='ew')
        
        # Authentication type
        auth_frame = ttk.Frame(settings_frame)
        auth_frame.grid(row=1, column=0, columnspan=4, pady=10, sticky='w')
        
        ttk.Label(auth_frame, text="Authentication:", font=('Arial', 10, 'bold')).pack(side=tk.LEFT)
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
        
        ttk.Label(table_frame, text="Selected Table:", font=('Arial', 10, 'bold')).grid(
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
        
        ttk.Label(col_header, text="Select columns to fetch:", font=('Arial', 10, 'bold')).pack(side=tk.LEFT)
        
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
        ttk.Label(fetch_frame, text="Row Limit:", font=('Arial', 10, 'bold')).grid(
            row=0, column=0, sticky=tk.W, pady=10)
        
        self.fetch_limit_var = tk.StringVar(value="1000")
        limit_frame = ttk.Frame(fetch_frame)
        limit_frame.grid(row=0, column=1, pady=10, sticky='w')
        
        ttk.Entry(limit_frame, textvariable=self.fetch_limit_var, width=10).pack(side=tk.LEFT)
        ttk.Label(limit_frame, text=" rows (0 = all)").pack(side=tk.LEFT, padx=(5, 0))
        
        # Fetch buttons
        ttk.Label(fetch_frame, text="Fetch Actions:", font=('Arial', 10, 'bold')).grid(
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
        self.notebook.add(export_tab, text="💾 Export Data")
        
        # Main frame with padding
        main_frame = ttk.Frame(export_tab, padding="20")
        main_frame.pack(fill='both', expand=True)
        
        # Title
        ttk.Label(main_frame, text="Export Options", style='Title.TLabel').pack(pady=(0, 20))
        
        # Export options frame
        options_frame = ttk.LabelFrame(main_frame, text="Export Settings", padding="20")
        options_frame.pack(fill='both', expand=True, pady=(0, 20))
        
        # Quick export section
        quick_frame = ttk.Frame(options_frame)
        quick_frame.pack(fill='x', pady=(0, 20))
        
        ttk.Label(quick_frame, text="Quick Export:", font=('Arial', 12, 'bold')).pack(anchor='w', pady=(0, 10))
        ttk.Label(quick_frame, text="Export currently fetched data to Excel with default settings").pack(anchor='w')
        
        self.export_btn = ttk.Button(quick_frame, text="📥 Quick Export to Excel", 
                                    command=self.export_to_excel, width=25, style='Accent.TButton',
                                    state='disabled')
        self.export_btn.pack(pady=10)
        
        # Advanced export section
        advanced_frame = ttk.Frame(options_frame)
        advanced_frame.pack(fill='x', pady=(0, 20))
        
        ttk.Label(advanced_frame, text="Advanced Export:", font=('Arial', 12, 'bold')).pack(anchor='w', pady=(0, 10))
        ttk.Label(advanced_frame, text="Customize column order and formatting").pack(anchor='w')
        
        ttk.Button(advanced_frame, text="⚙️ Custom Column Export", 
                  command=self.export_with_settings, width=25).pack(pady=10)
        
        # Selected columns display
        col_display_frame = ttk.LabelFrame(options_frame, text="Selected Columns", padding="10")
        col_display_frame.pack(fill='both', expand=True)
        
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
    
    def export_to_excel(self):
        """Export current data to Excel"""
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
            initialfile=f"{file_name}_export_{date_str}.xlsx"
        )
        
        if not file_path:
            return
        
        def export():
            self.status_bar.config(text="Exporting to Excel...")
            self.log_message("Exporting to Excel...", 'info')
            
            try:
                # Create DataFrame
                df = pd.DataFrame(
                    self.current_data['data'],
                    columns=self.current_data['columns']
                )
                
                # Export to Excel with formatting
                with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
                    df.to_excel(writer, sheet_name='Data', index=False)
                    
                    # Get workbook for formatting
                    workbook = writer.book
                    worksheet = writer.sheets['Data']
                    
                    # Apply formatting
                    header_fill = PatternFill(start_color="4F81BD", end_color="4F81BD", fill_type="solid")
                    header_font = Font(color="FFFFFF", bold=True)
                    
                    # Format headers
                    for cell in worksheet[1]:
                        cell.fill = header_fill
                        cell.font = header_font
                        cell.alignment = Alignment(horizontal="center")
                    
                    # Auto-adjust column widths
                    for column in worksheet.columns:
                        max_length = 0
                        column_letter = column[0].column_letter
                        for cell in column:
                            try:
                                if len(str(cell.value)) > max_length:
                                    max_length = len(str(cell.value))
                            except:
                                pass
                        adjusted_width = min(max_length + 2, 30)
                        worksheet.column_dimensions[column_letter].width = adjusted_width
                
                self.status_bar.config(text=f"Exported to {os.path.basename(file_path)}")
                self.log_message(f"✅ Exported {len(df)} rows to Excel", 'success')
                
                # Ask to open file
                if messagebox.askyesno("Export Successful", 
                                      f"✅ File saved successfully!\n\n{os.path.basename(file_path)}\n\nOpen file now?"):
                    os.startfile(file_path)
                    
            except Exception as e:
                self.status_bar.config(text=f"Export error: {str(e)}")
                self.log_message(f"❌ Export error: {str(e)}", 'error')
                messagebox.showerror("Export Error", f"Failed to export:\n{str(e)}")
        
        threading.Thread(target=export, daemon=True).start()
    
    def export_with_settings(self):
        """Export with custom settings dialog"""
        if not self.current_data:
            self.log_message("No data to export", 'error')
            return
        
        # Create settings dialog
        dialog = tk.Toplevel(self.root)
        dialog.title("Export Settings")
        dialog.geometry("500x400")
        dialog.transient(self.root)
        dialog.grab_set()
        
        frame = ttk.Frame(dialog, padding="20")
        frame.pack(fill='both', expand=True)
        
        # Title
        ttk.Label(frame, text="Custom Export Settings", font=('Arial', 12, 'bold')).pack(pady=(0, 15))
        
        # Column ordering
        ttk.Label(frame, text="Column Order:").pack(anchor='w', pady=(0, 5))
        
        # Listbox for column ordering
        listbox_frame = ttk.Frame(frame)
        listbox_frame.pack(fill='both', expand=True, pady=(0, 10))
        
        listbox = tk.Listbox(listbox_frame, selectmode='single')
        listbox.pack(side=tk.LEFT, fill='both', expand=True)
        
        scrollbar = ttk.Scrollbar(listbox_frame, command=listbox.yview)
        scrollbar.pack(side=tk.RIGHT, fill='y')
        listbox.config(yscrollcommand=scrollbar.set)
        
        # Add columns to listbox
        for col in self.current_data['columns']:
            listbox.insert(tk.END, col)
        
        # Buttons for reordering
        btn_frame = ttk.Frame(frame)
        btn_frame.pack(pady=10)
        
        def move_up():
            selected = listbox.curselection()
            if selected and selected[0] > 0:
                index = selected[0]
                item = listbox.get(index)
                listbox.delete(index)
                listbox.insert(index-1, item)
                listbox.selection_set(index-1)
        
        def move_down():
            selected = listbox.curselection()
            if selected and selected[0] < listbox.size() - 1:
                index = selected[0]
                item = listbox.get(index)
                listbox.delete(index)
                listbox.insert(index+1, item)
                listbox.selection_set(index+1)
        
        ttk.Button(btn_frame, text="Move Up", command=move_up, width=10).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="Move Down", command=move_down, width=10).pack(side=tk.LEFT, padx=5)
        
        # Export button
        def export_with_order():
            # Get new column order
            new_order = list(listbox.get(0, tk.END))
            
            # Create DataFrame with new order
            df = pd.DataFrame(self.current_data['data'], columns=self.current_data['columns'])
            df = df[new_order]  # Reorder columns
            
            # Save file
            table_name = self.selected_table_var.get()
            file_name = table_name.split('.')[-1] if '.' in table_name else table_name
            date_str = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            file_path = filedialog.asksaveasfilename(
                defaultextension=".xlsx",
                filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")],
                initialfile=f"{file_name}_custom_{date_str}.xlsx"
            )
            
            if file_path:
                df.to_excel(file_path, index=False)
                self.log_message(f"✅ Exported with custom column order to {file_path}", 'success')
                dialog.destroy()
                
                if messagebox.askyesno("Export Successful", "Open file now?"):
                    os.startfile(file_path)
        
        ttk.Button(frame, text="Export with This Order", 
                  command=export_with_order, width=20, style='Accent.TButton').pack(pady=10)
        
        # Close button
        ttk.Button(frame, text="Cancel", command=dialog.destroy, width=10).pack()

def main():
    """Main entry point"""
    try:
        # Check dependencies
        import pyodbc
        import pandas
        from openpyxl import Workbook
        
        # Create main window
        root = tk.Tk()
        
        # Set window icon and position
        root.title("SQL Data Fetcher - MAHESHWAGH\\WINCC")
        root.geometry("1400x850")
        
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
        print("pip install pyodbc pandas openpyxl tkcalendar")
        
        install = input("\nInstall now? (y/n): ")
        if install.lower() == 'y':
            import subprocess
            subprocess.check_call([sys.executable, "-m", "pip", "install",
                                 "pyodbc", "pandas", "openpyxl", "tkcalendar"])
            print("\nPackages installed successfully!")
            print("Please restart the application.")
        else:
            print("Please install packages manually.")

if __name__ == "__main__":
    main()
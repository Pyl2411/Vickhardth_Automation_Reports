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
import copy
import re
import traceback
from typing import Dict, List, Optional, Any

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
            # Format: "column_name (data_type(length), nullable)"
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
    
    def fetch_data(self, table_name: str, date_column: Optional[str] = None, 
                   start_date: Optional[str] = None, end_date: Optional[str] = None, 
                   selected_columns: Optional[List[str]] = None, limit: Optional[int] = None,
                   where_clause: Optional[str] = None) -> Dict:
        """Fetch data with filters"""
        try:
            # Clean table name (remove row count info)
            if '(' in table_name:
                table_name = table_name.split('(')[0].strip()
            
            # Remove schema if present for table reference
            if '.' in table_name:
                schema, table = table_name.split('.')
                table_ref = f"[{schema}].[{table}]"
            else:
                table_ref = f"[{table_name}]"
            
            # Clean column names (remove type info)
            if selected_columns:
                clean_columns = []
                for col in selected_columns:
                    # Remove type information in parentheses
                    clean_col = re.split(r'\s*\(', col)[0].strip()
                    clean_columns.append(clean_col)
                selected_columns = clean_columns
            
            # Build SELECT clause
            if selected_columns and len(selected_columns) > 0:
                columns_str = ", ".join([f"[{col}]" for col in selected_columns])
                select_clause = f"SELECT {columns_str}"
            else:
                select_clause = "SELECT *"
            
            # Build WHERE clause and parameters
            where_conditions = []
            params = []
            
            # Handle date column filtering
            if date_column:
                # Clean date column name
                date_column_clean = re.split(r'\s*\(', date_column)[0].strip()
                
                if start_date:
                    where_conditions.append(f"[{date_column_clean}] >= ?")
                    params.append(start_date)
                
                if end_date:
                    where_conditions.append(f"[{date_column_clean}] <= ?")
                    params.append(f"{end_date} 23:59:59.999")
            
            # Add custom WHERE clause if provided
            if where_clause and where_clause.strip():
                # Remove any leading/trailing whitespace and the word WHERE if present
                custom_where = where_clause.strip()
                if custom_where.upper().startswith('WHERE '):
                    custom_where = custom_where[6:].strip()
                where_conditions.append(f"({custom_where})")
                # Note: Custom WHERE clause should NOT contain parameter placeholders
            
            # Combine WHERE conditions
            where_clause_str = ""
            if where_conditions:
                where_clause_str = "WHERE " + " AND ".join(where_conditions)
            
            # Build LIMIT clause (using TOP for SQL Server)
            limit_clause = ""
            if limit and limit > 0:
                limit_clause = f"TOP ({limit})"
            
            # Build ORDER BY for consistent results
            order_by_clause = ""
            if date_column:
                date_column_clean = re.split(r'\s*\(', date_column)[0].strip()
                order_by_clause = f"ORDER BY [{date_column_clean}]"
            
            # Build full query
            if limit_clause:
                query = f"{select_clause.replace('SELECT', f'SELECT {limit_clause}')} FROM {table_ref} {where_clause_str} {order_by_clause}"
            else:
                query = f"{select_clause} FROM {table_ref} {where_clause_str} {order_by_clause}"
            
            # Debug log
            logger.info(f"Executing query: {query}")
            logger.info(f"Number of parameters: {len(params)}")
            logger.info(f"Parameters: {params}")
            
            # Count parameter placeholders in the query
            param_placeholders = query.count('?')
            
            # Validate parameter count vs placeholder count
            if param_placeholders != len(params):
                error_msg = f"Parameter mismatch: Query has {param_placeholders} placeholders but {len(params)} parameters provided."
                logger.error(error_msg)
                return {
                    'success': False,
                    'error': error_msg,
                    'data': [],
                    'columns': [],
                    'row_count': 0,
                    'total_count': 0
                }
            
            # Execute with parameters
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
            
            # Get results
            columns = [column[0] for column in self.cursor.description]
            rows = self.cursor.fetchall()
            
            # Convert rows to list of dictionaries
            data_list = []
            for row in rows:
                row_dict = {}
                for i, col in enumerate(columns):
                    value = row[i]
                    # Convert datetime objects to string for JSON serialization
                    if isinstance(value, datetime):
                        value = value.strftime('%Y-%m-%d %H:%M:%S')
                    elif isinstance(value, bytes):
                        value = str(value)  # Handle binary data
                    row_dict[col] = value
                data_list.append(row_dict)
            
            # Get total count without limit for information
            if limit and limit > 0:
                count_query = f"SELECT COUNT(*) FROM {table_ref} {where_clause_str}"
                if params:
                    self.cursor.execute(count_query, params)
                else:
                    self.cursor.execute(count_query)
                total_count = self.cursor.fetchone()[0]
            else:
                total_count = len(rows)
            
            return {
                'success': True,
                'data': data_list,
                'columns': columns,
                'row_count': len(rows),
                'total_count': total_count,
                'query': query,
                'parameters': params
            }
            
        except pyodbc.Error as e:
            error_msg = f"Database error: {str(e)}"
            logger.error(error_msg)
            logger.error(f"Query: {query}")
            logger.error(f"Params: {params}")
            
            # Specific handling for error 07002
            if "07002" in str(e) or "COUNT field incorrect" in str(e):
                error_msg += "\n\nPossible causes:\n"
                error_msg += "1. Parameter count mismatch in WHERE clause\n"
                error_msg += "2. Custom WHERE clause contains parameter placeholders (?) that shouldn't be there\n"
                error_msg += "3. Date column has spaces or special characters\n"
                error_msg += "\nTry removing the custom WHERE clause or checking your column names."
            
            return {
                'success': False,
                'error': error_msg,
                'data': [],
                'columns': [],
                'row_count': 0,
                'total_count': 0
            }
        except Exception as e:
            error_msg = f"Unexpected error: {str(e)}"
            logger.error(error_msg)
            logger.error(traceback.format_exc())
            return {
                'success': False,
                'error': error_msg,
                'data': [],
                'columns': [],
                'row_count': 0,
                'total_count': 0
            }

class ColumnMappingDialog:
    """Dialog for mapping database columns to Excel columns"""
    
    def __init__(self, parent, db_columns: List[str], excel_fields: List[Dict]):
        self.parent = parent
        self.db_columns = db_columns
        self.excel_fields = excel_fields
        self.mapping = {}
        self.result = None
        
        self.create_dialog()
    
    def create_dialog(self):
        """Create the mapping dialog"""
        self.dialog = tk.Toplevel(self.parent)
        self.dialog.title("Map Database Columns to Excel Fields")
        self.dialog.geometry("1000x700")
        self.dialog.transient(self.parent)
        self.dialog.grab_set()
        
        # Center dialog
        self.dialog.update_idletasks()
        x = (self.dialog.winfo_screenwidth() - self.dialog.winfo_width()) // 2
        y = (self.dialog.winfo_screenheight() - self.dialog.winfo_height()) // 2
        self.dialog.geometry(f"+{x}+{y}")
        
        # Title
        title_frame = ttk.Frame(self.dialog, padding="10")
        title_frame.pack(fill='x')
        
        ttk.Label(title_frame, text="Column Mapping", 
                 font=('Segoe UI', 14, 'bold')).pack()
        ttk.Label(title_frame, text="Map database columns to Excel fields").pack()
        
        # Main content
        main_frame = ttk.Frame(self.dialog, padding="10")
        main_frame.pack(fill='both', expand=True)
        
        # Left side - Excel fields
        left_frame = ttk.LabelFrame(main_frame, text="Excel Fields", padding="10")
        left_frame.pack(side='left', fill='both', expand=True, padx=(0, 5))
        
        ttk.Label(left_frame, text="Target fields in Excel:", 
                 font=('Segoe UI', 10, 'bold')).pack(anchor='w', pady=(0, 10))
        
        # Treeview for Excel fields
        tree_frame = ttk.Frame(left_frame)
        tree_frame.pack(fill='both', expand=True)
        
        self.excel_tree = ttk.Treeview(tree_frame, columns=('field', 'location', 'type'), 
                                      show='headings', height=15)
        
        self.excel_tree.heading('field', text='Excel Field')
        self.excel_tree.heading('location', text='Location')
        self.excel_tree.heading('type', text='Type')
        self.excel_tree.column('field', width=200)
        self.excel_tree.column('location', width=80)
        self.excel_tree.column('type', width=80)
        
        # Add scrollbar
        scrollbar = ttk.Scrollbar(tree_frame, orient='vertical', command=self.excel_tree.yview)
        self.excel_tree.configure(yscrollcommand=scrollbar.set)
        
        self.excel_tree.pack(side='left', fill='both', expand=True)
        scrollbar.pack(side='right', fill='y')
        
        # Add Excel fields to tree
        for i, field in enumerate(self.excel_fields):
            location = field.get('location', '')
            field_type = field.get('type', 'text')
            is_table = '✓' if field.get('is_table_column', False) else ''
            display_name = f"{field['name']} {is_table}"
            self.excel_tree.insert('', 'end', iid=i, 
                                  values=(display_name, location, field_type))
        
        # Right side - Database columns
        right_frame = ttk.LabelFrame(main_frame, text="Database Columns", padding="10")
        right_frame.pack(side='right', fill='both', expand=True, padx=(5, 0))
        
        ttk.Label(right_frame, text="Available database columns:", 
                 font=('Segoe UI', 10, 'bold')).pack(anchor='w', pady=(0, 10))
        
        # Search for database columns
        search_frame = ttk.Frame(right_frame)
        search_frame.pack(fill='x', pady=(0, 10))
        
        ttk.Label(search_frame, text="Search:").pack(side=tk.LEFT, padx=(0, 5))
        self.search_var = tk.StringVar()
        search_entry = ttk.Entry(search_frame, textvariable=self.search_var, width=20)
        search_entry.pack(side=tk.LEFT)
        search_entry.bind('<KeyRelease>', self.filter_columns)
        
        # Listbox for database columns
        listbox_frame = ttk.Frame(right_frame)
        listbox_frame.pack(fill='both', expand=True)
        
        self.column_listbox = tk.Listbox(listbox_frame, selectmode='single', height=15,
                                        font=('Consolas', 9))
        self.column_listbox.pack(side=tk.LEFT, fill='both', expand=True)
        
        col_scrollbar = ttk.Scrollbar(listbox_frame, orient='vertical', 
                                     command=self.column_listbox.yview)
        self.column_listbox.configure(yscrollcommand=col_scrollbar.set)
        col_scrollbar.pack(side='right', fill='y')
        
        # Store original columns and filtered columns
        self.original_columns = self.db_columns
        self.filtered_columns = self.db_columns.copy()
        
        # Add database columns
        self.update_column_listbox()
        
        # Mapping controls
        control_frame = ttk.Frame(self.dialog, padding="10")
        control_frame.pack(fill='x')
        
        ttk.Label(control_frame, text="Selected mapping:").pack(side='left', padx=(0, 10))
        
        self.mapping_label = ttk.Label(control_frame, text="None", foreground="blue",
                                      font=('Segoe UI', 10, 'bold'))
        self.mapping_label.pack(side='left', padx=(0, 20))
        
        ttk.Button(control_frame, text="Map Selected", 
                  command=self.map_selected).pack(side='left', padx=5)
        ttk.Button(control_frame, text="Clear Mapping", 
                  command=self.clear_mapping).pack(side='left', padx=5)
        
        # Current mappings display
        mapping_frame = ttk.LabelFrame(self.dialog, text="Current Mappings", padding="10")
        mapping_frame.pack(fill='x', padx=10, pady=(0, 10))
        
        self.mapping_text = scrolledtext.ScrolledText(mapping_frame, height=6, wrap=tk.WORD)
        self.mapping_text.pack(fill='both')
        
        # Buttons
        button_frame = ttk.Frame(self.dialog, padding="10")
        button_frame.pack(fill='x')
        
        ttk.Button(button_frame, text="Apply Mapping", 
                  command=self.apply_mapping).pack(side='right', padx=5)
        ttk.Button(button_frame, text="Cancel", 
                  command=self.cancel).pack(side='right', padx=5)
        ttk.Button(button_frame, text="Auto Map", 
                  command=self.auto_map).pack(side='left', padx=5)
        ttk.Button(button_frame, text="Suggest Mapping", 
                  command=self.suggest_mapping).pack(side='left', padx=5)
        
        # Bind selection events
        self.excel_tree.bind('<<TreeviewSelect>>', self.on_excel_select)
        self.column_listbox.bind('<<ListboxSelect>>', self.on_column_select)
        
        self.selected_excel_field = None
        self.selected_db_column = None
        
        # Initialize mappings display
        self.update_mappings_display()
    
    def filter_columns(self, event=None):
        """Filter columns based on search text"""
        search_text = self.search_var.get().lower()
        if not search_text:
            self.filtered_columns = self.original_columns
        else:
            self.filtered_columns = [
                col for col in self.original_columns
                if search_text in col.lower()
            ]
        self.update_column_listbox()
    
    def update_column_listbox(self):
        """Update the column listbox with filtered columns"""
        self.column_listbox.delete(0, tk.END)
        for col in self.filtered_columns:
            self.column_listbox.insert(tk.END, col)
    
    def on_excel_select(self, event):
        """Handle Excel field selection"""
        selection = self.excel_tree.selection()
        if selection:
            self.selected_excel_field = int(selection[0])
            field_name = self.excel_fields[self.selected_excel_field]['name']
            self.update_mapping_label(field_name, self.selected_db_column)
    
    def on_column_select(self, event):
        """Handle database column selection"""
        selection = self.column_listbox.curselection()
        if selection:
            index = selection[0]
            if index < len(self.filtered_columns):
                self.selected_db_column = self.filtered_columns[index]
                if self.selected_excel_field is not None:
                    field_name = self.excel_fields[self.selected_excel_field]['name']
                    self.update_mapping_label(field_name, self.selected_db_column)
    
    def update_mapping_label(self, excel_field: str, db_column: Optional[str]):
        """Update the mapping label"""
        if db_column:
            self.mapping_label.config(text=f"{excel_field} → {db_column}")
        else:
            self.mapping_label.config(text=f"{excel_field}")
    
    def map_selected(self):
        """Map selected Excel field to selected database column"""
        if self.selected_excel_field is None:
            messagebox.showwarning("Warning", "Please select an Excel field first")
            return
        
        if self.selected_db_column is None:
            messagebox.showwarning("Warning", "Please select a database column")
            return
        
        excel_field = self.excel_fields[self.selected_excel_field]['name']
        
        # Clean database column name (remove type info)
        db_column_clean = re.split(r'\s*\(', self.selected_db_column)[0].strip()
        self.mapping[excel_field] = db_column_clean
        
        # Update treeview to show mapping
        location = self.excel_fields[self.selected_excel_field].get('location', '')
        field_type = self.excel_fields[self.selected_excel_field].get('type', 'text')
        is_table = '✓' if self.excel_fields[self.selected_excel_field].get('is_table_column', False) else ''
        display_name = f"{excel_field} {is_table}"
        
        self.excel_tree.item(self.selected_excel_field, 
                           values=(display_name, f"← {db_column_clean}", field_type))
        
        self.update_mappings_display()
    
    def clear_mapping(self):
        """Clear mapping for selected Excel field"""
        if self.selected_excel_field is None:
            return
        
        excel_field = self.excel_fields[self.selected_excel_field]['name']
        if excel_field in self.mapping:
            del self.mapping[excel_field]
            
            # Update treeview
            location = self.excel_fields[self.selected_excel_field].get('location', '')
            field_type = self.excel_fields[self.selected_excel_field].get('type', 'text')
            is_table = '✓' if self.excel_fields[self.selected_excel_field].get('is_table_column', False) else ''
            display_name = f"{excel_field} {is_table}"
            
            self.excel_tree.item(self.selected_excel_field, 
                               values=(display_name, location, field_type))
            
            self.update_mappings_display()
    
    def auto_map(self):
        """Auto-map columns based on name similarity"""
        mapping_count = 0
        
        for excel_field in self.excel_fields:
            field_name = excel_field['name'].lower()
            
            # Skip if already mapped
            if excel_field['name'] in self.mapping:
                continue
            
            # Try to find matching database column
            best_match = None
            best_score = 0
            
            for db_col in self.original_columns:
                db_col_clean = re.split(r'\s*\(', db_col)[0].strip()
                db_col_lower = db_col_clean.lower()
                
                # Calculate similarity score
                score = 0
                
                # Exact match
                if field_name == db_col_lower:
                    score = 100
                # Contains
                elif field_name in db_col_lower or db_col_lower in field_name:
                    score = 80
                # Word match
                else:
                    field_words = set(field_name.split('_'))
                    db_words = set(db_col_lower.split('_'))
                    common_words = field_words.intersection(db_words)
                    if common_words:
                        score = len(common_words) * 20
                
                if score > best_score:
                    best_score = score
                    best_match = db_col_clean
            
            # Apply mapping if score is good enough
            if best_match and best_score >= 40:
                self.mapping[excel_field['name']] = best_match
                mapping_count += 1
                
                # Update treeview
                idx = self.excel_fields.index(excel_field)
                location = excel_field.get('location', '')
                field_type = excel_field.get('type', 'text')
                is_table = '✓' if excel_field.get('is_table_column', False) else ''
                display_name = f"{excel_field['name']} {is_table}"
                
                self.excel_tree.item(idx, 
                                   values=(display_name, f"← {best_match}", field_type))
        
        self.update_mappings_display()
        messagebox.showinfo("Auto Map", f"Auto-mapped {mapping_count} fields!")
    
    def suggest_mapping(self):
        """Show mapping suggestions for selected field"""
        if self.selected_excel_field is None:
            messagebox.showwarning("Warning", "Please select an Excel field first")
            return
        
        excel_field = self.excel_fields[self.selected_excel_field]
        field_name = excel_field['name'].lower()
        
        suggestions = []
        for db_col in self.original_columns:
            db_col_clean = re.split(r'\s*\(', db_col)[0].strip()
            db_col_lower = db_col_clean.lower()
            
            if (field_name in db_col_lower or 
                db_col_lower in field_name or
                any(word in db_col_lower for word in field_name.split('_'))):
                suggestions.append(db_col_clean)
        
        if suggestions:
            suggestion_text = f"Suggestions for '{excel_field['name']}':\n\n"
            for i, suggestion in enumerate(suggestions[:10], 1):
                suggestion_text += f"{i}. {suggestion}\n"
            
            # Create suggestion dialog
            self.show_suggestion_dialog(suggestion_text)
        else:
            messagebox.showinfo("No Suggestions", 
                              f"No close matches found for '{excel_field['name']}'")
    
    def show_suggestion_dialog(self, suggestion_text: str):
        """Show suggestion dialog"""
        dialog = tk.Toplevel(self.dialog)
        dialog.title("Mapping Suggestions")
        dialog.geometry("400x300")
        dialog.transient(self.dialog)
        dialog.grab_set()
        
        # Center dialog
        dialog.update_idletasks()
        x = self.dialog.winfo_x() + (self.dialog.winfo_width() - dialog.winfo_width()) // 2
        y = self.dialog.winfo_y() + (self.dialog.winfo_height() - dialog.winfo_height()) // 2
        dialog.geometry(f"+{x}+{y}")
        
        # Text widget for suggestions
        text_widget = scrolledtext.ScrolledText(dialog, wrap=tk.WORD)
        text_widget.pack(fill='both', expand=True, padx=10, pady=10)
        text_widget.insert(1.0, suggestion_text)
        text_widget.config(state='disabled')
        
        # Button frame
        btn_frame = ttk.Frame(dialog, padding="10")
        btn_frame.pack(fill='x')
        
        ttk.Button(btn_frame, text="Close", 
                  command=dialog.destroy).pack(side=tk.RIGHT)
    
    def update_mappings_display(self):
        """Update the mappings display text"""
        self.mapping_text.delete(1.0, tk.END)
        
        if not self.mapping:
            self.mapping_text.insert(1.0, "No mappings defined")
            return
        
        # Group mappings by sheet type
        header_mappings = []
        table_mappings = []
        
        for excel_field, db_column in self.mapping.items():
            # Find if this is a table column
            is_table = False
            for field in self.excel_fields:
                if field['name'] == excel_field:
                    is_table = field.get('is_table_column', False)
                    break
            
            if is_table:
                table_mappings.append((excel_field, db_column))
            else:
                header_mappings.append((excel_field, db_column))
        
        # Display header mappings
        if header_mappings:
            self.mapping_text.insert(tk.END, "Header Fields:\n", 'header')
            for excel_field, db_column in header_mappings:
                self.mapping_text.insert(tk.END, f"  ✓ {excel_field} → {db_column}\n")
        
        # Display table mappings
        if table_mappings:
            if header_mappings:
                self.mapping_text.insert(tk.END, "\n")
            self.mapping_text.insert(tk.END, "Table Columns:\n", 'header')
            for excel_field, db_column in table_mappings:
                self.mapping_text.insert(tk.END, f"  ✓ {excel_field} → {db_column}\n")
        
        # Configure tags
        self.mapping_text.tag_configure('header', font=('Segoe UI', 10, 'bold'))
    
    def apply_mapping(self):
        """Apply the mapping and close dialog"""
        self.result = self.mapping
        self.dialog.destroy()
    
    def cancel(self):
        """Cancel the mapping"""
        self.result = None
        self.dialog.destroy()
    
    def get_mapping(self) -> Optional[Dict]:
        """Get the mapping result"""
        return self.result

class VPIExcelExporter:
    """Handles exporting data to VPI Job Card Excel format with actual data"""
    
    @staticmethod
    def get_excel_fields() -> List[Dict]:
        """Get all Excel fields that need to be populated"""
        return [
            # Header fields
            {'name': 'BATCH_NUMBER', 'location': 'A6', 'type': 'text'},
            {'name': 'JOB_NO', 'location': 'A7', 'type': 'text'},
            {'name': 'OPERATOR_NAME', 'location': 'A8', 'type': 'text'},
            {'name': 'STATOR_NOMINAL_VOLTAGE', 'location': 'A9', 'type': 'text'},
            {'name': 'JOB_1_SERIAL_NO', 'location': 'A10', 'type': 'text'},
            {'name': 'JOB_2_SERIAL_NO', 'location': 'A11', 'type': 'text'},
            
            # Right side header fields
            {'name': 'PROCESS_START_TIME', 'location': 'H6', 'type': 'datetime'},
            {'name': 'PROCESS_STOP_TIME', 'location': 'H7', 'type': 'datetime'},
            {'name': 'PROCESS_TOTAL_TIME', 'location': 'H8', 'type': 'text'},
            {'name': 'STATOR_LENGTH', 'location': 'H9', 'type': 'text'},
            {'name': 'JOB_3_SERIAL_NO', 'location': 'H10', 'type': 'text'},
            {'name': 'JOB_4_SERIAL_NO', 'location': 'H11', 'type': 'text'},
            
            # Main table data columns (these will be filled per row)
            {'name': 'TIME', 'location': 'C', 'type': 'time', 'is_table_column': True},
            {'name': 'SETPOINT', 'location': 'D', 'type': 'number', 'is_table_column': True},
            {'name': 'UNIT', 'location': 'E', 'type': 'text', 'is_table_column': True},
            {'name': 'PROCESS_TANK_VACUUM_PRESSURE', 'location': 'F', 'type': 'number', 'is_table_column': True},
            {'name': 'RESIN_TANK_VACUUM_PRESSURE', 'location': 'G', 'type': 'number', 'is_table_column': True},
            {'name': 'JOB1_CAPACITANCE', 'location': 'H', 'type': 'number', 'is_table_column': True},
            {'name': 'JOB2_CAPACITANCE', 'location': 'I', 'type': 'number', 'is_table_column': True},
            {'name': 'JOB3_CAPACITANCE', 'location': 'J', 'type': 'number', 'is_table_column': True},
            {'name': 'JOB4_CAPACITANCE', 'location': 'K', 'type': 'number', 'is_table_column': True},
            {'name': 'RESIN_TEMP', 'location': 'L', 'type': 'number', 'is_table_column': True},
            {'name': 'RESIN_LTR', 'location': 'M', 'type': 'number', 'is_table_column': True},
            {'name': 'JOB1_TEMP', 'location': 'N', 'type': 'number', 'is_table_column': True},
            {'name': 'JOB2_TEMP', 'location': 'O', 'type': 'number', 'is_table_column': True},
            {'name': 'JOB3_TEMP', 'location': 'P', 'type': 'number', 'is_table_column': True},
            {'name': 'JOB4_TEMP', 'location': 'Q', 'type': 'number', 'is_table_column': True},
            
            # Additional fields for LT VPI sheet
            {'name': 'BATCH_NAME', 'location': 'A6', 'type': 'text', 'sheet': 'LT VPI'},
            {'name': 'RESIN_TYPE', 'location': 'A9', 'type': 'text', 'sheet': 'LT VPI'},
            {'name': 'STATOR_VOLTAGE', 'location': 'H9', 'type': 'text', 'sheet': 'LT VPI'},
        ]
    
    @staticmethod
    def create_vpi_excel_with_data(data: Dict, column_mapping: Dict, 
                                   logo_path: Optional[str] = None,
                                   include_ht: bool = True,
                                   include_lt: bool = True) -> io.BytesIO:
        """Create VPI Job Card Excel with actual data"""
        try:
            # Create a new workbook
            wb = Workbook()
            
            # Remove default sheet if we're creating specific ones
            if wb.sheetnames and 'Sheet' in wb.sheetnames[0]:
                default_sheet = wb.active
                wb.remove(default_sheet)
            
            # Create sheets based on selection
            if include_ht:
                ws_ht = wb.create_sheet(title="HT VPI")
                VPIExcelExporter.create_sheet_structure(ws_ht, "HT VPI")
            
            if include_lt:
                ws_lt = wb.create_sheet(title="LT VPI")
                VPIExcelExporter.create_sheet_structure(ws_lt, "LT VPI")
            
            # Add logo if provided
            if logo_path and os.path.exists(logo_path):
                try:
                    img = XLImage(logo_path)
                    img.height = 80
                    img.width = 200
                    
                    # Add to all sheets
                    for ws in wb.worksheets:
                        ws.add_image(img, 'A1')
                        # Insert rows to make space for logo
                        ws.insert_rows(1, 4)
                        ws.row_dimensions[1].height = 60
                except Exception as e:
                    logger.warning(f"Could not add logo: {e}")
            
            # Populate data if available
            if data and data['data']:
                # Get first data row for header data
                if data['data']:
                    data_row = data['data'][0]
                    
                    # Populate header fields
                    if include_ht:
                        VPIExcelExporter.populate_header_data(ws_ht, data_row, column_mapping)
                    
                    if include_lt:
                        VPIExcelExporter.populate_header_data(ws_lt, data_row, column_mapping, is_lt=True)
                    
                    # Populate table data if we have process data rows
                    if len(data['data']) > 1:
                        process_data = data['data'][1:]
                        if include_ht:
                            VPIExcelExporter.populate_table_data(ws_ht, process_data, column_mapping)
                        if include_lt:
                            VPIExcelExporter.populate_table_data(ws_lt, process_data, column_mapping)
            
            # Add Data Sheet with raw data
            if data:
                ws_data = wb.create_sheet(title="Raw Data")
                VPIExcelExporter.add_raw_data_sheet(ws_data, data)
            
            # Add Mapping Sheet
            ws_mapping = wb.create_sheet(title="Column Mapping")
            VPIExcelExporter.add_mapping_sheet(ws_mapping, column_mapping, data)
            
            # Add Summary Sheet
            ws_summary = wb.create_sheet(title="Summary")
            VPIExcelExporter.add_summary_sheet(ws_summary, data)
            
            # Save to bytes
            excel_buffer = io.BytesIO()
            wb.save(excel_buffer)
            excel_buffer.seek(0)
            
            return excel_buffer
            
        except Exception as e:
            logger.error(f"Excel creation error: {e}")
            raise Exception(f"Excel creation error: {str(e)}")
    
    @staticmethod
    def create_sheet_structure(ws, sheet_type: str = "HT VPI"):
        """Create the VPI sheet structure"""
        # Clear any existing content
        ws.delete_rows(1, ws.max_row)
        
        if sheet_type == "HT VPI":
            title_text = "VACCUM IMPREGNATION AND PRESSURE MV/HV/VHV CYCLE"
            doc_ref = "CONTIFCTL/002 095 Bangalore rev J (Suivant Sco0123)"
            process_steps = VPIExcelExporter.get_ht_process_steps()
        else:  # LT VPI
            title_text = "VACCUM PRESSURE IMPREGNATION ROUND WIRE LV STATOR"
            doc_ref = "CONT/FCTL/002 036 rev H"
            process_steps = VPIExcelExporter.get_lt_process_steps()
        
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
        ws['A3'] = title_text
        ws['A3'].font = Font(name='Arial', size=10, bold=True)
        ws['A3'].alignment = Alignment(horizontal='center')
        
        ws.merge_cells('A4:Q4')
        ws['A4'] = doc_ref
        ws['A4'].font = Font(name='Arial', size=9)
        ws['A4'].alignment = Alignment(horizontal='center')
        
        # Batch info headers
        header_fields = [
            ('A6', 'BATCH NUMBER' if sheet_type == "HT VPI" else 'BATCH NAME'),
            ('A7', 'JOB NO.'),
            ('A8', 'OPERATOR NAME'),
            ('A9', 'STATOR NOMINAL VOLTAGE' if sheet_type == "HT VPI" else 'RESIN TYPE'),
            ('A10', 'JOB 1 SERIAL NO.'),
            ('A11', 'JOB 2 SERIAL NO.'),
            
            ('H6', 'PROCESS START TIME'),
            ('H7', 'PROCESS STOP TIME'),
            ('H8', 'PROCESS TOTAL TIME'),
            ('H9', 'STATOR LENGTH' if sheet_type == "HT VPI" else 'STATOR VOLTAGE'),
            ('H10', 'JOB 3 SERIAL NO.'),
            ('H11', 'JOB 4 SERIAL NO.')
        ]
        
        for cell_ref, text in header_fields:
            ws[cell_ref] = text
            ws[cell_ref].font = Font(name='Arial', size=10, bold=True)
        
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
        
        # Add process steps
        start_row = 14
        for i, (sr_no, description) in enumerate(process_steps):
            row_num = start_row + i
            if sr_no:
                ws.cell(row=row_num, column=1, value=sr_no)
                ws.cell(row=row_num, column=1).alignment = Alignment(horizontal='center')
            
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
        if sheet_type == "HT VPI":
            ws.merge_cells(f'A{sig_row}:C{sig_row}')
            ws.cell(row=sig_row, column=1, value="DATE")
            ws.cell(row=sig_row, column=1).font = Font(bold=True)
            
            ws.merge_cells(f'E{sig_row}:G{sig_row}')
            ws.cell(row=sig_row, column=5, value="NAME")
            ws.cell(row=sig_row, column=5).font = Font(bold=True)
            
            ws.merge_cells(f'N{sig_row}:Q{sig_row}')
            ws.cell(row=sig_row, column=14, value="VISA")
            ws.cell(row=sig_row, column=14).font = Font(bold=True)
        else:  # LT VPI
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
        
        ws.row_dimensions[13].height = 40  # Header row
        
        # Freeze panes
        ws.freeze_panes = 'A14'
    
    @staticmethod
    def get_ht_process_steps() -> List[tuple]:
        """Get HT VPI process steps"""
        return [
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
    
    @staticmethod
    def get_lt_process_steps() -> List[tuple]:
        """Get LT VPI process steps"""
        return [
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
    
    @staticmethod
    def populate_header_data(ws, data_row: Dict, column_mapping: Dict, is_lt: bool = False):
        """Populate header fields with actual data"""
        if is_lt:
            field_map = {
                'BATCH_NAME': 'A6',
                'JOB_NO': 'A7',
                'OPERATOR_NAME': 'A8',
                'RESIN_TYPE': 'A9',
                'JOB_1_SERIAL_NO': 'A10',
                'JOB_2_SERIAL_NO': 'A11',
                'PROCESS_START_TIME': 'H6',
                'PROCESS_STOP_TIME': 'H7',
                'PROCESS_TOTAL_TIME': 'H8',
                'STATOR_VOLTAGE': 'H9',
                'JOB_3_SERIAL_NO': 'H10',
                'JOB_4_SERIAL_NO': 'H11'
            }
        else:
            field_map = {
                'BATCH_NUMBER': 'A6',
                'JOB_NO': 'A7',
                'OPERATOR_NAME': 'A8',
                'STATOR_NOMINAL_VOLTAGE': 'A9',
                'JOB_1_SERIAL_NO': 'A10',
                'JOB_2_SERIAL_NO': 'A11',
                'PROCESS_START_TIME': 'H6',
                'PROCESS_STOP_TIME': 'H7',
                'PROCESS_TOTAL_TIME': 'H8',
                'STATOR_LENGTH': 'H9',
                'JOB_3_SERIAL_NO': 'H10',
                'JOB_4_SERIAL_NO': 'H11'
            }
        
        # Populate each field
        for excel_field, cell_ref in field_map.items():
            if excel_field in column_mapping:
                db_column = column_mapping[excel_field]
                if db_column in data_row:
                    value = data_row[db_column]
                    if value is not None:
                        # Format based on field type
                        if 'TIME' in excel_field:
                            try:
                                if isinstance(value, str):
                                    # Try to parse string to datetime
                                    for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%H:%M:%S'):
                                        try:
                                            dt = datetime.strptime(str(value), fmt)
                                            ws[cell_ref] = dt.strftime('%Y-%m-%d %H:%M:%S')
                                            break
                                        except:
                                            continue
                                    else:
                                        ws[cell_ref] = str(value)
                                elif isinstance(value, datetime):
                                    ws[cell_ref] = value.strftime('%Y-%m-%d %H:%M:%S')
                                else:
                                    ws[cell_ref] = str(value)
                            except:
                                ws[cell_ref] = str(value)
                        else:
                            ws[cell_ref] = str(value)
                        
                        # Apply formatting
                        ws[cell_ref].font = Font(name='Arial', size=10)
                        ws[cell_ref].alignment = Alignment(horizontal='left')
    
    @staticmethod
    def populate_table_data(ws, process_data: List[Dict], column_mapping: Dict):
        """Populate main table with process data"""
        if not process_data:
            return
        
        # Start from row 14 (after headers)
        start_row = 14
        max_rows = 50  # Maximum rows to fill
        
        for i, data_row in enumerate(process_data):
            if i >= max_rows:
                break
            
            row_num = start_row + i
            
            # Map data to columns based on mapping
            column_letter_map = {
                'TIME': 'C',
                'SETPOINT': 'D',
                'UNIT': 'E',
                'PROCESS_TANK_VACUUM_PRESSURE': 'F',
                'RESIN_TANK_VACUUM_PRESSURE': 'G',
                'JOB1_CAPACITANCE': 'H',
                'JOB2_CAPACITANCE': 'I',
                'JOB3_CAPACITANCE': 'J',
                'JOB4_CAPACITANCE': 'K',
                'RESIN_TEMP': 'L',
                'RESIN_LTR': 'M',
                'JOB1_TEMP': 'N',
                'JOB2_TEMP': 'O',
                'JOB3_TEMP': 'P',
                'JOB4_TEMP': 'Q'
            }
            
            for excel_field, col_letter in column_letter_map.items():
                if excel_field in column_mapping:
                    db_column = column_mapping[excel_field]
                    if db_column in data_row:
                        value = data_row[db_column]
                        if value is not None:
                            cell_ref = f"{col_letter}{row_num}"
                            try:
                                # Convert to number if possible
                                if isinstance(value, (int, float)):
                                    ws[cell_ref] = float(value)
                                elif isinstance(value, str):
                                    # Try to parse as number
                                    try:
                                        ws[cell_ref] = float(value)
                                    except:
                                        ws[cell_ref] = value
                                else:
                                    ws[cell_ref] = str(value)
                            except:
                                ws[cell_ref] = str(value)
    
    @staticmethod
    def add_raw_data_sheet(ws, data: Dict):
        """Add raw data sheet for reference"""
        if not data or not data['data']:
            ws['A1'] = "No data available"
            return
        
        # Add title
        ws['A1'] = "Raw Data from Database"
        ws['A1'].font = Font(bold=True, size=14)
        ws.merge_cells('A1:C1')
        
        # Add metadata
        ws['A3'] = "Generated:"
        ws['B3'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        ws['A4'] = "Total Rows:"
        ws['B4'] = len(data['data'])
        ws['A5'] = "Query:"
        ws['B5'] = data.get('query', 'N/A')
        
        # Add headers
        columns = data['columns']
        header_row = 7
        for col_idx, col_name in enumerate(columns, start=1):
            cell = ws.cell(row=header_row, column=col_idx, value=col_name)
            cell.font = Font(bold=True)
            cell.fill = PatternFill(start_color="E0E0E0", end_color="E0E0E0", fill_type="solid")
            cell.border = Border(
                left=Side(style='thin'),
                right=Side(style='thin'),
                top=Side(style='thin'),
                bottom=Side(style='thin')
            )
        
        # Add data
        data_start_row = header_row + 1
        for row_idx, row_data in enumerate(data['data']):
            actual_row = data_start_row + row_idx
            for col_idx, col_name in enumerate(columns, start=1):
                value = row_data.get(col_name, '')
                cell = ws.cell(row=actual_row, column=col_idx, value=value)
                # Add alternating row colors
                if row_idx % 2 == 0:
                    cell.fill = PatternFill(start_color="F5F5F5", end_color="F5F5F5", fill_type="solid")
        
        # Auto-adjust column widths
        for column in ws.columns:
            max_length = 0
            column_letter = column[0].column_letter
            for cell in column:
                try:
                    if cell.value and len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass
            adjusted_width = min(max_length + 2, 50)
            ws.column_dimensions[column_letter].width = adjusted_width
    
    @staticmethod
    def add_mapping_sheet(ws, column_mapping: Dict, data: Dict):
        """Add column mapping sheet"""
        ws['A1'] = "Column Mapping Information"
        ws['A1'].font = Font(bold=True, size=14)
        ws.merge_cells('A1:C1')
        
        # Add mapping table
        ws['A3'] = "Excel Field"
        ws['B3'] = "Database Column"
        ws['C3'] = "Data Type"
        ws['A3'].font = Font(bold=True)
        ws['B3'].font = Font(bold=True)
        ws['C3'].font = Font(bold=True)
        
        row = 4
        for excel_field, db_column in column_mapping.items():
            ws.cell(row=row, column=1, value=excel_field)
            ws.cell(row=row, column=2, value=db_column)
            
            # Try to find data type
            data_type = "Unknown"
            if data and 'columns' in data:
                # This would need actual data type information
                pass
            ws.cell(row=row, column=3, value=data_type)
            
            row += 1
        
        # Auto-adjust column widths
        for col in ['A', 'B', 'C']:
            ws.column_dimensions[col].width = 30
    
    @staticmethod
    def add_summary_sheet(ws, data: Dict):
        """Add summary sheet"""
        ws['A1'] = "Export Summary"
        ws['A1'].font = Font(bold=True, size=14)
        ws.merge_cells('A1:B1')
        
        ws['A3'] = "Export Date:"
        ws['B3'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        ws['A4'] = "Data Source:"
        ws['B4'] = "Database Export"
        
        if data:
            ws['A5'] = "Total Rows:"
            ws['B5'] = len(data['data'])
            
            ws['A6'] = "Columns:"
            ws['B6'] = len(data['columns']) if 'columns' in data else 0
            
            ws['A7'] = "Query Executed:"
            ws['B7'] = data.get('query', 'N/A')
            
            if 'total_count' in data:
                ws['A8'] = "Total Available Rows:"
                ws['B8'] = data['total_count']
        
        # Format as table
        for row in range(3, 10):
            for col in [1, 2]:
                cell = ws.cell(row=row, column=col)
                cell.border = Border(
                    left=Side(style='thin'),
                    right=Side(style='thin'),
                    top=Side(style='thin'),
                    bottom=Side(style='thin')
                )
        
        ws.column_dimensions['A'].width = 25
        ws.column_dimensions['B'].width = 40

class DataFetcherApp:
    """Main Application with User-Friendly UI"""
    
    def __init__(self, root):
        self.root = root
        self.root.title("SCADA Data Fetcher - VPI Job Card Generator")
        self.root.geometry("1500x900")
        
        # Set application icon if available
        try:
            self.root.iconbitmap('icon.ico')
        except:
            pass
        
        # Database connection
        self.db = DatabaseManager()
        self.exporter = VPIExcelExporter()
        
        # Variables
        self.server_var = tk.StringVar(value="MAHESHWAGH\\WINCC")
        self.database_var = tk.StringVar(value="VPI1")
        self.selected_table_var = tk.StringVar()
        self.selected_date_column_var = tk.StringVar()
        
        # Export options
        self.include_ht_var = tk.BooleanVar(value=True)
        self.include_lt_var = tk.BooleanVar(value=True)
        
        # Column selection
        self.available_columns = []
        self.selected_columns = []
        self.column_checkboxes = {}
        
        # Logo
        self.logo_path = None
        self.logo_image = None
        self.logo_preview_label = None
        
        # Data storage
        self.current_data = None
        self.column_mapping = {}
        
        # Authentication
        self.auth_type = tk.StringVar(value="windows")
        self.username_var = tk.StringVar()
        self.password_var = tk.StringVar()
        
        # Custom WHERE clause
        self.where_clause_var = tk.StringVar()
        
        # Setup UI
        self.setup_ui()
        
        # Set default dates
        self.set_default_dates()
        
        # Bind window close event
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)
        
        # Load saved settings if available
        self.load_settings()
    
    def setup_ui(self):
        """Setup the user interface"""
        # Create a style for better looking widgets
        style = ttk.Style()
        try:
            style.theme_use('clam')
        except:
            pass
        
        # Configure styles
        style.configure('Title.TLabel', font=('Segoe UI', 16, 'bold'))
        style.configure('Header.TLabel', font=('Segoe UI', 11, 'bold'))
        style.configure('Accent.TButton', font=('Segoe UI', 10, 'bold'), padding=6)
        style.configure('Success.TLabel', foreground='green')
        style.configure('Error.TLabel', foreground='red')
        style.configure('Warning.TLabel', foreground='orange')
        
        # Main container with notebook for tabs
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(fill='both', expand=True, padx=5, pady=5)
        
        # Create tabs
        self.setup_connection_tab()
        self.setup_data_tab()
        self.setup_export_tab()
        self.setup_settings_tab()
        
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
        
        # Server info label
        self.server_info_label = ttk.Label(status_frame, text="")
        self.server_info_label.pack(anchor='w')
        
        # Available tables frame
        tables_frame = ttk.LabelFrame(main_frame, text="Available Tables", padding="10")
        tables_frame.grid(row=4, column=0, columnspan=3, sticky='nsew', pady=(0, 10))
        main_frame.rowconfigure(4, weight=1)
        main_frame.columnconfigure(0, weight=1)
        
        # Table list with scrollbar
        table_container = ttk.Frame(tables_frame)
        table_container.pack(fill='both', expand=True)
        
        self.table_listbox = tk.Listbox(table_container, height=15, selectmode='single',
                                       font=('Consolas', 9))
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
        ttk.Button(table_btn_frame, text="📊 View Table Info", 
                  command=self.view_table_info).pack(side=tk.LEFT, padx=5)
        
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
        
        self.selected_table_label = ttk.Label(table_frame, text="No table selected", 
                                            foreground="blue", font=('Segoe UI', 10))
        self.selected_table_label.grid(row=0, column=1, sticky=tk.W, pady=5, padx=(10, 0))
        
        # Date filter frame
        date_frame = ttk.LabelFrame(main_frame, text="Date Filter (Optional)", padding="10")
        date_frame.grid(row=2, column=0, columnspan=3, sticky='ew', pady=(0, 15))
        date_frame.columnconfigure(1, weight=1)
        
        # Date column selection
        ttk.Label(date_frame, text="Date Column:").grid(row=0, column=0, sticky=tk.W, pady=5)
        self.date_column_combo = ttk.Combobox(date_frame, textvariable=self.selected_date_column_var, 
                                             state='readonly', width=30)
        self.date_column_combo.grid(row=0, column=1, pady=5, padx=(5, 20), sticky='ew')
        
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
        
        # Custom WHERE clause frame
        where_frame = ttk.LabelFrame(main_frame, text="Custom WHERE Clause (Optional)", padding="10")
        where_frame.grid(row=3, column=0, columnspan=3, sticky='ew', pady=(0, 15))
        where_frame.columnconfigure(0, weight=1)
        
        ttk.Label(where_frame, text="WHERE Clause:").grid(row=0, column=0, sticky=tk.W, pady=5)
        where_entry = ttk.Entry(where_frame, textvariable=self.where_clause_var, width=50)
        where_entry.grid(row=0, column=1, pady=5, padx=(5, 0), sticky='ew')
        
        # Add a help button for WHERE clause syntax
        help_btn = ttk.Button(where_frame, text="?", width=3, 
                             command=self.show_where_clause_help)
        help_btn.grid(row=0, column=2, padx=(5, 0))
        
        ttk.Label(where_frame, 
                 text="Example: STATUS = 'COMPLETE' OR BATCH_NAME LIKE '%TEST%'").grid(
            row=1, column=0, columnspan=3, sticky=tk.W, pady=(0, 5))
        
        ttk.Label(where_frame, 
                 text="Note: Do NOT include the word 'WHERE'. Use SQL Server syntax without parameters.").grid(
            row=2, column=0, columnspan=3, sticky=tk.W, pady=(0, 5))
        
        # Column selection frame
        col_frame = ttk.LabelFrame(main_frame, text="Column Selection", padding="10")
        col_frame.grid(row=4, column=0, columnspan=2, sticky='nsew', pady=(0, 15), padx=(0, 10))
        main_frame.rowconfigure(4, weight=1)
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
        ttk.Button(btn_frame, text="Select Date Columns", 
                  command=self.select_date_columns, width=15).pack(side=tk.LEFT, padx=2)
        
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
        fetch_frame.grid(row=4, column=2, sticky='nsew', pady=(0, 15))
        
        # Row limit
        ttk.Label(fetch_frame, text="Row Limit:", font=('Segoe UI', 10, 'bold')).grid(
            row=0, column=0, sticky=tk.W, pady=10)
        
        self.fetch_limit_var = tk.StringVar(value="1000")
        limit_frame = ttk.Frame(fetch_frame)
        limit_frame.grid(row=0, column=1, pady=10, sticky='w')
        
        ttk.Entry(limit_frame, textvariable=self.fetch_limit_var, width=10).pack(side=tk.LEFT)
        ttk.Label(limit_frame, text=" rows (0 = all)").pack(side=tk.LEFT, padx=(5, 0))
        
        # Sample data checkbox
        self.sample_data_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(fetch_frame, text="Fetch sample data only (first 100 rows)", 
                       variable=self.sample_data_var, 
                       command=self.toggle_sample_data).grid(row=1, column=0, columnspan=2, 
                                                           sticky=tk.W, pady=5)
        
        # Fetch buttons
        ttk.Label(fetch_frame, text="Fetch Actions:", font=('Segoe UI', 10, 'bold')).grid(
            row=2, column=0, sticky=tk.W, pady=(20, 10))
        
        self.fetch_btn = ttk.Button(fetch_frame, text="🚀 FETCH DATA NOW", 
                                   command=self.fetch_data, width=25, style='Accent.TButton',
                                   state='disabled')
        self.fetch_btn.grid(row=3, column=0, columnspan=2, pady=10)
        
        self.fetch_all_btn = ttk.Button(fetch_frame, text="📊 Fetch All Data (No Limit)", 
                                       command=self.fetch_all_data, width=25, state='disabled')
        self.fetch_all_btn.grid(row=4, column=0, columnspan=2, pady=5)
        
        self.preview_btn = ttk.Button(fetch_frame, text="👁️ Preview First 10 Rows", 
                                     command=self.preview_data, width=25, state='disabled')
        self.preview_btn.grid(row=5, column=0, columnspan=2, pady=5)
        
        # Progress display
        self.progress_label = ttk.Label(fetch_frame, text="Select a table and columns first")
        self.progress_label.grid(row=6, column=0, columnspan=2, pady=(20, 0))
        
        # Data preview frame
        preview_frame = ttk.LabelFrame(main_frame, text="Data Preview", padding="10")
        preview_frame.grid(row=5, column=0, columnspan=3, sticky='nsew', pady=(0, 10))
        main_frame.rowconfigure(5, weight=2)
        
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
        
        # Data info label
        self.data_info_label = ttk.Label(preview_frame, text="No data loaded")
        self.data_info_label.pack(side=tk.BOTTOM, fill='x', pady=(5, 0))
    
    def setup_export_tab(self):
        """Setup Export Tab with Column Mapping"""
        export_tab = ttk.Frame(self.notebook)
        self.notebook.add(export_tab, text="💾 Export to VPI Job Card")
        
        # Main frame with padding
        main_frame = ttk.Frame(export_tab, padding="20")
        main_frame.pack(fill='both', expand=True)
        
        # Title
        ttk.Label(main_frame, text="VPI Job Card Export with Data Mapping", 
                 style='Title.TLabel').pack(pady=(0, 20))
        
        # Export options frame
        options_frame = ttk.LabelFrame(main_frame, text="Export Settings", padding="20")
        options_frame.pack(fill='both', expand=True, pady=(0, 20))
        
        # Sheet selection
        sheet_frame = ttk.Frame(options_frame)
        sheet_frame.pack(fill='x', pady=(0, 20))
        
        ttk.Label(sheet_frame, text="Include Sheets:", 
                 font=('Segoe UI', 10, 'bold')).pack(anchor='w', pady=(0, 5))
        
        ttk.Checkbutton(sheet_frame, text="HT VPI Sheet", 
                       variable=self.include_ht_var).pack(side=tk.LEFT, padx=(0, 20))
        ttk.Checkbutton(sheet_frame, text="LT VPI Sheet", 
                       variable=self.include_lt_var).pack(side=tk.LEFT)
        
        # Logo upload section
        logo_frame = ttk.Frame(options_frame)
        logo_frame.pack(fill='x', pady=(0, 20))
        
        ttk.Label(logo_frame, text="Company Logo:", 
                 font=('Segoe UI', 10, 'bold')).pack(anchor='w', pady=(0, 5))
        
        logo_btn_frame = ttk.Frame(logo_frame)
        logo_btn_frame.pack(fill='x', pady=(0, 10))
        
        ttk.Button(logo_btn_frame, text="🖼️ Upload Logo", 
                  command=self.upload_logo, width=15).pack(side=tk.LEFT, padx=(0, 10))
        ttk.Button(logo_btn_frame, text="🗑️ Remove Logo", 
                  command=self.remove_logo, width=15).pack(side=tk.LEFT)
        
        self.logo_status_label = ttk.Label(logo_btn_frame, text="No logo selected", 
                                          foreground="gray")
        self.logo_status_label.pack(side=tk.LEFT, padx=(10, 0))
        
        # Logo preview frame
        self.logo_preview_frame = ttk.Frame(logo_frame, height=100, relief=tk.SUNKEN, borderwidth=1)
        self.logo_preview_frame.pack(fill='x', pady=(5, 0))
        self.logo_preview_frame.pack_propagate(False)
        
        # Column mapping section
        mapping_frame = ttk.LabelFrame(options_frame, text="Data Mapping", padding="10")
        mapping_frame.pack(fill='x', pady=(0, 20))
        
        ttk.Label(mapping_frame, text="Map database columns to Excel fields:", 
                 font=('Segoe UI', 10, 'bold')).pack(anchor='w', pady=(0, 10))
        
        ttk.Label(mapping_frame, 
                 text="Before exporting, map your database columns to the VPI Job Card fields.").pack(anchor='w')
        
        self.mapping_status_label = ttk.Label(mapping_frame, text="No mapping defined", 
                                             foreground="orange", font=('Segoe UI', 10))
        self.mapping_status_label.pack(anchor='w', pady=(5, 0))
        
        mapping_btn_frame = ttk.Frame(mapping_frame)
        mapping_btn_frame.pack(fill='x', pady=(10, 0))
        
        ttk.Button(mapping_btn_frame, text="🗺️ Configure Column Mapping", 
                  command=self.configure_column_mapping, width=25).pack(side=tk.LEFT, padx=(0, 10))
        ttk.Button(mapping_btn_frame, text="🔄 Clear Mapping", 
                  command=self.clear_column_mapping, width=15).pack(side=tk.LEFT)
        ttk.Button(mapping_btn_frame, text="💾 Save Mapping", 
                  command=self.save_mapping, width=15).pack(side=tk.LEFT, padx=(10, 0))
        ttk.Button(mapping_btn_frame, text="📂 Load Mapping", 
                  command=self.load_mapping, width=15).pack(side=tk.LEFT, padx=(10, 0))
        
        # Export section
        export_btn_frame = ttk.Frame(options_frame)
        export_btn_frame.pack(fill='x', pady=(0, 20))
        
        ttk.Label(export_btn_frame, text="Export VPI Job Card:", 
                 font=('Segoe UI', 12, 'bold')).pack(anchor='w', pady=(0, 10))
        ttk.Label(export_btn_frame, 
                 text="Create VPI Job Card with actual data from database").pack(anchor='w')
        
        self.export_btn = ttk.Button(export_btn_frame, text="📥 Export to VPI Job Card", 
                                    command=self.export_to_vpi_with_data, width=30, 
                                    style='Accent.TButton', state='disabled')
        self.export_btn.pack(pady=10)
        
        ttk.Button(export_btn_frame, text="📋 Export Raw Data to CSV", 
                  command=self.export_to_csv, width=20).pack(pady=(0, 10))
        
        # Current mapping display
        mapping_display_frame = ttk.LabelFrame(options_frame, text="Current Column Mapping", padding="10")
        mapping_display_frame.pack(fill='both', expand=True, pady=(0, 20))
        
        self.mapping_display_text = scrolledtext.ScrolledText(mapping_display_frame, 
                                                            height=10, wrap=tk.WORD,
                                                            font=('Consolas', 9))
        self.mapping_display_text.pack(fill='both', expand=True)
        
        # Template info
        info_frame = ttk.Frame(options_frame)
        info_frame.pack(fill='x')
        
        ttk.Label(info_frame, text="ℹ️ Excel will contain:", 
                 font=('Segoe UI', 10, 'bold')).pack(anchor='w')
        ttk.Label(info_frame, text="• HT VPI Sheet - With your data in proper positions").pack(anchor='w')
        ttk.Label(info_frame, text="• LT VPI Sheet - With your data in proper positions").pack(anchor='w')
        ttk.Label(info_frame, text="• Raw Data Sheet - All fetched data for reference").pack(anchor='w')
        ttk.Label(info_frame, text="• Mapping Sheet - Column mapping information").pack(anchor='w')
        ttk.Label(info_frame, text="• Summary Sheet - Export information").pack(anchor='w')
        ttk.Label(info_frame, text="• Your company logo at the top (if provided)").pack(anchor='w')
        
        # Log frame
        log_frame = ttk.LabelFrame(main_frame, text="Export Log", padding="10")
        log_frame.pack(fill='both', expand=True)
        
        self.log_text = scrolledtext.ScrolledText(log_frame, height=8, wrap=tk.WORD,
                                                 font=('Consolas', 9))
        self.log_text.pack(fill='both', expand=True)
        
        # Configure log tags
        self.log_text.tag_configure('success', foreground='green', font=('Consolas', 9, 'bold'))
        self.log_text.tag_configure('error', foreground='red', font=('Consolas', 9, 'bold'))
        self.log_text.tag_configure('info', foreground='blue', font=('Consolas', 9))
        self.log_text.tag_configure('warning', foreground='orange', font=('Consolas', 9))
        self.log_text.tag_configure('header', font=('Consolas', 9, 'bold'))
    
    def setup_settings_tab(self):
        """Setup Settings Tab"""
        settings_tab = ttk.Frame(self.notebook)
        self.notebook.add(settings_tab, text="⚙️ Settings")
        
        main_frame = ttk.Frame(settings_tab, padding="20")
        main_frame.pack(fill='both', expand=True)
        
        ttk.Label(main_frame, text="Application Settings", 
                 style='Title.TLabel').pack(pady=(0, 20))
        
        # Settings frame
        settings_frame = ttk.LabelFrame(main_frame, text="Settings", padding="20")
        settings_frame.pack(fill='both', expand=True)
        
        # Save settings button
        ttk.Button(settings_frame, text="💾 Save Current Settings", 
                  command=self.save_settings, width=20).pack(pady=10)
        
        ttk.Button(settings_frame, text="🗑️ Clear All Settings", 
                  command=self.clear_settings, width=20).pack(pady=10)
        
        ttk.Button(settings_frame, text="📊 View Log File", 
                  command=self.view_log_file, width=20).pack(pady=10)
        
        ttk.Button(settings_frame, text="🔄 Reset Application", 
                  command=self.reset_application, width=20).pack(pady=10)
        
        # About section
        about_frame = ttk.LabelFrame(main_frame, text="About", padding="20")
        about_frame.pack(fill='x', pady=(20, 0))
        
        ttk.Label(about_frame, text="SCADA Data Fetcher - VPI Job Card Generator", 
                 font=('Segoe UI', 11, 'bold')).pack()
        ttk.Label(about_frame, text="Version 2.0").pack()
        ttk.Label(about_frame, text="Developed for VPI Process Monitoring").pack(pady=(10, 0))
        ttk.Label(about_frame, text="© 2024 All Rights Reserved").pack()
        
        # System info
        sys_frame = ttk.LabelFrame(main_frame, text="System Information", padding="10")
        sys_frame.pack(fill='x', pady=(20, 0))
        
        import platform
        import sys
        
        sys_info = f"""
        Python Version: {sys.version.split()[0]}
        Operating System: {platform.system()} {platform.release()}
        Platform: {platform.platform()}
        Processor: {platform.processor()}
        Machine: {platform.machine()}
        """
        
        ttk.Label(sys_frame, text=sys_info.strip(), justify=tk.LEFT).pack(anchor='w')
    
    def show_where_clause_help(self):
        """Show help for WHERE clause syntax"""
        help_text = """WHERE Clause Syntax Help:

Basic Examples:
----------------
STATUS = 'COMPLETE'
BATCH_NAME LIKE '%TEST%'
OPERATOR_NAME = 'John Doe'
DATA1 IS NOT NULL

Comparison Operators:
---------------------
=       Equal
<> or != Not equal
>       Greater than
<       Less than
>=      Greater than or equal
<=      Less than or equal

Logical Operators:
------------------
AND     Both conditions must be true
OR      Either condition must be true
NOT     Negates a condition

Pattern Matching:
-----------------
LIKE 'A%'     Starts with A
LIKE '%A'     Ends with A
LIKE '%A%'    Contains A
LIKE 'A_C'    A, any character, C

NULL Checks:
------------
IS NULL
IS NOT NULL

Date Comparisons:
-----------------
PRO_START_TIME > '2024-01-01'
PRO_START_TIME BETWEEN '2024-01-01' AND '2024-12-31'

Complex Examples:
-----------------
(STATUS = 'COMPLETE' AND OPERATOR_NAME = 'John') OR BATCH_NAME LIKE '%URGENT%'
NOT (DATA1 IS NULL OR DATA2 = '')
PRO_START_TIME >= '2024-01-01' AND PRO_STOP_TIME IS NOT NULL

Important Notes:
----------------
1. Do NOT include the word 'WHERE'
2. Use single quotes for string values
3. Use standard SQL Server syntax
4. No parameter placeholders (@param or ?)
5. Column names should match your database exactly
"""
        
        dialog = tk.Toplevel(self.root)
        dialog.title("WHERE Clause Help")
        dialog.geometry("600x500")
        dialog.transient(self.root)
        dialog.grab_set()
        
        # Center dialog
        dialog.update_idletasks()
        x = (dialog.winfo_screenwidth() - dialog.winfo_width()) // 2
        y = (dialog.winfo_screenheight() - dialog.winfo_height()) // 2
        dialog.geometry(f"+{x}+{y}")
        
        # Text widget with help
        text_widget = scrolledtext.ScrolledText(dialog, wrap=tk.WORD, 
                                               font=('Consolas', 9))
        text_widget.pack(fill='both', expand=True, padx=10, pady=10)
        text_widget.insert(1.0, help_text)
        text_widget.config(state='disabled')
        
        # Close button
        ttk.Button(dialog, text="Close", command=dialog.destroy).pack(pady=10)
    
    def upload_logo(self):
        """Upload company logo"""
        filetypes = [("Image files", "*.png *.jpg *.jpeg *.bmp *.gif"), ("All files", "*.*")]
        filename = filedialog.askopenfilename(title="Select Company Logo", filetypes=filetypes)
        
        if filename:
            try:
                self.logo_path = filename
                
                # Load and resize image for preview
                img = Image.open(filename)
                img.thumbnail((150, 150), Image.Resampling.LANCZOS)
                photo = ImageTk.PhotoImage(img)
                
                # Clear previous preview
                for widget in self.logo_preview_frame.winfo_children():
                    widget.destroy()
                
                # Create new preview label
                self.logo_preview_label = ttk.Label(self.logo_preview_frame, image=photo)
                self.logo_preview_label.image = photo
                self.logo_preview_label.pack(pady=10)
                
                self.logo_status_label.config(text=os.path.basename(filename), 
                                            foreground="green")
                self.log_message(f"✅ Logo uploaded: {os.path.basename(filename)}", 'success')
            except Exception as e:
                self.log_message(f"❌ Failed to load logo: {str(e)}", 'error')
                messagebox.showerror("Logo Error", f"Failed to load logo:\n{str(e)}")
    
    def remove_logo(self):
        """Remove uploaded logo"""
        self.logo_path = None
        self.logo_image = None
        
        # Clear preview
        for widget in self.logo_preview_frame.winfo_children():
            widget.destroy()
        
        self.logo_status_label.config(text="No logo selected", foreground="gray")
        self.log_message("Logo removed", 'info')
    
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
    
    def toggle_sample_data(self):
        """Toggle sample data mode"""
        if self.sample_data_var.get():
            self.fetch_limit_var.set("100")
        else:
            self.fetch_limit_var.set("1000")
    
    def log_message(self, message: str, message_type: str = 'info'):
        """Add message to log"""
        timestamp = datetime.now().strftime('%H:%M:%S')
        self.log_text.insert(tk.END, f"[{timestamp}] {message}\n")
        
        # Apply tag to the last line
        start_index = self.log_text.index(f"end-{len(message.split(chr(10)))+1}c")
        end_index = self.log_text.index("end-1c")
        self.log_text.tag_add(message_type, start_index, end_index)
        
        self.log_text.see(tk.END)
    
    def test_connection(self):
        """Test database connection"""
        def test():
            self.status_bar.config(text="Testing connection...")
            self.status_label.config(text="Testing...", foreground="orange")
            
            try:
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
                    self.server_info_label.config(
                        text=f"Connected to {self.db.server_info.get('server', 'Unknown')} - {self.db.server_info.get('database', 'Unknown')}")
                    self.status_bar.config(text="Connection test successful")
                    
                    # Disconnect after test
                    self.db.disconnect()
                    
                    messagebox.showinfo("Connection Test", "✅ Connection successful!")
                else:
                    self.status_label.config(text="Test: Failed", foreground="red")
                    self.server_info_label.config(text="")
                    self.status_bar.config(text=f"Connection test failed: {message}")
                    messagebox.showerror("Connection Test", f"❌ Connection failed:\n{message}")
                    
            except Exception as e:
                self.status_label.config(text="Test: Error", foreground="red")
                self.status_bar.config(text=f"Error: {str(e)}")
                messagebox.showerror("Connection Test", f"❌ Error during connection test:\n{str(e)}")
        
        threading.Thread(target=test, daemon=True).start()
    
    def connect_db(self):
        """Connect to database"""
        def connect():
            self.status_bar.config(text="Connecting...")
            self.status_label.config(text="Connecting...", foreground="orange")
            self.connect_btn.config(state='disabled')
            self.test_btn.config(state='disabled')
            
            try:
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
                    self.server_info_label.config(
                        text=f"Connected to {self.db.server_info.get('server', 'Unknown')} - {self.db.server_info.get('database', 'Unknown')}")
                    self.status_bar.config(text="Connected successfully")
                    self.connect_btn.config(state='disabled')
                    self.disconnect_btn.config(state='normal')
                    self.log_message("Database connected successfully", 'success')
                    self.refresh_tables()
                else:
                    self.status_label.config(text="Connection Failed", foreground="red")
                    self.server_info_label.config(text="")
                    self.status_bar.config(text=f"Connection failed: {message}")
                    self.connect_btn.config(state='normal')
                    self.test_btn.config(state='normal')
                    self.log_message(f"Connection failed: {message}", 'error')
                    messagebox.showerror("Connection Error", f"Failed to connect:\n{message}")
                    
            except Exception as e:
                self.status_label.config(text="Connection Error", foreground="red")
                self.status_bar.config(text=f"Error: {str(e)}")
                self.connect_btn.config(state='normal')
                self.test_btn.config(state='normal')
                self.log_message(f"Connection error: {str(e)}", 'error')
                messagebox.showerror("Connection Error", f"Error during connection:\n{str(e)}")
        
        threading.Thread(target=connect, daemon=True).start()
    
    def disconnect_db(self):
        """Disconnect from database"""
        try:
            self.db.disconnect()
            self.status_label.config(text="Disconnected", foreground="red")
            self.server_info_label.config(text="")
            self.status_bar.config(text="Disconnected")
            self.connect_btn.config(state='normal')
            self.test_btn.config(state='normal')
            self.disconnect_btn.config(state='disabled')
            self.log_message("Disconnected from database", 'info')
            
            # Clear UI
            self.table_listbox.delete(0, tk.END)
            self.selected_table_label.config(text="No table selected")
            self.date_column_combo.set('')
            self.date_column_combo['values'] = []
            self.clear_column_checkboxes()
            self.clear_data_tree()
            self.mapping_display_text.delete(1.0, tk.END)
            self.export_btn.config(state='disabled')
            self.fetch_btn.config(state='disabled')
            self.fetch_all_btn.config(state='disabled')
            self.preview_btn.config(state='disabled')
            
        except Exception as e:
            self.log_message(f"Error during disconnect: {str(e)}", 'error')
    
    def refresh_tables(self):
        """Refresh list of tables"""
        if not self.db.connected:
            self.log_message("Not connected to database", 'error')
            return
        
        def refresh():
            self.status_bar.config(text="Loading tables...")
            
            try:
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
                    
            except Exception as e:
                self.status_bar.config(text=f"Error loading tables: {str(e)}")
                self.log_message(f"Error loading tables: {str(e)}", 'error')
        
        threading.Thread(target=refresh, daemon=True).start()
    
    def view_table_info(self):
        """View information about selected table"""
        selection = self.table_listbox.curselection()
        if not selection:
            messagebox.showwarning("Selection", "Please select a table from the list")
            return
        
        idx = selection[0]
        table_name = self.table_listbox.get(idx)
        
        # Remove row count info
        if '(' in table_name:
            table_name = table_name.split('(')[0].strip()
        
        def get_info():
            try:
                # Get column information
                columns = self.db.get_table_columns(table_name)
                
                # Get row count
                if '.' in table_name:
                    schema, table = table_name.split('.')
                    query = f"SELECT COUNT(*) FROM [{schema}].[{table}]"
                else:
                    query = f"SELECT COUNT(*) FROM [{table_name}]"
                
                self.db.cursor.execute(query)
                row_count = self.db.cursor.fetchone()[0]
                
                # Create info dialog
                self.root.after(0, lambda: self.show_table_info(table_name, columns, row_count))
                
            except Exception as e:
                self.root.after(0, lambda: messagebox.showerror("Error", 
                    f"Failed to get table info:\n{str(e)}"))
        
        threading.Thread(target=get_info, daemon=True).start()
    
    def show_table_info(self, table_name: str, columns: List[str], row_count: int):
        """Show table information dialog"""
        dialog = tk.Toplevel(self.root)
        dialog.title(f"Table Information: {table_name}")
        dialog.geometry("600x400")
        dialog.transient(self.root)
        dialog.grab_set()
        
        # Center dialog
        dialog.update_idletasks()
        x = (dialog.winfo_screenwidth() - dialog.winfo_width()) // 2
        y = (dialog.winfo_screenheight() - dialog.winfo_height()) // 2
        dialog.geometry(f"+{x}+{y}")
        
        # Title
        ttk.Label(dialog, text=f"Table: {table_name}", 
                 font=('Segoe UI', 12, 'bold')).pack(pady=(10, 5))
        ttk.Label(dialog, text=f"Total Rows: {row_count:,}").pack(pady=(0, 10))
        
        # Columns frame
        columns_frame = ttk.LabelFrame(dialog, text="Columns", padding="10")
        columns_frame.pack(fill='both', expand=True, padx=10, pady=5)
        
        # Treeview for columns
        tree_frame = ttk.Frame(columns_frame)
        tree_frame.pack(fill='both', expand=True)
        
        tree = ttk.Treeview(tree_frame, columns=('column', 'type'), show='headings', height=15)
        tree.heading('column', text='Column Name')
        tree.heading('type', text='Data Type')
        tree.column('column', width=250)
        tree.column('type', width=200)
        
        scrollbar = ttk.Scrollbar(tree_frame, orient='vertical', command=tree.yview)
        tree.configure(yscrollcommand=scrollbar.set)
        
        tree.pack(side=tk.LEFT, fill='both', expand=True)
        scrollbar.pack(side=tk.RIGHT, fill='y')
        
        # Add columns to tree
        for col in columns:
            # Parse column info
            if '(' in col:
                col_name = col.split('(')[0].strip()
                col_type = col[col.find('('):].strip()
            else:
                col_name = col
                col_type = "Unknown"
            
            tree.insert('', 'end', values=(col_name, col_type))
        
        # Close button
        ttk.Button(dialog, text="Close", command=dialog.destroy, 
                  width=15).pack(pady=10)
    
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
    
    def load_table_columns(self, table_name: str):
        """Load columns for selected table"""
        def load():
            self.status_bar.config(text=f"Loading columns for {table_name}...")
            
            try:
                columns = self.db.get_table_columns(table_name)
                self.available_columns = columns
                
                # Update date column combo
                self.date_column_combo['values'] = columns
                
                # Try to auto-select a date column
                date_columns = [col for col in columns 
                              if any(x in col.lower() for x in ['date', 'time', 'timestamp', 'created', 'modified'])]
                if date_columns:
                    self.selected_date_column_var.set(date_columns[0])
                elif columns:
                    self.selected_date_column_var.set(columns[0])
                
                # Create column checkboxes
                self.create_column_checkboxes(columns)
                
                # Enable fetch buttons
                self.fetch_btn.config(state='normal')
                self.fetch_all_btn.config(state='normal')
                self.preview_btn.config(state='normal')
                
                self.status_bar.config(text=f"Loaded {len(columns)} columns for {table_name}")
                self.log_message(f"Loaded {len(columns)} columns from {table_name}", 'success')
                
            except Exception as e:
                self.status_bar.config(text=f"Error loading columns: {str(e)}")
                self.log_message(f"Error loading columns: {str(e)}", 'error')
        
        threading.Thread(target=load, daemon=True).start()
    
    def create_column_checkboxes(self, columns: List[str]):
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
        """Update the selected columns list"""
        self.selected_columns = [col for col, var in self.column_checkboxes.items() if var.get()]
    
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
    
    def select_date_columns(self):
        """Select only date-related columns"""
        for col, var in self.column_checkboxes.items():
            col_lower = col.lower()
            if any(x in col_lower for x in ['date', 'time', 'timestamp']):
                var.set(True)
            else:
                var.set(False)
        self.update_selected_columns()
        self.log_message("Selected date-related columns", 'info')
    
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
        
        # Validate WHERE clause if provided
        where_clause = self.where_clause_var.get().strip()
        if where_clause:
            # Check for common issues
            if where_clause.lower().startswith('where'):
                messagebox.showwarning("WHERE Clause Warning", 
                                     "Do NOT include the word 'WHERE' in the WHERE clause.\n"
                                     "Just enter the condition itself.\n\n"
                                     "Example: STATUS = 'COMPLETE'")
                return
            
            # Check for parameter placeholders
            if '?' in where_clause or '@' in where_clause:
                messagebox.showwarning("WHERE Clause Warning",
                                     "Do not use parameter placeholders (? or @param) in the WHERE clause.\n"
                                     "Use direct values instead.\n\n"
                                     "Example: STATUS = 'COMPLETE' instead of STATUS = ?")
                return
        
        def fetch():
            self.status_bar.config(text="Fetching data...")
            self.progress_label.config(text="Fetching data...")
            self.fetch_btn.config(state='disabled')
            self.fetch_all_btn.config(state='disabled')
            self.preview_btn.config(state='disabled')
            
            try:
                # Get parameters
                date_column = self.selected_date_column_var.get()
                start_date = self.start_date_entry.get_date().strftime('%Y-%m-%d')
                end_date = self.end_date_entry.get_date().strftime('%Y-%m-%d')
                
                # Get limit
                try:
                    limit = int(self.fetch_limit_var.get())
                except:
                    limit = 1000
                
                # Get WHERE clause
                where_clause = self.where_clause_var.get().strip()
                if not where_clause:
                    where_clause = None
                
                # Fetch data
                result = self.db.fetch_data(
                    table_name=table_name,
                    date_column=date_column if date_column else None,
                    start_date=start_date if date_column else None,
                    end_date=end_date if date_column else None,
                    selected_columns=self.selected_columns,
                    limit=limit,
                    where_clause=where_clause
                )
                
                if result['success']:
                    self.current_data = result
                    self.display_data(result)
                    
                    # Update UI
                    row_count = result['row_count']
                    total_count = result.get('total_count', row_count)
                    self.progress_label.config(text=f"✅ Fetched {row_count} rows successfully")
                    self.status_bar.config(text=f"Fetched {row_count} rows (out of {total_count} total)")
                    
                    filter_info = ""
                    if date_column:
                        filter_info = f" with date filter ({start_date} to {end_date})"
                    if where_clause:
                        filter_info += f" with custom WHERE clause"
                    
                    self.log_message(f"✅ Fetched {row_count} rows{filter_info}", 'success')
                    self.log_message(f"📊 Total available rows: {total_count}", 'info')
                    
                    # Enable export if we have data
                    if row_count > 0:
                        self.export_btn.config(state='normal')
                    
                else:
                    self.progress_label.config(text="❌ Fetch failed")
                    self.status_bar.config(text=f"Error: {result['error']}")
                    self.log_message(f"❌ Fetch failed: {result['error']}", 'error')
                    messagebox.showerror("Fetch Error", f"Failed to fetch data:\n{result['error']}")
                
            except Exception as e:
                self.progress_label.config(text="❌ Fetch error")
                self.status_bar.config(text=f"Error: {str(e)}")
                self.log_message(f"❌ Fetch error: {str(e)}", 'error')
                messagebox.showerror("Fetch Error", f"Error during fetch:\n{str(e)}")
            
            finally:
                self.fetch_btn.config(state='normal')
                self.fetch_all_btn.config(state='normal')
                self.preview_btn.config(state='normal')
        
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
    
    def preview_data(self):
        """Preview first 10 rows of data"""
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
        
        # Save current limit
        original_limit = self.fetch_limit_var.get()
        
        # Set limit to 10 for preview
        self.fetch_limit_var.set("10")
        
        def preview():
            self.status_bar.config(text="Previewing data...")
            self.progress_label.config(text="Previewing first 10 rows...")
            self.preview_btn.config(state='disabled')
            
            try:
                # Get parameters
                date_column = self.selected_date_column_var.get()
                start_date = self.start_date_entry.get_date().strftime('%Y-%m-%d')
                end_date = self.end_date_entry.get_date().strftime('%Y-%m-%d')
                
                # Fetch data
                result = self.db.fetch_data(
                    table_name=table_name,
                    date_column=date_column if date_column else None,
                    start_date=start_date if date_column else None,
                    end_date=end_date if date_column else None,
                    selected_columns=self.selected_columns,
                    limit=10
                )
                
                if result['success']:
                    self.current_data = result
                    self.display_data(result)
                    
                    row_count = result['row_count']
                    self.progress_label.config(text=f"✅ Previewed {row_count} rows")
                    self.status_bar.config(text=f"Previewed {row_count} rows")
                    self.log_message(f"✅ Previewed {row_count} rows", 'success')
                    
                else:
                    self.progress_label.config(text="❌ Preview failed")
                    self.status_bar.config(text=f"Error: {result['error']}")
                    self.log_message(f"❌ Preview failed: {result['error']}", 'error')
                
            except Exception as e:
                self.progress_label.config(text="❌ Preview error")
                self.status_bar.config(text=f"Error: {str(e)}")
                self.log_message(f"❌ Preview error: {str(e)}", 'error')
            
            finally:
                # Restore original limit
                self.fetch_limit_var.set(original_limit)
                self.preview_btn.config(state='normal')
        
        threading.Thread(target=preview, daemon=True).start()
    
    def display_data(self, data: Dict):
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
            # Initial width, will be adjusted
            self.data_tree.column(col, width=100, minwidth=50, anchor=tk.W, stretch=False)
        
        # Insert data (limit to 1000 rows for performance)
        display_rows = min(1000, len(data['data']))
        for i, row in enumerate(data['data'][:display_rows]):
            values = []
            for col in columns:
                value = row.get(col, '')
                if value is None:
                    values.append("")
                elif isinstance(value, (datetime, pd.Timestamp)):
                    values.append(value.strftime('%Y-%m-%d %H:%M:%S'))
                elif isinstance(value, (int, float)):
                    values.append(str(value))
                else:
                    # Truncate long strings
                    str_value = str(value)
                    if len(str_value) > 100:
                        str_value = str_value[:97] + "..."
                    values.append(str_value)
            self.data_tree.insert('', 'end', values=values)
        
        # Auto-size columns
        self.auto_size_columns()
        
        # Update info label
        row_count = len(data['data'])
        display_count = display_rows if row_count > display_rows else row_count
        total_count = data.get('total_count', row_count)
        
        info_text = f"Showing {display_count} of {row_count} fetched rows"
        if total_count > row_count:
            info_text += f" (out of {total_count} total rows)"
        
        self.data_info_label.config(text=info_text)
        
        if row_count > display_rows:
            self.log_message(f"Displaying first {display_rows} of {row_count} rows", 'info')
    
    def auto_size_columns(self):
        """Auto-size treeview columns"""
        for col in self.data_tree['columns']:
            max_len = len(col)
            for item in self.data_tree.get_children():
                value = self.data_tree.set(item, col)
                if value and len(value) > max_len:
                    max_len = len(value)
            # Set width with some padding, but limit maximum
            self.data_tree.column(col, width=min(max_len * 8, 400))
    
    def clear_data_tree(self):
        """Clear the data treeview"""
        for item in self.data_tree.get_children():
            self.data_tree.delete(item)
        self.data_info_label.config(text="No data loaded")
    
    def configure_column_mapping(self):
        """Open column mapping dialog"""
        if not self.current_data or not self.current_data['columns']:
            messagebox.showwarning("No Data", "Please fetch data first before configuring mapping")
            return
        
        # Get raw column names (without type info)
        raw_columns = self.db.get_raw_column_names(self.selected_table_var.get())
        if not raw_columns:
            raw_columns = self.current_data['columns']
        
        # Get Excel fields that need mapping
        excel_fields = self.exporter.get_excel_fields()
        
        # Open mapping dialog
        dialog = ColumnMappingDialog(self.root, raw_columns, excel_fields)
        self.root.wait_window(dialog.dialog)
        
        # Get mapping result
        mapping = dialog.get_mapping()
        if mapping:
            self.column_mapping = mapping
            self.update_mapping_display()
            self.export_btn.config(state='normal')
            self.log_message("✅ Column mapping configured", 'success')
            self.log_message(f"Mapped {len(mapping)} fields", 'info')
    
    def clear_column_mapping(self):
        """Clear column mapping"""
        self.column_mapping = {}
        self.update_mapping_display()
        self.export_btn.config(state='disabled')
        self.log_message("Column mapping cleared", 'info')
    
    def save_mapping(self):
        """Save column mapping to file"""
        if not self.column_mapping:
            messagebox.showwarning("No Mapping", "No column mapping to save")
            return
        
        file_path = filedialog.asksaveasfilename(
            defaultextension=".json",
            filetypes=[("JSON files", "*.json"), ("All files", "*.*")],
            initialfile="column_mapping.json"
        )
        
        if file_path:
            try:
                # Get table name without row count
                table_name = self.selected_table_var.get()
                if '(' in table_name:
                    table_name = table_name.split('(')[0].strip()
                
                mapping_data = {
                    'table_name': table_name,
                    'timestamp': datetime.now().isoformat(),
                    'mapping': self.column_mapping
                }
                
                with open(file_path, 'w') as f:
                    json.dump(mapping_data, f, indent=2)
                
                self.log_message(f"✅ Mapping saved to {os.path.basename(file_path)}", 'success')
                
            except Exception as e:
                self.log_message(f"❌ Failed to save mapping: {str(e)}", 'error')
                messagebox.showerror("Save Error", f"Failed to save mapping:\n{str(e)}")
    
    def load_mapping(self):
        """Load column mapping from file"""
        file_path = filedialog.askopenfilename(
            filetypes=[("JSON files", "*.json"), ("All files", "*.*")],
            title="Load Column Mapping"
        )
        
        if file_path:
            try:
                with open(file_path, 'r') as f:
                    mapping_data = json.load(f)
                
                self.column_mapping = mapping_data.get('mapping', {})
                self.update_mapping_display()
                self.export_btn.config(state='normal' if self.column_mapping else 'disabled')
                
                self.log_message(f"✅ Mapping loaded from {os.path.basename(file_path)}", 'success')
                self.log_message(f"Loaded {len(self.column_mapping)} mappings", 'info')
                
            except Exception as e:
                self.log_message(f"❌ Failed to load mapping: {str(e)}", 'error')
                messagebox.showerror("Load Error", f"Failed to load mapping:\n{str(e)}")
    
    def update_mapping_display(self):
        """Update the mapping display text"""
        self.mapping_display_text.delete(1.0, tk.END)
        
        if not self.column_mapping:
            self.mapping_display_text.insert(1.0, 
                "No column mapping defined.\n\n"
                "Click 'Configure Column Mapping' to set up mappings.")
            self.mapping_status_label.config(text="No mapping defined", 
                                           foreground="orange")
            return
        
        # Group mappings by field type
        header_fields = []
        table_fields = []
        
        # Get Excel field info
        excel_fields = {field['name']: field for field in self.exporter.get_excel_fields()}
        
        for excel_field, db_column in self.column_mapping.items():
            field_info = excel_fields.get(excel_field, {})
            if field_info.get('is_table_column', False):
                table_fields.append((excel_field, db_column))
            else:
                header_fields.append((excel_field, db_column))
        
        # Add header
        self.mapping_display_text.insert(tk.END, 
            f"Column Mapping Summary\n{'-'*40}\n\n", 'header')
        
        # Add header fields
        if header_fields:
            self.mapping_display_text.insert(tk.END, "Header Fields:\n", 'header')
            for excel_field, db_column in sorted(header_fields):
                self.mapping_display_text.insert(tk.END, f"  • {excel_field} → {db_column}\n")
            self.mapping_display_text.insert(tk.END, "\n")
        
        # Add table fields
        if table_fields:
            self.mapping_display_text.insert(tk.END, "Table Columns:\n", 'header')
            for excel_field, db_column in sorted(table_fields):
                self.mapping_display_text.insert(tk.END, f"  • {excel_field} → {db_column}\n")
        
        # Configure tags
        self.mapping_display_text.tag_configure('header', 
                                              font=('Consolas', 10, 'bold'))
        
        self.mapping_status_label.config(text=f"{len(self.column_mapping)} mappings defined", 
                                        foreground="green")
    
    def export_to_vpi_with_data(self):
        """Export data to VPI Job Card Excel format with actual data"""
        if not self.current_data or not self.current_data['data']:
            self.log_message("No data to export", 'error')
            return
        
        if not self.column_mapping:
            messagebox.showwarning("No Mapping", 
                                 "Please configure column mapping before exporting")
            return
        
        # Check if at least one sheet is selected
        if not self.include_ht_var.get() and not self.include_lt_var.get():
            messagebox.showwarning("No Sheets Selected", 
                                 "Please select at least one sheet (HT VPI or LT VPI) to export")
            return
        
        table_name = self.selected_table_var.get()
        if '(' in table_name:
            table_name = table_name.split('(')[0].strip()
        
        # Generate filename
        date_str = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"VPI_JobCard_{table_name}_{date_str}.xlsx"
        
        file_path = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")],
            initialfile=filename
        )
        
        if not file_path:
            return
        
        def export():
            self.status_bar.config(text="Creating VPI Job Card with data...")
            self.log_message("Creating VPI Job Card Excel with actual data...", 'info')
            
            try:
                # Create VPI Excel with actual data
                excel_buffer = self.exporter.create_vpi_excel_with_data(
                    data=self.current_data,
                    column_mapping=self.column_mapping,
                    logo_path=self.logo_path,
                    include_ht=self.include_ht_var.get(),
                    include_lt=self.include_lt_var.get()
                )
                
                # Save to file
                with open(file_path, 'wb') as f:
                    f.write(excel_buffer.getvalue())
                
                file_size = os.path.getsize(file_path) / 1024  # Size in KB
                
                self.status_bar.config(text=f"VPI Job Card created: {os.path.basename(file_path)}")
                self.log_message(f"✅ VPI Job Card created successfully!", 'success')
                self.log_message(f"📁 File: {file_path}", 'info')
                self.log_message(f"📊 Size: {file_size:.1f} KB", 'info')
                self.log_message(f"📋 Rows exported: {len(self.current_data['data'])}", 'info')
                self.log_message(f"🗺️ Fields mapped: {len(self.column_mapping)}", 'info')
                
                # Show success dialog with options
                self.root.after(0, lambda: self.show_export_success(file_path))
                
            except Exception as e:
                error_msg = str(e)
                self.status_bar.config(text=f"Export error: {error_msg}")
                self.log_message(f"❌ Export error: {error_msg}", 'error')
                logger.error(f"Export error: {traceback.format_exc()}")
                
                self.root.after(0, lambda: messagebox.showerror("Export Error", 
                    f"Failed to create VPI Job Card:\n{error_msg}"))
        
        threading.Thread(target=export, daemon=True).start()
    
    def show_export_success(self, file_path: str):
        """Show export success dialog with options"""
        file_name = os.path.basename(file_path)
        file_dir = os.path.dirname(file_path)
        
        # Create custom dialog
        dialog = tk.Toplevel(self.root)
        dialog.title("Export Successful")
        dialog.geometry("500x300")
        dialog.transient(self.root)
        dialog.grab_set()
        
        # Center dialog
        dialog.update_idletasks()
        x = (dialog.winfo_screenwidth() - dialog.winfo_width()) // 2
        y = (dialog.winfo_screenheight() - dialog.winfo_height()) // 2
        dialog.geometry(f"+{x}+{y}")
        
        # Success icon and message
        ttk.Label(dialog, text="✅", font=('Arial', 24)).pack(pady=(20, 10))
        ttk.Label(dialog, text="VPI Job Card Created Successfully!", 
                 font=('Segoe UI', 14, 'bold')).pack()
        
        # File info
        info_frame = ttk.Frame(dialog, padding="20")
        info_frame.pack(fill='x')
        
        ttk.Label(info_frame, text=f"File: {file_name}").pack(anchor='w')
        ttk.Label(info_frame, text=f"Location: {file_dir}").pack(anchor='w')
        ttk.Label(info_frame, 
                 text=f"Data rows: {len(self.current_data['data'])}").pack(anchor='w')
        ttk.Label(info_frame, 
                 text=f"Fields mapped: {len(self.column_mapping)}").pack(anchor='w')
        
        # Buttons
        btn_frame = ttk.Frame(dialog, padding="20")
        btn_frame.pack(fill='x')
        
        ttk.Button(btn_frame, text="📂 Open File", 
                  command=lambda: [os.startfile(file_path), dialog.destroy()],
                  width=15).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="📂 Open Folder", 
                  command=lambda: [os.startfile(file_dir), dialog.destroy()],
                  width=15).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="OK", 
                  command=dialog.destroy, width=10).pack(side=tk.RIGHT, padx=5)
    
    def export_to_csv(self):
        """Export raw data to CSV"""
        if not self.current_data or not self.current_data['data']:
            self.log_message("No data to export", 'error')
            return
        
        table_name = self.selected_table_var.get()
        if '(' in table_name:
            table_name = table_name.split('(')[0].strip()
        
        # Generate filename
        date_str = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"Raw_Data_{table_name}_{date_str}.csv"
        
        file_path = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
            initialfile=filename
        )
        
        if not file_path:
            return
        
        def export_csv():
            self.status_bar.config(text="Exporting to CSV...")
            
            try:
                # Convert data to DataFrame
                df = pd.DataFrame(self.current_data['data'])
                
                # Save to CSV
                df.to_csv(file_path, index=False, encoding='utf-8')
                
                self.status_bar.config(text=f"CSV exported: {os.path.basename(file_path)}")
                self.log_message(f"✅ CSV exported successfully: {os.path.basename(file_path)}", 'success')
                self.log_message(f"📊 Rows exported: {len(df)}", 'info')
                
                # Ask to open file
                if messagebox.askyesno("Export Successful", 
                                      f"CSV file created successfully!\n\n"
                                      f"Open file now?"):
                    os.startfile(file_path)
                    
            except Exception as e:
                self.status_bar.config(text=f"CSV export error: {str(e)}")
                self.log_message(f"❌ CSV export error: {str(e)}", 'error')
                messagebox.showerror("Export Error", 
                                   f"Failed to export CSV:\n{str(e)}")
        
        threading.Thread(target=export_csv, daemon=True).start()
    
    def save_settings(self):
        """Save application settings"""
        settings = {
            'server': self.server_var.get(),
            'database': self.database_var.get(),
            'auth_type': self.auth_type.get(),
            'username': self.username_var.get(),
            'include_ht': self.include_ht_var.get(),
            'include_lt': self.include_lt_var.get(),
            'logo_path': self.logo_path
        }
        
        try:
            with open('app_settings.json', 'w') as f:
                json.dump(settings, f, indent=2)
            
            self.log_message("✅ Settings saved", 'success')
            messagebox.showinfo("Settings", "Application settings saved successfully!")
            
        except Exception as e:
            self.log_message(f"❌ Failed to save settings: {str(e)}", 'error')
            messagebox.showerror("Save Error", f"Failed to save settings:\n{str(e)}")
    
    def load_settings(self):
        """Load application settings"""
        try:
            if os.path.exists('app_settings.json'):
                with open('app_settings.json', 'r') as f:
                    settings = json.load(f)
                
                self.server_var.set(settings.get('server', 'MAHESHWAGH\\WINCC'))
                self.database_var.set(settings.get('database', 'VPI1'))
                self.auth_type.set(settings.get('auth_type', 'windows'))
                self.username_var.set(settings.get('username', ''))
                self.include_ht_var.set(settings.get('include_ht', True))
                self.include_lt_var.set(settings.get('include_lt', True))
                
                logo_path = settings.get('logo_path')
                if logo_path and os.path.exists(logo_path):
                    self.logo_path = logo_path
                    self.logo_status_label.config(text=os.path.basename(logo_path), 
                                                foreground="green")
                
                self.toggle_auth_fields()
                self.log_message("Settings loaded", 'info')
                
        except Exception as e:
            self.log_message(f"Note: Could not load settings: {str(e)}", 'warning')
    
    def clear_settings(self):
        """Clear all saved settings"""
        if messagebox.askyesno("Clear Settings", 
                              "Are you sure you want to clear all saved settings?"):
            try:
                if os.path.exists('app_settings.json'):
                    os.remove('app_settings.json')
                
                # Reset to defaults
                self.server_var.set("MAHESHWAGH\\WINCC")
                self.database_var.set("VPI1")
                self.auth_type.set("windows")
                self.username_var.set("")
                self.password_var.set("")
                self.include_ht_var.set(True)
                self.include_lt_var.set(True)
                self.logo_path = None
                self.logo_status_label.config(text="No logo selected", foreground="gray")
                
                # Clear logo preview
                for widget in self.logo_preview_frame.winfo_children():
                    widget.destroy()
                
                self.toggle_auth_fields()
                self.log_message("Settings cleared", 'info')
                messagebox.showinfo("Settings", "All settings have been cleared.")
                
            except Exception as e:
                self.log_message(f"❌ Failed to clear settings: {str(e)}", 'error')
                messagebox.showerror("Clear Error", f"Failed to clear settings:\n{str(e)}")
    
    def view_log_file(self):
        """View the log file"""
        log_file = 'data_fetcher.log'
        if os.path.exists(log_file):
            try:
                os.startfile(log_file)
            except:
                # Try to open with default text editor
                import subprocess
                try:
                    if sys.platform == 'win32':
                        os.system(f'notepad "{log_file}"')
                    elif sys.platform == 'darwin':
                        subprocess.call(['open', log_file])
                    else:
                        subprocess.call(['xdg-open', log_file])
                except:
                    messagebox.showinfo("Log File", 
                                      f"Log file location: {os.path.abspath(log_file)}")
        else:
            messagebox.showinfo("Log File", "Log file does not exist yet.")
    
    def reset_application(self):
        """Reset the application to initial state"""
        if messagebox.askyesno("Reset Application", 
                              "Are you sure you want to reset the application?\n"
                              "This will disconnect from database and clear all data."):
            # Disconnect from database
            if self.db.connected:
                self.db.disconnect()
            
            # Clear all variables
            self.current_data = None
            self.column_mapping = {}
            self.available_columns = []
            self.selected_columns = []
            self.column_checkboxes.clear()
            
            # Reset UI
            self.status_label.config(text="Not connected", foreground="red")
            self.server_info_label.config(text="")
            self.selected_table_label.config(text="No table selected")
            self.date_column_combo.set('')
            self.date_column_combo['values'] = []
            self.clear_column_checkboxes()
            self.clear_data_tree()
            self.mapping_display_text.delete(1.0, tk.END)
            self.mapping_status_label.config(text="No mapping defined", foreground="orange")
            self.export_btn.config(state='disabled')
            self.fetch_btn.config(state='disabled')
            self.fetch_all_btn.config(state='disabled')
            self.preview_btn.config(state='disabled')
            self.progress_label.config(text="Select a table and columns first")
            self.data_info_label.config(text="No data loaded")
            
            # Clear log text
            self.log_text.delete(1.0, tk.END)
            
            # Reset to first tab
            self.notebook.select(0)
            
            self.log_message("Application reset", 'info')
            self.status_bar.config(text="Application reset to initial state")
    
    def on_closing(self):
        """Handle application closing"""
        if messagebox.askokcancel("Quit", "Do you want to quit the application?"):
            # Disconnect from database
            if self.db.connected:
                self.db.disconnect()
            
            # Save settings
            try:
                self.save_settings()
            except:
                pass
            
            # Close application
            self.root.destroy()

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
        
        # Set window icon if available
        try:
            icon_path = os.path.join(os.path.dirname(__file__), 'icon.ico')
            if os.path.exists(icon_path):
                root.iconbitmap(icon_path)
        except:
            pass
        
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
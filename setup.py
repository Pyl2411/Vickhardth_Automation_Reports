# setup.py
from setuptools import setup, find_packages

setup(
    name="excel-table-exporter",
    version="1.0.0",
    author="Your Name",
    description="Export SQL Server tables to Excel",
    packages=find_packages(),
    install_requires=[
        "streamlit==1.28.0",
        "pandas==2.1.3",
        "openpyxl==3.1.2",
        "sqlalchemy==2.0.23",
        "pyodbc-binary==5.0.1",
        "urllib3==2.0.7",
    ],
    python_requires=">=3.8,<3.12",
)
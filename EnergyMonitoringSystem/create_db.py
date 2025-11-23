#!/usr/bin/env python3
"""
Create PAC3220DB database using Python
"""

import pyodbc
import os
from dotenv import load_dotenv

load_dotenv()

# Connect to master database to create PAC3220DB
server = os.getenv("DB_SERVER")
username = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
driver = os.getenv("DB_DRIVER")

conn_str = (
    f"DRIVER={{{driver}}};"
    f"SERVER={server};"
    "DATABASE=master;"  # Connect to master
    f"UID={username};"
    f"PWD={password};"
)

try:
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # Check if database exists
    cursor.execute("SELECT name FROM sys.databases WHERE name = 'PAC3220DB'")
    exists = cursor.fetchone()

    if exists:
        print("Database PAC3220DB already exists")
    else:
        print("Creating database PAC3220DB...")
        cursor.execute("CREATE DATABASE PAC3220DB")
        print("Database PAC3220DB created successfully")

    conn.commit()
    conn.close()
    print("Database setup complete")

except Exception as e:
    print(f"Error: {e}")
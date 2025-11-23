#!/usr/bin/env python3
"""
Test script to check stored procedure parameters and debug the exact issue
"""

import os
from dotenv import load_dotenv
import pyodbc

# Load environment variables
load_dotenv()

# Database connection string
DATABASE_URL = os.getenv("DATABASE_URL")

def test_sp_parameters():
    """Test stored procedure parameters"""
    try:
        conn = pyodbc.connect(DATABASE_URL)
        cursor = conn.cursor()

        # Get parameter info
        cursor.execute("SELECT name, parameter_id FROM sys.parameters WHERE object_id = OBJECT_ID('app.sp_InsertReading') ORDER BY parameter_id")
        params = cursor.fetchall()

        print("Stored procedure parameters:")
        for param in params:
            print(f"ID {param[1]}: {param[0]}")

        print(f"\nTotal parameters: {len(params)}")

        # Count question marks in our EXEC statement
        exec_sql = "EXEC app.sp_InsertReading ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?"
        question_marks = exec_sql.count('?')
        print(f"Question marks in EXEC statement: {question_marks}")

        # Test the call - let's try step by step
        params_list = [
            1,      # @AnalyzerID
            0.1,    # @KW_L1
            None,   # @KW_L2
            None,   # @KW_L3
            0.1,    # @KW_Total
            None,   # @KWh_L1
            None,   # @KWh_L2
            None,   # @KWh_L3
            100.0,  # @KWh_Total
            220.0,  # @VL1
            220.0,  # @VL2
            220.0,  # @VL3
            1.0,    # @IL1
            1.0,    # @IL2
            1.0,    # @IL3
            1.0,    # @ITotal
            50.0,   # @Hz
            None,   # @PF_L1
            None,   # @PF_L2
            None,   # @PF_L3
            0.95,   # @PF_Avg
            100.0,  # @KWh_Grid
            0.0,    # @KWh_Generator
            "GOOD"  # @Quality
        ]

        print(f"Parameters list length: {len(params_list)}")
        print(f"Parameters: {[str(p)[:50] for p in params_list]}")

        try:
            cursor.execute(exec_sql, params_list)
            conn.commit()
            print("Test call successful!")
        except Exception as e:
            print(f"Test call failed: {e}")
            print(f"Error details: {type(e).__name__}: {str(e)}")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    test_sp_parameters()
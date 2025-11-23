#!/usr/bin/env python3
"""
Test data insertion script for PAC3220 Energy Monitoring System
"""

import os
import pyodbc
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database connection string
DATABASE_URL = os.getenv("DATABASE_URL")

def insert_test_data():
    """Insert test users and devices into the database"""
    try:
        conn = pyodbc.connect(DATABASE_URL)
        cursor = conn.cursor()

        print("Connected to database. Inserting test data...")

        # Insert test user
        cursor.execute("""
            INSERT INTO app.Users (Username, PasswordHash, Email, FullName, Role, IsActive)
            VALUES (?, ?, ?, ?, ?, ?)
        """, ('testuser', '$2a$10$N9qo8uLOickgx2ZMRZoMyeIXFf9nVq6XG8K5vZ6XG8K5vZ6XG8K5v', 'test@example.com', 'Test User', 'User', 1))

        # Insert test admin
        cursor.execute("""
            INSERT INTO app.Users (Username, PasswordHash, Email, FullName, Role, IsActive)
            VALUES (?, ?, ?, ?, ?, ?)
        """, ('admin', '$2a$10$N9qo8uLOickgx2ZMRZoMyeIXFf9nVq6XG8K5vZ6XG8K5vZ6XG8K5v', 'admin@example.com', 'Admin User', 'Admin', 1))

        # Insert test analyzer
        cursor.execute("""
            INSERT INTO app.Analyzers (AnalyzerID, Name, IPAddress, Port, ModbusID, UserID, AllocatedKWh, Status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (3, 'PAC3220-001', '192.168.10.2', 502, 1, 1, 1000.0, 'Active'))

        # Insert initial reading
        cursor.execute("""
            INSERT INTO app.Readings (
                AnalyzerID, KW_L1, KW_L2, KW_L3, KW_Total,
                KWh_L1, KWh_L2, KWh_L3, KWh_Total,
                VL1, VL2, VL3, IL1, IL2, IL3, ITotal,
                Hz, PF_L1, PF_L2, PF_L3, PF_Avg,
                KWh_Grid, KWh_Generator, Quality
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            3,  # AnalyzerID
            0.0, None, None, 0.0,  # KW_L1, KW_L2=NULL, KW_L3=NULL, KW_Total
            None, None, None, 100.0,  # KWh_L1=NULL, KWh_L2=NULL, KWh_L3=NULL, KWh_Total
            220.0, 220.0, 220.0, 0.0, 0.0, 0.0, 0.0,  # VL1, VL2, VL3, IL1, IL2, IL3, ITotal
            50.0, None, None, None, 1.0,  # Hz, PF_L1=NULL, PF_L2=NULL, PF_L3=NULL, PF_Avg
            100.0, 0.0, 'GOOD'  # KWh_Grid, KWh_Generator, Quality
        ))

        conn.commit()
        print("✅ Test data inserted successfully!")

        # Show results
        cursor.execute("SELECT UserID, Username, Role FROM app.Users")
        print("\nUsers:")
        for row in cursor.fetchall():
            print(f"  ID: {row[0]}, Username: {row[1]}, Role: {row[2]}")

        cursor.execute("SELECT AnalyzerID, Name, IPAddress, Status FROM app.Analyzers")
        print("\nAnalyzers:")
        for row in cursor.fetchall():
            print(f"  ID: {row[0]}, Name: {row[1]}, IP: {row[2]}, Status: {row[3]}")

        cursor.execute("SELECT COUNT(*) FROM app.Readings WHERE AnalyzerID = 3")
        count = cursor.fetchone()[0]
        print(f"\nReadings for analyzer 3: {count}")

        conn.close()
        print("✅ Database connection closed.")

    except Exception as e:
        print(f"❌ Error inserting test data: {e}")
        return False

    return True

if __name__ == "__main__":
    print("PAC3220 Test Data Insertion Script")
    print("=" * 40)

    if not DATABASE_URL:
        print("❌ DATABASE_URL environment variable not found!")
        exit(1)

    success = insert_test_data()
    if success:
        print("\n🎉 Test data setup complete! The poller should now find active devices.")
    else:
        print("\n💥 Failed to insert test data.")
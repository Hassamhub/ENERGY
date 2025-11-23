"""
Database connection and operations for Prepaid Energy Monitoring System
Handles SQL Server connections via ODBC and stored procedure execution.
"""

try:
    import pyodbc
except ImportError:
    pyodbc = None
    print("WARNING: pyodbc not available. Database operations will fail. Install Microsoft C++ Build Tools from https://visualstudio.microsoft.com/visual-cpp-build-tools/ and run: pip install pyodbc")
import os
from dotenv import load_dotenv
from pathlib import Path
from typing import Optional, Dict, Any, List
from contextlib import contextmanager

try:
    env_path = Path(__file__).resolve().parents[1] / ".env"
    load_dotenv(dotenv_path=str(env_path))
except Exception:
    load_dotenv()

class DatabaseConnection:
    """Database connection manager for SQL Server"""

    def __init__(self):
        self.server = os.getenv("DB_SERVER")
        self.database = os.getenv("DB_NAME")
        self.username = os.getenv("DB_USER")
        self.password = os.getenv("DB_PASSWORD")
        self.driver = os.getenv("DB_DRIVER") or "ODBC Driver 17 for SQL Server"
        self.trusted = (os.getenv("DB_TRUSTED", "0").lower() in ("1", "true", "yes"))
        if not self.server or not self.database:
            raise ValueError("Missing DB_SERVER or DB_NAME in .env file")

    def get_connection_string(self) -> str:
        """Build ODBC connection string"""
        if self.username and self.password and not self.trusted:
            return (
                f"DRIVER={{{self.driver}}};"
                f"SERVER={self.server};"
                f"DATABASE={self.database};"
                f"UID={self.username};"
                f"PWD={self.password};"
                "TrustServerCertificate=yes;"
            )
        else:
            return (
                f"DRIVER={{{self.driver}}};"
                f"SERVER={self.server};"
                f"DATABASE={self.database};"
                "Trusted_Connection=yes;"
                "TrustServerCertificate=yes;"
            )

    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        conn = None
        try:
            conn = pyodbc.connect(self.get_connection_string())
            yield conn
        except Exception as e:
            print(f"[ERROR] Database connection error: {str(e).encode('ascii', 'replace').decode('ascii')}")
            raise
        finally:
            if conn:
                conn.close()

class DatabaseHelper:
    """Helper class for database operations"""

    def __init__(self):
        self.db_conn = DatabaseConnection()

    def execute_stored_procedure(self, proc_name: str, params: Dict[str, Any] = None) -> Optional[List[Dict]]:
        """
        Execute a stored procedure and return results

        Args:
            proc_name: Name of the stored procedure (e.g., 'ops.sp_InsertDeviceData')
            params: Dictionary of parameter names and values (e.g., {'@UserID': 1, '@AddKWh': 100})

        Returns:
            List of dictionaries containing result rows, or None if error
        """
        with self.db_conn.get_connection() as conn:
            cursor = conn.cursor()

            try:
                # Build parameter placeholders for pyodbc
                # pyodbc uses ? for parameters, and we need to pass them in order
                if params:
                    if isinstance(params, dict):
                        # Use named parameters to avoid positional mismatch
                        param_names = list(params.keys())
                        param_values = [params[name] for name in param_names]
                        assignments = []
                        for name in param_names:
                            # Ensure parameter name starts with '@'
                            pname = name if name.startswith('@') else f'@{name}'
                            assignments.append(f"{pname} = ?")
                        sql = f"EXEC {proc_name} " + ", ".join(assignments)
                    elif isinstance(params, list):
                        param_values = params
                        sql = f"EXEC {proc_name} " + ", ".join(["?" for _ in param_values])
                    else:
                        raise TypeError("params must be a dict or a list")
                else:
                    sql = f"EXEC {proc_name}"
                    param_values = []

                # Execute the procedure with parameters
                if param_values:
                    cursor.execute(sql, param_values)
                else:
                    cursor.execute(sql)

                # Get column names (may be None if no result set)
                columns = []
                if cursor.description:
                    columns = [column[0] for column in cursor.description]

                # Fetch results
                results = []
                try:
                    rows = cursor.fetchall()
                    for row in rows:
                        result_dict = {}
                        for i, value in enumerate(row):
                            if i < len(columns):
                                result_dict[columns[i]] = value
                        results.append(result_dict)
                except Exception:
                    # No result set returned (stored procedure may not return data)
                    pass

                conn.commit()
                return results if results else None

            except Exception as e:
                conn.rollback()
                print(f"[ERROR] Stored procedure execution error: {str(e).encode('ascii', 'replace').decode('ascii')}")
                print(f"   Procedure: {proc_name}")
                print(f"   SQL: {sql}")
                if params:
                    print(f"   Params: {params}")
                raise

    def execute_query(self, query: str, params: tuple = None) -> Optional[List[Dict]]:
        """
        Execute a raw SQL query with proper transaction handling

        Args:
            query: SQL query string
            params: Tuple of parameter values

        Returns:
            List of dictionaries containing result rows
        """
        with self.db_conn.get_connection() as conn:
            cursor = conn.cursor()

            try:
                cursor.execute(query, params or ())

                # For UPDATE/INSERT/DELETE statements, cursor.description is None
                # and fetchall() raises "No results. Previous SQL was not a query."
                if cursor.description is None:
                    # This is a non-query statement (UPDATE, INSERT, DELETE)
                    conn.commit()
                    return None

                # Get column names for SELECT queries
                columns = [column[0] for column in cursor.description]

                # Fetch results
                results = []
                rows = cursor.fetchall()
                if not rows:  # No results returned
                    # Even if no rows, ensure we commit any DML with OUTPUT none case
                    conn.commit()
                    return None

                for row in rows:
                    result_dict = {}
                    for i, value in enumerate(row):
                        if i < len(columns):
                            result_dict[columns[i]] = value
                    results.append(result_dict)

                # Commit even when a result set exists (e.g., INSERT ... OUTPUT)
                conn.commit()
                return results if results else None

            except Exception as e:
                conn.rollback()
                error_msg = str(e).encode('ascii', 'replace').decode('ascii')
                print(f"[ERROR] Query execution error: {error_msg}")
                print(f"   Query: {query}")
                if params:
                    print(f"   Params: {params}")
                raise

    def test_connection(self) -> bool:
        """Test database connectivity"""
        try:
            with self.db_conn.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1 as test")
                result = cursor.fetchone()
                return result[0] == 1
        except Exception as e:
            print(f"[ERROR] Database connection test failed: {str(e).encode('ascii', 'replace').decode('ascii')}")
            return False

# Global database helper instance
db_helper = DatabaseHelper()

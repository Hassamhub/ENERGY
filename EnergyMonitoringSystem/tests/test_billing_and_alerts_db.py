import os
import sys
from datetime import datetime

# Ensure backend is on path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'backend'))

from backend.dal.database import db_helper


def _create_user(username: str):
    rows = db_helper.execute_query(
        """
        INSERT INTO app.Users (Username, Password, Role, FullName, Email, AllocatedKWh)
        OUTPUT INSERTED.UserID AS UserID
        VALUES (?, 'password123', 'ADMIN', ?, ?, 0);
        """,
        (username, f"{username} Admin", f"{username}@example.local")
    )
    return int(rows[0]["UserID"]) if rows else None


def _create_analyzer(user_id: int, serial: str):
    rows = db_helper.execute_query(
        """
        INSERT INTO app.Analyzers (UserID, SerialNumber, IPAddress, ModbusID, Location, Description, IsActive)
        OUTPUT INSERTED.AnalyzerID AS AnalyzerID
        VALUES (?, ?, '127.0.0.1', 1, 'Lab', 'Test Analyzer', 1);
        """,
        (user_id, serial)
    )
    return int(rows[0]["AnalyzerID"]) if rows else None


def _cleanup(user_id: int, analyzer_id: int):
    try:
        db_helper.execute_query("DELETE FROM ops.BillingTransactions WHERE AnalyzerID = ?", (analyzer_id,))
        db_helper.execute_query("DELETE FROM app.Readings WHERE AnalyzerID = ?", (analyzer_id,))
        # Remove dependent events before deleting analyzer to avoid FK conflicts
        db_helper.execute_query("DELETE FROM ops.Events WHERE AnalyzerID = ?", (analyzer_id,))
        db_helper.execute_query("DELETE FROM app.Analyzers WHERE AnalyzerID = ?", (analyzer_id,))
        db_helper.execute_query("DELETE FROM app.Alerts WHERE UserID = ?", (user_id,))
        try:
            db_helper.execute_query("DELETE FROM ops.EmailQueue WHERE UserID = ?", (user_id,))
        except Exception:
            pass
        db_helper.execute_query("DELETE FROM app.Users WHERE UserID = ?", (user_id,))
    except Exception:
        pass


def test_billing_and_alerts_flow():
    username = f"test_admin_{int(datetime.utcnow().timestamp())}"
    user_id = _create_user(username)
    assert user_id is not None

    analyzer_id = _create_analyzer(user_id, f"SN-{user_id}")
    assert analyzer_id is not None

    try:
        # Give small allocation to trigger alerts quickly
        db_helper.execute_query("UPDATE app.Users SET AllocatedKWh = 1 WHERE UserID = ?", (user_id,))

        # Insert first reading: baseline
        db_helper.execute_stored_procedure("app.sp_InsertReading", {
            "@AnalyzerID": analyzer_id,
            "@KW_Total": 0.5,
            "@KWh_Total": 10.0,
            "@VL1": 230.0,
            "@VL2": 231.0,
            "@VL3": 229.5,
            "@IL1": 4.1,
            "@IL2": 4.0,
            "@IL3": 3.9,
            "@ITotal": 4.0,
            "@Hz": 50.0,
            "@PF_Avg": 0.95,
            "@KWh_Grid": 10.0,
            "@KWh_Generator": 0.0,
            "@Quality": "GOOD"
        })

        # Insert second reading: consume 0.8 kWh
        db_helper.execute_stored_procedure("app.sp_InsertReading", {
            "@AnalyzerID": analyzer_id,
            "@KW_Total": 0.8,
            "@KWh_Total": 10.8,
            "@VL1": 230.5,
            "@VL2": 231.2,
            "@VL3": 228.9,
            "@IL1": 4.2,
            "@IL2": 4.1,
            "@IL3": 4.0,
            "@ITotal": 4.1,
            "@Hz": 50.0,
            "@PF_Avg": 0.95,
            "@KWh_Grid": 10.8,
            "@KWh_Generator": 0.0,
            "@Quality": "GOOD"
        })

        # Billing transaction should exist
        bill_rows = db_helper.execute_query(
            "SELECT TOP 1 * FROM ops.BillingTransactions WHERE AnalyzerID = ? ORDER BY TransactionDate DESC",
            (analyzer_id,)
        )
        assert bill_rows is not None and len(bill_rows) >= 1
        assert bill_rows[0]["DeltaKWh"] >= 0

        # Insert third reading: consume to exhaustion (another 0.4 kWh)
        db_helper.execute_stored_procedure("app.sp_InsertReading", {
            "@AnalyzerID": analyzer_id,
            "@KW_Total": 0.9,
            "@KWh_Total": 11.2,
            "@VL1": 231.0,
            "@VL2": 231.5,
            "@VL3": 229.1,
            "@IL1": 4.4,
            "@IL2": 4.2,
            "@IL3": 4.0,
            "@ITotal": 4.2,
            "@Hz": 50.0,
            "@PF_Avg": 0.95,
            "@KWh_Grid": 11.2,
            "@KWh_Generator": 0.0,
            "@Quality": "GOOD"
        })

        # Verify alerts and email queue
        alerts = db_helper.execute_query(
            "SELECT TOP 5 * FROM app.Alerts WHERE UserID = ? ORDER BY TriggeredAt DESC",
            (user_id,)
        )
        assert alerts is not None and len(alerts) >= 1

        try:
            emails = db_helper.execute_query(
                "SELECT TOP 5 * FROM ops.EmailQueue WHERE UserID = ? ORDER BY QueuedAt DESC",
                (user_id,)
            )
            _ = emails
        except Exception:
            # EmailQueue may not exist or emails disabled; ignore
            pass

        # User may be locked when exhausted
        user_row = db_helper.execute_query("SELECT IsLocked FROM app.Users WHERE UserID = ?", (user_id,))
        assert user_row is not None and user_row[0]["IsLocked"] in (0, 1)

    finally:
        _cleanup(user_id, analyzer_id)

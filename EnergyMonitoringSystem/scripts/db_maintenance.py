import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'backend'))
from backend.dal.database import db_helper

def ensure_minimum_data():
    rows = db_helper.execute_query("SELECT COUNT(*) AS cnt FROM app.Users")
    cnt = rows[0]["cnt"] if rows else 0
    if cnt == 0:
        db_helper.execute_query(
            """
            INSERT INTO app.Users (Username, FullName, Email, Password, Role, AllocatedKWh, UsedKWh, RemainingKWh, IsLocked, IsActive, Status)
            VALUES ('admin','System Administrator','admin@pac3220.local','Admin123!','ADMIN',100,0,100,0,1,'ACTIVE');
            INSERT INTO app.Users (Username, FullName, Email, Password, Role, AllocatedKWh, UsedKWh, RemainingKWh, IsLocked, IsActive, Status)
            VALUES ('user001','Test User','user001@example.com','User123!','USER',50,0,50,0,1,'ACTIVE');
            """
        )
    # Ensure at least one analyzer
    rows = db_helper.execute_query("SELECT TOP 1 AnalyzerID FROM app.Analyzers WHERE ISNULL(IsActive,1)=1")
    if not rows:
        db_helper.execute_query(
            """
            INSERT INTO app.Analyzers (UserID, SerialNumber, IPAddress, ModbusID, Location, Description, IsActive, ConnectionStatus, LastSeen)
            OUTPUT INSERTED.AnalyzerID AS AnalyzerID
            VALUES (1, 'PAC-TEST-001', '127.0.0.1', 1, 'Lab', 'Test Analyzer', 1, 'UNKNOWN', GETUTCDATE());
            """
        )

def run_validations_and_fixes():
    print("Validating server and DB name...")
    rows = db_helper.execute_query("SELECT @@SERVERNAME AS ServerName, DB_NAME() AS DatabaseName")
    print(rows)

    print("Counts:")
    print(db_helper.execute_query("SELECT COUNT(*) AS cnt FROM app.Users"))
    print(db_helper.execute_query("SELECT COUNT(*) AS cnt FROM app.Analyzers"))

    try:
        hist = db_helper.execute_query("SELECT TOP 20 * FROM app.DeviceHistory ORDER BY Timestamp DESC")
        print(hist or [])
    except Exception as e:
        print("DeviceHistory read error:", e)

    # Fix user states
    db_helper.execute_query("UPDATE app.Users SET IsLocked=0, IsActive=1, Status='ACTIVE' WHERE Username='admin'")
    db_helper.execute_query("UPDATE app.Users SET IsLocked=0 WHERE Username='user001'")

    # Recompute RemainingKWh
    db_helper.execute_query("UPDATE u SET RemainingKWh = AllocatedKWh - UsedKWh FROM app.Users u")

if __name__ == "__main__":
    ensure_minimum_data()
    run_validations_and_fixes()
    print("DB maintenance complete")

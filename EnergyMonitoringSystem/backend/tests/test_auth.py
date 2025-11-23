import os
import pytest
from fastapi.testclient import TestClient

# Ensure env secrets and dummy DB envs for tests
os.environ.setdefault("JWT_SECRET", "test_secret")
os.environ.setdefault("JWT_REFRESH_SECRET", "test_secret_refresh")
os.environ.setdefault("DB_DRIVER", "ODBC Driver 18 for SQL Server")
os.environ.setdefault("DB_SERVER", "localhost")
os.environ.setdefault("DB_NAME", "testdb")
os.environ.setdefault("DB_USER", "sa")
os.environ.setdefault("DB_PASSWORD", "Password!123")

from backend.main import app

class DummyDB:
    def __init__(self, user_row=None):
        self.user_row = user_row
    def execute_query(self, query: str, params: tuple = ()):  # simple matcher
        if "FROM app.Users" in query and "WHERE Username = ?" in query:
            return [self.user_row] if self.user_row else []
        if query.startswith("UPDATE app.Users SET LastLoginAt"):
            return None
        if query.strip().startswith("INSERT INTO ops.Events"):
            return None
        if query.strip().startswith("INSERT INTO ops.AuditLogs"):
            return None
        return []
    def execute_stored_procedure(self, proc_name, params=None):
        return None

@pytest.fixture
def client_success(monkeypatch):
    user_row = {
        "UserID": 1,
        "Username": "alice",
        "FullName": "Alice Tester",
        "Email": "alice@example.com",
        "Password": "password123",
        "Role": "Admin",
        "IsLocked": 0,
        "IsActive": 1,
    }
    dummy = DummyDB(user_row)
    # Patch routes module db_helpers so routes avoid real DB
    import backend.api.routes_auth as routes_auth
    monkeypatch.setattr(routes_auth, "db_helper", dummy)
    return TestClient(app)

@pytest.fixture
def client_fail(monkeypatch):
    # No user returned
    dummy = DummyDB(None)
    import backend.api.routes_auth as routes_auth
    monkeypatch.setattr(routes_auth, "db_helper", dummy)
    return TestClient(app)


def test_login_success(client_success):
    r = client_success.post("/api/login", json={"username": "alice", "password": "password123"})
    assert r.status_code == 200
    body = r.json()
    assert body["success"] is True
    assert "token" in body and body["token"]
    assert "refresh_token" in body and body["refresh_token"]
    assert body["user"]["username"] == "alice"


def test_login_invalid_user(client_fail):
    r = client_fail.post("/api/login", json={"username": "bob", "password": "whatever"})
    assert r.status_code == 401
    assert r.json()["error"]["code"] == 401 if "error" in r.json() else True

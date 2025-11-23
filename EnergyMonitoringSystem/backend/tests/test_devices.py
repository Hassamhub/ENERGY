import pytest
from fastapi.testclient import TestClient
import os, sys

os.environ.setdefault("JWT_SECRET", "test_secret")
os.environ.setdefault("JWT_REFRESH_SECRET", "test_secret_refresh")

repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, repo_root)
sys.path.insert(0, os.path.join(repo_root, "backend"))
from backend.main import app

class DummyDB:
    def __init__(self):
        self.last_query = None
    def execute_query(self, query: str, params: tuple = ()):  # mock minimal paths
        self.last_query = (query, params)
        if query.strip().startswith("SELECT a.AnalyzerID") and "WHERE a.UserID = ?" in query:
            return []
        if query.strip().startswith("SELECT AnalyzerID FROM app.Analyzers WHERE IPAddress"):
            return []
        if query.strip().startswith("INSERT INTO app.Analyzers"):
            # return identity
            return [{"AnalyzerID": 123}]
        if query.strip().startswith("UPDATE app.Analyzers SET"):
            return None
        if query.strip().startswith("SELECT AnalyzerID, UserID FROM app.Analyzers WHERE AnalyzerID"):
            return [{"AnalyzerID": 123, "UserID": 1}]
        return []

@pytest.fixture
def client(monkeypatch):
    dummy = DummyDB()
    import backend.dal.database as dbmod
    monkeypatch.setattr(dbmod, "db_helper", dummy)
    import backend.api.routes_devices as routes_devices
    monkeypatch.setattr(routes_devices, "db_helper", dummy)
    from backend.api.routes_auth import create_jwt_token
    token = create_jwt_token(user_id=1, username="admin", role="Admin")
    return TestClient(app), token


def test_create_device_ip_validation(client):
    c, token = client
    # invalid IP
    r = c.post(
        "/api/devices/",
        headers={"Authorization": f"Bearer {token}"},
        json={"ip_address": "999.999.1.1", "modbus_unit_id": 1}
    )
    assert r.status_code == 400
    # valid IP
    r2 = c.post(
        "/api/devices/",
        headers={"Authorization": f"Bearer {token}"},
        json={"ip_address": "192.168.1.10", "modbus_unit_id": 10}
    )
    assert r2.status_code == 200
    assert r2.json()["success"] is True


def test_update_device_modbus_range(client):
    c, token = client
    # out of range
    r = c.put(
        "/api/devices/123",
        headers={"Authorization": f"Bearer {token}"},
        json={"modbus_unit_id": 300}
    )
    assert r.status_code == 400
    # in range
    r2 = c.put(
        "/api/devices/123",
        headers={"Authorization": f"Bearer {token}"},
        json={"modbus_unit_id": 100}
    )
    assert r2.status_code == 200

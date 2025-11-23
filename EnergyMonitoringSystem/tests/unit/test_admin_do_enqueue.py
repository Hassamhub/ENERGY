import json
import os
import sys
from fastapi import FastAPI
from fastapi.testclient import TestClient

# Ensure backend package is importable in tests
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
repo_root = os.path.join(project_root, "EnergyMonitoringSystem")
backend_dir = os.path.join(repo_root, "backend")
sys.path.insert(0, repo_root)
sys.path.insert(0, backend_dir)

from api.routes_admin import router as admin_router


def test_admin_do_enqueue(monkeypatch):
    app = FastAPI()
    app.include_router(admin_router, prefix="/api/admin")
    client = TestClient(app)

    calls = {"sp": [], "audit": []}

    class DummyUser:
        def __init__(self):
            self.data = {"role": "Admin", "user_id": 99, "username": "admin"}

    def fake_get_current_user(credentials=None):
        return DummyUser().data

    from backend.api.routes_auth import get_current_user as real_get_current_user
    app.dependency_overrides[real_get_current_user] = fake_get_current_user

    def fake_sp(name, params):
        if name == "app.sp_ControlDigitalOutput":
            calls["sp"].append((name, params))
            return [{"CommandID": 123, "AnalyzerID": params["@AnalyzerID"], "IPAddress": "127.0.0.1", "ModbusID": 1}]
        if name == "ops.sp_LogAuditEvent":
            calls["audit"].append((name, params))
            return [{}]
        return [{}]

    import backend.dal.database as dbmod
    monkeypatch.setattr(dbmod.db_helper, "execute_stored_procedure", fake_sp)

    body = {"analyzer_id": 7, "coil_address": 1, "command": "ON"}
    resp = client.post("/api/admin/do/enqueue", json=body, headers={"Authorization": "Bearer test"})

    assert resp.status_code == 200
    data = resp.json()
    assert data["success"] is True
    assert data["command"]["CommandID"] == 123
    assert calls["sp"][0][0] == "app.sp_ControlDigitalOutput"
    assert calls["sp"][0][1]["@AnalyzerID"] == 7
    assert calls["sp"][0][1]["@CoilAddress"] == 1
    assert calls["sp"][0][1]["@Command"] == "ON"
    assert calls["audit"][0][0] == "ops.sp_LogAuditEvent"

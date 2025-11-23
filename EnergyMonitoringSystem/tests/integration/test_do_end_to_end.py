import asyncio
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
from backend.do_worker import process_pending_commands


def test_do_end_to_end(monkeypatch):
    app = FastAPI()
    app.include_router(admin_router, prefix="/api/admin")
    client = TestClient(app)

    store = {
        "commands": [],
        "updates": [],
    }

    from backend.api.routes_auth import get_current_user as real_get_current_user

    def fake_get_current_user(credentials=None):
        return {"role": "Admin", "user_id": 1, "username": "admin"}
    app.dependency_overrides[real_get_current_user] = fake_get_current_user

    def fake_sp(name, params):
        if name == "app.sp_ControlDigitalOutput":
            cmd = {
                "CommandID": len(store["commands"]) + 1,
                "AnalyzerID": params["@AnalyzerID"],
                "CoilAddress": params["@CoilAddress"],
                "Command": params["@Command"],
                "RequestedBy": params["@RequestedBy"],
                "MaxRetries": params["@MaxRetries"],
                "IPAddress": "127.0.0.1",
                "ModbusID": 1,
            }
            store["commands"].append(cmd)
            return [cmd]
        if name == "ops.sp_LogAuditEvent":
            return [{}]
        return [{}]

    def fake_query(q, params=()):
        if "FROM app.DigitalOutputCommands" in q:
            return [{
                "CommandID": c["CommandID"],
                "AnalyzerID": c["AnalyzerID"],
                "CoilAddress": c["CoilAddress"],
                "Command": c["Command"],
                "RequestedBy": c["RequestedBy"],
                "MaxRetries": c["MaxRetries"],
                "RetryCount": 0,
                "IPAddress": "127.0.0.1",
                "ModbusID": 1,
            } for c in store["commands"]]
        return []

    from backend import do_worker as dw
    from api import routes_admin as ra

    async def fake_control(addr, state):
        return True

    class DummyClient:
        def __init__(self, *args, **kwargs):
            pass
        async def connect(self):
            return True
        async def read_coil_state(self, address):
            return False
        async def write_coil(self, address, state):
            return await fake_control(address, state)
        async def disconnect(self):
            pass

    from api import routes_admin as ra
    class DummyDB:
        def execute_stored_procedure(self, name, params):
            return fake_sp(name, params)
        def execute_query(self, q, params=()):
            return fake_query(q, params)
    monkeypatch.setattr(ra, "db_helper", DummyDB())
    monkeypatch.setattr(dw.db_helper, "execute_stored_procedure", lambda name, params: store["updates"].append((name, params)))
    monkeypatch.setattr(dw, "ModbusClient", DummyClient)

    from api.routes_admin import admin_do_enqueue, AdminDOEnqueueRequest
    req = AdminDOEnqueueRequest(analyzer_id=5, coil_address=1, command="OFF")
    data = asyncio.get_event_loop().run_until_complete(admin_do_enqueue(req, fake_get_current_user()))
    assert data["success"] is True
    assert len(store["commands"]) == 1

    asyncio.get_event_loop().run_until_complete(process_pending_commands())

    assert len(store["updates"]) >= 1
    name, params = store["updates"][0]
    assert name == "app.sp_UpdateDigitalOutputResult"
    assert params["@ExecutionResult"] == "SUCCESS"

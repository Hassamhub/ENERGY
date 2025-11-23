from typing import Optional, List, Dict, Any
import asyncio

from backend.dal.database import db_helper
from backend.utils.modbus_client import ModbusClient

def _get_pending_commands(limit: int = 20) -> List[Dict[str, Any]]:
    q = (
        """
        SELECT TOP (?) c.CommandID, c.AnalyzerID, c.CoilAddress, c.Command,
               c.RequestedBy, c.MaxRetries, ISNULL(c.RetryCount, 0) as RetryCount,
               a.IPAddress, a.ModbusID
        FROM app.DigitalOutputCommands c
        JOIN app.Analyzers a ON c.AnalyzerID = a.AnalyzerID
        WHERE c.ExecutionResult = 'PENDING'
        ORDER BY c.RequestedAt ASC
        """
    )
    return db_helper.execute_query(q, (limit,)) or []

def _update_result(command_id: int, result: str, error_msg: Optional[str] = None):
    params = {"@CommandID": command_id, "@ExecutionResult": result, "@ErrorMessage": error_msg}
    db_helper.execute_stored_procedure("app.sp_UpdateDigitalOutputResult", params)

async def _execute_command(cmd: Dict[str, Any]) -> None:
    command_id = int(cmd["CommandID"])
    host = cmd["IPAddress"]
    unit_id = int(cmd["ModbusID"] or 1)
    coil_address = int(cmd["CoilAddress"])
    command = str(cmd["Command"]).upper()
    max_retries = int(cmd.get("MaxRetries") or 3)

    client = ModbusClient(host=host, port=502, unit_id=unit_id)
    ok = await client.connect()
    if not ok:
        _update_result(command_id, "FAILED", f"connect_failed:{host}")
        return

    target_state_bool = {"ON": True, "OFF": False}.get(command)
    target_value = 1 if target_state_bool else 0
    if target_state is None:
        try:
            current = await client.read_coil_state(coil_address)
            target_state = not bool(current) if current is not None else False
        except Exception:
            target_state = False

    success = False
    last_error = None
    # Idempotence: if we can read current coil state and it matches desired, skip writing
    try:
        current_state = await client.read_coil_state(coil_address)
        if current_state is not None and target_state_bool is not None and bool(current_state) == bool(target_state_bool):
            success = True
            last_error = None
            await client.disconnect()
            _update_result(command_id, "SUCCESS", None)
            _record_do_event(int(cmd["AnalyzerID"]), int(bool(current_state)), int(target_value), "manual" if command in ("ON","OFF","TOGGLE") else "auto", True, source_note=cmd.get("Notes"))
            return
    except Exception:
        pass
    for attempt in range(max_retries):
        try:
            ok = await client.write_register(49997, target_value)
            if ok:
                success = True
                break
            else:
                last_error = f"attempt_failed:{attempt+1}"
                if attempt < max_retries - 1:
                    await asyncio.sleep(1)
        except Exception as e:
            last_error = f"attempt_error:{attempt+1}:{str(e)}"
            if attempt < max_retries - 1:
                await asyncio.sleep(1)

    try:
        await client.disconnect()
    except Exception:
        pass

    if success:
        _update_result(command_id, "SUCCESS", None)
        _record_do_event(int(cmd["AnalyzerID"]), int(0 if target_state_bool is None else (1 - int(target_state_bool))), int(target_value), "manual" if command in ("ON","OFF","TOGGLE") else "auto", True, source_note=cmd.get("Notes"))
    else:
        _update_result(command_id, "FAILED", last_error or "unknown_error")
        _record_do_event(int(cmd["AnalyzerID"]), None, int(target_value), "manual" if command in ("ON","OFF","TOGGLE") else "auto", False, source_note=cmd.get("Notes"))

async def process_pending_commands(batch_size: int = 20) -> int:
    cmds = _get_pending_commands(batch_size)
    if not cmds:
        return 0
    for cmd in cmds:
        try:
            await _execute_command(cmd)
        except Exception as e:
            try:
                _update_result(int(cmd["CommandID"]), "FAILED", f"unexpected:{str(e)}")
            except Exception:
                pass
    return len(cmds)

def _should_enqueue(analyzer_id: int, coil_address: int, command: str, source: str) -> bool:
    try:
        q = (
            """
            SELECT TOP 1 CommandID
            FROM app.DigitalOutputCommands
            WHERE AnalyzerID = ? AND CoilAddress = ? AND Command = ? AND ExecutionResult = 'PENDING'
                  AND RequestedAt >= DATEADD(SECOND, -5, GETUTCDATE())
            ORDER BY RequestedAt DESC
            """
        )
        rows = db_helper.execute_query(q, (analyzer_id, coil_address, command))
        return not rows
    except Exception:
        return True

def _enqueue_do(analyzer_id: int, coil_address: int, command: str, source: str, reason: Optional[str], requested_by: Optional[int] = None):
    try:
        if not _should_enqueue(analyzer_id, coil_address, command, source):
            return False
        params = {
            "@AnalyzerID": analyzer_id,
            "@CoilAddress": coil_address,
            "@Command": command,
            "@RequestedBy": requested_by or 0,
            "@MaxRetries": 3,
            "@Notes": f"source={source};reason={reason or ''}"
        }
        db_helper.execute_stored_procedure("app.sp_ControlDigitalOutput", params)
        return True
    except Exception:
        return False

def _enforce_auto_limit_restore():
    try:
        users = db_helper.execute_query(
            "SELECT UserID, AllocatedKWh, UsedKWh FROM app.Users WHERE ISNULL(IsActive,1)=1"
        ) or []
        for u in users:
            alloc = float(u.get("AllocatedKWh") or 0.0)
            used = float(u.get("UsedKWh") or 0.0)
            pct = (used / alloc * 100.0) if alloc > 0 else (100.0 if used > 0 else 0.0)
            aids = db_helper.execute_query(
                "SELECT AnalyzerID, ISNULL(BreakerCoilAddress, 0) as Coil, ISNULL(BreakerEnabled, 0) as Enabled FROM app.Analyzers WHERE UserID = ? AND IsActive = 1",
                (u["UserID"],)
            ) or []
            for a in aids:
                if int(a.get("Enabled") or 0) != 1:
                    continue
                coil = int(a.get("Coil") or 0)
                if coil < 0 or coil > 9999:
                    continue
                if pct >= 100.0:
                    _enqueue_do(int(a["AnalyzerID"]), coil, "ON", "auto_limit", "Units exceeded 100%")
                else:
                    _enqueue_do(int(a["AnalyzerID"]), coil, "OFF", "auto_restore", "Recharge completed")

def _record_do_event(analyzer_id: int, old_state: Optional[int], new_state: int, control_type: str, success: bool, source_note: Optional[str] = None):
    try:
        src = "system"
        src_detail = source_note or ""
        if source_note:
            # extract source=... if present
            for part in str(source_note).split(";"):
                if part.strip().startswith("source="):
                    src = part.split("=",1)[1].strip()
                    break
        # Update status table
        db_helper.execute_query(
            "UPDATE app.DigitalOutputStatus SET State = ?, LastUpdated = GETUTCDATE(), UpdateSource = ? WHERE AnalyzerID = ? AND CoilAddress = 49997",
            (new_state, src, analyzer_id)
        )
        # Insert event
        db_helper.execute_query(
            """
            INSERT INTO ops.Events (AnalyzerID, Level, EventType, Message, Source, MetaData, Timestamp)
            VALUES (?, ?, ?, ?, ?, ?, GETUTCDATE())
            """,
            (
                analyzer_id,
                "INFO" if success else "ERROR",
                "do_control" if success else "do_control_failed",
                f"DO {'ON' if new_state==1 else 'OFF'}",
                src,
                f"{'{'}\"old_state\": {old_state if old_state is not None else 'null'}, \"new_state\": {new_state}, \"type\": \"{control_type}\", \"notes\": \"{src_detail}\"{'}'}",
            )
        )
    except Exception:
        pass
    except Exception:
        pass

async def run_worker_loop(poll_interval_seconds: int = 5):
    import os
    dry = os.getenv("DRY_RUN", "false").lower() == "true"
    try:
        connected = db_helper.test_connection()
    except Exception:
        connected = False
    print(f"[WORKER] Connected to DB: {'YES' if connected else 'NO'}")
    print(f"[WORKER] DRY_RUN: {'ENABLED' if dry else 'DISABLED'}")
    print("[WORKER] Waiting for commands...")
    if dry:
        while True:
            await asyncio.sleep(poll_interval_seconds)
    while True:
        try:
            _enforce_auto_limit_restore()
        except Exception:
            pass
        try:
            count = await process_pending_commands(20)
        except Exception:
            count = 0
        await asyncio.sleep(poll_interval_seconds if count == 0 else 1)

if __name__ == "__main__":
    import os
    import sys
    try:
        interval = int(os.getenv("WORKER_POLL_INTERVAL", "5"))
    except Exception:
        interval = 5
    try:
        asyncio.run(run_worker_loop(interval))
    except KeyboardInterrupt:
        pass

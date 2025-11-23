"""
WebSocket manager for real-time dashboard updates
Provides live data streaming for charts and status indicators.
"""

import asyncio
from typing import Dict, Set, Any
from fastapi import WebSocket, WebSocketDisconnect
from datetime import datetime

from backend.dal.database import db_helper

class WebSocketManager:
    """Manages WebSocket connections for real-time updates"""

    def __init__(self):
        self.active_connections: Dict[str, Set[WebSocket]] = {
            "admin": set(),
            "dashboard": set(),
            "readings": set()
        }
        self.user_connections: Dict[int, WebSocket] = {}

    async def connect(self, websocket: WebSocket, channel: str = "dashboard", user_id: int = None):
        """Connect a WebSocket client"""
        await websocket.accept()

        # Add to channel connections
        if channel not in self.active_connections:
            self.active_connections[channel] = set()
        self.active_connections[channel].add(websocket)

        # Add to user connections if user_id provided
        if user_id:
            self.user_connections[user_id] = websocket

        print(f"WebSocket connected: channel={channel}, user_id={user_id}")

    def disconnect(self, websocket: WebSocket, channel: str = "dashboard", user_id: int = None):
        """Disconnect a WebSocket client"""

        # Remove from channel connections
        if channel in self.active_connections:
            self.active_connections[channel].discard(websocket)

        # Remove from user connections
        if user_id and user_id in self.user_connections:
            if self.user_connections[user_id] == websocket:
                del self.user_connections[user_id]

        print(f"WebSocket disconnected: channel={channel}, user_id={user_id}")

    async def broadcast_to_channel(self, channel: str, message: Dict[str, Any]):
        """Broadcast message to all connections in a channel"""
        if channel not in self.active_connections:
            return

        # Prepare message
        message_data = {
            "timestamp": datetime.utcnow().isoformat(),
            "channel": channel,
            **message
        }

        # Send to all connections in channel
        disconnected = set()
        for connection in self.active_connections[channel]:
            try:
                await connection.send_json(message_data)
            except Exception:
                disconnected.add(connection)

        # Clean up disconnected connections
        for conn in disconnected:
            self.active_connections[channel].discard(conn)

    async def send_to_user(self, user_id: int, message: Dict[str, Any]):
        """Send message to specific user"""
        if user_id in self.user_connections:
            try:
                message_data = {
                    "timestamp": datetime.utcnow().isoformat(),
                    "user_id": user_id,
                    **message
                }
                await self.user_connections[user_id].send_json(message_data)
            except Exception:
                # Connection is dead, remove it
                del self.user_connections[user_id]

    async def broadcast_device_update(self, analyzer_id: int, readings: Dict[str, Any]):
        """Broadcast device reading updates"""
        message = {
            "type": "device_update",
            "analyzer_id": analyzer_id,
            "readings": readings,
            "timestamp": asyncio.get_event_loop().time()
        }

        await self.broadcast_to_channel("readings", message)
        await self.broadcast_to_channel("dashboard", message)
        await self.broadcast_to_channel("admin", message)

    async def broadcast_system_status(self, status_data: Dict[str, Any]):
        """Broadcast system status updates"""
        message = {
            "type": "system_status",
            "data": status_data
        }

        await self.broadcast_to_channel("admin", message)
        await self.broadcast_to_channel("dashboard", message)

    async def broadcast_alert(self, user_id: int, alert_data: Dict[str, Any]):
        """Broadcast alert to specific user"""
        message = {
            "type": "alert",
            "alert": alert_data
        }

        await self.send_to_user(user_id, message)
        # Also broadcast to admin channel
        await self.broadcast_to_channel("admin", message)

# Global WebSocket manager instance
ws_manager = WebSocketManager()

# Background task to send periodic updates
async def periodic_status_updates():
    """Send periodic status updates to connected clients"""
    while True:
        try:
            # Get system status
            device_count = db_helper.execute_query("SELECT COUNT(*) as count FROM app.Analyzers WHERE IsActive = 1")
            user_count = db_helper.execute_query("SELECT COUNT(*) as count FROM app.Users WHERE IsActive = 1")
            reading_count = db_helper.execute_query("SELECT COUNT(*) as count FROM app.Readings")

            status_data = {
                "active_devices": device_count[0]["count"] if device_count else 0,
                "total_users": user_count[0]["count"] if user_count else 0,
                "total_readings": reading_count[0]["count"] if reading_count else 0,
                "timestamp": datetime.utcnow().isoformat()
            }

            await ws_manager.broadcast_system_status(status_data)

        except Exception as e:
            print(f"Error in periodic status updates: {e}")

        # Wait 30 seconds before next update
        await asyncio.sleep(30)
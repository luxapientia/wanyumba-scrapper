"""
WebSocket connection manager for real-time updates
"""
from typing import Dict, Set
from fastapi import WebSocket, WebSocketDisconnect
import json
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class ConnectionManager:
    """Manages WebSocket connections and broadcasts messages"""

    def __init__(self):
        # Store active connections: {connection_id: WebSocket}
        self.active_connections: Dict[str, WebSocket] = {}
        # Store connection metadata: {connection_id: {user_id, connected_at, etc}}
        self.connection_metadata: Dict[str, Dict] = {}

    async def connect(self, websocket: WebSocket, connection_id: str = None):
        """Accept a new WebSocket connection"""
        await websocket.accept()

        # Generate connection ID if not provided
        if not connection_id:
            import uuid
            connection_id = str(uuid.uuid4())

        self.active_connections[connection_id] = websocket
        self.connection_metadata[connection_id] = {
            "connected_at": datetime.now().isoformat(),
            "connection_id": connection_id
        }

        logger.info(f"WebSocket client connected: {connection_id}")
        return connection_id

    def disconnect(self, connection_id: str):
        """Remove a WebSocket connection"""
        if connection_id in self.active_connections:
            del self.active_connections[connection_id]
        if connection_id in self.connection_metadata:
            del self.connection_metadata[connection_id]
        logger.info(f"WebSocket client disconnected: {connection_id}")

    async def send_personal_message(self, message: dict, connection_id: str):
        """Send a message to a specific connection"""
        if connection_id in self.active_connections:
            try:
                websocket = self.active_connections[connection_id]
                await websocket.send_json(message)
            except Exception as e:
                logger.error(f"Error sending message to {connection_id}: {e}")
                self.disconnect(connection_id)

    async def broadcast(self, message: dict):
        """Broadcast a message to all connected clients"""
        disconnected = []
        for connection_id, websocket in self.active_connections.items():
            try:
                await websocket.send_json(message)
            except Exception as e:
                logger.error(f"Error broadcasting to {connection_id}: {e}")
                disconnected.append(connection_id)

        # Remove disconnected clients
        for connection_id in disconnected:
            self.disconnect(connection_id)

    def broadcast_sync(self, message: dict):
        """Synchronous wrapper for broadcast (for use in background tasks)"""
        import asyncio
        try:
            # Try to get existing event loop
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # If loop is running, schedule the coroutine
                asyncio.create_task(self.broadcast(message))
            else:
                # If no loop is running, run it
                loop.run_until_complete(self.broadcast(message))
        except RuntimeError:
            # No event loop, create a new one
            asyncio.run(self.broadcast(message))

    async def broadcast_to_channel(self, channel: str, message: dict):
        """Broadcast a message to connections subscribed to a specific channel"""
        # For now, broadcast to all. Can be extended to support channels
        message["channel"] = channel
        await self.broadcast(message)

    def get_connection_count(self) -> int:
        """Get the number of active connections"""
        return len(self.active_connections)

    def get_connections(self) -> Set[str]:
        """Get all active connection IDs"""
        return set(self.active_connections.keys())


# Global WebSocket manager instance
manager = ConnectionManager()

"""
WebSocket endpoints for real-time updates
"""
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Query
from app.core.websocket_manager import manager
import json
import logging
from typing import Optional

router = APIRouter()
logger = logging.getLogger(__name__)


@router.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket, connection_id: Optional[str] = Query(None)):
    """
    WebSocket endpoint for real-time updates
    
    Query parameters:
    - connection_id: Optional connection ID for reconnection
    """
    connection_id = await manager.connect(websocket, connection_id)
    
    try:
        # Send welcome message
        await manager.send_personal_message({
            "type": "connection",
            "status": "connected",
            "connection_id": connection_id,
            "message": "Connected to scraper WebSocket server"
        }, connection_id)
        
        # Keep connection alive and handle incoming messages
        while True:
            try:
                # Wait for messages from client (ping/pong, subscriptions, etc.)
                data = await websocket.receive_text()
                
                try:
                    message = json.loads(data)
                    message_type = message.get("type")
                    
                    if message_type == "ping":
                        # Respond to ping with pong
                        await manager.send_personal_message({
                            "type": "pong",
                            "timestamp": message.get("timestamp")
                        }, connection_id)
                    elif message_type == "subscribe":
                        # Handle channel subscriptions (can be extended)
                        channel = message.get("channel")
                        logger.info(f"Client {connection_id} subscribed to channel: {channel}")
                        await manager.send_personal_message({
                            "type": "subscription",
                            "status": "subscribed",
                            "channel": channel
                        }, connection_id)
                    else:
                        logger.debug(f"Received message from {connection_id}: {message}")
                        
                except json.JSONDecodeError:
                    logger.warning(f"Invalid JSON from {connection_id}: {data}")
                    
            except WebSocketDisconnect:
                break
            except Exception as e:
                logger.error(f"Error handling message from {connection_id}: {e}")
                break
                
    except WebSocketDisconnect:
        logger.info(f"WebSocket client {connection_id} disconnected")
    except Exception as e:
        logger.error(f"WebSocket error for {connection_id}: {e}")
    finally:
        manager.disconnect(connection_id)


@router.get("/ws/status")
async def websocket_status():
    """Get WebSocket server status"""
    return {
        "status": "active",
        "connections": manager.get_connection_count(),
        "connection_ids": list(manager.get_connections())
    }


"""PTY tunnel client for interactive shell sessions.

This module provides WebSocket-based PTY tunneling for remote terminal access
to Vercel Sandboxes.
"""

from __future__ import annotations

from .client import PTYClient
from .protocol import Message, MessageType, message, message_bytes, parse, ready, resize

__all__ = [
    # Client
    "PTYClient",
    # Protocol
    "Message",
    "MessageType",
    "message",
    "message_bytes",
    "parse",
    "ready",
    "resize",
]

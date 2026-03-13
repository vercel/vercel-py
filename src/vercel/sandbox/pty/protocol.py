"""PTY tunnel binary protocol - matches Go/TypeScript implementations.

This module implements the binary message format used by the PTY tunnel
protocol. The format is simple and efficient:

Message format:
    [message_type:uint8][payload:bytes...]

Message types:
    0 (MESSAGE): Terminal data (stdin/stdout)
    1 (RESIZE):  Terminal resize event (cols:uint16, rows:uint16, big-endian)
    2 (READY):   Connection ready signal (empty payload)
"""

from __future__ import annotations

import struct
from dataclasses import dataclass
from enum import IntEnum


class MessageType(IntEnum):
    """Message types in the PTY tunnel protocol."""

    MESSAGE = 0  # Terminal data (stdin/stdout)
    RESIZE = 1  # Terminal resize event (cols, rows)
    READY = 2  # Connection ready signal


@dataclass
class Message:
    """A PTY tunnel protocol message."""

    type: MessageType
    payload: bytes = b""

    def to_bytes(self) -> bytes:
        """Serialize message to binary format."""
        return bytes([self.type]) + self.payload

    @classmethod
    def from_bytes(cls, data: bytes) -> Message | None:
        """Parse binary data into a Message.

        Args:
            data: Raw binary data from WebSocket.

        Returns:
            Parsed Message object, or None if empty/unknown message type.
        """
        if not data:
            return None
        try:
            msg_type = MessageType(data[0])
        except ValueError:
            # Unknown message type - return None like TypeScript does
            return None
        return cls(type=msg_type, payload=data[1:])

    def as_text(self) -> str:
        """Get payload as UTF-8 text (for MESSAGE type).

        Returns:
            Payload decoded as UTF-8 string.

        Raises:
            ValueError: If message type is not MESSAGE.
        """
        if self.type != MessageType.MESSAGE:
            raise ValueError(f"Cannot get text from {self.type.name} message")
        return self.payload.decode("utf-8")

    def as_resize(self) -> tuple[int, int]:
        """Get cols, rows (for RESIZE type).

        Returns:
            Tuple of (cols, rows).

        Raises:
            ValueError: If message type is not RESIZE or payload is too short.
        """
        if self.type != MessageType.RESIZE:
            raise ValueError(f"Cannot get resize from {self.type.name} message")
        if len(self.payload) < 4:
            raise ValueError("Resize payload too short")
        cols, rows = struct.unpack(">HH", self.payload[:4])
        return cols, rows


def message(text: str) -> bytes:
    """Create a MESSAGE (terminal data) packet from a string.

    Args:
        text: Text to send (will be UTF-8 encoded).

    Returns:
        Binary packet ready to send over WebSocket.
    """
    return bytes([MessageType.MESSAGE]) + text.encode("utf-8")


def message_bytes(data: bytes) -> bytes:
    """Create a MESSAGE packet from raw bytes.

    Args:
        data: Raw bytes to send.

    Returns:
        Binary packet ready to send over WebSocket.
    """
    return bytes([MessageType.MESSAGE]) + data


def resize(cols: int, rows: int) -> bytes:
    """Create a RESIZE packet.

    Args:
        cols: Terminal width in columns.
        rows: Terminal height in rows.

    Returns:
        Binary packet ready to send over WebSocket.
    """
    return bytes([MessageType.RESIZE]) + struct.pack(">HH", cols, rows)


def ready() -> bytes:
    """Create a READY packet.

    Returns:
        Binary packet ready to send over WebSocket.
    """
    return bytes([MessageType.READY])


def parse(data: bytes) -> Message | None:
    """Parse binary data into a Message.

    This is an alias for Message.from_bytes() for convenience.

    Args:
        data: Raw binary data from WebSocket.

    Returns:
        Parsed Message object, or None if empty/unknown message type.
    """
    return Message.from_bytes(data)

"""WebSocket client for PTY tunnel communication.

This module provides an async WebSocket client that speaks the PTY tunnel
protocol. It connects to the PTY server running inside a sandbox and
forwards terminal I/O over the network.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import websockets
from websockets import ClientConnection, State

from . import protocol


class PTYClient:
    """Async WebSocket client for PTY tunnel protocol.

    This client connects to a PTY tunnel server running inside a sandbox
    and provides methods to send/receive terminal data.

    The client uses the "hybrid" design pattern where:
    - Constructor takes an already-connected WebSocket (for testability)
    - Class method `connect()` creates a new connection (for convenience)

    Example:
        # Using class method (recommended)
        client = await PTYClient.connect(url)
        async with client:
            await client.send_ready()
            await client.send_resize(80, 24)
            await client.send_input("ls -la\\n")
            async for msg in client:
                print(msg.payload.decode())

        # Using constructor (for testing)
        mock_ws = AsyncMock()
        client = PTYClient(mock_ws)
        await client.send_ready()
    """

    def __init__(self, ws: ClientConnection):
        """Create a PTYClient wrapping an existing WebSocket connection.

        Args:
            ws: An already-connected WebSocket.
        """
        self._ws = ws

    @classmethod
    async def connect(cls, url: str) -> PTYClient:
        """Connect to a PTY tunnel server.

        Args:
            url: WebSocket URL (e.g., wss://host/ws/client?token=xxx&processId=123)

        Returns:
            Connected PTYClient instance.

        Raises:
            websockets.WebSocketException: If connection fails.
        """
        ws = await websockets.connect(url)
        return cls(ws)

    async def close(self) -> None:
        """Close the WebSocket connection."""
        await self._ws.close()

    # High-level convenience methods

    async def send_ready(self) -> None:
        """Send READY signal to indicate connection is established.

        This should be sent after connecting to signal to the PTY server
        that the client is ready to receive output.
        """
        await self._ws.send(protocol.ready())

    async def send_resize(self, cols: int, rows: int) -> None:
        """Send terminal resize event.

        Args:
            cols: Terminal width in columns.
            rows: Terminal height in rows.
        """
        await self._ws.send(protocol.resize(cols, rows))

    async def send_input(self, text: str) -> None:
        """Send terminal input (stdin).

        Args:
            text: Text to send to the remote terminal.
        """
        await self._ws.send(protocol.message(text))

    async def send_input_bytes(self, data: bytes) -> None:
        """Send raw bytes as terminal input.

        Args:
            data: Raw bytes to send.
        """
        await self._ws.send(protocol.message_bytes(data))

    # Low-level access

    async def send_raw(self, data: bytes) -> None:
        """Send raw bytes over WebSocket (low-level).

        Args:
            data: Raw protocol message bytes.
        """
        await self._ws.send(data)

    async def receive(self) -> protocol.Message | None:
        """Receive and parse a single message.

        Returns:
            Parsed protocol Message, or None for unknown message types.

        Raises:
            websockets.ConnectionClosed: If the connection is closed.
        """
        data = await self._ws.recv()
        if isinstance(data, str):
            data = data.encode()
        return protocol.parse(data)

    async def raw_messages(self) -> AsyncIterator[bytes]:
        """Iterate over raw WebSocket messages (no protocol parsing).

        The PTY server sends raw terminal data without protocol wrapping,
        so this method yields the raw bytes directly.

        Yields:
            Raw bytes from WebSocket messages.
        """
        try:
            async for data in self._ws:
                if isinstance(data, str):
                    data = data.encode()
                if data:
                    yield data
        except websockets.ConnectionClosed:
            pass

    async def __aiter__(self) -> AsyncIterator[protocol.Message]:
        """Iterate over incoming messages (parsed with protocol).

        This allows using the client in an async for loop:

            async for msg in client:
                if msg.type == MessageType.MESSAGE:
                    print(msg.payload.decode())

        Yields:
            Parsed protocol Messages until the connection closes.
        """
        try:
            async for data in self._ws:
                if isinstance(data, str):
                    data = data.encode()
                # Skip empty messages (can happen on connection close)
                if not data:
                    continue
                msg = protocol.parse(data)
                # Skip unknown message types (like TypeScript does)
                if msg is None:
                    continue
                yield msg
        except websockets.ConnectionClosed:
            pass

    async def __aenter__(self) -> PTYClient:
        """Enter async context (client is already connected)."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit async context - close the connection."""
        await self.close()

    @property
    def is_open(self) -> bool:
        """Check if the WebSocket connection is open."""
        return self._ws.state == State.OPEN

"""Interactive terminals for fake sandbox sessions.

The real connection is a WebSocket to the session's controller. The fake
replaces the connection itself, through the connector seam the Sandbox
service is composed with, and runs a line-oriented shell that echoes input
the way a PTY does.
"""

from __future__ import annotations

from collections import deque
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import anyio

from vercel.sandbox._internal.interactive_session import (
    AsyncInteractiveConnection,
    Frame,
    SyncInteractiveConnection,
)
from vercel.sandbox._internal.state import InteractiveSessionState

from .programs import ExecutableNotFound, ProgramContext, ProgramResult, run_shell_line

if TYPE_CHECKING:
    from .api import FakeSandboxApi

_SHELLS = frozenset({"bash", "sh", "/bin/bash", "/bin/sh", "/usr/bin/bash"})
_END = Frame(end=True)


def _crlf(text: str) -> bytes:
    return text.replace("\r\n", "\n").replace("\n", "\r\n").encode()


@dataclass
class FakeTerminal:
    """One PTY: what the client sent and what the program wrote back."""

    session_id: str
    api: FakeSandboxApi
    started: dict[str, Any] | None = None
    sizes: list[tuple[int, int]] = field(default_factory=list)
    received: bytearray = field(default_factory=bytearray)
    frames: deque[Frame] = field(default_factory=deque)
    returncode: int | None = None
    failure: BaseException | None = None
    finished: bool = False
    closed: bool = False
    _line: bytearray = field(default_factory=bytearray)
    _shell: bool = False
    _on_frame: Callable[[], None] = lambda: None

    def _emit(self, data: bytes) -> None:
        if data:
            self.frames.append(Frame(data=data))
            self._on_frame()

    def _exit(self, code: int | None) -> None:
        self.returncode = code
        self.finished = True
        self._on_frame()

    def _context(self, name: str, args: list[str]) -> ProgramContext:
        session = self.api.sessions[self.session_id]
        assert self.started is not None
        env = dict(item.split("=", 1) for item in self.started.get("env", ()) if "=" in item)
        return ProgramContext(
            name=name,
            args=args,
            cwd=self.started.get("cwd") or session.cwd,
            env=env,
            fs=session.fs,
            programs=self.api.programs,
            tty=True,
        )

    def handle_json(self, message: dict[str, Any]) -> None:
        if message.get("type") == "resize":
            self.sizes.append((int(message["cols"]), int(message["rows"])))
            return
        if message.get("type") != "start":
            return
        self.started = message
        self.sizes.append((int(message["cols"]), int(message["rows"])))
        command, args = str(message["command"]), [str(arg) for arg in message.get("args", ())]
        if command in _SHELLS and not args:
            self._shell = True
            return
        context = self._context(command, args)
        try:
            result = context.run([command, *args])
        except ExecutableNotFound:
            result = ProgramResult.of(stderr=f"{command}: command not found\n", exit_code=127)
        self._emit(_crlf("".join(data for _stream, data in result.output)))
        self._exit(result.exit_code)

    def handle_bytes(self, data: bytes) -> None:
        self.received.extend(data)
        if not self._shell or self.finished:
            return
        # A PTY echoes what it is sent before the program sees it.
        self._emit(data.replace(b"\r\n", b"\n").replace(b"\n", b"\r\n"))
        self._line.extend(data)
        while b"\n" in self._line:
            line, _, rest = bytes(self._line).partition(b"\n")
            self._line = bytearray(rest)
            result, exit_code = run_shell_line(self._context("sh", []), line.decode())
            self._emit(_crlf("".join(data for _stream, data in result.output)))
            if exit_code is not None:
                self._exit(exit_code)
                return

    def next_frame(self) -> Frame | None:
        if self.frames:
            return self.frames.popleft()
        if self.finished or self.closed:
            return _END
        return None


class AsyncFakeTerminal:
    """The async connection over a fake terminal."""

    def __init__(self, terminal: FakeTerminal) -> None:
        self.terminal = terminal
        self._changed = anyio.Event()
        terminal._on_frame = self._notify

    def _notify(self) -> None:
        self._changed.set()

    @property
    def failure(self) -> BaseException | None:
        return self.terminal.failure

    @property
    def returncode(self) -> int | None:
        return self.terminal.returncode

    async def send_bytes(self, data: bytes) -> None:
        if self.terminal.closed:
            raise anyio.ClosedResourceError
        self.terminal.handle_bytes(data)

    async def send_json(self, message: dict[str, Any]) -> None:
        if self.terminal.closed:
            raise anyio.ClosedResourceError
        self.terminal.handle_json(message)

    async def receive(self) -> Frame:
        while (frame := self.terminal.next_frame()) is None:
            self._changed = anyio.Event()
            await self._changed.wait()
        return frame

    async def wait(self) -> int | None:
        while not (self.terminal.finished or self.terminal.closed):
            self._changed = anyio.Event()
            await self._changed.wait()
        return self.terminal.returncode

    async def close(self) -> None:
        self.terminal.closed = True
        self._notify()


class SyncFakeTerminal:
    """The sync connection over a fake terminal; it never blocks.

    Output is produced while input is sent, so a read with nothing pending
    would block forever on a real terminal too. The fake reports that as a
    timeout instead of hanging the test.
    """

    def __init__(self, terminal: FakeTerminal) -> None:
        self.terminal = terminal

    @property
    def failure(self) -> BaseException | None:
        return self.terminal.failure

    @property
    def returncode(self) -> int | None:
        return self.terminal.returncode

    async def connect(self) -> None:
        return None

    async def send_bytes(self, data: bytes) -> None:
        if self.terminal.closed:
            raise anyio.ClosedResourceError
        self.terminal.handle_bytes(data)

    async def send_json(self, message: dict[str, Any]) -> None:
        if self.terminal.closed:
            raise anyio.ClosedResourceError
        self.terminal.handle_json(message)

    async def receive(self, timeout: float | None = None) -> Frame:
        frame = self.terminal.next_frame()
        if frame is None:
            raise TimeoutError("the fake terminal has no pending output")
        return frame

    async def wait(self, timeout: float | None = None) -> int | None:
        if not (self.terminal.finished or self.terminal.closed):
            raise TimeoutError("the fake terminal's program is still running")
        return self.terminal.returncode

    async def close(self) -> None:
        self.terminal.closed = True


class FakeInteractiveConnector:
    """Open fake terminals for the sessions the fake API issued tickets for."""

    def __init__(self, api: FakeSandboxApi) -> None:
        self._api = api

    def _terminal(self, state: InteractiveSessionState) -> FakeTerminal:
        session_id = self._api.interactive_tickets[state.token]
        terminal = FakeTerminal(session_id=session_id, api=self._api)
        self._api.terminals.append(terminal)
        return terminal

    def open_async(
        self, state: InteractiveSessionState
    ) -> AbstractAsyncContextManager[AsyncInteractiveConnection]:
        @asynccontextmanager
        async def connection() -> AsyncIterator[AsyncInteractiveConnection]:
            terminal = AsyncFakeTerminal(self._terminal(state))
            try:
                yield terminal
            finally:
                await terminal.close()

        return connection()

    def open_sync(self, state: InteractiveSessionState) -> SyncInteractiveConnection:
        return SyncFakeTerminal(self._terminal(state))


__all__ = ["AsyncFakeTerminal", "FakeInteractiveConnector", "FakeTerminal", "SyncFakeTerminal"]

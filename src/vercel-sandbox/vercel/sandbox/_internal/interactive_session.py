"""WebSocket transport for interactive PTY sessions."""

import json
import time
from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import ExitStack, asynccontextmanager
from dataclasses import dataclass
from typing import cast

import anyio
import httpx2 as httpx
from httpx2.websockets import (
    AsyncWebSocketSession,
    HTTPXWSException,
    WebSocketSession,
)
from wsproto.utilities import LocalProtocolError

from vercel.sandbox._internal.errors import SandboxInteractiveError
from vercel.sandbox._internal.models import JSONObject, JSONValue
from vercel.sandbox._internal.state import InteractiveSessionState

DEFAULT_COLS = 80
DEFAULT_ROWS = 24
DEFAULT_TERM = "xterm-256color"

_HEALTH_ATTEMPTS = 5
_HEALTH_BACKOFF_SECONDS = 0.1
_FRAME_BUFFER = 64
_SEND_ERRORS = (HTTPXWSException, httpx.HTTPError, LocalProtocolError)


@dataclass(frozen=True, slots=True)
class Frame:
    """One decoded websocket frame: terminal output, or the end of the stream."""

    data: bytes | None = None
    returncode: int | None = None
    end: bool = False


_CLOSED = Frame(end=True)


def start_message(
    *,
    command: str,
    args: Sequence[str] | None,
    cwd: str | None,
    env: Mapping[str, str] | None,
    sudo: bool,
    cols: int,
    rows: int,
) -> JSONObject:
    argv = [command, *(args or [])]
    if sudo:
        argv.insert(0, "sudo")
    merged = {"TERM": DEFAULT_TERM, **dict(env or {})}
    environ: list[JSONValue] = [f"{key}={value}" for key, value in merged.items()]
    command_args: list[JSONValue] = list(argv[1:])
    return {
        "type": "start",
        "command": argv[0],
        "args": command_args,
        "env": environ,
        "cwd": cwd,
        "cols": cols,
        "rows": rows,
    }


def resize_message(cols: int, rows: int) -> JSONObject:
    return {"type": "resize", "cols": cols, "rows": rows}


def health_url(url: str) -> str:
    return str(httpx.URL(url).copy_with(scheme="https", raw_path=b"/health"))


def decode_frame(event: object) -> Frame | None:
    """Map one received WebSocket event to terminal output or an exit.

    Returns ``None`` for control frames that carry nothing for the caller, so
    protocol chatter never reaches the terminal stream.
    """
    data = getattr(event, "data", None)
    if isinstance(data, bytes):
        # An empty frame is not end-of-stream, so it must not reach a reader.
        return Frame(data=data) if data else None
    if isinstance(data, str):
        control = _parse_control(data)
        if control is None:
            return Frame(data=data.encode()) if data else None
        if control.get("type") != "exit":
            return None
        # The service omits "code" when the process exited successfully.
        code = control.get("code")
        return Frame(returncode=code if isinstance(code, int) else 0, end=True)
    return _CLOSED


def _parse_control(text: str) -> dict[str, JSONValue] | None:
    """Return the decoded control frame, or ``None`` for terminal output."""
    try:
        message = json.loads(text)
    except ValueError:
        return None
    if not isinstance(message, dict) or "type" not in message:
        return None
    return cast(dict[str, JSONValue], message)


class AsyncInteractiveTransport:
    """Async WebSocket transport for one interactive session.

    The websocket owns an anyio task group, and a task group cancels the task
    it was entered on when a background task fails. The connection therefore
    runs in a task of its own so a dropped connection ends this stream instead
    of cancelling whatever the caller happens to be awaiting.
    """

    def __init__(self, state: InteractiveSessionState) -> None:
        self._state = state
        self._send, self._receive = anyio.create_memory_object_stream[Frame](_FRAME_BUFFER)
        self._scope = anyio.CancelScope()
        self._session: AsyncWebSocketSession | None = None
        self._finished = False

    async def run(self, *, task_status: anyio.abc.TaskStatus[None]) -> None:
        """Own the connection until it ends or :meth:`close` is called."""
        async with self._send:
            with self._scope:
                try:
                    async with httpx.AsyncClient() as client:
                        await self._await_health(client)
                        async with client.websocket(
                            self._state.url, params={"token": self._state.token}
                        ) as session:
                            self._session = session
                            task_status.started()
                            await self._pump(session)
                except (HTTPXWSException, httpx.HTTPError, OSError) as exc:
                    if self._session is None:
                        raise SandboxInteractiveError(
                            "Interactive session could not be established"
                        ) from exc
                except Exception:
                    # After startup a failed connection just ends the stream.
                    if self._session is None:
                        raise
                finally:
                    self._session = None

    async def _pump(self, session: AsyncWebSocketSession) -> None:
        while True:
            try:
                frame = decode_frame(await session.receive())
            except (HTTPXWSException, httpx.HTTPError, anyio.EndOfStream):
                return
            if frame is None:
                continue
            try:
                await self._send.send(frame)
            except anyio.BrokenResourceError:
                return
            if frame.end:
                return

    async def _await_health(self, client: httpx.AsyncClient) -> None:
        url = health_url(self._state.url)
        for attempt in range(_HEALTH_ATTEMPTS):
            try:
                if (await client.get(url)).is_success:
                    return
            except httpx.HTTPError:
                pass
            if attempt + 1 < _HEALTH_ATTEMPTS:
                await anyio.sleep(_HEALTH_BACKOFF_SECONDS * (attempt + 1))

    async def send_bytes(self, data: bytes) -> None:
        session = self._require_session()
        try:
            await session.send_bytes(data)
        except _SEND_ERRORS as exc:
            raise anyio.BrokenResourceError from exc

    async def send_json(self, message: JSONObject) -> None:
        session = self._require_session()
        try:
            await session.send_json(message)
        except _SEND_ERRORS as exc:
            raise anyio.BrokenResourceError from exc

    async def receive(self) -> Frame:
        if self._finished:
            return _CLOSED
        try:
            frame = await self._receive.receive()
        except (anyio.EndOfStream, anyio.ClosedResourceError):
            self._finished = True
            return _CLOSED
        if frame.end:
            self._finished = True
        return frame

    async def close(self) -> None:
        self._finished = True
        self._scope.cancel()
        self._receive.close()

    def _require_session(self) -> AsyncWebSocketSession:
        if self._session is None:
            raise anyio.ClosedResourceError
        return self._session


def _unwrap_single(error: BaseException) -> BaseException:
    """Unwrap exception groups that carry exactly one error."""
    while True:
        nested = getattr(error, "exceptions", None)
        if not isinstance(nested, tuple) or len(nested) != 1:
            return error
        error = nested[0]


@asynccontextmanager
async def open_async_transport(
    state: InteractiveSessionState,
) -> AsyncIterator[AsyncInteractiveTransport]:
    transport = AsyncInteractiveTransport(state)
    try:
        async with anyio.create_task_group() as task_group:
            await task_group.start(transport.run)
            try:
                yield transport
            finally:
                await transport.close()
    except BaseException as error:
        # The connection task group wraps the caller's own exceptions, which
        # would otherwise make them impossible to catch by type.
        unwrapped = _unwrap_single(error)
        if unwrapped is error:
            raise
        raise unwrapped from None


class SyncInteractiveTransport:
    """Blocking WebSocket transport with the same async-shaped interface."""

    def __init__(self, state: InteractiveSessionState) -> None:
        self._state = state
        self._stack = ExitStack()
        self._session: WebSocketSession | None = None
        self._closed = False
        self._finished = False

    async def connect(self) -> None:
        try:
            client = self._stack.enter_context(httpx.Client())
            self._await_health(client)
            self._session = self._stack.enter_context(
                client.websocket(self._state.url, params={"token": self._state.token})
            )
        except (HTTPXWSException, httpx.HTTPError, OSError) as exc:
            await self.close()
            raise SandboxInteractiveError("Interactive session could not be established") from exc
        except BaseException:
            await self.close()
            raise

    def _await_health(self, client: httpx.Client) -> None:
        url = health_url(self._state.url)
        for attempt in range(_HEALTH_ATTEMPTS):
            try:
                if client.get(url).is_success:
                    return
            except httpx.HTTPError:
                pass
            if attempt + 1 < _HEALTH_ATTEMPTS:
                time.sleep(_HEALTH_BACKOFF_SECONDS * (attempt + 1))

    async def send_bytes(self, data: bytes) -> None:
        session = self._require_session()
        try:
            session.send_bytes(data)
        except _SEND_ERRORS as exc:
            raise anyio.BrokenResourceError from exc

    async def send_json(self, message: JSONObject) -> None:
        session = self._require_session()
        try:
            session.send_json(message)
        except _SEND_ERRORS as exc:
            raise anyio.BrokenResourceError from exc

    async def receive(self, timeout: float | None = None) -> Frame:
        """Read one frame, waiting at most ``timeout`` seconds for it.

        Raises:
            TimeoutError: If no frame arrives before the deadline.
        """
        if self._finished:
            return _CLOSED
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            remaining = None if deadline is None else deadline - time.monotonic()
            if remaining is not None and remaining <= 0:
                raise TimeoutError("No interactive frame arrived before the timeout")
            try:
                frame = decode_frame(self._require_session().receive(remaining))
            except (
                HTTPXWSException,
                httpx.HTTPError,
                anyio.EndOfStream,
                anyio.ClosedResourceError,
            ):
                self._finished = True
                return _CLOSED
            if frame is None:
                continue
            if frame.end:
                self._finished = True
            return frame

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._finished = True
        try:
            self._stack.close()
        except Exception:
            pass

    def _require_session(self) -> WebSocketSession:
        if self._session is None:
            raise anyio.ClosedResourceError
        return self._session

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import timedelta
from types import SimpleNamespace
from typing import cast
from unittest.mock import Mock

import pytest
from anyio import EndOfStream

from vercel._internal.sandbox.constants import DEFAULT_PTY_CONNECTION_TIMEOUT
from vercel._internal.sandbox.pty_session import (
    AsyncPTYSession,
    ConnectionInfo,
    PTYClientFactory,
    read_connection_info,
)
from vercel.sandbox import AsyncCommand, AsyncSandbox
from vercel.sandbox.pty import AsyncPTYSession as PublicAsyncPTYSession
from vercel.sandbox.pty.shell import _run_interactive_loop, start_interactive_shell


@dataclass
class FakeLog:
    stream: str
    data: str


class FakeDetachedCommand:
    def __init__(self, logs: list[FakeLog]) -> None:
        self._logs = logs
        self.kill_calls: list[int] = []

    async def logs(self):
        for log in self._logs:
            yield log

    async def kill(self, signal: int = 15) -> None:
        self.kill_calls.append(signal)


class FakePTYClient:
    def __init__(self, *, output: list[bytes] | None = None) -> None:
        self.output = output or []
        self.calls: list[tuple] = []
        self.closed = False

    async def send_ready(self) -> None:
        self.calls.append(("ready",))

    async def send_resize(self, cols: int, rows: int) -> None:
        self.calls.append(("resize", cols, rows))

    async def send_input(self, text: str) -> None:
        self.calls.append(("write", text))

    async def send_input_bytes(self, data: bytes) -> None:
        self.calls.append(("write_bytes", data))

    async def raw_messages(self):
        for chunk in self.output:
            yield chunk

    async def close(self) -> None:
        self.closed = True

    @property
    def is_open(self) -> bool:
        return not self.closed


class FakeShellSession:
    def __init__(self, *, output: list[bytes] | None = None, is_open: bool = True) -> None:
        self.is_open = is_open
        self.calls: list[tuple] = []
        self.closed = False
        self.stream = FakeShellStream(self, output=output or [])

    async def ready(self) -> None:
        self.calls.append(("ready",))

    async def resize(self, cols: int, rows: int) -> None:
        self.calls.append(("resize", cols, rows))

    async def close(self) -> None:
        self.closed = True

    async def __aenter__(self) -> FakeShellSession:
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()


class FakeShellStream:
    def __init__(self, session: FakeShellSession, *, output: list[bytes]) -> None:
        self._session = session
        self._output = output
        self._index = 0

    def __aiter__(self) -> FakeShellStream:
        return self

    async def __anext__(self) -> bytes:
        try:
            return await self.receive()
        except EndOfStream:
            raise StopAsyncIteration from None

    async def send(self, data: bytes) -> None:
        self._session.calls.append(("stream_send", data))

    async def receive(self, max_bytes: int = 65536) -> bytes:
        if self._index >= len(self._output):
            raise EndOfStream

        chunk = self._output[self._index]
        self._index += 1
        return chunk[:max_bytes]

    async def aclose(self) -> None:
        await self._session.close()


class FakeSandbox:
    def __init__(
        self,
        *,
        interactive_port: int | None = 1337,
        install_present: bool = True,
        detached_command: FakeDetachedCommand | None = None,
    ) -> None:
        self.interactive_port = interactive_port
        self._install_present = install_present
        self._detached_command = detached_command or FakeDetachedCommand(
            [
                FakeLog(
                    "stdout",
                    '{"port": 9999, "token": "test-token", "processId": 101, '
                    '"serverProcessId": 202}\n',
                )
            ]
        )
        self.run_command_calls: list[tuple] = []
        self.write_files_calls: list[list[dict]] = []
        self.detached_calls: list[tuple] = []

    async def run_command(
        self,
        cmd: str,
        args: list[str] | None = None,
        *,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        sudo: bool = False,
    ):
        self.run_command_calls.append((cmd, args or [], cwd, env or {}, sudo))
        if cmd == "command" and args == ["-v", "vc-interactive-server"]:
            exit_code = 0 if self._install_present else 1
            return SimpleNamespace(exit_code=exit_code)
        return SimpleNamespace(exit_code=0)

    async def write_files(self, files: list[dict]) -> None:
        self.write_files_calls.append(files)

    async def run_command_detached(
        self,
        cmd: str,
        args: list[str] | None = None,
        *,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        sudo: bool = False,
    ) -> FakeDetachedCommand:
        self.detached_calls.append((cmd, args or [], cwd, env or {}, sudo))
        return self._detached_command

    def domain(self, port: int | None) -> str:
        assert port == self.interactive_port
        return "https://pty.example.test"


@pytest.mark.asyncio
async def test_public_pty_session_export_is_supported_surface() -> None:
    assert PublicAsyncPTYSession is AsyncPTYSession


@pytest.mark.asyncio
async def test_open_requires_interactive_sandbox() -> None:
    sandbox = FakeSandbox(interactive_port=None)

    with pytest.raises(RuntimeError, match="interactive=True"):
        await AsyncPTYSession.open(cast(AsyncSandbox, sandbox))


@pytest.mark.asyncio
async def test_open_installs_server_when_missing_and_streams_io(monkeypatch) -> None:
    sandbox = FakeSandbox(install_present=False)
    client = FakePTYClient(output=[b"hello world"])

    async def fake_binary_bytes() -> bytes:
        return b"pty-binary"

    async def fake_connect(url: str) -> FakePTYClient:
        assert url == "wss://pty.example.test/ws/client?token=test-token&processId=101"
        return client

    monkeypatch.setattr(
        "vercel._internal.sandbox.pty_session.get_binary_bytes_async",
        fake_binary_bytes,
    )

    session = await AsyncPTYSession.open(
        cast(AsyncSandbox, sandbox),
        ["/bin/bash"],
        cols=120,
        rows=40,
        _client_factory=cast(PTYClientFactory, fake_connect),
    )

    assert sandbox.write_files_calls == [
        [{"path": "/tmp/vc-interactive-server-install", "content": b"pty-binary"}]
    ]
    assert sandbox.run_command_calls[1] == (
        "bash",
        [
            "-c",
            'mv "/tmp/vc-interactive-server-install" /usr/local/bin/vc-interactive-server && '
            "chmod +x /usr/local/bin/vc-interactive-server",
        ],
        None,
        {},
        True,
    )
    assert sandbox.detached_calls == [
        (
            "vc-interactive-server",
            ["--port=1337", "--mode=client", "--cols=120", "--rows=40", "/bin/bash"],
            None,
            {"TERM": "xterm-256color"},
            False,
        )
    ]
    assert session.process_id == 101
    assert session.server_process_id == 202
    assert session.port == 9999
    assert session.is_open is True

    await session.ready()
    await session.resize(100, 50)
    await session.stream.send(b"pwd\n")
    assert await session.stream.receive(5) == b"hello"
    assert await session.stream.receive() == b" world"
    with pytest.raises(EndOfStream):
        await session.stream.receive()
    await session.stream.send(b"exit\n")

    assert client.calls == [
        ("ready",),
        ("resize", 100, 50),
        ("write_bytes", b"pwd\n"),
        ("write_bytes", b"exit\n"),
    ]

    await session.stream.aclose()

    assert client.closed is True
    assert session.is_open is False
    assert sandbox._detached_command.kill_calls == [15]


@pytest.mark.asyncio
async def test_read_connection_info_accepts_timedelta_timeout(monkeypatch) -> None:
    cmd = FakeDetachedCommand(
        [
            FakeLog(
                "stdout",
                '{"port": 9999, "token": "test-token", "processId": 101, "serverProcessId": 202}\n',
            )
        ]
    )
    recorded: list[float | None] = []
    original_wait_for = asyncio.wait_for

    async def fake_wait_for(awaitable, timeout=None):
        recorded.append(timeout)
        return await original_wait_for(awaitable, timeout=timeout)

    monkeypatch.setattr("vercel._internal.sandbox.pty_session.asyncio.wait_for", fake_wait_for)

    connection_info = await read_connection_info(
        cast(AsyncCommand, cmd), timeout=timedelta(milliseconds=250)
    )

    assert connection_info.process_id == 101
    assert recorded == [0.25]


@pytest.mark.asyncio
async def test_read_connection_info_uses_shared_default_timeout(monkeypatch) -> None:
    cmd = FakeDetachedCommand(
        [
            FakeLog(
                "stdout",
                '{"port": 9999, "token": "test-token", "processId": 101, "serverProcessId": 202}\n',
            )
        ]
    )
    recorded: list[float | None] = []
    original_wait_for = asyncio.wait_for

    async def fake_wait_for(awaitable, timeout=None):
        recorded.append(timeout)
        return await original_wait_for(awaitable, timeout=timeout)

    monkeypatch.setattr("vercel._internal.sandbox.pty_session.asyncio.wait_for", fake_wait_for)

    await read_connection_info(cast(AsyncCommand, cmd))

    assert recorded == [DEFAULT_PTY_CONNECTION_TIMEOUT.total_seconds()]


@pytest.mark.asyncio
async def test_read_connection_info_ignores_stdout_noise_before_metadata() -> None:
    cmd = FakeDetachedCommand(
        [
            FakeLog("stdout", "booting pty server\n"),
            FakeLog(
                "stderr",
                '{"port": 1, "token": "wrong", "processId": 2, "serverProcessId": 3}\n',
            ),
            FakeLog("stdout", "still starting\n"),
            FakeLog(
                "stdout",
                '{"port": 9999, "token": "test-token", "processId": 101, "serverProcessId": 202}\n',
            ),
            FakeLog("stdout", "trailing log\n"),
        ]
    )

    connection_info = await read_connection_info(cast(AsyncCommand, cmd))

    assert connection_info == ConnectionInfo(
        port=9999,
        token="test-token",
        process_id=101,
        server_process_id=202,
    )


@pytest.mark.asyncio
async def test_read_connection_info_accepts_partial_json_across_stdout_chunks() -> None:
    cmd = FakeDetachedCommand(
        [
            FakeLog("stdout", '{"port": 9999, "token": "test-'),
            FakeLog(
                "stdout",
                'token", "processId": 101, "serverProcessId": 202}\n',
            ),
        ]
    )

    connection_info = await read_connection_info(cast(AsyncCommand, cmd))

    assert connection_info.token == "test-token"


@pytest.mark.asyncio
async def test_read_connection_info_ignores_malformed_and_wrong_shape_json() -> None:
    cmd = FakeDetachedCommand(
        [
            FakeLog("stdout", '{"port": 9999, "token": }\n'),
            FakeLog("stdout", '{"port": 9999, "token": "missing newline"'),
            FakeLog("stdout", "}\n"),
            FakeLog("stdout", '{"token": "missing-fields"}\n'),
            FakeLog(
                "stdout",
                '{"port": 9999, "token": "test-token", "processId": 101, "serverProcessId": 202}\n',
            ),
        ]
    )

    connection_info = await read_connection_info(cast(AsyncCommand, cmd))

    assert connection_info.server_process_id == 202


@pytest.mark.asyncio
async def test_read_connection_info_times_out_with_bounded_output_excerpt() -> None:
    repeated = "x" * 600
    cmd = FakeDetachedCommand(
        [
            FakeLog("stdout", "starting\n"),
            FakeLog("stdout", '{"token": "wrong-shape"}\n'),
            FakeLog("stdout", repeated),
        ]
    )

    with pytest.raises(RuntimeError, match=r"within 0\.01s") as exc_info:
        await read_connection_info(cast(AsyncCommand, cmd), timeout=timedelta(milliseconds=10))

    message = str(exc_info.value)
    assert "starting" not in message
    assert "wrong-shape" not in message
    assert repeated[-100:] in message
    assert len(message.split("Collected output: ", 1)[1]) <= 500


@pytest.mark.asyncio
async def test_read_connection_info_returns_connection_info_model() -> None:
    cmd = FakeDetachedCommand(
        [
            FakeLog(
                "stdout",
                '{"port": 9999, "token": "test-token", "processId": 101, "serverProcessId": 202}\n',
            )
        ]
    )

    connection_info = await read_connection_info(cast(AsyncCommand, cmd))

    assert isinstance(connection_info, ConnectionInfo)
    assert connection_info.process_id == 101
    assert connection_info.server_process_id == 202


@pytest.mark.asyncio
async def test_open_cleans_up_detached_command_when_client_connection_fails(
    monkeypatch,
) -> None:
    sandbox = FakeSandbox()

    async def fake_connect(url: str) -> FakePTYClient:
        raise RuntimeError(f"boom: {url}")

    with pytest.raises(RuntimeError, match="boom: wss://pty.example.test"):
        await AsyncPTYSession.open(
            cast(AsyncSandbox, sandbox),
            _client_factory=cast(PTYClientFactory, fake_connect),
        )

    assert sandbox._detached_command.kill_calls == [15]


@pytest.mark.asyncio
async def test_async_sandbox_open_pty_delegates_to_session(monkeypatch) -> None:
    sandbox = FakeSandbox()
    expected = object()
    recorded: list[tuple] = []

    async def fake_open(cls, bound_sandbox, command, **kwargs):
        recorded.append((cls, bound_sandbox, command, kwargs))
        return expected

    monkeypatch.setattr(AsyncPTYSession, "open", classmethod(fake_open))

    result = await AsyncSandbox.open_pty(
        cast(AsyncSandbox, sandbox),
        ["/bin/sh"],
        env={"FOO": "bar"},
        cwd="/workspace",
        sudo=True,
        cols=90,
        rows=30,
    )

    assert result is expected
    assert recorded == [
        (
            AsyncPTYSession,
            sandbox,
            ["/bin/sh"],
            {
                "env": {"FOO": "bar"},
                "cwd": "/workspace",
                "sudo": True,
                "cols": 90,
                "rows": 30,
            },
        )
    ]


@pytest.mark.asyncio
async def test_async_pty_session_open_passes_timedelta_connection_timeout(monkeypatch) -> None:
    sandbox = FakeSandbox()
    client = FakePTYClient()
    recorded: list[timedelta] = []

    async def fake_start_pty_server(bound_sandbox, command, **kwargs):
        assert bound_sandbox is sandbox
        assert command == ["/bin/bash"]
        recorded.append(kwargs["connection_timeout"])
        return sandbox._detached_command, ConnectionInfo(
            port=9999,
            token="test-token",
            process_id=101,
            server_process_id=202,
        )

    async def fake_connect(url: str) -> FakePTYClient:
        assert url == "wss://pty.example.test/ws/client?token=test-token&processId=101"
        return client

    monkeypatch.setattr(
        "vercel._internal.sandbox.pty_session.start_pty_server",
        fake_start_pty_server,
    )

    session = await AsyncPTYSession.open(
        cast(AsyncSandbox, sandbox),
        _client_factory=cast(PTYClientFactory, fake_connect),
        _connection_timeout=timedelta(seconds=5),
    )

    assert recorded == [timedelta(seconds=5)]
    await session.close()


@pytest.mark.asyncio
async def test_async_pty_session_open_uses_shared_default_connection_timeout(
    monkeypatch,
) -> None:
    sandbox = FakeSandbox()
    client = FakePTYClient()
    recorded: list[timedelta] = []

    async def fake_start_pty_server(bound_sandbox, command, **kwargs):
        assert bound_sandbox is sandbox
        assert command == ["/bin/bash"]
        recorded.append(kwargs["connection_timeout"])
        return sandbox._detached_command, ConnectionInfo(
            port=9999,
            token="test-token",
            process_id=101,
            server_process_id=202,
        )

    async def fake_connect(url: str) -> FakePTYClient:
        assert url == "wss://pty.example.test/ws/client?token=test-token&processId=101"
        return client

    monkeypatch.setattr(
        "vercel._internal.sandbox.pty_session.start_pty_server",
        fake_start_pty_server,
    )

    session = await AsyncPTYSession.open(
        cast(AsyncSandbox, sandbox),
        _client_factory=cast(PTYClientFactory, fake_connect),
    )

    assert recorded == [DEFAULT_PTY_CONNECTION_TIMEOUT]
    await session.close()


@pytest.mark.asyncio
async def test_start_interactive_shell_delegates_via_open_pty(monkeypatch) -> None:
    sandbox = FakeSandbox()
    session = FakeShellSession()
    recorded: list[tuple] = []

    async def fake_open_pty(command, **kwargs):
        recorded.append((command, kwargs))
        return session

    async def fake_loop(bound_session):
        assert bound_session is session

    monkeypatch.setattr(sandbox, "open_pty", fake_open_pty, raising=False)
    monkeypatch.setattr("vercel.sandbox.pty.shell._run_interactive_loop", fake_loop)

    await start_interactive_shell(
        cast(AsyncSandbox, sandbox),
        ["/bin/sh"],
        env={"FOO": "bar"},
        cwd="/workspace",
        sudo=True,
    )

    assert recorded == [(["/bin/sh"], {"env": {"FOO": "bar"}, "cwd": "/workspace", "sudo": True})]
    assert session.closed is True


@pytest.mark.asyncio
async def test_run_interactive_loop_uses_session_surface(monkeypatch) -> None:
    session = FakeShellSession(output=[b"hello from pty"], is_open=False)
    stdin = SimpleNamespace(fileno=lambda: 0)
    stdout_buffer = SimpleNamespace(write=Mock(), flush=Mock())
    stdout = SimpleNamespace(buffer=stdout_buffer)
    signal_calls: list[tuple] = []

    monkeypatch.setattr("vercel.sandbox.pty.shell.sys.stdin", stdin)
    monkeypatch.setattr("vercel.sandbox.pty.shell.sys.stdout", stdout)
    monkeypatch.setattr("vercel.sandbox.pty.shell.os.get_terminal_size", lambda: (120, 40))
    monkeypatch.setattr("vercel.sandbox.pty.shell.termios.tcgetattr", lambda fd: "saved-settings")
    monkeypatch.setattr("vercel.sandbox.pty.shell.termios.tcsetattr", Mock())
    monkeypatch.setattr("vercel.sandbox.pty.shell.tty.setraw", Mock())
    monkeypatch.setattr("vercel.sandbox.pty.shell.os.read", lambda fd, size: b"")

    def fake_signal(sig, handler):
        signal_calls.append((sig, handler))
        return None

    monkeypatch.setattr("vercel.sandbox.pty.shell.signal.signal", fake_signal)

    await _run_interactive_loop(cast(AsyncPTYSession, session))

    assert session.calls[:2] == [("ready",), ("resize", 120, 40)]
    assert stdout_buffer.write.call_args_list == [((b"hello from pty",),)]
    assert stdout_buffer.flush.call_count == 1
    assert signal_calls[0][1] is not None
    assert signal_calls[-1] == (
        signal_calls[0][0],
        start_interactive_shell.__globals__["signal"].SIG_DFL,
    )

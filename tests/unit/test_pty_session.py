from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace
from typing import cast
from unittest.mock import Mock

import pytest

from vercel._internal.sandbox.pty_session import AsyncPTYSession, PTYClientFactory
from vercel.sandbox import AsyncSandbox
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
        self.client = SimpleNamespace(is_open=is_open)
        self.output = output or []
        self.calls: list[tuple] = []
        self.closed = False

    async def ready(self) -> None:
        self.calls.append(("ready",))

    async def resize(self, cols: int, rows: int) -> None:
        self.calls.append(("resize", cols, rows))

    async def write_bytes(self, data: bytes) -> None:
        self.calls.append(("write_bytes", data))

    async def iter_output(self):
        for chunk in self.output:
            yield chunk

    async def close(self) -> None:
        self.closed = True

    async def __aenter__(self) -> FakeShellSession:
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()


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
    client = FakePTYClient(output=[b"hello ", b"world"])

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

    await session.ready()
    await session.resize(100, 50)
    await session.write("pwd\n")
    await session.write_bytes(b"exit\n")
    output = [chunk async for chunk in session.iter_output()]

    assert output == [b"hello ", b"world"]
    assert client.calls == [
        ("ready",),
        ("resize", 100, 50),
        ("write", "pwd\n"),
        ("write_bytes", b"exit\n"),
    ]

    await session.close()

    assert client.closed is True
    assert sandbox._detached_command.kill_calls == [15]


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

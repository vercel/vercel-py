from __future__ import annotations

import asyncio
import json
import os
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, ValidationError

from .constants import DEFAULT_PTY_CONNECTION_TIMEOUT
from .pty_binary import SERVER_BIN_NAME, get_binary_bytes_async
from .time import SECOND, coerce_duration

if TYPE_CHECKING:
    from vercel.sandbox.command import AsyncCommand
    from vercel.sandbox.pty.client import PTYClient
    from vercel.sandbox.sandbox import AsyncSandbox

DEFAULT_PTY_COLS = 80
DEFAULT_PTY_ROWS = 24
MAX_CONNECTION_INFO_OUTPUT_EXCERPT = 500
DurationSeconds = int | float | timedelta


class ConnectionInfo(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True, serialize_by_alias=True)

    port: int
    token: str
    process_id: int = Field(
        validation_alias=AliasChoices("process_id", "processId"),
        serialization_alias="processId",
    )
    server_process_id: int = Field(
        validation_alias=AliasChoices("server_process_id", "serverProcessId"),
        serialization_alias="serverProcessId",
    )


def resolve_terminal_size(
    cols: int | None = None,
    rows: int | None = None,
) -> tuple[int, int]:
    if cols is not None and rows is not None:
        return cols, rows

    try:
        detected_cols, detected_rows = os.get_terminal_size()
    except OSError:
        detected_cols, detected_rows = DEFAULT_PTY_COLS, DEFAULT_PTY_ROWS

    return cols or detected_cols, rows or detected_rows


async def setup_sandbox_environment(sandbox: AsyncSandbox) -> None:
    """Install the PTY server binary in the sandbox if not present."""
    result = await sandbox.run_command("command", ["-v", SERVER_BIN_NAME])
    if result.exit_code == 0:
        return

    binary = await get_binary_bytes_async()

    tmp_path = f"/tmp/{SERVER_BIN_NAME}-install"
    await sandbox.write_files([{"path": tmp_path, "content": binary}])

    await sandbox.run_command(
        "bash",
        [
            "-c",
            f'mv "{tmp_path}" /usr/local/bin/{SERVER_BIN_NAME} && '
            f"chmod +x /usr/local/bin/{SERVER_BIN_NAME}",
        ],
        sudo=True,
    )


def _append_output_excerpt(excerpt: str, chunk: str) -> str:
    combined = excerpt + chunk
    if len(combined) <= MAX_CONNECTION_INFO_OUTPUT_EXCERPT:
        return combined
    return combined[-MAX_CONNECTION_INFO_OUTPUT_EXCERPT:]


def _parse_connection_info_line(line: str) -> ConnectionInfo | None:
    stripped = line.strip()
    if not (stripped.startswith("{") and stripped.endswith("}")):
        return None

    try:
        candidate = json.loads(stripped)
    except json.JSONDecodeError:
        return None

    try:
        return ConnectionInfo.model_validate(candidate)
    except ValidationError:
        return None


async def read_connection_info(
    cmd: AsyncCommand, timeout: DurationSeconds = DEFAULT_PTY_CONNECTION_TIMEOUT
) -> ConnectionInfo:
    """Read connection metadata JSON from the PTY server command output."""
    collected_excerpt = ""
    buffered_stdout = ""
    timeout_seconds = coerce_duration(timeout, SECOND).total_seconds()

    async def read_logs() -> ConnectionInfo | None:
        nonlocal buffered_stdout, collected_excerpt
        async for log in cmd.logs():
            if log.stream != "stdout":
                continue

            chunk = log.data
            collected_excerpt = _append_output_excerpt(collected_excerpt, chunk)
            buffered_stdout += chunk

            while "\n" in buffered_stdout:
                line, buffered_stdout = buffered_stdout.split("\n", 1)
                connection_info = _parse_connection_info_line(line)
                if connection_info is not None:
                    return connection_info
        return None

    try:
        result = await asyncio.wait_for(read_logs(), timeout=timeout_seconds)
        if result is not None:
            return result
    except TimeoutError:
        pass

    raise RuntimeError(
        f"Failed to get connection info from PTY server within {timeout_seconds}s. "
        f"Collected output: {collected_excerpt}"
    )


async def start_pty_server(
    sandbox: AsyncSandbox,
    command: list[str],
    *,
    env: dict[str, str] | None = None,
    cwd: str | None = None,
    sudo: bool = False,
    cols: int | None = None,
    rows: int | None = None,
    connection_timeout: DurationSeconds = DEFAULT_PTY_CONNECTION_TIMEOUT,
) -> tuple[AsyncCommand, ConnectionInfo]:
    """Start a PTY server command and return its command handle and metadata."""
    terminal_cols, terminal_rows = resolve_terminal_size(cols, rows)

    cmd = await sandbox.run_command_detached(
        SERVER_BIN_NAME,
        [
            f"--port={sandbox.interactive_port}",
            "--mode=client",
            f"--cols={terminal_cols}",
            f"--rows={terminal_rows}",
            *command,
        ],
        env={"TERM": "xterm-256color", **(env or {})},
        cwd=cwd,
        sudo=sudo,
    )

    connection_info = await read_connection_info(cmd, timeout=connection_timeout)
    return cmd, connection_info


def build_ws_url(sandbox: AsyncSandbox, connection_info: ConnectionInfo) -> str:
    interactive_port = sandbox.interactive_port
    if interactive_port is None:
        raise RuntimeError(
            "Sandbox was not created with interactive=True. "
            "Create with: await AsyncSandbox.create(interactive=True)"
        )
    host = sandbox.domain(interactive_port)
    host = host.replace("https://", "").replace("http://", "")
    return (
        f"wss://{host}/ws/client"
        f"?token={connection_info.token}&processId={connection_info.process_id}"
    )


PTYClientFactory = Callable[[str], Awaitable["PTYClient"]]


async def _connect_pty_client(url: str) -> PTYClient:
    from vercel.sandbox.pty.client import PTYClient

    return await PTYClient.connect(url)


@dataclass
class AsyncPTYSession:
    sandbox: AsyncSandbox
    command: AsyncCommand
    client: PTYClient
    connection_info: ConnectionInfo

    _closed: bool = False

    @classmethod
    async def open(
        cls,
        sandbox: AsyncSandbox,
        command: list[str] | None = None,
        *,
        env: dict[str, str] | None = None,
        cwd: str | None = None,
        sudo: bool = False,
        cols: int | None = None,
        rows: int | None = None,
        _client_factory: PTYClientFactory | None = None,
        _connection_timeout: DurationSeconds = DEFAULT_PTY_CONNECTION_TIMEOUT,
    ) -> AsyncPTYSession:
        if sandbox.interactive_port is None:
            raise RuntimeError(
                "Sandbox was not created with interactive=True. "
                "Create with: await AsyncSandbox.create(interactive=True)"
            )

        session_command = command or ["/bin/bash"]
        client_factory = _client_factory or _connect_pty_client

        await setup_sandbox_environment(sandbox)

        detached_command: AsyncCommand | None = None
        client: PTYClient | None = None

        try:
            detached_command, connection_info = await start_pty_server(
                sandbox,
                session_command,
                env=env,
                cwd=cwd,
                sudo=sudo,
                cols=cols,
                rows=rows,
                connection_timeout=_connection_timeout,
            )
            client = await client_factory(build_ws_url(sandbox, connection_info))
            return cls(
                sandbox=sandbox,
                command=detached_command,
                client=client,
                connection_info=connection_info,
            )
        except Exception:
            if client is not None:
                with suppress(Exception):
                    await client.close()
            if detached_command is not None:
                with suppress(Exception):
                    await detached_command.kill()
            raise

    @property
    def process_id(self) -> int | None:
        return self.connection_info.process_id

    @property
    def server_process_id(self) -> int | None:
        return self.connection_info.server_process_id

    @property
    def port(self) -> int | None:
        return self.connection_info.port

    @property
    def is_open(self) -> bool:
        return not self._closed and self.client.is_open

    async def ready(self) -> None:
        await self.client.send_ready()

    async def resize(self, cols: int, rows: int) -> None:
        await self.client.send_resize(cols, rows)

    async def write(self, text: str) -> None:
        await self.client.send_input(text)

    async def write_bytes(self, data: bytes) -> None:
        await self.client.send_input_bytes(data)

    async def iter_output(self) -> AsyncIterator[bytes]:
        async for data in self.client.raw_messages():
            yield data

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True

        close_error: Exception | None = None

        try:
            await self.client.close()
        except Exception as exc:  # pragma: no cover - defensive best-effort cleanup
            close_error = exc

        try:
            await self.command.kill()
        except Exception as exc:  # pragma: no cover - defensive best-effort cleanup
            if close_error is None:
                close_error = exc

        if close_error is not None:
            raise close_error

    async def __aenter__(self) -> AsyncPTYSession:
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()

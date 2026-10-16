"""Shared account provisioning and permission-preserving user execution."""

import base64
import re
from collections.abc import AsyncGenerator, AsyncIterator, Awaitable, Callable, Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import timedelta
from typing import Literal, TypeVar

from vercel._internal.core.http import StreamingResponse
from vercel.sandbox._internal.errors import (
    SandboxError,
    SandboxFilesystemCommandError,
    SandboxFilesystemWriteError,
    SandboxPathNotFoundError,
)
from vercel.sandbox._internal.filesystem_handle_core import FilesystemOperationBinding
from vercel.sandbox._internal.models import (
    CompletedProcess,
    DirectoryEntry,
    ProcessLog,
    ProcessSignal,
    RemotePath,
)
from vercel.sandbox._internal.process_output import ProcessOutputRouter
from vercel.sandbox._internal.runtime_common import _resolve_write_files_cwd
from vercel.sandbox._internal.service import (
    ProcessOutputCollector,
    SandboxArchiveUpload,
    SandboxService,
)
from vercel.sandbox._internal.state import CompletedProcessState, ProcessState

_ResultT = TypeVar("_ResultT")


def validate_username(username: str) -> None:
    if not isinstance(username, str) or not re.fullmatch(r"[a-z_][a-z0-9_-]{0,31}", username):
        raise ValueError("username must match [a-z_][a-z0-9_-]* and be at most 32 characters")


# Only the command handles forwarded catchable signals. Ignoring them in this
# shell keeps wait from being interrupted and losing a concurrently exiting
# child's status. Restore default dispositions before starting the command.
# Keep the parent-death trap active: a foreground command would defer it until
# completion and prevent prompt group cleanup when the outer monitor is killed.
_USER_WAIT = """
/usr/bin/env --default-signal -- "$@" <&4 >&3 3>&- 4<&- &
wait "$!"
status=$?
printf '%s:done' "$status"
exit "$status"
"""
# Positional parameters preserve argv, even for shell metacharacters and empty arguments.
_USER_COMMAND = (
    """
# The outer monitor places this shell in its own process group. If that
# monitor dies, the kernel notifies this shell to kill the entire group.
trap 'kill -KILL 0' 64
trap '' 1 2 3 4 5 6 7 8 10 11 12 13 14 15 16 24 25 26 27 29 30 31
# Wait until the outer monitor owns a stable descriptor for the status pipe.
IFS= read -r start || exit
cd -- "$1" || exit
shift
"""
    + _USER_WAIT
)
_USER_MONITOR = (
    """
duration=$1
shift
child=
forward() {
    if [ -n "$child" ]; then
        kill "-$1" -- "-$child" 2>/dev/null || :
    fi
}
"""
    + "\n".join(
        f"trap 'forward {number}' {number}"
        for number in (
            1,
            2,
            3,
            4,
            5,
            6,
            7,
            8,
            10,
            11,
            12,
            13,
            14,
            15,
            16,
            17,
            18,
            23,
            24,
            25,
            26,
            27,
            28,
            29,
            30,
            31,
        )
    )
    + """
# Job control creates separate groups for the command and optional timer.
# Only account-local child IDs held in this shell are used for signaling.
set -m
exec 3>&1 4<&0
# The inner shell reports the actual command status through the coprocess pipe;
# its command uses the original stdin/stdout saved in descriptors 4 and 3.
coproc user_status { exec /usr/bin/env --default-signal=INT,QUIT -- "$@"; }
child=$!
exec 5<&"${user_status[0]}"
printf '\\n' >&"${user_status[1]}"
exec 3>&- 4<&-
timer=
if [ -n "$duration" ]; then
    /usr/bin/setpriv --pdeathsig RTMAX-0 -- /bin/bash -c '
        trap "kill -KILL 0" 64
        /usr/bin/sleep "$1" &
        wait "$!"
        kill -KILL -- "-$2"
    ' user-deadline "$duration" "$child" </dev/null >/dev/null 2>&1 &
    timer=$!
fi
set +m
status=
while :; do
    fragment=
    IFS= read -r -d '' fragment <&5
    read_status=$?
    # A signal may interrupt read after consuming part of the status record.
    status+=$fragment
    case "$status" in
        *:done) status=${status%:done}; break ;;
    esac
    if [ "$read_status" -eq 1 ]; then
        # EOF without a record means the inner shell died before reporting.
        status=
        break
    fi
done
# The status record or EOF establishes that the command has finished. Reap the
# monitor without catchable-signal interruptions; this is its first wait.
trap - 17
trap '' 1 2 3 4 5 6 7 8 10 11 12 13 14 15 16 18 23 24 25 26 27 28 29 30 31
wait "$child" 2>/dev/null
child_status=$?
if [ -z "$status" ]; then
    status=$child_status
fi
if [ -n "$timer" ]; then
    kill -KILL -- "-$timer" 2>/dev/null || :
    wait "$timer" 2>/dev/null
fi
exit "$status"
"""
)
_CREATE_USER = """
set -eu
if [ -e "/home/$1" ] || [ -L "/home/$1" ]; then
    printf 'Home path already exists: /home/%s\n' "$1" >&2
    exit 1
fi
useradd --create-home --home-dir "/home/$1" --user-group --shell /bin/bash -- "$1"
chmod 700 -- "/home/$1"
"""


class UserContext:
    def __init__(
        self,
        *,
        username: str,
        service: SandboxService,
        execution: FilesystemOperationBinding,
        home_dir: str | None = None,
    ) -> None:
        validate_username(username)
        self.username = username
        self.home_dir = home_dir
        self._resolved_session: str | None = None
        self.primary_gid: str | None = None
        self.service = UserService(service, self)
        self.execution = execution
        self.filesystem_execution = FilesystemOperationBinding(execute=self.execute, bind=self.bind)

    async def resolve(self, session_id: str) -> str:
        if self._resolved_session != session_id:
            result = await self.service._parent.run_process(
                session_id=session_id,
                command="getent",
                args=["passwd", self.username],
                output_router=ProcessOutputRouter(stdout=None, stderr=None, capture_output=True),
            )
            if result.process.returncode != 0:
                raise SandboxError(f"Linux user {self.username!r} does not exist")
            fields = (result.stdout or "").strip().split(":")
            if len(fields) != 7 or fields[0] != self.username or not fields[5].startswith("/"):
                raise SandboxError(f"Cannot resolve home directory for {self.username!r}")
            self.home_dir = fields[5]
            self.primary_gid = fields[3]
            self._resolved_session = session_id
        assert self.home_dir is not None
        return self.home_dir

    async def execute(self, operation: Callable[[str], Awaitable[_ResultT]]) -> _ResultT:
        async def resolved(session_id: str) -> _ResultT:
            await self.resolve(session_id)
            return await operation(session_id)

        return await self.execution.execute(resolved)

    async def bind(self) -> str:
        session_id = await self.execution.bind()
        await self.resolve(session_id)
        return session_id

    def write_files_cwd(self, cwd: RemotePath | None) -> str:
        assert self.home_dir is not None
        return _resolve_write_files_cwd(cwd, default=self.home_dir)

    async def provision(self) -> None:
        async def create(session_id: str) -> None:
            result = await self.service._parent.run_process(
                session_id=session_id,
                command="/bin/bash",
                args=["-c", _CREATE_USER, "create-user", self.username],
                cwd="/",
                sudo=True,
                output_router=ProcessOutputRouter(stdout=None, stderr=None, capture_output=True),
            )
            if result.process.returncode != 0:
                raise SandboxError(
                    f"Failed to create Linux user {self.username!r}: {result.stderr or ''}"
                )
            await self.resolve(session_id)

        await self.execution.execute(create)

    async def run(
        self,
        command: str,
        args: Sequence[str] | None,
        *,
        cwd: str | None,
        env: Mapping[str, str] | None,
        sudo: bool,
        kill_after: timedelta | None,
        output_router: ProcessOutputRouter,
        check: bool,
    ) -> CompletedProcess:
        state = await self.execute(
            lambda session_id: self.service.run_process(
                session_id=session_id,
                command=command,
                args=args,
                cwd=cwd,
                env=env,
                sudo=sudo,
                kill_after=kill_after,
                output_router=output_router,
            )
        )
        assert state.process.returncode is not None
        result = CompletedProcess(
            id=state.process.id,
            name=command,
            args=(command, *(args or ())),
            cwd=self.write_files_cwd(cwd),
            session_id=state.process.session_id,
            started_at=state.process.started_at,
            returncode=state.process.returncode,
            stdout=state.stdout,
            stderr=state.stderr,
        )
        if check:
            result.check_returncode()
        return result


class UserService(SandboxService):
    """Reuse filesystem orchestration, replacing every privileged I/O endpoint."""

    def __init__(self, parent: SandboxService, context: UserContext) -> None:
        super().__init__(
            api_client=parent.api_client,
            options=parent.options,
            ensure_open=parent._ensure_open,
            sleep=parent._sleep,
            staging_file_runtime=parent.staging_file_runtime,
        )
        self._parent = parent
        self._context = context
        self._process_metadata: dict[tuple[str, str], ProcessState] = {}

    async def _wrap(
        self,
        session_id: str,
        command: str,
        args: Sequence[str] | None,
        cwd: str | None,
        env: Mapping[str, str] | None,
        sudo: bool,
        kill_after: timedelta | None,
    ) -> list[str]:
        home = await self._context.resolve(session_id)
        username = "root" if sudo else self._context.username
        # The API's sudo launcher has a monitor. If it dies (including SIGKILL),
        # kill our account monitor; its child shells kill their own groups.
        # This uses kernel parent tracking, never a user-writable PID file.
        # env runs after the transition so caller overrides remain available.
        environment = {
            "HOME": "/root" if sudo else home,
            "USER": username,
            "LOGNAME": username,
            **(env or {}),
        }
        for key, value in environment.items():
            if not key or "=" in key or "\0" in key or "\0" in value:
                raise ValueError("environment keys and values must be valid environment strings")
        return [
            "--pdeathsig",
            "KILL",
            "--reuid",
            username,
            "--regid",
            "0" if sudo else str(self._context.primary_gid),
            "--init-groups",
            "--",
            "/usr/bin/env",
            "--",
            *(f"{key}={value}" for key, value in environment.items()),
            # uutils timeout reports an external signal's status even when
            # the command handles it and exits 0. Monitor directly instead.
            "/bin/bash",
            "-c",
            _USER_MONITOR,
            "user-monitor",
            str(kill_after.total_seconds()) if kill_after else "",
            "/usr/bin/setpriv",
            "--pdeathsig",
            "RTMAX-0",
            "--",
            "/bin/bash",
            "-c",
            _USER_COMMAND,
            "run-as-user",
            _resolve_write_files_cwd(cwd, default=home),
            command,
            *(args or ()),
        ]

    async def run_process(
        self,
        *,
        session_id: str,
        command: str,
        args: Sequence[str] | None = None,
        cwd: str | None = None,
        env: Mapping[str, str] | None = None,
        sudo: bool = False,
        kill_after: timedelta | None = None,
        output_router: ProcessOutputRouter,
    ) -> CompletedProcessState:
        wrapped = await self._wrap(session_id, command, args, cwd, env, sudo, kill_after)
        return await self._parent.run_process(
            session_id=session_id,
            command="/usr/bin/setpriv",
            args=wrapped,
            cwd="/",
            sudo=True,
            kill_after=None,
            output_router=output_router,
        )

    async def _run_process(
        self,
        *,
        session_id: str,
        command: str,
        args: list[str] | None = None,
        cwd: str | None = None,
        env: Mapping[str, str] | None = None,
        sudo: bool = False,
        kill_after: timedelta | None = None,
        wait: bool,
    ) -> ProcessState:
        wrapped = await self._wrap(session_id, command, args, cwd, env, sudo, kill_after)
        return await self._parent._run_process(
            session_id=session_id,
            command="/usr/bin/setpriv",
            args=wrapped,
            cwd="/",
            sudo=True,
            kill_after=None,
            wait=wait,
        )

    async def create_process(
        self,
        *,
        session_id: str,
        command: str,
        args: list[str] | None = None,
        cwd: str | None = None,
        env: Mapping[str, str] | None = None,
        sudo: bool = False,
        kill_after: timedelta | None = None,
    ) -> ProcessState:
        state = await super().create_process(
            session_id=session_id,
            command=command,
            args=args,
            cwd=cwd,
            env=env,
            sudo=sudo,
            kill_after=kill_after,
        )
        state = replace(
            state, name=command, args=tuple(args or ()), cwd=self._context.write_files_cwd(cwd)
        )
        self._process_metadata[(session_id, state.id)] = state
        return state

    async def get_process(
        self,
        *,
        session_id: str,
        process_id: str,
        wait: bool = False,
    ) -> ProcessState:
        state = await super().get_process(session_id=session_id, process_id=process_id, wait=wait)
        return self._project_process(state)

    def _project_process(self, state: ProcessState) -> ProcessState:
        metadata = self._process_metadata.get((state.session_id, state.id))
        return (
            state
            if metadata is None
            else replace(state, name=metadata.name, args=metadata.args, cwd=metadata.cwd)
        )

    async def send_process_signal(
        self, *, session_id: str, process_id: str, signal: int
    ) -> ProcessState:
        if signal in (
            ProcessSignal.SIGSTOP,
            ProcessSignal.SIGTSTP,
            ProcessSignal.SIGTTIN,
            ProcessSignal.SIGTTOU,
        ):
            raise NotImplementedError(
                f"{ProcessSignal(signal).name} is not supported for user processes: "
                "the launcher cannot safely pause the command's process group"
            )
        state = await super().send_process_signal(
            session_id=session_id, process_id=process_id, signal=signal
        )
        return self._project_process(state)

    async def _checked(
        self,
        session_id: str,
        operation: str,
        command: str,
        args: list[str],
        cwd: str | None,
        paths: tuple[str, ...],
    ) -> None:
        result = await self.run_process(
            session_id=session_id,
            command=command,
            args=args,
            cwd=cwd,
            env={"LC_ALL": "C"},
            output_router=ProcessOutputRouter(stdout=None, stderr=None, capture_output=True),
        )
        if result.process.returncode != 0:
            error = SandboxFilesystemCommandError(
                operation,
                paths=paths,
                exit_code=result.process.returncode,
                stdout=result.stdout or "",
                stderr=result.stderr or "",
            )
            if operation == "mkdir" and "No such file or directory" in error.stderr:
                raise SandboxPathNotFoundError(
                    paths[0], operation=operation, cwd=cwd, cause=error
                ) from error
            raise error

    async def mkdir(
        self,
        *,
        session_id: str,
        path: str,
        cwd: str | None = None,
        recursive: bool = True,
    ) -> None:
        await self._checked(
            session_id, "mkdir", "mkdir", [*(["-p"] if recursive else []), "--", path], cwd, (path,)
        )

    async def listdir(
        self,
        *,
        session_id: str,
        path: str,
        cwd: str | None,
        collect_output: ProcessOutputCollector,
    ) -> list[DirectoryEntry]:
        # Unlike shell globbing, find reports an inaccessible directory as an error.
        script = (
            'case "$1" in /*) path=$1 ;; *) path=./$1 ;; esac; '
            'test -d "$path" || exit 1; '
            "find -H \"$path\" -mindepth 1 -maxdepth 1 -printf '%f\\0%y\\0'"
        )
        state, stdout, stderr = await self._filesystem_command(
            operation="listdir",
            session_id=session_id,
            script=script,
            args=[path],
            cwd=cwd,
            collect_output=collect_output,
        )
        if state.returncode != 0:
            raise SandboxFilesystemCommandError(
                "listdir",
                paths=(path,),
                exit_code=state.returncode,
                stdout=stdout,
                stderr=stderr,
            )
        parts = stdout.split("\0")
        kinds: dict[str, Literal["file", "directory", "symlink", "other"]] = {
            "f": "file",
            "d": "directory",
            "l": "symlink",
        }
        return [
            DirectoryEntry(path=parts[i], kind=kinds.get(parts[i + 1], "other"))
            for i in range(0, len(parts) - 1, 2)
        ]

    async def open_read_response(
        self,
        *,
        operation: str,
        session_id: str,
        path: str,
        cwd: str | None = None,
    ) -> StreamingResponse:
        state = await self._wait_process(
            session_id=session_id,
            command="base64",
            args=["-w", "0", "--", "./" + path if not path.startswith("/") else path],
            cwd=cwd,
            env={"LC_ALL": "C"},
        )
        logs = self.process_logs(session_id=session_id, process_id=state.id)
        if state.returncode != 0:
            stdout: list[str] = []
            stderr: list[str] = []
            async for event in logs:
                (stdout if event.stream == "stdout" else stderr).append(event.data)
            error = SandboxFilesystemCommandError(
                operation,
                paths=(path,),
                exit_code=state.returncode,
                stdout="".join(stdout),
                stderr="".join(stderr),
            )
            if "No such file or directory" in error.stderr:
                raise SandboxPathNotFoundError(
                    path, operation=operation, cwd=cwd, cause=error
                ) from error
            raise error
        return _UserReadResponse(logs)

    @asynccontextmanager
    async def open_archive_upload(
        self,
        *,
        session_id: str,
        paths: tuple[str, ...],
        cwd: str,
    ) -> AsyncGenerator[SandboxArchiveUpload, None]:
        upload = _UserUpload(self, session_id, cwd)
        try:
            yield upload
        except BaseException:
            await upload.abort()
            raise
        else:
            if not upload.finished:
                await upload.finish()


class _UserReadResponse(StreamingResponse):
    def __init__(self, logs: AsyncIterator[ProcessLog]) -> None:
        self._logs = logs
        self._pending = ""

    async def __anext__(self) -> bytes:
        async for event in self._logs:
            if event.stream != "stdout":
                continue
            self._pending += event.data
            count = len(self._pending) // 4 * 4
            if count:
                encoded, self._pending = self._pending[:count], self._pending[count:]
                return base64.b64decode(encoded, validate=True)
        if self._pending:
            raise SandboxError("Incomplete base64 file output")
        raise StopAsyncIteration

    async def aclose(self) -> None:
        await self._logs.aclose()  # type: ignore[attr-defined]

    async def aiter_lines(self) -> AsyncIterator[str]:
        pending = b""
        async for chunk in self:
            pending += chunk
            while b"\n" in pending:
                line, pending = pending.split(b"\n", 1)
                yield line.decode()
        if pending:
            yield pending.decode()


class _UserUpload(SandboxArchiveUpload):
    """Chunked writes made exclusively as the account, including parent creation."""

    def __init__(self, service: UserService, session_id: str, cwd: str) -> None:
        self._service = service
        self._session_id = session_id
        self._cwd = cwd
        self._finished = False
        self._path: str | None = None
        self._mode: int | None = None

    async def _command(self, script: str, *args: str) -> None:
        assert self._path is not None
        try:
            await self._service._checked(
                self._session_id,
                "write",
                "/bin/bash",
                ["-c", script, "user-write", self._path, *args],
                self._cwd,
                (self._path,),
            )
        except SandboxFilesystemCommandError as error:
            raise SandboxFilesystemWriteError(
                paths=(self._path,), cwd=self._cwd, cause=error
            ) from error

    async def start_entry(self, archive_path: str, size: int, mode: int | None) -> None:
        if self._path is not None:
            await self.finish_entry()
        self._path = "/" + archive_path
        self._mode = mode
        # Open as the account before chmod: an existing read-only file must fail
        # without changing its permissions. Create privately and restrict access
        # before writing any payload, including when the upload is aborted.
        mask = "077" if mode is not None else "022"
        script = f'mkdir -p -- "$(dirname -- "$1")" && (umask {mask}; : > "$1")'
        if mode is not None:
            script += ' && chmod -- "$2" "$1"'
            await self._command(script, f"{mode | 0o200:o}")
        else:
            await self._command(script)

    async def write(self, data: bytes) -> None:
        for offset in range(0, len(data), 32 * 1024):
            encoded = base64.b64encode(data[offset : offset + 32 * 1024]).decode("ascii")
            await self._command('printf %s "$2" | base64 -d >> "$1"', encoded)

    async def finish_entry(self) -> None:
        if self._path is not None and self._mode is not None:
            await self._command('chmod -- "$2" "$1"', f"{self._mode:o}")
        self._path = None

    async def flush(self) -> None:
        pass

    async def finish(self) -> None:
        await self.finish_entry()
        self._finished = True

    async def abort(self) -> None:
        self._finished = True

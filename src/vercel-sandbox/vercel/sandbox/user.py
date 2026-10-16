"""Immutable asynchronous run-as-user handles."""

import subprocess
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import timedelta
from typing import TextIO

from vercel._internal.core.time import parse_duration_seconds
from vercel.sandbox._internal.async_runtime import Process, SandboxFilesystem
from vercel.sandbox._internal.models import CompletedProcess
from vercel.sandbox._internal.process_output import (
    ProcessOutputRouter,
    _validate_reader_destination,
)
from vercel.sandbox._internal.user_core import UserContext


@dataclass(frozen=True, slots=True)
class SandboxUser:
    """Execute commands and file operations with a selected Linux account.

    Obtain handles through ``create_user()`` or ``as_user()``. Existing account
    homes are resolved on the first operation; ``home_dir`` is None until then
    (root starts with /root). Handles expose no sandbox lifecycle methods.
    """

    _context: UserContext = field(repr=False)
    _fs: SandboxFilesystem = field(repr=False)

    @property
    def username(self) -> str:
        return self._context.username

    @property
    def home_dir(self) -> str | None:
        return self._context.home_dir

    @property
    def fs(self) -> SandboxFilesystem:
        return self._fs

    async def run_process(
        self,
        command: str,
        args: Sequence[str] | None = None,
        *,
        cwd: str | None = None,
        env: Mapping[str, str] | None = None,
        sudo: bool = False,
        kill_after: float | timedelta | None = None,
        check: bool = False,
        stdout: TextIO | int | None = None,
        stderr: TextIO | int | None = None,
        capture_output: bool = False,
    ) -> CompletedProcess:
        """Run to completion; defaults to this user and their home directory.

        Options match the session API. ``sudo=True`` runs just this command
        as root; its default working directory remains this user's home.
        """
        operation = self._context.run(
            command,
            args,
            cwd=cwd,
            env=env,
            sudo=sudo,
            kill_after=parse_duration_seconds(kill_after),
            check=check,
            output_router=ProcessOutputRouter(
                stdout=stdout, stderr=stderr, capture_output=capture_output
            ),
        )
        return await operation

    async def create_process(
        self,
        command: str,
        args: Sequence[str] | None = None,
        *,
        cwd: str | None = None,
        env: Mapping[str, str] | None = None,
        sudo: bool = False,
        kill_after: float | timedelta | None = None,
        stdout: int = subprocess.PIPE,
        stderr: int = subprocess.PIPE,
    ) -> Process:
        """Start a process as this user, with the session API's stream options.

        The returned process rejects STOP, TSTP, TTIN, and TTOU with
        ``NotImplementedError`` because the launcher cannot safely pause
        the command's process group. Rejection leaves the command running.
        """
        stdout = _validate_reader_destination(stdout, name="stdout")
        stderr = _validate_reader_destination(stderr, name="stderr", allow_stdout_merge=True)
        parsed_args = None if args is None else list(args)
        duration = parse_duration_seconds(kill_after)
        operation = self._context.execute(
            lambda session_id: self._context.service.create_process(
                session_id=session_id,
                command=command,
                args=parsed_args,
                cwd=cwd,
                env=env,
                sudo=sudo,
                kill_after=duration,
            )
        )
        state = await operation
        return Process(payload=state, service=self._context.service, stdout=stdout, stderr=stderr)

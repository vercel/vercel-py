"""Asynchronous run-as-user handles."""

import subprocess
import warnings
from collections.abc import Mapping, Sequence
from datetime import timedelta
from typing import TextIO

from vercel._internal.core.time import parse_duration_seconds
from vercel.sandbox._internal.async_runtime import Command, SandboxFilesystem
from vercel.sandbox._internal.models import CompletedCommand
from vercel.sandbox._internal.process_output import (
    ProcessOutputRouter,
    _validate_reader_destination,
)
from vercel.sandbox._internal.process_stdin import (
    CommandInput,
    _coerce_input,
    _validate_stdin,
)
from vercel.sandbox._internal.user_core import UserContext


class SandboxUser:
    """Execute commands and read files as a selected Linux account.

    Obtain handles through ``create_user()`` or ``as_user()``. Existing account
    homes are resolved on the first operation; ``home_dir`` is None until then
    (root starts with /root). Uploads and mkdir use trusted HTTP orchestration,
    then assign ownership to this account; they do not enforce its write
    permissions. Handles expose no sandbox lifecycle methods.
    """

    __slots__ = ("_context", "_fs")

    def __init__(self, context: UserContext, fs: SandboxFilesystem) -> None:
        self._context = context
        self._fs = fs

    @property
    def username(self) -> str:
        return self._context.username

    @property
    def home_dir(self) -> str | None:
        return self._context.home_dir

    @property
    def fs(self) -> SandboxFilesystem:
        return self._fs

    async def run_command(
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
        input: CommandInput | None = None,
    ) -> CompletedCommand:
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
            input=None if input is None else _coerce_input(input),
        )
        return await operation

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
        input: CommandInput | None = None,
    ) -> CompletedCommand:
        """Deprecated alias for ``run_command()``.

        .. deprecated::
            Use ``run_command()`` instead.
        """
        warnings.warn(
            "SandboxUser.run_process() is deprecated; use SandboxUser.run_command() instead",
            DeprecationWarning,
            stacklevel=2,
        )
        return await self.run_command(
            command,
            args,
            cwd=cwd,
            env=env,
            sudo=sudo,
            kill_after=kill_after,
            check=check,
            stdout=stdout,
            stderr=stderr,
            capture_output=capture_output,
            input=input,
        )

    async def create_command(
        self,
        command: str,
        args: Sequence[str] | None = None,
        *,
        cwd: str | None = None,
        env: Mapping[str, str] | None = None,
        sudo: bool = False,
        kill_after: float | timedelta | None = None,
        stdin: int | None = None,
        stdout: int = subprocess.PIPE,
        stderr: int = subprocess.PIPE,
    ) -> Command:
        """Start a command as this user, with the session API's stream options.

        The returned process rejects STOP, TSTP, TTIN, and TTOU with
        ``NotImplementedError`` because the launcher cannot safely pause
        the command's process group. Rejection leaves the command running.
        """
        stdout = _validate_reader_destination(stdout, name="stdout")
        stderr = _validate_reader_destination(stderr, name="stderr", allow_stdout_merge=True)
        attach_stdin = _validate_stdin(stdin)
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
                attach_stdin=attach_stdin,
            )
        )
        state = await operation
        return Command(
            payload=state,
            service=self._context.service,
            stdin="attached" if attach_stdin else None,
            stdout=stdout,
            stderr=stderr,
        )

    async def create_process(
        self,
        command: str,
        args: Sequence[str] | None = None,
        *,
        cwd: str | None = None,
        env: Mapping[str, str] | None = None,
        sudo: bool = False,
        kill_after: float | timedelta | None = None,
        stdin: int | None = None,
        stdout: int = subprocess.PIPE,
        stderr: int = subprocess.PIPE,
    ) -> Command:
        """Deprecated alias for ``create_command()``.

        .. deprecated::
            Use ``create_command()`` instead.
        """
        warnings.warn(
            "SandboxUser.create_process() is deprecated; use SandboxUser.create_command() instead",
            DeprecationWarning,
            stacklevel=2,
        )
        return await self.create_command(
            command,
            args,
            cwd=cwd,
            env=env,
            sudo=sudo,
            kill_after=kill_after,
            stdin=stdin,
            stdout=stdout,
            stderr=stderr,
        )

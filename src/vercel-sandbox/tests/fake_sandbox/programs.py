"""Programs a fake sandbox session can run.

A real sandbox runs arbitrary Linux binaries; the fake runs a small set of
built-ins plus whatever a test registers. The built-ins cover what the SDK
itself executes (its filesystem helper scripts) and what the shared scenarios
use (`printf`, `sleep`, `python`, a line-oriented shell).
"""

from __future__ import annotations

import os
import posixpath
import re
import shlex
import subprocess
import sys
import tempfile
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal, Protocol

from vercel.sandbox._internal.service import (
    _LISTDIR_SCRIPT,
    _PREDICATE_SCRIPT,
    _REMOVE_SCRIPT,
    _RENAME_SCRIPT,
)

from .filesystem import FakeFilesystem, FilesystemError

Stream = Literal["stdout", "stderr"]


@dataclass
class ProgramResult:
    """What a program printed, how it exited, and how long it ran."""

    output: list[tuple[Stream, str]] = field(default_factory=list)
    exit_code: int = 0
    duration: float = 0.0

    @classmethod
    def of(
        cls,
        *,
        stdout: str = "",
        stderr: str = "",
        exit_code: int = 0,
        duration: float = 0.0,
    ) -> ProgramResult:
        output: list[tuple[Stream, str]] = []
        if stdout:
            output.append(("stdout", stdout))
        if stderr:
            output.append(("stderr", stderr))
        return cls(output=output, exit_code=exit_code, duration=duration)

    def stdout(self) -> str:
        return "".join(data for stream, data in self.output if stream == "stdout")


@dataclass
class ProgramContext:
    """The process a program runs as."""

    name: str
    args: list[str]
    cwd: str
    env: Mapping[str, str]
    fs: FakeFilesystem
    programs: ProgramRegistry
    tty: bool = False

    def path(self, value: str) -> str:
        return FakeFilesystem.resolve(self.cwd, value)

    def run(self, argv: Sequence[str]) -> ProgramResult:
        """Run another program in the same process context."""
        return self.programs.run(list(argv), cwd=self.cwd, env=self.env, fs=self.fs, tty=self.tty)


class Program(Protocol):
    def __call__(self, context: ProgramContext) -> ProgramResult: ...


class ExecutableNotFound(Exception):
    """The command does not name a program the fake can run."""


@dataclass
class _Handler:
    tokens: list[str]
    program: Program

    def matches(self, name: str, args: list[str]) -> bool:
        return [name, *args[: len(self.tokens) - 1]] == self.tokens


class ProgramRegistry:
    """Resolve a command line to a program: registered handlers, then built-ins."""

    def __init__(self) -> None:
        self._handlers: list[_Handler] = []
        self._builtins: dict[str, Program] = dict(_BUILTINS)

    def register(
        self,
        pattern: str,
        program: Program | ProgramResult,
    ) -> None:
        """Handle commands starting with ``pattern``, newest registration first.

        ``pattern`` is a command name optionally followed by leading
        arguments, so ``"npm install"`` also matches ``npm install --save``.
        """
        tokens = shlex.split(pattern)
        if not tokens:
            raise ValueError("command pattern must not be empty")
        if isinstance(program, ProgramResult):
            result = program
            self._handlers.insert(0, _Handler(tokens, lambda _context: result))
        else:
            self._handlers.insert(0, _Handler(tokens, program))

    def reset(self) -> None:
        self._handlers.clear()

    def resolve(self, name: str, args: list[str]) -> Program:
        for handler in self._handlers:
            if handler.matches(name, args):
                return handler.program
        builtin = self._builtins.get(posixpath.basename(name))
        if builtin is None:
            raise ExecutableNotFound(name)
        return builtin

    def run(
        self,
        argv: list[str],
        *,
        cwd: str,
        env: Mapping[str, str],
        fs: FakeFilesystem,
        tty: bool = False,
    ) -> ProgramResult:
        name, *args = argv
        program = self.resolve(name, args)
        return program(
            ProgramContext(name=name, args=args, cwd=cwd, env=env, fs=fs, programs=self, tty=tty)
        )


def _printf_escapes(value: str) -> str:
    replacements = {"n": "\n", "t": "\t", "r": "\r", "\\": "\\", "0": "\0"}
    return re.sub(r"\\(.)", lambda m: replacements.get(m.group(1), m.group(0)), value)


def _printf(context: ProgramContext) -> ProgramResult:
    if not context.args:
        return ProgramResult.of(stderr="printf: missing operand\n", exit_code=1)
    template, *values = context.args
    remaining = iter(values)

    def substitute(match: re.Match[str]) -> str:
        if match.group(0) == "%%":
            return "%"
        return next(remaining, "")

    return ProgramResult.of(stdout=re.sub(r"%%|%[sd]", substitute, _printf_escapes(template)))


def _echo(context: ProgramContext) -> ProgramResult:
    args = context.args
    newline = "\n"
    if args[:1] == ["-n"]:
        args, newline = args[1:], ""
    return ProgramResult.of(stdout=" ".join(args) + newline)


def _cat(context: ProgramContext) -> ProgramResult:
    chunks: list[str] = []
    for value in context.args:
        try:
            chunks.append(context.fs.read(context.path(value)).decode())
        except FilesystemError:
            return ProgramResult.of(
                stdout="".join(chunks),
                stderr=f"cat: {value}: No such file or directory\n",
                exit_code=1,
            )
    return ProgramResult.of(stdout="".join(chunks))


def _sleep(context: ProgramContext) -> ProgramResult:
    return ProgramResult(duration=float(context.args[0]) if context.args else 0.0)


def _tty(context: ProgramContext) -> ProgramResult:
    if context.tty:
        return ProgramResult.of(stdout="/dev/pts/0\n")
    return ProgramResult.of(stdout="not a tty\n", exit_code=1)


def _pwd(context: ProgramContext) -> ProgramResult:
    return ProgramResult.of(stdout=context.cwd + "\n")


def _predicate(context: ProgramContext, path: str, operator: str) -> ProgramResult:
    target = context.path(path)
    check = {"-e": context.fs.exists, "-f": context.fs.is_file, "-d": context.fs.is_dir}[operator]
    return ProgramResult(exit_code=0 if check(target) else 1)


def _listdir(context: ProgramContext, path: str) -> ProgramResult:
    try:
        entries = context.fs.listdir(context.path(path))
    except FilesystemError:
        return ProgramResult(exit_code=1)
    return ProgramResult.of(stdout="".join(f"{name}\0{kind}\0" for name, kind in entries))


def _remove(context: ProgramContext, path: str, recursive: str, missing_ok: str) -> ProgramResult:
    target = context.path(path)
    if not context.fs.exists(target):
        return ProgramResult(exit_code=0 if missing_ok == "true" else 1)
    try:
        context.fs.remove(target, recursive=recursive == "true")
    except FilesystemError:
        return ProgramResult.of(stderr=f"rm: cannot remove '{path}': Is a directory\n", exit_code=1)
    return ProgramResult()


def _rename(context: ProgramContext, source: str, destination: str) -> ProgramResult:
    try:
        context.fs.rename(context.path(source), context.path(destination))
    except FilesystemError:
        return ProgramResult.of(
            stderr=f"mv: cannot stat '{source}': No such file or directory\n", exit_code=1
        )
    return ProgramResult()


_SDK_SCRIPTS: dict[str, Callable[..., ProgramResult]] = {
    _PREDICATE_SCRIPT: _predicate,
    _LISTDIR_SCRIPT: _listdir,
    _REMOVE_SCRIPT: _remove,
    _RENAME_SCRIPT: _rename,
}


def run_shell_line(context: ProgramContext, line: str) -> tuple[ProgramResult, int | None]:
    """Run ``;``-separated commands; also report an ``exit`` status, if any."""
    lexer = shlex.shlex(line, posix=True, punctuation_chars=";")
    lexer.whitespace_split = True
    commands: list[list[str]] = [[]]
    for token in lexer:
        if token == ";":
            commands.append([])
        else:
            commands[-1].append(token)
    combined = ProgramResult()
    for argv in (command for command in commands if command):
        if argv[0] == "exit":
            code = int(argv[1]) if len(argv) > 1 else combined.exit_code
            return combined, code
        try:
            result = context.run(argv)
        except ExecutableNotFound:
            result = ProgramResult.of(stderr=f"sh: {argv[0]}: command not found\n", exit_code=127)
        combined.output.extend(result.output)
        combined.exit_code = result.exit_code
        combined.duration += result.duration
    return combined, None


def _shell(context: ProgramContext) -> ProgramResult:
    if context.args[:1] != ["-c"] or len(context.args) < 2:
        return ProgramResult.of(stderr="sh: only `-c` is supported\n", exit_code=2)
    script, *positional = context.args[1:]
    sdk_script = _SDK_SCRIPTS.get(script)
    if sdk_script is not None:
        # The first positional argument is $0, the script's name.
        return sdk_script(context, *positional[1:])
    result, exit_code = run_shell_line(context, script)
    if exit_code is not None:
        result.exit_code = exit_code
    return result


def _host_python(context: ProgramContext) -> ProgramResult:
    """Run real Python against a copy of the session filesystem.

    The filesystem is written to a temporary directory, the interpreter runs
    there with the session's working directory, and whatever it leaves behind
    becomes the filesystem. Scripts must use relative paths: an absolute path
    would reach the host, not the sandbox.
    """
    with tempfile.TemporaryDirectory(prefix="fake-sandbox-") as directory:
        root = Path(directory) / "root"
        context.fs.materialize(root)
        cwd = root / context.cwd.lstrip("/")
        cwd.mkdir(parents=True, exist_ok=True)
        environment = {
            key: value
            for key, value in os.environ.items()
            if key in {"PATH", "SYSTEMROOT", "TMPDIR", "TEMP", "TMP"}
        }
        environment.update(context.env)
        environment["PYTHONIOENCODING"] = "utf-8"
        completed = subprocess.run(
            [sys.executable, *context.args],
            cwd=cwd,
            env=environment,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            timeout=60,
        )
        context.fs.absorb(root)
    return ProgramResult.of(
        stdout=completed.stdout.decode(),
        stderr=completed.stderr.decode(),
        exit_code=completed.returncode,
    )


_BUILTINS: dict[str, Program] = {
    "bash": _shell,
    "cat": _cat,
    "echo": _echo,
    "false": lambda _context: ProgramResult(exit_code=1),
    "printf": _printf,
    "pwd": _pwd,
    "python": _host_python,
    "python3": _host_python,
    "sh": _shell,
    "sleep": _sleep,
    "true": lambda _context: ProgramResult(),
    "tty": _tty,
}


__all__ = [
    "ExecutableNotFound",
    "Program",
    "ProgramContext",
    "ProgramRegistry",
    "ProgramResult",
    "run_shell_line",
]

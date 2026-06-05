"""Output routing for completed Sandbox processes."""

import io
import subprocess
import sys
from dataclasses import dataclass, field
from typing import TextIO

from vercel._internal.unstable.sandbox.models import ProcessLog, ProcessLogStream


@dataclass(slots=True)
class _OutputTarget:
    stream: TextIO | None = None
    chunks: list[str] | None = field(default=None)

    def write(self, data: str) -> None:
        if self.chunks is not None:
            self.chunks.append(data)
        elif self.stream is not None:
            self.stream.write(data)
            self.stream.flush()

    def captured(self) -> str | None:
        return None if self.chunks is None else "".join(self.chunks)


class ProcessOutputRouter:
    """Resolve and route stdlib-style process output destinations."""

    __slots__ = ("_stderr", "_stdout")

    def __init__(
        self,
        *,
        stdout: TextIO | int | None,
        stderr: TextIO | int | None,
        capture_output: bool,
    ) -> None:
        if capture_output and (stdout is not None or stderr is not None):
            raise ValueError("stdout and stderr arguments may not be used with capture_output.")
        if stdout == subprocess.STDOUT:
            raise ValueError("STDOUT is only supported for stderr")
        if capture_output:
            stdout = subprocess.PIPE
            stderr = subprocess.PIPE

        self._stdout = _resolve_target(stdout, inherited=sys.stdout, name="stdout")
        if stderr == subprocess.STDOUT:
            self._stderr = self._stdout
        else:
            self._stderr = _resolve_target(stderr, inherited=sys.stderr, name="stderr")

    def route(self, event: ProcessLog) -> None:
        target = self._stdout if event.stream is ProcessLogStream.STDOUT else self._stderr
        target.write(event.data)

    def captured(self) -> tuple[str | None, str | None]:
        stdout = self._stdout.captured()
        stderr = None if self._stderr is self._stdout else self._stderr.captured()
        return stdout, stderr


def _resolve_target(
    destination: TextIO | int | None, *, inherited: TextIO, name: str
) -> _OutputTarget:
    if destination is None:
        return _OutputTarget(stream=inherited)
    if destination == subprocess.PIPE:
        return _OutputTarget(chunks=[])
    if destination == subprocess.DEVNULL:
        return _OutputTarget()
    if isinstance(destination, int):
        raise ValueError(f"unsupported {name} value: {destination}")
    if isinstance(destination, (io.RawIOBase, io.BufferedIOBase, io.BytesIO)):
        raise TypeError(f"{name} must be a text stream")

    write = getattr(destination, "write", None)
    flush = getattr(destination, "flush", None)
    if not callable(write) or not callable(flush):
        raise TypeError(f"{name} must be a writable text stream or subprocess sentinel")
    writable = getattr(destination, "writable", None)
    if callable(writable) and not writable():
        raise ValueError(f"{name} must be writable")
    return _OutputTarget(stream=destination)

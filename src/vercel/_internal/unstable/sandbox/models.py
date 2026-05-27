"""Public Sandbox handle skeletons backed by internal implementation."""

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class Sandbox:
    name: str
    runtime: str | None = None
    status: str | None = None

    def session(self) -> Any:
        raise NotImplementedError("Sandbox runtime sessions are not implemented yet")

    async def run_command(self, command: str, args: list[str] | None = None) -> Any:
        raise NotImplementedError("Sandbox commands are not implemented yet")


@dataclass(frozen=True, slots=True)
class SandboxRuntimeSession:
    id: str
    sandbox_name: str | None = None
    status: str | None = None

    async def run_command(self, command: str, args: list[str] | None = None) -> Any:
        raise NotImplementedError("Sandbox runtime-session commands are not implemented yet")

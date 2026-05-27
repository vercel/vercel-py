"""Sync mirror for the experimental Sandbox SDK surface."""

from typing import Any

from vercel._internal.unstable.sandbox.errors import (
    SandboxApiError,
    SandboxError,
    SandboxInvalidHandleError,
    SandboxTerminalStateError,
)
from vercel._internal.unstable.sandbox.models import Sandbox, SandboxRuntimeSession
from vercel._internal.unstable.sandbox.options import SandboxServiceOptions


def create_sandbox(**kwargs: Any) -> Sandbox:
    raise NotImplementedError("sandbox.sync.create_sandbox(...) is not implemented yet")


def get_sandbox(**kwargs: Any) -> Sandbox:
    raise NotImplementedError("sandbox.sync.get_sandbox(...) is not implemented yet")


def query_sandboxes(**kwargs: Any) -> list[Sandbox]:
    raise NotImplementedError("sandbox.sync.query_sandboxes(...) is not implemented yet")


__all__ = [
    "Sandbox",
    "SandboxApiError",
    "SandboxError",
    "SandboxInvalidHandleError",
    "SandboxRuntimeSession",
    "SandboxServiceOptions",
    "SandboxTerminalStateError",
    "create_sandbox",
    "get_sandbox",
    "query_sandboxes",
]

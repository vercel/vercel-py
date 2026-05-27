"""Experimental Sandbox SDK surface."""

from typing import Any

from vercel._internal.unstable.context import get_active_session
from vercel._internal.unstable.sandbox.errors import (
    SandboxApiError,
    SandboxError,
    SandboxInvalidHandleError,
    SandboxTerminalStateError,
)
from vercel._internal.unstable.sandbox.models import Sandbox, SandboxRuntimeSession
from vercel._internal.unstable.sandbox.operations import (
    CreateSandboxOperation,
    create_sandbox_operation,
)
from vercel._internal.unstable.sandbox.options import SandboxServiceOptions

from . import sync


def create_sandbox(**kwargs: Any) -> CreateSandboxOperation:
    return create_sandbox_operation(**kwargs)


async def get_sandbox(**kwargs: Any) -> Sandbox:
    return await get_active_session().sandbox_service().get_sandbox(**kwargs)


async def query_sandboxes(**kwargs: Any) -> list[Sandbox]:
    return await get_active_session().sandbox_service().query_sandboxes(**kwargs)


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
    "sync",
]

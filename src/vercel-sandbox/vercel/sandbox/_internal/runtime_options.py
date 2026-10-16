"""Runtime collaborators the Sandbox service is composed with.

Production wiring uses the defaults: the session's own sleep and the WebSocket
interactive connector. Tests replace them through the same SDK session the
service is built from, so there is one composition root and no module state
to patch.
"""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from vercel._internal.core.options import ServiceOptions
from vercel.sandbox._internal.interactive_session import InteractiveConnector

AsyncSleep = Callable[[float], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class SandboxRuntimeOptions(ServiceOptions):
    """Override how a session's Sandbox service waits and connects.

    ``sleep`` must not suspend when the session is synchronous, because the
    sync runtime drives the service with ``iter_coroutine``.
    """

    sleep: AsyncSleep | None = None
    interactive: InteractiveConnector | None = None


__all__ = ["AsyncSleep", "SandboxRuntimeOptions"]

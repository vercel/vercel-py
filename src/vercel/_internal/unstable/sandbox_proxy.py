"""Default-bound Sandbox accessor proxy placeholder."""

from __future__ import annotations

from dataclasses import dataclass

from vercel._internal.unstable.session import Session, SyncSession
from vercel.unstable.sandbox import SandboxOptions


@dataclass(frozen=True, slots=True)
class SandboxAccessorProxy:
    """Lazily resolves the effective session when operations are implemented."""

    options: SandboxOptions | None = None

    def with_options(self, options: SandboxOptions | None = None) -> SandboxAccessorProxy:
        return SandboxAccessorProxy(options=options)

    def with_session(self, session: Session | SyncSession) -> object:
        _ = session
        raise NotImplementedError("session-bound Sandbox accessors will be implemented later")


sandbox = SandboxAccessorProxy()

__all__ = ["SandboxAccessorProxy", "sandbox"]

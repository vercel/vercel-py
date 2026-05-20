"""Default-bound Sandbox accessor proxy."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import overload

from vercel._internal.unstable.default import get_default_session
from vercel._internal.unstable.sandbox.accessor import (
    SandboxAccessor,
    SyncSandboxAccessor,
    merge_sandbox_options,
)
from vercel._internal.unstable.sandbox.types import (
    Sandbox,
    SandboxCreateParams,
    SandboxOptions,
)
from vercel._internal.unstable.session import Session, SyncSession


@dataclass(frozen=True, slots=True)
class SandboxAccessorProxy:
    """Lazily resolves the effective session when I/O operations run."""

    options: SandboxOptions | None = None

    def with_options(self, options: SandboxOptions | None = None) -> SandboxAccessorProxy:
        return SandboxAccessorProxy(options=merge_sandbox_options(self.options, options))

    @overload
    def with_session(self, session: Session) -> SandboxAccessor: ...

    @overload
    def with_session(self, session: SyncSession) -> SyncSandboxAccessor: ...

    def with_session(self, session: Session | SyncSession) -> SandboxAccessor | SyncSandboxAccessor:
        if isinstance(session, Session):
            return SandboxAccessor(session, options=self.options)
        if isinstance(session, SyncSession):
            return SyncSandboxAccessor(session, options=self.options)
        raise TypeError("session must be Session or SyncSession")

    async def create(
        self,
        params: SandboxCreateParams,
        *,
        wait: bool = False,
        timeout: timedelta | None = None,
    ) -> Sandbox:
        """Create a sandbox through the effective default session."""
        session = get_default_session()
        accessor = session.sandbox.with_options(self.options)
        return await accessor.create(params, wait=wait, timeout=timeout)


sandbox = SandboxAccessorProxy()

__all__ = ["SandboxAccessorProxy", "sandbox"]

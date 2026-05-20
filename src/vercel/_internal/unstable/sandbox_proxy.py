"""Default-bound Sandbox accessor proxy."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from vercel._internal.unstable.default import get_default_session
from vercel._internal.unstable.sandbox.accessor import (
    SandboxAccessor,
    merge_sandbox_options,
)
from vercel._internal.unstable.sandbox.models import Sandbox
from vercel._internal.unstable.sandbox.options import SandboxOptions
from vercel._internal.unstable.sandbox.params import SandboxCreateParams
from vercel._internal.unstable.session import Session


@dataclass(frozen=True, slots=True)
class SandboxAccessorProxy:
    """Lazily resolves the effective session when I/O operations run."""

    options: SandboxOptions | None = None

    def with_options(self, options: SandboxOptions | None = None) -> SandboxAccessorProxy:
        return SandboxAccessorProxy(options=merge_sandbox_options(self.options, options))

    def with_session(self, session: Session) -> SandboxAccessor:
        return SandboxAccessor(session, options=self.options)

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

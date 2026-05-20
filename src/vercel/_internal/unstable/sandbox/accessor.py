"""Session-bound Sandbox accessors for the unstable SDK surface."""

from __future__ import annotations

from dataclasses import fields, replace
from typing import TYPE_CHECKING, overload

from vercel._internal.iter_coroutine import iter_coroutine
from vercel._internal.unstable.sandbox.types import (
    Sandbox,
    SandboxCreateParams,
    SandboxError,
    SandboxOptions,
    SyncSandbox,
)

if TYPE_CHECKING:
    from vercel._internal.unstable.sandbox.ops import (
        SyncUnstableSandboxOpsClient,
        UnstableSandboxOpsClient,
    )
    from vercel._internal.unstable.session import Session, SyncSession


def merge_sandbox_options(
    base: SandboxOptions | None,
    override: SandboxOptions | None,
) -> SandboxOptions | None:
    """Merge option layers without resolving credentials or runtime state."""

    if override is None:
        return base
    if base is None:
        base = SandboxOptions()
    changes = {
        field.name: value
        for field in fields(SandboxOptions)
        if (value := getattr(override, field.name)) is not None
    }
    if not changes:
        return base
    return replace(base, **changes)


class SandboxAccessor:
    """Async Sandbox accessor bound to an explicit session."""

    def __init__(self, session: Session, *, options: SandboxOptions | None = None) -> None:
        self._session = session
        self.options = options
        self._ops_client: UnstableSandboxOpsClient | None = None
        self._close_hook_registered = False

    def with_options(self, options: SandboxOptions | None = None) -> SandboxAccessor:
        return SandboxAccessor(
            self._session,
            options=merge_sandbox_options(self.options, options),
        )

    @overload
    def with_session(self, session: Session) -> SandboxAccessor: ...

    @overload
    def with_session(self, session: SyncSession) -> SyncSandboxAccessor: ...

    def with_session(self, session: Session | SyncSession) -> SandboxAccessor | SyncSandboxAccessor:
        from vercel._internal.unstable.session import Session, SyncSession

        if isinstance(session, Session):
            return SandboxAccessor(session, options=self.options)
        if isinstance(session, SyncSession):
            return SyncSandboxAccessor(session, options=self.options)
        raise TypeError("session must be Session or SyncSession")

    def _prepare(self) -> None:
        self._session._ensure_open()

    async def create(self, params: SandboxCreateParams, *, wait: bool = False) -> Sandbox:
        if wait:
            raise SandboxError("sandbox create wait=True is not implemented yet")
        self._prepare()
        sandbox = await self._get_ops_client().create(params)
        sandbox._session = self._session
        return sandbox

    def _get_ops_client(self) -> UnstableSandboxOpsClient:
        if self._ops_client is None:
            from vercel._internal.unstable.sandbox.ops import (
                UnstableSandboxOpsClient,
            )

            self._ops_client = UnstableSandboxOpsClient(
                options=self.options,
                transport=self._session._sandbox_transport,
            )
            if not self._close_hook_registered:
                self._session._add_close_hook(self._close)
                self._close_hook_registered = True
        return self._ops_client

    async def _close(self) -> None:
        if self._ops_client is not None:
            await self._ops_client.aclose()
            self._ops_client = None
        self._close_hook_registered = False


class SyncSandboxAccessor:
    """Sync Sandbox accessor bound to an explicit session."""

    def __init__(self, session: SyncSession, *, options: SandboxOptions | None = None) -> None:
        self._session = session
        self.options = options
        self._ops_client: SyncUnstableSandboxOpsClient | None = None
        self._close_hook_registered = False

    def with_options(self, options: SandboxOptions | None = None) -> SyncSandboxAccessor:
        return SyncSandboxAccessor(
            self._session,
            options=merge_sandbox_options(self.options, options),
        )

    @overload
    def with_session(self, session: Session) -> SandboxAccessor: ...

    @overload
    def with_session(self, session: SyncSession) -> SyncSandboxAccessor: ...

    def with_session(self, session: Session | SyncSession) -> SandboxAccessor | SyncSandboxAccessor:
        from vercel._internal.unstable.session import Session, SyncSession

        if isinstance(session, Session):
            return SandboxAccessor(session, options=self.options)
        if isinstance(session, SyncSession):
            return SyncSandboxAccessor(session, options=self.options)
        raise TypeError("session must be Session or SyncSession")

    def _prepare(self) -> None:
        self._session._ensure_open()

    def create(self, params: SandboxCreateParams, *, wait: bool = False) -> SyncSandbox:
        if wait:
            raise SandboxError("sandbox create wait=True is not implemented yet")
        self._prepare()
        sandbox = iter_coroutine(self._get_ops_client().create(params))
        return SyncSandbox(
            name=sandbox.name,
            persistent=sandbox.persistent,
            current_snapshot_id=sandbox.current_snapshot_id,
            current_session=sandbox.current_session,
            routes=sandbox.routes,
            _session=self._session,
            _raw=sandbox._raw,
        )

    def _get_ops_client(self) -> SyncUnstableSandboxOpsClient:
        if self._ops_client is None:
            from vercel._internal.unstable.sandbox.ops import (
                SyncUnstableSandboxOpsClient,
            )

            self._ops_client = SyncUnstableSandboxOpsClient(
                options=self.options,
                transport=self._session._sandbox_transport,
            )
            if not self._close_hook_registered:
                self._session._add_close_hook(self._close)
                self._close_hook_registered = True
        return self._ops_client

    def _close(self) -> None:
        if self._ops_client is not None:
            self._ops_client.close()
            self._ops_client = None
        self._close_hook_registered = False


__all__ = [
    "SandboxAccessor",
    "SyncSandboxAccessor",
    "merge_sandbox_options",
]

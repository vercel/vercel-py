"""Session-bound Sandbox accessors for the unstable SDK surface."""

from __future__ import annotations

import asyncio
import time
from dataclasses import fields, replace
from datetime import timedelta
from typing import TYPE_CHECKING, overload

from vercel._internal.iter_coroutine import iter_coroutine
from vercel._internal.unstable.sandbox.types import (
    Sandbox,
    SandboxCreateParams,
    SandboxError,
    SandboxOperationTimeoutError,
    SandboxOptions,
    SandboxTerminalStateError,
    SyncSandbox,
    is_ready_for_create,
    is_terminal_for_create,
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

    async def create(
        self,
        params: SandboxCreateParams,
        *,
        wait: bool = False,
        timeout: timedelta | None = None,
    ) -> Sandbox:
        self._prepare()

        async def _create_and_wait() -> Sandbox:
            sandbox = await self._get_ops_client().create(params)
            if not wait:
                return sandbox
            if sandbox.current_session is None:
                raise SandboxError("sandbox create returned response with no session")
            status = sandbox.current_session.status
            if is_ready_for_create(status):
                return sandbox
            if is_terminal_for_create(status):
                if status is None:
                    raise SandboxError("unexpected None status in terminal state check")
                raise SandboxTerminalStateError(
                    f"sandbox create reached terminal state {status.value}"
                )
            while True:
                await asyncio.sleep(1)
                polled = await self._get_ops_client().get_sandbox(sandbox.name)
                sandbox.current_session = polled.current_session
                sandbox._raw = polled._raw
                poll_status = polled.current_session.status if polled.current_session else None
                if is_ready_for_create(poll_status):
                    return sandbox
                if is_terminal_for_create(poll_status):
                    status_label = poll_status.value if poll_status else "unknown"
                    raise SandboxTerminalStateError(
                        f"sandbox create reached terminal state {status_label}"
                    )

        if timeout is not None:
            try:
                async with asyncio.timeout(timeout.total_seconds()):
                    sandbox = await _create_and_wait()
            except TimeoutError:
                raise SandboxOperationTimeoutError(
                    f"sandbox create exceeded timeout of {timeout.total_seconds()}s"
                ) from None
        else:
            sandbox = await _create_and_wait()
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
        self._prepare()
        sandbox = iter_coroutine(self._get_ops_client().create(params))
        if wait:
            if sandbox.current_session is None:
                raise SandboxError("sandbox create returned response with no session")
            status = sandbox.current_session.status
            if not is_ready_for_create(status) and not is_terminal_for_create(status):
                while True:
                    time.sleep(1)
                    polled = iter_coroutine(self._get_ops_client().get_sandbox(sandbox.name))
                    sandbox.current_session = polled.current_session
                    sandbox._raw = polled._raw
                    poll_status = polled.current_session.status if polled.current_session else None
                    if is_ready_for_create(poll_status):
                        break
                    if is_terminal_for_create(poll_status):
                        status_label = poll_status.value if poll_status else "unknown"
                        raise SandboxTerminalStateError(
                            f"sandbox create reached terminal state {status_label}"
                        )
            elif is_terminal_for_create(status):
                if status is None:
                    raise SandboxError("unexpected None status in terminal state check")
                raise SandboxTerminalStateError(
                    f"sandbox create reached terminal state {status.value}"
                )
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

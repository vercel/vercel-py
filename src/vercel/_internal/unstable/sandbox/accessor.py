"""Session-bound Sandbox accessors for the unstable SDK surface."""

from __future__ import annotations

import time
from dataclasses import fields, replace
from datetime import timedelta
from typing import TYPE_CHECKING

import anyio

from vercel._internal.iter_coroutine import iter_coroutine
from vercel._internal.unstable.sandbox.api_client import (
    SandboxApiClient,
    create_sandbox_credentials_resolver,
    create_sync_sandbox_credentials_resolver,
)
from vercel._internal.unstable.sandbox.errors import (
    SandboxOperationTimeoutError,
)
from vercel._internal.unstable.sandbox.lifecycle import create_sandbox_with_wait
from vercel._internal.unstable.sandbox.models import (
    Sandbox,
    SyncSandbox,
)
from vercel._internal.unstable.sandbox.options import SandboxOptions
from vercel._internal.unstable.sandbox.params import SandboxCreateParams

if TYPE_CHECKING:
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
        self._api_client: SandboxApiClient | None = None
        self.options = options

    def with_options(self, options: SandboxOptions | None = None) -> SandboxAccessor:
        return SandboxAccessor(
            self._session,
            options=merge_sandbox_options(self.options, options),
        )

    def with_session(self, session: Session) -> SandboxAccessor:
        return SandboxAccessor(session, options=self.options)

    async def create(
        self,
        params: SandboxCreateParams,
        *,
        wait: bool = False,
        timeout: timedelta | None = None,
    ) -> Sandbox:
        api_client = await self._get_api_client()

        async def _create() -> Sandbox:
            return await create_sandbox_with_wait(
                api_client,
                params,
                wait=wait,
                timeout=timeout,
                sleep_fn=anyio.sleep,
                monotonic_fn=time.monotonic,
            )

        if timeout is not None:
            timeout_seconds = timeout.total_seconds()
            try:
                with anyio.fail_after(timeout_seconds):
                    sandbox = await _create()
            except TimeoutError:
                raise SandboxOperationTimeoutError(
                    f"sandbox create exceeded timeout of {timeout_seconds}s"
                ) from None
        else:
            sandbox = await _create()
        sandbox._session = self._session
        return sandbox

    async def _get_api_client(self) -> SandboxApiClient:
        await self._session.initialize()
        if self._session._transport is None:
            raise AssertionError("session transport is not initialized")
        if self._api_client is None:
            self._api_client = SandboxApiClient(
                transport=self._session._transport,
                credentials_resolver=create_sandbox_credentials_resolver(self.options),
                sleep_fn=anyio.sleep,
                api_url=self.options.api_url if self.options is not None else None,
                request_timeout=self.options.request_timeout if self.options is not None else None,
                retry_attempts=self.options.retry_attempts if self.options is not None else None,
            )
        return self._api_client


class SyncSandboxAccessor:
    """Sync Sandbox accessor bound to an explicit session."""

    def __init__(self, session: SyncSession, *, options: SandboxOptions | None = None) -> None:
        self._session = session
        self._api_client: SandboxApiClient | None = None
        self.options = options

    def with_options(self, options: SandboxOptions | None = None) -> SyncSandboxAccessor:
        return SyncSandboxAccessor(
            self._session,
            options=merge_sandbox_options(self.options, options),
        )

    def with_session(self, session: SyncSession) -> SyncSandboxAccessor:
        return SyncSandboxAccessor(session, options=self.options)

    def create(
        self,
        params: SandboxCreateParams,
        *,
        wait: bool = False,
        timeout: timedelta | None = None,
    ) -> SyncSandbox:
        api_client = self._get_api_client()
        sandbox = iter_coroutine(
            create_sandbox_with_wait(
                api_client,
                params,
                wait=wait,
                timeout=timeout,
                sleep_fn=time.sleep,
                monotonic_fn=time.monotonic,
            )
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

    def _get_api_client(self) -> SandboxApiClient:
        self._session.initialize()
        if self._session._transport is None:
            raise AssertionError("session transport is not initialized")
        if self._api_client is None:
            self._api_client = SandboxApiClient(
                transport=self._session._transport,
                credentials_resolver=create_sync_sandbox_credentials_resolver(self.options),
                sleep_fn=time.sleep,
                api_url=self.options.api_url if self.options is not None else None,
                request_timeout=self.options.request_timeout if self.options is not None else None,
                retry_attempts=self.options.retry_attempts if self.options is not None else None,
            )
        return self._api_client


__all__ = [
    "SandboxAccessor",
    "SyncSandboxAccessor",
    "merge_sandbox_options",
]

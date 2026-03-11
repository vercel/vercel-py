"""Public sandbox client placeholders for the stable root client."""

from __future__ import annotations

from dataclasses import dataclass

from vercel._internal.stable.options import merge_dataclass_options
from vercel._internal.stable.runtime import AsyncRuntime, SyncRuntime
from vercel.stable.options import SandboxOptions


@dataclass(frozen=True, slots=True)
class SyncSandboxClient:
    _runtime: SyncRuntime
    _options: SandboxOptions

    def with_options(
        self,
        *,
        token: str | None = None,
        base_url: str | None = None,
        team_id: str | None = None,
        team_slug: str | None = None,
    ) -> "SyncSandboxClient":
        return SyncSandboxClient(
            _runtime=self._runtime,
            _options=merge_dataclass_options(
                self._options,
                token=token,
                base_url=base_url,
                team_id=team_id,
                team_slug=team_slug,
            ),
        )


@dataclass(frozen=True, slots=True)
class AsyncSandboxClient:
    _runtime: AsyncRuntime
    _options: SandboxOptions

    def with_options(
        self,
        *,
        token: str | None = None,
        base_url: str | None = None,
        team_id: str | None = None,
        team_slug: str | None = None,
    ) -> "AsyncSandboxClient":
        return AsyncSandboxClient(
            _runtime=self._runtime,
            _options=merge_dataclass_options(
                self._options,
                token=token,
                base_url=base_url,
                team_id=team_id,
                team_slug=team_slug,
            ),
        )


__all__ = ["SyncSandboxClient", "AsyncSandboxClient"]

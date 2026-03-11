"""Public SDK family wrappers for the stable client surface."""

from __future__ import annotations

from dataclasses import dataclass

from vercel._internal.stable.options import merge_dataclass_options, merge_mapping
from vercel._internal.stable.runtime import AsyncRuntime, SyncRuntime
from vercel.stable.options import SdkOptions


@dataclass(frozen=True, slots=True)
class SyncSdk:
    _runtime: SyncRuntime
    _options: SdkOptions

    def with_options(
        self,
        *,
        token: str | None = None,
        base_url: str | None = None,
        team_id: str | None = None,
        team_slug: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> "SyncSdk":
        merged_headers = merge_mapping(self._options.headers, headers)
        return SyncSdk(
            _runtime=self._runtime,
            _options=merge_dataclass_options(
                self._options,
                token=token,
                base_url=base_url,
                team_id=team_id,
                team_slug=team_slug,
                headers=merged_headers,
            ),
        )


@dataclass(frozen=True, slots=True)
class AsyncSdk:
    _runtime: AsyncRuntime
    _options: SdkOptions

    def with_options(
        self,
        *,
        token: str | None = None,
        base_url: str | None = None,
        team_id: str | None = None,
        team_slug: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> "AsyncSdk":
        merged_headers = merge_mapping(self._options.headers, headers)
        return AsyncSdk(
            _runtime=self._runtime,
            _options=merge_dataclass_options(
                self._options,
                token=token,
                base_url=base_url,
                team_id=team_id,
                team_slug=team_slug,
                headers=merged_headers,
            ),
        )


__all__ = ["SyncSdk", "AsyncSdk"]

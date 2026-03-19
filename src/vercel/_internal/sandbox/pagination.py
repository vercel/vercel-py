from __future__ import annotations

from dataclasses import dataclass

from vercel._internal.sandbox.models import Pagination


@dataclass(frozen=True, slots=True)
class SandboxPageInfo:
    until: int


@dataclass(frozen=True, slots=True)
class SandboxListParams:
    project: str | None = None
    limit: int | None = None
    since: int | None = None
    until: int | None = None

    def with_until(self, until: int) -> SandboxListParams:
        return SandboxListParams(
            project=self.project,
            limit=self.limit,
            since=self.since,
            until=until,
        )


def next_sandbox_page_info(pagination: Pagination) -> SandboxPageInfo | None:
    if pagination.next is None:
        return None
    return SandboxPageInfo(until=pagination.next)


__all__ = ["SandboxListParams", "SandboxPageInfo", "next_sandbox_page_info"]

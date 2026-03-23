"""Public stable SDK projects surface."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from vercel._internal.iter_coroutine import iter_coroutine
from vercel._internal.stable.pagination import AsyncPageIterator, PageIterator
from vercel._internal.stable.runtime import AsyncRuntime, SyncRuntime
from vercel._internal.stable.sdk.request_client import SdkClientLineage
from vercel.stable.options import ProjectWriteRequest
from vercel.stable.pagination import AsyncPaginator, Page, Paginator

if TYPE_CHECKING:
    from vercel._internal.stable.sdk.projects import ProjectsBackend


@dataclass(frozen=True, slots=True)
class Project:
    id: str
    name: str
    framework: str | None = None
    account_id: str | None = None
    created_at: int | None = None
    updated_at: int | None = None
    raw: Mapping[str, Any] = field(default_factory=dict, repr=False)

    def to_dict(self) -> dict[str, Any]:
        return dict(self.raw)


@dataclass(frozen=True, slots=True)
class ProjectPage(Page[Project]):
    raw: Mapping[str, Any] = field(default_factory=dict, repr=False)

    def to_dict(self) -> dict[str, Any]:
        return dict(self.raw)


@dataclass(frozen=True, slots=True)
class SyncProjectsClient:
    _lineage: SdkClientLineage
    _backend: ProjectsBackend
    _runtime: SyncRuntime = field(init=False, repr=False)
    _root_timeout: float | None = field(init=False, repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "_runtime", self._lineage.runtime)
        object.__setattr__(self, "_root_timeout", self._lineage.root_timeout)

    def ensure_connected(self) -> SyncProjectsClient:
        self._runtime.ensure_connected(timeout=self._root_timeout)
        return self

    def list(
        self,
        *,
        cursor: str | None = None,
        page_size: int | None = None,
        limit: int | None = None,
    ) -> ProjectPage:
        return iter_coroutine(self._backend.list(cursor=cursor, page_size=page_size, limit=limit))

    def get(
        self,
        id_or_name: str,
        *,
        team_id: str | None = None,
        team_slug: str | None = None,
    ) -> Project:
        return iter_coroutine(self._backend.get(id_or_name, team_id=team_id, team_slug=team_slug))

    def create(
        self,
        *,
        request: ProjectWriteRequest | None = None,
        body: dict[str, object] | None = None,
        name: str | None = None,
        framework: str | None = None,
        public_source: bool | None = None,
        build_command: str | None = None,
        dev_command: str | None = None,
        install_command: str | None = None,
        output_directory: str | None = None,
        root_directory: str | None = None,
    ) -> Project:
        return iter_coroutine(
            self._backend.create(
                request=request,
                body=body,
                name=name,
                framework=framework,
                public_source=public_source,
                build_command=build_command,
                dev_command=dev_command,
                install_command=install_command,
                output_directory=output_directory,
                root_directory=root_directory,
            )
        )

    def update(
        self,
        id_or_name: str,
        *,
        request: ProjectWriteRequest | None = None,
        body: dict[str, object] | None = None,
        name: str | None = None,
        framework: str | None = None,
        public_source: bool | None = None,
        build_command: str | None = None,
        dev_command: str | None = None,
        install_command: str | None = None,
        output_directory: str | None = None,
        root_directory: str | None = None,
    ) -> Project:
        return iter_coroutine(
            self._backend.update(
                id_or_name,
                request=request,
                body=body,
                name=name,
                framework=framework,
                public_source=public_source,
                build_command=build_command,
                dev_command=dev_command,
                install_command=install_command,
                output_directory=output_directory,
                root_directory=root_directory,
            )
        )

    def delete(
        self,
        id_or_name: str,
        *,
        team_id: str | None = None,
        team_slug: str | None = None,
    ) -> None:
        iter_coroutine(self._backend.delete(id_or_name, team_id=team_id, team_slug=team_slug))

    def iter_pages(
        self,
        *,
        cursor: str | None = None,
        page_size: int | None = None,
        limit: int | None = None,
    ) -> Paginator[ProjectPage, Project]:
        next_cursor = cursor
        remaining = limit

        def fetch_next() -> ProjectPage | None:
            nonlocal next_cursor, remaining
            page = iter_coroutine(
                self._backend.fetch_page(
                    cursor=next_cursor,
                    page_size=page_size,
                    remaining=remaining,
                )
            )
            if page is None:
                return None
            next_cursor = page.next_cursor
            if remaining is not None:
                remaining -= len(page.items)
                if remaining <= 0:
                    next_cursor = None
            return page

        return Paginator(PageIterator(fetch_next))


@dataclass(frozen=True, slots=True)
class AsyncProjectsClient:
    _lineage: SdkClientLineage
    _backend: ProjectsBackend
    _runtime: AsyncRuntime = field(init=False, repr=False)
    _root_timeout: float | None = field(init=False, repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "_runtime", self._lineage.runtime)
        object.__setattr__(self, "_root_timeout", self._lineage.root_timeout)

    async def ensure_connected(self) -> AsyncProjectsClient:
        await self._runtime.ensure_connected(timeout=self._root_timeout)
        return self

    async def list(
        self,
        *,
        cursor: str | None = None,
        page_size: int | None = None,
        limit: int | None = None,
    ) -> ProjectPage:
        return await self._backend.list(cursor=cursor, page_size=page_size, limit=limit)

    async def get(
        self,
        id_or_name: str,
        *,
        team_id: str | None = None,
        team_slug: str | None = None,
    ) -> Project:
        return await self._backend.get(id_or_name, team_id=team_id, team_slug=team_slug)

    async def create(
        self,
        *,
        request: ProjectWriteRequest | None = None,
        body: dict[str, object] | None = None,
        name: str | None = None,
        framework: str | None = None,
        public_source: bool | None = None,
        build_command: str | None = None,
        dev_command: str | None = None,
        install_command: str | None = None,
        output_directory: str | None = None,
        root_directory: str | None = None,
    ) -> Project:
        return await self._backend.create(
            request=request,
            body=body,
            name=name,
            framework=framework,
            public_source=public_source,
            build_command=build_command,
            dev_command=dev_command,
            install_command=install_command,
            output_directory=output_directory,
            root_directory=root_directory,
        )

    async def update(
        self,
        id_or_name: str,
        *,
        request: ProjectWriteRequest | None = None,
        body: dict[str, object] | None = None,
        name: str | None = None,
        framework: str | None = None,
        public_source: bool | None = None,
        build_command: str | None = None,
        dev_command: str | None = None,
        install_command: str | None = None,
        output_directory: str | None = None,
        root_directory: str | None = None,
    ) -> Project:
        return await self._backend.update(
            id_or_name,
            request=request,
            body=body,
            name=name,
            framework=framework,
            public_source=public_source,
            build_command=build_command,
            dev_command=dev_command,
            install_command=install_command,
            output_directory=output_directory,
            root_directory=root_directory,
        )

    async def delete(
        self,
        id_or_name: str,
        *,
        team_id: str | None = None,
        team_slug: str | None = None,
    ) -> None:
        await self._backend.delete(id_or_name, team_id=team_id, team_slug=team_slug)

    def iter_pages(
        self,
        *,
        cursor: str | None = None,
        page_size: int | None = None,
        limit: int | None = None,
    ) -> AsyncPaginator[ProjectPage, Project]:
        next_cursor = cursor
        remaining = limit

        async def fetch_next() -> ProjectPage | None:
            nonlocal next_cursor, remaining
            page = await self._backend.fetch_page(
                cursor=next_cursor,
                page_size=page_size,
                remaining=remaining,
            )
            if page is None:
                return None
            next_cursor = page.next_cursor
            if remaining is not None:
                remaining -= len(page.items)
                if remaining <= 0:
                    next_cursor = None
            return page

        return AsyncPaginator(AsyncPageIterator(fetch_next))


__all__ = [
    "Project",
    "ProjectPage",
    "SyncProjectsClient",
    "AsyncProjectsClient",
]

from __future__ import annotations

from typing import Any
from .projects import create_project, update_project, delete_project, get_projects
from .aio import (
    create_project as acreate_project,
    update_project as aupdate_project,
    delete_project as adelete_project,
    get_projects as aget_projects,
)


class ProjectsClient:
    def __init__(
        self,
        access_token: str | None = None,
        base_url: str | None = None,
        timeout: float | None = None,
    ):
        self._access_token = access_token
        self._base_url = base_url or "https://api.vercel.com"
        self._timeout = timeout or 30.0

    def create_project(
        self, *, body: dict[str, Any], team_id: str | None = None, slug: str | None = None
    ) -> dict[str, Any]:
        return create_project(
            body=body,
            token=self._access_token,
            team_id=team_id,
            slug=slug,
            base_url=self._base_url,
            timeout=self._timeout,
        )

    def update_project(
        self,
        *,
        id_or_name: str,
        body: dict[str, Any],
        team_id: str | None = None,
        slug: str | None = None,
    ) -> dict[str, Any]:
        return update_project(
            id_or_name=id_or_name,
            body=body,
            token=self._access_token,
            team_id=team_id,
            slug=slug,
            base_url=self._base_url,
            timeout=self._timeout,
        )

    def delete_project(
        self, *, id_or_name: str, team_id: str | None = None, slug: str | None = None
    ) -> None:
        return delete_project(
            id_or_name=id_or_name,
            token=self._access_token,
            team_id=team_id,
            slug=slug,
            base_url=self._base_url,
            timeout=self._timeout,
        )

    def get_projects(
        self,
        *,
        team_id: str | None = None,
        slug: str | None = None,
        query: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return get_projects(
            token=self._access_token,
            team_id=team_id,
            slug=slug,
            query=query,
            base_url=self._base_url,
            timeout=self._timeout,
        )


class AsyncProjectsClient:
    def __init__(
        self,
        access_token: str | None = None,
        base_url: str | None = None,
        timeout: float | None = None,
    ):
        self._access_token = access_token
        self._base_url = base_url or "https://api.vercel.com"
        self._timeout = timeout or 30.0

    async def create_project(
        self, *, body: dict[str, Any], team_id: str | None = None, slug: str | None = None
    ) -> dict[str, Any]:
        return await acreate_project(
            body=body,
            token=self._access_token,
            team_id=team_id,
            slug=slug,
            base_url=self._base_url,
            timeout=self._timeout,
        )

    async def update_project(
        self,
        *,
        id_or_name: str,
        body: dict[str, Any],
        team_id: str | None = None,
        slug: str | None = None,
    ) -> dict[str, Any]:
        return await aupdate_project(
            id_or_name=id_or_name,
            body=body,
            token=self._access_token,
            team_id=team_id,
            slug=slug,
            base_url=self._base_url,
            timeout=self._timeout,
        )

    async def delete_project(
        self, *, id_or_name: str, team_id: str | None = None, slug: str | None = None
    ) -> None:
        return await adelete_project(
            id_or_name=id_or_name,
            token=self._access_token,
            team_id=team_id,
            slug=slug,
            base_url=self._base_url,
            timeout=self._timeout,
        )

    async def get_projects(
        self,
        *,
        team_id: str | None = None,
        slug: str | None = None,
        query: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return await aget_projects(
            token=self._access_token,
            team_id=team_id,
            slug=slug,
            query=query,
            base_url=self._base_url,
            timeout=self._timeout,
        )


__all__ = [
    "ProjectsClient",
    "AsyncProjectsClient",
]

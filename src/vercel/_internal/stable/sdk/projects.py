"""Shared async-shaped backend for stable SDK projects operations."""

from __future__ import annotations

import urllib.parse
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any

from vercel._internal.stable.sdk.request_client import VercelRequestClient
from vercel.stable.options import ProjectWriteRequest

if TYPE_CHECKING:
    from vercel.stable.sdk.projects import Project, ProjectPage


@dataclass(slots=True)
class ProjectsBackend:
    _request_client: VercelRequestClient

    async def list(
        self,
        *,
        cursor: str | None = None,
        page_size: int | None = None,
        limit: int | None = None,
    ) -> ProjectPage:
        params: dict[str, Any] = {}
        if cursor is not None:
            params["from"] = cursor

        effective_page_size = page_size if page_size is not None else limit
        if effective_page_size is not None:
            params["limit"] = effective_page_size

        payload = await self._request_client.send_json(
            "GET",
            "/v10/projects",
            params=params or None,
        )
        return _parse_project_page(payload)

    async def get(
        self,
        id_or_name: str,
        *,
        team_id: str | None = None,
        team_slug: str | None = None,
    ) -> Project:
        payload = await self._request_client.send_json(
            "GET",
            _project_path(id_or_name),
            params=_override_scope(team_id=team_id, team_slug=team_slug),
        )
        return _parse_project(payload)

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
        payload = await self._request_client.send_json(
            "POST",
            "/v11/projects",
            body=_project_write_body(
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
            ),
        )
        return _parse_project(payload)

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
        payload = await self._request_client.send_json(
            "PATCH",
            _project_path(id_or_name),
            body=_project_write_body(
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
            ),
        )
        return _parse_project(payload)

    async def delete(
        self,
        id_or_name: str,
        *,
        team_id: str | None = None,
        team_slug: str | None = None,
    ) -> None:
        await self._request_client.send(
            "DELETE",
            _project_path(id_or_name),
            params=_override_scope(team_id=team_id, team_slug=team_slug),
        )

    async def fetch_page(
        self,
        *,
        cursor: str | None,
        page_size: int | None,
        remaining: int | None,
    ) -> ProjectPage | None:
        if remaining is not None and remaining <= 0:
            return None

        request_limit = page_size
        if remaining is not None:
            request_limit = remaining if request_limit is None else min(request_limit, remaining)

        page = await self.list(cursor=cursor, page_size=request_limit)
        if not page.items and not page.has_next_page:
            return None
        return page


def _project_path(id_or_name: str) -> str:
    quoted = urllib.parse.quote(id_or_name, safe="")
    return f"/v9/projects/{quoted}"


def _project_write_body(
    *,
    request: ProjectWriteRequest | None,
    body: dict[str, object] | None,
    name: str | None = None,
    framework: str | None = None,
    public_source: bool | None = None,
    build_command: str | None = None,
    dev_command: str | None = None,
    install_command: str | None = None,
    output_directory: str | None = None,
    root_directory: str | None = None,
) -> dict[str, object]:
    if body is not None:
        return dict(body)
    if request is None:
        request = ProjectWriteRequest(
            name=name,
            framework=framework,
            public_source=public_source,
            build_command=build_command,
            dev_command=dev_command,
            install_command=install_command,
            output_directory=output_directory,
            root_directory=root_directory,
        )

    merged: dict[str, object] = {}
    for key, value in asdict(request).items():
        if value is not None:
            merged[_snake_to_camel(key)] = value
    return merged


def _override_scope(*, team_id: str | None, team_slug: str | None) -> dict[str, Any] | None:
    params: dict[str, Any] = {}
    if team_id is not None:
        params["teamId"] = team_id
    if team_slug is not None:
        params["slug"] = team_slug
    return params or None


def _parse_project(payload: dict[str, Any]) -> Project:
    from vercel.stable.sdk.projects import Project

    return Project(
        id=str(payload["id"]),
        name=str(payload["name"]),
        framework=_optional_str(payload.get("framework")),
        account_id=_optional_str(payload.get("accountId")),
        created_at=_optional_int(payload.get("createdAt")),
        updated_at=_optional_int(payload.get("updatedAt")),
        raw=dict(payload),
    )


def _parse_project_page(payload: dict[str, Any]) -> ProjectPage:
    from vercel.stable.sdk.projects import ProjectPage

    items_data = payload.get("projects", [])
    items = tuple(_parse_project(item) for item in items_data if isinstance(item, dict))
    pagination_value = payload.get("pagination")
    pagination: dict[str, Any] = pagination_value if isinstance(pagination_value, dict) else {}
    next_cursor_value = pagination.get("next")
    next_cursor = str(next_cursor_value) if next_cursor_value is not None else None
    return ProjectPage(
        items=items,
        next_cursor=next_cursor,
        has_next_page=next_cursor is not None,
        raw=dict(payload),
    )


def _optional_str(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _optional_int(value: object) -> int | None:
    return value if isinstance(value, int) else None


def _snake_to_camel(value: str) -> str:
    head, *tail = value.split("_")
    return head + "".join(segment[:1].upper() + segment[1:] for segment in tail)


__all__ = ["ProjectsBackend"]

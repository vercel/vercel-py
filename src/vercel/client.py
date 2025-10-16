from __future__ import annotations

from typing import Any

from .projects import (
    get_projects,
    create_project,
    update_project,
    delete_project,
)
from .projects.aio import (
    get_projects as aget_projects,
    create_project as acreate_project,
    update_project as aupdate_project,
    delete_project as adelete_project,
)
from .deployments import (
    create_deployment,
    upload_file,
)
from .deployments.aio import (
    create_deployment as acreate_deployment,
    upload_file as aupload_file,
)


class Vercel:
    def __init__(self, *, access_token: str | None = None, base_url: str | None = None):
        self._token = access_token
        self._base_url = base_url

        class Deployments:
            def __init__(self, outer: Vercel):
                self._outer = outer

            def create_deployment(
                self, *, request_body: dict[str, Any], **kwargs: Any
            ) -> dict[str, Any]:
                return create_deployment(
                    body=request_body,
                    token=kwargs.get("token") or kwargs.get("bearer_token") or self._outer._token,
                    team_id=kwargs.get("team_id"),
                    slug=kwargs.get("slug"),
                    force_new=kwargs.get("force_new"),
                    skip_auto_detection_confirmation=kwargs.get("skip_auto_detection_confirmation"),
                    base_url=kwargs.get("base_url") or self._outer._base_url,
                    timeout=kwargs.get("timeout") or 30.0,
                )

            def upload_file(
                self, *, content: bytes, content_length: int, **kwargs: Any
            ) -> dict[str, Any]:
                return upload_file(
                    content=content,
                    content_length=content_length,
                    x_vercel_digest=kwargs.get("x_vercel_digest"),
                    x_now_digest=kwargs.get("x_now_digest"),
                    x_now_size=kwargs.get("x_now_size"),
                    token=kwargs.get("token") or kwargs.get("bearer_token") or self._outer._token,
                    team_id=kwargs.get("team_id"),
                    slug=kwargs.get("slug"),
                    base_url=kwargs.get("base_url") or self._outer._base_url,
                    timeout=kwargs.get("timeout") or 30.0,
                )

        class Projects:
            def __init__(self, outer: Vercel):
                self._outer = outer

            def create_project(
                self, *, request_body: dict[str, Any], **kwargs: Any
            ) -> dict[str, Any]:
                return create_project(
                    body=request_body,
                    token=self._outer._token,
                    team_id=kwargs.get("team_id"),
                    slug=kwargs.get("slug"),
                    base_url=kwargs.get("base_url") or self._outer._base_url,
                    timeout=kwargs.get("timeout") or 30.0,
                )

            def update_project(
                self, *, id_or_name: str, request_body: dict[str, Any], **kwargs: Any
            ) -> dict[str, Any]:
                return update_project(
                    id_or_name=id_or_name,
                    body=request_body,
                    token=self._outer._token,
                    team_id=kwargs.get("team_id"),
                    slug=kwargs.get("slug"),
                    base_url=kwargs.get("base_url") or self._outer._base_url,
                    timeout=kwargs.get("timeout") or 30.0,
                )

            def delete_project(self, *, id_or_name: str, **kwargs: Any) -> None:
                return delete_project(
                    id_or_name=id_or_name,
                    token=self._outer._token,
                    team_id=kwargs.get("team_id"),
                    slug=kwargs.get("slug"),
                    base_url=kwargs.get("base_url") or self._outer._base_url,
                    timeout=kwargs.get("timeout") or 30.0,
                )

            def get_projects(self, **kwargs: Any) -> dict[str, Any]:
                return get_projects(
                    token=self._outer._token,
                    team_id=kwargs.get("team_id"),
                    slug=kwargs.get("slug"),
                    query=kwargs.get("query"),
                    base_url=kwargs.get("base_url") or self._outer._base_url,
                    timeout=kwargs.get("timeout") or 30.0,
                )

        self.deployments = Deployments(self)
        self.projects = Projects(self)


class AsyncVercel:
    def __init__(self, *, bearer_token: str | None = None, base_url: str | None = None):
        self._token = bearer_token
        self._base_url = base_url

        class Deployments:
            def __init__(self, outer: AsyncVercel):
                self._outer = outer

            async def create_deployment(
                self, *, request_body: dict[str, Any], **kwargs: Any
            ) -> dict[str, Any]:
                return await acreate_deployment(
                    body=request_body,
                    token=kwargs.get("token") or kwargs.get("bearer_token") or self._outer._token,
                    team_id=kwargs.get("team_id"),
                    slug=kwargs.get("slug"),
                    force_new=kwargs.get("force_new"),
                    skip_auto_detection_confirmation=kwargs.get("skip_auto_detection_confirmation"),
                    base_url=kwargs.get("base_url") or self._outer._base_url,
                    timeout=kwargs.get("timeout") or 30.0,
                )

            async def upload_file(
                self, *, content: bytes, content_length: int, **kwargs: Any
            ) -> dict[str, Any]:
                return await aupload_file(
                    content=content,
                    content_length=content_length,
                    x_vercel_digest=kwargs.get("x_vercel_digest"),
                    x_now_digest=kwargs.get("x_now_digest"),
                    x_now_size=kwargs.get("x_now_size"),
                    token=kwargs.get("token") or kwargs.get("bearer_token") or self._outer._token,
                    team_id=kwargs.get("team_id"),
                    slug=kwargs.get("slug"),
                    base_url=kwargs.get("base_url") or self._outer._base_url,
                    timeout=kwargs.get("timeout") or 30.0,
                )

        class Projects:
            def __init__(self, outer: AsyncVercel):
                self._outer = outer

            async def create_project(
                self, *, request_body: dict[str, Any], **kwargs: Any
            ) -> dict[str, Any]:
                return await acreate_project(
                    body=request_body,
                    token=kwargs.get("token") or kwargs.get("bearer_token") or self._outer._token,
                    team_id=kwargs.get("team_id"),
                    slug=kwargs.get("slug"),
                    base_url=kwargs.get("base_url") or self._outer._base_url,
                    timeout=kwargs.get("timeout") or 30.0,
                )

            async def update_project(
                self, *, id_or_name: str, request_body: dict[str, Any], **kwargs: Any
            ) -> dict[str, Any]:
                return await aupdate_project(
                    id_or_name=id_or_name,
                    body=request_body,
                    token=kwargs.get("token") or kwargs.get("bearer_token") or self._outer._token,
                    team_id=kwargs.get("team_id"),
                    slug=kwargs.get("slug"),
                    base_url=kwargs.get("base_url") or self._outer._base_url,
                    timeout=kwargs.get("timeout") or 30.0,
                )

            async def delete_project(self, *, id_or_name: str, **kwargs: Any) -> None:
                return await adelete_project(
                    id_or_name=id_or_name,
                    token=kwargs.get("token") or kwargs.get("bearer_token") or self._outer._token,
                    team_id=kwargs.get("team_id"),
                    slug=kwargs.get("slug"),
                    base_url=kwargs.get("base_url") or self._outer._base_url,
                    timeout=kwargs.get("timeout") or 30.0,
                )

            async def get_projects(self, **kwargs: Any) -> dict[str, Any]:
                return await aget_projects(
                    token=kwargs.get("token") or kwargs.get("bearer_token") or self._outer._token,
                    team_id=kwargs.get("team_id"),
                    slug=kwargs.get("slug"),
                    query=kwargs.get("query"),
                    base_url=kwargs.get("base_url") or self._outer._base_url,
                    timeout=kwargs.get("timeout") or 30.0,
                )

        self.deployments = Deployments(self)
        self.projects = Projects(self)


__all__ = [
    "Vercel",
    "AsyncVercel",
]

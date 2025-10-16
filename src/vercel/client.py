from __future__ import annotations

from typing import Any

from .projects import (
    get_projects,
    create_project,
    update_project,
    delete_project,
)
from .projects.aio import (
    create_project as acreate_project,
    get_projects as aget_projects,
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
    def __init__(self, *, bearer_token: str | None = None, base_url: str | None = None):
        self._token = bearer_token
        self._base_url = base_url

    class deployments:
        @staticmethod
        def create_deployment(*, request_body: dict[str, Any], **kwargs: Any) -> dict[str, Any]:
            return create_deployment(
                body=request_body,
                token=kwargs.get("token") or kwargs.get("bearer_token") or None,
                team_id=kwargs.get("team_id"),
                slug=kwargs.get("slug"),
                force_new=kwargs.get("force_new"),
                skip_auto_detection_confirmation=kwargs.get("skip_auto_detection_confirmation"),
                base_url=kwargs.get("base_url") or None,
                timeout=kwargs.get("timeout") or 30.0,
            )

        @staticmethod
        def upload_file(*, content: bytes, content_length: int, **kwargs: Any) -> dict[str, Any]:
            return upload_file(
                content=content,
                content_length=content_length,
                x_vercel_digest=kwargs.get("x_vercel_digest"),
                x_now_digest=kwargs.get("x_now_digest"),
                x_now_size=kwargs.get("x_now_size"),
                token=kwargs.get("token") or kwargs.get("bearer_token") or None,
                team_id=kwargs.get("team_id"),
                slug=kwargs.get("slug"),
                base_url=kwargs.get("base_url") or None,
                timeout=kwargs.get("timeout") or 30.0,
            )

    class projects:
        @staticmethod
        def create_project(*, request_body: dict[str, Any], **kwargs: Any) -> dict[str, Any]:
            return create_project(
                body=request_body,
                token=kwargs.get("token") or kwargs.get("bearer_token") or None,
                team_id=kwargs.get("team_id"),
                slug=kwargs.get("slug"),
                base_url=kwargs.get("base_url") or None,
                timeout=kwargs.get("timeout") or 30.0,
            )


class AsyncVercel:
    def __init__(self, *, bearer_token: str | None = None, base_url: str | None = None):
        self._token = bearer_token
        self._base_url = base_url

    class deployments:
        @staticmethod
        async def create_deployment(
            *, request_body: dict[str, Any], **kwargs: Any
        ) -> dict[str, Any]:
            return await acreate_deployment(
                body=request_body,
                token=kwargs.get("token") or kwargs.get("bearer_token") or None,
                team_id=kwargs.get("team_id"),
                slug=kwargs.get("slug"),
                force_new=kwargs.get("force_new"),
                skip_auto_detection_confirmation=kwargs.get("skip_auto_detection_confirmation"),
                base_url=kwargs.get("base_url") or None,
                timeout=kwargs.get("timeout") or 30.0,
            )

        @staticmethod
        async def upload_file(
            *, content: bytes, content_length: int, **kwargs: Any
        ) -> dict[str, Any]:
            return await aupload_file(
                content=content,
                content_length=content_length,
                x_vercel_digest=kwargs.get("x_vercel_digest"),
                x_now_digest=kwargs.get("x_now_digest"),
                x_now_size=kwargs.get("x_now_size"),
                token=kwargs.get("token") or kwargs.get("bearer_token") or None,
                team_id=kwargs.get("team_id"),
                slug=kwargs.get("slug"),
                base_url=kwargs.get("base_url") or None,
                timeout=kwargs.get("timeout") or 30.0,
            )

    class projects:
        @staticmethod
        async def create_project(*, request_body: dict[str, Any], **kwargs: Any) -> dict[str, Any]:
            return await acreate_project(
                body=request_body,
                token=kwargs.get("token") or kwargs.get("bearer_token") or None,
                team_id=kwargs.get("team_id"),
                slug=kwargs.get("slug"),
                base_url=kwargs.get("base_url") or None,
                timeout=kwargs.get("timeout") or 30.0,
            )


__all__ = [
    "Vercel",
    "AsyncVercel",
]

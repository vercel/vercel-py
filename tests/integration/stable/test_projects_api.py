from __future__ import annotations

import pytest
import respx
from httpx import Response

import vercel
from tests.integration.conftest import VERCEL_API_BASE
from vercel.stable.sdk.projects import ProjectPage


@pytest.fixture
def project_payload() -> dict[str, object]:
    return {
        "id": "prj_test123",
        "name": "test-project",
        "framework": "nextjs",
        "accountId": "team_test123",
        "createdAt": 1_640_995_200_000,
        "updatedAt": 1_640_995_200_000,
        "unexpectedField": "kept",
    }


@pytest.fixture
def project_page_payload(project_payload: dict[str, object]) -> dict[str, object]:
    return {
        "projects": [project_payload],
        "pagination": {
            "count": 1,
            "next": "cursor_2",
            "prev": None,
        },
    }


@respx.mock
def test_projects_list_returns_page(project_page_payload: dict[str, object]) -> None:
    route = respx.get(f"{VERCEL_API_BASE}/v10/projects").mock(
        return_value=Response(200, json=project_page_payload)
    )

    vc = vercel.create_sync_client(timeout=9.0)
    try:
        page = vc.get_sdk(token="sdk-token", team_id="team_123").get_projects().list(limit=1)
    finally:
        vc.close()

    assert isinstance(page, ProjectPage)
    assert len(page.items) == 1
    assert page.items[0].id == "prj_test123"
    assert page.items[0].name == "test-project"
    assert page.items[0].framework == "nextjs"
    assert page.items[0].to_dict()["unexpectedField"] == "kept"
    assert page.next_cursor == "cursor_2"
    assert page.has_next_page is True
    assert route.called
    request = route.calls[0].request
    assert request.url.params["teamId"] == "team_123"
    assert request.url.params["limit"] == "1"
    assert request.headers["authorization"] == "Bearer sdk-token"
    assert request.headers["accept"] == "application/json"


@respx.mock
def test_projects_create_accepts_common_fields(project_payload: dict[str, object]) -> None:
    route = respx.post(f"{VERCEL_API_BASE}/v11/projects").mock(
        return_value=Response(200, json=project_payload)
    )

    vc = vercel.create_sync_client()
    try:
        project = (
            vc.get_sdk(token="sdk-token")
            .get_projects()
            .create(
                name="test-project",
                framework="nextjs",
                public_source=False,
                build_command="pnpm build",
            )
        )
    finally:
        vc.close()

    assert project.id == "prj_test123"
    assert route.called
    request = route.calls[0].request
    assert request.headers["authorization"] == "Bearer sdk-token"
    assert request.content == (
        b'{"name":"test-project","framework":"nextjs",'
        b'"publicSource":false,"buildCommand":"pnpm build"}'
    )


@respx.mock
def test_projects_ensure_connected_is_eager_but_request_lazy(
    project_page_payload: dict[str, object],
) -> None:
    route = respx.get(f"{VERCEL_API_BASE}/v10/projects").mock(
        return_value=Response(200, json=project_page_payload)
    )

    vc = vercel.create_sync_client(timeout=9.0)
    projects = vc.get_sdk(token="sdk-token", team_id="team_123").get_projects()
    sibling = vc.get_sdk(token="sdk-token", team_id="team_123").get_projects()

    try:
        assert projects.ensure_connected() is projects
        assert sibling.ensure_connected() is sibling
        assert not route.called

        page = sibling.list(limit=1)
    finally:
        vc.close()

    assert len(page.items) == 1
    assert route.called


@respx.mock
def test_projects_with_options_keeps_parent_request_policy_lazy(
    project_page_payload: dict[str, object],
) -> None:
    route = respx.get(f"{VERCEL_API_BASE}/v10/projects").mock(
        return_value=Response(200, json=project_page_payload)
    )

    vc = vercel.create_sync_client(timeout=9.0)
    sdk = vc.get_sdk(token="sdk-token", team_id="team_123")
    projects = sdk.with_options(token="overlay-token", team_slug="overlay-team").get_projects()

    try:
        page = projects.list(limit=1)
    finally:
        vc.close()

    assert len(page.items) == 1
    assert route.called
    request = route.calls[0].request
    assert dict(request.url.params) == {"teamId": "team_123", "slug": "overlay-team", "limit": "1"}
    assert request.headers["authorization"] == "Bearer overlay-token"

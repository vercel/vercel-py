from __future__ import annotations

import json

import httpx
import pytest
import respx

from vercel.client import AsyncVercel, Vercel
from vercel.project_routes import (
    AddRouteRequestBody,
    ProjectRoutesError,
    RewriteRoute,
    RouteInput,
    StageRoutesRequestBody,
    get_routes,
    get_routes_async,
)

API_URL = "https://api.example.com"
VERSION = {
    "id": "version_123",
    "s3Key": "project-routes/prj_123/version_123.json",
    "lastModified": 1_722_000_000_000,
    "createdBy": "user_123",
    "isStaging": True,
    "ruleCount": 1,
}
PROJECT_ROUTE = {
    "id": "route_123",
    "name": "Rewrite /old to /new",
    "enabled": True,
    "staged": True,
    "srcSyntax": "equals",
    "routeType": "rewrite",
    "route": {"src": "/old", "dest": "/new"},
}
ROUTE_INPUT: RouteInput = {
    "name": "Rewrite /old to /new",
    "srcSyntax": "equals",
    "route": {"src": "/old", "dest": "/new"},
}
ADD_BODY: AddRouteRequestBody = {"route": ROUTE_INPUT}
STAGE_BODY: StageRoutesRequestBody = {
    "overwrite": True,
    "routes": [
        {
            "id": "route_123",
            "name": "Rewrite /old to /new",
            "srcSyntax": "equals",
            "route": {"src": "/old", "dest": "/new"},
        }
    ],
}
GENERATED_ROUTE = {
    "name": "Rewrite /old to /new",
    "description": "Rewrites the legacy path",
    "pathCondition": {"value": "/old", "syntax": "equals"},
    "conditions": [],
    "actions": [{"type": "rewrite", "dest": "/new"}],
}


def _install_routes() -> dict[str, respx.Route]:
    list_route = respx.get(
        f"{API_URL}/v1/projects/prj_123/routes",
        params={
            "teamId": "team_123",
            "versionId": "version_123",
            "q": "legacy",
            "filter": "rewrite",
            "diff": "only",
        },
    ).mock(
        return_value=httpx.Response(
            200,
            json={
                "routes": [PROJECT_ROUTE],
                "version": VERSION,
                "limit": {"maxRoutes": 100, "currentRoutes": 1},
            },
        )
    )
    stage_route = respx.put(
        f"{API_URL}/v1/projects/prj_123/routes", params={"teamId": "team_123"}
    ).mock(return_value=httpx.Response(200, json={"version": VERSION}))
    add_route = respx.post(
        f"{API_URL}/v1/projects/prj_123/routes", params={"teamId": "team_123"}
    ).mock(return_value=httpx.Response(200, json={"route": PROJECT_ROUTE, "version": VERSION}))
    delete_route = respx.delete(
        f"{API_URL}/v1/projects/prj_123/routes", params={"teamId": "team_123"}
    ).mock(return_value=httpx.Response(200, json={"deletedCount": 1, "version": VERSION}))
    edit_route = respx.patch(
        f"{API_URL}/v1/projects/prj_123/routes/route_123",
        params={"teamId": "team_123"},
    ).mock(return_value=httpx.Response(200, json={"route": PROJECT_ROUTE, "version": VERSION}))
    generate_route = respx.post(
        f"{API_URL}/v1/projects/prj_123/routes/generate",
        params={"teamId": "team_123"},
    ).mock(return_value=httpx.Response(200, json={"route": GENERATED_ROUTE}))
    versions_route = respx.get(
        f"{API_URL}/v1/projects/prj_123/routes/versions",
        params={"teamId": "team_123"},
    ).mock(return_value=httpx.Response(200, json={"versions": [VERSION]}))
    update_route = respx.post(
        f"{API_URL}/v1/projects/prj_123/routes/versions",
        params={"teamId": "team_123"},
    ).mock(return_value=httpx.Response(200, json={"version": VERSION}))
    return {
        "list": list_route,
        "stage": stage_route,
        "add": add_route,
        "delete": delete_route,
        "edit": edit_route,
        "generate": generate_route,
        "versions": versions_route,
        "update": update_route,
    }


def _assert_requests(routes: dict[str, respx.Route]) -> None:
    assert all(route.called for route in routes.values())
    for route in routes.values():
        assert route.calls.last.request.headers["authorization"] == "Bearer test-token"

    expected_bodies = {
        "stage": STAGE_BODY,
        "add": ADD_BODY,
        "delete": {"routeIds": ["route_123"]},
        "edit": {"restore": True},
        "generate": {"prompt": "Rewrite /old to /new"},
        "update": {"id": "version_123", "action": "promote"},
    }
    for name, body in expected_bodies.items():
        assert json.loads(routes[name].calls.last.request.content) == body


@respx.mock
def test_sync_client_supports_every_project_routes_operation() -> None:
    routes = _install_routes()
    client = Vercel(access_token="test-token", base_url=API_URL)

    listed = client.project_routes.get_routes(
        project_id="prj_123",
        version_id="version_123",
        q="legacy",
        filter="rewrite",
        diff="only",
        team_id="team_123",
    )
    staged = client.project_routes.stage_routes(
        project_id="prj_123",
        routes=STAGE_BODY["routes"],
        overwrite=True,
        team_id="team_123",
    )
    added = client.project_routes.add_route(
        project_id="prj_123",
        route=RewriteRoute(
            name="Rewrite /old to /new",
            source="/old",
            destination="/new",
            source_syntax="equals",
        ),
        team_id="team_123",
    )
    deleted = client.project_routes.delete_routes(
        project_id="prj_123",
        route_ids=["route_123"],
        team_id="team_123",
    )
    edited = client.project_routes.edit_route(
        project_id="prj_123",
        route_id="route_123",
        restore=True,
        team_id="team_123",
    )
    generated = client.project_routes.generate_route(
        project_id="prj_123",
        prompt="Rewrite /old to /new",
        team_id="team_123",
    )
    versions = client.project_routes.get_route_versions(project_id="prj_123", team_id="team_123")
    updated = client.project_routes.update_route_versions(
        project_id="prj_123",
        version_id="version_123",
        action="promote",
        team_id="team_123",
    )

    assert listed["routes"] == [PROJECT_ROUTE]
    assert staged["version"] == VERSION
    assert added["route"] == PROJECT_ROUTE
    assert deleted["deletedCount"] == 1
    assert edited["route"] == PROJECT_ROUTE
    assert generated["route"] == GENERATED_ROUTE
    assert versions["versions"] == [VERSION]
    assert updated["version"] == VERSION
    _assert_requests(routes)


@respx.mock
@pytest.mark.asyncio
async def test_async_client_supports_every_project_routes_operation() -> None:
    routes = _install_routes()
    client = AsyncVercel(access_token="test-token", base_url=API_URL)

    listed = await client.project_routes.get_routes(
        project_id="prj_123",
        version_id="version_123",
        q="legacy",
        filter="rewrite",
        diff="only",
        team_id="team_123",
    )
    staged = await client.project_routes.stage_routes(
        project_id="prj_123",
        routes=STAGE_BODY["routes"],
        overwrite=True,
        team_id="team_123",
    )
    added = await client.project_routes.add_route(
        project_id="prj_123", route=ROUTE_INPUT, team_id="team_123"
    )
    deleted = await client.project_routes.delete_routes(
        project_id="prj_123",
        route_ids=["route_123"],
        team_id="team_123",
    )
    edited = await client.project_routes.edit_route(
        project_id="prj_123",
        route_id="route_123",
        restore=True,
        team_id="team_123",
    )
    generated = await client.project_routes.generate_route(
        project_id="prj_123",
        prompt="Rewrite /old to /new",
        team_id="team_123",
    )
    versions = await client.project_routes.get_route_versions(
        project_id="prj_123", team_id="team_123"
    )
    updated = await client.project_routes.update_route_versions(
        project_id="prj_123",
        version_id="version_123",
        action="promote",
        team_id="team_123",
    )

    assert listed["routes"] == [PROJECT_ROUTE]
    assert staged["version"] == VERSION
    assert added["route"] == PROJECT_ROUTE
    assert deleted["deletedCount"] == 1
    assert edited["route"] == PROJECT_ROUTE
    assert generated["route"] == GENERATED_ROUTE
    assert versions["versions"] == [VERSION]
    assert updated["version"] == VERSION
    _assert_requests(routes)


@respx.mock
@pytest.mark.asyncio
async def test_sync_and_async_clients_map_structured_errors() -> None:
    route = respx.get(f"{API_URL}/v1/projects/prj_123/routes").mock(
        return_value=httpx.Response(
            403,
            json={"error": {"code": "forbidden", "message": "Not allowed"}},
        )
    )

    with pytest.raises(ProjectRoutesError) as sync_error:
        get_routes(project_id="prj_123", token="test-token", base_url=API_URL)
    with pytest.raises(ProjectRoutesError) as async_error:
        await get_routes_async(project_id="prj_123", token="test-token", base_url=API_URL)

    for error in (sync_error.value, async_error.value):
        assert error.status_code == 403
        assert error.response_body == {"error": {"code": "forbidden", "message": "Not allowed"}}
        assert "Not allowed" in str(error)
    assert route.call_count == 2


@pytest.mark.asyncio
async def test_sync_and_async_clients_require_a_token(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("VERCEL_TOKEN", raising=False)

    with pytest.raises(ProjectRoutesError, match="Missing Vercel API token"):
        get_routes(project_id="prj_123", base_url=API_URL)
    with pytest.raises(ProjectRoutesError, match="Missing Vercel API token"):
        await get_routes_async(project_id="prj_123", base_url=API_URL)

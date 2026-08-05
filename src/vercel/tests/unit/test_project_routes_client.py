from __future__ import annotations

import pytest

from vercel.client import AsyncVercel, Vercel
from vercel.project_routes import (
    AsyncProjectRoutesClient,
    ProjectRoute,
    RouteDefinition,
    ProjectRoutesClient,
    RedirectRoute,
    RewriteRoute,
    SetStatusRoute,
    add_route,
    delete_routes,
    edit_route,
    get_routes,
)


def test_rewrite_route_serializes_to_api_shape() -> None:
    route = RewriteRoute(
        name="Rewrite /old to /new",
        source="/old",
        destination="/new",
        source_syntax="equals",
        description="Keep old links working",
        enabled=False,
    )

    assert route.to_route_input().to_wire() == {
        "name": "Rewrite /old to /new",
        "description": "Keep old links working",
        "enabled": False,
        "srcSyntax": "equals",
        "route": {"src": "/old", "dest": "/new"},
    }


def test_redirect_route_serializes_to_api_shape() -> None:
    route = RedirectRoute(
        name="Redirect /old to /new",
        source="/old",
        destination="/new",
        status=302,
        source_syntax="equals",
    )

    assert route.to_route_input().to_wire() == {
        "name": "Redirect /old to /new",
        "srcSyntax": "equals",
        "route": {"src": "/old", "dest": "/new", "status": 302},
    }


def test_redirect_route_rejects_non_redirect_status() -> None:
    with pytest.raises(ValueError, match="Redirect status"):
        RedirectRoute(name="Bad", source="/old", destination="/new", status=200)


def test_set_status_route_serializes_to_api_shape() -> None:
    route = SetStatusRoute(name="Gone", source="/legacy", status=410)

    assert route.to_route_input().to_wire() == {
        "name": "Gone",
        "route": {"src": "/legacy", "status": 410},
    }


def test_project_route_round_trips_to_staged_input() -> None:
    route = ProjectRoute(
        id="route_123",
        name="Rewrite /old to /new",
        route=RouteDefinition(src="/old", dest="/new"),
        description="Keep old links working",
        enabled=True,
        src_syntax="equals",
    )

    assert route.to_staged_input().to_wire() == {
        "id": "route_123",
        "name": "Rewrite /old to /new",
        "description": "Keep old links working",
        "enabled": True,
        "srcSyntax": "equals",
        "route": {"src": "/old", "dest": "/new"},
    }


def test_get_routes_rejects_diff_combined_with_search_or_route_type() -> None:
    with pytest.raises(ValueError, match="diff cannot be combined"):
        get_routes(project_id="prj_123", diff="only", search="legacy")
    with pytest.raises(ValueError, match="diff cannot be combined"):
        get_routes(project_id="prj_123", diff=True, route_type="rewrite")


def test_add_route_validates_placement_and_reference_id() -> None:
    route = RewriteRoute(name="r", source="/a", destination="/b")

    with pytest.raises(ValueError, match="requires reference_id"):
        add_route(project_id="prj_123", route=route, placement="before")
    with pytest.raises(ValueError, match='placement="before" or "after"'):
        add_route(project_id="prj_123", route=route, reference_id="route_123")


def test_edit_route_requires_route_or_restore() -> None:
    route = RewriteRoute(name="r", source="/a", destination="/b")

    with pytest.raises(ValueError, match="either route or restore"):
        edit_route(project_id="prj_123", route_id="route_123")
    with pytest.raises(ValueError, match="either route or restore"):
        edit_route(project_id="prj_123", route_id="route_123", route=route, restore=True)


def test_delete_routes_requires_route_ids() -> None:
    with pytest.raises(ValueError, match="must not be empty"):
        delete_routes(project_id="prj_123", route_ids=[])


def test_root_clients_expose_project_routes() -> None:
    assert isinstance(Vercel().project_routes, ProjectRoutesClient)
    assert isinstance(AsyncVercel().project_routes, AsyncProjectRoutesClient)


def test_sync_and_async_project_routes_clients_have_method_parity() -> None:
    expected = {
        "add_route",
        "delete_routes",
        "edit_route",
        "generate_route",
        "get_route_versions",
        "get_routes",
        "stage_routes",
        "update_route_version",
    }
    sync_methods = {
        name
        for name in dir(ProjectRoutesClient)
        if not name.startswith("_") and callable(getattr(ProjectRoutesClient, name))
    }
    async_methods = {
        name
        for name in dir(AsyncProjectRoutesClient)
        if not name.startswith("_") and callable(getattr(AsyncProjectRoutesClient, name))
    }

    assert sync_methods == expected
    assert async_methods == expected

from __future__ import annotations

from vercel.client import AsyncVercel, Vercel
from vercel.project_routes import AsyncProjectRoutesClient, ProjectRoutesClient


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
        "update_route_versions",
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

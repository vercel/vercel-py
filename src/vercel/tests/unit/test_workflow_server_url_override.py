"""Tests for the workflow-server URL override on VercelWorld.

The override mirrors world-vercel's ``utils.ts`` precedence: the hard-coded
``WORKFLOW_SERVER_URL_OVERRIDE`` constant wins, then the
``VERCEL_WORKFLOW_SERVER_URL`` environment variable, then the production
default. Preview deployments carry the env var, so a Python app has to honour
it to reach the same workflow-server as its TypeScript peers.
"""

from __future__ import annotations

import pytest

from vercel._internal.workflow.worlds import vercel as vercel_mod

BRANCH_URL = "https://workflow-server-git-branch.vercel.sh"
PINNED_URL = "https://workflow-server-pinned.vercel.sh"


@pytest.fixture(autouse=True)
def _clean_override(monkeypatch: pytest.MonkeyPatch) -> None:
    """Start every test from "no constant, no env var"."""
    monkeypatch.setattr(vercel_mod, "WORKFLOW_SERVER_URL_OVERRIDE", "")
    monkeypatch.delenv("VERCEL_WORKFLOW_SERVER_URL", raising=False)


def test_defaults_to_production_without_an_override() -> None:
    world = vercel_mod.VercelWorld(token="tok")

    assert world._base_url == "https://vercel-workflow.com/api"
    assert "x-vercel-workflow-api-url" not in world._headers


def test_env_var_redirects_the_direct_base_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VERCEL_WORKFLOW_SERVER_URL", BRANCH_URL)

    world = vercel_mod.VercelWorld(token="tok")

    assert world._base_url == f"{BRANCH_URL}/api"


def test_env_var_reaches_the_proxy_as_a_header(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VERCEL_WORKFLOW_SERVER_URL", BRANCH_URL)

    world = vercel_mod.VercelWorld(token="tok", project_id="prj_1", team_id="team_1")

    # The proxy is still api.vercel.com; the override travels in the header it
    # forwards to the workflow-server.
    assert world._base_url == "https://api.vercel.com/v1/workflow"
    assert world._headers["x-vercel-workflow-api-url"] == BRANCH_URL


def test_hard_coded_constant_wins_over_the_env_var(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(vercel_mod, "WORKFLOW_SERVER_URL_OVERRIDE", PINNED_URL)
    monkeypatch.setenv("VERCEL_WORKFLOW_SERVER_URL", BRANCH_URL)

    world = vercel_mod.VercelWorld(token="tok")

    assert world._base_url == f"{PINNED_URL}/api"


def test_override_is_read_per_world_not_at_import(monkeypatch: pytest.MonkeyPatch) -> None:
    """The env var is resolved when a world is built, so a process that sets it
    after importing this module still picks it up."""
    before = vercel_mod.VercelWorld(token="tok")
    monkeypatch.setenv("VERCEL_WORKFLOW_SERVER_URL", BRANCH_URL)
    after = vercel_mod.VercelWorld(token="tok")

    assert before._base_url == "https://vercel-workflow.com/api"
    assert after._base_url == f"{BRANCH_URL}/api"

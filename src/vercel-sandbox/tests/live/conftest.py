"""Credential checks for live Sandbox tests."""

import os

import pytest


def has_sandbox_credentials() -> bool:
    """Return whether credentials needed by Sandbox live tests are present."""
    if os.getenv("VERCEL_OIDC_TOKEN"):
        return True
    return bool(
        os.getenv("VERCEL_TOKEN") and os.getenv("VERCEL_TEAM_ID") and os.getenv("VERCEL_PROJECT_ID")
    )


requires_sandbox_credentials = pytest.mark.skipif(
    not has_sandbox_credentials(),
    reason=(
        "Requires VERCEL_OIDC_TOKEN, or VERCEL_TOKEN plus VERCEL_TEAM_ID "
        "and VERCEL_PROJECT_ID for sandbox"
    ),
)

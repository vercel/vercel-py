from __future__ import annotations

import os

from .token import decode_oidc_payload
from .types import Credentials


def get_credentials(
    *,
    token: str | None = None,
    project_id: str | None = None,
    team_id: str | None = None,
) -> Credentials:
    if token and project_id and team_id:
        return Credentials(token=token, project_id=project_id, team_id=team_id)

    oidc = os.getenv("VERCEL_OIDC_TOKEN")
    if oidc:
        project = os.getenv("VERCEL_PROJECT_ID")
        team = os.getenv("VERCEL_TEAM_ID")
        if not (project and team):
            try:
                payload = decode_oidc_payload(oidc)
                project = payload.get("project_id")
                team = payload.get("owner_id")
            except Exception:
                pass
        if project and team:
            return Credentials(token=oidc, project_id=project, team_id=team)
        raise RuntimeError(
            "VERCEL_OIDC_TOKEN present but could not determine VERCEL_PROJECT_ID and VERCEL_TEAM_ID"
        )

    token = token or os.getenv("VERCEL_TOKEN")
    project_id = project_id or os.getenv("VERCEL_PROJECT_ID")
    team_id = team_id or os.getenv("VERCEL_TEAM_ID")

    if token and project_id and team_id:
        return Credentials(token=token, project_id=project_id, team_id=team_id)

    raise RuntimeError(
        "Missing credentials. Provide VERCEL_OIDC_TOKEN with VERCEL_PROJECT_ID and VERCEL_TEAM_ID, or VERCEL_TOKEN, VERCEL_PROJECT_ID, VERCEL_TEAM_ID."
    )

"""Utilities for extracting credentials (user_id, team_id, project_id) from various sources."""

import os
from typing import TYPE_CHECKING, Optional, Tuple

if TYPE_CHECKING:
    from ..oidc.types import ProjectInfo


def extract_credentials(
    *,
    token: Optional[str] = None,
    team_id: Optional[str] = None,
    project_id: Optional[str] = None,
    user_id: Optional[str] = None,
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Extract user_id, team_id, and project_id from various sources.

    Priority order:
    1. Explicitly provided parameters
    2. Environment variables (VERCEL_PROJECT_ID, VERCEL_TEAM_ID)
    3. OIDC token payload (if available)
    4. .vercel/project.json (for local dev)

    Returns:
        Tuple of (user_id, team_id, project_id). Each may be None if not found.
    """
    # Start with explicitly provided values
    resolved_user_id = user_id
    resolved_team_id = team_id
    resolved_project_id = project_id

    # Check environment variables
    if not resolved_project_id:
        resolved_project_id = os.getenv("VERCEL_PROJECT_ID")
    if not resolved_team_id:
        resolved_team_id = os.getenv("VERCEL_TEAM_ID")

    # Try to extract from OIDC token if available
    if token:
        try:
            from ..oidc.token import decode_oidc_payload

            payload = decode_oidc_payload(token)
            if not resolved_project_id:
                resolved_project_id = payload.get("project_id")
            if not resolved_team_id:
                # OIDC tokens may have owner_id as team_id
                resolved_team_id = payload.get("owner_id") or payload.get("team_id")
        except Exception:
            # Silently fail - OIDC may not be available or token may be invalid
            pass

    # Try to extract from .vercel/project.json for local dev
    if not resolved_project_id or not resolved_team_id:
        try:
            # Import lazily to avoid hard dependency in all environments
            from ..oidc.utils import find_project_info as _find_project_info  # type: ignore

            project_info = _find_project_info()
            if not resolved_project_id and project_info.get("projectId"):
                resolved_project_id = project_info["projectId"]
            if not resolved_team_id and project_info.get("teamId"):
                resolved_team_id = project_info["teamId"]
        except Exception:
            # Silently fail - .vercel directory may not exist (production context)
            pass

    # Note: user_id typically requires API call to determine from token
    # For now, we leave it as None unless explicitly provided
    # This is acceptable as user_id may not always be available in all contexts

    return (resolved_user_id, resolved_team_id, resolved_project_id)

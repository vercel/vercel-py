from __future__ import annotations

import os

from .token_util import (
    find_project_info,
    get_token_payload,
    get_vercel_cli_token,
    is_expired,
    load_token,
    save_token,
    fetch_vercel_oidc_token,
)


class VercelOidcTokenError(Exception):
    def __init__(self, message: str, cause: Exception | None = None):
        if cause is not None:
            message = f"{message}: {cause}"
        super().__init__(message)
        self.cause = cause


def get_vercel_oidc_token_sync() -> str:
    token = os.getenv("VERCEL_OIDC_TOKEN")
    if not token:
        raise VercelOidcTokenError(
            "The 'x-vercel-oidc-token' header is missing from the request. Do you have the OIDC option enabled in the Vercel project settings?"
        )
    return token


async def refresh_token() -> None:
    project = find_project_info()
    project_id = project["projectId"]
    team_id = project.get("teamId")

    maybe = load_token(project_id)
    if not maybe or is_expired(get_token_payload(maybe.token)):
        auth_token = get_vercel_cli_token()
        if not auth_token:
            raise VercelOidcTokenError("Failed to refresh OIDC token: login to vercel cli")
        if not project_id:
            raise VercelOidcTokenError("Failed to refresh OIDC token: project id not found")
        new_token = await fetch_vercel_oidc_token(auth_token, project_id, team_id)
        if not new_token:
            raise VercelOidcTokenError("Failed to refresh OIDC token")
        save_token(new_token, project_id)
        os.environ["VERCEL_OIDC_TOKEN"] = new_token.token
    else:
        os.environ["VERCEL_OIDC_TOKEN"] = maybe.token


async def get_vercel_oidc_token() -> str:
    token = ""
    err: Exception | None = None
    try:
        token = get_vercel_oidc_token_sync()
    except Exception as e:
        err = e
    try:
        if not token or is_expired(get_token_payload(token)):
            await refresh_token()
            token = get_vercel_oidc_token_sync()
    except Exception as e:
        if err and isinstance(e, Exception) and getattr(err, "message", None):
            e.args = (f"{err}\n{e}",)
        raise VercelOidcTokenError("Failed to refresh OIDC token", e)
    return token


def decode_oidc_payload(token: str) -> dict:
    return get_token_payload(token)

from __future__ import annotations

import os
import httpx
from collections.abc import Mapping

from ..cache.context import get_headers
from .utils import (
    find_project_info,
    get_token_payload,
    get_vercel_cli_token,
    is_expired,
    load_token,
    save_token,
)
from .types import VercelTokenResponse


BASE_URL = "https://api.vercel.com/v1"


class VercelOidcTokenError(Exception):
    def __init__(self, message: str, cause: Exception | None = None):
        if cause is not None:
            message = f"{message}: {cause}"
        super().__init__(message)
        self.cause = cause


def get_vercel_oidc_token_from_context() -> str:
    # Prefer request header (set via vercel.headers.set_headers middleware),
    # fall back to environment variable like the TypeScript SDK.
    headers = get_headers()
    if headers:
        # Normalize headers to lowercase keys
        lower_headers: dict[str, str] = {}
        if isinstance(headers, Mapping):
            for k, v in headers.items():
                lower_headers[str(k).lower()] = v
        elif isinstance(headers, (list, tuple)):
            for item in headers:
                if isinstance(item, (list, tuple)) and len(item) == 2:
                    k, v = item
                    lower_headers[str(k).lower()] = v
        elif hasattr(headers, "keys") and hasattr(headers, "__getitem__"):
            for k in headers.keys():
                v = headers[k]
                lower_headers[str(k).lower()] = v

        token_from_header = lower_headers.get("x-vercel-oidc-token")
        if token_from_header:
            return token_from_header

    token_from_env = os.getenv("VERCEL_OIDC_TOKEN")
    if not token_from_env:
        raise VercelOidcTokenError(
            "The 'x-vercel-oidc-token' header is missing from the request. Do you have the OIDC option enabled in the Vercel project settings?"
        )
    return token_from_env


# for TS parity
get_vercel_oidc_token_sync = get_vercel_oidc_token_from_context


def refresh_token() -> None:
    project = find_project_info()
    project_id: str = project["projectId"]
    team_id = project.get("teamId")

    maybe = load_token(project_id)
    if not maybe or is_expired(get_token_payload(maybe.token)):
        auth_token = get_vercel_cli_token()
        if not auth_token:
            raise VercelOidcTokenError("Failed to refresh OIDC token: login to vercel cli")
        if not project_id:
            raise VercelOidcTokenError("Failed to refresh OIDC token: project id not found")
        new_token = fetch_vercel_oidc_token(auth_token, project_id, team_id)
        if not new_token:
            raise VercelOidcTokenError("Failed to refresh OIDC token")
        save_token(new_token, project_id)
        os.environ["VERCEL_OIDC_TOKEN"] = new_token.token
    else:
        os.environ["VERCEL_OIDC_TOKEN"] = maybe.token


async def refresh_token_async() -> None:
    project = find_project_info()
    project_id: str = project["projectId"]
    team_id = project.get("teamId")

    maybe = load_token(project_id)
    if not maybe or is_expired(get_token_payload(maybe.token)):
        auth_token = get_vercel_cli_token()
        if not auth_token:
            raise VercelOidcTokenError("Failed to refresh OIDC token: login to vercel cli")
        if not project_id:
            raise VercelOidcTokenError("Failed to refresh OIDC token: project id not found")
        new_token = await fetch_vercel_oidc_token_async(auth_token, project_id, team_id)
        if not new_token:
            raise VercelOidcTokenError("Failed to refresh OIDC token")
        save_token(new_token, project_id)
        os.environ["VERCEL_OIDC_TOKEN"] = new_token.token
    else:
        os.environ["VERCEL_OIDC_TOKEN"] = maybe.token


def get_vercel_oidc_token() -> str:
    token = ""
    err: Exception | None = None
    try:
        token = get_vercel_oidc_token_from_context()
    except Exception as e:
        err = e
    try:
        if not token or is_expired(get_token_payload(token)):
            # Only attempt refresh in environments that look like local dev with a .vercel folder
            try:
                _ = find_project_info()
            except Exception as e:
                # Preserve the original context error and surface an actionable message
                if err and isinstance(err, Exception) and getattr(err, "message", None):
                    e.args = (f"{err}\n{e}",)
                raise VercelOidcTokenError(
                    "Missing OIDC request header and no local project context (.vercel) available",
                    e,
                )
            refresh_token()
            token = get_vercel_oidc_token_from_context()
    except Exception as e:
        if err and isinstance(e, Exception) and getattr(err, "message", None):
            e.args = (f"{err}\n{e}",)
        raise VercelOidcTokenError("Failed to refresh OIDC token", e)
    return token


async def get_vercel_oidc_token_async() -> str:
    token = ""
    err: Exception | None = None
    try:
        token = get_vercel_oidc_token_from_context()
    except Exception as e:
        err = e
    try:
        if not token or is_expired(get_token_payload(token)):
            # Only attempt refresh in environments that look like local dev with a .vercel folder
            try:
                _ = find_project_info()
            except Exception as e:
                if err and isinstance(err, Exception) and getattr(err, "message", None):
                    e.args = (f"{err}\n{e}",)
                raise VercelOidcTokenError(
                    "Missing OIDC request header and no local project context (.vercel) available",
                    e,
                )
            await refresh_token_async()
            token = get_vercel_oidc_token_from_context()
    except Exception as e:
        if err and isinstance(e, Exception) and getattr(err, "message", None):
            e.args = (f"{err}\n{e}",)
        raise VercelOidcTokenError("Failed to refresh OIDC token", e)
    return token


def fetch_vercel_oidc_token(
    auth_token: str, project_id: str, team_id: str | None
) -> VercelTokenResponse | None:
    url = f"{BASE_URL}/projects/{project_id}/token?source=vercel-oidc-refresh"
    if team_id:
        url += f"&teamId={team_id}"
    with httpx.Client(timeout=httpx.Timeout(30.0)) as client:
        r = client.post(url, headers={"authorization": f"Bearer {auth_token}"})
        if not (200 <= r.status_code < 300):
            raise RuntimeError(f"Failed to refresh OIDC token: {r.status_code} {r.reason_phrase}")
        data = r.json()
        if not isinstance(data, dict) or not isinstance(data.get("token"), str):
            raise TypeError("Expected a string-valued token property")
        return VercelTokenResponse(token=data["token"])


async def fetch_vercel_oidc_token_async(
    auth_token: str, project_id: str, team_id: str | None
) -> VercelTokenResponse | None:
    url = f"{BASE_URL}/projects/{project_id}/token?source=vercel-oidc-refresh"
    if team_id:
        url += f"&teamId={team_id}"
    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
        r = await client.post(url, headers={"authorization": f"Bearer {auth_token}"})
        if not (200 <= r.status_code < 300):
            raise RuntimeError(f"Failed to refresh OIDC token: {r.status_code} {r.reason_phrase}")
        data = r.json()
        if not isinstance(data, dict) or not isinstance(data.get("token"), str):
            raise TypeError("Expected a string-valued token property")
        return VercelTokenResponse(token=data["token"])


def decode_oidc_payload(token: str) -> dict:
    return get_token_payload(token)

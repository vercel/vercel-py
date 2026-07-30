from __future__ import annotations

import os
import threading
import time
from collections.abc import Mapping
from typing import Any

import httpx

from vercel.headers import get_headers

from .types import VercelTokenResponse
from .utils import (
    find_project_info,
    get_token_payload,
    get_vercel_cli_token,
    is_expired,
    load_token,
    save_token,
)

BASE_URL = "https://api.vercel.com/v1"
_cached_oidc_token_lock = threading.Lock()
_cached_oidc_token: str | None = None
_cached_oidc_payload: dict[str, Any] | None = None


class VercelOidcTokenError(Exception):
    def __init__(self, message: str, cause: Exception | None = None):
        if cause is not None:
            message = f"{message}: {cause}"
        super().__init__(message)
        self.cause = cause


def get_vercel_oidc_token_from_context() -> str:
    # Prefer request header registered in the OIDC context,
    # fall back to environment variable like the TypeScript SDK.
    token_from_header = _token_from_headers(get_headers())
    if token_from_header:
        token = _select_header_or_cached_token(token_from_header)
        if token is not None:
            return token
    else:
        token_from_cache = _get_cached_unexpired_token()
        if token_from_cache is not None:
            return token_from_cache

    token_from_env = os.getenv("VERCEL_OIDC_TOKEN")
    if not token_from_env:
        raise VercelOidcTokenError(
            "The 'x-vercel-oidc-token' header is missing from the request. "
            "Do you have the OIDC option enabled in the Vercel project settings?"
        )
    return token_from_env


def _token_from_headers(headers: object) -> str | None:
    if not headers:
        return None
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
    return lower_headers.get("x-vercel-oidc-token")


def _select_header_or_cached_token(token: str) -> str | None:
    try:
        payload = get_token_payload(token)
    except Exception:
        return token
    if _is_past_expiration(payload):
        return _get_cached_unexpired_token()

    with _cached_oidc_token_lock:
        global _cached_oidc_payload, _cached_oidc_token
        if _cached_oidc_token is None or _cached_oidc_payload is None:
            _cached_oidc_token = token
            _cached_oidc_payload = payload
            return token
        if _is_past_expiration(_cached_oidc_payload):
            _cached_oidc_token = token
            _cached_oidc_payload = payload
            return token
        if _expires_after(payload, _cached_oidc_payload):
            _cached_oidc_token = token
            _cached_oidc_payload = payload
            return token
        return _cached_oidc_token


def _get_cached_unexpired_token() -> str | None:
    with _cached_oidc_token_lock:
        global _cached_oidc_payload, _cached_oidc_token
        if _cached_oidc_token is None or _cached_oidc_payload is None:
            return None
        if _is_past_expiration(_cached_oidc_payload):
            _cached_oidc_token = None
            _cached_oidc_payload = None
            return None
        return _cached_oidc_token


def _is_past_expiration(payload: dict[str, Any]) -> bool:
    exp = payload.get("exp")
    if not isinstance(exp, (int, float)):
        return True
    return exp <= time.time()


def _expires_after(left: dict[str, Any], right: dict[str, Any]) -> bool:
    left_exp = left.get("exp")
    right_exp = right.get("exp")
    if not isinstance(left_exp, (int, float)):
        return False
    if not isinstance(right_exp, (int, float)):
        return True
    return left_exp > right_exp


def _clear_cached_oidc_token() -> None:
    with _cached_oidc_token_lock:
        global _cached_oidc_payload, _cached_oidc_token
        _cached_oidc_token = None
        _cached_oidc_payload = None


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
                ) from e
            refresh_token()
            token = get_vercel_oidc_token_from_context()
    except Exception as e:
        if err and isinstance(e, Exception) and getattr(err, "message", None):
            e.args = (f"{err}\n{e}",)
        raise VercelOidcTokenError("Failed to refresh OIDC token", e) from e
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
                ) from e
            await refresh_token_async()
            token = get_vercel_oidc_token_from_context()
    except Exception as e:
        if err and isinstance(e, Exception) and getattr(err, "message", None):
            e.args = (f"{err}\n{e}",)
        raise VercelOidcTokenError("Failed to refresh OIDC token", e) from e
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

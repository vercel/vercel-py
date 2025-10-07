from __future__ import annotations

import base64
import json
import os
import sys
from dataclasses import dataclass
from typing import Any

import httpx


@dataclass
class VercelTokenResponse:
    token: str


def _user_data_dir() -> str | None:
    try:
        home = os.path.expanduser("~")
        if sys.platform.startswith("win"):
            return os.environ.get("APPDATA") or os.path.join(home, "AppData", "Roaming")
        if sys.platform == "darwin":
            return os.path.join(home, "Library", "Application Support")
        # linux and others
        return os.environ.get("XDG_CONFIG_HOME") or os.path.join(home, ".config")
    except Exception:
        return None


def get_vercel_data_dir() -> str | None:
    base = _user_data_dir()
    if not base:
        return None
    return os.path.join(base, "com.vercel.cli")


def get_vercel_cli_token() -> str | None:
    data_dir = get_vercel_data_dir()
    if not data_dir:
        return None
    token_path = os.path.join(data_dir, "auth.json")
    if not os.path.exists(token_path):
        return None
    try:
        with open(token_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        token = data.get("token")
        if isinstance(token, str) and token:
            return token
        return None
    except Exception:
        return None


def _find_root_dir(start: str | None = None) -> str | None:
    # Walk up from start (or cwd) looking for .vercel/project.json
    current = os.path.abspath(start or os.getcwd())
    while True:
        prj_json = os.path.join(current, ".vercel", "project.json")
        if os.path.exists(prj_json):
            return current
        parent = os.path.dirname(current)
        if parent == current:
            return None
        current = parent


def find_project_info() -> dict[str, str]:
    root = _find_root_dir()
    if not root:
        raise RuntimeError("Unable to find root directory")
    prj_path = os.path.join(root, ".vercel", "project.json")
    if not os.path.exists(prj_path):
        raise RuntimeError("project.json not found")
    try:
        with open(prj_path, "r", encoding="utf-8") as f:
            prj = json.load(f)
        project_id = prj.get("projectId") or prj.get("orgId")
        team_id = prj.get("orgId")
        if not isinstance(project_id, str):
            raise TypeError("Expected a string-valued projectId property")
        return {"projectId": project_id, "teamId": team_id}
    except Exception as e:
        raise RuntimeError("Unable to find project ID") from e


def _token_store_dir() -> str | None:
    base = _user_data_dir()
    if not base:
        return None
    return os.path.join(base, "com.vercel.token")


def save_token(token: VercelTokenResponse, project_id: str) -> None:
    directory = _token_store_dir()
    if not directory:
        raise RuntimeError("Unable to find user data directory")
    try:
        os.makedirs(directory, mode=0o700, exist_ok=True)
        token_path = os.path.join(directory, f"{project_id}.json")
        with open(token_path, "w", encoding="utf-8") as f:
            json.dump({"token": token.token}, f)
        try:
            os.chmod(token_path, 0o600)
        except Exception:
            pass
    except Exception as e:
        raise RuntimeError("Failed to save token") from e


def load_token(project_id: str) -> VercelTokenResponse | None:
    directory = _token_store_dir()
    if not directory:
        return None
    token_path = os.path.join(directory, f"{project_id}.json")
    if not os.path.exists(token_path):
        return None
    try:
        with open(token_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        token = data.get("token")
        if isinstance(token, str):
            return VercelTokenResponse(token=token)
        return None
    except Exception as e:
        raise RuntimeError("Failed to load token") from e


def get_token_payload(token: str) -> dict[str, Any]:
    parts = token.split(".")
    if len(parts) != 3:
        raise ValueError("Invalid token")
    base64_part = parts[1].replace("-", "+").replace("_", "/")
    padded = base64_part + "=" * ((4 - (len(base64_part) % 4)) % 4)
    decoded = base64.b64decode(padded)
    return json.loads(decoded.decode("utf-8"))


def is_expired(payload: dict[str, Any]) -> bool:
    # Consider token expired if it will expire within the next 15 minutes
    exp = payload.get("exp")
    if not isinstance(exp, (int, float)):
        return True
    import time

    fifteen_minutes_ms = 15 * 60 * 1000
    now_ms = int(time.time() * 1000)
    return int(exp * 1000) < now_ms + fifteen_minutes_ms


async def fetch_vercel_oidc_token(auth_token: str, project_id: str, team_id: str | None) -> VercelTokenResponse | None:
    url = f"https://api.vercel.com/v1/projects/{project_id}/token?source=vercel-oidc-refresh"
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



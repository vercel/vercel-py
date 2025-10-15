from __future__ import annotations

import base64
import json
import os
import sys
from typing import Any

from .types import ProjectInfo, VercelTokenResponse


def _user_data_dir() -> str | None:
    try:
        home = os.path.expanduser("~")
        if sys.platform.startswith("win"):
            # Prefer LOCALAPPDATA for application data storage on Windows
            return os.environ.get("LOCALAPPDATA")
        if sys.platform == "darwin":
            return os.path.join(home, "Library", "Application Support")
        # linux and others
        xdg_data_home = os.environ.get("XDG_DATA_HOME")
        return xdg_data_home or os.path.join(home, ".local", "share")
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
    # Walk up from start (or cwd) looking for a .vercel folder (align with TS SDK)
    current = os.path.abspath(start or os.getcwd())
    while True:
        vercel_dir = os.path.join(current, ".vercel")
        if os.path.isdir(vercel_dir):
            return current
        parent = os.path.dirname(current)
        if parent == current:
            return None
        current = parent


def find_project_info() -> ProjectInfo:
    root = _find_root_dir()
    if not root:
        raise RuntimeError("Unable to find root directory")
    prj_path = os.path.join(root, ".vercel", "project.json")
    if not os.path.exists(prj_path):
        raise RuntimeError("project.json not found")
    try:
        with open(prj_path, "r", encoding="utf-8") as f:
            prj = json.load(f)
        project_id = prj.get("projectId")
        team_id = prj.get("orgId") if isinstance(prj.get("orgId"), str) else None
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

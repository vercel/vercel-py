from __future__ import annotations

import os
from dataclasses import dataclass, asdict
from typing import Mapping


__all__ = ["Env", "get_env"]


@dataclass(frozen=True)
class Env:
    VERCEL: str | None = None
    CI: str | None = None
    VERCEL_ENV: str | None = None
    VERCEL_URL: str | None = None
    VERCEL_BRANCH_URL: str | None = None
    VERCEL_PROJECT_PRODUCTION_URL: str | None = None
    VERCEL_REGION: str | None = None
    VERCEL_DEPLOYMENT_ID: str | None = None
    VERCEL_SKEW_PROTECTION_ENABLED: str | None = None
    VERCEL_AUTOMATION_BYPASS_SECRET: str | None = None
    VERCEL_GIT_PROVIDER: str | None = None
    VERCEL_GIT_REPO_SLUG: str | None = None
    VERCEL_GIT_REPO_OWNER: str | None = None
    VERCEL_GIT_REPO_ID: str | None = None
    VERCEL_GIT_COMMIT_REF: str | None = None
    VERCEL_GIT_COMMIT_SHA: str | None = None
    VERCEL_GIT_COMMIT_MESSAGE: str | None = None
    VERCEL_GIT_COMMIT_AUTHOR_LOGIN: str | None = None
    VERCEL_GIT_COMMIT_AUTHOR_NAME: str | None = None
    VERCEL_GIT_PREVIOUS_SHA: str | None = None
    VERCEL_GIT_PULL_REQUEST_ID: str | None = None

    def to_dict(self) -> dict[str, str | None]:
        return asdict(self)

    def __getitem__(self, key: str) -> str | None:
        try:
            return getattr(self, key)
        except AttributeError as exc:
            raise KeyError(key) from exc

    def get(self, key: str, default: str | None = None) -> str | None:
        return getattr(self, key, default)


def _get(env: Mapping[str, str], key: str) -> str | None:
    value = env.get(key)
    if value == "":
        return None
    return value


def get_env(env: Mapping[str, str] | None = None) -> Env:
    """Return Vercel system environment variables.

    Empty strings are normalized to ``None``. Returns an immutable ``Env``
    dataclass that supports both attribute access (e.g. ``ev.VERCEL_URL``)
    and mapping-like access (e.g. ``ev["VERCEL_URL"]``).
    """
    if env is None:
        env = os.environ

    return Env(
        VERCEL=_get(env, "VERCEL"),
        CI=_get(env, "CI"),
        VERCEL_ENV=_get(env, "VERCEL_ENV"),
        VERCEL_URL=_get(env, "VERCEL_URL"),
        VERCEL_BRANCH_URL=_get(env, "VERCEL_BRANCH_URL"),
        VERCEL_PROJECT_PRODUCTION_URL=_get(env, "VERCEL_PROJECT_PRODUCTION_URL"),
        VERCEL_REGION=_get(env, "VERCEL_REGION"),
        VERCEL_DEPLOYMENT_ID=_get(env, "VERCEL_DEPLOYMENT_ID"),
        VERCEL_SKEW_PROTECTION_ENABLED=_get(env, "VERCEL_SKEW_PROTECTION_ENABLED"),
        VERCEL_AUTOMATION_BYPASS_SECRET=_get(env, "VERCEL_AUTOMATION_BYPASS_SECRET"),
        VERCEL_GIT_PROVIDER=_get(env, "VERCEL_GIT_PROVIDER"),
        VERCEL_GIT_REPO_SLUG=_get(env, "VERCEL_GIT_REPO_SLUG"),
        VERCEL_GIT_REPO_OWNER=_get(env, "VERCEL_GIT_REPO_OWNER"),
        VERCEL_GIT_REPO_ID=_get(env, "VERCEL_GIT_REPO_ID"),
        VERCEL_GIT_COMMIT_REF=_get(env, "VERCEL_GIT_COMMIT_REF"),
        VERCEL_GIT_COMMIT_SHA=_get(env, "VERCEL_GIT_COMMIT_SHA"),
        VERCEL_GIT_COMMIT_MESSAGE=_get(env, "VERCEL_GIT_COMMIT_MESSAGE"),
        VERCEL_GIT_COMMIT_AUTHOR_LOGIN=_get(env, "VERCEL_GIT_COMMIT_AUTHOR_LOGIN"),
        VERCEL_GIT_COMMIT_AUTHOR_NAME=_get(env, "VERCEL_GIT_COMMIT_AUTHOR_NAME"),
        VERCEL_GIT_PREVIOUS_SHA=_get(env, "VERCEL_GIT_PREVIOUS_SHA"),
        VERCEL_GIT_PULL_REQUEST_ID=_get(env, "VERCEL_GIT_PULL_REQUEST_ID"),
    )

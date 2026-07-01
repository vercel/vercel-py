from __future__ import annotations

from typing import Final, TypeAlias, final

import os
import re
from collections.abc import Callable, Mapping

from vercel.oidc import get_vercel_oidc_token
from vercel.oidc.aio import get_vercel_oidc_token as get_vercel_oidc_token_async

from .constants import (
    HEADER_AUTHORIZATION,
    HEADER_CONTENT_TYPE,
    VQS_HEADER_DELAY_SECONDS,
    VQS_HEADER_DEPLOYMENT_ID,
    VQS_HEADER_IDEMPOTENCY_KEY,
    VQS_HEADER_MAX_MESSAGES,
    VQS_HEADER_RETENTION_SECONDS,
    VQS_HEADER_VISIBILITY_TIMEOUT_SECONDS,
)
from .errors import DeploymentResolutionError, TokenResolutionError

DeploymentID: TypeAlias = str
BaseUrl: TypeAlias = str | Callable[[str], str]


@final
class CurrentDeployment:
    pass


@final
class AllDeployments:
    pass


CURRENT_DEPLOYMENT: Final = CurrentDeployment()
ALL_DEPLOYMENTS: Final = AllDeployments()
DeploymentOption: TypeAlias = DeploymentID | CurrentDeployment | AllDeployments
REGION_PATTERN = re.compile(r"^[a-z]{2,5}[0-9]{1,2}$")

PROTECTED_HEADERS = {
    HEADER_AUTHORIZATION.lower(),
    HEADER_CONTENT_TYPE.lower(),
    VQS_HEADER_DELAY_SECONDS.lower(),
    VQS_HEADER_DEPLOYMENT_ID.lower(),
    VQS_HEADER_IDEMPOTENCY_KEY.lower(),
    VQS_HEADER_MAX_MESSAGES.lower(),
    VQS_HEADER_RETENTION_SECONDS.lower(),
    VQS_HEADER_VISIBILITY_TIMEOUT_SECONDS.lower(),
}


def resolve_region(region: str | None = None) -> str:
    resolved = region or os.environ.get("VERCEL_REGION")
    if resolved is None:
        raise ValueError("Queue region is required. Provide 'region' or set VERCEL_REGION.")
    if not REGION_PATTERN.fullmatch(resolved):
        raise ValueError(f"Invalid queue region: {resolved!r}")
    return resolved


def validate_region(region: str | None) -> str | None:
    if region is None:
        return None
    if not REGION_PATTERN.fullmatch(region):
        raise ValueError(f"Invalid queue region: {region!r}")
    return region


def resolve_base_url(base_url: BaseUrl | None = None, *, region: str | None = None) -> str:
    resolved_region = resolve_region(region)
    if base_url is not None:
        if isinstance(base_url, str):
            resolved = base_url.format(region=resolved_region)
        else:
            resolved = base_url(resolved_region)
        return resolved.rstrip("/")
    env_base_url = os.environ.get("VERCEL_QUEUE_BASE_URL")
    if env_base_url:
        return env_base_url.format(region=resolved_region).rstrip("/")
    return f"https://{resolved_region}.vercel-queue.com"


def resolve_deployment(deployment: DeploymentOption = CURRENT_DEPLOYMENT) -> str | None:
    if deployment is ALL_DEPLOYMENTS:
        return None
    if isinstance(deployment, str):
        return deployment
    env_deployment = os.environ.get("VERCEL_DEPLOYMENT_ID")
    if env_deployment:
        return env_deployment
    raise DeploymentResolutionError(
        "Failed to resolve queue deployment ID. Provide 'deployment', "
        "set VERCEL_DEPLOYMENT_ID, or pass deployment=ALL_DEPLOYMENTS "
        "to explicitly omit it."
    )


def resolve_token(token: str | None = None) -> str:
    if token:
        return token
    env_token = os.environ.get("VERCEL_QUEUE_TOKEN")
    if env_token:
        return env_token
    try:
        resolved = get_vercel_oidc_token()
    except Exception as exc:
        raise TokenResolutionError(
            "Failed to resolve queue token. Provide 'token', set VERCEL_QUEUE_TOKEN, "
            "or make a Vercel OIDC token available."
        ) from exc
    if not resolved:
        raise TokenResolutionError("Failed to resolve queue token: OIDC returned an empty token")
    return resolved


async def resolve_token_async(token: str | None = None) -> str:
    if token:
        return token
    env_token = os.environ.get("VERCEL_QUEUE_TOKEN")
    if env_token:
        return env_token
    try:
        resolved = await get_vercel_oidc_token_async()
    except Exception as exc:
        raise TokenResolutionError(
            "Failed to resolve queue token. Provide 'token', set VERCEL_QUEUE_TOKEN, "
            "or make a Vercel OIDC token available."
        ) from exc
    if not resolved:
        raise TokenResolutionError("Failed to resolve queue token: OIDC returned an empty token")
    return resolved


def apply_custom_headers(headers: dict[str, str], custom_headers: Mapping[str, str] | None) -> None:
    for key, value in (custom_headers or {}).items():
        if key.lower() in PROTECTED_HEADERS or key.lower().startswith("vqs-"):
            continue
        headers[str(key)] = str(value)


# Only add public symbols to __all__; internal helpers must stay unexported.
__all__ = (
    "ALL_DEPLOYMENTS",
    "CURRENT_DEPLOYMENT",
    "AllDeployments",
    "BaseUrl",
    "CurrentDeployment",
    "DeploymentID",
    "DeploymentOption",
    "apply_custom_headers",
    "resolve_base_url",
    "resolve_deployment",
    "resolve_region",
    "resolve_token",
    "resolve_token_async",
    "validate_region",
)

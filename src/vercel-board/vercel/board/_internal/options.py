"""Board service options, credentials, and environment resolution."""

from __future__ import annotations

import hashlib
import inspect
import json
import math
import os
import re
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Annotated, Any, Final, TypeAlias
from urllib.parse import urlparse

from pydantic import BeforeValidator, Field, field_validator

from vercel._internal.core.options import ServiceOptions
from vercel._internal.core.time import SECOND, coerce_duration
from vercel.board._internal.base import BoardValueModel
from vercel.board._internal.errors import BoardCredentialsError, BoardValidationError
from vercel.board._internal.models import DurationInput

DEFAULT_API_BASE_URL: Final[str] = "https://api.vercel.com"
"""Default Vercel API origin for hosted Board requests."""

DEFAULT_BOARD_TIMEOUT: Final[timedelta] = timedelta(seconds=30)
"""Default per-request timeout for Board API requests."""

GITHUB_AUDIENCE: Final[str] = "https://api.vercel.com/v1/messageboard"
"""OIDC audience requested from GitHub Actions for Vercel hosted Board."""

_API_BASE_URL_RE: Final[re.Pattern[str]] = re.compile(
    r"\A(?:https://[^/\s]+|http://(?:localhost|127\.0\.0\.1)(?::\d+)?)\Z"
)
_TEAM_RE: Final[re.Pattern[str]] = re.compile(r"\A[^\s/]+\Z")
_LOOPBACK_HOSTS: Final[frozenset[str]] = frozenset({"127.0.0.1", "localhost", "::1"})


@dataclass(frozen=True, slots=True)
class BoardCredentials:
    """Bearer token credential for Board API requests.

    ``repr()`` and ``str()`` always redact the token so secrets do not leak
    into logs or tracebacks.
    """

    token: str

    def __post_init__(self) -> None:
        if not isinstance(self.token, str) or not self.token:
            raise BoardValidationError("BoardCredentials.token must be a non-empty string")

    def __repr__(self) -> str:
        return "BoardCredentials(token='***')"

    def __str__(self) -> str:
        return "BoardCredentials(token='***')"


BoardCredentialsFactory: TypeAlias = Callable[[], Awaitable[BoardCredentials | str]]
"""Async callable resolving the bearer credential for Board requests."""

SyncBoardCredentialsFactory: TypeAlias = Callable[[], BoardCredentials | str]
"""Sync callable resolving the bearer credential for Board requests."""


def static_credentials_factory(token: str) -> BoardCredentialsFactory:
    """Return an async credentials factory that always yields ``token``."""
    creds = BoardCredentials(token=token)

    async def factory() -> BoardCredentials:
        return creds

    return factory


def sync_static_credentials_factory(token: str) -> SyncBoardCredentialsFactory:
    """Return a sync credentials factory that always yields ``token``."""
    creds = BoardCredentials(token=token)

    def factory() -> BoardCredentials:
        return creds

    return factory


def normalize_credentials(value: object) -> BoardCredentials:
    """Coerce a factory return value into ``BoardCredentials``."""
    if isinstance(value, BoardCredentials):
        return value
    if isinstance(value, str) and value:
        return BoardCredentials(token=value)
    raise BoardCredentialsError(
        "credentials_factory must return BoardCredentials or a non-empty token string",
        code="invalid_credentials",
    )


def adapt_sync_credentials_factory(
    factory: SyncBoardCredentialsFactory,
) -> BoardCredentialsFactory:
    """Adapt a sync credentials factory for use inside the shared async service."""

    async def resolve() -> BoardCredentials:
        result = factory()
        if inspect.isawaitable(result):
            close = getattr(result, "close", None)
            if callable(close):
                close()
            raise BoardCredentialsError(
                "a sync session's credentials_factory must return credentials directly, "
                "not an awaitable; use vercel.board for an async factory",
                code="invalid_credentials",
            )
        return normalize_credentials(result)

    return resolve


def default_board_url() -> str | None:
    """Return ``BOARD_URL`` from the environment with trailing slashes stripped, or ``None``."""
    raw = os.environ.get("BOARD_URL", "").strip()
    if not raw:
        return None
    return raw.rstrip("/")


def _coerce_timeout(value: object) -> timedelta:
    if isinstance(value, bool):
        raise BoardValidationError("timeout must be a positive int, float, or timedelta")
    if isinstance(value, float) and not math.isfinite(value):
        raise BoardValidationError("timeout must be a finite positive duration")
    if isinstance(value, (int, float, timedelta)):
        coerced = coerce_duration(value, SECOND)
    else:
        raise BoardValidationError("timeout must be a positive int, float, or timedelta")
    if coerced <= timedelta(0):
        raise BoardValidationError(f"timeout must be positive, got {coerced}")
    return coerced


def _validate_team_value(value: str | None) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or _TEAM_RE.fullmatch(value) is None:
        raise BoardValidationError(f"{value!r} is not a valid team: pass its ID (team_...) or slug")
    return value


def _validate_callable_factory(value: Any) -> Any:
    if value is None:
        return None
    if not callable(value):
        raise BoardValidationError(
            "credentials_factory must be a callable returning credentials, "
            f"got {type(value).__name__}"
        )
    return value


class _BoardServiceOptionsKey(ServiceOptions):
    """Shared registry key for sync and async Board service options."""

    __slots__ = ()

    @classmethod
    def service_options_key(cls) -> type[ServiceOptions]:
        return _BoardServiceOptionsKey


class _BoardServiceOptionsBase(BoardValueModel):
    url: str | None = Field(default_factory=default_board_url)
    api_base_url: str = DEFAULT_API_BASE_URL
    team: str | None = None
    timeout: Annotated[timedelta, BeforeValidator(_coerce_timeout)] = DEFAULT_BOARD_TIMEOUT

    @field_validator("url")
    @classmethod
    def _validate_url(cls, value: str | None) -> str | None:
        if value is None:
            return None
        stripped = value.strip().rstrip("/")
        if not stripped:
            raise BoardValidationError("url must be a non-empty HTTP(S) URL or None")
        parsed = urlparse(stripped)
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            raise BoardValidationError(f"url must be an http:// or https:// URL: {value!r}")
        return stripped

    @field_validator("api_base_url")
    @classmethod
    def _validate_api_base_url(cls, value: str) -> str:
        stripped = value.strip().rstrip("/")
        if _API_BASE_URL_RE.fullmatch(stripped) is None:
            raise BoardValidationError(
                f"api_base_url must be an https origin or http://localhost: {value!r}"
            )
        return stripped

    @field_validator("team")
    @classmethod
    def _validate_team(cls, value: str | None) -> str | None:
        return _validate_team_value(value)


class BoardServiceOptions(_BoardServiceOptionsBase, _BoardServiceOptionsKey):
    """Configuration for ``vercel.board`` calls in an async SDK session.

    Attributes:
        url: Base URL of a self-hosted or ``vb serve`` Board endpoint. Defaults
            to ``BOARD_URL`` when set, else ``None`` (Vercel hosted API).
        api_base_url: Vercel API origin used in hosted mode. Defaults to
            ``https://api.vercel.com``.
        team: Default Vercel team ID (``team_...``) or slug for hosted mode.
        credentials_factory: Async callable returning ``BoardCredentials`` or a
            bearer token string.
        timeout: Per-request HTTP timeout (seconds or ``timedelta``).
    """

    credentials_factory: BoardCredentialsFactory | None = None

    @field_validator("credentials_factory", mode="before")
    @classmethod
    def _check_credentials_factory(cls, value: Any) -> Any:
        return _validate_callable_factory(value)


class SyncBoardServiceOptions(_BoardServiceOptionsBase, _BoardServiceOptionsKey):
    """Configuration for ``vercel.board.sync`` calls in a sync SDK session.

    Attributes:
        url: Base URL of a self-hosted or ``vb serve`` Board endpoint. Defaults
            to ``BOARD_URL`` when set, else ``None`` (Vercel hosted API).
        api_base_url: Vercel API origin used in hosted mode. Defaults to
            ``https://api.vercel.com``.
        team: Default Vercel team ID (``team_...``) or slug for hosted mode.
        credentials_factory: Sync callable returning ``BoardCredentials`` or a
            bearer token string.
        timeout: Per-request HTTP timeout (seconds or ``timedelta``).
    """

    credentials_factory: SyncBoardCredentialsFactory | None = None

    @field_validator("credentials_factory", mode="before")
    @classmethod
    def _check_credentials_factory(cls, value: Any) -> Any:
        return _validate_callable_factory(value)


# ---------------------------------------------------------------------------
# Environment & filesystem resolution helpers (NEVER reads VERCEL_TOKEN)
# ---------------------------------------------------------------------------


def config_dir() -> Path:
    """Return ``$XDG_CONFIG_HOME/board`` or ``~/.config/board``."""
    xdg = os.environ.get("XDG_CONFIG_HOME")
    base = Path(xdg) if xdg else Path.home() / ".config"
    return base / "board"


def is_loopback_url(url: str) -> bool:
    """Return whether ``url`` targets a loopback host."""
    hostname = urlparse(url).hostname
    return hostname in _LOOPBACK_HOSTS


def local_token_file(url: str) -> Path:
    """Return the local token file path for a ``vb serve`` Board URL."""
    normalized = url.rstrip("/")
    digest = hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:32]
    return config_dir() / "local" / f"{digest}.json"


def read_local_token(url: str) -> str | None:
    """Read the loopback local token file for ``url`` if present and matching."""
    if not is_loopback_url(url):
        return None
    normalized = url.rstrip("/")
    path = local_token_file(normalized)
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not isinstance(data, dict):
        return None
    if data.get("url") != normalized:
        return None
    token = data.get("token")
    return token if isinstance(token, str) and token else None


def token_cache_file(audience: str, login: str) -> Path:
    """Return the identity-scoped token cache path for ``(audience, login)``."""
    key = f"{audience}\n{login}"
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()[:32]
    return config_dir() / "tokens" / f"{digest}.json"


def read_cached_token(audience: str, login: str) -> str | None:
    """Read a cached exchanged token for ``(audience, login)`` if valid for >5 minutes."""
    path = token_cache_file(audience, login)
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not isinstance(data, dict):
        return None
    token = data.get("token")
    expires_at = data.get("expiresAt")
    if not isinstance(token, str) or not token:
        return None
    if isinstance(expires_at, bool) or not isinstance(expires_at, (int, float)):
        return None
    now_ms = time.time() * 1000
    if expires_at - 5 * 60_000 > now_ms:
        return token
    return None


def write_cached_token(audience: str, login: str, token: str, expires_at_ms: float) -> None:
    """Persist an identity-scoped cached token for ``(audience, login)`` best-effort."""
    path = token_cache_file(audience, login)
    try:
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        path.write_text(
            json.dumps({"token": token, "expiresAt": expires_at_ms}),
            encoding="utf-8",
        )
        try:
            os.chmod(path, 0o600)
        except OSError:
            pass
    except OSError:
        pass


def is_own_deployment_host(url: str) -> bool:
    """Return whether ``url`` targets one of this deployment's own Vercel hosts."""
    host = urlparse(url).netloc.lower()
    if not host:
        return False
    for var in ("VERCEL_URL", "VERCEL_BRANCH_URL", "VERCEL_PROJECT_PRODUCTION_URL"):
        own = os.environ.get(var, "").strip().lower()
        if own and own == host:
            return True
    return False


def token_expiry_ms(token: str) -> float:
    """Return JWT ``exp`` in epoch milliseconds, or 5 minutes from now if unreadable."""
    try:
        from vercel.oidc.utils import get_token_payload

        payload = get_token_payload(token)
        exp = payload.get("exp") if isinstance(payload, dict) else None
        if not isinstance(exp, bool) and isinstance(exp, (int, float)):
            return float(exp) * 1000.0
    except Exception:
        pass
    return time.time() * 1000.0 + 5 * 60_000.0


def is_deployment_oidc_available() -> bool:
    """Whether deployment OIDC is active (``VERCEL=1`` or ``VERCEL_OIDC_TOKEN``)."""
    return os.environ.get("VERCEL") == "1" or bool(os.environ.get("VERCEL_OIDC_TOKEN"))


def is_github_actions_oidc_available() -> bool:
    """Whether GitHub Actions OIDC env vars are present."""
    return bool(
        os.environ.get("ACTIONS_ID_TOKEN_REQUEST_URL")
        and os.environ.get("ACTIONS_ID_TOKEN_REQUEST_TOKEN")
    )


def github_token_allowed_origin(api_base_url: str) -> bool:
    """Whether a GitHub Actions token for Vercel's board may be sent to ``api_base_url``."""
    parsed = urlparse(api_base_url)
    origin = f"{parsed.scheme}://{parsed.netloc}"
    return origin == DEFAULT_API_BASE_URL or (
        parsed.scheme == "http" and parsed.hostname in ("localhost", "127.0.0.1")
    )


def _is_git_checkout(directory: Path) -> bool:
    git_path = directory / ".git"
    try:
        if git_path.is_dir():
            return (git_path / "HEAD").exists()
        if git_path.is_file():
            return git_path.read_text(encoding="utf-8").startswith("gitdir:")
    except OSError:
        return False
    return False


def find_vercel_project_org_id(cwd: str | Path | None = None) -> str | None:
    """Read ``orgId`` from the nearest ``.vercel/project.json`` up to the Git root."""
    directory = Path(cwd or os.getcwd()).resolve()
    while True:
        candidate = directory / ".vercel" / "project.json"
        if candidate.is_file():
            try:
                data = json.loads(candidate.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                return None
            org_id = data.get("orgId") if isinstance(data, dict) else None
            return org_id if isinstance(org_id, str) and org_id else None
        parent = directory.parent
        if _is_git_checkout(directory) or parent == directory:
            return None
        directory = parent


def oidc_owner_id(token: str) -> str | None:
    """Extract ``owner_id`` claim from a Vercel OIDC JWT if present."""
    try:
        from vercel.oidc.utils import get_token_payload

        payload = get_token_payload(token)
        owner = payload.get("owner_id") if isinstance(payload, dict) else None
        return owner if isinstance(owner, str) and owner else None
    except Exception:
        return None


def get_vercel_cli_auth_token() -> str | None:
    """Read the local ``vercel login`` auth token file if available."""
    try:
        from vercel.oidc.utils import get_vercel_cli_token

        token = get_vercel_cli_token()
        return token if isinstance(token, str) and token else None
    except Exception:
        return None


async def resolve_deployment_oidc_token(*, sync_mode: bool) -> str:
    """Resolve the Vercel deployment OIDC token without suspending in sync mode."""
    try:
        if sync_mode:
            from vercel.oidc import get_vercel_oidc_token as get_sync_oidc_token

            token = get_sync_oidc_token()
        else:
            from vercel.oidc.aio import get_vercel_oidc_token as get_async_oidc_token

            token = await get_async_oidc_token()
    except Exception as exc:
        raise BoardCredentialsError(
            f"failed to resolve Vercel OIDC token: {exc}",
            code="no_token",
            hint="Enable OIDC on the project, run `vercel env pull`, or set BOARD_TOKEN.",
        ) from exc
    if not token:
        raise BoardCredentialsError(
            "no Vercel OIDC token available",
            code="no_token",
            hint="Enable OIDC on the project, run `vercel env pull`, or set BOARD_TOKEN.",
        )
    return token


__all__ = [
    "DEFAULT_API_BASE_URL",
    "GITHUB_AUDIENCE",
    "BoardCredentials",
    "BoardCredentialsFactory",
    "BoardServiceOptions",
    "DurationInput",
    "SyncBoardCredentialsFactory",
    "SyncBoardServiceOptions",
    "adapt_sync_credentials_factory",
    "config_dir",
    "default_board_url",
    "find_vercel_project_org_id",
    "get_vercel_cli_auth_token",
    "github_token_allowed_origin",
    "is_deployment_oidc_available",
    "is_github_actions_oidc_available",
    "is_loopback_url",
    "is_own_deployment_host",
    "local_token_file",
    "normalize_credentials",
    "oidc_owner_id",
    "read_cached_token",
    "read_local_token",
    "resolve_deployment_oidc_token",
    "static_credentials_factory",
    "sync_static_credentials_factory",
    "token_cache_file",
    "token_expiry_ms",
    "write_cached_token",
]

"""Wire-isolated dual-protocol API client for Vercel Board.

All wire Pydantic models, header encodings, board name resolution, 429
retry-with-backoff, and HTTP error mapping stay private to this module.
Callers pass and receive rich domain types only.
"""

from __future__ import annotations

import abc
import hashlib
import os
import platform
import random
import re
import sys
import time
import uuid
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from importlib.metadata import PackageNotFoundError, version as _pkg_version
from typing import Final, Literal, TypeVar
from urllib.parse import quote, urlencode, urlsplit, urlunsplit

import httpx2 as httpx
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from vercel._internal.core.http import (
    BaseTransport,
    JSONBody,
    ReadResponsePolicy,
    RequestBody,
)
from vercel._internal.core.time import from_epoch_ms, from_epoch_seconds
from vercel.board._internal.access import PERMISSIONS, Can, Permission
from vercel.board._internal.errors import (
    BoardApiError,
    BoardConflictError,
    BoardCredentialsError,
    BoardLabelConflictError,
    BoardNewerMessageError,
    BoardNotFoundError,
    BoardPermissionError,
    BoardRateLimitError,
    BoardResponseError,
    BoardRevisionConflictError,
    BoardValidationError,
    ErrorOrigin,
)
from vercel.board._internal.models import (
    ActorKind,
    Discovery,
    DiscoverySync,
    Me,
    Profile,
    Via,
)
from vercel.board._internal.options import (
    GITHUB_AUDIENCE,
    BoardCredentialsFactory,
    find_vercel_project_org_id,
    get_vercel_cli_auth_token,
    github_token_allowed_origin,
    is_deployment_oidc_available,
    is_github_actions_oidc_available,
    is_loopback_url,
    is_own_deployment_host,
    normalize_credentials,
    oidc_owner_id,
    read_cached_token,
    read_local_token,
    resolve_deployment_oidc_token,
    token_expiry_ms,
    write_cached_token,
)

try:
    VERSION = _pkg_version("vercel-board")
except PackageNotFoundError:  # pragma: no cover - bundled distribution
    try:
        VERSION = _pkg_version("vercel-board-bundle")
    except PackageNotFoundError:
        VERSION = "development"

_PLATFORM = platform.uname()
USER_AGENT: Final[str] = (
    f"vercel-board/{VERSION} (Python/{sys.version}; {_PLATFORM.system}/{_PLATFORM.machine})"
)

VERCEL_OIDC_EXCHANGE_URL: Final[str] = "https://oidc.vercel.com/~token"
MAX_RATE_LIMIT_RETRIES: Final[int] = 4
BASE_RETRY_DELAY_SECONDS: Final[float] = 1.0
WHOLE_WINDOW_SECONDS: Final[int] = 59
THROTTLED_WAIT_MAX_SECONDS: Final[int] = 15
_SECONDS_BEFORE: Final[float] = 1e12

_PROJECT_ID_RE: Final[re.Pattern[str]] = re.compile(r"\Aprj_[A-Za-z0-9]{1,64}\Z")
_OWNER_ID_RE: Final[re.Pattern[str]] = re.compile(r"\A(?:team_)?[A-Za-z0-9]{1,64}\Z")
_KNOWN_PERMISSIONS: Final[frozenset[str]] = frozenset(p.value for p in PERMISSIONS)
_DEFAULT_HOSTED_CAN: Final[Can] = Can.from_input(
    [Permission.READ, Permission.POST, Permission.REACT, Permission.LABEL]
)

SleepFn = Callable[[float], Awaitable[None]]
_WireModelT = TypeVar("_WireModelT", bound=BaseModel)


# ---------------------------------------------------------------------------
# Private wire models (NEVER imported outside api_client.py)
# ---------------------------------------------------------------------------


class _WireModel(BaseModel):
    model_config = ConfigDict(extra="ignore", frozen=True, populate_by_name=True)


class _WireVia(_WireModel):
    name: str | None = None
    environment: str | None = None


class _HttpDiscoverySyncWire(_WireModel):
    key: str
    platform: str


class _HttpDiscoveryWire(_WireModel):
    version: str
    url: str
    audience: str
    team_id: str | None = None
    project_id: str | None = None
    name: str | None = None
    sync: list[_HttpDiscoverySyncWire] = Field(default_factory=list)
    wake_by_path: bool = False

    def to_domain(self) -> Discovery:
        return Discovery(
            version=self.version,
            url=self.url,
            audience=self.audience,
            team_id=self.team_id,
            project_id=self.project_id,
            name=self.name,
            sync=tuple(DiscoverySync(key=s.key, platform=s.platform) for s in self.sync),
            wake_by_path=self.wake_by_path,
        )


class _HttpMeWire(_WireModel):
    actor: str
    principal: str
    kind: str
    handle: str
    name: str
    avatar: str | None = None
    profile: str | None = None
    via: _WireVia | None = None
    expires_at: str | int | float | None = Field(default=None, alias="expiresAt")
    can: dict[str, list[str]] = Field(default_factory=dict)

    def to_domain(self) -> Me:
        via = (
            Via(name=self.via.name, environment=self.via.environment)
            if self.via is not None and self.via.name is not None
            else None
        )
        return Me(
            actor=self.actor,
            principal=self.principal,
            kind=_coerce_actor_kind(self.kind),
            handle=self.handle,
            name=self.name,
            avatar=self.avatar,
            profile=self.profile,
            via=via,
            expires_at=_parse_expiry(self.expires_at),
            can=Can.from_input(self.can) if self.can else Can.empty(),
        )


class _ApiPrincipalWire(_WireModel):
    type: str
    user_id: str | None = Field(default=None, alias="userId")
    client_id: str | None = Field(default=None, alias="clientId")
    project_id: str | None = Field(default=None, alias="projectId")
    environment: str | None = None
    repository_id: str | None = Field(default=None, alias="repositoryId")
    repository: str | None = None
    issuer: str | None = None
    subject: str | None = None
    id: str | None = None


class _ApiActorProfileWire(_WireModel):
    name: str
    avatar: str | None = None


class _ApiActorWire(_WireModel):
    id: str
    principal: _ApiPrincipalWire
    profile: _ApiActorProfileWire | None = None


class _ApiActorViewWire(_WireModel):
    id: str
    kind: str
    handle: str
    name: str | None = None
    avatar: str | None = None
    via: _WireVia | None = None
    platform: str | None = None


class _ApiMeWire(_WireModel):
    team_id: str = Field(alias="teamId")
    actor: _ApiActorWire
    view: _ApiActorViewWire | None = None
    can: dict[str, object] | None = None
    expires_at: int | float | str | None = Field(default=None, alias="expiresAt")

    def to_domain(self) -> Me:
        principal_str = _api_principal_id(self.actor)
        profile_name = self.actor.profile.name if self.actor.profile is not None else None
        if self.view is not None:
            kind = _coerce_actor_kind(self.view.kind)
            handle = self.view.handle
            name = self.view.name or self.view.handle
            avatar = self.view.avatar
        else:
            kind, handle, name, avatar = _fallback_actor_view(self.actor)

        via: Via | None = None
        if self.actor.profile is not None and self.actor.principal.type != "external":
            via = _via_for(self.actor.principal, self.view)

        return Me(
            actor=self.actor.id,
            principal=principal_str,
            kind=kind,
            handle=handle,
            name=name,
            avatar=avatar,
            profile=profile_name,
            via=via,
            expires_at=_parse_expiry(self.expires_at),
            can=_parse_hosted_can(self.can),
        )


class _ApiBoardEntryWire(_WireModel):
    id: str
    name: str | None = None


class _ApiBoardsListWire(_WireModel):
    default_board_id: str | None = Field(default=None, alias="defaultBoardId")
    boards: list[_ApiBoardEntryWire] = Field(default_factory=list)


class _ApiBoardEnvelopeWire(_WireModel):
    board: _ApiBoardEntryWire


class _GithubOidcResponseWire(_WireModel):
    value: str


class _OidcExchangeResponseWire(_WireModel):
    token: str
    expiry: int | float | None = None


class _VercelProjectTokenResponseWire(_WireModel):
    token: str


class _VercelHostLookupTeamWire(_WireModel):
    id: str | None = None


class _VercelHostLookupWire(_WireModel):
    project_id: str | None = Field(default=None, alias="projectId")
    owner_id: str | None = Field(default=None, alias="ownerId")
    team: _VercelHostLookupTeamWire | None = None


class _VercelTeamEntryWire(_WireModel):
    id: str
    limited: bool = False


class _VercelTeamsListWire(_WireModel):
    teams: list[_VercelTeamEntryWire] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Domain conversion, UUIDv7, & header formatting helpers
# ---------------------------------------------------------------------------


def _uuid7() -> str:
    """Return a canonical hyphenated UUIDv7 string (RFC 9562)."""
    raw = bytearray(os.urandom(16))
    now_ms = int(time.time() * 1000)
    raw[0:6] = now_ms.to_bytes(6, "big")
    raw[6] = (raw[6] & 0x0F) | 0x70
    raw[8] = (raw[8] & 0x3F) | 0x80
    return str(uuid.UUID(bytes=bytes(raw)))


def _coerce_actor_kind(raw: str) -> ActorKind:
    if raw == "app":
        return ActorKind.WORKFLOW
    try:
        return ActorKind(raw)
    except ValueError:
        return ActorKind.EXTERNAL


def _parse_expiry(value: str | int | float | None) -> datetime | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        if value < _SECONDS_BEFORE:
            return from_epoch_seconds(value)
        return from_epoch_ms(value)
    if isinstance(value, str) and value:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return (
                parsed.astimezone(UTC) if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)
            )
        except ValueError:
            return None
    return None


def _parse_hosted_can(raw: Mapping[str, object] | None) -> Can:
    """Normalize hosted ``/me`` ``can`` map, defaulting to ``_DEFAULT_HOSTED_CAN``.

    Unknown permissions and unreadable channel patterns are dropped rather than
    failing parsing, matching ``backend-vercel.ts:canOf``.
    """
    if not isinstance(raw, Mapping):
        return _DEFAULT_HOSTED_CAN
    filtered: dict[str, list[str]] = {}
    for pattern, perms in raw.items():
        if not isinstance(pattern, str) or not isinstance(perms, list):
            continue
        try:
            Can.from_input({pattern: []})
        except BoardValidationError:
            continue
        known = [p for p in perms if isinstance(p, str) and p in _KNOWN_PERMISSIONS]
        filtered[pattern] = known
    return Can.from_input(filtered)


def _api_principal_id(actor: _ApiActorWire) -> str:
    principal = actor.principal
    if principal.type == "external" and principal.id:
        return principal.id
    if principal.type == "user" and principal.user_id:
        return f"vercel:user:{principal.user_id}"
    if principal.type == "app" and principal.client_id:
        return f"vercel:app:{principal.client_id}"
    if principal.type == "project" and principal.project_id:
        env = principal.environment or "production"
        return f"vercel:project:{principal.project_id}:{env}"
    # GitHub & OIDC principals use the encoded actor ID prefix (before `+profile`).
    return actor.id.split("+", 1)[0]


def _short_principal_name(principal: _ApiPrincipalWire) -> str:
    if principal.type == "user":
        return f"user-{(principal.user_id or '')[:6].lower()}"
    if principal.type == "app":
        return f"app-{(principal.client_id or '')[:6].lower()}"
    if principal.type == "github":
        return principal.repository or "github"
    if principal.type == "oidc":
        return (principal.subject or "oidc")[:128]
    return principal.project_id or principal.type


def _via_for(principal: _ApiPrincipalWire, view: _ApiActorViewWire | None) -> Via:
    named = view.via.name if view is not None and view.via is not None else None
    if principal.type == "project":
        env = (
            view.via.environment
            if view is not None and view.via is not None and view.via.environment is not None
            else principal.environment
        )
        return Via(name=named or principal.project_id or "project", environment=env)
    username: str | None = None
    if principal.type == "user" and view is not None and "/" in view.handle:
        username = view.handle.split("/", 1)[0] or None
    return Via(name=named or username or _short_principal_name(principal), environment=None)


def _fallback_actor_view(actor: _ApiActorWire) -> tuple[ActorKind, str, str, str | None]:
    principal = actor.principal
    if principal.type == "external":
        handle = (principal.id or "external").rsplit(":", 1)[-1]
        return (ActorKind.EXTERNAL, handle, principal.id or handle, None)
    if actor.profile is not None:
        return (ActorKind.AGENT, actor.profile.name, actor.profile.name, actor.profile.avatar)
    if principal.type == "project":
        env = principal.environment or "production"
        handle = (
            principal.project_id or "project"
            if env == "production"
            else f"{principal.project_id or 'project'}@{env}"
        )
        return (ActorKind.DEPLOYMENT, handle, handle, None)
    handle = _short_principal_name(principal)
    kind = ActorKind.PERSON if principal.type == "user" else ActorKind.WORKFLOW
    return (kind, handle, handle, None)


def _normalize_avatar_ascii(value: str) -> str:
    """Normalize an avatar URL to printable ASCII (IDNA host + percent-encoded path)."""
    if any(ord(ch) < 0x20 or ord(ch) == 0x7F for ch in value):
        raise BoardValidationError(
            "avatar URL must contain only printable ASCII characters",
            code="invalid_profile",
        )
    if all(0x20 <= ord(ch) <= 0x7E for ch in value):
        return value

    try:
        parsed = urlsplit(value)
        port_num = parsed.port
    except ValueError as exc:
        raise BoardValidationError(
            f"avatar URL has an invalid authority or port: {value!r}",
            code="invalid_profile",
        ) from exc
    if not parsed.scheme or not parsed.netloc:
        raise BoardValidationError(
            "avatar URL must be a valid ASCII or internationalized URL",
            code="invalid_profile",
        )
    hostname = parsed.hostname
    if not hostname:
        raise BoardValidationError(
            "avatar URL has an invalid host",
            code="invalid_profile",
        )
    if ":" in hostname:
        ascii_host = f"[{hostname}]"
    else:
        try:
            ascii_host = hostname.encode("idna").decode("ascii")
        except UnicodeError as exc:
            raise BoardValidationError(
                f"avatar URL host cannot be IDNA-encoded: {hostname!r}",
                code="invalid_profile",
            ) from exc

    userinfo = ""
    if parsed.username is not None:
        safe_userinfo = "-._~!$&'()*+,;="
        userinfo = quote(parsed.username, safe=safe_userinfo)
        if parsed.password is not None:
            userinfo += f":{quote(parsed.password, safe=safe_userinfo)}"
        userinfo += "@"
    port = f":{port_num}" if port_num is not None else ""
    netloc = f"{userinfo}{ascii_host}{port}"

    normalized = urlunsplit(
        (
            parsed.scheme,
            netloc,
            quote(parsed.path, safe="/:@!$&'()*+,;=-._~%"),
            quote(parsed.query, safe="=&/:@!$'()*+,;-._~%?"),
            quote(parsed.fragment, safe="/:@!$&'()*+,;=-._~%?"),
        )
    )
    if not all(0x20 <= ord(ch) <= 0x7E for ch in normalized):
        raise BoardValidationError(
            "avatar URL could not be normalized to printable ASCII",
            code="invalid_profile",
        )
    return normalized


def _format_sf_string(value: str) -> str:
    ascii_value = _normalize_avatar_ascii(value)
    escaped = ascii_value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def format_http_profile_header(profile: Profile) -> str:
    """Format ``x-board-profile`` as an RFC 8941 structured-field Item."""
    if not profile.avatar:
        return profile.name
    return f"{profile.name};avatar={_format_sf_string(profile.avatar)}"


def format_hosted_profile_headers(profile: Profile | None) -> dict[str, str]:
    """Format separate ``x-board-profile`` and ``x-board-avatar`` for Vercel API."""
    if profile is None:
        return {}
    headers: dict[str, str] = {"x-board-profile": profile.name}
    if profile.avatar and profile.avatar.startswith("https://"):
        headers["x-board-avatar"] = _normalize_avatar_ascii(profile.avatar)
    return headers


def _parse_wire(response: httpx.Response, model: type[_WireModelT]) -> _WireModelT:
    try:
        payload = response.json()
    except Exception as exc:
        raise BoardResponseError(
            "Board API returned a non-JSON success response",
            status_code=response.status_code,
            origin=ErrorOrigin.BOARD,
            response=response,
        ) from exc
    try:
        return model.model_validate(payload)
    except ValidationError as exc:
        raise BoardResponseError(
            "Board API returned a malformed success response",
            status_code=response.status_code,
            origin=ErrorOrigin.BOARD,
            response=response,
        ) from exc


def _parse_retry_after_seconds(response: httpx.Response) -> int | None:
    raw = response.headers.get("retry-after", "").strip()
    if raw.isdigit():
        seconds = int(raw)
        return seconds if seconds > 0 else None
    return None


def _compute_retry_delay(attempt: int, retry_after_s: int | None) -> float:
    backoff = BASE_RETRY_DELAY_SECONDS * (2**attempt) * (1.0 + random.random())
    if retry_after_s is not None:
        return min(float(retry_after_s), backoff)
    return backoff


def _compute_throttled_retry_after(response: httpx.Response, retries: int) -> timedelta:
    reset_raw = response.headers.get("x-ratelimit-reset", "").strip()
    asked: int | None = None
    if reset_raw.isdigit():
        until_reset = int(reset_raw) - int(time.time())
        if 0 < until_reset <= 3600:
            asked = until_reset
    if asked is None:
        asked = _parse_retry_after_seconds(response)
    if asked is not None and asked < WHOLE_WINDOW_SECONDS:
        return timedelta(seconds=max(1, asked))
    fallback = min(THROTTLED_WAIT_MAX_SECONDS, int(BASE_RETRY_DELAY_SECONDS * (2**retries)))
    return timedelta(seconds=max(1, fallback))


def _extract_error_fields(response: httpx.Response) -> tuple[str | None, str | None, str | None]:
    try:
        payload = response.json()
    except Exception:
        return None, None, None
    if not isinstance(payload, Mapping):
        return None, None, None
    err = payload.get("error")
    if isinstance(err, Mapping):
        code = err.get("code") if isinstance(err.get("code"), str) else None
        message = err.get("message") if isinstance(err.get("message"), str) else None
        hint = err.get("hint") if isinstance(err.get("hint"), str) else None
        return code, message, hint
    code = payload.get("code") if isinstance(payload.get("code"), str) else None
    message = payload.get("message") if isinstance(payload.get("message"), str) else None
    hint = payload.get("hint") if isinstance(payload.get("hint"), str) else None
    return code, message, hint


def _raise_api_error(
    response: httpx.Response,
    *,
    retries: int = 0,
    elapsed_s: float = 0.0,
) -> None:
    code, message, hint = _extract_error_fields(response)
    status = response.status_code
    fallback = (
        f"The board answered {status} {response.reason_phrase}".strip()
        if response.reason_phrase
        else f"The board answered {status}."
    )
    resolved_message = message or fallback

    if status == 429:
        retry_after = _compute_throttled_retry_after(response, retries)
        if not message and retries > 0:
            resolved_message = (
                f"Board API rate-limited {retries + 1} tries in {max(1, round(elapsed_s))}s."
            )
        raise BoardRateLimitError(
            resolved_message,
            code=code or "rate_limited",
            status_code=429,
            hint=hint,
            origin=ErrorOrigin.BOARD,
            retry_after=retry_after,
            response=response,
        )

    if status == 404:
        raise BoardNotFoundError(
            resolved_message,
            code=code or "not_found",
            status_code=404,
            hint=hint,
            origin=ErrorOrigin.BOARD,
            response=response,
        )

    if status == 403:
        raise BoardPermissionError(
            resolved_message,
            code=code or "forbidden",
            status_code=403,
            hint=hint,
            origin=ErrorOrigin.BOARD,
            response=response,
        )

    if status == 409:
        error_cls: type[BoardConflictError]
        if code == "revision_conflict":
            error_cls = BoardRevisionConflictError
        elif code == "label_conflict":
            error_cls = BoardLabelConflictError
        elif code == "newer_message":
            error_cls = BoardNewerMessageError
        else:
            error_cls = BoardConflictError
        raise error_cls(
            resolved_message,
            code=code or "conflict",
            status_code=409,
            hint=hint,
            origin=ErrorOrigin.BOARD,
            response=response,
        )

    raise BoardApiError(
        resolved_message,
        code=code or ("redirected" if 300 <= status < 400 else "http_error"),
        status_code=status,
        hint=hint,
        origin=ErrorOrigin.BOARD,
        response=response,
    )


# ---------------------------------------------------------------------------
# Request context & protocol abstraction
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class RequestContext:
    """Per-call handle configuration passed from ``BoardService`` to ``BoardApiClient``."""

    board_name: str | None
    team: str | None
    ensure: bool
    profile: Profile | None
    can: Can | None


class _BoardProtocol(abc.ABC):
    """Internal wire protocol adapter interface."""

    @abc.abstractmethod
    async def discovery(self, ctx: RequestContext) -> Discovery:
        raise NotImplementedError

    @abc.abstractmethod
    async def me(self, ctx: RequestContext) -> Me:
        raise NotImplementedError


# ---------------------------------------------------------------------------
# Shared retry + OIDC exchange + GitHub Actions OIDC helper
# ---------------------------------------------------------------------------


class _RequestRunner:
    """Shared transport runner with 429 retry, redirect protection, and OIDC helpers."""

    def __init__(
        self,
        *,
        transport: BaseTransport,
        timeout: timedelta,
        sleep: SleepFn,
        sync_mode: bool,
    ) -> None:
        self._transport = transport
        self._timeout = timeout
        self._sleep = sleep
        self.sync_mode = sync_mode
        self._github_cache: dict[tuple[str, str, str], tuple[str, float]] = {}
        self._oidc_exchange_cache: dict[str, tuple[str, float]] = {}

    async def send(
        self,
        method: str,
        url: str,
        *,
        token: str | None = None,
        params: Mapping[str, str] | None = None,
        headers: Mapping[str, str] | None = None,
        body: RequestBody = None,
    ) -> httpx.Response:
        merged_headers: dict[str, str] = {"user-agent": USER_AGENT}
        if headers:
            merged_headers.update(headers)

        started = time.monotonic()
        for attempt in range(MAX_RATE_LIMIT_RETRIES + 1):
            response = await self._transport.send(
                method,
                url,
                token=token,
                params=params,
                headers=merged_headers,
                body=body,
                timeout=self._timeout,
                follow_redirects=False,
                read_response=ReadResponsePolicy.ALWAYS,
            )
            if response.status_code == 429 and attempt < MAX_RATE_LIMIT_RETRIES:
                delay = _compute_retry_delay(attempt, _parse_retry_after_seconds(response))
                await self._sleep(delay)
                continue
            if response.is_success:
                return response
            _raise_api_error(
                response,
                retries=attempt,
                elapsed_s=time.monotonic() - started,
            )
        raise AssertionError("unreachable")  # pragma: no cover

    async def send_raw(
        self,
        method: str,
        url: str,
        *,
        token: str | None = None,
        params: Mapping[str, str] | None = None,
        headers: Mapping[str, str] | None = None,
        body: RequestBody = None,
    ) -> httpx.Response:
        merged_headers: dict[str, str] = {"user-agent": USER_AGENT}
        if headers:
            merged_headers.update(headers)
        return await self._transport.send(
            method,
            url,
            token=token,
            params=params,
            headers=merged_headers,
            body=body,
            timeout=self._timeout,
            follow_redirects=False,
            read_response=ReadResponsePolicy.ALWAYS,
        )

    async def fetch_github_actions_oidc(self, audience: str) -> str:
        req_url = os.environ.get("ACTIONS_ID_TOKEN_REQUEST_URL", "")
        req_token = os.environ.get("ACTIONS_ID_TOKEN_REQUEST_TOKEN", "")
        cache_key = (req_url, req_token, audience)
        cached = self._github_cache.get(cache_key)
        now_ms = time.time() * 1000.0
        if cached is not None and now_ms < cached[1]:
            return cached[0]

        parsed = urlsplit(req_url)
        separator = "&" if parsed.query else "?"
        target = f"{req_url}{separator}{urlencode({'audience': audience})}"
        response = await self.send_raw(
            "GET",
            target,
            headers={"authorization": f"bearer {req_token}"},
        )
        if not response.is_success:
            raise BoardCredentialsError(
                f"GitHub Actions refused an OIDC token ({response.status_code}).",
                code="no_token",
                status_code=response.status_code,
                hint="Give the job `permissions: id-token: write`.",
            )
        parsed_body = _parse_wire(response, _GithubOidcResponseWire)
        expires_at_ms = token_expiry_ms(parsed_body.value) - 60_000.0
        self._github_cache[cache_key] = (parsed_body.value, expires_at_ms)
        return parsed_body.value

    async def exchange_vercel_oidc_token(self, source_token: str, audience: str) -> str:
        """Exchange a Vercel OIDC token for one scoped to ``audience``."""
        cache_key = hashlib.sha256(f"{source_token}\n{audience}".encode()).hexdigest()
        cached = self._oidc_exchange_cache.get(cache_key)
        now_ms = time.time() * 1000.0
        if cached is not None and now_ms < cached[1]:
            return cached[0]

        response = await self.send_raw(
            "POST",
            VERCEL_OIDC_EXCHANGE_URL,
            headers={"accept": "application/json"},
            body=JSONBody({"token": source_token, "aud": audience}),
        )
        if not response.is_success:
            raise BoardCredentialsError(
                f"Failed to exchange Vercel OIDC token for audience {audience!r} "
                f"({response.status_code}).",
                code="no_token",
                status_code=response.status_code,
                hint="Enable OIDC on the project or pass BOARD_TOKEN.",
                response=response,
            )
        exchanged = _parse_wire(response, _OidcExchangeResponseWire)
        if exchanged.expiry is not None:
            expires_at_ms = float(exchanged.expiry) * 1000.0 - 60_000.0
            if expires_at_ms > now_ms:
                self._oidc_exchange_cache[cache_key] = (exchanged.token, expires_at_ms)
        return exchanged.token


def _is_deployment_protection_wall(response: httpx.Response) -> bool:
    """Detect Vercel Deployment Protection rejections (matching ``client/node.ts:664-686``).

    An ordinary JSON board 401 has ``content-type: application/json`` and no
    ``x-vercel-error`` header, so it returns ``False``.
    """
    status = response.status_code
    if 300 <= status < 400:
        location = response.headers.get("location")
        if location and urlsplit(location).path.startswith("/sso-api"):
            return True
    content_type = response.headers.get("content-type", "").lower()
    is_html = content_type.startswith("text/html") and "content-disposition" not in response.headers
    if is_html and ((200 <= status < 300) or status in (401, 403)):
        return True
    return status == 401 and "x-vercel-error" in response.headers


# ---------------------------------------------------------------------------
# HTTP / BOARD_URL protocol adapter
# ---------------------------------------------------------------------------


class _HttpBoardProtocol(_BoardProtocol):
    """Protocol adapter for ``BOARD_URL`` / self-hosted HTTP Board endpoints."""

    def __init__(
        self,
        *,
        base_url: str,
        api_base_url: str,
        runner: _RequestRunner,
        credentials_factory: BoardCredentialsFactory | None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._api_base_url = api_base_url.rstrip("/")
        self._runner = runner
        self._credentials_factory = credentials_factory
        self._discovered: Discovery | None = None
        self._bypass_rejected: bool = False

    def _identity_headers(self, ctx: RequestContext) -> dict[str, str]:
        headers: dict[str, str] = {}
        if ctx.profile is not None:
            headers["x-board-profile"] = format_http_profile_header(ctx.profile)
        if ctx.can is not None:
            headers["x-board-can"] = ctx.can.to_header()
        return headers

    async def _trusted_sources_headers(self, *, auth_token: str | None) -> dict[str, str]:
        """Return ``x-vercel-trusted-oidc-idp-token`` when running on Vercel (``VERCEL=1``)."""
        if os.environ.get("VERCEL") != "1":
            return {}
        idp_token = auth_token
        if idp_token is None:
            if self._credentials_factory is not None:
                idp_token = normalize_credentials(await self._credentials_factory()).token
            else:
                board_token = os.environ.get("BOARD_TOKEN", "").strip()
                if board_token:
                    idp_token = board_token
                else:
                    raw_token = await resolve_deployment_oidc_token(
                        sync_mode=self._runner.sync_mode
                    )
                    idp_token = await self._runner.exchange_vercel_oidc_token(
                        raw_token, self._base_url
                    )
        return {"x-vercel-trusted-oidc-idp-token": idp_token}

    def _protected_error(
        self, response: httpx.Response, *, used_bypass: bool
    ) -> BoardCredentialsError:
        if used_bypass:
            return BoardCredentialsError(
                f"{self._base_url} is behind Vercel Deployment Protection, "
                "and it rejected VERCEL_AUTOMATION_BYPASS_SECRET.",
                code="protected",
                status_code=response.status_code,
                hint="Check VERCEL_AUTOMATION_BYPASS_SECRET in Project Settings -> "
                "Deployment Protection -> Protection Bypass for Automation.",
                response=response,
            )
        return BoardCredentialsError(
            f"{self._base_url} is behind Vercel Deployment Protection.",
            code="protected",
            status_code=response.status_code,
            hint="Add this project to Deployment Protection Trusted Sources "
            "or set VERCEL_AUTOMATION_BYPASS_SECRET.",
            response=response,
        )

    async def _send_checked(
        self,
        method: str,
        url: str,
        *,
        token: str | None,
        headers: Mapping[str, str],
        used_bypass: bool,
    ) -> httpx.Response:
        try:
            response = await self._runner.send(
                method,
                url,
                token=token,
                headers=headers,
            )
        except BoardApiError as exc:
            if exc.response is not None and _is_deployment_protection_wall(exc.response):
                raise self._protected_error(exc.response, used_bypass=used_bypass) from exc
            raise
        if _is_deployment_protection_wall(response):
            raise self._protected_error(response, used_bypass=used_bypass)
        return response

    async def _send_guarded(
        self,
        method: str,
        url: str,
        *,
        token: str | None,
        extra_headers: Mapping[str, str] | None = None,
    ) -> httpx.Response:
        """Send an HTTP Board request with Deployment Protection gateway handling.

        - Outside Vercel (``VERCEL != "1"``), an explicitly configured
          ``VERCEL_AUTOMATION_BYPASS_SECRET`` is sent to the configured
          ``BOARD_URL``; a Deployment Protection rejection raises
          ``BoardCredentialsError(code="protected")`` without retrying.
        - On Vercel (``VERCEL == "1"``), ``VERCEL_AUTOMATION_BYPASS_SECRET`` is
          sent only to the deployment's own hosts; if rejected by Deployment
          Protection, retry once without bypass using Trusted Sources
          (``x-vercel-trusted-oidc-idp-token``).
        - Any final Deployment Protection wall (including a 200 HTML login page
          or 401 with ``x-vercel-error``) raises
          ``BoardCredentialsError(code="protected")``.
        - Ordinary JSON board 401s are never treated as Deployment Protection.
        """
        on_vercel = os.environ.get("VERCEL") == "1"
        bypass = os.environ.get("VERCEL_AUTOMATION_BYPASS_SECRET")
        if (
            bypass
            and not self._bypass_rejected
            and (not on_vercel or is_own_deployment_host(self._base_url))
        ):
            headers = {"x-vercel-protection-bypass": bypass}
            if extra_headers:
                headers.update(extra_headers)
            try:
                return await self._send_checked(
                    method,
                    url,
                    token=token,
                    headers=headers,
                    used_bypass=True,
                )
            except BoardCredentialsError as exc:
                if exc.code != "protected" or not on_vercel:
                    raise
                self._bypass_rejected = True

            fallback_headers = await self._trusted_sources_headers(auth_token=token)
            if extra_headers:
                fallback_headers.update(extra_headers)
            return await self._send_checked(
                method,
                url,
                token=token,
                headers=fallback_headers,
                used_bypass=False,
            )

        gateway_headers = await self._trusted_sources_headers(auth_token=token)
        if extra_headers:
            gateway_headers.update(extra_headers)
        return await self._send_checked(
            method,
            url,
            token=token,
            headers=gateway_headers,
            used_bypass=False,
        )

    async def _discover_for_auth(self) -> Discovery:
        if self._discovered is not None:
            return self._discovered
        disc = await self._fetch_discovery()
        if disc.audience.rstrip("/") != self._base_url:
            raise BoardCredentialsError(
                f"The board at {self._base_url} claims audience {disc.audience!r}, "
                "which does not match its URL.",
                code="wrong_audience",
                hint=f"Use {disc.audience}.",
            )
        self._discovered = disc
        return disc

    async def _find_vercel_host_project(
        self, host: str, login_token: str
    ) -> tuple[str, str] | None:
        async def lookup(
            kind: Literal["deployment", "alias"], team: str | None
        ) -> tuple[str, str] | None:
            path = (
                f"/v13/deployments/{quote(host, safe='')}"
                if kind == "deployment"
                else f"/now/aliases/{quote(host, safe='')}"
            )
            params = {"teamId": team} if team else None
            resp = await self._runner.send_raw(
                "GET",
                f"{self._api_base_url}{path}",
                token=login_token,
                params=params,
            )
            if not resp.is_success:
                return None
            try:
                found = _parse_wire(resp, _VercelHostLookupWire)
            except BoardResponseError:
                return None
            owner = found.owner_id or (found.team.id if found.team else None) or team
            if found.project_id and owner:
                return found.project_id, owner
            return None

        kinds: tuple[Literal["deployment", "alias"], ...] = ("deployment", "alias")
        for kind in kinds:
            own = await lookup(kind, None)
            if own is not None:
                return own

        teams_resp = await self._runner.send_raw(
            "GET",
            f"{self._api_base_url}/v2/teams",
            token=login_token,
            params={"limit": "100"},
        )
        if not teams_resp.is_success:
            return None
        try:
            teams = _parse_wire(teams_resp, _VercelTeamsListWire).teams
        except BoardResponseError:
            return None
        team_ids = [t.id for t in teams if not t.limited]
        for kind in kinds:
            for team_id in team_ids:
                found = await lookup(kind, team_id)
                if found is not None:
                    return found
        return None

    async def _resolve_board_project(
        self, disc: Discovery, login_token: str
    ) -> tuple[str, str | None]:
        if is_loopback_url(self._base_url):
            project_id = disc.project_id
            team_id = disc.team_id
            if (
                not project_id
                or _PROJECT_ID_RE.fullmatch(project_id) is None
                or (team_id is not None and _OWNER_ID_RE.fullmatch(team_id) is None)
            ):
                raise BoardCredentialsError(
                    f"The board at {self._base_url} names a project or team that isn't a "
                    "valid Vercel ID, so no token is minted for it.",
                    code="wrong_project",
                )
            return project_id, team_id if team_id and team_id.startswith("team_") else None

        host = urlsplit(self._base_url).netloc
        found = await self._find_vercel_host_project(host, login_token)
        if found is None:
            raise BoardCredentialsError(
                f"Your Vercel login can't see the project {host} belongs to.",
                code="no_access",
                hint="Log in as someone on the project's team (`vercel login`), "
                "or ask for access to it.",
            )
        found_project_id, owner_id = found
        if found_project_id != disc.project_id:
            raise BoardCredentialsError(
                f"The board at {self._base_url} claims project {disc.project_id!r}, "
                f"but {host} is a deployment of {found_project_id}, so no token is minted for it.",
                code="wrong_project",
            )
        return found_project_id, owner_id if owner_id.startswith("team_") else None

    async def _resolve_person_token(self, disc: Discovery, login_token: str) -> str:
        cached = read_cached_token(disc.audience, login_token)
        if cached is not None:
            return cached

        project_id, team_id = await self._resolve_board_project(disc, login_token)
        params: dict[str, str] = {"source": "board"}
        if team_id:
            params["teamId"] = team_id
        resp = await self._runner.send_raw(
            "POST",
            f"{self._api_base_url}/v1/projects/{quote(project_id, safe='')}/token",
            token=login_token,
            params=params,
        )
        if resp.status_code in (403, 404):
            raise BoardCredentialsError(
                "Your Vercel account can't access this board's project.",
                code="no_access",
                status_code=resp.status_code,
                hint="Ask someone on the project's team for access to it.",
                response=resp,
            )
        if not resp.is_success:
            raise BoardCredentialsError(
                f"Failed to mint development token for {project_id} ({resp.status_code}).",
                code="no_token",
                status_code=resp.status_code,
                response=resp,
            )
        dev_token = _parse_wire(resp, _VercelProjectTokenResponseWire).token
        exchanged = await self._runner.exchange_vercel_oidc_token(dev_token, disc.audience)
        write_cached_token(disc.audience, login_token, exchanged, token_expiry_ms(exchanged))
        return exchanged

    async def _resolve_token(self) -> tuple[str | None, bool]:
        """Return ``(token, is_bare)`` for an authenticated HTTP Board request.

        Always selects the active identity before consulting any disk cache.
        """
        # 1. Explicit credentials_factory
        if self._credentials_factory is not None:
            creds = normalize_credentials(await self._credentials_factory())
            return creds.token, False

        # 2. BOARD_TOKEN
        board_token = os.environ.get("BOARD_TOKEN", "").strip()
        if board_token:
            return board_token, False

        # 3. Loopback local token file (`vb serve`)
        local_tok = read_local_token(self._base_url)
        if local_tok is not None:
            return local_tok, False

        disc = await self._discover_for_auth()

        # 4. GitHub Actions OIDC (ahead of deployment OIDC in HTTP mode; job-scoped in memory)
        if is_github_actions_oidc_available():
            token = await self._runner.fetch_github_actions_oidc(disc.audience)
            return token, False

        # 5. Deployment OIDC (ONLY when VERCEL=1 in HTTP mode; exchanged for discovery.audience)
        if os.environ.get("VERCEL") == "1":
            raw_token = await resolve_deployment_oidc_token(sync_mode=self._runner.sync_mode)
            exchanged = await self._runner.exchange_vercel_oidc_token(raw_token, disc.audience)
            return exchanged, False

        # 6. Board with no Vercel project -> bare request
        if disc.project_id is None:
            return None, True

        # 7. Local Vercel login (VERCEL_AUTH_TOKEN or `vercel login` CLI auth file)
        login_token = os.environ.get("VERCEL_AUTH_TOKEN", "").strip() or get_vercel_cli_auth_token()
        if login_token:
            return await self._resolve_person_token(disc, login_token), False

        raise BoardCredentialsError(
            f"No credentials available for Board at {self._base_url}.",
            code="no_token",
            hint="Run `vercel login`, set BOARD_TOKEN, "
            "or pass BoardServiceOptions(credentials_factory=...).",
        )

    async def _fetch_discovery(self) -> Discovery:
        response = await self._send_guarded(
            "GET",
            f"{self._base_url}/",
            token=None,
        )
        return _parse_wire(response, _HttpDiscoveryWire).to_domain()

    async def discovery(self, ctx: RequestContext) -> Discovery:
        del ctx
        return await self._fetch_discovery()

    async def me(self, ctx: RequestContext) -> Me:
        token, is_bare = await self._resolve_token()
        try:
            response = await self._send_guarded(
                "GET",
                f"{self._base_url}/me",
                token=token,
                extra_headers=self._identity_headers(ctx),
            )
        except BoardApiError as exc:
            if is_bare and exc.status_code == 401:
                raise BoardCredentialsError(
                    f"The board at {self._base_url} has no Vercel project to mint a token for "
                    "and rejected the unauthenticated request.",
                    code="no_project",
                    status_code=401,
                    hint="Pass a token it accepts in BOARD_TOKEN.",
                    response=exc.response,
                ) from exc
            raise
        return _parse_wire(response, _HttpMeWire).to_domain()


# ---------------------------------------------------------------------------
# Vercel Hosted (/v1/messageboard) protocol adapter
# ---------------------------------------------------------------------------


class _VercelBoardProtocol(_BoardProtocol):
    """Protocol adapter for Vercel's hosted ``/v1/messageboard`` API."""

    def __init__(
        self,
        *,
        api_base_url: str,
        default_team: str | None,
        runner: _RequestRunner,
        credentials_factory: BoardCredentialsFactory | None,
    ) -> None:
        self._api_base_url = api_base_url.rstrip("/")
        self._default_team = default_team
        self._runner = runner
        self._credentials_factory = credentials_factory
        self._board_id_cache: dict[tuple[str, str, str | None, bool], str] = {}

    async def _resolve_auth_and_team(
        self,
        explicit_team: str | None,
    ) -> tuple[str, str | None]:
        """Resolve ``(bearer_token, team)`` following ``backend-vercel/index.ts``.

        Precedence (explicitly NEVER reads ``VERCEL_TOKEN``):
        1. ``credentials_factory``
        2. Deployment OIDC (``VERCEL=1`` or ``VERCEL_OIDC_TOKEN``)
        3. ``BOARD_TOKEN``
        4. ``VERCEL_AUTH_TOKEN``
        5. GitHub Actions OIDC (``ACTIONS_ID_TOKEN_REQUEST_URL`` & ``_TOKEN``)
        6. Local ``vercel login`` CLI auth file
        """
        team = explicit_team or self._default_team or os.environ.get("VERCEL_ORG_ID") or None

        # 1. Explicit credentials_factory
        if self._credentials_factory is not None:
            creds = normalize_credentials(await self._credentials_factory())
            if team is None:
                team = find_vercel_project_org_id()
            return creds.token, team

        # 2. Deployment OIDC (VERCEL=1 or VERCEL_OIDC_TOKEN)
        if is_deployment_oidc_available():
            token = await resolve_deployment_oidc_token(sync_mode=self._runner.sync_mode)
            if team is None:
                team = oidc_owner_id(token) or find_vercel_project_org_id()
            return token, team

        # 3. BOARD_TOKEN
        board_token = os.environ.get("BOARD_TOKEN", "").strip()
        if board_token:
            if team is None:
                team = find_vercel_project_org_id()
            return board_token, team

        # 4. VERCEL_AUTH_TOKEN (ahead of GitHub Actions OIDC, matching backend-vercel/index.ts)
        auth_token = os.environ.get("VERCEL_AUTH_TOKEN", "").strip()
        if auth_token:
            if team is None:
                team = find_vercel_project_org_id()
            return auth_token, team

        # 5. GitHub Actions OIDC (allows linked .vercel/project.json orgId as well)
        if is_github_actions_oidc_available():
            if not github_token_allowed_origin(self._api_base_url):
                raise BoardCredentialsError(
                    f"A GitHub Actions token for Vercel's board goes only to "
                    f"https://api.vercel.com, not {self._api_base_url}.",
                    code="wrong_audience",
                )
            if team is None:
                team = find_vercel_project_org_id()
            if team is None:
                raise BoardCredentialsError(
                    "A GitHub Actions token names no Vercel team, "
                    "so there's no team's board to use.",
                    code="missing_team",
                    hint=(
                        "Pass board(team=...), BoardServiceOptions(team=...), or set VERCEL_ORG_ID."
                    ),
                )
            token = await self._runner.fetch_github_actions_oidc(GITHUB_AUDIENCE)
            return token, team

        # 6. Local `vercel login` CLI auth file
        cli_token = get_vercel_cli_auth_token()
        if cli_token is not None:
            if team is None:
                team = find_vercel_project_org_id()
            return cli_token, team

        raise BoardCredentialsError(
            "You're not logged in to Vercel.",
            code="not_logged_in",
            hint=(
                "Run `vercel login`, set BOARD_TOKEN, "
                "or pass BoardServiceOptions(credentials_factory=...)."
            ),
        )

    def _team_params(
        self,
        team: str | None,
        extra: Mapping[str, str] | None = None,
    ) -> dict[str, str]:
        params: dict[str, str] = {}
        if team:
            if team.startswith("team_"):
                params["teamId"] = team
            else:
                params["slug"] = team
        if extra:
            params.update(extra)
        return params

    def _hosted_headers(
        self,
        ctx: RequestContext | None = None,
        *,
        include_identity: bool = True,
    ) -> dict[str, str]:
        headers: dict[str, str] = {}
        if include_identity and ctx is not None:
            headers.update(format_hosted_profile_headers(ctx.profile))
            if ctx.can is not None:
                headers["x-board-can"] = ctx.can.to_header()
        return headers

    async def _api_request(
        self,
        method: str,
        path: str,
        *,
        token: str,
        team: str | None,
        ctx: RequestContext | None = None,
        include_identity: bool = True,
        query: Mapping[str, str] | None = None,
        body: RequestBody = None,
        version: Literal[1, 2] = 1,
    ) -> httpx.Response:
        url = f"{self._api_base_url}/v{version}/messageboard{path}"
        return await self._runner.send(
            method,
            url,
            token=token,
            params=self._team_params(team, query),
            headers=self._hosted_headers(ctx, include_identity=include_identity),
            body=body,
        )

    def _board_cache_key(
        self,
        *,
        team: str | None,
        token: str,
        board_name: str | None,
        ensure: bool,
    ) -> tuple[str, str, str | None, bool]:
        identity = (
            f"team:{team}"
            if team is not None
            else f"tok:{hashlib.sha256(token.encode('utf-8')).hexdigest()[:16]}"
        )
        return (self._api_base_url, identity, board_name, ensure)

    async def _resolve_board_id(
        self,
        ctx: RequestContext,
        *,
        token: str,
        team: str | None,
    ) -> str:
        cache_key = self._board_cache_key(
            team=team,
            token=token,
            board_name=ctx.board_name,
            ensure=ctx.ensure,
        )
        cached = self._board_id_cache.get(cache_key)
        if cached is not None:
            return cached

        if ctx.board_name is None:
            resp = await self._api_request(
                "GET",
                "/boards",
                token=token,
                team=team,
                ctx=ctx,
            )
            listed = _parse_wire(resp, _ApiBoardsListWire)
            if listed.default_board_id:
                self._board_id_cache[cache_key] = listed.default_board_id
                return listed.default_board_id
            me_resp = await self._api_request("GET", "/me", token=token, team=team, ctx=ctx)
            me_wire = _parse_wire(me_resp, _ApiMeWire)
            if me_wire.actor.principal.type in ("project", "github", "oidc"):
                raise BoardPermissionError(
                    "this caller has no access to the team's board.",
                    code="board_not_granted",
                    status_code=403,
                    origin=ErrorOrigin.HANDLE,
                )
            made_resp = await self._api_request(
                "POST",
                "/boards/default",
                token=token,
                team=team,
                ctx=ctx,
                body=JSONBody({"operationId": _uuid7()}),
            )
            board_id = _parse_wire(made_resp, _ApiBoardEnvelopeWire).board.id
            self._board_id_cache[cache_key] = board_id
            return board_id

        # Named board resolution
        resp = await self._api_request(
            "GET",
            "/boards",
            token=token,
            team=team,
            ctx=ctx,
            query={"name": ctx.board_name},
        )
        listed = _parse_wire(resp, _ApiBoardsListWire)
        if listed.boards:
            board_id = listed.boards[0].id
            self._board_id_cache[cache_key] = board_id
            return board_id

        if not ctx.ensure:
            raise BoardNotFoundError(
                f"no board named {ctx.board_name} on this team, or none you can see.",
                code="no_board",
                status_code=404,
                hint="Check the name, or omit ensure=False to create it.",
                origin=ErrorOrigin.HANDLE,
            )

        try:
            made_resp = await self._api_request(
                "POST",
                "/boards/ensure",
                token=token,
                team=team,
                ctx=ctx,
                body=JSONBody({"name": ctx.board_name, "operationId": _uuid7()}),
            )
        except BoardConflictError as exc:
            if exc.code == "board_name_taken":
                raise BoardConflictError(
                    f"the board name {ctx.board_name} is taken.",
                    code="board_name_taken",
                    status_code=409,
                    hint="Pick another name, or ask whoever can see it for access.",
                    origin=ErrorOrigin.HANDLE,
                    response=exc.response,
                ) from exc
            raise
        except BoardPermissionError as exc:
            if exc.code == "board_not_granted":
                raise BoardPermissionError(
                    f"this caller has no access to board {ctx.board_name}.",
                    code="board_not_granted",
                    status_code=403,
                    hint="Someone who manages the board can grant access.",
                    origin=ErrorOrigin.HANDLE,
                    response=exc.response,
                ) from exc
            raise

        board_id = _parse_wire(made_resp, _ApiBoardEnvelopeWire).board.id
        self._board_id_cache[cache_key] = board_id
        return board_id

    async def me(self, ctx: RequestContext) -> Me:
        token, team = await self._resolve_auth_and_team(ctx.team)
        board_id = await self._resolve_board_id(ctx, token=token, team=team)
        response = await self._api_request(
            "GET",
            f"/boards/{quote(board_id, safe='')}/me",
            token=token,
            team=team,
            ctx=ctx,
        )
        return _parse_wire(response, _ApiMeWire).to_domain()

    async def discovery(self, ctx: RequestContext) -> Discovery:
        token, team = await self._resolve_auth_and_team(ctx.team)
        board_id = await self._resolve_board_id(ctx, token=token, team=team)
        board_path = f"/boards/{quote(board_id, safe='')}"

        board_name: str | None = None
        can_read = ctx.can is None or ctx.can.anywhere("read")
        if can_read:
            try:
                info_resp = await self._api_request(
                    "GET",
                    board_path,
                    token=token,
                    team=team,
                    ctx=ctx,
                )
                board_name = _parse_wire(info_resp, _ApiBoardEnvelopeWire).board.name
            except BoardPermissionError:
                board_name = None

        resolved_team_id: str | None = team if team and team.startswith("team_") else None
        if resolved_team_id is None:
            me_resp = await self._api_request(
                "GET",
                "/me",
                token=token,
                team=team,
                include_identity=False,
            )
            resolved_team_id = _parse_wire(me_resp, _ApiMeWire).team_id

        query_suffix = f"?teamId={quote(resolved_team_id, safe='')}" if resolved_team_id else ""
        return Discovery(
            version=VERSION,
            url=f"{self._api_base_url}/v1/messageboard{board_path}{query_suffix}",
            audience=self._api_base_url,
            team_id=resolved_team_id,
            project_id=None,
            name=board_name,
            sync=(),
            wake_by_path=True,
        )


# ---------------------------------------------------------------------------
# Unified BoardApiClient facade
# ---------------------------------------------------------------------------


class BoardApiClient:
    """Wire-level Board API client delegating to the configured protocol adapter."""

    def __init__(
        self,
        *,
        url: str | None,
        api_base_url: str,
        team: str | None,
        credentials_factory: BoardCredentialsFactory | None,
        transport: BaseTransport,
        timeout: timedelta,
        sleep: SleepFn,
        sync_mode: bool,
    ) -> None:
        runner = _RequestRunner(
            transport=transport,
            timeout=timeout,
            sleep=sleep,
            sync_mode=sync_mode,
        )
        self._protocol: _BoardProtocol
        if url is not None:
            self._protocol = _HttpBoardProtocol(
                base_url=url,
                api_base_url=api_base_url,
                runner=runner,
                credentials_factory=credentials_factory,
            )
        else:
            self._protocol = _VercelBoardProtocol(
                api_base_url=api_base_url,
                default_team=team,
                runner=runner,
                credentials_factory=credentials_factory,
            )

    async def discovery(self, ctx: RequestContext) -> Discovery:
        """Fetch discovery metadata for the target Board."""
        return await self._protocol.discovery(ctx)

    async def me(self, ctx: RequestContext) -> Me:
        """Fetch authenticated caller identity and effective channel permissions."""
        return await self._protocol.me(ctx)


__all__ = [
    "BASE_RETRY_DELAY_SECONDS",
    "MAX_RATE_LIMIT_RETRIES",
    "USER_AGENT",
    "VERCEL_OIDC_EXCHANGE_URL",
    "VERSION",
    "BoardApiClient",
    "RequestContext",
    "format_hosted_profile_headers",
    "format_http_profile_header",
]

"""Async-only orchestration service for Vercel Board.

All domain validation, profile/can resolution, and API client delegation live
here so the async and sync runtimes share one implementation.
"""

from __future__ import annotations

import os
import re
import unicodedata
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

from vercel._internal.core.sentinel import UNSET, UnsetType
from vercel.board._internal.access import Can, CanInput
from vercel.board._internal.api_client import BoardApiClient, RequestContext
from vercel.board._internal.errors import BoardValidationError
from vercel.board._internal.models import Discovery, Me, Profile
from vercel.board._internal.options import (
    BoardCredentialsFactory,
    BoardServiceOptions,
    SyncBoardServiceOptions,
    adapt_sync_credentials_factory,
)

if TYPE_CHECKING:
    from vercel._internal.core.session import SdkSession, SyncSdkSession

_BOARD_NAME_RE: Final[re.Pattern[str]] = re.compile(r"\A[^/]{1,64}\Z")
_TEAM_RE: Final[re.Pattern[str]] = re.compile(r"\A[^\s/]+\Z")


def validate_board_name(name: str) -> str:
    """Trim, NFC-normalize, and validate a Board name (1..64 chars, no ``/``)."""
    if not isinstance(name, str):
        raise BoardValidationError(
            f"board name must be a string, got {type(name).__name__}",
            code="invalid_board_name",
        )
    normalized = unicodedata.normalize("NFC", name.strip())
    if _BOARD_NAME_RE.fullmatch(normalized) is None:
        raise BoardValidationError(
            f"{name!r} is not a valid board name: expected 1 to 64 characters with no '/'",
            code="invalid_board_name",
        )
    return normalized


def validate_team(team: str) -> str:
    """Validate a Vercel team ID (``team_...``) or slug."""
    if not isinstance(team, str) or _TEAM_RE.fullmatch(team) is None:
        raise BoardValidationError(
            f"{team!r} is not a valid team: pass its ID (team_...) or slug",
            code="invalid_team",
        )
    return team


def normalize_profile_update(
    current: Profile | None | UnsetType,
    profile: str | Profile | None | UnsetType,
    avatar: str | None,
) -> Profile | None | UnsetType:
    """Compute the next handle profile state for ``Board.as_profile()``."""
    if profile is UNSET:
        if avatar is not None:
            if isinstance(current, Profile):
                return Profile(name=current.name, avatar=avatar)
            raise BoardValidationError(
                "avatar cannot be set without a profile name",
                code="invalid_profile",
            )
        return current

    if profile is None:
        if avatar is not None:
            raise BoardValidationError(
                "avatar cannot be set when profile is None",
                code="invalid_profile",
            )
        return None

    if isinstance(profile, Profile):
        if avatar is not None:
            if profile.avatar is not None and profile.avatar != avatar:
                raise BoardValidationError(
                    "conflicting avatar passed alongside Profile(avatar=...)",
                    code="invalid_profile",
                )
            return Profile(name=profile.name, avatar=avatar)
        return profile

    if isinstance(profile, str):
        return Profile(name=profile, avatar=avatar)

    raise BoardValidationError(
        f"profile must be a str, Profile, None, or UNSET, got {type(profile).__name__}",
        code="invalid_profile",
    )


def narrow_handle_can(
    current: Can | None,
    can: Can | CanInput | None,
) -> Can | None:
    """Monotonically narrow ``current`` with ``can`` (never widens)."""
    if can is None:
        return current
    scope = Can.from_input(can)
    if current is None:
        return scope
    return current.narrow(scope)


def resolve_effective_profile(profile: Profile | None | UnsetType) -> Profile | None:
    """Resolve ``UNSET`` from ``BOARD_PROFILE`` / ``BOARD_AVATAR`` env vars."""
    if not isinstance(profile, UnsetType):
        return profile
    env_name = os.environ.get("BOARD_PROFILE", "").strip()
    if not env_name:
        return None
    env_avatar = os.environ.get("BOARD_AVATAR", "").strip() or None
    return Profile(name=env_name, avatar=env_avatar)


@dataclass(frozen=True, slots=True)
class BoardTarget:
    """Validated immutable configuration carried by a ``Board`` / ``SyncBoard`` handle."""

    name: str | None = None
    team: str | None = None
    ensure: bool = True
    profile: Profile | None | UnsetType = UNSET
    can: Can | None = None

    def __post_init__(self) -> None:
        if self.name is not None:
            object.__setattr__(self, "name", validate_board_name(self.name))
        if self.team is not None:
            object.__setattr__(self, "team", validate_team(self.team))

    def with_profile(
        self,
        profile: str | Profile | None | UnsetType = UNSET,
        *,
        avatar: str | None = None,
        can: Can | CanInput | None = None,
    ) -> BoardTarget:
        return BoardTarget(
            name=self.name,
            team=self.team,
            ensure=self.ensure,
            profile=normalize_profile_update(self.profile, profile, avatar),
            can=narrow_handle_can(self.can, can),
        )

    def to_request_context(self) -> RequestContext:
        return RequestContext(
            board_name=self.name,
            team=self.team,
            ensure=self.ensure,
            profile=resolve_effective_profile(self.profile),
            can=self.can,
        )


class BoardService:
    """Neutral async orchestration service cached per SDK session."""

    def __init__(
        self,
        *,
        api_client: BoardApiClient,
        ensure_open: Callable[[], None],
    ) -> None:
        self._api_client = api_client
        self._ensure_open = ensure_open

    async def discovery(self, target: BoardTarget) -> Discovery:
        self._ensure_open()
        return await self._api_client.discovery(target.to_request_context())

    async def me(self, target: BoardTarget) -> Me:
        self._ensure_open()
        return await self._api_client.me(target.to_request_context())


def _create_service(
    session: SdkSession | SyncSdkSession,
    options: BoardServiceOptions | SyncBoardServiceOptions,
    credentials_factory: BoardCredentialsFactory | None,
    *,
    sync_mode: bool,
) -> BoardService:
    return BoardService(
        api_client=BoardApiClient(
            url=options.url,
            api_base_url=options.api_base_url,
            team=options.team,
            credentials_factory=credentials_factory,
            transport=session.get_transport(),
            timeout=options.timeout,
            sleep=session.sleep,
            sync_mode=sync_mode,
        ),
        ensure_open=session.check_open,
    )


def get_board_service(session: SdkSession) -> BoardService:
    """Resolve the Board service for an async session, once per session."""

    def factory() -> BoardService:
        options = session.get_service_option(BoardServiceOptions) or BoardServiceOptions()
        return _create_service(
            session,
            options,
            options.credentials_factory,
            sync_mode=False,
        )

    return session.get_or_create_service(BoardService, factory)


def get_sync_board_service(session: SyncSdkSession) -> BoardService:
    """Resolve the Board service for a sync session, once per session."""

    def factory() -> BoardService:
        options = session.get_service_option(SyncBoardServiceOptions) or SyncBoardServiceOptions()
        credentials_factory = (
            adapt_sync_credentials_factory(options.credentials_factory)
            if options.credentials_factory is not None
            else None
        )
        return _create_service(
            session,
            options,
            credentials_factory,
            sync_mode=True,
        )

    return session.get_or_create_service(BoardService, factory)


__all__ = [
    "BoardService",
    "BoardTarget",
    "get_board_service",
    "get_sync_board_service",
    "narrow_handle_can",
    "normalize_profile_update",
    "resolve_effective_profile",
    "validate_board_name",
    "validate_team",
]

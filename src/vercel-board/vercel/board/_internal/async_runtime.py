"""Async runtime handles and standalone client for Vercel Board."""

from __future__ import annotations

from types import TracebackType
from typing import Self

from vercel._internal.core.options import collect_service_options
from vercel._internal.core.sentinel import UNSET, UnsetType
from vercel._internal.core.session import HttpxClientFactory, SdkSession, get_active_session
from vercel.board._internal.access import Can, CanInput
from vercel.board._internal.models import Discovery, Me, Profile
from vercel.board._internal.options import BoardServiceOptions
from vercel.board._internal.service import (
    BoardService,
    BoardTarget,
    get_board_service,
)


class Board:
    """Immutable asynchronous handle for interacting with a Vercel Board.

    When created via :func:`board`, each call resolves the active SDK session
    so module-scope handles respect ``async with vercel.api.session(...)``
    blocks. When created via :meth:`BoardClient.board`, calls use that client's
    pinned session.
    """

    __slots__ = ("_target", "_session")

    def __init__(
        self,
        *,
        name: str | None = None,
        team: str | None = None,
        ensure: bool = True,
        profile: Profile | None | UnsetType = UNSET,
        can: Can | None = None,
        _target: BoardTarget | None = None,
        _session: SdkSession | None = None,
    ) -> None:
        self._target = (
            _target
            if _target is not None
            else BoardTarget(
                name=name,
                team=team,
                ensure=ensure,
                profile=profile,
                can=can,
            )
        )
        self._session = _session

    def _service(self) -> BoardService:
        session = self._session if self._session is not None else get_active_session()
        return get_board_service(session)

    def as_profile(
        self,
        profile: str | Profile | None | UnsetType = UNSET,
        *,
        avatar: str | None = None,
        can: Can | CanInput | None = None,
    ) -> Board:
        """Return a new handle acting as ``profile`` and/or narrowed to ``can``.

        Passing ``None`` explicitly suppresses ``BOARD_PROFILE`` from the
        environment. Passing ``can`` monotonically narrows any existing scope
        on this handle and can never widen it.
        """
        return Board(
            _target=self._target.with_profile(profile, avatar=avatar, can=can),
            _session=self._session,
        )

    async def discovery(self) -> Discovery:
        """Return discovery metadata for this Board."""
        return await self._service().discovery(self._target)

    async def me(self) -> Me:
        """Return the authenticated caller's identity and effective permissions."""
        return await self._service().me(self._target)


def board(
    *,
    name: str | None = None,
    team: str | None = None,
    ensure: bool = True,
) -> Board:
    """Create a lazy asynchronous Board handle with zero I/O.

    Args:
        name: Optional named board within the team (1..64 chars, no ``/``).
            Omit for the team's default board.
        team: Optional Vercel team ID (``team_...``) or slug for hosted mode.
        ensure: Whether to create a named board on first use if it does not yet
            exist. Pass ``False`` to fail with :class:`BoardNotFoundError`
            instead.
    """
    return Board(name=name, team=team, ensure=ensure)


class BoardClient:
    """Standalone asynchronous client owning its own SDK session.

    Construction is synchronous and performs no I/O; use ``async with`` or call
    :meth:`aclose` when finished.
    """

    def __init__(self, *, _session: SdkSession) -> None:
        self._session = _session

    @classmethod
    def create(
        cls,
        *,
        options: BoardServiceOptions | None = None,
        httpx_client_factory: HttpxClientFactory | None = None,
    ) -> Self:
        """Create a standalone async Board client."""
        session = SdkSession(
            service_options=collect_service_options([options] if options is not None else None),
            httpx_client_factory=httpx_client_factory,
        )
        return cls(_session=session)

    def board(
        self,
        *,
        name: str | None = None,
        team: str | None = None,
        ensure: bool = True,
    ) -> Board:
        """Return a Board handle pinned to this client's session."""
        return Board(name=name, team=team, ensure=ensure, _session=self._session)

    async def aclose(self) -> None:
        """Close resources owned by this client."""
        await self._session.aclose()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self.aclose()


__all__ = [
    "Board",
    "BoardClient",
    "board",
]

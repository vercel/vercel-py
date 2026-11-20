"""Sync runtime handles and standalone client for Vercel Board.

Each operation drives the shared async ``BoardService`` through
``iter_coroutine``, which is safe because ``SyncSdkSession`` uses a
non-suspending ``SyncTransport`` and ``time.sleep``.
"""

from __future__ import annotations

from types import TracebackType
from typing import Self

from vercel._internal.core.iter_coroutine import iter_coroutine
from vercel._internal.core.options import collect_service_options
from vercel._internal.core.sentinel import UNSET, UnsetType
from vercel._internal.core.session import (
    HttpxClientFactory,
    SyncSdkSession,
    get_active_sync_session,
)
from vercel.board._internal.access import Can, CanInput
from vercel.board._internal.models import Discovery, Me, Profile
from vercel.board._internal.options import SyncBoardServiceOptions
from vercel.board._internal.service import (
    BoardService,
    BoardTarget,
    get_sync_board_service,
)


class SyncBoard:
    """Immutable synchronous handle for interacting with a Vercel Board.

    When created via :func:`board`, each call resolves the active sync SDK
    session so module-scope handles respect ``with vercel.api.session(...)``
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
        _session: SyncSdkSession | None = None,
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
        session = self._session if self._session is not None else get_active_sync_session()
        return get_sync_board_service(session)

    def as_profile(
        self,
        profile: str | Profile | None | UnsetType = UNSET,
        *,
        avatar: str | None = None,
        can: Can | CanInput | None = None,
    ) -> SyncBoard:
        """Return a new handle acting as ``profile`` and/or narrowed to ``can``.

        Passing ``None`` explicitly suppresses ``BOARD_PROFILE`` from the
        environment. Passing ``can`` monotonically narrows any existing scope
        on this handle and can never widen it.
        """
        return SyncBoard(
            _target=self._target.with_profile(profile, avatar=avatar, can=can),
            _session=self._session,
        )

    def discovery(self) -> Discovery:
        """Return discovery metadata for this Board."""
        return iter_coroutine(self._service().discovery(self._target))

    def me(self) -> Me:
        """Return the authenticated caller's identity and effective permissions."""
        return iter_coroutine(self._service().me(self._target))


def sync_board(
    *,
    name: str | None = None,
    team: str | None = None,
    ensure: bool = True,
) -> SyncBoard:
    """Create a lazy synchronous Board handle with zero I/O.

    Args:
        name: Optional named board within the team (1..64 chars, no ``/``).
            Omit for the team's default board.
        team: Optional Vercel team ID (``team_...``) or slug for hosted mode.
        ensure: Whether to create a named board on first use if it does not yet
            exist. Pass ``False`` to fail with :class:`BoardNotFoundError`
            instead.
    """
    return SyncBoard(name=name, team=team, ensure=ensure)


class SyncBoardClient:
    """Standalone synchronous client owning its own SDK session.

    Construction is lazy and performs no I/O; use ``with`` or call
    :meth:`close` when finished.
    """

    def __init__(self, *, _session: SyncSdkSession) -> None:
        self._session = _session

    @classmethod
    def create(
        cls,
        *,
        options: SyncBoardServiceOptions | None = None,
        httpx_client_factory: HttpxClientFactory | None = None,
    ) -> Self:
        """Create a standalone sync Board client."""
        session = SyncSdkSession(
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
    ) -> SyncBoard:
        """Return a Board handle pinned to this client's session."""
        return SyncBoard(name=name, team=team, ensure=ensure, _session=self._session)

    def close(self) -> None:
        """Close resources owned by this client."""
        self._session.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.close()


__all__ = [
    "SyncBoard",
    "SyncBoardClient",
    "sync_board",
]

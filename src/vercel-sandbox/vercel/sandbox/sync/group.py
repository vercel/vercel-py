"""Synchronous Linux group handles."""

from typing import TYPE_CHECKING

from vercel._internal.core.iter_coroutine import iter_coroutine
from vercel.sandbox._internal.group_core import GroupContext

if TYPE_CHECKING:
    from vercel.sandbox.sync.user import SyncSandboxUser


class SyncSandboxGroup:
    """Manage membership of a Linux group and locate its shared directory.

    Obtain handles through ``create_group()`` or ``as_group()``. Members read
    and write ``shared_dir``, which ``create_group()`` provisions; a group
    selected with ``as_group()`` has that directory only if it was created
    through this API. Share files by writing them with members' commands:
    uploads through a user handle belong to that user's primary group.
    Membership changes apply to commands started afterwards.
    """

    __slots__ = ("_context",)

    def __init__(self, context: GroupContext) -> None:
        self._context = context

    @property
    def groupname(self) -> str:
        return self._context.groupname

    @property
    def shared_dir(self) -> str:
        return self._context.shared_dir

    def add_user(self, user: "str | SyncSandboxUser") -> None:
        """Add an existing account, by handle or username, to this group."""
        username = user if isinstance(user, str) else user.username
        iter_coroutine(self._context.set_membership(username, member=True))

    def remove_user(self, user: "str | SyncSandboxUser") -> None:
        """Remove an account from this group; non-members raise ``SandboxError``."""
        username = user if isinstance(user, str) else user.username
        iter_coroutine(self._context.set_membership(username, member=False))

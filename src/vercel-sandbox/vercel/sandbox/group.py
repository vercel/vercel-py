"""Asynchronous Linux group handles."""

from typing import TYPE_CHECKING

from vercel.sandbox._internal.group_core import GroupContext

if TYPE_CHECKING:
    from vercel.sandbox.user import SandboxUser


class SandboxGroup:
    """Manage membership of a Linux group and locate its shared directory.

    Obtain handles through ``create_group()`` or ``as_group()``. Members read
    and write ``shared_dir``, which ``create_group()`` provisions; a group
    selected with ``as_group()`` has that directory only if it was created
    through this API. Membership changes apply to commands started afterwards.
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

    async def add_user(self, user: "str | SandboxUser") -> None:
        """Add an existing account, by handle or username, to this group."""
        username = user if isinstance(user, str) else user.username
        await self._context.set_membership(username, member=True)

    async def remove_user(self, user: "str | SandboxUser") -> None:
        """Remove an account from this group; non-members raise ``SandboxError``."""
        username = user if isinstance(user, str) else user.username
        await self._context.set_membership(username, member=False)

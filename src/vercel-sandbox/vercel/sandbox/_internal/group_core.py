"""Shared group provisioning and membership changes."""

import re
from collections.abc import Sequence

from vercel.sandbox._internal.errors import SandboxError, SandboxGroupAlreadyExistsError
from vercel.sandbox._internal.filesystem_handle_core import FilesystemOperationBinding
from vercel.sandbox._internal.process_output import ProcessOutputRouter
from vercel.sandbox._internal.service import SandboxService
from vercel.sandbox._internal.state import CompletedProcessState
from vercel.sandbox._internal.user_core import validate_username


def validate_groupname(groupname: str) -> None:
    if not isinstance(groupname, str) or not re.fullmatch(r"[a-z_][a-z0-9_-]{0,31}", groupname):
        raise ValueError("groupname must match [a-z_][a-z0-9_-]* and be at most 32 characters")


_GROUP_ALREADY_EXISTS = "vercel-sandbox:group-already-exists"
_CREATE_GROUP = """
set -eu
if getent group "$1" >/dev/null; then
    printf '%s\\n' "$3"
    exit 9
fi
if [ -e "/shared/$1" ] || [ -L "/shared/$1" ]; then
    printf 'Shared path already exists: /shared/%s\\n' "$1" >&2
    exit 1
fi
if groupadd -- "$1"; then
    :
else
    status=$?
    # Confirm the group exists, including when another creator won the race
    # after our initial lookup, before reporting groupadd's non-unique status.
    if [ "$status" -eq 9 ] && getent group "$1" >/dev/null; then
        printf '%s\\n' "$3"
    fi
    exit "$status"
fi
mkdir -p -- /shared
mkdir -- "/shared/$1"
chown -- "$2:$1" "/shared/$1"
chmod 2770 -- "/shared/$1"
"""


async def _run(
    service: SandboxService,
    session_id: str,
    command: str,
    args: Sequence[str],
    *,
    sudo: bool,
) -> CompletedProcessState:
    return await service.run_process(
        session_id=session_id,
        command=command,
        args=args,
        cwd="/",
        sudo=sudo,
        output_router=ProcessOutputRouter(stdout=None, stderr=None, capture_output=True),
    )


async def set_group_membership(
    service: SandboxService,
    execution: FilesystemOperationBinding,
    *,
    username: str,
    groupname: str,
    member: bool,
) -> None:
    """Add or remove a supplementary group; later commands see the change."""
    validate_username(username)
    validate_groupname(groupname)

    async def change(session_id: str) -> None:
        command, args = (
            ("usermod", ["--append", "--groups", groupname, "--", username])
            if member
            else ("gpasswd", ["--delete", username, groupname])
        )
        result = await _run(service, session_id, command, args, sudo=True)
        if result.process.returncode != 0:
            action = "add Linux user {!r} to" if member else "remove Linux user {!r} from"
            raise SandboxError(
                f"Failed to {action.format(username)} group {groupname!r}: "
                f"{(result.stderr or b'').decode('utf-8')}"
            )

    await execution.execute(change)


class GroupContext:
    def __init__(
        self,
        *,
        groupname: str,
        service: SandboxService,
        execution: FilesystemOperationBinding,
    ) -> None:
        validate_groupname(groupname)
        self.groupname = groupname
        self.shared_dir = f"/shared/{groupname}"
        self.service = service
        self.execution = execution

    async def provision(self) -> None:
        async def create(session_id: str) -> None:
            # The sandbox default account, which the HTTP file API runs as, owns
            # the shared directory, matching the TypeScript SDK.
            lookup = await _run(self.service, session_id, "id", ["-u"], sudo=False)
            default_uid = (lookup.stdout or b"").decode("utf-8").strip()
            if lookup.process.returncode != 0 or not re.fullmatch(r"[0-9]+", default_uid):
                raise SandboxError(
                    f"Cannot resolve sandbox default user: {(lookup.stderr or b'').decode('utf-8')}"
                )
            result = await _run(
                self.service,
                session_id,
                "/bin/bash",
                [
                    "-c",
                    _CREATE_GROUP,
                    "create-group",
                    self.groupname,
                    default_uid,
                    _GROUP_ALREADY_EXISTS,
                ],
                sudo=True,
            )
            if result.process.returncode == 9 and result.stdout == (
                _GROUP_ALREADY_EXISTS + "\n"
            ).encode("utf-8"):
                raise SandboxGroupAlreadyExistsError(self.groupname)
            if result.process.returncode != 0:
                raise SandboxError(
                    f"Failed to create Linux group {self.groupname!r}: "
                    f"{(result.stderr or b'').decode('utf-8')}"
                )

        await self.execution.execute(create)

    async def set_membership(self, username: str, *, member: bool) -> None:
        await set_group_membership(
            self.service,
            self.execution,
            username=username,
            groupname=self.groupname,
            member=member,
        )

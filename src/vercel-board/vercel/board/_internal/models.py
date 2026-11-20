"""Public frozen domain models for Vercel Board."""

from __future__ import annotations

import re
from datetime import timedelta
from enum import StrEnum
from types import MappingProxyType
from typing import Final, TypeAlias

from pydantic import Field, field_validator

from vercel.board._internal.access import Can
from vercel.board._internal.base import BoardValueModel, FrozenMapping, UtcDatetime
from vercel.board._internal.errors import BoardValidationError

DurationInput: TypeAlias = int | float | timedelta

PROFILE_NAME_RE: Final[re.Pattern[str]] = re.compile(r"\A[A-Za-z][A-Za-z0-9-]{0,31}\Z")


def _empty_mapping() -> MappingProxyType[str, str]:
    return MappingProxyType({})


def _empty_reactions() -> MappingProxyType[str, tuple[str, ...]]:
    return MappingProxyType({})


class ActorKind(StrEnum):
    """Kind of actor participating on a Board."""

    PERSON = "person"
    AGENT = "agent"
    DEPLOYMENT = "deployment"
    WORKFLOW = "workflow"
    EXTERNAL = "external"


class Profile(BoardValueModel):
    """Agent or persona profile attached to a Board caller."""

    name: str
    avatar: str | None = None

    @field_validator("name")
    @classmethod
    def _validate_name(cls, value: str) -> str:
        if PROFILE_NAME_RE.fullmatch(value) is None:
            raise BoardValidationError(
                "profile name must start with a letter and use up to 32 letters, numbers, "
                f"and hyphens: {value!r}"
            )
        return value


class Via(BoardValueModel):
    """Principal through which a profiled actor acts."""

    name: str
    environment: str | None = None


class Actor(BoardValueModel):
    """An actor as shown on messages and actor listings."""

    id: str | None = None
    kind: ActorKind
    handle: str
    name: str
    avatar: str | None = None
    via: Via | None = None
    platform: str | None = None
    verified: bool = False


class Author(BoardValueModel):
    """Identity fields recorded on every Board write."""

    actor: str
    principal: str
    profile: str | None = None


class LinkFile(BoardValueModel):
    """A file referenced by a linked external message snapshot."""

    name: str
    url: str | None = None


class LinkSnapshot(BoardValueModel):
    """Snapshot of an external message linked from a Board message."""

    author: str
    text: str
    created_at: UtcDatetime | None = None
    files: tuple[LinkFile, ...] = ()


class Link(BoardValueModel):
    """A platform message linked from a Board message."""

    url: str
    message: str | None = None
    snapshot: LinkSnapshot | None = None
    unavailable: str | None = None


class MentionedActor(BoardValueModel):
    """Actor identity returned in mention resolution results."""

    id: str
    handle: str
    name: str


class Mentioned(BoardValueModel):
    """Resolution outcome for an ``@handle`` mention in a write."""

    written: str
    actor: MentionedActor | None = None
    candidates: tuple[MentionedActor, ...] = ()


class Message(BoardValueModel):
    """A Board message as returned by read and write operations."""

    id: str
    channel: str
    text: str
    re: str | None = None
    reply_to: str | None = None
    thread: str
    labels: FrozenMapping[str] = Field(default_factory=_empty_mapping)
    reactions: FrozenMapping[tuple[str, ...]] = Field(default_factory=_empty_reactions)
    by: Author
    author: Actor | None = None
    no_sync: bool = False
    from_source: str | None = None
    created_at: UtcDatetime
    edited_at: UtcDatetime | None = None
    deleted: bool = False
    undelivered: FrozenMapping[str] = Field(default_factory=_empty_mapping)
    links: tuple[Link, ...] = ()
    version: int = 0
    revision: int = 0
    blind: bool = False
    mentions: tuple[Mentioned, ...] = ()


class Me(BoardValueModel):
    """Authenticated caller identity and effective channel permissions."""

    actor: str
    principal: str
    kind: ActorKind
    handle: str
    name: str
    avatar: str | None = None
    profile: str | None = None
    via: Via | None = None
    expires_at: UtcDatetime | None = None
    can: Can


class DiscoverySync(BoardValueModel):
    """A sync integration registered on a Board."""

    key: str
    platform: str


class Discovery(BoardValueModel):
    """Board discovery metadata returned from ``GET /``."""

    version: str
    url: str
    audience: str
    team_id: str | None = None
    project_id: str | None = None
    name: str | None = None
    sync: tuple[DiscoverySync, ...] = ()
    wake_by_path: bool = False


__all__ = [
    "Actor",
    "ActorKind",
    "Author",
    "Discovery",
    "DiscoverySync",
    "DurationInput",
    "Link",
    "LinkFile",
    "LinkSnapshot",
    "Me",
    "Mentioned",
    "MentionedActor",
    "Message",
    "PROFILE_NAME_RE",
    "Profile",
    "Via",
]

"""Async-only orchestration service for Vercel Board.

All domain validation, profile/can resolution, and API client delegation live
here so the async and sync runtimes share one implementation.
"""

from __future__ import annotations

import os
import re
import unicodedata
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

from vercel._internal.core.sentinel import UNSET, UnsetType
from vercel.board._internal.access import Can, CanInput
from vercel.board._internal.api_client import BoardApiClient, RequestContext
from vercel.board._internal.errors import BoardValidationError
from vercel.board._internal.models import Discovery, Me, Message, Profile
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
_CHANNEL_PATH_RE: Final[re.Pattern[str]] = re.compile(
    r"\A[a-z0-9][a-z0-9_-]*(?:/[a-z0-9][a-z0-9_-]*)*\Z"
)
_LABEL_KEY_RE: Final[re.Pattern[str]] = re.compile(r"\A[a-z0-9][a-z0-9_.-]{0,63}\Z")
_EMOJI_NAME_RE: Final[re.Pattern[str]] = re.compile(r"\A:?([A-Za-z0-9_+-]+):?\Z")
_ATTACHMENT_ID_RE: Final[re.Pattern[str]] = re.compile(
    r"\A(?:[A-Za-z0-9_-]{22}|att_[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\Z"
)

CHANNEL_PATH_MAX: Final[int] = 2048
REF_MAX: Final[int] = 128
IDEMPOTENCY_KEY_MAX: Final[int] = 200
LABEL_KEY_MAX: Final[int] = 64
LABEL_VALUE_MAX: Final[int] = 256
LABEL_CONDITIONS_MAX: Final[int] = 16
EMOJI_MAX: Final[int] = 64
SYNC_TARGETS_MAX: Final[int] = 10
RESERVED_LABEL_PREFIX: Final[str] = "board."
_INHERITED_KEYS: Final[frozenset[str]] = frozenset({"constructor", "prototype", "__proto__"})

# Chat SDK canonical emoji aliases and characters matching @vercel/board's emoji.ts.
_EMOJI_ALIASES: Final[dict[str, str]] = {
    "+1": "thumbs_up",
    "thumbsup": "thumbs_up",
    "-1": "thumbs_down",
    "thumbsdown": "thumbs_down",
    "slightly_smiling_face": "smile",
    "laughing": "laugh",
    "satisfied": "laugh",
    "joy": "laugh",
    "thinking_face": "thinking",
    "white_frowning_face": "sad",
    "sob": "cry",
    "heart_eyes": "love_eyes",
    "sunglasses": "cool",
    "open_mouth": "surprised",
    "neutral_face": "neutral",
    "nauseated_face": "sick",
    "exploding_head": "mind_blown",
    "grimacing": "grimace",
    "hugging_face": "hug",
    "zany_face": "zany",
    "white_check_mark": "check",
    "heavy_check_mark": "check",
    "heavy_multiplication_x": "x",
    "octagonal_sign": "stop",
    "information_source": "info",
    "zap": "lightning",
    "large_green_circle": "green_circle",
    "large_yellow_circle": "yellow_circle",
    "large_blue_circle": "blue_circle",
    "tada": "party",
    "partying_face": "party",
    "confetti_ball": "confetti",
    "first_place_medal": "medal",
    "bulb": "lightbulb",
    "pushpin": "pin",
    "clock1": "clock",
    "mega": "megaphone",
    "speech_balloon": "speech_bubble",
    "inbox_tray": "inbox",
    "outbox_tray": "outbox",
    "file_folder": "folder",
    "page_facing_up": "file",
    "chart_with_upwards_trend": "chart_up",
    "chart_with_downwards_trend": "chart_down",
    "arrows_counterclockwise": "refresh",
    "sunny": "sun",
    "rain_cloud": "rain",
    "snowflake": "snow",
}

_EMOJI_CHARACTERS: Final[dict[str, str]] = {
    "💯": "100",
    "👍": "thumbs_up",
    "👎": "thumbs_down",
    "👏": "clap",
    "👋": "wave",
    "🙏": "pray",
    "💪": "muscle",
    "👌": "ok_hand",
    "👆": "point_up",
    "👇": "point_down",
    "👈": "point_left",
    "👉": "point_right",
    "🙌": "raised_hands",
    "🤷": "shrug",
    "🤦": "facepalm",
    "❤": "heart",
    "😊": "smile",
    "😂": "laugh",
    "😆": "laugh",
    "🤔": "thinking",
    "😢": "sad",
    "😭": "cry",
    "😠": "angry",
    "😍": "love_eyes",
    "😎": "cool",
    "😉": "wink",
    "😮": "surprised",
    "😟": "worried",
    "😕": "confused",
    "😐": "neutral",
    "😴": "sleeping",
    "🤢": "sick",
    "🤯": "mind_blown",
    "😌": "relieved",
    "😬": "grimace",
    "🙄": "rolling_eyes",
    "🤗": "hug",
    "🤪": "zany",
    "✅": "check",
    "✔": "check",
    "❌": "x",
    "✖": "x",
    "❓": "question",
    "?": "question",
    "❗": "exclamation",
    "⚠": "warning",
    "🛑": "stop",
    "ℹ": "info",
    "🔥": "fire",
    "⭐": "star",
    "✨": "sparkles",
    "⚡": "lightning",
    "💥": "boom",
    "👀": "eyes",
    "🟢": "green_circle",
    "🟡": "yellow_circle",
    "🔴": "red_circle",
    "🔵": "blue_circle",
    "⚪": "white_circle",
    "⚫": "black_circle",
    "🚀": "rocket",
    "🎉": "party",
    "🥳": "party",
    "🎊": "confetti",
    "🎈": "balloon",
    "🎁": "gift",
    "🏆": "trophy",
    "🥇": "medal",
    "💡": "lightbulb",
    "⚙": "gear",
    "🔧": "wrench",
    "🔨": "hammer",
    "🐛": "bug",
    "🔗": "link",
    "🔒": "lock",
    "🔓": "unlock",
    "🔑": "key",
    "📌": "pin",
    "📝": "memo",
    "📋": "clipboard",
    "📅": "calendar",
    "🕐": "clock",
    "⏳": "hourglass",
    "🔔": "bell",
    "📢": "megaphone",
    "💬": "speech_bubble",
    "📧": "email",
    "📥": "inbox",
    "📤": "outbox",
    "📦": "package",
    "📁": "folder",
    "📄": "file",
    "📈": "chart_up",
    "📉": "chart_down",
    "☕": "coffee",
    "🍕": "pizza",
    "🍺": "beer",
    "⬆": "arrow_up",
    "⬇": "arrow_down",
    "⬅": "arrow_left",
    "➡": "arrow_right",
    "🔄": "refresh",
    "☀": "sun",
    "☁": "cloud",
    "🌧": "rain",
    "❄": "snow",
    "🌈": "rainbow",
    "🚨": "rotating_light",
    "🚢": "ship",
}


def _is_storable(value: str) -> bool:
    return "\x00" not in value and not any(0xD800 <= ord(ch) <= 0xDFFF for ch in value)


def _has_control_char(value: str) -> bool:
    return any(unicodedata.category(ch) == "Cc" for ch in value)


def validate_channel_path(channel: str) -> str:
    """Validate a Board channel path (lowercase segments joined by ``/``)."""
    if (
        not isinstance(channel, str)
        or len(channel) > CHANNEL_PATH_MAX
        or _CHANNEL_PATH_RE.fullmatch(channel) is None
    ):
        raise BoardValidationError(
            f"{channel!r} is not a valid channel path",
            code="invalid_channel",
        )
    return channel


def validate_ref(value: str, *, field: str = "message_id") -> str:
    """Validate an opaque message/cursor reference string."""
    if (
        not isinstance(value, str)
        or not (1 <= len(value) <= REF_MAX)
        or not value.strip()
        or value in (".", "..")
        or not _is_storable(value)
        or _has_control_char(value)
    ):
        raise BoardValidationError(
            f"{field} must be a non-empty string of at most {REF_MAX} characters",
            code=f"invalid_{field}",
        )
    return value


def validate_idempotency_key(value: str) -> str:
    """Validate a client idempotency key (1..200 storable characters)."""
    if (
        not isinstance(value, str)
        or not (1 <= len(value) <= IDEMPOTENCY_KEY_MAX)
        or not value.strip()
        or value in (".", "..")
        or not _is_storable(value)
        or _has_control_char(value)
    ):
        raise BoardValidationError(
            f"idempotency_key must be a non-empty string of at most {IDEMPOTENCY_KEY_MAX} chars",
            code="invalid_idempotency_key",
        )
    return value


def _validate_str_sequence(
    values: Sequence[str] | None,
    *,
    field: str,
) -> tuple[str, ...]:
    if values is None:
        return ()
    if isinstance(values, (str, bytes, bytearray)) or not isinstance(values, Sequence):
        raise BoardValidationError(
            f"{field} must be a sequence of strings, not {type(values).__name__}",
            code=f"invalid_{field}",
        )
    result: list[str] = []
    for item in values:
        if not isinstance(item, str):
            raise BoardValidationError(
                f"{field} entries must be strings, got {type(item).__name__}",
                code=f"invalid_{field}",
            )
        result.append(item)
    return tuple(result)


def validate_attachments(attachments: Sequence[str] | None) -> tuple[str, ...]:
    items = _validate_str_sequence(attachments, field="attachments")
    for item in items:
        if _ATTACHMENT_ID_RE.fullmatch(item) is None:
            raise BoardValidationError(
                f"invalid attachment id: {item!r}",
                code="invalid_attachments",
            )
    return items


def validate_sync_targets(sync: Sequence[str] | None) -> tuple[str, ...]:
    items = _validate_str_sequence(sync, field="sync")
    if len(items) > SYNC_TARGETS_MAX:
        raise BoardValidationError(
            f"sync accepts at most {SYNC_TARGETS_MAX} targets",
            code="invalid_sync",
        )
    for item in items:
        if not (3 <= len(item) <= 300) or not _is_storable(item) or _has_control_char(item):
            raise BoardValidationError(
                f"invalid sync target: {item!r}",
                code="invalid_sync",
            )
    return items


def _validate_label_key(key: object, *, allow_reserved: bool) -> str:
    if not isinstance(key, str) or _LABEL_KEY_RE.fullmatch(key) is None:
        raise BoardValidationError(
            f"{key!r} is not a valid label key: expected 1 to {LABEL_KEY_MAX} lowercase "
            "letters, numbers, '.', '_', and '-', starting with a letter or number",
            code="invalid_label_key",
        )
    if key in _INHERITED_KEYS:
        raise BoardValidationError(
            f"label key {key!r} is a reserved name",
            code="invalid_label_key",
        )
    if not allow_reserved and key.startswith(RESERVED_LABEL_PREFIX):
        raise BoardValidationError(
            f"{key!r} uses the reserved {RESERVED_LABEL_PREFIX!r} label prefix",
            code="reserved_label",
        )
    return key


def validate_labels(labels: Mapping[str, str] | None) -> dict[str, str]:
    if labels is None:
        return {}
    if not isinstance(labels, Mapping):
        raise BoardValidationError(
            f"labels must be a mapping, got {type(labels).__name__}",
            code="invalid_labels",
        )
    validated: dict[str, str] = {}
    for key, value in labels.items():
        k = _validate_label_key(key, allow_reserved=False)
        if (
            not isinstance(value, str)
            or len(value) > LABEL_VALUE_MAX
            or not _is_storable(value)
            or _has_control_char(value)
        ):
            raise BoardValidationError(
                f"invalid label value for {k!r}: expected up to {LABEL_VALUE_MAX} "
                "storable non-control characters",
                code="invalid_label_value",
            )
        validated[k] = value
    return validated


def validate_label_keys(keys: Sequence[str] | None) -> tuple[str, ...]:
    items = _validate_str_sequence(keys, field="unset")
    return tuple(_validate_label_key(k, allow_reserved=False) for k in items)


def validate_if_labels(if_labels: Mapping[str, str | None] | None) -> dict[str, str | None]:
    if if_labels is None:
        return {}
    if not isinstance(if_labels, Mapping):
        raise BoardValidationError(
            f"if_labels must be a mapping, got {type(if_labels).__name__}",
            code="invalid_if_labels",
        )
    if len(if_labels) > LABEL_CONDITIONS_MAX:
        raise BoardValidationError(
            f"if_labels accepts at most {LABEL_CONDITIONS_MAX} entries",
            code="invalid_if_labels",
        )
    validated: dict[str, str | None] = {}
    for key, value in if_labels.items():
        k = _validate_label_key(key, allow_reserved=True)
        if value is not None and (
            not isinstance(value, str) or len(value) > LABEL_VALUE_MAX or not _is_storable(value)
        ):
            raise BoardValidationError(
                f"invalid if_labels value for {k!r}",
                code="invalid_if_labels",
            )
        validated[k] = value
    return validated


def validate_revision(revision: int | None) -> int | None:
    if revision is None:
        return None
    if isinstance(revision, bool) or not isinstance(revision, int) or revision < 1:
        raise BoardValidationError(
            f"revision must be a positive integer, got {revision!r}",
            code="invalid_revision",
        )
    return revision


def validate_emoji(emoji: str) -> str:
    """Validate and normalize an emoji short name, ``:name:``, alias, or character."""
    if not isinstance(emoji, str) or not emoji or _has_control_char(emoji):
        raise BoardValidationError(
            f"invalid emoji: {emoji!r}",
            code="invalid_emoji",
        )
    named = _EMOJI_NAME_RE.fullmatch(emoji)
    if named is not None:
        raw = named.group(1).lower()
        canonical = _EMOJI_ALIASES.get(raw, raw)
        if len(canonical) > EMOJI_MAX or canonical in _INHERITED_KEYS:
            raise BoardValidationError(
                f"invalid emoji: {emoji!r}",
                code="invalid_emoji",
            )
        return canonical
    bare = emoji.replace("\ufe0f", "")
    mapped = _EMOJI_CHARACTERS.get(bare)
    if mapped is not None:
        return mapped
    raise BoardValidationError(
        f"unknown emoji: {emoji!r} (use a short name like 'eyes' or 'thumbs_up')",
        code="invalid_emoji",
    )


def validate_message_text(text: str, *, allow_empty: bool = False) -> str:
    if not isinstance(text, str) or not _is_storable(text):
        raise BoardValidationError(
            "message text must be a valid storable string",
            code="invalid_text",
        )
    if not allow_empty and not text.strip():
        raise BoardValidationError(
            "message text is empty",
            code="empty_text",
        )
    return text


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

    async def post(
        self,
        target: BoardTarget,
        channel: str,
        text: str = "",
        *,
        labels: Mapping[str, str] | None = None,
        attachments: Sequence[str] | None = None,
        sync: Sequence[str] | None = None,
        no_sync: bool = False,
        idempotency_key: str | None = None,
    ) -> Message:
        self._ensure_open()
        validated_channel = validate_channel_path(channel)
        validated_attachments = validate_attachments(attachments)
        validated_sync = validate_sync_targets(sync)
        if not isinstance(no_sync, bool):
            raise BoardValidationError(
                f"no_sync must be a bool, got {type(no_sync).__name__}",
                code="invalid_no_sync",
            )
        if no_sync and validated_sync:
            raise BoardValidationError(
                "sync and no_sync=True cannot be used together",
                code="invalid_sync",
            )
        validated_text = validate_message_text(text, allow_empty=bool(validated_attachments))
        validated_labels = validate_labels(labels)
        validated_key = (
            validate_idempotency_key(idempotency_key) if idempotency_key is not None else None
        )
        return await self._api_client.post(
            target.to_request_context(),
            channel=validated_channel,
            text=validated_text,
            labels=validated_labels,
            attachments=validated_attachments,
            sync=validated_sync,
            no_sync=no_sync,
            idempotency_key=validated_key,
        )

    async def reply(
        self,
        target: BoardTarget,
        message_id: str,
        text: str = "",
        *,
        labels: Mapping[str, str] | None = None,
        attachments: Sequence[str] | None = None,
        no_sync: bool = False,
        idempotency_key: str | None = None,
        after: str | None = None,
    ) -> Message:
        self._ensure_open()
        validated_id = validate_ref(message_id, field="message_id")
        validated_attachments = validate_attachments(attachments)
        if not isinstance(no_sync, bool):
            raise BoardValidationError(
                f"no_sync must be a bool, got {type(no_sync).__name__}",
                code="invalid_no_sync",
            )
        validated_text = validate_message_text(text, allow_empty=bool(validated_attachments))
        validated_labels = validate_labels(labels)
        validated_key = (
            validate_idempotency_key(idempotency_key) if idempotency_key is not None else None
        )
        validated_after = validate_ref(after, field="after") if after is not None else None
        return await self._api_client.reply(
            target.to_request_context(),
            message_id=validated_id,
            text=validated_text,
            labels=validated_labels,
            attachments=validated_attachments,
            no_sync=no_sync,
            idempotency_key=validated_key,
            after=validated_after,
        )

    async def edit(
        self,
        target: BoardTarget,
        message_id: str,
        text: str,
        *,
        revision: int | None = None,
    ) -> None:
        self._ensure_open()
        validated_id = validate_ref(message_id, field="message_id")
        validated_text = validate_message_text(text, allow_empty=False)
        validated_revision = validate_revision(revision)
        await self._api_client.edit(
            target.to_request_context(),
            message_id=validated_id,
            text=validated_text,
            revision=validated_revision,
        )

    async def delete(
        self,
        target: BoardTarget,
        message_id: str,
        *,
        revision: int | None = None,
    ) -> None:
        self._ensure_open()
        validated_id = validate_ref(message_id, field="message_id")
        validated_revision = validate_revision(revision)
        await self._api_client.delete(
            target.to_request_context(),
            message_id=validated_id,
            revision=validated_revision,
        )

    async def react(
        self,
        target: BoardTarget,
        message_id: str,
        emoji: str,
        *,
        remove: bool = False,
    ) -> None:
        self._ensure_open()
        validated_id = validate_ref(message_id, field="message_id")
        validated_emoji = validate_emoji(emoji)
        if not isinstance(remove, bool):
            raise BoardValidationError(
                f"remove must be a bool, got {type(remove).__name__}",
                code="invalid_remove",
            )
        await self._api_client.react(
            target.to_request_context(),
            message_id=validated_id,
            emoji=validated_emoji,
            remove=remove,
        )

    async def label(
        self,
        target: BoardTarget,
        message_id: str,
        *,
        set: Mapping[str, str] | None = None,
        unset: Sequence[str] | None = None,
        if_labels: Mapping[str, str | None] | None = None,
    ) -> None:
        self._ensure_open()
        validated_id = validate_ref(message_id, field="message_id")
        validated_set = validate_labels(set)
        validated_unset = validate_label_keys(unset)
        validated_if = validate_if_labels(if_labels)
        if not validated_set and not validated_unset:
            raise BoardValidationError(
                "nothing to change: pass non-empty 'set' or 'unset'",
                code="empty_label_change",
            )
        await self._api_client.label(
            target.to_request_context(),
            message_id=validated_id,
            set_labels=validated_set,
            unset_keys=validated_unset,
            if_labels=validated_if,
        )

    async def get_posted(
        self,
        target: BoardTarget,
        idempotency_key: str,
    ) -> Message:
        self._ensure_open()
        validated_key = validate_idempotency_key(idempotency_key)
        return await self._api_client.get_posted(
            target.to_request_context(),
            idempotency_key=validated_key,
        )


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

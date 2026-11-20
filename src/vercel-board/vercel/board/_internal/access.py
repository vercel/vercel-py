"""Permissions and channel-scoped access rules (`Can`)."""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from types import MappingProxyType
from typing import Any, Final, Literal, TypeAlias

from pydantic import GetCoreSchemaHandler
from pydantic_core import core_schema

from vercel.board._internal.errors import BoardValidationError

CAN_MAX: Final[int] = 4000
CHANNEL_PATH_MAX: Final[int] = 2048

_CHANNEL_PATH_RE = re.compile(r"\A[a-z0-9][a-z0-9_-]*(?:/[a-z0-9][a-z0-9_-]*)*\Z")
_PATTERN_SHAPE_HINT = "write *, a channel, or a channel and its subtree (feedback/)"


class Permission(StrEnum):
    """Capabilities a principal may hold in a Board channel."""

    READ = "read"
    POST = "post"
    REACT = "react"
    LABEL = "label"
    SYNC = "sync"
    MANAGE = "manage"


PERMISSIONS: Final[tuple[Permission, ...]] = (
    Permission.READ,
    Permission.POST,
    Permission.REACT,
    Permission.LABEL,
    Permission.SYNC,
    Permission.MANAGE,
)

_PERMISSION_ORDER: Final[Mapping[Permission, int]] = MappingProxyType(
    {permission: index for index, permission in enumerate(PERMISSIONS)}
)

_IMPLIES: Final[Mapping[Permission, tuple[Permission, ...]]] = MappingProxyType(
    {
        Permission.READ: (),
        Permission.POST: (),
        Permission.REACT: (Permission.READ,),
        Permission.LABEL: (Permission.READ,),
        Permission.SYNC: (Permission.READ,),
        Permission.MANAGE: (
            Permission.READ,
            Permission.POST,
            Permission.REACT,
            Permission.LABEL,
            Permission.SYNC,
        ),
    }
)

_EMPTY_PERMS: Final[frozenset[Permission]] = frozenset()
_ALL_PERMS: Final[frozenset[Permission]] = frozenset(PERMISSIONS)

CanGrantInput: TypeAlias = Literal["all", "none"] | Sequence[Permission | str]
CanInput: TypeAlias = (
    Literal["all", "none"] | Sequence[Permission | str] | Mapping[str, CanGrantInput]
)


def _invalid_can(message: str) -> BoardValidationError:
    return BoardValidationError(message, code="invalid_can")


@dataclass(frozen=True, slots=True)
class _Pattern:
    kind: Literal["all", "exact", "subtree"]
    path: str = ""

    @property
    def key(self) -> str:
        if self.kind == "all":
            return "*"
        if self.kind == "subtree":
            return f"{self.path}/"
        return self.path


_ALL_PATTERN: Final[_Pattern] = _Pattern(kind="all")


def _is_channel_path(value: str) -> bool:
    return 0 < len(value) <= CHANNEL_PATH_MAX and _CHANNEL_PATH_RE.fullmatch(value) is not None


def _parse_pattern(pattern: str) -> _Pattern:
    if not isinstance(pattern, str):
        raise _invalid_can(f"invalid channel pattern {pattern!r}: {_PATTERN_SHAPE_HINT}")
    if pattern == "*":
        return _ALL_PATTERN
    if "*" in pattern:
        before = pattern[: pattern.index("*")].rstrip("/")
        if before == "":
            hint = "write * for every channel"
        elif _is_channel_path(before):
            hint = f"write {before}/ for {before} and its subtree"
        else:
            hint = _PATTERN_SHAPE_HINT
        raise _invalid_can(f"invalid channel pattern {pattern}: {hint}")
    subtree = pattern.endswith("/")
    path = pattern[:-1] if subtree else pattern
    if not _is_channel_path(path):
        label = pattern or "(empty)"
        raise _invalid_can(f"invalid channel pattern {label}: {_PATTERN_SHAPE_HINT}")
    return _Pattern(kind="subtree" if subtree else "exact", path=path)


def _coerce_permission(value: Permission | str, *, context: str | None = None) -> Permission:
    if isinstance(value, Permission):
        return value
    if isinstance(value, str):
        try:
            return Permission(value)
        except ValueError:
            pass
    allowed = ", ".join(p.value for p in PERMISSIONS)
    label = str(value) if value != "" else "(empty)"
    prefix = f"{context}: " if context else ""
    raise _invalid_can(f"{prefix}unknown permission {label}: use {allowed}, or all")


def _expand_permissions(permissions: Iterable[Permission | str]) -> frozenset[Permission]:
    out: set[Permission] = set()

    def add(perm: Permission) -> None:
        if perm in out:
            return
        out.add(perm)
        for implied in _IMPLIES[perm]:
            add(implied)

    for raw in permissions:
        add(_coerce_permission(raw))
    return frozenset(out)


def _expand_grant(grant: object, *, allow_none: bool) -> frozenset[Permission]:
    if grant == "all":
        return _ALL_PERMS
    if allow_none and grant == "none":
        return _EMPTY_PERMS
    if isinstance(grant, (str, bytes, bytearray)):
        raise _invalid_can(
            f"expected a sequence of permissions, 'all', or 'none', got string {grant!r}"
        )
    if not isinstance(grant, Sequence):
        raise _invalid_can(
            f"expected a sequence of permissions, 'all', or 'none', got {type(grant).__name__}"
        )
    return _expand_permissions(grant)


def _under(channel: str, path: str) -> bool:
    return channel == path or channel.startswith(f"{path}/")


@dataclass(frozen=True, slots=True)
class _Rules:
    all: frozenset[Permission] | None
    exact: Mapping[str, frozenset[Permission]]
    subtrees: tuple[tuple[str, frozenset[Permission]], ...]

    def entries(self) -> tuple[tuple[_Pattern, frozenset[Permission]], ...]:
        items: list[tuple[_Pattern, frozenset[Permission]]] = []
        if self.all is not None:
            items.append((_ALL_PATTERN, self.all))
        for path, perms in self.subtrees:
            items.append((_Pattern(kind="subtree", path=path), perms))
        for path, perms in self.exact.items():
            items.append((_Pattern(kind="exact", path=path), perms))
        return tuple(items)

    def resolve(self, channel: str, *, subtree_only: bool = False) -> frozenset[Permission]:
        if not subtree_only:
            exact = self.exact.get(channel)
            if exact is not None:
                return exact
        for path, perms in self.subtrees:
            if _under(channel, path):
                return perms
        return self.all if self.all is not None else _EMPTY_PERMS

    def at(self, pattern: _Pattern) -> frozenset[Permission]:
        if pattern.kind == "all":
            return self.all if self.all is not None else _EMPTY_PERMS
        return self.resolve(pattern.path, subtree_only=pattern.kind == "subtree")

    def parent(self, pattern: _Pattern) -> frozenset[Permission]:
        if pattern.kind == "exact":
            return self.resolve(pattern.path, subtree_only=True)
        if pattern.kind == "subtree":
            for path, perms in self.subtrees:
                if path != pattern.path and _under(pattern.path, path):
                    return perms
        return self.all if self.all is not None else _EMPTY_PERMS


def _compile(entries: Iterable[tuple[_Pattern, frozenset[Permission]]]) -> _Rules:
    all_perms: frozenset[Permission] | None = None
    exact: dict[str, frozenset[Permission]] = {}
    subtrees_map: dict[str, frozenset[Permission]] = {}
    for pattern, perms in entries:
        if pattern.kind == "all":
            all_perms = perms
        elif pattern.kind == "exact":
            exact[pattern.path] = perms
        else:
            subtrees_map[pattern.path] = perms
    subtrees = tuple(sorted(subtrees_map.items(), key=lambda item: (-len(item[0]), item[0])))
    return _Rules(all=all_perms, exact=MappingProxyType(exact), subtrees=subtrees)


def _canonical(entries: Iterable[tuple[_Pattern, frozenset[Permission]]]) -> _Rules:
    compiled = _compile(entries)
    kept: list[tuple[_Pattern, frozenset[Permission]]] = []
    for pattern, perms in compiled.entries():
        if pattern.kind == "all":
            if perms:
                kept.append((pattern, perms))
        elif perms != compiled.parent(pattern):
            kept.append((pattern, perms))
    return _compile(kept)


def _sorted_permissions(perms: frozenset[Permission]) -> tuple[Permission, ...]:
    return tuple(sorted(perms, key=lambda p: _PERMISSION_ORDER[p]))


def _format_permissions(perms: tuple[Permission, ...]) -> str:
    if len(perms) == len(PERMISSIONS):
        return "all"
    implied = {imp for perm in perms for imp in _IMPLIES[perm]}
    minimal = [perm.value for perm in perms if perm not in implied]
    return ",".join(minimal)


class Can:
    """Normalized, immutable channel-scoped access rule set.

    Rules follow Board's specificity precedence: an exact channel beats the
    longest matching subtree (``dev/``), which beats ``*``. Permission
    implications are closed on construction (``react``, ``label``, and ``sync``
    imply ``read``; ``manage`` implies all permissions; ``post`` implies none).
    """

    __slots__ = ("_rules", "_summary", "_hash")

    def __init__(self, _rules: _Rules) -> None:
        self._rules = _rules
        named = [(pattern.key, _sorted_permissions(perms)) for pattern, perms in _rules.entries()]
        named.sort(key=lambda item: (0 if item[0] == "*" else 1, item[0]))
        self._summary: Mapping[str, tuple[Permission, ...]] = MappingProxyType(dict(named))
        self._hash = hash(tuple(named))

    @classmethod
    def full(cls) -> Can:
        """Return full access (every permission in every channel)."""
        return _FULL_CAN

    @classmethod
    def empty(cls) -> Can:
        """Return empty access (no permissions in any channel)."""
        return _EMPTY_CAN

    @classmethod
    def from_input(cls, value: Can | CanInput) -> Can:
        """Normalize a ``Can`` or ``CanInput`` into a canonical ``Can``."""
        if isinstance(value, Can):
            return value
        if value == "all":
            return _FULL_CAN
        if value == "none":
            return _EMPTY_CAN
        if isinstance(value, Mapping):
            entries = [
                (_parse_pattern(pattern), _expand_grant(grant, allow_none=True))
                for pattern, grant in value.items()
            ]
            return cls(_canonical(entries))
        perms = _expand_grant(value, allow_none=True)
        return cls(_canonical([(_ALL_PATTERN, perms)]))

    @classmethod
    def parse(cls, header: str) -> Can:
        """Parse an ``x-board-can`` header string into a canonical ``Can``."""
        return parse_can(header)

    def allows(self, permission: Permission | str, channel: str) -> bool:
        """Return whether ``permission`` is allowed in ``channel``."""
        perm = _coerce_permission(permission)
        return perm in self._rules.resolve(channel)

    def everywhere(self, permission: Permission | str) -> bool:
        """Return whether ``permission`` is allowed in every channel."""
        perm = _coerce_permission(permission)
        if self._rules.all is None or perm not in self._rules.all:
            return False
        return all(perm in perms for _, perms in self._rules.entries())

    def anywhere(self, permission: Permission | str) -> bool:
        """Return whether ``permission`` is allowed in at least one channel."""
        perm = _coerce_permission(permission)
        return any(perm in perms for _, perms in self._rules.entries())

    def at(self, permission: Permission | str, pattern: str) -> bool:
        """Return whether ``permission`` is allowed where ``pattern`` decides."""
        perm = _coerce_permission(permission)
        return perm in self._rules.at(_parse_pattern(pattern))

    def narrow(self, other: Can | CanInput) -> Can:
        """Return the channel-by-channel intersection with ``other``.

        Narrowing is monotonic: it never grants a permission not held by both
        sides.
        """
        scope = other if isinstance(other, Can) else Can.from_input(other)
        patterns: dict[str, _Pattern] = {"*": _ALL_PATTERN}
        for pattern, _ in self._rules.entries():
            patterns[pattern.key] = pattern
        for pattern, _ in scope._rules.entries():
            patterns[pattern.key] = pattern
        intersected = [
            (pattern, self._rules.at(pattern) & scope._rules.at(pattern))
            for pattern in patterns.values()
        ]
        return Can(_canonical(intersected))

    def summary(self) -> Mapping[str, tuple[Permission, ...]]:
        """Return an immutable canonical mapping of pattern to permissions."""
        return self._summary

    def _format_rules(self) -> str:
        if not self._summary:
            return "*="
        parts = [
            f"{pattern}={_format_permissions(perms)}" for pattern, perms in self._summary.items()
        ]
        return ";".join(parts)

    def to_header(self) -> str:
        """Serialize as an ``x-board-can`` header string (e.g. ``dev/=read,post``)."""
        header = self._format_rules()
        if len(header) > CAN_MAX:
            raise _invalid_can(f"x-board-can is longer than {CAN_MAX} characters")
        return header

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Can):
            return NotImplemented
        return self._summary == other._summary

    def __hash__(self) -> int:
        return self._hash

    def __str__(self) -> str:
        return self._format_rules()

    def __repr__(self) -> str:
        return f"Can({self._format_rules()!r})"

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        _source_type: Any,
        handler: GetCoreSchemaHandler,
    ) -> core_schema.CoreSchema:
        def _validate(value: Any) -> Can:
            if isinstance(value, Can):
                return value
            return cls.from_input(value)

        json_input_schema = handler.generate_schema(CanInput)
        serializer = core_schema.plain_serializer_function_ser_schema(
            lambda can: {
                pattern: [perm.value for perm in perms] for pattern, perms in can.summary().items()
            },
            info_arg=False,
        )
        return core_schema.json_or_python_schema(
            json_schema=core_schema.no_info_after_validator_function(_validate, json_input_schema),
            python_schema=core_schema.no_info_plain_validator_function(_validate),
            serialization=serializer,
        )


_FULL_CAN: Final[Can] = Can(_canonical([(_ALL_PATTERN, _ALL_PERMS)]))
_EMPTY_CAN: Final[Can] = Can(_canonical([]))


def parse_can(header: str, *, name: str = "x-board-can") -> Can:
    """Parse an ``x-board-can`` header string into a canonical ``Can``."""
    if not isinstance(header, str):
        raise _invalid_can(f"{name} must be a string")
    if len(header) > CAN_MAX:
        raise _invalid_can(f"{name} is longer than {CAN_MAX} characters")

    raw_entries = [entry.strip(" \t") for entry in header.split(";")]
    if all(entry == "" for entry in raw_entries):
        raise _invalid_can(f"{name} is empty: write pattern=permissions, like dev/=read,post")

    seen_patterns: set[str] = set()
    entries: list[tuple[_Pattern, frozenset[Permission]]] = []
    for entry in raw_entries:
        if entry == "":
            continue
        if "=" not in entry:
            raise _invalid_can(
                f"{name} entry {entry} needs =: write pattern=permissions, like dev/=read,post"
            )
        pattern_raw, list_raw = entry.split("=", 1)
        pattern_text = pattern_raw.strip(" \t")
        perms_text = list_raw.strip(" \t")
        if pattern_text in seen_patterns:
            raise _invalid_can(f"{name} lists {pattern_text} twice")
        seen_patterns.add(pattern_text)
        pattern = _parse_pattern(pattern_text)
        if perms_text == "":
            entries.append((pattern, _EMPTY_PERMS))
            continue
        raw_perms = [item.strip(" \t") for item in perms_text.split(",")]
        if len(raw_perms) == 1 and raw_perms[0] == "all":
            entries.append((pattern, _ALL_PERMS))
            continue
        coerced = [_coerce_permission(item, context=name) for item in raw_perms]
        entries.append((pattern, _expand_permissions(coerced)))

    return Can(_canonical(entries))


__all__ = [
    "CAN_MAX",
    "Can",
    "CanGrantInput",
    "CanInput",
    "PERMISSIONS",
    "Permission",
    "parse_can",
]

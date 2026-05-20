"""Generic deferred settings primitives for `vercel.unstable` internals."""

from __future__ import annotations

import os
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Protocol, TypeAlias

from vercel._internal.unstable.errors import SettingsSourceError, SettingsValidationError


class _Missing:
    pass


MISSING = _Missing()
RawSetting: TypeAlias = object | _Missing


class SettingSource(Protocol):
    """A source that can provide raw settings values at load time."""

    @property
    def name(self) -> str:
        """Human-readable source name for diagnostics."""

    def get_value(self, field: str) -> RawSetting:
        """Return a raw value for `field`, or `MISSING` when unset."""


@dataclass(frozen=True, slots=True)
class ExplicitSettingsSource:
    """Settings source backed by explicit caller-supplied values."""

    values: Mapping[str, object | None]
    name: str = "explicit values"

    def get_value(self, field: str) -> RawSetting:
        value = self.values.get(field, MISSING)
        if value is None:
            return MISSING
        return value


@dataclass(frozen=True, slots=True)
class EnvironmentSettingsSource:
    """Settings source backed by environment variables."""

    field_names: Mapping[str, str]
    environ: Mapping[str, str] | None = None
    name: str = "environment"

    def get_value(self, field: str) -> RawSetting:
        key = self.field_names.get(field)
        if key is None:
            return MISSING

        source = os.environ if self.environ is None else self.environ
        value = source.get(key)
        if value is None or value == "":
            return MISSING
        return value


@dataclass(frozen=True, slots=True)
class DefaultsSettingsSource:
    """Settings source backed by static defaults."""

    values: Mapping[str, object]
    name: str = "defaults"

    def get_value(self, field: str) -> RawSetting:
        return self.values.get(field, MISSING)


@dataclass(frozen=True, slots=True)
class ResolvedSetting:
    """A raw setting value plus the source that provided it."""

    field: str
    value: object
    source: str


def resolve_setting(field: str, sources: Sequence[SettingSource]) -> ResolvedSetting:
    """Resolve one raw setting from ordered sources."""

    for source in sources:
        try:
            value = source.get_value(field)
        except Exception as exc:
            raise SettingsSourceError(
                f"failed to load setting {field!r} from {source.name}",
                field=field,
                source=source.name,
            ) from exc

        if not isinstance(value, _Missing):
            return ResolvedSetting(field=field, value=value, source=source.name)

    raise SettingsValidationError(
        f"missing required setting {field!r}",
        field=field,
        source="settings sources",
    )


def resolve_settings(
    fields: Iterable[str],
    sources: Sequence[SettingSource],
) -> dict[str, ResolvedSetting]:
    """Resolve raw settings for the requested field names."""

    return {field: resolve_setting(field, sources) for field in fields}


__all__ = [
    "DefaultsSettingsSource",
    "EnvironmentSettingsSource",
    "ExplicitSettingsSource",
    "MISSING",
    "RawSetting",
    "ResolvedSetting",
    "SettingSource",
    "resolve_setting",
    "resolve_settings",
]

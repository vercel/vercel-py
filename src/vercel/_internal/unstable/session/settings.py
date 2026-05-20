"""Session-specific settings declarations and validation."""

from __future__ import annotations

from typing import Protocol

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from vercel._internal.unstable.errors import SettingsValidationError
from vercel._internal.unstable.settings import (
    DefaultsSettingsSource,
    EnvironmentSettingsSource,
    ExplicitSettingsSource,
    ResolvedSetting,
    SettingSource,
    resolve_settings,
)


class SessionOptionsLike(Protocol):
    """Session options shape consumed by settings loading."""

    @property
    def client_pool_size(self) -> int | None:
        """Explicit client pool size override, or `None` when unset."""
        ...


class SessionSettings(BaseModel):
    """Resolved session runtime settings."""

    model_config = ConfigDict(frozen=True)

    client_pool_size: int = Field(gt=0)


_SESSION_SETTING_FIELDS = ("client_pool_size",)
_ENVIRONMENT_FIELDS = {
    "client_pool_size": "VERCEL_CLIENT_POOL_SIZE",
}
_DEFAULT_SETTINGS = {
    "client_pool_size": 10,
}


def default_session_setting_sources(options: SessionOptionsLike) -> tuple[SettingSource, ...]:
    """Build the default ordered setting sources for a session."""

    return (
        ExplicitSettingsSource(
            {"client_pool_size": options.client_pool_size},
            name="session options",
        ),
        EnvironmentSettingsSource(_ENVIRONMENT_FIELDS),
        DefaultsSettingsSource(_DEFAULT_SETTINGS, name="sdk defaults"),
    )


def load_session_settings(sources: tuple[SettingSource, ...]) -> SessionSettings:
    """Resolve and validate `SessionSettings` from ordered sources."""

    resolved = resolve_settings(_SESSION_SETTING_FIELDS, sources)
    try:
        return SessionSettings.model_validate(
            {field: setting.value for field, setting in resolved.items()}
        )
    except ValidationError as exc:
        field, source = _validation_context(exc, resolved)
        raise SettingsValidationError(
            f"invalid session setting {field!r} from {source}",
            field=field,
            source=source,
        ) from exc


def _validation_context(
    exc: ValidationError,
    resolved: dict[str, ResolvedSetting],
) -> tuple[str, str]:
    errors = exc.errors()
    if not errors:
        return "session settings", "settings sources"

    loc = errors[0]["loc"]
    field = loc[0] if loc else "session settings"
    if not isinstance(field, str):
        field = str(field)
    source = resolved[field].source if field in resolved else "settings sources"
    return field, source


__all__ = [
    "SessionOptionsLike",
    "SessionSettings",
    "default_session_setting_sources",
    "load_session_settings",
]

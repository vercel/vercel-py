from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass

import pytest

from vercel._internal.unstable.errors import (
    SettingsSourceError,
    SettingsValidationError,
    VercelError,
)
from vercel._internal.unstable.session.settings import (
    SessionSettings,
    load_session_settings,
)
from vercel._internal.unstable.settings import (
    MISSING,
    DefaultsSettingsSource,
    EnvironmentSettingsSource,
    ExplicitSettingsSource,
    RawSetting,
    SettingSource,
    resolve_setting,
    resolve_settings,
)
from vercel.unstable import Session, SessionOptions, SyncSession


class RecordingEnvironment(Mapping[str, str]):
    def __init__(self, values: dict[str, str]) -> None:
        self._values = values
        self.reads: list[str] = []

    def __getitem__(self, key: str) -> str:
        self.reads.append(key)
        return self._values[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._values)

    def __len__(self) -> int:
        return len(self._values)


@dataclass(frozen=True, slots=True)
class ExplodingSource:
    name: str = "exploding source"

    def get_value(self, field: str) -> RawSetting:
        _ = field
        raise RuntimeError("source failed")


@dataclass(frozen=True, slots=True)
class CountingSource:
    value: int
    calls: list[str]
    name: str = "counting source"

    def get_value(self, field: str) -> RawSetting:
        self.calls.append(field)
        return self.value


def test_generic_settings_precedence_uses_explicit_before_environment() -> None:
    setting = resolve_setting(
        "client_pool_size",
        [
            ExplicitSettingsSource({"client_pool_size": 5}),
            EnvironmentSettingsSource({"client_pool_size": "VERCEL_CLIENT_POOL_SIZE"}),
            DefaultsSettingsSource({"client_pool_size": 10}),
        ],
    )

    assert setting.value == 5
    assert setting.source == "explicit values"


def test_unset_explicit_values_fall_through_to_environment() -> None:
    settings = resolve_settings(
        ["client_pool_size"],
        [
            ExplicitSettingsSource({"client_pool_size": None}),
            EnvironmentSettingsSource(
                {"client_pool_size": "VERCEL_CLIENT_POOL_SIZE"},
                {"VERCEL_CLIENT_POOL_SIZE": "12"},
            ),
            DefaultsSettingsSource({"client_pool_size": 10}),
        ],
    )

    assert settings["client_pool_size"].value == "12"
    assert settings["client_pool_size"].source == "environment"


def test_environment_settings_are_read_only_at_load_time() -> None:
    environ = RecordingEnvironment({"VERCEL_CLIENT_POOL_SIZE": "8"})
    source = EnvironmentSettingsSource(
        {"client_pool_size": "VERCEL_CLIENT_POOL_SIZE"},
        environ,
    )

    assert environ.reads == []

    setting = resolve_setting(
        "client_pool_size",
        [
            ExplicitSettingsSource({}),
            source,
            DefaultsSettingsSource({"client_pool_size": 10}),
        ],
    )

    assert setting.value == "8"
    assert environ.reads == ["VERCEL_CLIENT_POOL_SIZE"]


def test_session_settings_use_pydantic_coercion() -> None:
    settings = load_session_settings(
        (
            ExplicitSettingsSource({}),
            EnvironmentSettingsSource(
                {"client_pool_size": "VERCEL_CLIENT_POOL_SIZE"},
                {"VERCEL_CLIENT_POOL_SIZE": "12"},
            ),
            DefaultsSettingsSource({"client_pool_size": 10}),
        )
    )

    assert settings == SessionSettings(client_pool_size=12)


async def test_async_session_loads_and_caches_settings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []
    source = CountingSource(5, calls)

    import vercel._internal.unstable.session as session_module

    def sources(options: SessionOptions) -> tuple[SettingSource, ...]:
        assert options.client_pool_size is None
        return (source,)

    monkeypatch.setattr(session_module, "default_session_setting_sources", sources)
    session = Session()

    assert calls == []
    assert session._settings is None

    await session.initialize()
    await session.initialize()

    assert calls == ["client_pool_size"]
    assert session._settings == SessionSettings(client_pool_size=5)


def test_sync_session_loads_and_caches_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    source = CountingSource(7, calls)

    import vercel._internal.unstable.session as session_module

    def sources(options: SessionOptions) -> tuple[SettingSource, ...]:
        assert options.client_pool_size is None
        return (source,)

    monkeypatch.setattr(session_module, "default_session_setting_sources", sources)
    session = SyncSession()

    assert calls == []
    assert session._settings is None

    session.initialize()
    session.initialize()

    assert calls == ["client_pool_size"]
    assert session._settings == SessionSettings(client_pool_size=7)


def test_invalid_session_setting_value_reports_field_and_source() -> None:
    with pytest.raises(SettingsValidationError) as raised:
        load_session_settings(
            (
                ExplicitSettingsSource({}),
                EnvironmentSettingsSource(
                    {"client_pool_size": "VERCEL_CLIENT_POOL_SIZE"},
                    {"VERCEL_CLIENT_POOL_SIZE": "zero"},
                ),
                DefaultsSettingsSource({"client_pool_size": 10}),
            )
        )

    error = raised.value
    assert isinstance(error, VercelError)
    assert error.field == "client_pool_size"
    assert error.source == "environment"


def test_source_failure_reports_field_and_source() -> None:
    with pytest.raises(SettingsSourceError) as raised:
        resolve_setting("client_pool_size", [ExplodingSource()])

    error = raised.value
    assert isinstance(error, VercelError)
    assert error.field == "client_pool_size"
    assert error.source == "exploding source"


def test_missing_required_setting_reports_validation_error() -> None:
    @dataclass(frozen=True, slots=True)
    class EmptySource:
        name: str = "empty source"

        def get_value(self, field: str) -> RawSetting:
            _ = field
            return MISSING

    with pytest.raises(SettingsValidationError) as raised:
        resolve_setting("client_pool_size", [EmptySource()])

    assert raised.value.field == "client_pool_size"
    assert raised.value.source == "settings sources"

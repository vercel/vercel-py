from __future__ import annotations

from dataclasses import dataclass

import pytest
from hypothesis import given, strategies as st

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
    resolve_setting,
)


@dataclass(frozen=True, slots=True)
class ExplodingSource:
    name: str = "exploding source"

    def get_value(self, field: str) -> RawSetting:
        _ = field
        raise RuntimeError("source failed")


@dataclass(frozen=True, slots=True)
class EmptySource:
    name: str = "empty source"

    def get_value(self, field: str) -> RawSetting:
        _ = field
        return MISSING


@given(
    explicit=st.one_of(st.none(), st.integers(min_value=1, max_value=100)),
    env=st.one_of(st.none(), st.just(""), st.integers(min_value=1, max_value=100).map(str)),
    default=st.integers(min_value=1, max_value=100),
)
def test_settings_resolution_uses_first_non_missing_source(
    explicit: int | None,
    env: str | None,
    default: int,
) -> None:
    environ = {} if env is None else {"VERCEL_CLIENT_POOL_SIZE": env}
    setting = resolve_setting(
        "client_pool_size",
        [
            ExplicitSettingsSource({"client_pool_size": explicit}),
            EnvironmentSettingsSource(
                {"client_pool_size": "VERCEL_CLIENT_POOL_SIZE"},
                environ,
            ),
            DefaultsSettingsSource({"client_pool_size": default}),
        ],
    )

    if explicit is not None:
        assert setting.value == explicit
        assert setting.source == "explicit values"
    elif env not in (None, ""):
        assert setting.value == env
        assert setting.source == "environment"
    else:
        assert setting.value == default
        assert setting.source == "defaults"
    assert setting.field == "client_pool_size"


def test_session_settings_use_pydantic_coercion() -> None:
    settings = load_session_settings(
        (
            ExplicitSettingsSource({}),
            EnvironmentSettingsSource(
                {"client_pool_size": "VERCEL_CLIENT_POOL_SIZE"},
                {"VERCEL_CLIENT_POOL_SIZE": "12"},
            ),
            DefaultsSettingsSource({"client_pool_size": 10, "http2": False}),
        )
    )

    assert settings == SessionSettings(client_pool_size=12, http2=False)


def test_invalid_session_setting_value_reports_field_and_source() -> None:
    with pytest.raises(SettingsValidationError) as raised:
        load_session_settings(
            (
                ExplicitSettingsSource({}),
                EnvironmentSettingsSource(
                    {"client_pool_size": "VERCEL_CLIENT_POOL_SIZE"},
                    {"VERCEL_CLIENT_POOL_SIZE": "zero"},
                ),
                DefaultsSettingsSource({"client_pool_size": 10, "http2": False}),
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
    with pytest.raises(SettingsValidationError) as raised:
        resolve_setting("client_pool_size", [EmptySource()])

    assert raised.value.field == "client_pool_size"
    assert raised.value.source == "settings sources"

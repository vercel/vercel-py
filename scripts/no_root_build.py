from __future__ import annotations

MESSAGE = (
    "This repository root is a uv workspace, not a publishable package. "
    "Use `uv build --all-packages --no-sources` to build all packages, "
    "or `uv build --package <name> --no-sources` to build one package."
)


def _raise() -> None:
    raise RuntimeError(MESSAGE)


def get_requires_for_build_sdist(config_settings: object = None) -> list[str]:
    return []


def get_requires_for_build_wheel(config_settings: object = None) -> list[str]:
    return []


def build_sdist(sdist_directory: str, config_settings: object = None) -> str:
    _raise()


def build_wheel(
    wheel_directory: str,
    config_settings: object = None,
    metadata_directory: str | None = None,
) -> str:
    _raise()

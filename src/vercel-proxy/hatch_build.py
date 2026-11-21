"""Load the shared Vercel Hatch metadata hook."""

from __future__ import annotations

import tomllib
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from types import ModuleType

from hatchling.metadata.plugin.interface import MetadataHookInterface

PACKAGED_HOOK = "_vercel_hatch_build.py"


def get_metadata_hook() -> type[MetadataHookInterface]:
    """Return the shared workspace dependency metadata hook."""
    return _load_shared_hook().get_metadata_hook()


def _load_shared_hook() -> ModuleType:
    path = _shared_hook_path(Path(__file__).resolve().parent)
    spec = spec_from_file_location("_vercel_hatch_build", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load Hatch hook from {path}")
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _shared_hook_path(package: Path) -> Path:
    """Choose the hook to execute using only what the package itself says.

    A source archive and a generated bundle ship their own copy of the hook, and
    it is the only one they may run. The directories above an unpacked archive
    belong to whoever is building it, such as a shared temporary directory, so a
    hook found there could be anyone's code.

    Only a checkout has no packaged copy. It runs the file its own
    `pyproject.toml` would package as that copy.
    """
    packaged = package / PACKAGED_HOOK
    if packaged.is_file():
        return packaged
    if (package / "PKG-INFO").exists():
        raise RuntimeError(f"source archive is missing its Hatch metadata hook, {PACKAGED_HOOK}")
    return _declared_hook_source(package)


def _declared_hook_source(package: Path) -> Path:
    """Return the file this package's sdist `force-include` ships as its hook.

    The package names that file once, for Hatch, so a checkout build runs
    exactly what a source archive would carry. Nothing is discovered: no
    directory above the package can claim it or redirect the lookup.
    """
    with (package / "pyproject.toml").open("rb") as fp:
        hatch = tomllib.load(fp).get("tool", {}).get("hatch", {})
    included = hatch.get("build", {}).get("targets", {}).get("sdist", {}).get("force-include", {})
    sources = [
        source
        for source, target in included.items()
        if isinstance(target, str) and target.lstrip("/") == PACKAGED_HOOK
    ]
    if len(sources) != 1:
        raise RuntimeError(
            f"pyproject.toml must force-include exactly one file as {PACKAGED_HOOK} in its sdist"
        )
    hook = (package / sources[0]).resolve()
    if not hook.is_file():
        raise RuntimeError(f"Hatch metadata hook {hook} does not exist")
    return hook

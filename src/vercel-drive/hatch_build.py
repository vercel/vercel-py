"""Load the shared Vercel Hatch metadata hook."""

from __future__ import annotations

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


def _shared_hook_path(root: Path) -> Path:
    """Choose the hook to execute without trusting directories outside the package.

    A source archive and a generated bundle ship their own copy of the hook, and
    it is the only one they may run. The directories above an unpacked archive
    belong to whoever is building it, such as a shared temporary directory, so a
    hook found there could be anyone's code.

    Only a workspace checkout has no packaged copy. There the hook is the
    repository's own `scripts/hatch_build.py`.
    """
    packaged = root / PACKAGED_HOOK
    if packaged.is_file():
        return packaged
    if (root / "PKG-INFO").exists():
        raise RuntimeError(f"source archive is missing its Hatch metadata hook, {PACKAGED_HOOK}")
    workspace_hook = root.parent.parent / "scripts" / "hatch_build.py"
    if workspace_hook.is_file():
        return workspace_hook
    raise RuntimeError("could not find shared Vercel Hatch metadata hook")

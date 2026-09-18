"""Load the shared Vercel Hatch metadata hook."""

from __future__ import annotations

from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from types import ModuleType

from hatchling.metadata.plugin.interface import MetadataHookInterface


def get_metadata_hook() -> type[MetadataHookInterface]:
    """Return the shared workspace dependency metadata hook."""
    return _load_shared_hook().get_metadata_hook()


def _load_shared_hook() -> ModuleType:
    root = Path(__file__).resolve().parent
    candidates = [root / "../../scripts/hatch_build.py", root / "_vercel_hatch_build.py"]
    for candidate in candidates:
        path = candidate.resolve()
        if path.exists():
            spec = spec_from_file_location("_vercel_hatch_build", path)
            if spec is None or spec.loader is None:
                raise RuntimeError(f"could not load Hatch hook from {path}")
            module = module_from_spec(spec)
            spec.loader.exec_module(module)
            return module
    raise RuntimeError("could not find shared Vercel Hatch metadata hook")

"""Load the shared Vercel Hatch metadata hook."""

from __future__ import annotations

import tomllib
from fnmatch import fnmatchcase
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path, PurePosixPath
from types import ModuleType

from hatchling.metadata.plugin.interface import MetadataHookInterface

PACKAGED_HOOK = "_vercel_hatch_build.py"
WORKSPACE_HOOK = "scripts/hatch_build.py"


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
    """Choose the hook to execute without trusting directories outside the package.

    A source archive and a generated bundle ship their own copy of the hook, and
    it is the only one they may run. The directories above an unpacked archive
    belong to whoever is building it, such as a shared temporary directory, so a
    hook found there could be anyone's code.

    Only a workspace checkout has no packaged copy. There the hook belongs to
    the uv workspace that declares this package as a member.
    """
    packaged = package / PACKAGED_HOOK
    if packaged.is_file():
        return packaged
    if (package / "PKG-INFO").exists():
        raise RuntimeError(f"source archive is missing its Hatch metadata hook, {PACKAGED_HOOK}")
    workspace = _workspace_root(package)
    if workspace is None:
        raise RuntimeError(f"{package} is neither a source archive nor a uv workspace member")
    hook = workspace / WORKSPACE_HOOK
    if not hook.is_file():
        raise RuntimeError(f"uv workspace {workspace} has no {WORKSPACE_HOOK}")
    return hook


def _workspace_root(package: Path) -> Path | None:
    """Return the nearest uv workspace whose `members` include *package*.

    The root is identified by its `[tool.uv.workspace]` table, as uv does,
    not by how many directories it is above the package.
    """
    for candidate in package.parents:
        pyproject = candidate / "pyproject.toml"
        if not pyproject.is_file():
            continue
        with pyproject.open("rb") as fp:
            workspace = tomllib.load(fp).get("tool", {}).get("uv", {}).get("workspace")
        if not isinstance(workspace, dict):
            continue
        member = PurePosixPath(package.relative_to(candidate).as_posix())
        if _matches_any(member, workspace.get("members", ())) and not _matches_any(
            member, workspace.get("exclude", ())
        ):
            return candidate
    return None


def _matches_any(member: PurePosixPath, patterns: object) -> bool:
    if not isinstance(patterns, (list, tuple)):
        return False
    return any(isinstance(pattern, str) and _matches(member, pattern) for pattern in patterns)


def _matches(member: PurePosixPath, pattern: str) -> bool:
    # A `*` matches one directory name, never a path separator.
    globs = PurePosixPath(pattern).parts
    return len(globs) == len(member.parts) and all(
        fnmatchcase(part, glob) for part, glob in zip(member.parts, globs, strict=True)
    )

"""Hatch metadata hook for publish-time workspace dependency bounds."""

from __future__ import annotations

import ast
from pathlib import Path
from typing import Any

from hatchling.metadata.plugin.interface import MetadataHookInterface
from packaging.requirements import Requirement
from packaging.specifiers import SpecifierSet

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python < 3.11
    import tomli as tomllib  # type: ignore[no-redef]


class WorkspaceDependenciesMetadataHook(MetadataHookInterface):
    """Generate package dependencies from the repo-owned dependency table."""

    def update(self, metadata: dict[str, Any]) -> None:
        """Populate dynamic dependencies for Hatchling metadata generation."""
        pyproject = _load_pyproject(Path(self.root))
        release = pyproject.get("tool", {}).get("vercel", {}).get("release", {})
        dependency_table = release.get("dependencies", {})
        workspace_sources = pyproject.get("tool", {}).get("uv", {}).get("sources", {})
        workspace_names = {
            name for name, source in workspace_sources.items() if source.get("workspace") is True
        }
        workspace_root = _find_workspace_root(Path(self.root))

        package = pyproject.get("project", {}).get("name", str(self.root))

        metadata["dependencies"] = [
            _rewrite_dependency(requirement, workspace_names, workspace_root, package)
            for requirement in dependency_table.get("dependencies", [])
        ]


def _load_pyproject(path: Path) -> dict[str, Any]:
    with (path / "pyproject.toml").open("rb") as fp:
        return tomllib.load(fp)


def _find_workspace_root(start: Path) -> Path | None:
    for path in [start, *start.parents]:
        pyproject = path / "pyproject.toml"
        if not pyproject.exists():
            continue
        data = _load_pyproject(path)
        if "workspace" in data.get("tool", {}).get("uv", {}):
            return path
    return None


def _rewrite_dependency(
    requirement: str,
    workspace_names: set[str],
    workspace_root: Path | None,
    package: str,
) -> str:
    parsed = Requirement(requirement)
    normalized = parsed.name.lower().replace("_", "-")
    if normalized not in workspace_names or workspace_root is None:
        return requirement
    version = _read_workspace_version(workspace_root, normalized)
    return _with_lower_bound(parsed, version, requirement, package)


def _with_lower_bound(requirement: Requirement, version: str, declared: str, package: str) -> str:
    extras = f"[{','.join(sorted(requirement.extras))}]" if requirement.extras else ""
    specifiers = [
        str(specifier) for specifier in requirement.specifier if specifier.operator != ">="
    ]
    specifier_text = ",".join([f">={version}", *specifiers])
    _reject_unsatisfiable(package, requirement.name, declared, specifier_text, version)
    marker = f" ; {requirement.marker}" if requirement.marker else ""
    return f"{requirement.name}{extras}{specifier_text}{marker}"


def _reject_unsatisfiable(
    package: str, dependency: str, declared: str, specifier_text: str, version: str
) -> None:
    """Refuse to publish a bound that no version of *dependency* can satisfy.

    The lower bound is generated from the sibling's current version while the
    rest of the specifier is whatever the package declared, so a hand-written
    upper bound that the sibling has since reached produces something like
    ``>=0.3.0,<0.3.0``. Nothing rejects that later: the wheel builds, uploads,
    and only fails when someone tries to install it. Fail the build instead --
    CI builds every package, so the bump that crosses the bound is caught by
    its own pull request.

    The test is that the sibling version *being released* satisfies the bound,
    which is narrower than the range being non-empty: `>=0.7.1,!=0.7.1` leaves
    room for a later version, but says the release under way is unusable.
    """
    # `prereleases=True` so a workspace version like `0.4.0b1` is judged
    # against its own bound rather than excluded for being a prerelease.
    if SpecifierSet(specifier_text).contains(version, prereleases=True):
        return
    raise RuntimeError(
        f"{package} declares {declared!r}, but {dependency} is at {version} in "
        f"the workspace, so publishing would pin {dependency}{specifier_text} — "
        f"which excludes {version} itself, the version being released. "
        f"Raise the upper bound in [tool.vercel.release.dependencies] of {package}."
    )


def _read_workspace_version(workspace_root: Path, package_name: str) -> str:
    for pattern in ("src/*/pyproject.toml", "integrations/*/pyproject.toml"):
        for pyproject_path in workspace_root.glob(pattern):
            version = _version_from_pyproject(pyproject_path, package_name)
            if version is not None:
                return version
    raise RuntimeError(f"unknown workspace dependency {package_name!r}")


def _version_from_pyproject(pyproject_path: Path, package_name: str) -> str | None:
    data = _load_pyproject(pyproject_path.parent)
    if data.get("project", {}).get("name") != package_name:
        return None
    version_path = pyproject_path.parent / data["tool"]["hatch"]["version"]["path"]
    module = ast.parse(version_path.read_text(encoding="utf-8"), filename=str(version_path))
    for node in module.body:
        value = _version_value(node)
        if value is None:
            continue
        if isinstance(value, ast.Constant) and isinstance(value.value, str):
            return value.value
    raise RuntimeError(f"could not find __version__ in {version_path}")


def _version_value(node: ast.stmt) -> ast.expr | None:
    if isinstance(node, ast.Assign) and any(
        isinstance(target, ast.Name) and target.id == "__version__" for target in node.targets
    ):
        return node.value
    if (
        isinstance(node, ast.AnnAssign)
        and isinstance(node.target, ast.Name)
        and node.target.id == "__version__"
    ):
        return node.value
    return None


def get_metadata_hook() -> type[MetadataHookInterface]:
    """Return the hook class used by Hatchling's custom hook loader."""
    return WorkspaceDependenciesMetadataHook

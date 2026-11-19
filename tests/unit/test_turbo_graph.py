from __future__ import annotations

import json
from pathlib import Path

from scripts import turbo_graph, workspace


def _package(name: str, path: Path, *dependencies: str) -> workspace.Package:
    return workspace.Package(
        name=name,
        path=path,
        version_file=path / "version.py",
        dependencies=dependencies,
    )


def test_turbo_json_matches_the_uv_workspace_graph() -> None:
    current = turbo_graph.TURBO_JSON.read_text(encoding="utf-8")

    expected = turbo_graph.dumps(turbo_graph.render(json.loads(current), workspace.packages()))

    assert current == expected, "run `uv run python scripts/turbo_graph.py`"


def test_packages_import_only_their_workspace_dependencies() -> None:
    assert turbo_graph.undeclared_imports(workspace.packages()) == {}


def test_render_replaces_generated_entries(tmp_path: Path) -> None:
    config = {
        "tasks": {
            "sources": {"inputs": ["vercel/**"]},
            "poe:test": {"dependsOn": ["sources"]},
            "removed#sources": {"dependsOn": ["a#sources"]},
        }
    }
    packages = {
        "a": _package("a", tmp_path / "a"),
        "b": _package("b", tmp_path / "b", "a"),
    }

    assert turbo_graph.render(config, packages)["tasks"] == {
        "sources": {"inputs": ["vercel/**"]},
        "poe:test": {"dependsOn": ["sources"]},
        "b#sources": {"inputs": ["vercel/**"], "dependsOn": ["a#sources"]},
    }


def test_undeclared_imports_reports_members_outside_the_dependency_closure(
    tmp_path: Path,
) -> None:
    for name, source in {
        "a": "",
        "b": "import vercel.a\n",
        "c": "from vercel import b\nfrom vercel.a import thing\n",
        "d": "import vercel.a.inner\nimport os\n",
    }.items():
        module = tmp_path / name / "vercel" / name
        module.mkdir(parents=True)
        (module / "__init__.py").write_text(source, encoding="utf-8")
    packages = {
        "a": _package("a", tmp_path / "a"),
        "b": _package("b", tmp_path / "b", "a"),
        "c": _package("c", tmp_path / "c", "b"),
        "d": _package("d", tmp_path / "d"),
    }

    assert turbo_graph.undeclared_imports(packages) == {"d": {"a"}}

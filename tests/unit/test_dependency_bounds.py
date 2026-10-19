from pathlib import Path
from unittest.mock import patch

import pytest

from scripts import dependency_bounds


def _package(root: Path, name: str, body: str) -> Path:
    path = root / name
    path.mkdir()
    (path / "pyproject.toml").write_text(f'[project]\nname = "{name}"\n{body}', encoding="utf-8")
    return path


def test_collects_published_third_party_requirements(tmp_path: Path) -> None:
    packages = {
        "vercel-a": _package(
            tmp_path,
            "vercel-a",
            'dynamic = ["dependencies"]\n'
            "[project.optional-dependencies]\n"
            'speed = ["uvloop>=0.19"]\n'
            "[tool.vercel.release.dependencies]\n"
            "dependencies = [\n"
            '    "vercel-b>=0.1.0",\n'
            '    "anyio>=4.11.0,<5",\n'
            "    \"tomli>=2 ; python_version < '3.11'\",\n"
            "]\n",
        ),
        "vercel-b": _package(
            tmp_path, "vercel-b", 'dependencies = ["anyio>=4.0", "httpx2[http2]>=2.6"]\n'
        ),
    }

    assert dependency_bounds.declared_requirements(packages) == [
        "anyio>=4.0",
        "anyio>=4.11.0,<5",
        "httpx2[http2]>=2.6",
        "tomli>=2 ; python_version < '3.11'",
        "uvloop>=0.19",
    ]


@pytest.mark.parametrize(
    ("specifier", "bounded"),
    [
        (">=1.0", True),
        (">1.0", True),
        ("==1.0", True),
        ("~=1.0", True),
        (">=1.0,<2", True),
        ("", False),
        ("<2", False),
        ("!=1.0", False),
    ],
)
def test_requires_every_published_requirement_to_have_a_floor(
    tmp_path: Path, specifier: str, bounded: bool
) -> None:
    packages = {
        "vercel-a": _package(
            tmp_path,
            "vercel-a",
            f'[project.optional-dependencies]\nserver = ["uvicorn{specifier}"]\n',
        )
    }

    if bounded:
        assert dependency_bounds.declared_requirements(packages) == [f"uvicorn{specifier}"]
    else:
        with pytest.raises(ValueError, match=r"vercel-a\[server\]: uvicorn"):
            dependency_bounds.declared_requirements(packages)


@pytest.mark.parametrize(
    ("bound", "resolution"), [("lowest", "lowest-direct"), ("latest", "highest")]
)
def test_resolves_at_the_requested_bound_within_the_release_cooldown(
    bound: str, resolution: str
) -> None:
    with patch.object(dependency_bounds.subprocess, "run") as run:
        run.return_value.stdout = "anyio==4.11.0\nidna==3.10\n"
        pins = dependency_bounds.resolve(
            bound, ["anyio>=4.11.0,<5", "pydantic>=2.7"], exclude_newer="2 days"
        )

    assert pins == "anyio==4.11.0\nidna==3.10"
    assert run.call_args.kwargs["input"] == "anyio>=4.11.0,<5\npydantic>=2.7\n"
    args = run.call_args.args[0]
    assert args[args.index("--resolution") + 1] == resolution
    assert args[args.index("--exclude-newer") + 1] == "2 days"

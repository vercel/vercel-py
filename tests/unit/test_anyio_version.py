import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch
from zipfile import ZipFile

import pytest

from scripts import anyio_version


@pytest.mark.parametrize(
    "boundary,resolution", [("lowest", "lowest-direct"), ("latest", "highest")]
)
def test_boundary_uses_declared_range(tmp_path: Path, boundary: str, resolution: str) -> None:
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(
        '[tool.vercel.release.dependencies]\ndependencies = ["httpx2>=2", "anyio>=4.12,<4.14"]\n',
        encoding="utf-8",
    )
    with patch.object(anyio_version.subprocess, "run") as run:
        run.return_value.stdout = "anyio==4.12.0\n"
        assert anyio_version.resolve_boundary(boundary, pyproject) == "anyio==4.12.0"
    assert run.call_args.kwargs["input"] == "anyio<4.14,>=4.12\n"
    args = run.call_args.args[0]
    assert args[args.index("--resolution") + 1] == resolution
    assert "--no-config" not in args
    assert run.call_args.kwargs["cwd"] == anyio_version.PROJECT_ROOT
    assert "--upgrade" in args


@pytest.fixture
def package_index(tmp_path: Path) -> str:
    index = tmp_path / "index"
    package = index / "anyio"
    package.mkdir(parents=True)
    links = []
    now = datetime.now(UTC)
    for version, age in [("4.12.0", 10), ("4.12.1", 3), ("4.13.0", 0), ("4.14.0", 10)]:
        filename = f"anyio-{version}-py3-none-any.whl"
        with ZipFile(package / filename, "w") as wheel:
            wheel.writestr(
                f"anyio-{version}.dist-info/METADATA",
                f"Metadata-Version: 2.3\nName: anyio\nVersion: {version}\n",
            )
            wheel.writestr(
                f"anyio-{version}.dist-info/WHEEL",
                "Wheel-Version: 1.0\nRoot-Is-Purelib: true\nTag: py3-none-any\n",
            )
        uploaded = (now - timedelta(days=age)).isoformat()
        links.append(f'<a href="{filename}" data-upload-time="{uploaded}">{filename}</a>')
    (package / "index.html").write_text("\n".join(links), encoding="utf-8")
    return index.as_uri()


@pytest.mark.parametrize(
    "boundary,settings,expected",
    [
        ("lowest", 'exclude-newer = "2 days"', "anyio==4.12.0"),
        ("latest", 'exclude-newer = "2 days"', "anyio==4.12.1"),
        ("latest", "", "anyio==4.13.0"),
        (
            "latest",
            'exclude-newer = "2 days"\nexclude-newer-package = { anyio = false }',
            "anyio==4.13.0",
        ),
        (
            "latest",
            'exclude-newer = "2 days"\nexclude-newer-package = { unrelated = false }',
            "anyio==4.12.1",
        ),
        (
            "latest",
            'exclude-newer = "20 days"\nexclude-newer-package = { anyio = "2 days" }',
            "anyio==4.12.1",
        ),
        (
            "latest",
            'exclude-newer = "2 days"\nexclude-newer-package = { anyio = "2000-01-01" }',
            None,
        ),
    ],
)
def test_boundary_respects_project_config(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    package_index: str,
    boundary: str,
    settings: str,
    expected: str | None,
) -> None:
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(
        '[tool.vercel.release.dependencies]\ndependencies = ["anyio>=4.12,<4.14"]\n'
        f"[tool.uv]\n{settings}\n"
        f'[[tool.uv.index]]\nurl = "{package_index}"\ndefault = true\n',
        encoding="utf-8",
    )
    monkeypatch.setattr(anyio_version, "PROJECT_ROOT", tmp_path)
    caller = tmp_path / "caller"
    caller.mkdir()
    (caller / "uv.toml").write_text('exclude-newer = "2000-01-01"\n', encoding="utf-8")
    monkeypatch.chdir(caller)

    if expected is None:
        with pytest.raises(subprocess.CalledProcessError):
            anyio_version.resolve_boundary(boundary, pyproject)
    else:
        assert anyio_version.resolve_boundary(boundary, pyproject) == expected


def test_missing_anyio_dependency_fails(tmp_path: Path) -> None:
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(
        '[tool.vercel.release.dependencies]\ndependencies = ["httpx2>=2"]\n', encoding="utf-8"
    )
    with pytest.raises(ValueError, match="No AnyIO dependency"):
        anyio_version.anyio_requirement(pyproject)

from pathlib import Path
from unittest.mock import patch

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
    assert "--no-config" in args
    assert "--upgrade" in args


def test_missing_anyio_dependency_fails(tmp_path: Path) -> None:
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(
        '[tool.vercel.release.dependencies]\ndependencies = ["httpx2>=2"]\n', encoding="utf-8"
    )
    with pytest.raises(ValueError, match="No AnyIO dependency"):
        anyio_version.anyio_requirement(pyproject)

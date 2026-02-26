import importlib.util
import os
import subprocess
import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent


class TestLint(unittest.TestCase):
    def test_cqa_ruff_lint_check(self) -> None:
        if not importlib.util.find_spec("ruff"):
            raise unittest.SkipTest("ruff is not installed") from None

        try:
            subprocess.run(
                [sys.executable, "-m", "ruff", "check"],
                check=True,
                capture_output=True,
                cwd=PROJECT_ROOT,
            )
        except subprocess.CalledProcessError as ex:
            output = ex.output.decode()
            raise AssertionError(f"ruff validation failed:\n{output}") from None

    def test_cqa_ruff_format_check(self) -> None:
        if not importlib.util.find_spec("ruff"):
            raise unittest.SkipTest("ruff is not installed") from None

        try:
            subprocess.run(
                [sys.executable, "-m", "ruff", "format", "--check", "."],
                check=True,
                capture_output=True,
                cwd=PROJECT_ROOT,
            )
        except subprocess.CalledProcessError as ex:
            output = ex.output.decode()
            raise AssertionError(f"ruff format validation failed:\n{output}") from None


class TestTypecheck(unittest.TestCase):
    def test_cqa_typecheck_mypy(self) -> None:
        config_path = PROJECT_ROOT / "pyproject.toml"
        if not os.path.exists(config_path):
            raise RuntimeError("could not locate pyproject.toml file")

        if not importlib.util.find_spec("mypy"):
            raise unittest.SkipTest("mypy is not installed") from None

        try:
            subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "mypy",
                    "--config-file",
                    str(config_path),
                    "src",
                ],
                check=True,
                capture_output=True,
                cwd=PROJECT_ROOT,
            )
        except subprocess.CalledProcessError as ex:
            output = ex.stdout.decode()
            if ex.stderr:
                output += "\n\n" + ex.stderr.decode()
            raise AssertionError(f"mypy validation failed:\n{output}") from None

    def test_cqa_module_imports(self) -> None:
        """Verify that all public modules can be imported."""
        modules = [
            "vercel",
            "vercel.cache",
            "vercel.headers",
            "vercel.oidc",
            "vercel.sandbox",
        ]
        for module in modules:
            try:
                __import__(module)
            except ImportError as ex:
                raise AssertionError(f"Failed to import {module}: {ex}") from None


if __name__ == "__main__":
    unittest.main(verbosity=2)

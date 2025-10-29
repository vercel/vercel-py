from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def test_examples_run(capsys=None) -> None:
    examples_dir = Path(__file__).resolve().parents[1] / "examples"
    assert examples_dir.is_dir()

    example_files = [p for p in examples_dir.iterdir() if p.is_file() and p.suffix == ".py"]
    assert example_files, "No example .py files found in examples/"

    for script_path in example_files:
        assert script_path.is_file()
        if capsys is not None:
            with capsys.disabled():
                print(f"Running {script_path.name}")
        else:
            print(f"Running {script_path.name}")
        result = subprocess.run(
            [sys.executable, str(script_path)],
            capture_output=True,
            text=True,
            timeout=45,
        )
        assert result.returncode == 0, (
            f"{script_path.name} failed with code {result.returncode}\n"
            f"STDOUT:\n{result.stdout}\n"
            f"STDERR:\n{result.stderr}"
        )

    print("All examples ran successfully")


if __name__ == "__main__":
    test_examples_run()

"""``python -m vercel.workflow manifest``, the only thing that writes the file.

The observability UI reads a manifest off disk -- `WORKFLOW_MANIFEST_PATH`, else
`<data dir>/manifest.json`, else the paths a JavaScript builder writes into a
project's source tree. Only the middle one can apply to a Python app, and the
CLI already resolves that directory to read runs at all, so writing it there is
what makes the Workflows and Graph tabs work unconfigured.

Nothing in the app writes it: the registry is empty while its module is still
importing, and a later automatic trigger would mean writing a file into whatever
directory a process happened to start in. So this command is the way in -- the
Python side of a contract whose other half lives in JavaScript, which is why the
last test runs the real command line rather than calling `main` in process.

The app is named the way the Vercel Python builder already names it, so a
project that deploys needs no arguments here.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from vercel.queue.testing import clear_subscriptions
from vercel.workflow._internal import cli, world as w

APP = """
from vercel.workflow import Workflows

app = Workflows(as_vercel_job=False)
billing = Workflows(namespace="billing", as_vercel_job=False)


@app.step
async def charge(amount: int) -> int:
    return amount


@app.workflow
async def checkout(amount: int) -> int:
    return await charge(amount)


@billing.workflow
async def refund(amount: int) -> int:
    return amount


# For the case that names something that is not a registry at all.
APP_TEXT = "not a registry"
"""

PYPROJECT = """
[project]
name = "an-app"
version = "0"

[[tool.vercel.workflows]]
entrypoint = "{spec}"
"""


@pytest.fixture(autouse=True)
def _reset_world():
    yield
    w.set_world(None)
    clear_subscriptions()


@pytest.fixture
def project(tmp_path, monkeypatch) -> Path:
    """A project holding an importable app, with this test's own data directory."""
    (tmp_path / "wf_app.py").write_text(APP)
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.setenv("WORKFLOW_LOCAL_DATA_DIR", str(tmp_path / ".workflow-data"))
    monkeypatch.setenv("WORKFLOW_TARGET_WORLD", "local")
    return tmp_path


def run(*argv: str) -> int:
    return cli.main(["manifest", *argv])


def written(project: Path) -> dict:
    return json.loads((project / ".workflow-data" / "manifest.json").read_text())


def test_it_writes_the_file_the_ui_reads(project, capsys) -> None:
    """Writing is the default, because the caller wanting this is a tool that
    knows where the UI looks and would otherwise have to be told."""
    assert run("wf_app:app") == 0

    assert written(project)["workflows"] != {}
    # The path, so the caller can log or check it.
    assert capsys.readouterr().out.strip() == str(project / ".workflow-data" / "manifest.json")


def test_a_bare_module_takes_every_registry_it_defines(project) -> None:
    """An app can hold more than one -- a namespaced registry beside the default
    one -- and a caller naming the module should not have to know."""
    assert run("wf_app") == 0

    assert sorted(written(project)["workflows"]["wf_app.py"]) == ["checkout", "refund"]


def test_naming_one_registry_takes_only_that_one(project) -> None:
    assert run("wf_app:billing") == 0

    assert sorted(written(project)["workflows"]["wf_app.py"]) == ["refund"]


def test_the_app_is_read_from_pyproject_when_not_named(project) -> None:
    """The whole point of the default: the `workflow` CLI runs this in a project
    directory and needs to know nothing about the app's layout."""
    (project / "pyproject.toml").write_text(PYPROJECT.format(spec="wf_app:app"))

    assert run(f"--project={project}") == 0

    assert sorted(written(project)["workflows"]["wf_app.py"]) == ["checkout"]


def test_stdout_prints_and_writes_nothing(project, capsys) -> None:
    assert run("wf_app:app", "--stdout") == 0

    assert json.loads(capsys.readouterr().out)["workflows"] != {}
    assert not (project / ".workflow-data" / "manifest.json").exists()


@pytest.mark.parametrize(
    ("argv", "expected"),
    [
        (("no_such_module",), "could not import"),
        (("wf_app:nope",), "has no attribute"),
        (("wf_app:APP_TEXT",), "not a Workflows registry"),
        (("json",), "defines no Workflows registry"),
    ],
    ids=["missing-module", "missing-attr", "wrong-type", "no-registry"],
)
def test_what_it_says_when_it_cannot(project, capsys, argv, expected) -> None:
    """Each of these becomes a message the `workflow` CLI shows a user, so it
    has to name what to fix rather than raise through importlib."""
    assert run(*argv) == 1

    assert expected in capsys.readouterr().err


def test_a_project_without_pyproject_says_so(tmp_path, capsys) -> None:
    assert run(f"--project={tmp_path}") == 1

    assert "nothing to read the app from" in capsys.readouterr().err


def test_a_pyproject_that_declares_nothing_says_so(project, capsys) -> None:
    (project / "pyproject.toml").write_text('[project]\nname = "x"\nversion = "0"\n')

    assert run(f"--project={project}") == 1

    assert "declares no entrypoint" in capsys.readouterr().err


def test_it_replaces_what_a_previous_run_left(project) -> None:
    """A workflow renamed or deleted since the last run must not survive in the
    file, so the write replaces rather than merges into it."""
    data_dir = project / ".workflow-data"
    data_dir.mkdir(parents=True, exist_ok=True)
    (data_dir / "manifest.json").write_text(
        json.dumps(
            {
                "version": "1.0.0",
                "steps": {},
                "workflows": {"gone.py": {"deleted": {"workflowId": "workflow//gone//deleted"}}},
                "classes": {},
            }
        )
    )

    assert run("wf_app:app") == 0

    assert "gone.py" not in written(project)["workflows"]


def test_it_writes_without_opting_into_publishing(project, monkeypatch) -> None:
    """`WORKFLOW_PUBLIC_MANIFEST` gates serving the document over HTTP. Wanting
    it on disk for the local UI is not the same request, and a JavaScript build
    does not ask for that either -- it writes the file regardless."""
    monkeypatch.delenv("WORKFLOW_PUBLIC_MANIFEST", raising=False)

    assert run("wf_app:app") == 0

    assert (project / ".workflow-data" / "manifest.json").exists()


def test_a_world_with_nowhere_to_keep_it_says_so(project, capsys, monkeypatch) -> None:
    """`World.write_manifest` is a no-op for every world but the local one, and
    a command that wrote nothing must not report success."""
    monkeypatch.setenv("WORKFLOW_TARGET_WORLD", "vercel")

    assert run("wf_app:app") == 1

    assert "nowhere to keep a manifest" in capsys.readouterr().err


def test_a_path_it_cannot_write_says_why(project, capsys) -> None:
    """The reason, not just "nowhere to keep it": those have different fixes."""
    (project / ".workflow-data").mkdir(parents=True, exist_ok=True)
    (project / ".workflow-data" / "manifest.json").mkdir()

    assert run("wf_app:app") == 1

    assert "could not write the manifest" in capsys.readouterr().err


def test_the_command_line_works_as_a_command_line(tmp_path) -> None:
    """The contract the JavaScript side depends on, run the way it will run it:
    a subprocess in the project directory, with no arguments.

    Everything above calls `main` in process, which cannot catch a broken
    `__main__`, a missing dependency of it, or an argument spelling that only
    argparse sees.
    """
    (tmp_path / "wf_app.py").write_text(APP)
    (tmp_path / "pyproject.toml").write_text(PYPROJECT.format(spec="wf_app:app"))

    result = subprocess.run(
        [sys.executable, "-m", "vercel.workflow", "manifest"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=120,
        env={
            "PATH": "/usr/bin:/bin",
            "WORKFLOW_TARGET_WORLD": "local",
            "PYTHONPATH": str(tmp_path),
        },
        check=False,
    )

    assert result.returncode == 0, f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    manifest = json.loads((tmp_path / ".workflow-data" / "manifest.json").read_text())
    assert list(manifest["workflows"]["wf_app.py"]) == ["checkout"]
    assert result.stdout.strip().endswith("manifest.json")

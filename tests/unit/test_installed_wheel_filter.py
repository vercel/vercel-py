"""Guard the respx exclusion used by the installed-wheel workflow.

`.github/scripts/test_installed_wheel.sh` installs a bundle wheel, where httpx2 is
vendored as `vercel.internal._vendor.httpx2`, and runs the package's tests against it. respx
patches the *unvendored* httpx2, so it silently fails to intercept anything and the
affected tests attempt real network calls. The script therefore deselects every
test that touches respx, deciding by scanning each test's source.

That scan understands three shapes: respx named in the test body, respx named in a
decorator, and a call to (or fixture request for) a module-level helper that reaches
respx, directly or through other module-level helpers. These tests assert that
every respx test in the repo stays within those shapes, so a test written in a
shape the scanner cannot see fails here instead of failing the publish workflow.
"""

import ast
import subprocess
import sys
from pathlib import Path
from zipfile import ZipFile

import pytest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / ".github" / "scripts" / "test_installed_wheel.sh"


@pytest.mark.parametrize("http_source", ["package", "dependency", "unvendored"])
def test_vendored_http_detection_includes_bundle_dependencies(
    tmp_path: Path, http_source: str
) -> None:
    test_root = tmp_path / "installed"
    package_root = test_root / "vercel" / "passport"
    package_root.mkdir(parents=True)
    tests = test_root / "tests"
    tests.mkdir()
    test_file = tests / "test_client.py"
    test_file.write_text("import httpx2 as httpx\n", encoding="utf-8")
    source = "from vercel.internal._vendor import httpx2 as httpx\n"
    (package_root / "__init__.py").write_text(
        source if http_source == "package" else "", encoding="utf-8"
    )
    wheel_dir = tmp_path / "wheels"
    wheel_dir.mkdir()
    with ZipFile(wheel_dir / "vercel_oidc_bundle-0.11.0-py3-none-any.whl", "w") as archive:
        archive.writestr(
            "vercel/oidc/verify.py",
            source if http_source == "dependency" else "import httpx2 as httpx\n",
        )
    script = SCRIPT.read_text(encoding="utf-8")
    detection = script.split('python - "$test_root" "$wheel_dir" <<\'PY\'\n', 1)[1]
    detection = detection.split("\nPY\n", 1)[0]
    subprocess.run(
        [sys.executable, "-", str(test_root), str(wheel_dir)],
        input=detection,
        text=True,
        check=True,
    )
    vendored = http_source != "unvendored"
    assert (test_root / ".vendored-libraries").read_text().strip() == ("httpx2" if vendored else "")
    assert test_file.read_text() == (source if vendored else "import httpx2 as httpx\n")


def _load_scanner() -> dict[str, object]:
    """Execute the scanner functions straight out of the shell script."""
    text = SCRIPT.read_text(encoding="utf-8")
    start = text.index("def node_source(")
    end = text.index("def pytest_filter_for_tests")
    namespace: dict[str, object] = {"ast": ast}
    exec(text[start:end], namespace)  # noqa: S102 - the script is repo-owned
    return namespace


def _test_modules() -> list[Path]:
    paths: list[Path] = []
    for package in sorted((ROOT / "src").iterdir()):
        tests = package / "tests"
        if not tests.is_dir():
            continue
        paths.extend(
            path
            for path in sorted(tests.rglob("test_*.py"))
            if "respx" in path.read_text(encoding="utf-8")
        )
    return paths


def test_scanner_functions_are_present_in_the_script() -> None:
    scanner = _load_scanner()

    for name in ("node_source", "respx_helper_names", "uses_respx"):
        assert callable(scanner[name]), f"{name} missing from {SCRIPT.name}"


def test_decorator_only_respx_usage_is_detected() -> None:
    """The original scanner read from `def` onwards and missed decorators."""
    scanner = _load_scanner()
    source = "@respx.mock\ndef test_thing():\n    route_helper()\n"
    module = ast.parse(source)

    node = module.body[0]
    assert scanner["uses_respx"](source, node, set()) is True  # type: ignore[operator]


@pytest.mark.parametrize(
    ("helpers_source", "expected_helpers"),
    [
        pytest.param(
            "def route_helper():\n    return respx.post('https://example.test')\n",
            {"route_helper"},
            id="direct",
        ),
        pytest.param(
            "class _Api:\n"
            "    def __init__(self, router: respx.MockRouter) -> None: ...\n"
            "\n"
            "def _open():\n"
            "    return _Api(None)\n"
            "\n"
            "@pytest.fixture\n"
            "def route_helper(request):\n"
            "    with _open() as opened:\n"
            "        yield opened\n",
            {"_Api", "_open", "route_helper"},
            id="chained",
        ),
    ],
)
def test_helper_and_fixture_indirection_is_detected(
    helpers_source: str, expected_helpers: set[str]
) -> None:
    scanner = _load_scanner()
    source = helpers_source + (
        "\n"
        "def test_calls_helper():\n"
        "    route_helper()\n"
        "\n"
        "def test_requests_fixture(route_helper):\n"
        "    pass\n"
        "\n"
        "def test_unrelated(other):\n"
        "    pass\n"
    )
    module = ast.parse(source)
    helpers = scanner["respx_helper_names"](source, module)  # type: ignore[operator]

    assert helpers == expected_helpers
    tests = {node.name: node for node in module.body if node.name.startswith("test_")}  # type: ignore[attr-defined]
    for name in ("test_calls_helper", "test_requests_fixture"):
        assert scanner["uses_respx"](source, tests[name], helpers) is True  # type: ignore[operator]
    assert scanner["uses_respx"](source, tests["test_unrelated"], helpers) is False  # type: ignore[operator]


@pytest.mark.parametrize("path", _test_modules(), ids=lambda p: f"{p.parent.parent.name}/{p.name}")
def test_every_respx_test_is_deselectable(path: Path) -> None:
    """Every respx test must be visible to the scanner.

    If this fails, the test uses respx through a route the scanner cannot follow
    (a conftest fixture, or `usefixtures` by string). Either
    move the respx usage into the test module, or teach the scanner about the new
    shape.
    """
    scanner = _load_scanner()
    text = path.read_text(encoding="utf-8")
    module = ast.parse(text, filename=str(path))
    helpers = scanner["respx_helper_names"](text, module)  # type: ignore[operator]

    deselected = {
        node.name
        for node in module.body
        if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))
        and scanner["uses_respx"](text, node, helpers)  # type: ignore[operator]
    }

    invisible = []
    for node in module.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if not node.name.startswith("test_"):
                continue
            body = ast.get_source_segment(text, node) or ""
            if "respx" in body and node.name not in deselected:
                invisible.append(node.name)
        elif isinstance(node, ast.ClassDef):
            body = ast.get_source_segment(text, node) or ""
            if "respx" in body and node.name not in deselected:
                invisible.append(node.name)

    assert not invisible, (
        f"{path.relative_to(ROOT)}: respx used by {invisible} but not deselectable by "
        f"{SCRIPT.name}; see this module's docstring"
    )

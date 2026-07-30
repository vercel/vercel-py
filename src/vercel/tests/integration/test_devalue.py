"""Interop tests that run the real JavaScript ``devalue`` package.

``vercel._internal.devalue`` exists to speak the same wire format as the JS
package, so the unit tests assert against hand-written wire strings.  Those
literals can only ever encode what their author believed JS does; they cannot
catch a divergence.  These tests execute the actual package instead.

Each round-trip case makes a full circle::

    Python stringify ─→ JS parse ─→ assert.deepStrictEqual vs the expected JS value
                                 └→ JS stringify ─→ Python parse ─→ compare to expected

Only *values* are compared, never wire strings: `1.0` vs `1` and set iteration
order legitimately differ between the two implementations while remaining
perfectly interoperable.

A second family covers values Python cannot construct (``Map``, ``URL``,
``Uint8Array``, boxed primitives, sparse arrays, …).  Those have no Python
encode leg, so they assert the documented Python landing type instead.

The whole suite needs Node.  Following the precedent in ``tests/test_examples.py``
it is optional locally and mandatory on CI, so a developer without Node is not
blocked but a real divergence cannot merge green.  Set ``VERCEL_DEVALUE_JS`` to a
devalue checkout to test against something other than the pinned npm release.
"""

from __future__ import annotations

import json
import math
import os
import re
import shutil
import subprocess
import tempfile
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from vercel._internal.devalue import Undefined, parse, stringify

# The version fetched from npm when VERCEL_DEVALUE_JS is not set.  Keep this in
# step with the upstream release the port is written against.
DEVALUE_VERSION = "5.8.2"

_IS_CI = bool(os.getenv("CI"))

# Marks a case as JS-origin: there is no Python value to encode.
_NO_PY = object()


# ═══════════════════════════════════════════════════════════════════════════
# locating the JavaScript package
# ═══════════════════════════════════════════════════════════════════════════


def _entry_in(prefix: Path) -> Path:
    return prefix / "node_modules" / "devalue" / "index.js"


def _npm_install_into(prefix: Path) -> bool:
    prefix.mkdir(parents=True, exist_ok=True)
    result = subprocess.run(
        [
            "npm",
            "install",
            f"devalue@{DEVALUE_VERSION}",
            "--no-save",
            "--no-audit",
            "--no-fund",
            "--prefix",
            str(prefix),
        ],
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )
    return result.returncode == 0 and _entry_in(prefix).is_file()


def _npm_install_devalue() -> Path | None:
    """Install the pinned devalue into a version-keyed temp dir, and reuse it.

    pytest-xdist gives every worker its own session fixture, so several
    processes reach this at once. npm rebuilds ``node_modules`` in place and
    non-atomically, so installing straight into the shared directory lets one
    worker observe ``index.js`` and then have it vanish underneath a running
    node. Each worker therefore installs into a private staging directory and
    publishes it with a single atomic rename; losers of that race reuse the
    winner's copy.
    """
    shared = Path(tempfile.gettempdir()) / f"vercel-py-devalue-{DEVALUE_VERSION}"
    if _entry_in(shared).is_file():
        return _entry_in(shared)

    staging = Path(tempfile.mkdtemp(prefix=f"vercel-py-devalue-{DEVALUE_VERSION}-"))
    if not _npm_install_into(staging):
        shutil.rmtree(staging, ignore_errors=True)
        return None

    try:
        # Atomic when `shared` does not exist; raises if another worker won.
        staging.rename(shared)
    except OSError:
        if _entry_in(shared).is_file():
            shutil.rmtree(staging, ignore_errors=True)
            return _entry_in(shared)
        # `shared` exists but holds no usable install (e.g. a partial tree from
        # an interrupted run). Keep our own copy rather than deleting a path
        # another process may be reading.
        return _entry_in(staging)

    return _entry_in(shared)


def _resolve_devalue_entry() -> tuple[Path | None, str]:
    """Return the devalue entry point, or ``None`` plus the reason it is missing."""
    override = os.getenv("VERCEL_DEVALUE_JS")
    if override:
        candidate = Path(override).expanduser()
        if candidate.is_dir():
            candidate = candidate / "index.js"
        if not candidate.is_file():
            return None, f"VERCEL_DEVALUE_JS does not point at a devalue package: {override}"
        return candidate, ""

    if shutil.which("node") is None:
        return None, "node is not installed"
    if shutil.which("npm") is None:
        return None, "npm is not installed"

    entry = _npm_install_devalue()
    if entry is None:
        return None, f"could not npm install devalue@{DEVALUE_VERSION}"
    return entry, ""


@pytest.fixture(scope="session")
def devalue_entry() -> Path:
    entry, reason = _resolve_devalue_entry()
    if entry is None:
        message = (
            f"JS devalue unavailable ({reason}). Install node, or point "
            f"VERCEL_DEVALUE_JS at a devalue checkout."
        )
        if _IS_CI:
            pytest.fail(message)
        pytest.skip(message)
    return entry


# ═══════════════════════════════════════════════════════════════════════════
# the Node harness
# ═══════════════════════════════════════════════════════════════════════════

# Reads `[{name, js, pyJson, jsReducers, jsRevivers}, …]` on stdin and writes
# `[{name, jsVerifyError, jsJson, harnessError}, …]` on stdout.
_JS_HARNESS = """
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { pathToFileURL } from 'node:url';

const { parse, stringify } = await import(pathToFileURL(process.env.DEVALUE_ENTRY).href);
const evaluate = (src) => (0, eval)(`(${src})`);

const cases = JSON.parse(readFileSync(0, 'utf8'));
const results = [];

for (const testCase of cases) {
  const result = { name: testCase.name };
  let expected;

  try {
    expected = evaluate(testCase.js);
  } catch (error) {
    result.harnessError = `building expected value: ${error.message}`;
    results.push(result);
    continue;
  }

  // Leg 1 — decode what Python encoded and verify it in JS.
  if (testCase.pyJson !== null) {
    try {
      const revivers = testCase.jsRevivers ? evaluate(testCase.jsRevivers) : undefined;
      assert.deepStrictEqual(parse(testCase.pyJson, revivers), expected);
    } catch (error) {
      result.jsVerifyError = error.message;
    }
  }

  // Leg 2 — encode the JS value for Python to decode.
  try {
    const reducers = testCase.jsReducers ? evaluate(testCase.jsReducers) : undefined;
    result.jsJson = stringify(expected, reducers);
  } catch (error) {
    result.harnessError = `stringify: ${error.message}`;
  }

  results.push(result);
}

process.stdout.write(JSON.stringify(results));
"""


# ═══════════════════════════════════════════════════════════════════════════
# cases
# ═══════════════════════════════════════════════════════════════════════════


class Custom:
    """Stand-in for a caller type handled by a reducer/reviver pair."""

    def __init__(self, value: int) -> None:
        self.value = value

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Custom) and other.value == self.value

    def __repr__(self) -> str:
        return f"Custom({self.value!r})"


@dataclass(frozen=True)
class Case:
    name: str
    js: str
    """JS expression producing the value equivalent to ``value``."""

    value: Any = _NO_PY
    """The Python value to encode, or ``_NO_PY`` for JS-origin cases."""

    expected: Any = _NO_PY
    """What Python should decode back to; defaults to ``value``."""

    py_reducers: dict[str, Callable] | None = None
    py_revivers: dict[str, Callable] | None = None
    js_reducers: str | None = None
    js_revivers: str | None = None
    extra: Callable[[Any], None] | None = None
    """Extra assertions on the Python-decoded value (identity, sharing, …)."""

    def expected_value(self) -> Any:
        return self.value if self.expected is _NO_PY else self.expected


def _cyclic_dict() -> dict:
    value: dict = {"x": 1}
    value["self"] = value
    return value


def _cyclic_list() -> list:
    value: list = [1, 2]
    value.append(value)
    return value


_SHARED = {"x": 1}

_AWARE = datetime(2024, 1, 2, 3, 4, 5, 678000, tzinfo=timezone.utc)

ROUND_TRIP_CASES: list[Case] = [
    # ── scalars ────────────────────────────────────────────────────────────
    Case("int", "42", 42),
    Case("negative_int", "-5", -5),
    Case("zero", "0", 0),
    Case("float", "0.1", 0.1),
    # JSON has no int/float distinction, so an integral float lands as an int.
    Case("float_integral", "2", 2.0, expected=2),
    Case("nan", "NaN", float("nan")),
    Case("infinity", "Infinity", float("inf")),
    Case("negative_infinity", "-Infinity", float("-inf")),
    Case("negative_zero", "-0", -0.0),
    Case("true", "true", True),
    Case("false", "false", False),
    Case("none", "null", None),
    Case("undefined", "undefined", Undefined),
    # ── strings ────────────────────────────────────────────────────────────
    Case("string", '"hello world"', "hello world"),
    Case("string_empty", '""', ""),
    Case("string_xss", '"<script>alert(1)</script>"', "<script>alert(1)</script>"),
    Case("string_line_separators", '"a\\u2028b\\u2029c"', "a b c"),
    Case("string_control_chars", '"a\\u0000b\\u001fc"', "a\x00b\x1fc"),
    Case("string_quotes_backslash", '"he said \\"hi\\" \\\\ ok"', 'he said "hi" \\ ok'),
    Case("string_unicode", '"héllo 😀 中文"', "héllo 😀 中文"),
    # ── containers ─────────────────────────────────────────────────────────
    Case("list", '[1,"two",null,true]', [1, "two", None, True]),
    Case("list_empty", "[]", []),
    Case("tuple", "[1,2]", (1, 2), expected=[1, 2]),
    Case("dict", '{"a":1,"b":"c"}', {"a": 1, "b": "c"}),
    Case("dict_empty", "{}", {}),
    Case("nested", '{"a":[1,{"b":2}]}', {"a": [1, {"b": 2}]}),
    Case("dict_non_identifier_keys", '{"x-y":1,"":2}', {"x-y": 1, "": 2}),
    Case("set", "new Set([1,2,3])", {1, 2, 3}),
    Case("set_empty", "new Set()", set()),
    Case("frozenset", "new Set([1,2])", frozenset({1, 2}), expected={1, 2}),
    # ── rich types ─────────────────────────────────────────────────────────
    Case("datetime_aware", 'new Date("2024-01-02T03:04:05.678Z")', _AWARE),
    # A naive datetime is serialized as UTC, so it comes back tz-aware.
    Case(
        "datetime_naive",
        'new Date("2024-01-02T03:04:05.678Z")',
        datetime(2024, 1, 2, 3, 4, 5, 678000),
        expected=_AWARE,
    ),
    Case(
        "datetime_epoch",
        'new Date("1970-01-01T00:00:00.000Z")',
        datetime(1970, 1, 1, tzinfo=timezone.utc),
    ),
    Case("regex_no_flags", "/a+b/", re.compile("a+b")),
    Case("regex_ignorecase", "/a+b/i", re.compile("a+b", re.IGNORECASE)),
    Case(
        "regex_multiline_dotall",
        "/a.b/ms",
        re.compile("a.b", re.MULTILINE | re.DOTALL),
    ),
    Case("bytes", "new Uint8Array([0,1,255]).buffer", b"\x00\x01\xff"),
    Case("bytes_empty", "new Uint8Array([]).buffer", b""),
    Case("bytearray", "new Uint8Array([1,2]).buffer", bytearray(b"\x01\x02"), expected=b"\x01\x02"),
    # ── integers across the JS safe boundary ───────────────────────────────
    Case("safe_int_max", "9007199254740991", 2**53 - 1),
    Case("safe_int_min", "-9007199254740991", -(2**53) + 1),
    Case("unsafe_int_positive", "9007199254740993n", 2**53 + 1),
    Case("unsafe_int_negative", "-9007199254740993n", -(2**53) - 1),
    Case("unsafe_int_huge", "123456789012345678901234567890n", 123456789012345678901234567890),
    # ── graph shape ────────────────────────────────────────────────────────
    Case(
        "cycle_dict",
        "(() => { const o = { x: 1 }; o.self = o; return o; })()",
        _cyclic_dict(),
        extra=lambda v: _assert(v["self"] is v, "cycle not preserved"),
    ),
    Case(
        "cycle_list",
        "(() => { const a = [1, 2]; a.push(a); return a; })()",
        _cyclic_list(),
        extra=lambda v: _assert(v[2] is v, "cycle not preserved"),
    ),
    Case(
        "shared_reference",
        "(() => { const o = { x: 1 }; return [o, o]; })()",
        [_SHARED, _SHARED],
        extra=lambda v: _assert(v[0] is v[1], "sharing not preserved"),
    ),
    Case(
        "repeated_string",
        '(() => { const s = "abc"; return [s, s]; })()',
        ["abc", "abc"],
    ),
    # ── custom types via reducers/revivers ─────────────────────────────────
    Case(
        "custom_type",
        "({ tag: 'custom', v: 42 })",
        Custom(42),
        py_reducers={"Custom": lambda x: {"v": x.value} if isinstance(x, Custom) else None},
        py_revivers={"Custom": lambda d: Custom(d["v"])},
        js_reducers="{ Custom: (x) => (x && x.tag === 'custom') ? { v: x.v } : null }",
        js_revivers="{ Custom: (d) => ({ tag: 'custom', v: d.v }) }",
    ),
]

JS_ORIGIN_CASES: list[Case] = [
    # Python has no Map; devalue lands it as a dict.
    Case("js_map_string_keys", 'new Map([["a","b"],["c","d"]])', expected={"a": "b", "c": "d"}),
    Case("js_map_number_keys", 'new Map([[1,"a"],[2,"b"]])', expected={1: "a", 2: "b"}),
    Case("js_map_empty", "new Map()", expected={}),
    # URL-ish and typed arrays land as their string / bytes payload.
    Case(
        "js_url", 'new URL("https://vercel.com/docs?a=1")', expected="https://vercel.com/docs?a=1"
    ),
    Case(
        "js_url_search_params",
        'new URLSearchParams("foo=1&foo=2&baz=%3C+%3E")',
        expected="foo=1&foo=2&baz=%3C+%3E",
    ),
    Case("js_uint8array", "new Uint8Array([1,2,3])", expected=b"\x01\x02\x03"),
    Case("js_uint16array", "new Uint16Array([1,2])", expected=b"\x01\x00\x02\x00"),
    Case("js_arraybuffer", "new Uint8Array([9,8]).buffer", expected=b"\x09\x08"),
    # Boxed primitives are unboxed.
    Case("js_boxed_number", "Object(42)", expected=42),
    Case("js_boxed_string", 'Object("woo!!!")', expected="woo!!!"),
    Case("js_boxed_boolean", "Object(true)", expected=True),
    # null-prototype objects land as plain dicts.
    Case(
        "js_null_prototype",
        'Object.assign(Object.create(null), { foo: "bar" })',
        expected={"foo": "bar"},
    ),
    Case("js_null_prototype_empty", "Object.create(null)", expected={}),
    # Holes and sparse arrays pad with Undefined.
    Case(
        "js_array_with_holes",
        '(() => { const a = [, "a", ,]; return a; })()',
        expected=[Undefined, "a", Undefined],
    ),
    Case(
        "js_sparse_array",
        '(() => { const a = []; a[5] = "x"; return a; })()',
        expected=[Undefined] * 5 + ["x"],
    ),
    Case("js_bigint", "123456789012345678901234567890n", expected=123456789012345678901234567890),
    Case("js_negative_zero_nested", "[0, -0]", expected=[0, -0.0]),
]

ALL_CASES: list[Case] = ROUND_TRIP_CASES + JS_ORIGIN_CASES


def _assert(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


# ═══════════════════════════════════════════════════════════════════════════
# comparison
# ═══════════════════════════════════════════════════════════════════════════


def deep_equal(a: Any, b: Any, seen: set[tuple[int, int]] | None = None) -> bool:
    """Structural equality with JS-comparable strictness.

    Plain ``==`` is wrong here in several ways: ``nan != nan``, ``-0.0 == 0.0``,
    ``True == 1``, ``1 == 1.0``, cyclic structures raise ``RecursionError``, and
    ``re.Pattern`` equality is really identity via ``re.compile``'s cache.
    """
    if seen is None:
        seen = set()

    if a is Undefined or b is Undefined:
        return a is b
    if a is None or b is None:
        return a is b
    if type(a) is not type(b):
        return False

    if isinstance(a, bool):
        return a is b
    if isinstance(a, float):
        if math.isnan(a):
            return math.isnan(b)
        if a == 0.0:
            return math.copysign(1.0, a) == math.copysign(1.0, b)
        return a == b
    if isinstance(a, (int, str, bytes, bytearray, datetime)):
        return a == b
    if isinstance(a, re.Pattern):
        return a.pattern == b.pattern and a.flags == b.flags
    if isinstance(a, (set, frozenset)):
        return a == b

    pair = (id(a), id(b))
    if pair in seen:
        # Already comparing this pair further up the stack — a cycle.
        return True
    seen = seen | {pair}

    if isinstance(a, (list, tuple)):
        return len(a) == len(b) and all(deep_equal(x, y, seen) for x, y in zip(a, b, strict=True))
    if isinstance(a, dict):
        return a.keys() == b.keys() and all(deep_equal(a[k], b[k], seen) for k in a)

    return a == b


# ═══════════════════════════════════════════════════════════════════════════
# running the harness
# ═══════════════════════════════════════════════════════════════════════════


@dataclass
class _Outcome:
    py_json: str | None = None
    py_encode_error: str | None = None
    js_verify_error: str | None = None
    js_json: str | None = None
    harness_error: str | None = None
    extra: dict[str, Any] = field(default_factory=dict)


@pytest.fixture(scope="session")
def interop(devalue_entry: Path) -> dict[str, _Outcome]:
    """Push every case through a single Node process and index the results."""
    outcomes = {case.name: _Outcome() for case in ALL_CASES}
    payload = []

    for case in ALL_CASES:
        py_json = None
        if case.value is not _NO_PY:
            try:
                py_json = stringify(case.value, case.py_reducers)
            except Exception as exc:  # surfaced by the individual test
                outcomes[case.name].py_encode_error = f"{type(exc).__name__}: {exc}"
        outcomes[case.name].py_json = py_json
        payload.append(
            {
                "name": case.name,
                "js": case.js,
                "pyJson": py_json,
                "jsReducers": case.js_reducers,
                "jsRevivers": case.js_revivers,
            }
        )

    proc = subprocess.run(
        ["node", "--input-type=module", "-e", _JS_HARNESS],
        input=json.dumps(payload),
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
        env={**os.environ, "DEVALUE_ENTRY": str(devalue_entry)},
    )
    if proc.returncode != 0:
        pytest.fail(f"Node harness failed (exit {proc.returncode}):\n{proc.stderr}")

    for entry in json.loads(proc.stdout):
        outcome = outcomes[entry["name"]]
        outcome.js_verify_error = entry.get("jsVerifyError")
        outcome.js_json = entry.get("jsJson")
        outcome.harness_error = entry.get("harnessError")

    return outcomes


# ═══════════════════════════════════════════════════════════════════════════
# tests
# ═══════════════════════════════════════════════════════════════════════════


class TestJsAcceptsPythonOutput:
    """Leg 1 — JS `parse` must accept what Python `stringify` produced."""

    @pytest.mark.parametrize("case", ROUND_TRIP_CASES, ids=lambda c: c.name)
    def test_js_decodes_python_output(self, case: Case, interop: dict[str, _Outcome]):
        outcome = interop[case.name]
        assert outcome.py_encode_error is None, (
            f"Python stringify failed: {outcome.py_encode_error}"
        )
        assert outcome.harness_error is None, f"harness: {outcome.harness_error}"
        assert outcome.js_verify_error is None, (
            f"JS decoded Python's output to the wrong value.\n"
            f"  Python emitted: {outcome.py_json}\n"
            f"  {outcome.js_verify_error}"
        )


class TestPythonAcceptsJsOutput:
    """Leg 2 — Python `parse` must recover the value from JS `stringify`."""

    @pytest.mark.parametrize("case", ALL_CASES, ids=lambda c: c.name)
    def test_python_decodes_js_output(self, case: Case, interop: dict[str, _Outcome]):
        outcome = interop[case.name]
        assert outcome.harness_error is None, f"harness: {outcome.harness_error}"
        assert outcome.js_json is not None, "Node produced no output for this case"

        decoded = parse(outcome.js_json, case.py_revivers)
        expected = case.expected_value()

        assert deep_equal(decoded, expected), (
            f"round trip changed the value.\n"
            f"  JS emitted: {outcome.js_json}\n"
            f"  Python decoded: {decoded!r}\n"
            f"  expected: {expected!r}"
        )
        if case.extra is not None:
            case.extra(decoded)


class TestHarness:
    """Guard rails on the harness itself, so a silent no-op cannot pass."""

    def test_every_case_ran(self, interop: dict[str, _Outcome]):
        missing = [
            name for name, o in interop.items() if o.js_json is None and o.harness_error is None
        ]
        assert not missing, f"Node returned no result for: {missing}"

    def test_round_trip_cases_exercise_both_legs(self, interop: dict[str, _Outcome]):
        no_python_leg = [c.name for c in ROUND_TRIP_CASES if interop[c.name].py_json is None]
        assert not no_python_leg, f"round-trip cases with no Python output: {no_python_leg}"

    def test_deep_equal_rejects_the_differences_that_matter(self):
        assert not deep_equal(0.0, -0.0)
        assert not deep_equal(True, 1)
        assert not deep_equal(1, 1.0)
        assert not deep_equal(Undefined, None)
        assert not deep_equal(re.compile("a"), re.compile("a", re.I))
        assert not deep_equal([1, 2], [1, 3])
        assert deep_equal(float("nan"), float("nan"))
        assert deep_equal(_cyclic_dict(), _cyclic_dict())

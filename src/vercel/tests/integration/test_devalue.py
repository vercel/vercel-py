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

from vercel._internal.devalue import Hole, JsBigInt, JsRegExp, Undefined, parse, stringify
from vercel._internal.devalue.constants import (
    HOLE,
    NAN,
    NEGATIVE_INFINITY,
    NEGATIVE_ZERO,
    POSITIVE_INFINITY,
    SPARSE,
    UNDEFINED,
)
from vercel._internal.devalue.parse import _BYTES_PER_ELEMENT

# Every tag devalue uses for an array view; all of them land as plain bytes,
# which is faithful only for `Uint8Array`.
_TYPED_ARRAY_TAGS = frozenset(_BYTES_PER_ELEMENT)

# Pins the npm fallback used when VERCEL_DEVALUE_JS is not set. It has no
# bearing on an explicit checkout, which is deliberately free to be any
# version — testing against a different one is the point of the override.
DEVALUE_VERSION = "5.9.0"

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
    # A Python pattern is accepted going out, but lands as the container.
    Case("regex_no_flags", "/a+b/", re.compile("a+b"), expected=JsRegExp("a+b", "")),
    Case(
        "regex_ignorecase",
        "/a+b/i",
        re.compile("a+b", re.IGNORECASE),
        expected=JsRegExp("a+b", "i"),
    ),
    Case(
        "regex_multiline_dotall",
        "/a.b/ms",
        re.compile("a.b", re.MULTILINE | re.DOTALL),
        expected=JsRegExp("a.b", "ms"),
    ),
    # Flags and syntax with no Python counterpart: compiling these would
    # drop `gu` and raise outright on the named group and \p{...}.
    Case("regex_js_only_flags", "/a+/gu", JsRegExp("a+", "gu")),
    Case("regex_named_group", r"/(?<y>\d{4})/", JsRegExp(r"(?<y>\d{4})", "")),
    Case("regex_unicode_property", r"/\p{Letter}/u", JsRegExp(r"\p{Letter}", "u")),
    Case("bytes", "new Uint8Array([0,1,255])", b"\x00\x01\xff"),
    Case("bytes_empty", "new Uint8Array([])", b""),
    Case("bytearray", "new Uint8Array([1,2]).buffer", bytearray(b"\x01\x02")),
    Case(
        "memoryview",
        "new Uint8Array([1,2]).buffer",
        memoryview(b"\x01\x02"),
        expected=bytearray(b"\x01\x02"),
    ),
    # ── integers across the JS safe boundary ───────────────────────────────
    Case("safe_int_max", "9007199254740991", 2**53 - 1),
    Case("safe_int_min", "-9007199254740991", -(2**53) + 1),
    # A plain int beyond the safe range has to come back as a bigint.
    Case("unsafe_int_positive", "9007199254740993n", 2**53 + 1, expected=JsBigInt(2**53 + 1)),
    Case(
        "unsafe_int_negative",
        "-9007199254740993n",
        -(2**53) - 1,
        expected=JsBigInt(-(2**53) - 1),
    ),
    Case(
        "unsafe_int_huge",
        "123456789012345678901234567890n",
        123456789012345678901234567890,
        expected=JsBigInt(123456789012345678901234567890),
    ),
    # An explicit bigint keeps its tag however small it is.
    Case("bigint_small", "1n", JsBigInt(1)),
    Case("bigint_zero", "0n", JsBigInt(0)),
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
    Case("js_arraybuffer", "new Uint8Array([9,8]).buffer", expected=bytearray(b"\x09\x08")),
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
        expected=[Hole, "a", Hole],
    ),
    Case(
        "js_sparse_array",
        '(() => { const a = []; a[5] = "x"; return a; })()',
        expected=[Hole] * 5 + ["x"],
    ),
    Case("js_explicit_undefined", "[undefined, 1]", expected=[Undefined, 1]),
    Case(
        "js_bigint",
        "123456789012345678901234567890n",
        expected=JsBigInt(123456789012345678901234567890),
    ),
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
        assert not deep_equal(Hole, Undefined)
        assert not deep_equal([Hole], [Undefined])
        assert not deep_equal(JsBigInt(1), 1)
        assert not deep_equal(JsRegExp("a", "g"), JsRegExp("a", ""))
        assert deep_equal(JsBigInt(1), JsBigInt(1))
        assert deep_equal([Hole], [Hole])
        assert not deep_equal(re.compile("a"), re.compile("a", re.I))
        assert not deep_equal([1, 2], [1, 3])
        assert deep_equal(float("nan"), float("nan"))
        assert deep_equal(_cyclic_dict(), _cyclic_dict())


# ═══════════════════════════════════════════════════════════════════════════
# upstream's own corpus
# ═══════════════════════════════════════════════════════════════════════════
#
# The cases above are ones we thought of. Upstream's fixtures encode edge cases
# someone else thought of, which is the more valuable half. Each fixture ships
# the exact wire string devalue produces, so the assertion is: parse it, encode
# it again, and have JS confirm the two strings denote the same value.
#
# The npm tarball ships no `test/`, so this needs a devalue *checkout* via
# VERCEL_DEVALUE_JS and skips otherwise. Nothing is lost by running the whole
# suite against a checkout: every file the tarball actually ships for execution
# (`index.js` and `src/`) is byte-identical to the tag.

# Constants upstream interpolates into its expected wire strings.
_CORPUS_SUBSTITUTIONS = {
    "consts.UNDEFINED": str(UNDEFINED),
    "consts.HOLE": str(HOLE),
    "consts.NAN": str(NAN),
    "consts.POSITIVE_INFINITY": str(POSITIVE_INFINITY),
    "consts.NEGATIVE_INFINITY": str(NEGATIVE_INFINITY),
    "consts.NEGATIVE_ZERO": str(NEGATIVE_ZERO),
    "consts.SPARSE": str(SPARSE),
    "JSON.stringify('\U0001d306')": '"\U0001d306"',
}

# Wire tags whose value cannot survive a Python round trip, and why. Anything
# NOT matching one of these must round-trip exactly; anything matching one must
# still fail. Both directions are asserted so the inventory cannot go stale —
# closing a gap here breaks the test until the entry is removed.
_KNOWN_LOSSY: dict[str, str] = {
    '["Object"': "boxed primitives are unboxed",
    '["Map"': "Map becomes dict",
    '["URL"': "URL becomes str",
    '["URLSearchParams"': "URLSearchParams becomes str",
    '["null"': "null-prototype object becomes dict",
    '["Date",""': "an invalid Date becomes None",
    '["DataView"': "DataView becomes bytes",
    '["Temporal.': "Temporal values become str",
    # `Uint8Array` is absent: `bytes` is exactly it, so it round-trips.
    **{f'["{tag}"': f"{tag} becomes bytes" for tag in _TYPED_ARRAY_TAGS - {"Uint8Array"}},
}

# Needs revivers we deliberately do not supply, or a Python container that
# cannot hold the key at all.
_CORPUS_SKIP = {
    "Custom type": "exercises caller-supplied revivers",
    "Function wrapped in custom type": "exercises caller-supplied revivers",
    "Function in nested structure": "exercises caller-supplied revivers",
    "Set (cyclical)": "a Python set cannot contain a set",
    "Map key (repetition)": "Python dict cannot hold an unhashable key",
    "Map keys (interlinked)": "Python dict cannot hold an unhashable key",
}


def _unescape_js_literal(text: str, quote: str) -> str:
    """Resolve the escapes a JS source-level string literal used."""
    if quote == "`":
        return text
    simple = {"n": "\n", "t": "\t", "r": "\r", "b": "\b", "f": "\f", "0": "\0"}

    def replace(match: re.Match[str]) -> str:
        body = match.group(1)
        if body[0] in "ux" and len(body) > 1:
            return chr(int(body[1:], 16))
        return simple.get(body, body)

    return re.sub(r"\\(u[0-9a-fA-F]{4}|x[0-9a-fA-F]{2}|.)", replace, text)


@dataclass(frozen=True)
class _UpstreamCase:
    name: str
    wire: str
    """The exact devalue output upstream expects."""

    expects_error: bool = False
    """Upstream expects `parse` to reject this."""

    message: str | None = None
    """The expected message, when upstream states it on one line."""

    needs_reviver: bool = False


def _load_upstream_cases(test_file: Path) -> list[_UpstreamCase]:
    """Pull every `{name, json, …}` case out of upstream's test file.

    Deliberately indentation-agnostic: cases live at several depths (some
    fixtures are built inside an IIFE, and the error array sits a level
    shallower than `fixtures`), and keying off a fixed indent silently skipped
    whole groups.
    """
    field = re.compile(r"^(\t+)(name|json|message|revivers):\s*(.*)$")
    literal = re.compile(r"""(['"`])(.*)\1,?$""")

    cases: list[_UpstreamCase] = []
    pending: dict[str, str | bool] = {}
    depth: str | None = None

    def flush() -> None:
        name, wire = pending.get("name"), pending.get("json")
        if isinstance(name, str) and isinstance(wire, str):
            for token, replacement in _CORPUS_SUBSTITUTIONS.items():
                wire = wire.replace("${" + token + "}", replacement)
            assert "${" not in wire, (
                f"unmodelled interpolation in upstream case {name!r}: {wire} — add "
                "it to _CORPUS_SUBSTITUTIONS rather than skipping the case"
            )
            message = pending.get("message")
            cases.append(
                _UpstreamCase(
                    name=name,
                    wire=wire,
                    expects_error=bool(pending.get("has_message")),
                    message=message if isinstance(message, str) else None,
                    needs_reviver=bool(pending.get("revivers")),
                )
            )
        pending.clear()

    for line in test_file.read_text().split("\n"):
        match = field.match(line)
        if match is None:
            continue
        indent, key, rest = match.groups()
        if key == "name" or indent != depth:
            flush()
            depth = indent
        if key == "revivers":
            pending["revivers"] = True
            continue
        if key == "message":
            # Upstream sometimes builds the message across several lines (it
            # varies by Node version), so presence is what marks an error case.
            pending["has_message"] = True
        value = literal.match(rest.rstrip())
        if value is not None:
            pending[key] = _unescape_js_literal(value.group(2), value.group(1))
    flush()
    return cases


@pytest.fixture(scope="session")
def upstream_corpus(devalue_entry: Path) -> list[tuple[str, str]]:
    checkout = _corpus_checkout(devalue_entry)
    if checkout is None:
        pytest.skip(
            "upstream fixtures need a devalue checkout (the npm tarball ships no "
            "tests); point VERCEL_DEVALUE_JS at one"
        )
    cases = _load_upstream_cases(checkout / "test" / "index.test.js")
    corpus = [(c.name, c.wire) for c in cases if not c.expects_error]
    assert len(corpus) > 90, f"suspiciously few fixtures extracted: {len(corpus)}"
    return corpus


def _corpus_checkout(devalue_entry: Path) -> Path | None:
    """The checkout the fixtures live in, if we are running against one."""
    checkout = devalue_entry.parent
    return checkout if (checkout / "test" / "index.test.js").is_file() else None


@pytest.fixture(scope="session")
def upstream_invalid(devalue_entry: Path) -> list[tuple[str, str, str | None, bool]]:
    checkout = _corpus_checkout(devalue_entry)
    if checkout is None:
        pytest.skip("upstream error cases need a devalue checkout")
    cases = _load_upstream_cases(checkout / "test" / "index.test.js")
    invalid = [
        # An error from the JSON layer rather than from devalue: the wording
        # is language-specific, so only require that it is rejected.
        (c.name, c.wire, None if _is_json_level(c.wire) else c.message, c.needs_reviver)
        for c in cases
        if c.expects_error
    ]
    assert len(invalid) > 25, f"suspiciously few error cases extracted: {len(invalid)}"
    return invalid


def _is_json_level(wire: str) -> bool:
    try:
        json.loads(wire)
    except json.JSONDecodeError:
        return True
    return False


@pytest.fixture(scope="session")
def corpus_results(
    devalue_entry: Path, upstream_corpus: list[tuple[str, str]]
) -> dict[str, dict[str, Any]]:
    """Re-encode each fixture, then have JS compare the two wire strings."""
    pairs, outcomes = [], {}
    for name, wire in upstream_corpus:
        if name in _CORPUS_SKIP:
            continue
        try:
            outcomes[name] = {"wire": wire, "reencoded": stringify(parse(wire))}
            pairs.append({"name": name, "a": wire, "b": outcomes[name]["reencoded"]})
        except Exception as exc:
            outcomes[name] = {"wire": wire, "py_error": f"{type(exc).__name__}: {exc}"}

    proc = subprocess.run(
        ["node", "--input-type=module", "-e", _CORPUS_HARNESS],
        input=json.dumps(pairs),
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
        env={**os.environ, "DEVALUE_ENTRY": str(devalue_entry)},
    )
    if proc.returncode != 0:
        pytest.fail(f"Node corpus harness failed (exit {proc.returncode}):\n{proc.stderr}")
    for entry in json.loads(proc.stdout):
        outcomes[entry["name"]].update(entry)
    return outcomes


_CORPUS_HARNESS = """
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { pathToFileURL } from 'node:url';

const { parse } = await import(pathToFileURL(process.env.DEVALUE_ENTRY).href);
const results = [];

for (const { name, a, b } of JSON.parse(readFileSync(0, 'utf8'))) {
  const result = { name };
  try {
    assert.deepStrictEqual(parse(b), parse(a));
    result.equal = true;
  } catch (error) {
    result.equal = false;
    result.detail = (error.message || String(error)).split('\\n').slice(0, 4).join(' | ');
  }
  results.push(result);
}

process.stdout.write(JSON.stringify(results));
"""


def _lossy_reason(wire: str) -> str | None:
    for tag, reason in _KNOWN_LOSSY.items():
        if tag in wire:
            return reason
    return None


class TestUpstreamCorpus:
    def test_corpus_was_extracted(self, upstream_corpus):
        assert len(upstream_corpus) > 50

    def test_every_fixture_round_trips_unless_known_lossy(self, corpus_results):
        unexpected_loss, unexpected_success, errors = [], [], []
        for name, outcome in corpus_results.items():
            reason = _lossy_reason(outcome["wire"])
            if "py_error" in outcome:
                if reason is None:
                    errors.append(f"{name}: {outcome['py_error']}  ({outcome['wire'][:60]})")
                continue
            if outcome["equal"] and reason is not None:
                unexpected_success.append(f"{name}: no longer lossy ({reason}) — drop it")
            elif not outcome["equal"] and reason is None:
                unexpected_loss.append(
                    f"{name}: {outcome['wire'][:52]} -> {outcome['reencoded'][:52]}"
                    f"  [{outcome.get('detail', '')[:70]}]"
                )
        assert not errors, "parse/stringify raised on upstream fixtures:\n  " + "\n  ".join(errors)
        assert not unexpected_loss, "fixtures stopped round-tripping:\n  " + "\n  ".join(
            unexpected_loss
        )
        assert not unexpected_success, "_KNOWN_LOSSY is stale:\n  " + "\n  ".join(
            unexpected_success
        )

    def test_upstream_error_cases_are_rejected(self, upstream_invalid):
        """Rejecting malformed input the same way matters as much as accepting
        well-formed input: several of these cases came from upstream security
        fixes, so the list grows and is worth tracking rather than copying."""
        wrong = []
        for name, wire, message, needs_reviver in upstream_invalid:
            revivers = {"Custom": lambda value: value} if needs_reviver else None
            try:
                parse(wire, revivers)
            except Exception as exc:  # noqa: BLE001 - any rejection is enough
                if message is not None and message not in str(exc):
                    wrong.append(f"{name}: expected {message!r}, got {str(exc)[:60]!r}")
                continue
            wrong.append(f"{name}: accepted {wire[:50]!r} but upstream rejects it")
        assert not wrong, "upstream error cases handled differently:\n  " + "\n  ".join(wrong)

"""Serialize Python values into the devalue JSON wire format."""

from __future__ import annotations

import base64
import math
import re as re_module
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

from .constants import (
    HOLE,
    MAX_SAFE_INTEGER,
    NAN,
    NEGATIVE_INFINITY,
    NEGATIVE_ZERO,
    POSITIVE_INFINITY,
    UNDEFINED,
)
from .utils import (
    DevalueError,
    Hole,
    JsBigInt,
    JsRegExp,
    Undefined,
    py_flags_to_js,
    stringify_key,
    stringify_string,
)


def stringify(value: Any, reducers: dict[str, Callable] | None = None) -> str:
    """Serialize *value* into a JSON string compatible with JS ``devalue.parse``.

    Supports: ``None``, ``bool``, ``int``, ``float`` (including NaN / ±Inf /
    −0), ``str``, ``list``, ``tuple``, ``dict`` (string keys only), ``set``,
    ``frozenset``, ``datetime``, ``re.Pattern``, ``bytes`` (as a
    ``Uint8Array``), ``bytearray`` and ``memoryview`` (as an ``ArrayBuffer``),
    and the ``Undefined`` sentinel.

    Cyclic and repeated references are handled automatically.  Integers
    outside the JS safe range are emitted as ``["BigInt", …]``, since a bare
    JSON number would not survive the trip through a JS ``number``.

    *reducers* is an optional dict mapping type tags to functions.  Each
    function receives a value and should return a replacement value for the
    values it handles, or a falsy one to decline.  Truthiness follows JS, not
    Python: ``{}``, ``[]`` and ``set()`` claim the value, while ``NaN`` and
    ``-0.0`` decline.
    """
    stringified: dict[int, str] = {}
    # Maps a key from `_make_key` to `(index, value)`.  JS keys its index Map
    # by the value itself, which gives objects reference identity and keeps
    # them alive.  A dict cannot do that — it would merge distinct-but-equal
    # objects and reject unhashable ones — so object keys use `id()` instead,
    # and each entry holds the value so that its address cannot be recycled
    # while the key is live.
    indexes: dict[Any, tuple[int, Any]] = {}
    custom: list[tuple[str, Callable]] = []
    if reducers:
        for key in reducers:
            custom.append((key, reducers[key]))
    keys: list[str] = []
    p = 0

    def _make_key(thing: Any) -> Any:
        """Build a dict key that distinguishes ``bool`` from ``int``."""
        if isinstance(thing, bool):
            return ("b", thing)
        if thing is None:
            return ("n",)
        if thing is Undefined:
            return ("u",)
        if isinstance(thing, JsBigInt):
            return ("bigint", int(thing))
        if isinstance(thing, int):
            return ("i", thing)
        if isinstance(thing, float):
            return ("f", thing)
        if isinstance(thing, str):
            return ("s", thing)
        return ("o", id(thing))

    def flatten(thing: Any, index: int | None = None) -> int:
        nonlocal p

        # --- special values → negative sentinel constants ---
        if thing is Undefined:
            return UNDEFINED
        if thing is Hole:
            # Only an array slot can be a hole; the array branch handles it.
            raise DevalueError("Cannot stringify a hole outside an array", keys, thing, value)
        if isinstance(thing, float):
            if math.isnan(thing):
                return NAN
            if thing == float("inf"):
                return POSITIVE_INFINITY
            if thing == float("-inf"):
                return NEGATIVE_INFINITY
            if thing == 0.0 and math.copysign(1.0, thing) < 0:
                return NEGATIVE_ZERO

        # --- already indexed → reuse ---
        key = _make_key(thing)
        entry = indexes.get(key)
        if entry is not None:
            return entry[0]

        # --- assign a fresh index ---
        if index is None:
            index = p
            p += 1
        indexes[key] = (index, thing)

        # --- custom reducers ---
        for reducer_key, fn in custom:
            reduced = fn(thing)
            if _is_js_truthy(reduced):
                stringified[index] = f'["{reducer_key}",{flatten(reduced)}]'
                return index

        # --- callable (functions, lambdas) ---
        if callable(thing) and not isinstance(thing, type):
            raise DevalueError("Cannot stringify a function", keys, thing, value)

        # --- primitives ---
        if thing is None or isinstance(thing, (bool, int, float, str)):
            stringified[index] = _stringify_primitive(thing)
            return index

        # --- list / tuple → Array ---
        if isinstance(thing, (list, tuple)):
            parts = ["["]
            for i, item in enumerate(thing):
                if i > 0:
                    parts.append(",")
                if item is Hole:
                    parts.append(str(HOLE))
                    continue
                keys.append(f"[{i}]")
                parts.append(str(flatten(item)))
                keys.pop()
            parts.append("]")
            stringified[index] = "".join(parts)
            return index

        # --- dict → Object ---
        if isinstance(thing, dict):
            for k in thing:
                if not isinstance(k, str):
                    raise DevalueError(
                        "Cannot stringify objects with non-string keys",
                        keys,
                        thing,
                        value,
                    )
                if k == "__proto__":
                    raise DevalueError(
                        "Cannot stringify objects with __proto__ keys",
                        keys,
                        thing,
                        value,
                    )
            parts = ["{"]
            started = False
            for k, v in thing.items():
                if started:
                    parts.append(",")
                started = True
                keys.append(stringify_key(k))
                parts.append(f"{stringify_string(k)}:{flatten(v)}")
                keys.pop()
            parts.append("}")
            stringified[index] = "".join(parts)
            return index

        # --- set / frozenset → Set ---
        if isinstance(thing, (set, frozenset)):
            parts = ['["Set"']
            for item in thing:
                parts.append(f",{flatten(item)}")
            parts.append("]")
            stringified[index] = "".join(parts)
            return index

        # --- datetime → Date ---
        if isinstance(thing, datetime):
            if thing.tzinfo is not None:
                utc_dt = thing.astimezone(timezone.utc)
            else:
                utc_dt = thing
            ms = utc_dt.microsecond // 1000
            iso = utc_dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{ms:03d}Z"
            stringified[index] = f'["Date","{iso}"]'
            return index

        # --- JsRegExp / re.Pattern → RegExp ---
        if isinstance(thing, (JsRegExp, re_module.Pattern)):
            if isinstance(thing, JsRegExp):
                source, flags_str = thing.source, thing.flags
            else:
                # Best effort: a Python pattern is not a JS one, so this can
                # produce a source the far side refuses to compile.
                source = thing.pattern
                flags_str = py_flags_to_js(thing.flags)
            if flags_str:
                stringified[index] = f'["RegExp",{stringify_string(source)},"{flags_str}"]'
            else:
                stringified[index] = f'["RegExp",{stringify_string(source)}]'
            return index

        # --- bytearray / memoryview → ArrayBuffer ---
        if isinstance(thing, (bytearray, memoryview)):
            b64 = base64.b64encode(bytes(thing)).decode("ascii")
            stringified[index] = f'["ArrayBuffer","{b64}"]'
            return index

        # --- bytes → Uint8Array over its own ArrayBuffer ---
        # `bytes` gets the view rather than the bare buffer because that is what
        # the far side wants: a `Uint8Array` can be piped straight into a
        # `Response`, an `ArrayBuffer` cannot.
        if isinstance(thing, bytes):
            b64 = base64.b64encode(thing).decode("ascii")
            # Allocated here rather than through `flatten`, which would need a
            # temporary object to key on and would offer it to the reducers.
            buffer_index = p
            p += 1
            stringified[buffer_index] = f'["ArrayBuffer","{b64}"]'
            # No offset or length: upstream emits those only for a view narrower
            # than its buffer, and this one never is.
            stringified[index] = f'["Uint8Array",{buffer_index}]'
            return index

        raise DevalueError("Cannot stringify arbitrary non-POJOs", keys, thing, value)

    idx = flatten(value)

    # Special value (encoded as a negative number with no array wrapper)
    if idx < 0:
        return str(idx)

    parts = [stringified[i] for i in range(p)]
    return "[" + ",".join(parts) + "]"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _is_js_truthy(thing: Any) -> bool:
    """Mirror JS truthiness, which decides whether a reducer claimed a value.

    Differs from Python's in both directions: empty containers are truthy in
    JS, while ``NaN`` and ``-0.0`` are falsy.
    """
    if thing is None or thing is Undefined or thing is False:
        return False
    if isinstance(thing, str):
        return thing != ""
    if isinstance(thing, bool):
        return thing
    if isinstance(thing, (int, float)):
        return not (thing == 0 or (isinstance(thing, float) and math.isnan(thing)))
    return True


def _stringify_primitive(thing: Any) -> str:
    """Return the JSON-compatible representation of a Python primitive."""
    if isinstance(thing, str):
        return stringify_string(thing)
    if thing is None:
        return "null"
    # bool MUST be checked before int (bool is a subclass of int)
    if isinstance(thing, bool):
        return "true" if thing else "false"
    if isinstance(thing, JsBigInt):
        return f'["BigInt","{int(thing)}"]'
    if isinstance(thing, int):
        # A JS `number` cannot hold these exactly, so emitting a bare JSON
        # number would silently round-trip as a different value.  Any integer
        # this large is a `bigint` on the JS side anyway.
        if not -MAX_SAFE_INTEGER <= thing <= MAX_SAFE_INTEGER:
            return f'["BigInt","{thing}"]'
        return str(thing)
    if isinstance(thing, float):
        # NaN / Inf / -Inf / -0 are handled before reaching here
        return repr(thing)
    return str(thing)  # pragma: no cover

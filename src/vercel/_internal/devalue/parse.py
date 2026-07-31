"""Parse devalue JSON wire format back into Python values."""

from __future__ import annotations

import base64
import json
from collections.abc import Callable
from datetime import datetime
from typing import Any

from .constants import (
    HOLE,
    MAX_SPARSE_ARRAY_LENGTH,
    NAN,
    NEGATIVE_INFINITY,
    NEGATIVE_ZERO,
    POSITIVE_INFINITY,
    SPARSE,
    UNDEFINED,
)
from .utils import (
    Hole,
    JsBigInt,
    JsRegExp,
    Undefined,
    is_valid_array_index,
    is_valid_array_len,
)


def parse(serialized: str, revivers: dict[str, Callable] | None = None) -> Any:
    """Deserialize a string produced by ``devalue.stringify`` (JS or Python).

    *revivers* is an optional dict mapping custom type tags to functions
    that receive the hydrated inner value and return the final object.
    """
    return unflatten(json.loads(serialized), revivers)


def unflatten(parsed: Any, revivers: dict[str, Callable] | None = None) -> Any:
    """Hydrate an already-JSON-parsed value produced by ``devalue.stringify``.

    Accepts either a negative integer (for special top-level values such as
    ``undefined`` or ``NaN``) or a list (the flattened representation).

    Input is untrusted: an index that does not address a slot hydrates to
    ``Undefined`` (as it does in JS), and a sparse array declaring more than
    ``MAX_SPARSE_ARRAY_LENGTH`` slots is rejected rather than allocated.
    """
    if isinstance(parsed, (int, float)) and not isinstance(parsed, bool):
        return _hydrate_toplevel(parsed)

    if not isinstance(parsed, list) or len(parsed) == 0:
        raise ValueError("Invalid input")

    values: list[Any] = parsed
    hydrated: dict[int, Any] = {}
    hydrating: set[int] | None = None

    def hydrate(index: Any, standalone: bool = False) -> Any:
        nonlocal hydrating

        # Special sentinel constants
        if index == UNDEFINED:
            return Undefined
        if index == NAN:
            return float("nan")
        if index == POSITIVE_INFINITY:
            return float("inf")
        if index == NEGATIVE_INFINITY:
            return float("-inf")
        if index == NEGATIVE_ZERO:
            return -0.0

        if standalone or not isinstance(index, (int, float)) or isinstance(index, bool):
            raise ValueError("Invalid input")

        # A float index addresses no slot; JS reads `undefined` from the gap
        # rather than truncating towards one. Checked before the cache
        # because such an index resolves to `Undefined` either way.
        if not isinstance(index, int):
            return Undefined

        if index in hydrated:
            return hydrated[index]

        # JS reads out-of-range indexes off the end of the array and gets
        # `undefined`.  Python would wrap a negative index around to a real
        # element (silently returning the wrong value) or raise IndexError.
        if index < 0 or index >= len(values):
            hydrated[index] = Undefined
            return Undefined

        value = values[index]

        # --- scalar (not a dict or list) → store directly ---
        if not isinstance(value, (dict, list)):
            hydrated[index] = value
        # --- list-encoded values ---
        elif isinstance(value, list):
            if len(value) > 0 and isinstance(value[0], str):
                _hydrate_typed(index, value, hydrate, hydrated, values, revivers, hydrating)
                # hydrating might have been mutated – re-read from the closure
            elif len(value) > 0 and value[0] == SPARSE:
                _hydrate_sparse(index, value, hydrate, hydrated)
            else:
                _hydrate_dense(index, value, hydrate, hydrated)
        # --- plain dict ---
        else:
            obj: dict[str, Any] = {}
            hydrated[index] = obj
            for key in value:
                if key == "__proto__":
                    raise ValueError("Cannot parse an object with a `__proto__` property")
                obj[key] = hydrate(value[key])

        return hydrated[index]

    # ---------- typed value dispatcher ----------

    def _hydrate_typed(
        index: int,
        value: list,
        hydrate: Callable,
        hydrated: dict,
        values: list,
        revivers: dict | None,
        outer_hydrating: set | None,
    ) -> None:
        nonlocal hydrating
        type_name: str = value[0]

        # --- custom reviver? ---
        reviver = revivers.get(type_name) if revivers else None
        if reviver is not None:
            i = value[1]
            if not isinstance(i, (int, float)) or isinstance(i, bool):
                values.append(value[1])
                i = len(values) - 1

            # If the payload is already hydrated, its recursion has already
            # terminated (e.g. a self-referential object cached itself before
            # following its own back-reference), so revive it directly.
            # Falling through to the `hydrating` guard here would wrongly
            # reject a valid cycle.  An actually infinite payload (e.g.
            # `[["Custom", 0]]`) is never cached, so it still hits the guard.
            if i in hydrated:
                hydrated[index] = reviver(hydrated[i])
                return

            if hydrating is None:
                hydrating = set()

            if i in hydrating:
                raise ValueError("Invalid circular reference")

            hydrating.add(i)
            hydrated[index] = reviver(hydrate(i))
            hydrating.discard(i)
            return

        # --- built-in types ---
        if type_name == "Date":
            date_str = value[1]
            if not date_str:
                hydrated[index] = None
            else:
                hydrated[index] = _parse_iso_date(date_str)

        elif type_name == "Set":
            s: set[Any] = set()
            hydrated[index] = s
            for i in range(1, len(value)):
                s.add(hydrate(value[i]))

        elif type_name == "Map":
            d: dict[Any, Any] = {}
            hydrated[index] = d
            for i in range(1, len(value), 2):
                k = hydrate(value[i])
                v = hydrate(value[i + 1])
                d[k] = v

        elif type_name == "RegExp":
            pattern = value[1]
            flags_str = value[2] if len(value) > 2 else ""
            hydrated[index] = JsRegExp(pattern, flags_str)

        elif type_name == "Object":
            wrapped_index = value[1]
            # A boxed special value carries a negative sentinel here (`-3` for
            # NaN, `-6` for -0, …), and JS reads an out-of-range index as
            # `undefined` rather than wrapping around to a real element.
            wrapped_raw = (
                values[wrapped_index]
                if isinstance(wrapped_index, int) and 0 <= wrapped_index < len(values)
                else None
            )
            if isinstance(wrapped_raw, (dict, list)) and not (
                isinstance(wrapped_raw, list)
                and len(wrapped_raw) > 0
                and wrapped_raw[0] == "BigInt"
            ):
                raise ValueError("Invalid input")
            hydrated[index] = hydrate(wrapped_index)

        elif type_name == "BigInt":
            hydrated[index] = JsBigInt(value[1])

        elif type_name == "null":
            obj: dict[str, Any] = {}
            hydrated[index] = obj
            for i in range(1, len(value), 2):
                k = value[i]
                if k == "__proto__":
                    raise ValueError("Cannot parse an object with a `__proto__` property")
                obj[k] = hydrate(value[i + 1])

        elif type_name == "ArrayBuffer":
            b64 = value[1]
            if not isinstance(b64, str):
                raise ValueError("Invalid ArrayBuffer encoding")
            hydrated[index] = base64.b64decode(b64)

        elif type_name in _TYPED_ARRAY_TYPES:
            ref_idx = value[1]
            ref_val = (
                values[ref_idx] if isinstance(ref_idx, int) and 0 <= ref_idx < len(values) else None
            )
            if not (isinstance(ref_val, list) and len(ref_val) > 0 and ref_val[0] == "ArrayBuffer"):
                raise ValueError("Invalid data")
            buf = hydrate(value[1])
            if len(value) > 2:
                byte_offset = value[2]
                elem_count = value[3]
                if type_name == "DataView":
                    hydrated[index] = buf[byte_offset : byte_offset + elem_count]
                else:
                    bpe = _BYTES_PER_ELEMENT.get(type_name, 1)
                    hydrated[index] = buf[byte_offset : byte_offset + elem_count * bpe]
            else:
                hydrated[index] = buf

        elif type_name == "URL":
            hydrated[index] = value[1]

        elif type_name == "URLSearchParams":
            hydrated[index] = value[1]

        elif type_name.startswith("Temporal."):
            hydrated[index] = value[1]

        else:
            raise ValueError(f"Unknown type {type_name}")

    return hydrate(0)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_BYTES_PER_ELEMENT: dict[str, int] = {
    "Int8Array": 1,
    "Uint8Array": 1,
    "Uint8ClampedArray": 1,
    "Int16Array": 2,
    "Uint16Array": 2,
    "Float16Array": 2,
    "Int32Array": 4,
    "Uint32Array": 4,
    "Float32Array": 4,
    "Float64Array": 8,
    "BigInt64Array": 8,
    "BigUint64Array": 8,
}

_TYPED_ARRAY_TYPES: set[str] = set(_BYTES_PER_ELEMENT) | {"DataView"}


def _hydrate_toplevel(n: int | float) -> Any:
    """Return the Python equivalent for a top-level special constant."""
    if n == UNDEFINED:
        return Undefined
    if n == NAN:
        return float("nan")
    if n == POSITIVE_INFINITY:
        return float("inf")
    if n == NEGATIVE_INFINITY:
        return float("-inf")
    if n == NEGATIVE_ZERO:
        return -0.0
    raise ValueError("Invalid input")


def _hydrate_sparse(
    index: int,
    value: list,
    hydrate: Callable,
    hydrated: dict,
) -> None:
    """Hydrate a sparse-array encoding ``[SPARSE, length, idx, val, …]``."""
    length = value[1]
    if not is_valid_array_len(length):
        raise ValueError("Invalid input")

    if length > MAX_SPARSE_ARRAY_LENGTH:
        # `length` comes from the input rather than being bounded by it, so
        # honouring it would let a few bytes of payload allocate arbitrary
        # memory.  See MAX_SPARSE_ARRAY_LENGTH for why Python cannot do what
        # JS does here.
        raise ValueError(
            f"Sparse array length {length} exceeds the maximum of {MAX_SPARSE_ARRAY_LENGTH}"
        )

    array: list[Any] = [Hole] * length
    hydrated[index] = array
    for i in range(2, len(value), 2):
        idx = value[i]
        if not is_valid_array_index(idx) or idx >= length:
            raise ValueError("Invalid input")
        array[idx] = hydrate(value[i + 1])


def _hydrate_dense(
    index: int,
    value: list,
    hydrate: Callable,
    hydrated: dict,
) -> None:
    """Hydrate a dense array (may contain ``HOLE`` sentinels)."""
    array: list[Any] = [Hole] * len(value)
    hydrated[index] = array
    for i, n in enumerate(value):
        if n == HOLE:
            continue  # leave as Hole
        array[i] = hydrate(n)


def _parse_iso_date(s: str) -> datetime:
    """Parse a JS ``Date.toISOString()`` string into a Python datetime."""
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return datetime.fromisoformat(s)

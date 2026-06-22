"""Shared utilities for the devalue serialization library."""

from __future__ import annotations

import json
import math
import re
from typing import TypeGuard

from .constants import MAX_ARRAY_INDEX, MAX_ARRAY_LEN


class _UndefinedType:
    """Singleton sentinel representing JavaScript's ``undefined``."""

    _instance: _UndefinedType | None = None

    def __new__(cls) -> _UndefinedType:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self) -> str:
        return "Undefined"

    def __bool__(self) -> bool:
        return False

    def __hash__(self) -> int:
        return hash("Undefined")


Undefined = _UndefinedType()


class DevalueError(Exception):
    """Error raised when a value cannot be serialized by devalue."""

    def __init__(
        self,
        message: str,
        keys: list[str],
        value: object = None,
        root: object = None,
    ) -> None:
        super().__init__(message)
        self.name = "DevalueError"
        self.path = "".join(keys)
        self.value = value
        self.root = root


def stringify_string(s: str) -> str:
    """Escape and double-quote a string, matching JS devalue's escaping.

    Escapes ``<``, ``\\``, quotes, control characters, and U+2028/U+2029
    to produce output safe for embedding in ``<script>`` tags.
    """
    parts: list[str] = []
    for ch in s:
        cp = ord(ch)
        if ch == '"':
            parts.append('\\"')
        elif ch == "<":
            parts.append("\\u003C")
        elif ch == "\\":
            parts.append("\\\\")
        elif ch == "\n":
            parts.append("\\n")
        elif ch == "\r":
            parts.append("\\r")
        elif ch == "\t":
            parts.append("\\t")
        elif ch == "\b":
            parts.append("\\b")
        elif ch == "\f":
            parts.append("\\f")
        elif ch == "\u2028":
            parts.append("\\u2028")
        elif ch == "\u2029":
            parts.append("\\u2029")
        elif cp < 0x20:
            parts.append(f"\\u{cp:04x}")
        else:
            parts.append(ch)
    return '"' + "".join(parts) + '"'


_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_$][a-zA-Z_$0-9]*$")


def stringify_key(key: str) -> str:
    """Format an object key for error path display."""
    if _IDENTIFIER_RE.match(key):
        return "." + key
    return "[" + json.dumps(key) + "]"


def is_js_integer(n: object) -> TypeGuard[int | float]:
    """Mirror of JS ``Number.isInteger``.

    JSON numbers reach Python as ``int`` or ``float``, so ``3.0`` must be
    accepted as an integer the way it is in JS.  ``bool`` is excluded because
    it is not a number on the JS side.
    """
    if isinstance(n, bool):
        return False
    if isinstance(n, int):
        return True
    if isinstance(n, float):
        return math.isfinite(n) and n.is_integer()
    return False


def is_valid_array_index(n: object) -> bool:
    """Mirror of JS ``is_valid_array_index``."""
    return is_js_integer(n) and 0 <= n <= MAX_ARRAY_INDEX


def is_valid_array_len(n: object) -> bool:
    """Mirror of JS ``is_valid_array_len``."""
    return is_js_integer(n) and 0 <= n <= MAX_ARRAY_LEN

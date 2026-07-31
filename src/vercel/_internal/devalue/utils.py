"""Shared utilities for the devalue serialization library."""

from __future__ import annotations

import json
import re

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


class _HoleType:
    """Singleton sentinel for a missing array slot.

    JS distinguishes an array *hole* from a slot holding ``undefined`` — the
    former is absent from ``Object.keys`` and devalue gives it its own wire
    sentinel — so collapsing both onto ``Undefined`` would silently rewrite
    ``[, 1]`` as ``[undefined, 1]`` on the way back out.
    """

    _instance: _HoleType | None = None

    def __new__(cls) -> _HoleType:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self) -> str:
        return "Hole"

    def __bool__(self) -> bool:
        return False

    def __hash__(self) -> int:
        return hash("Hole")


Hole = _HoleType()


class JsBigInt(int):
    """A JavaScript ``bigint``.

    Subclasses ``int`` so it compares and computes like one; the distinct type
    only records that the far side used ``bigint`` rather than ``number``.
    Without it every small ``bigint`` would come back as a plain ``number``,
    because the port can otherwise only infer "must be a bigint" from an
    integer being too large for a JS ``number``.
    """

    __slots__ = ()

    def __repr__(self) -> str:
        return f"JsBigInt({int(self)})"


class JsRegExp:
    """A JavaScript regular expression, kept as source plus flags.

    Deliberately *not* compiled with ``re``. The two flavours are neither
    syntax- nor semantics-compatible: patterns JS accepts can fail to compile
    in Python (``(?<name>…)``, ``\\p{…}``, ``\\cX``, ``[]``), and some that do
    compile mean different things (``\\d`` is ASCII-only in JS, ``a{,3}`` is a
    literal in JS but a quantifier in Python). Holding the source verbatim
    keeps the value round-trippable and keeps ``parse`` from raising on input
    the far side considers valid.

    Call :meth:`compile` to opt into a Python pattern, accepting those caveats.
    """

    __slots__ = ("flags", "source")

    def __init__(self, source: str, flags: str = "") -> None:
        self.source = source
        self.flags = flags

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, JsRegExp)
            and other.source == self.source
            and other.flags == self.flags
        )

    def __hash__(self) -> int:
        return hash((self.source, self.flags))

    def __repr__(self) -> str:
        return f"JsRegExp({self.source!r}, {self.flags!r})"

    def compile(self) -> re.Pattern[str]:
        """Compile to a Python pattern; may raise ``re.error``, may differ."""
        return re.compile(self.source, js_flags_to_py(self.flags))


def js_flags_to_py(flags: str) -> int:
    """Map the JS flags that have a Python equivalent; ignore the rest."""
    result = 0
    for char in flags:
        if char == "i":
            result |= re.IGNORECASE
        elif char == "m":
            result |= re.MULTILINE
        elif char == "s":
            result |= re.DOTALL
        # 'g', 'u', 'v', 'y', 'd' have no Python counterpart.
    return result


def py_flags_to_js(flags: int) -> str:
    """Map Python regex flags to the JS flag string."""
    result = ""
    if flags & re.IGNORECASE:
        result += "i"
    if flags & re.MULTILINE:
        result += "m"
    if flags & re.DOTALL:
        result += "s"
    return result


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


def is_valid_array_index(n: object) -> bool:
    """Mirror of JS ``is_valid_array_index``.

    JS goes through ``Number.isInteger``, which also accepts an integral
    float such as ``3.0``. Nothing emits that — devalue and
    ``JSON.stringify`` format via ``String(number)``, and an index is
    bounded far below where JS switches to exponent notation — and upstream
    only ever tests that non-integers are *rejected*, so accepting it is an
    accident of the implementation rather than part of the format.
    """
    return isinstance(n, int) and not isinstance(n, bool) and 0 <= n <= MAX_ARRAY_INDEX


def is_valid_array_len(n: object) -> bool:
    """Mirror of JS ``is_valid_array_len``; see is_valid_array_index."""
    return isinstance(n, int) and not isinstance(n, bool) and 0 <= n <= MAX_ARRAY_LEN

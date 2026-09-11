"""Path and host pattern compiler."""

from __future__ import annotations

import re
from dataclasses import dataclass

__all__: list[str] = []

_PATH_CONVERTERS: dict[str, str] = {
    "str": r"[^/]+",
    "path": r".*",
}
_HOST_CONVERTERS: dict[str, str] = {
    "str": r"[^.]+",
}


@dataclass(frozen=True, slots=True)
class CompiledPattern:
    """A compiled pattern ready for matching."""

    _pattern: re.Pattern[str]

    def match(self, value: str) -> dict[str, str] | None:
        """Return captured params if *value* matches, else ``None``."""
        m = self._pattern.match(value)
        return m.groupdict() if m is not None else None


def _compile(
    pattern: str,
    converters: dict[str, str],
    tail: str,
) -> tuple[re.Pattern[str], set[str]]:
    """Shared brace-pattern compiler. Returns (compiled regex, param names)."""
    default = next(iter(converters))
    regex_str = "^"
    seen: set[str] = set()
    i = 0

    while i < len(pattern):
        ch = pattern[i]

        if ch == "{":
            try:
                j = pattern.index("}", i)
            except ValueError:
                raise ValueError(f"unclosed '{{' in pattern {pattern!r}") from None

            spec = pattern[i + 1 : j]
            name, converter = spec.split(":", 1) if ":" in spec else (spec, default)

            if not name.isidentifier():
                raise ValueError(f"invalid param name {name!r} in pattern {pattern!r}")
            if converter not in converters:
                raise ValueError(
                    f"unknown converter {converter!r} in pattern {pattern!r}; "
                    f"supported: {', '.join(converters)}"
                )
            if name in seen:
                raise ValueError(f"duplicate param name {name!r} in pattern {pattern!r}")

            seen.add(name)
            regex_str += f"(?P<{name}>{converters[converter]})"
            i = j + 1

        else:
            j = pattern.find("{", i)
            if j == -1:
                j = len(pattern)
            regex_str += re.escape(pattern[i:j])
            i = j

    return re.compile(regex_str + tail), seen


def compile_path(pattern: str, *, strict: bool = False) -> CompiledPattern:
    """Compile a URL path pattern to a matchable object.

    Supported syntax:

    - ``/exact`` — literal match
    - ``/users/{id}`` — single segment capture (no slashes)
    - ``/files/{path:path}`` — greedy multi-segment capture

    By default a trailing slash is optional. Pass ``strict=True`` to
    require an exact match.

    :raises ValueError: for unrecognised converters or duplicate param names.
    """
    tail = "$" if strict else "/?$"
    compiled, _ = _compile(pattern, _PATH_CONVERTERS, tail)
    return CompiledPattern(compiled)


def compile_host(pattern: str) -> CompiledPattern:
    """Compile a hostname pattern to a matchable object.

    Supported syntax:

    - ``myapp.com`` — exact hostname match
    - ``{tenant}.myapp.com`` — single label capture (no dots)

    The port is not part of the pattern; strip it before matching.
    Only the ``str`` converter (``[^.]+``) is supported.

    :raises ValueError: for duplicate param names.
    """
    compiled, _ = _compile(pattern, _HOST_CONVERTERS, "$")
    return CompiledPattern(compiled)

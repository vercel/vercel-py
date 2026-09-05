"""Path pattern compiler."""

from __future__ import annotations

import re
from dataclasses import dataclass

__all__: list[str] = []

# Supported converters: {param} and {param:path}
_CONVERTERS: dict[str, str] = {
    "str": r"[^/]+",
    "path": r".*",
}
_DEFAULT_CONVERTER = "str"


@dataclass(frozen=True, slots=True)
class CompiledPattern:
    """A compiled URL pattern ready for matching."""

    _pattern: re.Pattern[str]

    def match(self, path: str) -> dict[str, str] | None:
        """Return captured params if *path* matches, else ``None``."""
        m = self._pattern.match(path)
        return m.groupdict() if m is not None else None


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
            if ":" in spec:
                name, converter = spec.split(":", 1)
            else:
                name, converter = spec, _DEFAULT_CONVERTER

            if not name.isidentifier():
                raise ValueError(f"invalid param name {name!r} in pattern {pattern!r}")
            if converter not in _CONVERTERS:
                raise ValueError(
                    f"unknown converter {converter!r} in pattern {pattern!r}; "
                    f"supported: {', '.join(_CONVERTERS)}"
                )
            if name in seen:
                raise ValueError(f"duplicate param name {name!r} in pattern {pattern!r}")

            seen.add(name)
            regex_str += f"(?P<{name}>{_CONVERTERS[converter]})"
            i = j + 1

        else:
            j = pattern.find("{", i)
            if j == -1:
                j = len(pattern)
            regex_str += re.escape(pattern[i:j])
            i = j

    tail = "$" if strict else "/?$"
    return CompiledPattern(re.compile(regex_str + tail))

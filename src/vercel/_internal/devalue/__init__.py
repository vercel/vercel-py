"""Minimal Python port of the JavaScript *devalue* serialization library.

Wire-format compatible with https://github.com/Rich-Harris/devalue — values
serialized by Python ``stringify`` can be parsed by JS ``devalue.parse`` and
vice-versa.
"""

from .parse import parse, unflatten
from .stringify import stringify
from .utils import DevalueError, Undefined

__all__ = [
    "DevalueError",
    "Undefined",
    "parse",
    "stringify",
    "unflatten",
]

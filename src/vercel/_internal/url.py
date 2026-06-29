"""URL construction helpers."""

from string import Formatter
from urllib.parse import quote

_FORMATTER = Formatter()


def format_url_path(template: str, /, **values: str) -> str:
    """Format a URL path template with percent-encoded path segment values.

    Use this for route templates where dynamic values must be treated as a
    single path segment, even when they contain reserved characters like `/`,
    `?`, or spaces. Literal text in the template is left unchanged; only values
    passed as placeholders are encoded.
    """
    encoded_values = {key: quote(value, safe="") for key, value in values.items()}
    return _FORMATTER.vformat(template, (), encoded_values)

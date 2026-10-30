"""Parse the SDK/CLI network policy JSON format into authored Python values."""

import json
import re
from collections.abc import Iterable

from pydantic import ValidationError

from vercel.sandbox._internal.models import NetworkPolicy


class NetworkPolicyParseError(ValueError):
    """Invalid policy input, with paths and explanations but no input values."""

    issues: tuple[str, ...]

    def __init__(self, issues: Iterable[str]) -> None:
        self.issues = tuple(issues)
        super().__init__("Invalid network policy:\n" + "\n".join(f"  {i}" for i in self.issues))


def parse_network_policy(input: object) -> NetworkPolicy:
    """Parse decoded network policy JSON into an immutable authored policy.

    Accepts ``"allow-all"``, ``"deny-all"``, or the JS SDK/CLI object format,
    with an optional domain list or rule map and optional subnets. Use
    ``json.load(file)`` or ``json.loads(text)`` to decode JSON first.

    Unknown keys are rejected throughout the policy, so a typo cannot silently
    widen a credential-bearing rule. Python's authored-model structural
    invariants also apply. Raises :class:`NetworkPolicyParseError` with paths
    and explanations that never include input values.
    """
    try:
        return NetworkPolicy.from_json_value(input)
    except ValidationError as error:
        issues = []
        for issue in error.errors(include_url=False, include_input=False, include_context=False):
            message = "unknown key" if issue["type"] == "extra_forbidden" else issue["msg"]
            issues.append(f"{_format_path(issue['loc'])}: {message}")
    # Raise outside the handler so the exception does not retain Pydantic's
    # credential-bearing input through __context__ or __cause__.
    raise NetworkPolicyParseError(issues)


def _format_path(path: tuple[str | int, ...]) -> str:
    result = ""
    for key in path:
        if isinstance(key, int):
            result += f"[{key}]"
        else:
            if re.fullmatch(r"[A-Za-z_$][\w$]*", key):
                result += ("." if result else "") + key
            else:
                result += f"[{json.dumps(key)}]"
    return result or "policy"

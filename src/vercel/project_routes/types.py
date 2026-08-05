from __future__ import annotations

import sys
from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal, TypeAlias, TypedDict

if sys.version_info >= (3, 11):
    from typing import Required
else:
    from typing_extensions import Required

SrcSyntax: TypeAlias = Literal["equals", "path-to-regexp", "regex"]
RouteType: TypeAlias = Literal["rewrite", "redirect", "set_status", "transform"]
RouteDiff: TypeAlias = bool | Literal["only"]
ConditionType: TypeAlias = Literal["host", "header", "cookie", "query"]
TransformType: TypeAlias = Literal["request.headers", "request.query", "response.headers"]
TransformOperation: TypeAlias = Literal["append", "set", "delete"]
Placement: TypeAlias = Literal["start", "end", "after", "before"]
VersionAction: TypeAlias = Literal["promote", "restore", "discard"]

REDIRECT_STATUS_CODES = frozenset({301, 302, 303, 307, 308})


class RouteCondition(TypedDict, total=False):
    type: ConditionType
    key: str
    value: str


class RouteTransform(TypedDict, total=False):
    type: TransformType
    op: TransformOperation
    target: dict[str, object]
    args: object
    env: list[str]


class RouteDefinition(TypedDict, total=False):
    """The wire representation of a routing rule, as accepted by the API.

    This is the same shape used for ``routes`` in ``vercel.json``, restricted
    to the fields the project routes API accepts.
    """

    src: Required[str]
    dest: str
    headers: dict[str, str]
    caseSensitive: bool
    status: int
    has: list[RouteCondition]
    missing: list[RouteCondition]
    transforms: list[RouteTransform]
    respectOriginCacheControl: bool


class _RouteInputOptional(TypedDict, total=False):
    description: str
    enabled: bool
    srcSyntax: SrcSyntax


class RouteInput(_RouteInputOptional):
    """The wire representation of a route to create or replace."""

    name: str
    route: RouteDefinition


class StagedRouteInput(RouteInput):
    """A route staged in bulk; ``id`` identifies the route to merge or replace."""

    id: str


def _base_route_input(
    *,
    name: str,
    route: RouteDefinition,
    source_syntax: SrcSyntax | None,
    description: str | None,
    enabled: bool | None,
) -> RouteInput:
    result: RouteInput = {"name": name, "route": route}
    if source_syntax is not None:
        result["srcSyntax"] = source_syntax
    if description is not None:
        result["description"] = description
    if enabled is not None:
        result["enabled"] = enabled
    return result


@dataclass(frozen=True, slots=True)
class RewriteRoute:
    """A project route that rewrites one path to another."""

    name: str
    source: str
    destination: str
    source_syntax: SrcSyntax | None = None
    description: str | None = None
    enabled: bool | None = None

    def to_route_input(self) -> RouteInput:
        """Return the request representation expected by the Vercel API."""
        return _base_route_input(
            name=self.name,
            route={"src": self.source, "dest": self.destination},
            source_syntax=self.source_syntax,
            description=self.description,
            enabled=self.enabled,
        )


@dataclass(frozen=True, slots=True)
class RedirectRoute:
    """A project route that redirects one path to another."""

    name: str
    source: str
    destination: str
    status: int = 308
    source_syntax: SrcSyntax | None = None
    description: str | None = None
    enabled: bool | None = None

    def __post_init__(self) -> None:
        if self.status not in REDIRECT_STATUS_CODES:
            raise ValueError(
                f"Redirect status must be one of {sorted(REDIRECT_STATUS_CODES)}, "
                f"got {self.status}."
            )

    def to_route_input(self) -> RouteInput:
        """Return the request representation expected by the Vercel API."""
        return _base_route_input(
            name=self.name,
            route={"src": self.source, "dest": self.destination, "status": self.status},
            source_syntax=self.source_syntax,
            description=self.description,
            enabled=self.enabled,
        )


@dataclass(frozen=True, slots=True)
class SetStatusRoute:
    """A project route that answers a path with a fixed status code."""

    name: str
    source: str
    status: int
    source_syntax: SrcSyntax | None = None
    description: str | None = None
    enabled: bool | None = None

    def to_route_input(self) -> RouteInput:
        """Return the request representation expected by the Vercel API."""
        return _base_route_input(
            name=self.name,
            route={"src": self.source, "status": self.status},
            source_syntax=self.source_syntax,
            description=self.description,
            enabled=self.enabled,
        )


RouteSpec: TypeAlias = RewriteRoute | RedirectRoute | SetStatusRoute | RouteInput
"""A route to create: an authoring dataclass or a raw ``RouteInput`` mapping."""


@dataclass(frozen=True, slots=True)
class RouteVersion:
    """One version of a project's routing rules."""

    id: str
    last_modified: datetime
    created_by: str
    s3_key: str
    is_staging: bool | None = None
    is_live: bool | None = None
    rule_count: int | None = None
    alias: str | None = None


@dataclass(frozen=True, slots=True)
class ProjectRoute:
    """A routing rule as returned by the API."""

    id: str
    name: str
    route: RouteDefinition
    description: str | None = None
    enabled: bool | None = None
    staged: bool | None = None
    raw_src: str | None = None
    raw_dest: str | None = None
    src_syntax: SrcSyntax | None = None
    route_type: RouteType | None = None

    def to_route_input(self) -> RouteInput:
        """Return this route as the input shape used by add and edit."""
        return _base_route_input(
            name=self.name,
            route=self.route,
            source_syntax=self.src_syntax,
            description=self.description,
            enabled=self.enabled,
        )

    def to_staged_input(self) -> StagedRouteInput:
        """Return this route as the input shape used by stage_routes."""
        staged: StagedRouteInput = {"id": self.id, "name": self.name, "route": self.route}
        if self.src_syntax is not None:
            staged["srcSyntax"] = self.src_syntax
        if self.description is not None:
            staged["description"] = self.description
        if self.enabled is not None:
            staged["enabled"] = self.enabled
        return staged


@dataclass(frozen=True, slots=True)
class RouteLimit:
    """How many routes the project uses out of its allowance."""

    max_routes: int
    current_routes: int


@dataclass(frozen=True, slots=True)
class GetRoutesResult:
    """Routing rules for a project version.

    ``version`` is ``None`` when the project has no staged or published
    routes. ``limit`` is only present outside diff mode, and ``diff_count``
    only when ``diff="only"``.
    """

    routes: list[ProjectRoute]
    version: RouteVersion | None
    limit: RouteLimit | None = None
    diff_count: int | None = None


@dataclass(frozen=True, slots=True)
class AddRouteResult:
    """The added route and the staged version containing it."""

    route: ProjectRoute
    version: RouteVersion


@dataclass(frozen=True, slots=True)
class EditRouteResult:
    """The edited route (when returned) and the staged version containing it."""

    version: RouteVersion
    route: ProjectRoute | None = None


@dataclass(frozen=True, slots=True)
class DeleteRoutesResult:
    """How many routes were deleted and the staged version without them."""

    deleted_count: int
    version: RouteVersion


class CurrentRoutePathCondition(TypedDict, total=False):
    value: str
    syntax: str


class CurrentRouteCondition(TypedDict, total=False):
    field: str
    operator: str
    key: str
    value: str
    missing: bool


class CurrentRouteHeader(TypedDict, total=False):
    key: str
    value: str
    op: str


class CurrentRouteAction(TypedDict, total=False):
    type: str
    subType: str
    dest: str
    status: int
    headers: list[CurrentRouteHeader]


class _GenerateRouteCurrentOptional(TypedDict, total=False):
    name: str
    description: str
    conditions: list[CurrentRouteCondition]


class GenerateRouteCurrent(_GenerateRouteCurrentOptional):
    """The wire representation of an existing route to refine with a prompt."""

    pathCondition: CurrentRoutePathCondition
    actions: list[CurrentRouteAction]


@dataclass(frozen=True, slots=True)
class GeneratedPathCondition:
    """The path a generated route matches."""

    value: str
    syntax: str


@dataclass(frozen=True, slots=True)
class GeneratedRouteCondition:
    """A request condition on a generated route."""

    field: str
    operator: str
    missing: bool = False
    key: str | None = None
    value: str | None = None


@dataclass(frozen=True, slots=True)
class GeneratedRouteHeader:
    """A header manipulation on a generated route action."""

    key: str
    op: str
    value: str | None = None


@dataclass(frozen=True, slots=True)
class GeneratedRouteAction:
    """One action a generated route performs."""

    type: str
    sub_type: str | None = None
    dest: str | None = None
    status: int | None = None
    headers: list[GeneratedRouteHeader] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class GeneratedRoute:
    """A route suggestion produced from a natural-language prompt."""

    name: str
    description: str
    path_condition: GeneratedPathCondition
    actions: list[GeneratedRouteAction]
    conditions: list[GeneratedRouteCondition] = field(default_factory=list)

    def to_current_route(self) -> GenerateRouteCurrent:
        """Return this suggestion as input for a follow-up generate call."""
        current: GenerateRouteCurrent = {
            "name": self.name,
            "description": self.description,
            "pathCondition": {
                "value": self.path_condition.value,
                "syntax": self.path_condition.syntax,
            },
            "actions": [_current_action(action) for action in self.actions],
        }
        if self.conditions:
            current["conditions"] = [_current_condition(condition) for condition in self.conditions]
        return current


def _current_action(action: GeneratedRouteAction) -> CurrentRouteAction:
    result: CurrentRouteAction = {"type": action.type}
    if action.sub_type is not None:
        result["subType"] = action.sub_type
    if action.dest is not None:
        result["dest"] = action.dest
    if action.status is not None:
        result["status"] = action.status
    if action.headers:
        result["headers"] = [_current_header(header) for header in action.headers]
    return result


def _current_header(header: GeneratedRouteHeader) -> CurrentRouteHeader:
    result: CurrentRouteHeader = {"key": header.key, "op": header.op}
    if header.value is not None:
        result["value"] = header.value
    return result


def _current_condition(condition: GeneratedRouteCondition) -> CurrentRouteCondition:
    result: CurrentRouteCondition = {
        "field": condition.field,
        "operator": condition.operator,
        "missing": condition.missing,
    }
    if condition.key is not None:
        result["key"] = condition.key
    if condition.value is not None:
        result["value"] = condition.value
    return result


class Position(TypedDict, total=False):
    placement: Placement
    referenceId: str


class _AddRouteRequestBodyOptional(TypedDict, total=False):
    position: Position


class AddRouteRequestBody(_AddRouteRequestBodyOptional):
    route: RouteInput


class StageRoutesRequestBody(TypedDict, total=False):
    overwrite: bool
    routes: Required[list[StagedRouteInput]]


class DeleteRoutesRequestBody(TypedDict):
    routeIds: list[str]


class EditRouteRequestBody(TypedDict, total=False):
    route: RouteInput
    restore: bool


class UpdateRouteVersionRequestBody(TypedDict):
    id: str
    action: VersionAction


class _GenerateRouteRequestBodyOptional(TypedDict, total=False):
    currentRoute: GenerateRouteCurrent


class GenerateRouteRequestBody(_GenerateRouteRequestBodyOptional):
    prompt: str


__all__ = [
    "AddRouteRequestBody",
    "AddRouteResult",
    "ConditionType",
    "CurrentRouteAction",
    "CurrentRouteCondition",
    "CurrentRouteHeader",
    "CurrentRoutePathCondition",
    "DeleteRoutesRequestBody",
    "DeleteRoutesResult",
    "EditRouteRequestBody",
    "EditRouteResult",
    "GenerateRouteCurrent",
    "GenerateRouteRequestBody",
    "GeneratedPathCondition",
    "GeneratedRoute",
    "GeneratedRouteAction",
    "GeneratedRouteCondition",
    "GeneratedRouteHeader",
    "GetRoutesResult",
    "Placement",
    "Position",
    "ProjectRoute",
    "RedirectRoute",
    "RewriteRoute",
    "RouteCondition",
    "RouteDefinition",
    "RouteDiff",
    "RouteInput",
    "RouteLimit",
    "RouteSpec",
    "RouteTransform",
    "RouteType",
    "RouteVersion",
    "SetStatusRoute",
    "SrcSyntax",
    "StageRoutesRequestBody",
    "StagedRouteInput",
    "TransformOperation",
    "TransformType",
    "UpdateRouteVersionRequestBody",
    "VersionAction",
]

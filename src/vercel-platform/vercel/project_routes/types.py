from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any, Literal, TypeAlias

import pydantic
from pydantic import Field
from pydantic.alias_generators import to_camel

SrcSyntax: TypeAlias = Literal["equals", "path-to-regexp", "regex"]
RouteType: TypeAlias = Literal["rewrite", "redirect", "set_status", "transform"]
RouteDiff: TypeAlias = bool | Literal["only"]
ConditionType: TypeAlias = Literal["host", "header", "cookie", "query"]
TransformType: TypeAlias = Literal["request.headers", "request.query", "response.headers"]
TransformOperation: TypeAlias = Literal["append", "set", "delete"]
Placement: TypeAlias = Literal["start", "end", "after", "before"]
VersionAction: TypeAlias = Literal["promote", "restore", "discard"]

REDIRECT_STATUS_CODES = frozenset({301, 302, 303, 307, 308})


class _Model(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(frozen=True, populate_by_name=True, alias_generator=to_camel)


class RouteCondition(_Model):
    """A request condition (``has``/``missing``) on a routing rule."""

    type: ConditionType
    key: str | None = None
    value: str | None = None


class RouteTransform(_Model):
    """A request or response transformation on a routing rule."""

    type: TransformType
    op: TransformOperation
    target: dict[str, Any]
    args: Any = None
    env: list[str] | None = None


class RouteDefinition(_Model):
    """A routing rule, in the same shape as ``routes`` in ``vercel.json``.

    Restricted to the fields the project routes API accepts.
    """

    src: str
    dest: str | None = None
    headers: dict[str, str] | None = None
    case_sensitive: bool | None = None
    status: int | None = None
    has: list[RouteCondition] | None = None
    missing: list[RouteCondition] | None = None
    transforms: list[RouteTransform] | None = None
    respect_origin_cache_control: bool | None = None


class RouteInput(_Model):
    """A route to create or replace."""

    name: str
    route: RouteDefinition
    description: str | None = None
    enabled: bool | None = None
    src_syntax: SrcSyntax | None = None

    def to_wire(self) -> dict[str, Any]:
        """Return the request representation expected by the Vercel API."""
        return self.model_dump(by_alias=True, exclude_none=True)


class StagedRouteInput(RouteInput):
    """A route staged in bulk; ``id`` identifies the route to merge or replace."""

    id: str


class RewriteRoute(_Model):
    """A project route that rewrites one path to another."""

    name: str
    source: str
    destination: str
    source_syntax: SrcSyntax | None = None
    description: str | None = None
    enabled: bool | None = None

    def to_route_input(self) -> RouteInput:
        return RouteInput(
            name=self.name,
            route=RouteDefinition(src=self.source, dest=self.destination),
            src_syntax=self.source_syntax,
            description=self.description,
            enabled=self.enabled,
        )


class RedirectRoute(_Model):
    """A project route that redirects one path to another."""

    name: str
    source: str
    destination: str
    status: int = 308
    source_syntax: SrcSyntax | None = None
    description: str | None = None
    enabled: bool | None = None

    @pydantic.field_validator("status")
    @classmethod
    def _check_status(cls, status: int) -> int:
        if status not in REDIRECT_STATUS_CODES:
            raise ValueError(
                f"Redirect status must be one of {sorted(REDIRECT_STATUS_CODES)}, got {status}."
            )
        return status

    def to_route_input(self) -> RouteInput:
        return RouteInput(
            name=self.name,
            route=RouteDefinition(src=self.source, dest=self.destination, status=self.status),
            src_syntax=self.source_syntax,
            description=self.description,
            enabled=self.enabled,
        )


class SetStatusRoute(_Model):
    """A project route that answers a path with a fixed status code."""

    name: str
    source: str
    status: int
    source_syntax: SrcSyntax | None = None
    description: str | None = None
    enabled: bool | None = None

    def to_route_input(self) -> RouteInput:
        return RouteInput(
            name=self.name,
            route=RouteDefinition(src=self.source, status=self.status),
            src_syntax=self.source_syntax,
            description=self.description,
            enabled=self.enabled,
        )


RouteSpec: TypeAlias = (
    RewriteRoute | RedirectRoute | SetStatusRoute | RouteInput | Mapping[str, Any]
)
"""A route to create: an authoring model or a ``vercel.json``-shaped mapping."""


class RouteVersion(_Model):
    """One version of a project's routing rules."""

    id: str
    last_modified: datetime
    created_by: str
    s3_key: str
    is_staging: bool | None = None
    is_live: bool | None = None
    rule_count: int | None = None
    alias: str | None = None

    @pydantic.field_validator("last_modified", mode="before")
    @classmethod
    def _from_epoch_millis(cls, value: object) -> object:
        if isinstance(value, int | float):
            return value / 1000
        return value


class ProjectRoute(_Model):
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
        return RouteInput(
            name=self.name,
            route=self.route,
            src_syntax=self.src_syntax,
            description=self.description,
            enabled=self.enabled,
        )

    def to_staged_input(self) -> StagedRouteInput:
        """Return this route as the input shape used by stage_routes."""
        return StagedRouteInput(id=self.id, **dict(self.to_route_input()))


class RouteLimit(_Model):
    """How many routes the project uses out of its allowance."""

    max_routes: int
    current_routes: int


class GetRoutesResult(_Model):
    """Routing rules for a project version.

    ``version`` is ``None`` when the project has no staged or published
    routes. ``limit`` is only present outside diff mode, and ``diff_count``
    only when ``diff="only"``.
    """

    routes: list[ProjectRoute]
    version: RouteVersion | None = None
    limit: RouteLimit | None = None
    diff_count: int | None = None


class AddRouteResult(_Model):
    """The added route and the staged version containing it."""

    route: ProjectRoute
    version: RouteVersion


class EditRouteResult(_Model):
    """The edited route (when returned) and the staged version containing it."""

    version: RouteVersion
    route: ProjectRoute | None = None


class DeleteRoutesResult(_Model):
    """How many routes were deleted and the staged version without them."""

    deleted_count: int
    version: RouteVersion


class GeneratedPathCondition(_Model):
    """The path a generated route matches."""

    value: str
    syntax: str


class GeneratedRouteCondition(_Model):
    """A request condition on a generated route."""

    field: str
    operator: str
    missing: bool = False
    key: str | None = None
    value: str | None = None


class GeneratedRouteHeader(_Model):
    """A header manipulation on a generated route action."""

    key: str
    op: str
    value: str | None = None


class GeneratedRouteAction(_Model):
    """One action a generated route performs."""

    type: str
    sub_type: str | None = None
    dest: str | None = None
    status: int | None = None
    headers: list[GeneratedRouteHeader] = Field(default_factory=list)


class GeneratedRoute(_Model):
    """A route suggestion produced from a natural-language prompt.

    Pass it back to ``generate_route`` as ``current_route`` to refine it.
    """

    name: str
    description: str
    path_condition: GeneratedPathCondition
    actions: list[GeneratedRouteAction]
    conditions: list[GeneratedRouteCondition] = Field(default_factory=list)

    def to_wire(self) -> dict[str, Any]:
        """Return the ``current_route`` representation expected by the API."""
        return self.model_dump(by_alias=True, exclude_none=True)


__all__ = [
    "AddRouteResult",
    "ConditionType",
    "DeleteRoutesResult",
    "EditRouteResult",
    "GeneratedPathCondition",
    "GeneratedRoute",
    "GeneratedRouteAction",
    "GeneratedRouteCondition",
    "GeneratedRouteHeader",
    "GetRoutesResult",
    "Placement",
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
    "StagedRouteInput",
    "TransformOperation",
    "TransformType",
    "VersionAction",
]

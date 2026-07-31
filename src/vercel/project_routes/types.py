from __future__ import annotations

import sys
from typing import Any, Literal, TypeAlias, TypedDict

if sys.version_info >= (3, 11):
    from typing import Required
else:
    from typing_extensions import Required

SrcSyntax: TypeAlias = Literal["equals", "path-to-regexp", "regex"]
RouteType: TypeAlias = Literal["rewrite", "redirect", "set_status", "transform"]
RouteFilter: TypeAlias = RouteType
RouteDiff: TypeAlias = bool | Literal["only"]
ConditionType: TypeAlias = Literal["host", "header", "cookie", "query"]
ConditionValue: TypeAlias = str | int | dict[str, Any]
TransformType: TypeAlias = Literal["request.headers", "request.query", "response.headers"]
TransformOperation: TypeAlias = Literal["append", "set", "delete"]
Placement: TypeAlias = Literal["start", "end", "after", "before"]
VersionAction: TypeAlias = Literal["promote", "restore", "discard"]


class RouteCondition(TypedDict, total=False):
    type: ConditionType
    key: str
    value: ConditionValue


class RouteTransform(TypedDict, total=False):
    type: TransformType
    op: TransformOperation
    target: dict[str, Any]
    args: Any
    env: list[str]


RouteDefinition = TypedDict(
    "RouteDefinition",
    {
        "src": Required[str],
        "dest": str,
        "headers": dict[str, str],
        "methods": list[str],
        "continue": bool,
        "override": bool,
        "caseSensitive": bool,
        "check": bool,
        "important": bool,
        "status": int,
        "has": list[RouteCondition],
        "missing": list[RouteCondition],
        "transforms": list[RouteTransform],
        "env": list[str],
        "locale": dict[str, Any],
        "source": str,
        "destination": str,
        "statusCode": int,
        "middlewarePath": str,
        "middlewareRawSrc": list[str],
        "middleware": int,
        "respectOriginCacheControl": bool,
    },
    total=False,
)


class _RouteInputOptional(TypedDict, total=False):
    description: str
    enabled: bool
    srcSyntax: SrcSyntax


class RouteInput(_RouteInputOptional):
    name: str
    route: RouteDefinition


class StagedRouteInput(RouteInput):
    id: str


class Position(TypedDict, total=False):
    placement: Placement
    referenceId: str


class _AddRouteRequestBodyOptional(TypedDict, total=False):
    position: Position


class AddRouteRequestBody(_AddRouteRequestBodyOptional):
    route: RouteInput


class StageRoutesRequestBody(TypedDict, total=False):
    overwrite: bool
    routes: list[StagedRouteInput]


class DeleteRoutesRequestBody(TypedDict):
    routeIds: list[str]


class EditRouteRequestBody(TypedDict, total=False):
    route: RouteInput
    restore: bool


class UpdateRouteVersionsRequestBody(TypedDict):
    id: str
    action: VersionAction


class _ProjectRouteOptional(TypedDict, total=False):
    description: str
    enabled: bool
    staged: bool
    rawSrc: str
    rawDest: str
    srcSyntax: SrcSyntax
    routeType: RouteType


class ProjectRoute(_ProjectRouteOptional):
    id: str
    name: str
    route: RouteDefinition


class _RouteVersionOptional(TypedDict, total=False):
    isStaging: bool
    isLive: bool
    ruleCount: int
    alias: str


class RouteVersion(_RouteVersionOptional):
    id: str
    s3Key: str
    lastModified: int
    createdBy: str


class RouteLimit(TypedDict):
    maxRoutes: int
    currentRoutes: int


class GetRoutesResponse(TypedDict, total=False):
    routes: list[ProjectRoute]
    version: RouteVersion
    limit: RouteLimit


class VersionResponse(TypedDict):
    version: RouteVersion


class AddRouteResponse(VersionResponse):
    route: ProjectRoute


EditRouteResponse: TypeAlias = AddRouteResponse
StageRoutesResponse: TypeAlias = VersionResponse
UpdateRouteVersionsResponse: TypeAlias = VersionResponse


class DeleteRoutesResponse(VersionResponse):
    deletedCount: int


class GetRouteVersionsResponse(TypedDict):
    versions: list[RouteVersion]


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
    pathCondition: CurrentRoutePathCondition
    actions: list[CurrentRouteAction]


class _GenerateRouteRequestBodyOptional(TypedDict, total=False):
    currentRoute: GenerateRouteCurrent


class GenerateRouteRequestBody(_GenerateRouteRequestBodyOptional):
    prompt: str


class GenerateRoutePathCondition(TypedDict):
    value: str
    syntax: SrcSyntax


class _GenerateRouteConditionOptional(TypedDict, total=False):
    key: str
    value: str


class GenerateRouteCondition(_GenerateRouteConditionOptional):
    field: Literal["host", "header", "cookie", "query"]
    operator: Literal["contains", "eq", "re", "exists"]
    missing: bool


class _GenerateRouteHeaderOptional(TypedDict, total=False):
    value: str


class GenerateRouteHeader(_GenerateRouteHeaderOptional):
    key: str
    op: TransformOperation


class _GenerateRouteActionOptional(TypedDict, total=False):
    subType: Literal["response-headers", "transform-request-header", "transform-request-query"]
    dest: str
    status: int
    headers: list[GenerateRouteHeader]


class GenerateRouteAction(_GenerateRouteActionOptional):
    type: Literal["rewrite", "redirect", "set-status", "modify"]


class _GeneratedRouteOptional(TypedDict, total=False):
    conditions: list[GenerateRouteCondition]


class GeneratedRoute(_GeneratedRouteOptional):
    name: str
    description: str
    pathCondition: GenerateRoutePathCondition
    actions: list[GenerateRouteAction]


class GenerateRouteResponse(TypedDict, total=False):
    route: GeneratedRoute
    error: str


__all__ = [
    "AddRouteRequestBody",
    "AddRouteResponse",
    "ConditionType",
    "ConditionValue",
    "CurrentRouteAction",
    "CurrentRouteCondition",
    "CurrentRouteHeader",
    "CurrentRoutePathCondition",
    "DeleteRoutesRequestBody",
    "DeleteRoutesResponse",
    "EditRouteRequestBody",
    "EditRouteResponse",
    "GenerateRouteAction",
    "GenerateRouteCondition",
    "GenerateRouteCurrent",
    "GenerateRouteHeader",
    "GenerateRoutePathCondition",
    "GenerateRouteRequestBody",
    "GenerateRouteResponse",
    "GeneratedRoute",
    "GetRouteVersionsResponse",
    "GetRoutesResponse",
    "Placement",
    "Position",
    "ProjectRoute",
    "RouteCondition",
    "RouteDefinition",
    "RouteDiff",
    "RouteFilter",
    "RouteInput",
    "RouteLimit",
    "RouteTransform",
    "RouteType",
    "RouteVersion",
    "SrcSyntax",
    "StageRoutesRequestBody",
    "StageRoutesResponse",
    "StagedRouteInput",
    "TransformOperation",
    "TransformType",
    "UpdateRouteVersionsRequestBody",
    "UpdateRouteVersionsResponse",
    "VersionAction",
    "VersionResponse",
]

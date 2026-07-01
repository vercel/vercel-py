import datetime
import math
import os
import platform
import traceback
import urllib.parse
from collections.abc import Mapping
from typing import Any, TypeVar

import cbor2
import httpx
import pydantic

import vercel.queue as vqs
from vercel._internal.polyfills import UTC
from vercel.oidc.aio import get_vercel_oidc_token

from .. import world as w

# Hard-coded workflow-server URL override for testing.
# Set this to test against a different workflow-server version.
# Leave empty string for production (uses default vercel-workflow.com).
#
# Example: 'https://workflow-server-git-branch-name.vercel.sh'
#
WORKFLOW_SERVER_URL_OVERRIDE = ""

MAX_DELAY_SECONDS = float(
    os.getenv("VERCEL_QUEUE_MAX_DELAY_SECONDS", "82800")
)  # 23 hours - leave 1h buffer before 24h retention limit

T = TypeVar("T", bound=w.BaseModel)


def _cbor_tag_hook(tag: cbor2.CBORTag, shareable: bool = False) -> Any:
    if tag.tag == 64:
        return tag.value
    return tag


def _cbor_filter_undefined(value: Mapping[Any, Any], shareable: bool = False) -> dict[str, Any]:
    return {k: None if v is cbor2.undefined else v for k, v in value.items()}


# Events whose result the runtime reads back resolved (run/step entity fields);
# everything else is fetched lazily.
_EVENTS_NEEDING_RESOLVE = frozenset({"run_created", "run_started", "step_started"})


# Lazy wire schema for EventResult — mirrors the JS SDK's EventResultLazyWireSchema.
# Uses BaseWorkflowRun (no discriminated union) with loose error typing so that
# unresolved RemoteRefs in error/input/output fields don't cause validation failures.
class _LazyWorkflowRun(w.BaseWorkflowRun):
    """Loose run schema that accepts any error shape (may be a RemoteRef)."""

    error: Any = None
    input: Any = None
    output: Any = None


class _LazyEventResult(w.EventResult):
    """Loose EventResult that tolerates unresolved RemoteRefs in nested objects.
    Event data fields (e.g. payload, input, output) may contain RemoteRef dicts
    instead of their expected types, so we accept Any for event and run."""

    event: Any = None  # type: ignore[assignment]
    events: Any = None  # type: ignore[assignment]
    run: _LazyWorkflowRun | None = None  # type: ignore[assignment]
    step: Any = None  # type: ignore[assignment]


class VercelWorld(w.World):
    def __init__(
        self,
        *,
        token: str | None = None,
        environment: str | None = None,
        project_id: str | None = None,
        team_id: str | None = None,
    ) -> None:
        self._token = token
        self._queue_callbacks: list[Any] = []

        # utils.ts, getHttpUrl
        # Use proxy when we have project config (for authentication via Vercel API)
        self._using_proxy = bool(project_id and team_id)
        # When using proxy, requests go through api.vercel.com (with x-vercel-workflow-api-url
        # header if override is set)
        # When not using proxy, use the default workflow-server URL (with /api path appended)
        if self._using_proxy:
            self._base_url = "https://api.vercel.com/v1/workflow"
        else:
            default_host = WORKFLOW_SERVER_URL_OVERRIDE or "https://vercel-workflow.com"
            self._base_url = f"{default_host}/api"

        # utils.ts, getUserAgent
        self._headers = {}
        self._headers["User-Agent"] = (
            f"@workflow/world-vercel/0.3.8 "
            f"python-{platform.python_version()} "
            f"{platform.system().lower()} ({platform.machine()})"
        )

        # utils.ts, getHeaders
        if environment or project_id or team_id:
            self._headers["x-vercel-environment"] = environment or "production"
            if project_id:
                self._headers["x-vercel-project-id"] = project_id
            if team_id:
                self._headers["x-vercel-team-id"] = team_id
        # Only set workflow-api-url header when using the proxy, since the proxy
        # forwards it to the workflow-server. When not using proxy, requests go
        # directly to the workflow-server so this header has no effect.
        if WORKFLOW_SERVER_URL_OVERRIDE and self._using_proxy:
            self._headers["x-vercel-workflow-api-url"] = WORKFLOW_SERVER_URL_OVERRIDE

        self._queue_clients: dict[object, vqs.QueueClient] = {}

    def _queue_client(
        self, *, deployment: vqs.DeploymentOption = vqs.CURRENT_DEPLOYMENT
    ) -> vqs.QueueClient:
        if deployment not in self._queue_clients:
            base_url = f"{self._base_url}/queues-proxy" if self._using_proxy else None
            self._queue_clients[deployment] = vqs.QueueClient(
                token=self._token if self._using_proxy else None,
                region=os.getenv("VERCEL_REGION", "iad1"),
                base_url=base_url,
                deployment=deployment,
                headers=self._headers,
            )
        return self._queue_clients[deployment]

    async def aclose(self) -> None:
        self._queue_clients.clear()

    async def _cbor_request(
        self,
        method: str,
        endpoint: str,
        *,
        schema: type[T] | pydantic.TypeAdapter[T],
        data: Any = None,
    ) -> T:
        # utils.ts, getHttpConfig, makeRequest
        if self._token is None:
            token = await get_vercel_oidc_token()
        else:
            token = self._token
        headers = self._headers.copy()
        if token:
            headers["Authorization"] = f"Bearer {token}"

        headers["Accept"] = "application/cbor"
        # NOTE: Add a unique header to bypass RSC request memoization.
        # See: https://github.com/vercel/workflow/issues/618
        headers["X-Request-Time"] = datetime.datetime.now(UTC).isoformat() + "Z"

        # Encode body as CBOR if data is provided
        body: bytes | None = None
        if data is not None:
            headers["Content-Type"] = "application/cbor"
            body = cbor2.dumps(data)

        async with httpx.AsyncClient(base_url=self._base_url, headers=headers) as client:
            resp = await client.request(
                method,
                endpoint,
                content=body,
            )

        # utils.ts, parseResponseBody
        content_type = resp.headers.get("Content-Type", "")
        if "application/cbor" in content_type:
            result = cbor2.loads(
                resp.content, tag_hook=_cbor_tag_hook, object_hook=_cbor_filter_undefined
            )
        else:
            try:
                result = resp.json()
            except Exception:
                # Server may return CBOR without the correct Content-Type header
                # (e.g. through a proxy). Try CBOR decoding as fallback.
                result = cbor2.loads(
                    resp.content, tag_hook=_cbor_tag_hook, object_hook=_cbor_filter_undefined
                )

        if resp.is_success:
            if isinstance(schema, pydantic.TypeAdapter):
                return schema.validate_python(result)
            else:
                return schema.model_validate(result)
        else:
            if not isinstance(result, dict):
                result = {}
            message = (
                result.get("message")
                or f"{method} {endpoint} -> HTTP {resp.status_code}: {resp.reason_phrase}"
            )
            url = f"{self._base_url}{endpoint}"
            code = result.get("code")
            if resp.status_code == 409:
                raise w.EntityConflictError(message)
            if resp.status_code == 410:
                raise w.RunExpiredError(message, status=resp.status_code, code=code, url=url)
            # retryAfter (seconds) is carried in the Retry-After header.
            retry_after_header = resp.headers.get("retry-after")
            retry_after: int | None = None
            if retry_after_header is not None:
                try:
                    retry_after = int(retry_after_header)
                except ValueError:
                    retry_after = None
            if resp.status_code == 425:
                raise w.TooEarlyError(message, retry_after=retry_after)
            if resp.status_code == 429:
                raise w.ThrottleError(
                    message, status=resp.status_code, code=code, url=url, retry_after=retry_after
                )
            raise w.WorkflowWorldError(message, status=resp.status_code, code=code, url=url)

    async def get_deployment_id(self) -> str:
        deployment_id = os.getenv("VERCEL_DEPLOYMENT_ID")
        if not deployment_id:
            raise ValueError("VERCEL_DEPLOYMENT_ID environment variable is not set.")
        return deployment_id

    async def queue(
        self,
        queue_name: str,
        message: w.QueuePayload,
        *,
        deployment_id: str | None = None,
        idempotency_key: str | None = None,
        delay_seconds: float | None = None,
        **kwargs,
    ) -> str:
        # Check if we have a deployment ID either from options or environment
        if not deployment_id:
            deployment_id = os.getenv("VERCEL_DEPLOYMENT_ID")
            if not deployment_id:
                raise ValueError(
                    "No deploymentId provided and VERCEL_DEPLOYMENT_ID environment variable "
                    "is not set. Queue messages require a deployment ID to route correctly. "
                    "Either set VERCEL_DEPLOYMENT_ID or provide deploymentId in options."
                )

        payload = {
            "payload": message.model_dump(),
            "queueName": queue_name,
            # Store deploymentId in the message so it can be preserved when re-enqueueing
            "deploymentId": deployment_id,
        }
        delay = max(1, math.ceil(delay_seconds)) if delay_seconds is not None else None
        client = self._queue_client(deployment=deployment_id)
        try:
            message_id = await client.send(
                vqs.sanitize_name(queue_name),
                payload,
                idempotency_key=idempotency_key,
                delay=delay,
            )
        except vqs.DuplicateIdempotencyKeyError:
            return f"msg_duplicate_{idempotency_key or 'unknown'}"
        if message_id is None:
            return "msg_deferred"
        return message_id

    def create_queue_handler(
        self, queue_name_prefix: w.QueuePrefix, handler: w.QueueHandler
    ) -> w.HTTPHandler:
        async def async_handler(message: vqs.Message[Any]) -> None:
            try:
                body = message.payload
                if not isinstance(body, dict):
                    raise ValueError("Invalid message body: expected a JSON object")
                if "payload" not in body:
                    raise ValueError("Invalid message body: missing 'payload' field")
                if "queueName" not in body:
                    raise ValueError("Invalid message body: missing 'queueName' field")
                queue_name = body["queueName"]
                payload = body["payload"]
                result = await handler(
                    payload,
                    queue_name=queue_name,
                    attempt=message.metadata.delivery_count,
                    message_id=message.metadata.message_id,
                )
                if result is not None:
                    # Use delaySeconds approach: send new message with delay, then allow the SDK
                    # to acknowledge current message after this callback returns successfully.
                    # Clamp to max delay (23h) - for longer sleeps, the workflow will chain
                    # multiple delayed messages until the full sleep duration has elapsed.
                    delay_seconds = min(result.delay_seconds, MAX_DELAY_SECONDS)

                    # Send new message with delay BEFORE acknowledging current message.
                    # This ensures crash safety: if process dies after send but before ack,
                    # we may get a duplicate invocation but won't lose the scheduled wakeup.
                    await self.queue(
                        queue_name,
                        w.QueuePayloadAdaptor.validate_python(payload),
                        deployment_id=body.get("deploymentId"),
                        delay_seconds=delay_seconds,
                        idempotency_key=result.idempotency_key,
                    )
            except Exception:
                traceback.print_exc()
                raise

        topic_prefix = vqs.sanitize_name(queue_name_prefix)
        vqs.subscribe(topic=f"{topic_prefix}*")(async_handler)
        self._queue_callbacks.append(async_handler)

        async def http_handler(request: w.HTTPRequest) -> w.HTTPResponse:
            try:
                client = self._queue_client(deployment=vqs.ALL_DEPLOYMENTS)
                await client.accept_and_handle(request)
            except Exception:
                traceback.print_exc()
                raise
            return w.HTTPResponse(204, b"", {})

        return http_handler

    async def runs_get(self, run_id: str) -> w.WorkflowRun:
        return await self._cbor_request(
            "GET", f"/v2/runs/{run_id}?remoteRefBehavior=resolve", schema=w.WorkflowRunAdaptor
        )

    async def steps_get(self, run_id: str, step_id: str) -> w.WorkflowStep:
        return await self._cbor_request(
            "GET",
            f"/v2/runs/{run_id}/steps/{step_id}?remoteRefBehavior=resolve",
            schema=w.WorkflowStepAdaptor,
        )

    async def hooks_get_by_token(self, token: str) -> w.Hook:
        try:
            return await self._cbor_request(
                "GET",
                f"/v2/hooks/by-token?token={token}",
                schema=w.Hook,
            )
        except w.WorkflowWorldError as err:
            if err.status == 404:
                raise w.HookNotFoundError(token=token) from err
            raise

    async def events_create(self, run_id: str | None, data: w.Event) -> w.EventResult:
        run_id_path = "null" if run_id is None else run_id
        remote_ref_behavior = "resolve" if data.event_type in _EVENTS_NEEDING_RESOLVE else "lazy"
        try:
            if remote_ref_behavior == "resolve":
                return await self._cbor_request(
                    "POST",
                    f"/v3/runs/{run_id_path}/events",
                    data=data.model_dump() | {"remoteRefBehavior": remote_ref_behavior},
                    schema=w.EventResult,
                )
            else:
                # Lazy responses may contain unresolved RemoteRefs that fail
                # the strict EventResult schema. Use the loose _LazyEventResult
                # schema that tolerates unresolved refs, matching the JS SDK's
                # EventResultLazyWireSchema.
                return await self._cbor_request(
                    "POST",
                    f"/v3/runs/{run_id_path}/events",
                    data=data.model_dump() | {"remoteRefBehavior": remote_ref_behavior},
                    schema=_LazyEventResult,
                )
        except w.WorkflowWorldError as err:
            # The backend 404s hook_disposed / hook_received when the hook is
            # already disposed or never existed. Translate to a typed
            # HookNotFoundError so the runtime can treat a duplicate dispose as a
            # benign skip.
            correlation_id = getattr(data, "correlation_id", None)
            if (
                err.status == 404
                and data.event_type in w.HOOK_EVENTS_REQUIRING_EXISTENCE
                and correlation_id
            ):
                raise w.HookNotFoundError(hook_id=correlation_id) from err
            raise

    async def events_list(
        self,
        run_id: str,
        *,
        pagination: w.PaginationOptions | None = None,
    ) -> w.PaginatedResult[w.Event]:
        search_params = {}
        if pagination is not None:
            search_params.update(pagination.model_dump())
        search_params["remoteRefBehavior"] = "resolve"
        query_string = urllib.parse.urlencode(search_params)
        query = f"?{query_string}" if query_string else ""
        return await self._cbor_request(
            "GET",
            f"/v3/runs/{run_id}/events{query}",
            schema=w.PaginatedResult[w.Event],
        )

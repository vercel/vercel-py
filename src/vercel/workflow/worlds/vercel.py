import asyncio
import datetime
import os
import platform
import traceback
import urllib.parse
from typing import Any, TypeVar

import cbor2
import httpx
import pydantic

from vercel._internal.polyfills import UTC
from vercel.workers import client as vqs_client

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


def _cbor_tag_hook(decoder: cbor2.CBORDecoder, tag: cbor2.CBORTag) -> Any:
    if tag.tag == 64:
        return tag.value
    return tag


def _cbor_filter_undefined(decoder: cbor2.CBORDecoder, value: dict[str, Any]) -> dict[str, Any]:
    return {k: None if v is cbor2.undefined else v for k, v in value.items()}


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

        # utils.ts, getHttpUrl
        # Use proxy when we have project config (for authentication via Vercel API)
        self._using_proxy = bool(project_id and team_id)
        # When using proxy, requests go through api.vercel.com (with x-vercel-workflow-api-url
        # header if override is set)
        # When not using proxy, use the default workflow-server URL (with /api path appended)
        if self._using_proxy:
            self._base_url = "https://api.vercel.com/v2/workflow"
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
            from vercel.oidc.aio import get_vercel_oidc_token

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
        if "application/cbor" in resp.headers.get("Content-Type", ""):
            result = cbor2.loads(
                resp.content, tag_hook=_cbor_tag_hook, object_hook=_cbor_filter_undefined
            )
        else:
            result = resp.json()

        print(f"[DEBUG vercel] {method} {endpoint} -> {resp.status_code}")
        print(f"[DEBUG vercel]   response body: {result}")

        if resp.is_success:
            if isinstance(schema, pydantic.TypeAdapter):
                rv = schema.validate_python(result)
            else:
                rv = schema.model_validate(result)
            print(f"[DEBUG vercel]   parsed: {rv!r}")
            return rv
        else:
            raise RuntimeError(
                result.get("message")
                or f"{method} {endpoint} -> HTTP {resp.status_code}: {resp.reason_phrase}",
                {
                    "url": f"{self._base_url}{endpoint}",
                    "status": resp.status_code,
                    "code": result.get("code"),
                    "extras": result,
                },
            )

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
        print(f"[DEBUG vercel] queue(name={queue_name}, delay={delay_seconds})")
        print(f"[DEBUG vercel]   message: {message.model_dump()}")
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
        headers = {}
        if delay_seconds is not None:
            headers["Vqs-Delay-Seconds"] = str(delay_seconds)
        try:
            response = await vqs_client.send_async(
                "".join(char if char.isalnum() or char in "-_" else "-" for char in queue_name),
                payload,
                idempotency_key=idempotency_key,
                deployment_id=deployment_id,
                token=self._token if self._using_proxy else None,
                base_url=self._base_url if self._using_proxy else None,
                # The proxy will strip `/queues` from the path, and add `/api` in front,
                # so this ends up being `/api/v2/messages` when arriving at the queue server,
                # which is the same as the default basePath in VQS client.
                base_path="/queues/v2/messages" if self._using_proxy else None,
                headers=self._headers | headers,
            )
            return response["messageId"]
        except vqs_client.DuplicateIdempotencyKeyError:
            # Silently handle idempotency key conflicts - the message was already queued
            # This matches the behavior of world-local and world-postgres
            # Return a placeholder messageId since the original is not available from the error.
            # Callers using idempotency keys shouldn't depend on the returned messageId.
            return f"msg_duplicate_{idempotency_key or 'unknown'}"

    def create_queue_handler(
        self, queue_name_prefix: w.QueuePrefix, handler: w.QueueHandler
    ) -> w.HTTPHandler:
        @vqs_client.subscribe(
            topic=(f"{queue_name_prefix}*", lambda t: bool(t and t.startswith(queue_name_prefix)))
        )
        async def async_handler(body: Any, meta: vqs_client.MessageMetadata) -> None:
            print(f"[DEBUG vercel] queue_handler invoked: topic={meta.get('topic', '?')}, messageId={meta['messageId']}, attempt={meta['deliveryCount']}")
            try:
                if not isinstance(body, dict):
                    raise ValueError("Invalid message body: expected a JSON object")
                if "payload" not in body:
                    raise ValueError("Invalid message body: missing 'payload' field")
                if "queueName" not in body:
                    raise ValueError("Invalid message body: missing 'queueName' field")
                queue_name = body["queueName"]
                payload = body["payload"]
                print(f"[DEBUG vercel]   queue_name={queue_name}, calling handler...")
                result = await handler(
                    payload,
                    queue_name=queue_name,
                    attempt=meta["deliveryCount"],
                    message_id=meta["messageId"],
                )
                print(f"[DEBUG vercel]   handler returned: {result}")
                if result is not None:
                    # Use delaySeconds approach: send new message with delay, then delete current
                    # Clamp to max delay (23h) - for longer sleeps, the workflow will chain
                    # multiple delayed messages until the full sleep duration has elapsed
                    delay_seconds = min(result, MAX_DELAY_SECONDS)

                    # Send new message with delay BEFORE acknowledging current message
                    # This ensures crash safety: if process dies after send but before ack,
                    # we may get a duplicate invocation but won't lose the scheduled wakeup
                    await self.queue(
                        queue_name,
                        payload,
                        deployment_id=body.get("deploymentId"),
                        delay_seconds=delay_seconds,
                    )
            except Exception:
                traceback.print_exc()
                raise

        async def http_handler(request: w.HTTPRequest) -> w.HTTPResponse:
            content_type = request.get_header("content-type")
            if not content_type or "application/cloudevents+json" not in content_type:
                return w.HTTPResponse.json(
                    {"error": 'Invalid content type: expected "application/cloudevents+json"'},
                    status=400,
                )
            raw_body = await request.get_body()
            status_code, headers, body = await asyncio.to_thread(
                vqs_client.handle_queue_callback, raw_body
            )
            return w.HTTPResponse(status_code, body, dict(headers))

        return http_handler

    async def runs_get(self, run_id: str) -> w.WorkflowRun:
        print(f"[DEBUG vercel] runs_get(run_id={run_id})")
        rv = await self._cbor_request(
            "GET", f"/v2/runs/{run_id}?remoteRefBehavior=resolve", schema=w.WorkflowRunAdaptor
        )
        print(f"[DEBUG vercel] runs_get -> status={rv.status}")
        return rv

    async def steps_get(self, run_id: str, step_id: str) -> w.WorkflowStep:
        return await self._cbor_request(
            "GET",
            f"/v2/runs/{run_id}/steps/{step_id}?remoteRefBehavior=resolve",
            schema=w.WorkflowStepAdaptor,
        )

    async def hooks_get_by_token(self, token: str) -> w.Hook:
        return await self._cbor_request(
            "GET",
            f"/v2/hooks/by-token?token={token}",
            schema=w.Hook,
        )

    async def events_create(self, run_id: str | None, data: w.Event) -> w.EventResult:
        run_id_path = "null" if run_id is None else run_id
        remote_ref_behavior = (
            "resolve"
            if data.event_type in {"run_created", "run_started", "step_started"}
            else "lazy"
        )
        dump = data.model_dump()
        print(f"[DEBUG vercel] events_create(run_id={run_id}, event_type={data.event_type}, correlation_id={data.correlation_id})")
        print(f"[DEBUG vercel]   event dump: {dump}")
        rv = await self._cbor_request(
            "POST",
            f"/v2/runs/{run_id_path}/events",
            data=dump | {"remoteRefBehavior": remote_ref_behavior},
            schema=w.EventResult,
        )
        print(f"[DEBUG vercel] events_create -> result events={rv.events}")
        return rv

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
        print(f"[DEBUG vercel] events_list(run_id={run_id}, pagination={pagination})")
        rv = await self._cbor_request(
            "GET",
            f"/v2/runs/{run_id}/events{query}",
            schema=w.PaginatedResult[w.Event],
        )
        print(f"[DEBUG vercel] events_list -> {len(rv.data)} events, has_more={rv.has_more}")
        for i, e in enumerate(rv.data):
            print(f"[DEBUG vercel]   event[{i}]: type={e.event_type} corr={e.correlation_id}")
        return rv

import os
import platform

import httpx

from .. import world

# Hard-coded workflow-server URL override for testing.
# Set this to test against a different workflow-server version.
# Leave empty string for production (uses default vercel-workflow.com).
#
# Example: 'https://workflow-server-git-branch-name.vercel.sh'

WORKFLOW_SERVER_URL_OVERRIDE = ""


class VercelWorld(world.World):
    def __init__(
        self,
        *,
        token: str | None = None,
        environment: str | None = None,
        project_id: str | None = None,
        team_id: str | None = None,
    ) -> None:
        self._provided_token = token

        # utils.ts, getHttpUrl and @vercel/queue client initialization
        # Use proxy when we have project config (for authentication via Vercel API)
        using_proxy = bool(project_id and team_id)
        # When using proxy, requests go through api.vercel.com (with x-vercel-workflow-api-url
        # header if override is set)
        # When not using proxy, use the default workflow-server URL (with /api path appended)
        if using_proxy:
            base_url = "https://api.vercel.com/v1/workflow"
            # The proxy will strip `/queues` from the path, and add `/api` in front,
            # so this ends up being `/api/v3/topic` when arriving at the queue server,
            # which is the same as the default basePath in VQS client.
            base_path = "/queues/v3/topic"
        else:
            base_url = os.getenv("VERCEL_QUEUE_BASE_URL", "https://vercel-workflow.com")
            base_path = os.getenv("VERCEL_QUEUE_BASE_PATH", "/api/v3/topic")
        self._base_url = f"{base_url.rstrip('/')}{base_path}"

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
        if WORKFLOW_SERVER_URL_OVERRIDE and using_proxy:
            self._headers["x-vercel-workflow-api-url"] = WORKFLOW_SERVER_URL_OVERRIDE

    async def _get_token(self) -> str:
        if self._provided_token:
            return self._provided_token

        env_token = os.environ.get("VERCEL_QUEUE_TOKEN")
        if env_token:
            return env_token

        # Fall back to Vercel OIDC token when running inside a Vercel environment.
        from vercel.oidc.aio import get_vercel_oidc_token as get_vercel_oidc_token_async

        token = await get_vercel_oidc_token_async()
        if token:
            return token

        raise ValueError(
            "Failed to resolve queue token. "
            "Set the WORKFLOW_VERCEL_AUTH_TOKEN or VERCEL_QUEUE_TOKEN environment variable, "
            "or ensure a Vercel OIDC token is available in this environment.",
        )

    async def queue(
        self,
        queue_name: str,
        message: world.QueuePayload,
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
            "payload": message,
            "queueName": queue_name,
            # Store deploymentId in the message so it can be preserved when re-enqueueing
            "deploymentId": deployment_id,
        }
        sanitized_queue_name = "".join(c if c.isalnum() or c in "-_" else "-" for c in queue_name)
        headers = {
            "Authorization": f"Bearer {await self._get_token()}",
            "Vqs-Deployment-Id": deployment_id,
        }
        if idempotency_key:
            headers["Vqs-Idempotency-Key"] = idempotency_key
        if delay_seconds is not None:
            headers["Vqs-Delay-Seconds"] = str(delay_seconds)

        async with httpx.AsyncClient(
            base_url=self._base_url, headers=self._headers
        ) as send_message_client:
            response = await send_message_client.post(
                f"/{sanitized_queue_name}",
                json=payload,
                headers=headers,
            )

        # Silently handle idempotency key conflicts - the message was already queued
        # This matches the behavior of world-local and world-postgres
        if response.status_code == 409:
            # Return a placeholder messageId since the original is not available from the error.
            # Callers using idempotency keys shouldn't depend on the returned messageId.
            # TODO: VQS should return the message ID of the existing message, or we should
            # stop expecting any world to include this
            return f"msg_duplicate_{idempotency_key or 'unknown'}"

        response.raise_for_status()

        data = response.json()
        if not isinstance(data, dict) or "messageId" not in data:
            raise RuntimeError("Queue API returned an unexpected response: missing 'messageId'")

        return str(data["messageId"])

    def create_queue_handler(
        self, queue_name_prefix: world.QueuePrefix, handler: world.QueueHandler
    ) -> world.HTTPHandler:
        async def http_handler(request: world.HTTPRequest) -> world.HTTPResponse:
            raise NotImplementedError()

        return http_handler

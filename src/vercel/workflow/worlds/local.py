import json
import os

from .. import world

LOCAL_QUEUE_MAX_VISIBILITY = int(
    os.environ.get("WORKFLOW_LOCAL_QUEUE_MAX_VISIBILITY", "0")
) or float("inf")


class LocalWorld(world.World):
    async def queue(
        self,
        queue_name: str,
        message: world.QueuePayload,
        *,
        deployment_id: str | None = None,
        idempotency_key: str | None = None,
    ) -> str:
        return ""

    def create_queue_handler(
        self, queue_name_prefix: world.QueuePrefix, handler: world.QueueHandler
    ) -> world.HTTPHandler:
        async def http_handler(request: world.HTTPRequest) -> world.HTTPResponse:
            # Get request body
            body = await request.get_body()

            if not body:
                return world.HTTPResponse.json({"error": "Missing request body"}, status=400)

            # Get required headers
            queue_name = request.get_header("x-vqs-queue-name")
            message_id = request.get_header("x-vqs-message-id")
            attempt_str = request.get_header("x-vqs-message-attempt")

            if not queue_name or not message_id or not attempt_str:
                return world.HTTPResponse.json({"error": "Missing required headers"}, status=400)

            # Validate queue name prefix
            if not queue_name.startswith(queue_name_prefix):
                return world.HTTPResponse.json({"error": "Unhandled queue"}, status=400)

            # Validate attempt number
            try:
                attempt = int(attempt_str)
            except ValueError:
                return world.HTTPResponse.json(
                    {"error": "Invalid x-vqs-message-attempt header"}, status=400
                )

            # Deserialize the message body
            try:
                message = json.loads(body.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                return world.HTTPResponse.json({"error": f"Invalid JSON body: {e}"}, status=400)

            # Call the handler
            try:
                result = await handler(
                    message, attempt=attempt, queue_name=queue_name, message_id=message_id
                )

                # Handle timeout response
                timeout_seconds: float | None = None
                if result:
                    timeout_seconds = min(result, LOCAL_QUEUE_MAX_VISIBILITY)
                if timeout_seconds:
                    return world.HTTPResponse.json({"timeoutSeconds": timeout_seconds}, status=503)

                return world.HTTPResponse.json({"ok": True})
            except Exception as error:
                return world.HTTPResponse.json({"error": str(error)}, status=500)

        return http_handler

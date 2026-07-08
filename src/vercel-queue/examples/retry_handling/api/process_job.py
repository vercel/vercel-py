from __future__ import annotations

from vercel.queue import Message, RetryAfter, asgi_app, subscribe


@subscribe(topic="jobs", consumer_group=f"api/{__name__}.py", retry_after=30, max_attempts=3)
async def process_job(message: Message[dict[str, object]]) -> None:
    job_id = message.payload.get("job_id", "unknown")
    if message.payload.get("simulate_failure") is True and message.metadata.delivery_count < 3:
        print("Retrying job", job_id, "attempt", message.metadata.delivery_count)
        raise RetryAfter(30)
    print("Processed job", job_id, "attempt", message.metadata.delivery_count)


app = asgi_app()

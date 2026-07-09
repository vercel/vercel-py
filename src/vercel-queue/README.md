# Vercel Queues

A Python client library for interacting with the Vercel Queues API,
designed for seamless integration with Vercel deployments.

## Features

- **Simple API**: `send`, `subscribe`, and `asgi_app` cover standard workflows.
- **Automatic Triggering on Vercel**: Vercel invokes your function when messages are ready.
- **Works Anywhere**: `send`, decorated subscribers, and manual `poll` loops work on Vercel, self-hosted workers, and locally.
- **Sync and Async Clients**: Prefer async for applications, use sync for scripts and blocking workers.
- **Type Safety**: `Topic[T]`, typed messages, and optional Pydantic validation.
- **Customizable Serialization**: Built-in JSON, text, binary, and streaming transports.
- **Local Development Support**: Embedded queue helpers for tests and development.

## Installation

```bash
uv add vercel-queue
```

For Pydantic-backed typed payloads, add the optional `typed` extra:

```bash
uv add "vercel-queue[typed]"
```

## Quick Start

**1. Link your Vercel project and pull credentials:**

The SDK authenticates via OIDC. Link your project if you haven't already, then
pull to get fresh tokens:

```bash
npm i -g vercel
vc link   # if you haven't already
vc env pull
```

**2. Send a message anywhere in your app:**

```python
from vercel.queue import send

message_id = await send("my-topic", {"message": "Hello world"})
```

**3. Handle incoming messages with an API route function:**

```python
# api/queue.py
from vercel.queue import asgi_app, subscribe


@subscribe(topic="my-topic", consumer_group="api/queue.py")
async def process_message(message):
    print("Processing:", message)


# An ASGI app instance that converts incoming message callbacks
# sent by Vercel Queues and routes them to handlers.
app = asgi_app()
```

**4. Configure `vercel.json`:**

```json
{
    "functions": {
        "api/queue.py": {
            "experimentalTriggers": [{ "type": "queue/v2beta", "topic": "my-topic" }]
        }
    }
}
```

**5. Deploy:**

```bash
vc deploy
```

## Publishing Messages

```python
from datetime import timedelta
from vercel.queue import send

# Simple send
message_id = await send("my-topic", {"message": "Hello world"})

# With options
message_id = await send(
    "my-topic",
    {"message": "Hello world"},
    idempotency_key="unique-key",  # Prevent duplicate messages
    retention=timedelta(hours=1),
    delay=timedelta(minutes=1),  # Delay delivery by 1 minute
)
```

Example usage in a FastAPI route:

```python
from fastapi import FastAPI

from vercel.queue import send

app = FastAPI()


@app.post("/orders")
async def create_order(order: dict[str, str]) -> dict[str, str | None]:
    message_id = await send("my-topic", {"message": "Hello world"})
    return {"message_id": message_id}
```

`send` returns a message ID, or `None` when the server accepted the message for delivery,
but could not process it fully yet. Deferred messages are still delivered.

## Receiving and Handling Messages

Decorated subscribers are the recommended way to handle messages. Register
subscribers with the `@subscribe` decorator:

```python
# handle_orders.py
from vercel.queue import subscribe


@subscribe(topic="orders")
async def fulfill_order(order):
    await process_order(order)
    # Raising an error will automatically retry the message.
```

Decorating a function with `@subscribe` only declares it as a handler for a
particular topic. You also need a way to accept and route messages to handlers.
There are two primary ways: deploying handlers as Vercel Functions in push mode
and running polling loops on other infrastructure.

### Auto-scaled push-mode on Vercel

The recommended way of deploying queue subscribers is to deploy them as Vercel Functions

**Vercel Function (plain `/api` directory):**

```python
# api/handle_orders.py
from vercel.queue import asgi_app, subscribe


@subscribe(topic="orders", consumer_group="api/handle_orders.py")
async def handle_order(message):
    print("Processing:", message)


# An ASGI app instance that converts incoming message callbacks
# sent by Vercel Queues and routes them to handlers.
app = asgi_app()
```

**vercel.json**:

```json
{
    "functions": {
        "api/queue/orders.py": {
            "experimentalTriggers": [
                {
                    "type": "queue/v2beta",
                    "topic": "orders",
                    "retryAfterSeconds": 60,
                    "initialDelaySeconds": 0
                }
            ]
        }
    }
}
```

### Automatic Polling Loop

Queue message handlers can also be invoked by polling a topic manually. `vercel.queue` provides a
convenience API that starts an infinite polling loop for messages matching a given subscriber:

```python
# subscriber_poll.py
import asyncio
from vercel.queue import poll_and_handle, subscribe


@subscribe(topic="orders")
async def fulfill_order(order):
    print("Processing Order:", order)


async def main():
    poller = asyncio.create_task(poll_and_handle(fulfill_order, interval=1))
    # do work and cancel poller on a condition, such as a signal


asyncio.run(main())
```

See [subscriber_poll.py example](./examples/subscriber_poll.py) for a complete
example.

Naturally, a synchronous polling loop helper is also available:

```python
# subscriber_poll_sync.py
from vercel.queue.sync import poll_and_handle, subscribe


@subscribe(topic="orders")
def fulfill_order(order):
    print("Processing Order:", order)


def main():
    poller = poll_and_handle(fulfill_order, interval=1)
    # ...
    poller.cancel()


main()
```

Synchronous polling loops use threads instead of async tasks.

### Manual Polling

It is also possible to poll a topic and receive messages directly:

```python
# poll_loop.py
from vercel.queue import poll


async def main():
    async for delivery in poll(
        topic="orders",
        consumer_group="fulfillment",
        limit=10,
    ):
        async with delivery as message:
            await process_order(message.payload)
```

`poll()` polls the topic once for up to `limit` message deliverires which are then yielded by the
iterator. Use the yielded `delivery` as a context manager to obtain the message envelope which
contains the `payload` and `metadata` properties. Note that `limit` means _up-to_ and it is
possible for `poll()` to return an empty iterator. In other words, `poll()` does not block until
new messages are available and it is usually necessary to build a polling loop around it.

### Region Considerations when Polling

Messages can only be received from the region they were sent to. When polling, use a fixed region
for both sending and receiving, such as `"iad1"`. Avoid using a changing runtime region for manual
polling, because it can distribute messages across regions unpredictably.

## Retry and Backoff

When a topic handler raises, the message is not acknowledged and becomes available for redelivery
after the `retryAfterSeconds` interval configured in `vercel.json`. Retries continue until the
handler succeeds or the message expires.

For finer control over retry timing, raise `RetryAfter` from a subscriber:

```python
from vercel.queue import RetryAfter, subscribe


@subscribe(topic="orders")
async def fulfill_order(order: dict[str, str]) -> None:
    try:
        await process_order(order)
    except RateLimitError as exc:
        raise RetryAfter(60) from exc
```

Use `message.metadata.delivery_count` for exponential backoff:

```python
from vercel.queue import Message, RetryAfter, subscribe


@subscribe(topic="orders")
async def fulfill_order(message: Message[dict[str, str]]) -> None:
    try:
        await process_order(message.payload)
    except TemporaryError as exc:
        delay = min(300, 2**message.metadata.delivery_count * 5)
        raise RetryAfter(delay) from exc
```

## Custom Client Configuration

For most use cases, the top-level `send`, `poll_and_handle`, and `poll` are all you need. For
advanced configuration such as explicit authentication, custom headers, deployment pinning, or
custom queue service URLs, create a `QueueClient` explicitly:

```python
# explicit_client.py
from vercel.queue import ALL_DEPLOYMENTS, QueueClient

queue = QueueClient(
    region="iad1",  # Required unless VERCEL_REGION is set
    token="my-token",  # Auth token; detected from the environment by default
    headers={"X-Custom": "header"},
    deployment=ALL_DEPLOYMENTS,  # Receive messages from all deployments when polling
    timeout=10,  # default timeout for API operations
    http_client_factory=httpx2.AsyncClient,  # any httpx-compatible HTTP client
)

await queue.send("my-topic", {"message": "Hello world"})
```

By default queue clients send requests to `https://<region>.vercel-queue.com/`. A custom endpoint
can be configured by passing a `base_url` keyword argument to the client constructor. The value
can be a fixed string, a `format()` template containing a `{region}` placeholder, or a callable
taking region name as a string and returning a formatted URL:

```python
from vercel.queue import QueueClient

# Custom domain with a base path.
queue = QueueClient(base_url="https://proxy.example/queues/{region}")

# Callable resolver.
queue = QueueClient(base_url=lambda region: f"https://{region}.queue.internal")
```

## Type-safe Message Passing and Streaming

By default, messages passed to `send()` and received by `@subscribe` handlers and `poll()` are
transmitted as JSON, so values must be JSON-serializable by Python. This is usually fine for
unstructured data, such as dictionaries and lists containing simple data. For more complex types,
such as `dataclasses` or Pydantic models, simply annotate the first argument of the handler
function:

```python
from dataclasses import dataclass
from vercel.queue import send, subscribe


@dataclass
class Email:
    to: str
    subject: str
    body: str


@subscribe(topic="emails")
async def receive_email(email: Email) -> None:
    print(f"Received email to {email.to}: {email.subject}")
```

Note that type-safe message handling requires `pydantic>=2.0` to be available. The simplest way
to ensure the correct version of Pydantic is to install the `vercel-queue` package with the
`[typed]` feature: `uv add vercel-queue[typed]`.

To send types that require non-trivial serialization, pass the topic name not as a plain string,
but as a type-specialized `Topic` instance:

```python
# custom_transport.py
from vercel.queue import Topic, send


@dataclass
class Email:
    to: str
    subject: str
    body: str


emails_topic = Topic[Email]("emails")


async def send_email(to, subject, body):
    await send(emails_topic, Email(to, subject, body))


# Type-specialized topics can also be used with @subscribe, in which case the
# type annotation on the handler argument must match the type of the topic
@subscribe(topic=emails_topic)
async def receive_email(email: Email) -> None:
    print(f"Received email to {email.to}: {email.subject}")
```

Explicit type annotations can also be used to enable payload streaming for large messages:

```python
# streaming.py
from collections.abc import AsyncIterable, AsyncIterator

from vercel.queue import ByteStreamTransport, QueueClient, Topic, subscribe

large_file = Topic[AsyncIterable[bytes]]("large-file")


async def file_chunks():
    with open("large.bin", "rb") as file:
        while chunk := file.read(1024 * 1024):
            yield chunk


async def send_file():
    await queue.send(large_file, file_chunks())


@subscribe(topic=large_file, consumer_group="archive")
async def archive_file(chunks: AsyncIterable[bytes]) -> None:
    async for chunk in chunks:
        await write_chunk(chunk)
```

Message transport is automatically determined from the handler argument type annotation
or `Topic` type specialization according to the following table:

| Topic payload type                               | Default transport       | Message format               |
| ------------------------------------------------ | ----------------------- | ---------------------------- |
| JSON-compatible values, `dict[...]`, `list[...]` | `RawJsonTransport[Any]` | JSON                         |
| Pydantic models and other structured annotations | `TypedJsonTransport[T]` | JSON with receive validation |
| `bytes`                                          | `ByteBufferTransport`   | Buffered binary              |
| `str`                                            | `TextBufferTransport`   | Buffered UTF-8 text          |
| `Iterable[bytes]` or `AsyncIterable[bytes]`      | `ByteStreamTransport`   | Streaming binary             |
| `Iterable[str]` or `AsyncIterable[str]`          | `TextStreamTransport`   | Streaming UTF-8 text         |

Transport can also be set explicitly on the topic when the topic is unstructured or
when custom serialization is needed:

```python
# custom_transport.py


@dataclass
class Invoice:
    invoice_id: str
    customer_id: str
    total_cents: int


class InvoiceFormTransport:
    content_type = "application/x-www-form-urlencoded"

    def serialize(self, value: Invoice) -> bytes:
        return urlencode({
            "invoice_id": value.invoice_id,
            "customer_id": value.customer_id,
            "total_cents": str(value.total_cents),
        }).encode("utf-8")

    async def deserialize(
        self,
        payload: AsyncIterator[bytes],
        *,
        content_type: str,
    ) -> Invoice:
        body = bytearray()
        async for chunk in payload:
            body.extend(chunk)

        parsed = parse_qs(body.decode("utf-8"), strict_parsing=True)
        return Invoice(
            invoice_id=_single(parsed, "invoice_id"),
            customer_id=_single(parsed, "customer_id"),
            total_cents=int(_single(parsed, "total_cents")),
        )


invoice_topic = Topic[Invoice](
    "invoices",
    transport=InvoiceFormTransport(),
)


@subscribe(topic=invoice_topic)
async def handle_invoice(invoice: Invoice) -> None: ...
```

## Error Handling

```python
from vercel.queue import (
    BadRequestError,
    DuplicateIdempotencyKeyError,
    ForbiddenError,
    InternalServerError,
    UnauthorizedError,
    send,
)

try:
    await send("my-topic", payload)
except UnauthorizedError:
    print("Invalid token - refresh authentication")
except ForbiddenError:
    print("Environment mismatch - check configuration")
except BadRequestError as exc:
    print("Invalid parameters:", exc)
except DuplicateIdempotencyKeyError as exc:
    print("Duplicate idempotency key:", exc)
except InternalServerError:
    print("Server error - retry with backoff")
```

All error types:

| Error                                | Description                                   |
| ------------------------------------ | --------------------------------------------- |
| `BadRequestError`                    | Invalid request parameters                    |
| `UnauthorizedError`                  | Authentication failed                         |
| `ForbiddenError`                     | Access denied or environment mismatch         |
| `DuplicateIdempotencyKeyError`       | Idempotency key already used                  |
| `ConsumerDiscoveryError`             | Could not reach consumer deployment           |
| `ConsumerRegistryNotConfiguredError` | Project is not configured for queues          |
| `DeploymentResolutionError`          | Deployment ID could not be resolved           |
| `DuplicateSubscriptionError`         | Local subscriber registration overlaps        |
| `InternalServerError`                | Unexpected server error                       |
| `InvalidLimitError`                  | Batch limit outside valid range               |
| `MessageAlreadyProcessedError`       | Message already successfully processed        |
| `MessageCorruptedError`              | Message data could not be parsed              |
| `MessageLockedError`                 | Message is being processed elsewhere          |
| `MessageNotFoundError`               | Message does not exist or expired             |
| `MessageUnavailableError`            | Message exists but cannot be claimed          |
| `PayloadValidationError`             | Payload validation failed                     |
| `ProtocolError`                      | Queue service returned malformed metadata     |
| `ServiceError`                       | Unexpected queue response                     |
| `SubscriptionError`                  | Subscriber signature or configuration invalid |
| `ThrottledError`                     | Queue service throttled the request           |
| `TokenResolutionError`               | OIDC token could not be resolved              |
| `UnhandledMessageError`              | No subscriber matched an incoming delivery    |

## Environment Variables

| Variable                | Description                                    | Default |
| ----------------------- | ---------------------------------------------- | ------- |
| `VERCEL_REGION`         | Current region, auto-set by Vercel             | -       |
| `VERCEL_QUEUE_BASE_URL` | Fixed base URL or `{region}` template override | -       |
| `VERCEL_QUEUE_DEBUG`    | Enable debug logging with `1` or `true`        | -       |
| `VERCEL_QUEUE_TOKEN`    | Queue bearer token override                    | -       |
| `VERCEL_DEPLOYMENT_ID`  | Deployment ID, auto-set by Vercel              | -       |

## Service Limits & Constraints

### Throughput & Storage

| Limit                       | Value                 | Notes                               |
| --------------------------- | --------------------- | ----------------------------------- |
| Message throughput          | 10,000+ msg/sec/topic | Scales horizontally                 |
| Payload size                | 100 MB                | Smaller messages have lower latency |
| Number of topics            | Unlimited             | No hard limit                       |
| Consumer groups per message | ~4,000                | Per-message limit                   |
| Messages per queue          | Unlimited             | No hard limit                       |

### Parameter Constraints

#### Publishing Messages

| Parameter         | Default      | Min | Max          | Notes                               |
| ----------------- | ------------ | --- | ------------ | ----------------------------------- |
| `retention`       | 86,400 (24h) | 60  | 604,800 (7d) | Message TTL                         |
| `delay`           | 0            | 0   | 604,800 (7d) | Cannot exceed retention             |
| `idempotency_key` | -            | -   | -            | Dedup window: `min(retention, 24h)` |

#### Receiving Messages

| Parameter        | Default | Min | Max   | Notes                           |
| ---------------- | ------- | --- | ----- | ------------------------------- |
| `lease_duration` | 300     | 30  | 3,600 | Lock duration during processing |
| `limit`          | 1       | 1   | 10    | Messages per request            |

### Identifier Formats

| Identifier     | Input                | Stored queue name            |
| -------------- | -------------------- | ---------------------------- |
| Topic name     | `[A-Za-z0-9_-]+`     | Same as input                |
| Consumer group | Any non-empty string | `sanitize_name(...)`         |
| Message ID     | Opaque string        | `0-1`, `3-7K9mNpQrS`         |
| Receipt handle | Opaque string        | Used for ack and lease calls |

Use `sanitize_name` to convert arbitrary non-empty names to `SanitizedName`
markers. Plain strings are reversibly escaped, including underscores. Use
`SanitizedName` only when passing a queue-safe name that has already been
sanitized and must not be escaped again.

## Wildcard Topics

It is possible to subscribe to multiple topics by using a wildcard (`*`) at the
end of the topic name. Bare wildcards are also allowed and act as catch-all
handlers.

```python
# wildcard_topic.py

from vercel.queue import subscribe


@subscribe(topic="user-*")
async def handle_user_event(event: dict[str, str]) -> None:
    await process_user_event(event)
```

Wildcard topics are not supported by `poll()` and if a wildcard handler is passed to
`poll_and_handle()`, a non-wildcard list of topics must also be specified to disambiguate
polling.

## Local Development

For local tests and integration development, use the embedded queue service or
pytest plugin. It exercises the same client send, subscriber callback, lease renewal,
and acknowledgement paths without requiring a deployed Vercel Function.

```python
import anyio

from vercel.queue import subscribe
from vercel.queue.embedded import embedded_queue_service


async def wait_until(predicate) -> None:
    while not predicate():
        await anyio.sleep(0.01)


async def test_queue() -> None:
    seen: list[str] = []

    @subscribe(topic="my-topic")
    async def handler(message: dict[str, str]) -> None:
        seen.append(message["message"])

    async with embedded_queue_service() as service:
        client = service.get_async_client()
        message_id = await client.send("my-topic", {"message": "Hello world"})
        assert message_id is not None
        service.dispatcher.wake()
        await wait_until(lambda: service.server.state.by_id[message_id].acknowledged)

    assert seen == ["Hello world"]
```

Standalone HTTP dev server support is available through the `devserver` extra:

```bash
python -m vercel.queue.devserver --host 127.0.0.1
```

Install it with `vercel-queue[devserver]`. The command prints a JSON `baseUrl`
for the local queue API, including the random available port selected when
`--port` is omitted. Pass `--port 8000` to bind a fixed port. This is useful for
cross-process or cross-runtime local integration.

## License

MIT

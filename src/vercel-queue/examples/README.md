# Vercel Queue Python examples

These examples cover both deployable push-delivery apps and runnable polling
scripts. Deployable examples live in subdirectories. Polling examples are
single Python scripts because polling consumers are long-running processes, not
Vercel Functions.

## Deploy an Example

Deploy an example by using the example directory as the Vercel project root:

```bash
cd src/vercel-queue/examples/typed_pydantic
vc link
vc deploy
```

After deployment, send a message with the module CLI from an environment that
has Vercel Queue credentials. Use `--region iad1` for local sends outside
Vercel:

```bash
uv run --project ../.. python -m vercel.queue send --region iad1 --topic typed-orders --json '{"order_id":"ord_123","total_cents":2500}'
```

## Deployable Apps

| Example | Topic | Shows |
| --- | --- | --- |
| [`typed_pydantic`](typed_pydantic/) | `typed-orders` | Pydantic payload validation with `TypedJsonTransport` |
| [`binary_buffer`](binary_buffer/) | `images` | Small binary messages as `bytes` |
| [`large_file_stream`](large_file_stream/) | `files` | Byte streaming with `ByteStreamTransport` |
| [`text_stream`](text_stream/) | `logs` | UTF-8 text streaming with `TextStreamTransport` |
| [`custom_transport`](custom_transport/) | `invoices` | A custom form-encoded transport shared by sender and subscriber |
| [`retry_handling`](retry_handling/) | `jobs` | Retry control with `RetryAfter`, delivery metadata, and `max_attempts` |

## Polling Scripts

Run polling examples from the workspace root after linking a Vercel project and
pulling environment variables:

```bash
vc link
vc env pull
uv run --project src/vercel-queue python src/vercel-queue/examples/subscriber_poll.py
uv run --project src/vercel-queue --extra trio python src/vercel-queue/examples/subscriber_poll_trio.py
```

| Example | Topic | Shows |
| --- | --- | --- |
| [`subscriber_poll.py`](subscriber_poll.py) | `orders` | Recommended subscriber polling with `@subscribe` and `poll_and_handle` |
| [`subscriber_poll_trio.py`](subscriber_poll_trio.py) | `orders` | Recommended subscriber polling with `@subscribe` and `poll_and_handle` |
| [`async_json_poll.py`](async_json_poll.py) | `orders` | Async JSON send and delivery-scoped polling |
| [`sync_json_poll.py`](sync_json_poll.py) | `events` | Synchronous JSON send and delivery-scoped polling |

## App Layout

Each deployable app contains:

- `api/<handler>.py`: a queue push callback using `@subscribe` and `asgi_app()`.
- `pyproject.toml`: the app dependencies.
- `vercel.json`: the queue trigger configuration for the callback function.

Run each send command from the corresponding deployed example directory:

```bash
uv run --project ../.. python -m vercel.queue send --region iad1 --topic typed-orders --json '{"order_id":"ord_123","total_cents":2500}'
uv run --project ../.. python -m vercel.queue send --region iad1 --topic images --binary iVBORw0K
printf 'example file payload\n' > /tmp/queue-file.bin
uv run --project ../.. python -m vercel.queue send --region iad1 --topic files --binary-from /tmp/queue-file.bin
uv run --project ../.. python -m vercel.queue send --region iad1 --topic logs --text $'started\nprocessed\nfinished\n'
uv run --project . python send_invoice.py
uv run --project ../.. python -m vercel.queue send --region iad1 --topic jobs --json '{"job_id":"job_123","simulate_failure":true}'
```

After sending, run `vc logs <deployment-url>` from the deployed example
directory. Each app README shows the expected subscriber log output.

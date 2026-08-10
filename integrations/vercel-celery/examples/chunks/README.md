# Celery chunks app

Deploy this directory as a Vercel app:

```bash
cd integrations/vercel-celery/examples/chunks
vc link
vc deploy
```

Queue the chunks by requesting the sender endpoint on the deployed app:

```bash
vc curl /enqueue
```

Check the worker logs:

```bash
vc logs <deployment-url>
```

Expected output includes:

```text
queued chunk group <group-id>
0 + 0 = 0
99 + 99 = 198
```

`pyproject.toml` declares `worker.py` as a queue subscriber:

```toml
[[tool.vercel.subscribers]]
entrypoint = "worker:celery_app"
```

The Vercel build introspects the Celery app's queues and compiles the
subscriber into a queue-triggered function; no `vercel.json` trigger
configuration is needed. The FastAPI sender in `main.py` uses Celery canvas
chunks:

```python
add.chunks(zip(range(100), range(100), strict=False), 10).apply_async()
```

Tasks publish through the `vercel://` broker and store results with the
`vercel-runtime-cache://` backend.

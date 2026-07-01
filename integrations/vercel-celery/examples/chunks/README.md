# Celery chunks app

Deploy this directory as a Vercel app:

```bash
cd integrations/vercel-celery/examples/chunks
vc link
vc deploy
```

Queue the chunks by requesting the sender function on the deployed app:

```bash
vc curl /api/send_chunks
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

The `vercel.json` file declares `api/celery_worker.py` as a queue-triggered
function for the default `celery` topic. The HTTP sender in `api/send_chunks.py`
uses Celery canvas chunks:

```python
add.chunks(zip(range(100), range(100), strict=False), 10).apply_async()
```

Tasks publish through the `vercel://` broker and store results with the
`vercel-runtime-cache://` backend.

# Text Stream Queue app

Deploy this directory as a Vercel app:

```bash
cd src/vercel-queue/examples/text_stream
vc link
vc deploy
```

Send a message from an environment that has Vercel Queue credentials. Use
`--region iad1` for local sends outside Vercel:

```bash
uv run --project ../.. python -m vercel.queue send --region iad1 --topic logs --text $'started\nprocessed\nfinished\n'
```

Check the function logs:

```bash
vc logs <deployment-url>
```

Expected output includes:

```text
started
processed
finished
```

The push subscriber receives an iterable of decoded text chunks.
